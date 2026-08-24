import { assertEquals } from "@std/assert";
import type * as RefWorkerModule from "./examples/remote_ref/worker.ts";
import type * as RefProcessWorker from "./test_fixtures/ref_process_worker.ts";
import { spawn, spawnProcess } from "./spawn.ts";

// Mixed-topology test: a Worker actor (messageport transport) and a process
// actor (Mux transport) interoperate. A remote reference produced by the
// Worker is handed to the process actor, whose first call triggers the
// acquire — a Mux holder acquiring from a messageport owner, routed by the
// main thread's registry.
const refModule = await import("./examples/remote_ref/ref_codec.ts");
const REF_CODEC = [refModule.remoteRefCodec];

interface CounterRef {
  increment(): Promise<number>;
  get(): Promise<number>;
}

Deno.test("mixed topology: Worker-owned ref acquired by a process actor over Mux", async () => {
  // Owner: a Worker serving remote-ref (messageport transport).
  const ownerWorker = await spawn<typeof RefWorkerModule.rpc>(
    new Worker(import.meta.resolve("./examples/remote_ref/worker.ts"), {
      type: "module",
    }),
    { codecs: REF_CODEC },
  );
  // Holder: a process actor (Mux transport) that holds a handed-off ref.
  const holderProc = await spawnProcess<typeof RefProcessWorker.rpc>(
    "./test_fixtures/ref_process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    // Fresh ref from the Worker: direct messageport transfer to main.
    const refFromWorker = await ownerWorker
      .createCounter() as unknown as CounterRef;
    assertEquals(await refFromWorker.increment(), 1);

    // Hand the Worker-owned ref to the process actor. B's first call is a
    // refId-only acquire: main routes owner (messageport Worker) ↔ requester
    // (Mux process) — the acquire must complete across the mixed transports.
    await holderProc.holdRef(refFromWorker);
    const outcome = await Promise.race([
      holderProc.callHeldIncrement().then(
        (n) => `ok:${n}` as const,
        (e: unknown) => e,
      ),
      new Promise((r) => setTimeout(() => r("TIMEOUT"), 5_000)),
    ]);
    assertEquals(outcome, "ok:2");
    // Same shared counter in the Worker: still reachable from main.
    assertEquals(await refFromWorker.increment(), 3);
  } finally {
    await holderProc.dispose();
    await ownerWorker.dispose();
  }
});
