import { assertEquals } from "@std/assert";
import type * as ProcessWorker from "./test_fixtures/process_worker.ts";
import type * as RefProcessWorker from "./test_fixtures/ref_process_worker.ts";
import { spawnProcess } from "./spawn.ts";

// The process fixture serves remote-ref codec; every spawn must register it
// so the handshake codec lists match.
const refModule = await import("./examples/remote_ref/ref_codec.ts");
const REF_CODEC = [refModule.remoteRefCodec];

Deno.test("spawnProcess: RPC round-trip over fork IPC", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { permissions: { read: true }, codecs: REF_CODEC },
  );
  try {
    assertEquals(await actor.add(1, 2), 3);
    assertEquals(await actor.greet("world"), "hello world");
    const m = await actor.map();
    assertEquals(m.get("a"), 1);
    assertEquals(m.get("b"), 2);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: dispose terminates the child process", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  await actor.add(1, 1);
  await actor.dispose();
  // After dispose, further calls reject with ActorDiedError.
  await assertEquals(
    await actor.add(1, 1).then(() => "ok", () => "rejected"),
    "rejected",
  );
});

Deno.test("spawnProcess: signal aborts creation", async () => {
  const aborted = AbortSignal.timeout(1);
  await aborted; // let it fire
  const result = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { signal: aborted, codecs: REF_CODEC },
  ).then(() => "resolved", (e) => `rejected:${e.name}`);
  assertEquals(result.startsWith("rejected"), true);
});

Deno.test("spawnProcess: AsyncIterable streams across processes (Mux token)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const stream = await Promise.race([
      actor.count(5),
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("count() timeout")), 2000)
      ),
    ]) as AsyncIterable<number>;
    const got: number[] = [];
    for await (const n of stream) got.push(n);
    assertEquals(got, [0, 1, 2, 3, 4]);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: AbortSignal crosses processes and cancels work", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const controller = new AbortController();
    const running = actor.spin(5000, controller.signal);
    await new Promise((r) => setTimeout(r, 50));
    controller.abort();
    const iterations = await running;
    assertEquals(typeof iterations, "number");
    // The child saw the abort and stopped well before the 5s budget.
    assertEquals(iterations < 5000, true);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: a pre-aborted signal still cancels remote work (status frame not lost)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    // The signal is already aborted before the call: the abort-signal codec
    // sends its status frame immediately, which can race the placeholder
    // decode on a Mux transport. The rebuilt signal must still be aborted.
    const controller = new AbortController();
    controller.abort("already");
    const iterations = await actor.spin(5000, controller.signal);
    // The child saw the pre-aborted state and returned quickly (0 iterations).
    assertEquals(iterations, 0);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: callbacks cross processes and return results", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const result = await actor.apply((x: number) => x * 2, 21);
    assertEquals(result, 42);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: remote references cross processes (Mux token)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const counter = await actor
      .getCounter() as unknown as import("./examples/remote_ref/ref_codec.ts").RemoteRef<
        { inc(): number; get(): number }
      >;
    assertEquals(await counter.inc(), 1);
    assertEquals(await counter.inc(), 2);
    assertEquals(await counter.get(), 2);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: abort still works after a stream was consumed (channel reuse)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    // Consume a stream first (opens/closes an iterable Mux channel).
    for await (const _ of await actor.count(2)) {
      // drain the stream to trigger its channel close
    }
    // Then an AbortSignal must still propagate. The abort fires 50ms in; the
    // child's spin loop must stop almost immediately (well under 100 iterations).
    const controller = new AbortController();
    const running = actor.spin(5000, controller.signal);
    await new Promise((r) => setTimeout(r, 50));
    controller.abort();
    const iterations = await running;
    assertEquals(iterations < 100, true);
  } finally {
    await actor.dispose();
  }
});

interface CounterRef {
  inc(): Promise<number>;
  get(): Promise<number>;
}

Deno.test(
  "ref acquire: a process actor's ref handed to ANOTHER process actor acquires over Mux",
  async () => {
    // Owner A and holder B are separate Deno processes (Mux transports). Main
    // gets a fresh ref from A, B holds it, B's first call triggers the acquire:
    // the refId-only hand-off must bootstrap a per-holder Mux channel on A's
    // transport and complete (no MessagePort, no hang).
    const ownerA = await spawnProcess<typeof RefProcessWorker.rpc>(
      "./test_fixtures/ref_process_worker.ts",
      { codecs: REF_CODEC },
    );
    const holderB = await spawnProcess<typeof RefProcessWorker.rpc>(
      "./test_fixtures/ref_process_worker.ts",
      { codecs: REF_CODEC },
    );
    try {
      const refFromA = await ownerA
        .getCounter() as unknown as import("./examples/remote_ref/ref_codec.ts").RemoteRef<
          CounterRef
        >;
      assertEquals(await refFromA.inc(), 1); // fresh token works across processes
      await holderB.holdRef(refFromA);
      // B's call is the refId-only acquire: routed by the main thread over
      // Mux channels, served by A, materialized in B.
      const outcome = await Promise.race([
        holderB.callHeld().then(
          (n) => `ok:${n}` as const,
          (e: unknown) => e,
        ),
        new Promise((r) => setTimeout(() => r("TIMEOUT"), 5_000)),
      ]);
      assertEquals(outcome, "ok:2");
      // The same shared counter: a second call reaches the same object in A.
      assertEquals(await holderB.callHeld(), 3);
      // A's own fresh ref still works after the hand-off.
      assertEquals(await refFromA.inc(), 4);
    } finally {
      await ownerA.dispose();
      await holderB.dispose();
    }
  },
);

Deno.test(
  "ref acquire: a process actor hands a ref to main (main-side Mux acquire)",
  async () => {
    // The owner is a process; the requester is the main thread. The acquire is
    // routed directly on main (open a Mux channel on the owner's transport and
    // serve it there), so the pending proxy must materialize with the local
    // channel end and complete without hanging.
    const ownerA = await spawnProcess<typeof RefProcessWorker.rpc>(
      "./test_fixtures/ref_process_worker.ts",
      { codecs: REF_CODEC },
    );
    try {
      const refFromA = await ownerA
        .getCounter() as unknown as import("./examples/remote_ref/ref_codec.ts").RemoteRef<
          CounterRef
        >;
      // Hand the proxy (refId-only) back into the owner — it restores to a
      // local call-through reference on A (no channel involved on A's side).
      const accept = await ownerA.holdRef(refFromA);
      assertEquals(accept, "held");
      // Main acquires: ownerA's Mux transport, main-side materialization.
      const outcome = await Promise.race([
        refFromA.inc().then(
          (n) => `ok:${n}` as const,
          (e: unknown) => e,
        ),
        new Promise((r) => setTimeout(() => r("TIMEOUT"), 5_000)),
      ]);
      assertEquals(outcome, "ok:1");
    } finally {
      await ownerA.dispose();
    }
  },
);
