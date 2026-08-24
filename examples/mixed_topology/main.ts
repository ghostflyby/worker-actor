// Mixed topology: a Web Worker (messageport transport) and a process actor
// (Mux transport) interoperate — a reference owned by the Worker is called
// from the process actor across the mixed transports.
//
// Run: deno run --allow-read --allow-run --allow-env examples/mixed_topology/main.ts
import type * as RefWorkerModule from "../remote_ref/worker.ts";
import type * as HolderModule from "../cross_process_ref/worker.ts";
import { spawn, spawnProcess } from "@ghostflyby/worker-actor";

const remoteRefCodec = (await import("../remote_ref/ref_codec.ts"))
  .remoteRefCodec;
const CODECS = [remoteRefCodec];

interface CounterRef {
  increment(): Promise<number>;
  get(): Promise<number>;
}

// Owner: a Web Worker serving remote-ref (messageport transport).
const ownerWorker = await spawn<typeof RefWorkerModule.rpc>(
  new Worker(import.meta.resolve("../remote_ref/worker.ts"), {
    type: "module",
  }),
  { codecs: CODECS },
);

// Holder: a process actor (Mux transport) that can hold and call references.
const holderProc = await spawnProcess<typeof HolderModule.rpc>(
  "./examples/cross_process_ref/worker.ts",
  { codecs: CODECS },
);

try {
  // Fresh reference from the Worker: direct messageport transfer to main.
  const ref = await ownerWorker.createCounter() as unknown as CounterRef;
  console.log("ownerWorker.increment() =", await ref.increment()); // 1

  // Hand the Worker-owned ref to the process actor. Its first call is a
  // refId-only acquire: main routes owner (messageport Worker) ↔ requester
  // (Mux process) — the acquire completes across the mixed transports.
  await holderProc.holdRef(ref);
  console.log(
    "holderProc.callHeld()   =",
    await holderProc.callHeldIncrement(),
  ); // 2
  console.log(
    "holderProc.callHeld()   =",
    await holderProc.callHeldIncrement(),
  ); // 3

  // The same shared object in the Worker, still reachable from main.
  console.log("ownerWorker.increment() =", await ref.increment()); // 4
} finally {
  await holderProc.dispose();
  await ownerWorker.dispose();
}
