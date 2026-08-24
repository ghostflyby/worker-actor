// Cross-process references: two separate processes share an object reference.
// Owner A produces a remoteRef(); holder B holds it and calls it — the first
// call from B triggers a reference-acquire that the main thread routes over
// the processes' fork IPC transports, after which B talks to A's object
// directly (no main-thread relay).
//
// Run: deno run --allow-read --allow-run --allow-env examples/cross_process_ref/main.ts
import type * as OwnerModule from "./worker.ts";
import { spawnProcess } from "@ghostflyby/worker-actor";

const remoteRefCodec = (await import("../remote_ref/ref_codec.ts"))
  .remoteRefCodec;
const CODECS = [remoteRefCodec];

// The counter's remote surface (RemoteRef<T> projects methods to Promises).
interface CounterRef {
  inc(): Promise<number>;
  get(): Promise<number>;
}

// Owner A: a separate Deno process that owns the shared counter object.
const ownerA = await spawnProcess<typeof OwnerModule.rpc>(
  "./examples/cross_process_ref/worker.ts",
  { codecs: CODECS, permissions: { read: true } },
);

// Holder B: a separate Deno process that can hold and call references.
const holderB = await spawnProcess<typeof OwnerModule.rpc>(
  "./examples/cross_process_ref/worker.ts",
  { codecs: CODECS },
);

try {
  // 1. Get a fresh reference to A's counter (works across processes).
  const counter = await ownerA.getCounter() as unknown as CounterRef;
  console.log("A.inc()            =", await counter.inc()); // 1

  // 2. Hand the reference to B (refId-only hand-off: identity travels).
  await holderB.holdRef(counter);
  console.log("held by B");

  // 3. B's first call triggers the acquire: main routes owner A ↔ holder B,
  //    then B calls A's object directly.
  console.log("B.callHeld()       =", await holderB.callHeld()); // 2
  console.log("B.callHeld() again =", await holderB.callHeld()); // 3

  // 4. A's own reference still works after the hand-off — same shared object.
  console.log("A.inc() again      =", await counter.inc()); // 4
  console.log("A.get()            =", await counter.get()); // 4
} finally {
  await holderB.dispose();
  await ownerA.dispose();
}
