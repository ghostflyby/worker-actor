import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { spawn } from "./spawn.ts";
import { RemoteError } from "./core/protocol.ts";
import type * as WorkerModule from "./examples/calculator/worker.ts";

const WORKER_URL = import.meta.resolve("./examples/calculator/worker.ts");

function makeActor() {
  return spawn<typeof WorkerModule.rpc>(
    new Worker(WORKER_URL, { type: "module" }),
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

const forceGc = (globalThis as { gc?: () => void }).gc;

Deno.test("callback: raw function travels byref and runs at its registration point", async () => {
  const actor = await makeActor();
  // The closure lives on the main thread; the worker calls it through the
  // reference and the result comes back — the function ran here, not in worker.
  let calls = 0;
  const result = await actor.callCallback((x: number) => {
    calls++;
    return x * 2;
  });
  assertEquals(result, 42);
  assertEquals(calls, 1); // executed in the main-thread context
  await actor.dispose();
});

Deno.test("callback: async callback result is awaited across the channel", async () => {
  const actor = await makeActor();
  const result = await actor.callAsyncCallback(async (s: string) => {
    await sleep(5);
    return `${s} world`;
  });
  assertEquals(result, "hello world");
  await actor.dispose();
});

Deno.test("callback: nested function field travels byref automatically", async () => {
  const actor = await makeActor();
  const result = await actor.callNestedCallback({
    onDone: (v: number) => `done:${v}`,
  });
  assertEquals(result, "done:7");
  await actor.dispose();
});

Deno.test("callback: thrown error surfaces as RemoteError at the caller", async () => {
  const actor = await makeActor();
  const outcome = await actor.callThrowingCallback((x: number) => {
    throw new RangeError(`callback boom ${x}`);
  });
  // worker's callThrowingCallback catches and returns the error value
  assertInstanceOf(outcome, RemoteError);
  assertEquals(outcome.name, "RangeError");
  assert(outcome.message.includes("callback boom"));
  await actor.dispose();
});

Deno.test("callback: re-encoding a callback reference fails loudly", async () => {
  const actor = await makeActor();
  // Hold a reference on the worker side: the worker now holds a callback proxy.
  await actor.holdCallback((x: number) => x + 1);
  // The worker returns the held proxy back to the main thread: encoding a
  // callback reference (not a fresh function) must be refused loudly.
  const outcome = await actor.returnHeld().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, Error);
  assert((outcome as Error).message.includes("cannot be re-encoded"));
  await actor.dispose();
});

Deno.test("callback: dispose() makes further calls reject", async () => {
  const actor = await makeActor();
  let mainCalls = 0;
  const cb = (x: number) => {
    mainCalls++;
    return x + 1;
  };
  await actor.holdCallback(cb);
  assertEquals(await actor.callHeld(1), 2);
  assertEquals(mainCalls, 1);
  // dispose the held reference on the worker side: further calls reject.
  await actor.disposeHeld();
  const outcome = await actor.callHeld(1).then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, Error);
  assert((outcome as Error).message.includes("disposed"));
  assertEquals(mainCalls, 1); // the callback never ran again
  await actor.dispose();
});

Deno.test("callback: GC of the reference releases the owner side (best-effort)", async () => {
  const actor = await makeActor();
  if (!forceGc) {
    console.log(
      "  (skipped forced collection: run with --v8-flags=--expose-gc)",
    );
    await actor.dispose();
    return;
  }
  // The worker stores callback proxies; an owner-side dispose counter would
  // need a [Symbol.dispose] hook on the function — plain functions don't have
  // one. Instead we verify GC does not crash and the actor stays functional:
  // the callback channel is closed by the finalizer without errors.
  await (async () => {
    await actor.callCallback((x: number) => x + 10);
  })();
  await sleep(10);
  forceGc();
  // The actor must still be fully functional after GC cycles.
  assertEquals(await actor.callCallback((x: number) => x + 20), 41);
  await actor.dispose();
});
