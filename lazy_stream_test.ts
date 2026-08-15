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

async function waitFor(
  probe: () => Promise<boolean>,
  timeoutMs = 2_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return true;
    await sleep(10);
  }
  return false;
}

Deno.test("lazy stream: bare AsyncIterable method is not Promise-wrapped", async () => {
  const actor = await makeActor();
  // Compile-time proof of the Remote<T> special case: `stream` is typed
  // AsyncIterable<string>, not Promise<AsyncIterable<string>> — for-await works
  // directly on the call result.
  const s: AsyncIterable<string> = actor.stream("x", 3);
  // Element-level laziness: the worker generator body has not run yet.
  assertEquals(await actor.streamStarts(), 0);
  const out: string[] = [];
  for await (const v of s) out.push(v);
  assertEquals(out, ["x-0", "x-1", "x-2"]);
  // Only now did the body run (once).
  assertEquals(await actor.streamStarts(), 1);
  await actor.dispose();
});

Deno.test("lazy stream: explicit Promise<AsyncIterable> keeps its eager Promise", async () => {
  const actor = await makeActor();
  // Compile-time proof: `streamEager` stays Promise<AsyncIterable<number>>.
  const p: Promise<AsyncIterable<number>> = actor.streamEager(3);
  const stream = await p;
  const out: number[] = [];
  for await (const v of stream) out.push(v);
  assertEquals(out, [0, 1, 2]);
  await actor.dispose();
});

Deno.test("lazy stream: creation errors surface at first next() as RemoteError", async () => {
  const actor = await makeActor();
  const out: number[] = [];
  const outcome = await (async () => {
    try {
      for await (const v of actor.failingCreationStream()) out.push(v);
      return "resolved" as const;
    } catch (e) {
      return e as unknown;
    }
  })();
  assertEquals(out, []);
  assertInstanceOf(outcome, RemoteError);
  assert((outcome as RemoteError).message.includes("stream creation failed"));
  await actor.dispose();
});

Deno.test("lazy stream: return() after starting forwards cancellation", async () => {
  const actor = await makeActor();
  const it = actor.infiniteStream()[Symbol.asyncIterator]();
  const first = await it.next();
  assertEquals(first.done, false);
  await it.return?.();
  const cancelled = await waitFor(async () =>
    (await actor.getStreamCancelCount()) > 0
  );
  assert(cancelled, "the worker generator finally should run after return()");
  await actor.dispose();
});

Deno.test("lazy stream: return() before starting abandons cleanly (nothing leaks)", async () => {
  const actor = await makeActor();
  const it = actor.infiniteStream()[Symbol.asyncIterator]();
  await it.return?.(); // never started: no cancel frame needed, no worker side effect
  assertEquals(await actor.getStreamCancelCount(), 0);
  assertEquals(await actor.streamStarts(), 0);
  await actor.dispose();
});
