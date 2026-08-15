import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { RemoteError } from "./core/protocol.ts";
import { spawn } from "./spawn.ts";
import {
  type RemoteRef,
  remoteRefCodec,
} from "./examples/remote_ref/ref_codec.ts";
import type * as RefWorkerModule from "./examples/remote_ref/worker.ts";

const REF_WORKER_URL = import.meta.resolve("./examples/remote_ref/worker.ts");

const forceGc = (globalThis as { gc?: () => void }).gc;

async function yieldToEventLoop(times: number): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 1));
}

function spawnRefActor() {
  return spawn<typeof RefWorkerModule.rpc>(
    new Worker(REF_WORKER_URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
}

interface CounterRef {
  increment(): Promise<number>;
  get(): Promise<number>;
}

Deno.test("remote ref: method calls marshal to the owner side", async () => {
  const actor = await spawnRefActor();
  const ref = await actor.createCounter() as RemoteRef<CounterRef>;
  assertEquals(await ref.increment(), 1);
  assertEquals(await ref.increment(), 2);
  assertEquals(await ref.get(), 2);
  await ref.dispose();
  await actor.dispose();
});

Deno.test("remote ref: errors marshal back as RemoteError", async () => {
  const actor = await spawnRefActor();
  const ref = await actor.createCounter() as RemoteRef<{
    boom(): Promise<never>;
  }>;
  const outcome = await ref.boom().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, RemoteError);
  assertEquals(outcome.name, "RangeError");
  assert(outcome.message.includes("counter exploded"));
  await ref.dispose();
  await actor.dispose();
});

Deno.test("remote ref: nested AsyncIterable flows through the ref channel", async () => {
  const actor = await spawnRefActor();
  const ref = await actor.createCounter() as RemoteRef<{
    countdown(n: number): Promise<AsyncIterable<number>>;
  }>;
  const out: number[] = [];
  for await (const v of await ref.countdown(3)) out.push(v);
  assertEquals(out, [3, 2, 1, 0]);
  await ref.dispose();
  await actor.dispose();
});

Deno.test("remote ref: explicit dispose runs the owner's [Symbol.dispose]", async () => {
  const actor = await spawnRefActor();
  const ref = await actor.createCounter();
  await ref.dispose();
  const deadline = Date.now() + 2_000;
  let disposed = 0;
  while (Date.now() < deadline) {
    disposed = await actor.disposedCount();
    if (disposed > 0) break;
    await new Promise((r) => setTimeout(r, 10));
  }
  assertEquals(disposed, 1);
  await actor.dispose();
});

Deno.test("remote ref: calls after dispose reject", async () => {
  const actor = await spawnRefActor();
  const ref = await actor.createCounter() as RemoteRef<CounterRef>;
  await ref.dispose();
  const outcome = await ref.get().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assert(outcome instanceof Error);
  await actor.dispose();
});

Deno.test("remote ref: GC of the proxy releases the owner (best-effort)", async () => {
  const actor = await spawnRefActor();
  if (!forceGc) {
    console.log(
      "  (skipped forced collection: run with --v8-flags=--expose-gc)",
    );
    await actor.dispose();
    return;
  }
  assertEquals(await actor.disposedCount(), 0);
  // Create and drop the proxy inside a completed async IIFE: while the outer
  // test function runs, `await` keeps its promise (and the resolved proxy)
  // alive until the frame exits.
  await (async () => {
    const ref = await actor.createCounter();
    await (ref as RemoteRef<CounterRef>).increment();
    // abandon without dispose(): release must be driven purely by GC
  })();
  await yieldToEventLoop(4);
  forceGc();
  const deadline = Date.now() + 5_000;
  let disposed = 0;
  while (Date.now() < deadline) {
    disposed = await actor.disposedCount();
    if (disposed > 0) break;
    forceGc();
    await yieldToEventLoop(4);
  }
  assertEquals(disposed, 1);
  await actor.dispose();
});

// —— Identity & restore semantics ——

Deno.test("identity: same object refs reuse one proxy (refs are comparable)", async () => {
  const actor = await spawnRefActor();
  const r1 = await actor.sharedCounter();
  const r2 = await actor.sharedCounter();
  assertEquals(r1, r2); // same refId → same proxy on this side
  assertEquals(await (r1 as RemoteRef<CounterRef>).increment(), 1);
  assertEquals(await (r2 as RemoteRef<CounterRef>).get(), 1); // same underlying object
  await actor.dispose();
});

Deno.test("restore: reference back to owner collapses to a local direct call", async () => {
  const actor = await spawnRefActor();
  const r1 = await actor.sharedCounter() as RemoteRef<CounterRef>;
  assertEquals(await r1.increment(), 1); // shared counter in the worker → 1
  // Handing the ref back to its owner restores it (no proxy, no channel).
  assertEquals(await actor.acceptBack(r1), "local");
  // The restored ref calls the real object directly: a fresh ref round-trips
  // again and increments the same counter → 2.
  assertEquals(
    await actor.callBack(await actor.sharedCounter() as RemoteRef<CounterRef>),
    2,
  );
  // Restoring closed the owner-side channels for the shared counter.
  assertEquals(await actor.sharedOwnerChannels(), 0);
  await actor.dispose();
});

Deno.test("transfer: the original proxy dies after its port is handed off", async () => {
  const actor = await spawnRefActor();
  const r1 = await actor.sharedCounter() as RemoteRef<CounterRef>;
  await actor.acceptBack(r1); // r1's underlying port was transferred back
  const outcome = await r1.increment().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assert(outcome instanceof Error);
  await actor.dispose();
});
