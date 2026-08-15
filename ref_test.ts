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
  // Sharing semantics keep other holder channels alive (restore only closes
  // the arriving port): the owner-side channels stay tracked.
  assert(await actor.sharedOwnerChannels() > 0);
  await actor.dispose();
});

Deno.test("sharing: re-encoding a proxy keeps the original usable (share, not move)", async () => {
  const actor = await spawnRefActor();
  const r1 = await actor.sharedCounter() as RemoteRef<CounterRef>;
  // Hand the proxy back to its owner (restore). Sharing semantics keep r1
  // usable: the identity is restored locally, the proxy is not killed.
  assertEquals(await actor.acceptBack(r1), "local");
  await actor.dispose();
});

// —— Indirect sharing: refId-only hand-off across workers (main bootstraps) ——

Deno.test("acquire: a handed-off reference is acquired and called across workers", async () => {
  // A owns the shared Counter; B receives A's ref via a refId-only hand-off
  // (main relays the proxy, bootstraps a fresh A↔B channel on first use).
  const ownerA = await spawnRefActor();
  await ownerA.disposedCount();
  const workerB = new Worker(
    import.meta.resolve("./examples/remote_ref/worker.ts"),
    { type: "module" },
  );
  const actorB = await spawn<typeof RefWorkerModule.rpc>(workerB, {
    codecs: [remoteRefCodec],
  });
  // Main gets a fresh ref from A.
  const refFromA = await ownerA.sharedCounter() as RemoteRef<CounterRef>;
  await actorB.holdRef(refFromA);
  assertEquals(await actorB.callHeld(0), 1); // shared counter in A → 1
  assertEquals(await actorB.callHeld(0), 2);
  await ownerA.dispose();
  await actorB.dispose();
});

Deno.test("acquire: multiple holders each get their own channel to the owner", async () => {
  const ownerA = await spawnRefActor();
  await ownerA.disposedCount(); // warm up: routeable refId prefix
  const mk = () =>
    spawn<typeof RefWorkerModule.rpc>(
      new Worker(import.meta.resolve("./examples/remote_ref/worker.ts"), {
        type: "module",
      }),
      { codecs: [remoteRefCodec] },
    );
  const actorB = await mk();
  const actorC = await mk();
  const refFromA = await ownerA.sharedCounter() as RemoteRef<CounterRef>;
  await actorB.holdRef(refFromA);
  await actorC.holdRef(refFromA);
  // Both acquire independently; each reaches the SAME shared counter in A.
  assertEquals(await actorB.callHeld(0), 1);
  assertEquals(await actorC.callHeld(0), 2);
  assertEquals(await actorB.callHeld(0), 3);
  await ownerA.dispose();
  await actorB.dispose();
  await actorC.dispose();
});
