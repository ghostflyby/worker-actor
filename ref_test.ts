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

async function waitFor2(
  probe: () => boolean | Promise<boolean>,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await new Promise((r) => setTimeout(r, 10));
  }
  throw new Error("waitFor2 timed out");
}

async function yieldToEventLoop(times: number): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 1));
}

function spawnRefActor() {
  return spawn<typeof RefWorkerModule.rpc>(
    new Worker(REF_WORKER_URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
}

/** A second actor (used as a reference holder). */
function spawnHolder() {
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

Deno.test("acquire: multi-hop sharing A→main→B→C all reach the same owner object", async () => {
  // A owns the shared Counter. The ref travels: A → main (direct) → B
  // (refId token, acquire) → C (B's proxy re-encoded as a refId token,
  // acquire). Every hop shares the identity; every holder ends with its own
  // channel to A, and all calls reach the SAME object in A.
  const ownerA = await spawnRefActor();
  await ownerA.disposedCount(); // warm up: routeable refId prefix
  const refFromA = await ownerA.sharedCounter() as RemoteRef<CounterRef>;

  // main → B
  const b = await spawn<typeof RefWorkerModule.rpc>(
    new Worker(REF_WORKER_URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
  await b.holdRef(refFromA);
  assertEquals(await b.callHeld(0), 1); // B acquired → reaches A's object

  // B → C: B returns its held proxy; the main thread re-encodes it as a
  // refId token (sharing) and hands it to C.
  const refViaB = await b.getHeldRef();
  const c = await spawn<typeof RefWorkerModule.rpc>(
    new Worker(REF_WORKER_URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
  await c.holdRef(refViaB);
  assertEquals(await c.callHeld(0), 2); // C acquired → same object in A
  // B's own channel still works after the hand-off (share, not move).
  assertEquals(await b.callHeld(0), 3);
  // C again → 4: all three holders share one counter in A.
  assertEquals(await c.callHeld(0), 4);

  await ownerA.dispose();
  await b.dispose();
  await c.dispose();
});

Deno.test("release: mode-2 object's strong hold drops once the holder releases it", async () => {
  const actor = await spawnRefActor();
  const base = await actor.strongRefCount(); // 0 normally
  // Acquire an ephemeral object (only reachable through its reference), then
  // drop the reference on this side. The owner's conditional strong hold must
  // disappear (release probe), making the object collectable — deterministic,
  // unlike relying on finalizer timing.
  await (async () => {
    const ref = await actor.createEphemeral();
    await (ref as RemoteRef<CounterRef>).increment();
    assertEquals(await actor.strongRefCount(), base + 1); // held while in use
    // abandon: the async IIFE frame releases the reference
  })();
  await waitFor2(async () => (await actor.strongRefCount()) === base);
  assertEquals(await actor.strongRefCount(), base);
  if (forceGc) {
    // GC is a bonus: the object should eventually be collected.
    forceGc();
    await yieldToEventLoop(4);
  }
  await actor.dispose();
});

Deno.test("release: owner releaseRef broadcasts — peers die immediately", async () => {
  const actor = await spawnRefActor();
  await actor.disposedCount(); // warm up: routeable refId prefix
  const ref = await actor.registerNamed("svc") as RemoteRef<CounterRef>;
  // A second holder acquires the same object.
  const b = await spawn<typeof RefWorkerModule.rpc>(
    new Worker(REF_WORKER_URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
  await b.holdRef(ref);
  assertEquals(await ref.increment(), 1);
  assertEquals(await b.callHeld(0), 2);
  // Owner releases the object: both holders' proxies die immediately.
  assertEquals(await actor.releaseRefByName("svc"), "released");
  const outcome1 = await ref.increment().then(
    () => "resolved" as const,
    (e: unknown) => e,
  );
  assert(outcome1 instanceof Error); // released → proxy dies → call rejects
  const outcome2 = await Promise.race([
    b.callHeld(0).then(
      () => "resolved" as const,
      (e: unknown) => e,
    ),
    new Promise((r) => setTimeout(() => r("TIMEOUT"), 1500)),
  ]);
  assert(outcome2 instanceof Error);
  await actor.dispose();
  await b.dispose();
});

Deno.test("liveness: a terminated holder's refs are released (owner-pull detection)", async () => {
  // Deterministic dead-holder simulation: the owner PULLS heartbeats over one
  // channel per worker pair, so terminating the holder worker (no dispose frame)
  // stops the pongs and the owner releases its refs after the timeout.
  const owner = await spawnRefActor();
  await owner.disposedCount(); // warm up: routeable refId prefix
  await owner.setLiveness(50, 250); // fast params for the test
  const ref = await owner.registerNamed("zombie") as RemoteRef<CounterRef>;

  const holder = await spawnHolder();
  await holder.holdRef(ref);
  assertEquals(await holder.callHeld(0), 1); // holder acquired (pair created)
  // Two owner-side channels serve the object: the main thread's and the holder's.
  assertEquals(await owner.holderChannelsFor("zombie"), 2);

  // The holder worker is terminated hard: no dispose frame, pongs stop, and
  // the owner's liveness sweep releases its ref channel after the timeout.
  await holder.dispose();

  // The holder's channel must be released by the liveness sweep; the main
  // thread's own channel (this test still holds `ref`) stays open.
  await waitFor2(async () => (await owner.holderChannelsFor("zombie")) === 1);
  await owner.dispose();
});

Deno.test("liveness: killing one holder leaves other holders of the same ref alive", async () => {
  const owner = await spawnRefActor();
  await owner.disposedCount();
  await owner.setLiveness(50, 250);
  const ref = await owner.registerNamed("multi") as RemoteRef<CounterRef>;

  const b1 = await spawnHolder();
  const b2 = await spawnHolder();
  await b1.holdRef(ref);
  await b2.holdRef(ref);
  assertEquals(await b1.callHeld(0), 1); // b1 acquired (pair with owner)
  assertEquals(await b2.callHeld(0), 2); // b2 acquired (pair with owner)
  await b1.dispose(); // only b1 dies

  // b2 stays usable after b1's death: per-holder cleanup, not a broadcast.
  await waitFor2(async () => {
    return await b2.callHeld(0).then(
      () => true,
      () => false,
    );
  });
  assertEquals(await owner.strongRefCount(), 1); // the shared object is still held by b2
  await owner.dispose();
  await b2.dispose();
});

Deno.test("liveness: reverse — a dead owner fails its holders' refs (no hanging calls)", async () => {
  // The worker-level monitor is bidirectional: the holder detects the owner's
  // death (its pings stop) and fails its refs instead of hanging. Both sides
  // run their own liveness timers, so each needs the fast params.
  const owner = await spawnRefActor();
  await owner.disposedCount();
  await owner.setLiveness(50, 250);
  const ref = await owner.registerNamed("owner-dead") as RemoteRef<CounterRef>;

  const holder = await spawnHolder();
  await holder.setLiveness(50, 250); // holder-side detection cadence
  await holder.holdRef(ref);
  assertEquals(await holder.callHeld(0), 1); // pair established

  // The owner worker dies; the holder must notice via the missing pings and
  // reject subsequent calls (no calls hanging forever).
  await owner.dispose();

  const outcome = await Promise.race([
    holder.callHeld(0).then(
      () => "resolved" as const,
      (e: unknown) => e,
    ),
    new Promise((r) => setTimeout(() => r("TIMEOUT"), 3_000)),
  ]);
  assert(outcome instanceof Error);
  await holder.dispose();
});

Deno.test("liveness: the owner posts one death notice to main per dead holder", async () => {
  // Worker-level monitoring, batch-release semantics: the owner releases a dead
  // holder's refs together and posts exactly ONE __holder-dead notice to the
  // main thread. The notice rides on the owner's main channel (a worker-level
  // monitor message, not a per-reference one).
  const ownerWorker = new Worker(REF_WORKER_URL, { type: "module" });
  const owner = await spawn<typeof RefWorkerModule.rpc>(ownerWorker, {
    codecs: [remoteRefCodec],
  });
  await owner.disposedCount();
  await owner.setLiveness(50, 250);
  const ref = await owner.registerNamed("notify") as RemoteRef<CounterRef>;

  const holder = await spawnHolder();
  await holder.holdRef(ref);
  await holder.callHeld(0); // pair established

  let notices = 0;
  const listener = (ev: MessageEvent) => {
    const frame = ev.data as { type?: string };
    if (frame.type === "__holder-dead") notices++;
  };
  ownerWorker.addEventListener("message", listener);
  await holder.dispose(); // the holder worker dies; owner notices via liveness
  await waitFor2(() => notices === 1);
  ownerWorker.removeEventListener("message", listener);
  assertEquals(notices, 1);
  await owner.dispose();
});
