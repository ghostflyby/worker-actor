import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertThrows,
} from "@std/assert";
import { RemoteError } from "./core/protocol.ts";
import {
  type Codec,
  CODEC_PLACEHOLDER_KEY,
  PayloadCodecRegistry,
} from "./core/codec.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { createErrorCodec, errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";
import { spawn } from "./spawn.ts";
import type * as FixtureModule from "./test_fixtures/codec_worker.ts";
import type * as WorkerModule from "./examples/calculator/worker.ts";

const FIXTURE_URL = import.meta.resolve("./test_fixtures/codec_worker.ts");
const WORKER_URL = import.meta.resolve("./examples/calculator/worker.ts");

/** Main-thread capsule codec with the same tag/logic as the fixture's (no shared code across processes; the tag must agree). */
const capsuleCodec: Codec<object> = {
  tag: "capsule",
  matches(v: unknown): v is object {
    return typeof v === "object" && v !== null &&
      typeof (v as { [Symbol.asyncIterator]?: unknown })[
          Symbol.asyncIterator
        ] ===
        "function";
  },
  encode(v: object): unknown {
    return { [CODEC_PLACEHOLDER_KEY]: "capsule", payload: v };
  },
  decode(placeholder: { payload: unknown }): object {
    return placeholder.payload as object;
  },
};

function spawnCapsule(codecs: Codec<unknown>[] = [capsuleCodec]) {
  return spawn<typeof FixtureModule.rpc>(
    new Worker(FIXTURE_URL, { type: "module" }),
    { codecs },
  );
}

function spawnCalculator() {
  return spawn<typeof WorkerModule.rpc>(
    new Worker(WORKER_URL, { type: "module" }),
  );
}

class BizError extends Error {
  constructor() {
    super("biz failed");
    this.name = "BizError";
  }
}

Deno.test("codec: stateful sync iterator is transferred through iterable codec", async () => {
  const actor = await spawnCalculator();
  // Stateful iterator: [Symbol.iterator] returns this
  const counter = {
    i: 0,
    [Symbol.iterator]() {
      return this;
    },
    next() {
      if (this.i < 3) return { done: false, value: this.i++ };
      return { done: true, value: undefined };
    },
  };
  assertEquals(await actor.sumSyncIterator(counter as Iterable<number>), 3);
  await actor.dispose();
});

Deno.test("codec: custom codec takes precedence over built-in (capsule)", async () => {
  const actor = await spawnCapsule();
  // An object with asyncIterator would normally go through the iterable channel;
  // since capsule registers first, it takes over.
  const v = { [Symbol.asyncIterator]: async function* () {}, tag: "x" };
  const result = await actor.echoCapsule(v);
  // The worker received a plain object (no asyncIterator), proving the iterable channel was bypassed
  assert(!(Symbol.asyncIterator in (result as object)));
  assertEquals((result as { tag: string }).tag, "x");
  await actor.dispose();
});

Deno.test("codec: unknown placeholder tag fails loudly on decode", () => {
  const registry = new PayloadCodecRegistry();
  registry.register(iterableCodec).register(errorCodec).register(
    abortSignalCodec,
  );
  assertThrows(
    () => registry.decode({ [CODEC_PLACEHOLDER_KEY]: "no-such-tag" }),
    Error,
    "no-such-tag",
  );
});

Deno.test("codec: handshake rejects when codec lists mismatch", async () => {
  // The worker registered capsule but the main thread did not → handshake validation fails
  const outcome = await spawn<typeof FixtureModule.rpc>(
    new Worker(FIXTURE_URL, { type: "module" }),
    { codecs: [] },
  ).then(() => "unexpectedly spawned" as const, (e: unknown) => e);
  assertInstanceOf(outcome, Error);
  assert((outcome as Error).message.includes("capsule"));
});

Deno.test("codec: built-in Error stays native, custom subclass keeps name", async () => {
  const actor = await spawnCapsule();
  assertEquals(await actor.isRangeError(new RangeError("x")), true);
  assertEquals(await actor.echoError(new BizError()), {
    name: "BizError",
    message: "biz failed",
  });
  await actor.dispose();
});

Deno.test("codec: keepOwnProperties preserves custom error props", async () => {
  const actor = await spawnCapsule([
    capsuleCodec,
    createErrorCodec({ keepOwnProperties: true }),
  ]);
  const e = new BizError();
  (e as unknown as { code: number }).code = 42;
  assertEquals(await actor.echoErrorProp(e, "code"), 42);
  await actor.dispose();
});

Deno.test("codec: default error codec drops custom props", async () => {
  const actor = await spawnCapsule();
  const e = new BizError();
  (e as unknown as { code: number }).code = 42;
  assertEquals(await actor.echoErrorProp(e, "code"), undefined);
  await actor.dispose();
});

Deno.test("codec: AbortSignal is rebuilt and abort propagates", async () => {
  const actor = await spawnCapsule();
  const controller = new AbortController();
  const p = actor.onAbort(controller.signal);
  await new Promise((r) => setTimeout(r, 20)); // let the status frame land and the listener attach
  controller.abort("cancel-me");
  const outcome = await Promise.race([
    p,
    new Promise<string>((r) => setTimeout(() => r("timeout"), 2_000)),
  ]);
  assertEquals(outcome, "aborted");
  await actor.dispose();
});

Deno.test("codec: already-aborted signal keeps state through rebuild", async () => {
  const actor = await spawnCapsule();
  const controller = new AbortController();
  controller.abort("already");
  // An already-aborted source signal must rebuild as aborted in the worker.
  // The state lands asynchronously on its own channel, so assert via the abort
  // event (more reliable than reading aborted synchronously).
  const deadline = Date.now() + 2_000;
  let outcome = false;
  while (Date.now() < deadline) {
    outcome = await actor.waitAborted(controller.signal);
    if (outcome) break;
    await new Promise((r) => setTimeout(r, 10));
  }
  assertEquals(outcome, true);
  await actor.dispose();
});

Deno.test("codec: disposing one actor does not kill another actor's streams", async () => {
  const a1 = await spawnCalculator();
  const a2 = await spawnCalculator();
  const inf1 = await a1.infiniteStream();
  const it1 = inf1[Symbol.asyncIterator]();
  const first1 = await it1.next(); // a1 的流开始产出
  assertEquals(first1.done, false);
  await a1.dispose(); // closing a1 must not affect a2

  const stream2 = await a2.stream("keep", 3);
  const out: string[] = [];
  for await (const s of stream2) out.push(s);
  assertEquals(out, ["keep-0", "keep-1", "keep-2"]);
  await a2.dispose();
});

Deno.test("codec: stream error still surfaces as RemoteError", async () => {
  const actor = await spawnCalculator();
  const out: number[] = [];
  const outcome = await (async () => {
    try {
      for await (const v of await actor.failingStream()) out.push(v);
      return "resolved" as const;
    } catch (e) {
      return e as unknown;
    }
  })();
  assertEquals(out, [1, 2]);
  assertInstanceOf(outcome, RemoteError);
  assert((outcome as RemoteError).message.includes("stream exploded"));
  await actor.dispose();
});

// —— GC-based release (FinalizationRegistry) ——

// gc() is only available under `deno test --v8-flags=--expose-gc`; without it the
// GC-based tests skip the parts that need a forced collection (finalizers are
// best-effort by spec, so the deterministic paths are covered by the tests above).
const forceGc = (globalThis as { gc?: () => void }).gc;

async function yieldToEventLoop(times: number): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 1));
}

Deno.test("gc: abandoned remote iterable releases the producer on the other side", async () => {
  const actor = await spawnCalculator();
  // The worker keeps a counter of generator-finally runs; a release triggered by
  // the main side GC must reach the worker and run the producer's finally.
  assertEquals(await actor.getStreamCancelCount(), 0);
  // Get, start, then abandon the stream inside a completed async IIFE: while the
  // outer test function is still running, `await` keeps its promise (and the
  // resolved iterable) alive until the frame exits — a bare block isn't enough.
  await (async () => {
    const stream = await actor.infiniteStream();
    const it = stream[Symbol.asyncIterator]();
    // Start the generator so it suspends on its first yield: an async generator
    // never entered via next() skips its finally when return() is called later.
    const first = await it.next();
    assertEquals(first.done, false);
  })(); // IIFE completed: its frame and all resolved values are collectable
  if (forceGc) {
    // give the finalizer a chance to fire, then force a collection
    await yieldToEventLoop(4);
    forceGc();
    const deadline = Date.now() + 5_000;
    let cancels = 0;
    while (Date.now() < deadline) {
      cancels = await actor.getStreamCancelCount();
      if (cancels > 0) break;
      forceGc();
      await yieldToEventLoop(4);
    }
    // under --expose-gc the chain (finalizer → release → producer stop →
    // generator finally) must actually fire; best-effort only applies without gc()
    assertEquals(cancels, 1);
  } else {
    // no forced GC available: the mechanism can't be exercised, so just verify
    // the stream object itself is dropable without side effects
    console.log(
      "  (skipped forced collection: run with --v8-flags=--expose-gc)",
    );
  }
  await actor.dispose();
});

Deno.test("gc: explicit return() keeps the release single (no double notify)", async () => {
  const actor = await spawnCalculator();
  const stream = await actor.infiniteStream();
  const it = stream[Symbol.asyncIterator]();
  await it.next();
  await it.return?.(); // explicit abandon: sends release and unregisters the finalizer
  const deadline = Date.now() + 2_000;
  let cancels = 0;
  while (Date.now() < deadline) {
    cancels = await actor.getStreamCancelCount();
    if (cancels >= 1) break;
    await new Promise((r) => setTimeout(r, 10));
  }
  assertEquals(cancels, 1);
  if (forceGc) {
    forceGc();
    await yieldToEventLoop(4);
  }
  // even after collection, the release must not fire twice
  assertEquals(await actor.getStreamCancelCount(), 1);
  await actor.dispose();
});
