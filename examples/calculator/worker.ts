/** Example worker: the exported rpc object is the Actor's API surface. */
import { serveWorker } from "../../worker_runtime.ts";
import type { RemoteCallback } from "../../core/codecs/callback.ts";

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function accumulate(items: AsyncIterable<number>): Promise<number> {
  let total = 0;
  for await (const v of items) total += v;
  return total;
}

/** Times the main thread cancelled a stream: proves the generator finally runs in the worker on early stop. */
let streamCancels = 0;
/** How many stream bodies started iterating (laziness probe). */
let streamStartCount = 0;

export const rpc = {
  /** Plain numeric RPC. */
  add(a: number, b: number): number {
    return a + b;
  },

  /** Structured clone: returns a deeply nested object (with Map/Set/Date/TypedArray). */
  report(user: { id: string; name: string }): object {
    return {
      generatedAt: new Date(),
      user,
      balances: new Map([
        ["wallet", 1_000],
        ["bank", 250.5],
      ]),
      tags: new Set(["premium", "beta"]),
      samples: new Uint8Array([1, 2, 3]),
      nested: { level1: { level2: { level3: { at: user.id } } } },
    };
  },

  /** Error propagation: exceptions thrown in the worker are rebuilt as RemoteError on the main thread. */
  divide(a: number, b: number): number {
    if (b === 0) throw new RangeError("division by zero");
    return a / b;
  },

  /** Delayed side effect: proves concurrent calls correlate to the right ids. */
  async delay(ms: number, tag: string): Promise<string> {
    await new Promise((r) => setTimeout(r, ms));
    return tag;
  },

  /** Consumes an AsyncIterable from the main thread (argument-side transport). */
  sumIterable(items: AsyncIterable<number>): Promise<number> {
    return accumulate(items);
  },

  /** AsyncIterable nested inside an object also transfers (deep conversion). */
  sumNested(payload: { items: AsyncIterable<number> }): Promise<number> {
    return accumulate(payload.items);
  },

  /** Returns an AsyncIterable (return-value-side transport): pulled on demand by the main thread. */
  stream(prefix: string, n: number): AsyncIterable<string> {
    return (async function* () {
      streamStartCount++; // body runs only once iteration starts (laziness probe)
      for (let i = 0; i < n; i++) {
        await sleep(5);
        yield `${prefix}-${i}`;
      }
    })();
  },

  /**
   * Explicitly Promise-wrapped stream: Remote<T> keeps the Promise (eager
   * intent spelled out by the writer), unlike the bare AsyncIterable above.
   */
  streamEager(n: number): Promise<AsyncIterable<number>> {
    return Promise.resolve((async function* () {
      for (let i = 0; i < n; i++) {
        await sleep(5);
        yield i;
      }
    })());
  },

  /** Laziness probe: how many stream bodies actually started iterating. */
  streamStarts(): number {
    return streamStartCount;
  },

  /** Stream whose creation throws: the error surfaces at first next() on the caller side. */
  failingCreationStream(): AsyncIterable<number> {
    throw new Error("stream creation failed");
  },

  /** Stream that throws mid-way: the error is rebuilt as RemoteError on the main thread. */
  failingStream(): AsyncIterable<number> {
    return (async function* () {
      yield 1;
      yield 2;
      throw new Error("stream exploded");
    })();
  },

  /** Infinite stream: when the main thread stops early, the generator finally runs (proven by the counter). */
  infiniteStream(): AsyncIterable<number> {
    return (async function* () {
      try {
        let i = 0;
        while (true) {
          await sleep(50);
          yield i++;
        }
      } finally {
        streamCancels++;
      }
    })();
  },

  getStreamCancelCount(): number {
    return streamCancels;
  },

  /**
   * Consumes a sync Iterable/stateful Iterator from the main thread (the
   * iterable codec wraps it into a channel; the receiver side rebuilds it as
   * AsyncIterable; for await accepts both sync and async).
   */
  sumSyncIterator(it: Iterable<number>): Promise<number> {
    return (async () => {
      let total = 0;
      for await (const v of it) total += v;
      return total;
    })();
  },

  /** Error payload snapshot: verifies custom subclass names survive via the error codec. */
  echoError(e: Error): { name: string; message: string } {
    return { name: e.name, message: e.message };
  },

  /** Built-in Error stays native: not corrupted by the error codec taking over. */
  isRangeError(e: Error): boolean {
    return e instanceof RangeError;
  },

  /** Custom error property readback: verifies the keepOwnProperties codec preserves it. */
  echoErrorProp(e: Error, key: string): unknown {
    return (e as unknown as Record<string, unknown>)[key];
  },

  /** AbortSignal propagation: waits for the main thread's abort. */
  onAbort(signal: AbortSignal): Promise<string> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve("already-aborted");
        return;
      }
      signal.addEventListener("abort", () => resolve("aborted"), {
        once: true,
      });
    });
  },

  /** AbortSignal state readback: verifies the already-aborted state survives the codec rebuild. */
  isAborted(signal: AbortSignal): boolean {
    return signal.aborted;
  },

  /** Registration-order probe: "iterable" or "plain", depending on which codec took over. */
  whatIsIt(v: { [Symbol.asyncIterator]?: unknown }): string {
    return typeof v[Symbol.asyncIterator] === "function" ? "iterable" : "plain";
  },

  /** Callback byref: worker calls the raw function passed from the main thread. */
  callCallback(cb: (x: number) => number): Promise<number> {
    // At runtime cb is a RemoteCallback (returns a Promise); Promise.resolve
    // flattens it. The worker writes it as an ordinary function — that is the
    // point of automatic byref.
    return Promise.resolve(cb(21));
  },

  /** Async callback: result is awaited across the channel. */
  callAsyncCallback(cb: (x: string) => Promise<string>): Promise<string> {
    return cb("hello");
  },

  /** Nested field: a function inside an object also travels byref automatically. */
  callNestedCallback(opts: { onDone: (v: number) => string }): Promise<string> {
    return Promise.resolve(opts.onDone(7));
  },

  /** Callback that throws: the error surfaces at the main-thread registration point. */
  callThrowingCallback(cb: (x: number) => number): Promise<unknown> {
    return Promise.resolve(cb(1)).catch((e) => e);
  },

  /** Re-encode probe: passing a callback reference back must fail loudly. */
  reencodeCallback(cb: (x: number) => number): string {
    return typeof cb;
  },

  /** Hold a callback reference persistently (so tests can dispose it). */
  holdCallback(cb: (x: number) => number): string {
    heldCallback = cb as unknown as RemoteCallback<[number], number>;
    return "held";
  },
  /** Call the held callback reference. */
  callHeld(x: number): Promise<number> {
    return Promise.resolve(heldCallback!(x));
  },
  /** Dispose the held callback reference. */
  disposeHeld(): string {
    heldCallback!.dispose();
    return "disposed";
  },
  /** Return the held callback reference (re-encoding a proxy must fail loudly). */
  returnHeld(): (x: number) => number {
    return heldCallback as unknown as (x: number) => number;
  },
};

let heldCallback: RemoteCallback<[number], number> | undefined;

serveWorker(rpc);
