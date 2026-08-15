/** Example worker: the exported rpc object is the Actor's API surface. */
import { serveWorker } from "../../worker_runtime.ts";

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
      for (let i = 0; i < n; i++) {
        await sleep(5);
        yield `${prefix}-${i}`;
      }
    })();
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
   * 消费主线程传来的同步 Iterable/状态化 Iterator（iterable codec 包装入通道，
   * 接收侧统一重建为 AsyncIterable；for await 同时兼容同步/异步）。
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
};

serveWorker(rpc);
