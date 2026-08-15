/** Test-only worker: minimal RPC surface for exercising the codec mechanism. */
import { type Codec, serveWorker } from "../mod.ts";

// User-defined codec: takes over any object with an asyncIterator, wrapping it
// as a capsule instead of opening a channel. It registers before the built-in
// iterable codec, proving user codecs match first.
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
    return { __wCodec: "capsule", payload: v };
  },
  decode(placeholder: { payload: unknown }): object {
    lastCapsule = placeholder.payload;
    return placeholder.payload as object;
  },
};

// The decoded capsule lands here (single worker instance, module-level is safe);
// echoCapsule returns it to prove deep traversal went through the user codec
let lastCapsule: unknown;

export const rpc = {
  isRangeError(e: Error): boolean {
    return e instanceof RangeError;
  },
  echoError(e: Error): { name: string; message: string } {
    return { name: e.name, message: e.message };
  },
  echoErrorProp(e: Error, key: string): unknown {
    return (e as unknown as Record<string, unknown>)[key];
  },
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
  isAborted(signal: AbortSignal): boolean {
    return signal.aborted;
  },
  /** Event-driven: waits for the rebuilt signal to be aborted (status lands asynchronously on its own channel). */
  waitAborted(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(true);
    return new Promise((resolve) => {
      signal.addEventListener("abort", () => resolve(true), { once: true });
    });
  },
  echoCapsule(v: object): unknown {
    lastCapsule = v;
    return lastCapsule;
  },
};

serveWorker(rpc, { codecs: [capsuleCodec] });
