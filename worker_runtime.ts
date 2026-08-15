/**
 * Worker-side runtime: registers the module's top-level rpc object as an Actor,
 * handles request frames and replies with responses.
 *
 * Usage (call once at the worker module's top level):
 *   import { serveWorker } from "./worker_runtime.ts";
 *   export const rpc = { async add(a: number, b: number) { return a + b; } };
 *   serveWorker(rpc);
 *
 * Conventions:
 *   - The module has exactly one RPC entry object (name is free, but the main
 *     thread's `typeof WorkerModule.rpc` must refer to it).
 *   - Methods and return values must be structured-cloneable (plain data +
 *     Map/Set/Date/TypedArray, ...); functions/class instances/prototype chains
 *     can't cross threads — Remote<T> enforces at compile time that only
 *     functions are exposed.
 *   - Exceptions inside the worker are serialized back and rebuilt as RemoteError.
 */

import { Frame, PROTOCOL_VERSION, serializeError } from "./core/protocol.ts";
import { type Codec, PayloadCodecRegistry } from "./core/codec.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";

// Deno types `self` as Window by default; in a worker script it is actually a
// DedicatedWorkerGlobalScope. Only the members used are declared here, so we
// don't depend on lib.webworker.
declare const self: {
  postMessage(message: unknown, options?: { transfer?: Transferable[] }): void;
  onmessage: ((ev: MessageEvent<Frame>) => void) | null;
  close(): void;
};

export interface WorkerApi {
  // The RPC boundary accepts any signature; concrete worker method types are
  // derived and checked by Remote<T> on the main thread.
  // deno-lint-ignore no-explicit-any
  [method: string]: (...args: any[]) => any;
}

export interface ServeWorkerOptions {
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal. The tag list must match
   * spawn()'s; the handshake frame carries and validates it.
   */
  codecs?: Codec<unknown>[];
}

export function serveWorker(
  api: WorkerApi,
  options: ServeWorkerOptions = {},
): void {
  const registry = new PayloadCodecRegistry();
  // User codecs register first (can override a built-in of the same tag); built-ins fill in after.
  for (const codec of options.codecs ?? []) registry.register(codec);
  for (const codec of [iterableCodec, errorCodec, abortSignalCodec]) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  const post = (frame: Frame) => self.postMessage(frame);

  self.onmessage = async (ev: MessageEvent<Frame>) => {
    const frame = ev.data;
    if (frame.type === "request") {
      const fn = api[frame.method];
      if (typeof fn !== "function") {
        post({
          type: "response",
          id: frame.id,
          ok: false,
          error: serializeError(
            new Error(`No such RPC method: "${frame.method}"`),
          ),
        });
        return;
      }
      try {
        const args = registry.decode(frame.args) as unknown[];
        const value = await fn(...args);
        const transfer: Transferable[] = [];
        self.postMessage(
          {
            type: "response",
            id: frame.id,
            ok: true,
            value: registry.encode(value, transfer),
          },
          { transfer },
        );
      } catch (e) {
        // Errors must be returned, not thrown: onmessage is an async callback
        // where an exception would be silently lost.
        post({
          type: "response",
          id: frame.id,
          ok: false,
          error: serializeError(e),
        });
      }
      return;
    }
    if (frame.type === "dispose") {
      registry.failAll();
      self.close();
    }
  };

  // Module loaded, API ready — spawn() wakes up from pending on this frame.
  post({ type: "handshake", version: PROTOCOL_VERSION, codecs: registry.tags });
}
