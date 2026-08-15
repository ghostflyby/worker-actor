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
import { type Channel, connectChannel } from "./core/channel.ts";
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

/** A direct, bidirectional link between this worker and a peer worker. */
export interface LinkHandle {
  /** Label of the link (shared by both endpoints). */
  label: string;
  /**
   * Send any codec value to the peer: references, streams, AbortSignals and
   * plain structured-cloneable values all work. Encoding runs through this
   * worker's registry, so the peer must register a compatible codec set.
   */
  send(value: unknown): void;
  /** Register the handler for values arriving from the peer (last handler wins). */
  onValue(handler: (value: unknown) => void): void;
  /** Close this endpoint of the link; idempotent. */
  close(): void;
}

/** Frame carried on a link channel (not on the main RPC channel). */
interface LinkValueFrame {
  type: "__link-value";
  value: unknown;
}

export interface ServeWorkerOptions {
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal. The tag list must match
   * spawn()'s; the handshake frame carries and validates it.
   */
  codecs?: Codec<unknown>[];
  /**
   * Called when the main thread links this worker to a peer via link().
   * The handle exposes send/onValue over a direct channel that bypasses the
   * main thread; the peer must register a compatible codec set for the values
   * sent over it. Link channels are closed by failAll when the actor dies.
   */
  onLink?: (link: LinkHandle) => void;
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
  const links = new Map<string, Channel>();

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
    if (frame.type === "__link") {
      // Direct link to a peer worker, bypassing the main thread. Values sent on
      // it are encoded/decoded through this worker's registry, so a reference
      // handed over the link is owned by this worker and called by the peer.
      const channel = connectChannel(frame.port);
      registry.registerChannel(channel); // failAll closes every open link too
      links.set(frame.label, channel);
      let valueHandler: ((value: unknown) => void) | undefined;
      channel.onMessage((message) => {
        const linkFrame = message as LinkValueFrame;
        if (linkFrame.type === "__link-value") {
          valueHandler?.(registry.decode(linkFrame.value));
        }
      });
      options.onLink?.({
        label: frame.label,
        send(value: unknown): void {
          const transfer: Transferable[] = [];
          channel.send(
            {
              type: "__link-value",
              value: registry.encode(value, transfer),
            } satisfies LinkValueFrame,
            transfer,
          );
        },
        onValue(handler: (value: unknown) => void): void {
          valueHandler = handler;
        },
        close(): void {
          channel.close();
          links.delete(frame.label);
        },
      });
      return;
    }
    if (frame.type === "__link-close") {
      links.get(frame.label)?.close();
      links.delete(frame.label);
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
