/**
 * Worker-side runtime: registers the module's top-level rpc object as an Actor,
 * handles request frames and replies with responses. Direct worker-to-worker
 * links (link() from the main thread) get the same RPC machinery via the shared
 * factories in core/rpc.ts — a channel is just an adapter.
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

import { Frame, PROTOCOL_VERSION } from "./core/protocol.ts";
import { type Codec, PayloadCodecRegistry } from "./core/codec.ts";
import { type Channel, connectChannel } from "./core/channel.ts";
import {
  dispatchControlFrame,
  setActiveRegistry,
  setWorkerId,
} from "./core/worker-context.ts";
import {
  createRpcProxy,
  makeRpcHandler,
  type PeerRpc,
  type RpcResponse,
} from "./core/rpc.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";
import { callbackCodec } from "./core/codecs/callback.ts";

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
  /**
   * Declare the RPC surface the peer can call over this link. Defaults to the
   * main-thread api; call serve() to expose a narrower peer-facing surface
   * (the main-thread api usually contains management methods the peer
   * shouldn't see). Calling serve() again replaces the surface.
   */
  serve(api: WorkerApi): void;
  /**
   * Proxy for calling the peer's RPC surface. The peer type cannot be derived
   * across modules, so cast to your contract: `link.rpc as PeerRpc<Contract>`.
   */
  rpc: PeerRpc<object>;
  /** Close this endpoint of the link; in-flight RPC calls reject. */
  close(): void;
}

/** Frame carried on a link channel (not on the main RPC channel). */
type LinkFrame =
  | { type: "__link-value"; value: unknown }
  | { type: "call"; id: number; method: string; args: unknown[] }
  | { type: "result"; id: number; ok: true; value: unknown }
  | {
    type: "result";
    id: number;
    ok: false;
    error: { name: string; message: string; stack?: string };
  };

export interface ServeWorkerOptions {
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal. The tag list must match
   * spawn()'s; the handshake frame carries and validates it.
   */
  codecs?: Codec<unknown>[];
  /**
   * Called when the main thread links this worker to a peer via link().
   * The handle exposes value send/onValue and peer RPC (serve/rpc) over a
   * direct channel that bypasses the main thread; the peer must register a
   * compatible codec set for the values exchanged. Link channels are closed
   * by failAll when the actor dies.
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
  for (
    const codec of [
      iterableCodec,
      errorCodec,
      abortSignalCodec,
      callbackCodec,
    ]
  ) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  const post = (frame: Frame) => self.postMessage(frame);
  const links = new Map<string, Channel>();
  // The RPC machinery is channel-agnostic: the main channel and every link
  // reuse the same handler/proxy factories from core/rpc.ts. The registry is
  // exposed module-level so codec control handlers (ref acquire) can
  // materialize values in this worker's context.
  setActiveRegistry(registry);
  const mainHandler = makeRpcHandler(api, registry);

  self.onmessage = async (ev: MessageEvent<Frame>) => {
    const frame = ev.data;
    if (frame.type === "request") {
      const res = await mainHandler(frame);
      if (res.ok) {
        self.postMessage(
          { type: "response", id: res.id, ok: true, value: res.value },
          { transfer: res.transfer },
        );
      } else {
        post({ type: "response", id: res.id, ok: false, error: res.error });
      }
      return;
    }
    if (frame.type === "__link") {
      // Direct link to a peer worker, bypassing the main thread. Values and
      // RPC calls on it are encoded/decoded through this worker's registry.
      const channel = connectChannel(frame.port);
      registry.registerChannel(channel); // failAll closes every open link too
      links.set(frame.label, channel);
      let valueHandler: ((value: unknown) => void) | undefined;
      // Peer-callable surface: defaults to the main-thread api; serve() overrides.
      let linkApi: WorkerApi = api;
      let linkHandler = makeRpcHandler(linkApi, registry);
      // Calling side toward the peer (bidirectional RPC).
      const proxy = createRpcProxy(registry, {
        send: (request, transfer) =>
          channel.send(
            {
              type: "call",
              id: request.id,
              method: request.method,
              args: request.args,
            } satisfies LinkFrame,
            transfer,
          ),
        isDead: () => channel.closed,
        deadReason: () => new Error("Link closed"),
      });
      const peerRpc = new Proxy({} as PeerRpc<object>, {
        get(_target, prop) {
          // The proxy must not be detected as a thenable, or await behavior breaks.
          if (prop === "then") return undefined;
          if (typeof prop === "string") {
            return (...args: unknown[]) => proxy.call(prop, args);
          }
          return undefined;
        },
      });
      channel.onMessage((message) => {
        const linkFrame = message as LinkFrame;
        if (linkFrame.type === "__link-value") {
          valueHandler?.(registry.decode(linkFrame.value));
        } else if (linkFrame.type === "call") {
          void linkHandler(linkFrame).then((res) => {
            if (res.ok) {
              channel.send(
                {
                  type: "result",
                  id: res.id,
                  ok: true,
                  value: res.value,
                } satisfies LinkFrame,
                res.transfer,
              );
            } else {
              channel.send(
                {
                  type: "result",
                  id: res.id,
                  ok: false,
                  error: res.error,
                } satisfies LinkFrame,
              );
            }
          });
        } else if (linkFrame.type === "result") {
          proxy.deliver(linkFrame as RpcResponse);
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
            } satisfies LinkFrame,
            transfer,
          );
        },
        onValue(handler: (value: unknown) => void): void {
          valueHandler = handler;
        },
        serve(newApi: WorkerApi): void {
          linkApi = newApi;
          linkHandler = makeRpcHandler(linkApi, registry);
        },
        rpc: peerRpc,
        close(): void {
          proxy.rejectAll(new Error("Link closed"));
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
    if (frame.type === "__worker-id") {
      // Main-assigned stable id, embedded in refIds so the main thread can
      // route acquire requests back to this worker. Also dispatched to codec
      // control handlers (the ref codec adopts it as its refId prefix).
      setWorkerId(frame.id);
      dispatchControlFrame({ type: "__worker-id", refId: frame.id });
      return;
    }
    if (
      frame.type === "__serve-ref" || frame.type === "__ref-acquired"
    ) {
      // Reference-acquire control frames: dispatched to the ref codec's
      // handlers (materialize the proxy on the acquired port, or register the
      // fresh per-holder channel on the owner side).
      dispatchControlFrame(frame);
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
