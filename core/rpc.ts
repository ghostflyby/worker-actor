/**
 * Shared RPC machinery for every channel type (main thread ↔ worker, and
 * worker ↔ worker links). A channel is just an adapter: the request/response
 * protocol, pending-map correlation, codec round-trips and error serialization
 * are identical everywhere, so they live here once.
 *
 *   - makeRpcHandler(api, registry): given an API surface, returns a function
 *     that processes one request frame (lookup → decode args → await →
 *     encode result). The caller owns the transport (postMessage or
 *     channel.send) and only sends the returned result frame.
 *   - createRpcProxy(registry, send, opts): pending-map + id correlation +
 *     codec encoding for the calling side. The caller owns the Proxy shape
 *     (spawn adds dispose()/ActorHandle; a link adds the peer-rpc proxy).
 */

import {
  ActorDiedError,
  RemoteError,
  type SerializedError,
  serializeError,
} from "./protocol.ts";
import type { PayloadCodecRegistry } from "./codec.ts";
import type { Transport } from "./transport.ts";

// The RPC boundary is inherently dynamic; concrete method types are derived by
// Remote<T>/PeerRpc on the calling side.
// deno-lint-ignore no-explicit-any
export type RpcApi = { [method: string]: (...args: any[]) => any };

/**
 * Proxy type for calling a peer worker's RPC surface over a link. The peer
 * type cannot be derived across modules (no import relationship between
 * workers), so declare the contract explicitly:
 *   link.rpc as PeerRpc<{ echo(s: string): Promise<string> }>
 */
export type PeerRpc<T> = {
  [K in keyof T]: T[K] extends (...args: infer A) => infer R
    ? (...args: A) => Promise<Awaited<R>>
    : never;
};

export interface RpcResultOk {
  ok: true;
  id: number;
  value: unknown;
  /** Transfer list for the result message (e.g. channel ports created while encoding). */
  transfer: Transferable[];
}

export interface RpcResultError {
  ok: false;
  id: number;
  error: SerializedError;
}

export type RpcResult = RpcResultOk | RpcResultError;

/** One request frame (channel-agnostic: id/method/args). */
export interface RpcRequest {
  id: number;
  method: string;
  args: unknown[];
}

/** One result frame (channel-agnostic). */
export type RpcResponse =
  | { id: number; ok: true; value: unknown }
  | { id: number; ok: false; error: SerializedError };

/** Process one request frame against an API surface; the caller sends the result. */
export function makeRpcHandler(
  api: RpcApi,
  registry: PayloadCodecRegistry,
  transport?: Transport,
): (request: RpcRequest) => Promise<RpcResult> {
  return async (request) => {
    const fn = api[request.method];
    if (typeof fn !== "function") {
      return {
        ok: false,
        id: request.id,
        error: serializeError(
          new Error(`No such RPC method: "${request.method}"`),
        ),
      };
    }
    try {
      const args = registry.decode(request.args, transport) as unknown[];
      const value = await fn(...args);
      const transfer: Transferable[] = [];
      return {
        ok: true,
        id: request.id,
        value: registry.encode(value, transfer, transport),
        transfer,
      };
    } catch (e) {
      // Errors must be returned, not thrown: the caller is an async callback
      // where an exception would be silently lost.
      return { ok: false, id: request.id, error: serializeError(e) };
    }
  };
}

export interface RpcProxyOptions {
  /** Encode request args and send the request frame over the channel. */
  send: (request: RpcRequest, transfer: Transferable[]) => void;
  /** Returns true once the channel is dead; calls then reject with deadReason(). */
  isDead?: () => boolean;
  /** Rejection reason for calls made after death (default ActorDiedError). */
  deadReason?: () => Error;
  /** The transport this proxy encodes/decodes over (Mux-aware codec values). */
  transport?: Transport;
}

export interface RpcProxy {
  /** Invoke a method on the remote side; resolves with the decoded result. */
  call(method: string, args: unknown[]): Promise<unknown>;
  /** Feed a result frame back; routes to the matching pending call. */
  deliver(response: RpcResponse): void;
  /** Reject every in-flight call (channel closed / actor killed). */
  rejectAll(reason: unknown): void;
}

/** Calling side: pending-map correlation + codec encoding for one channel. */
export function createRpcProxy(
  registry: PayloadCodecRegistry,
  options: RpcProxyOptions,
): RpcProxy {
  const pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (reason: unknown) => void;
  }>();
  let nextId = 1;
  const deadReason = options.deadReason ?? (() => new ActorDiedError());

  return {
    call(method, args): Promise<unknown> {
      if (options.isDead?.()) return Promise.reject(deadReason());
      return new Promise((resolve, reject) => {
        const id = nextId++;
        pending.set(id, { resolve, reject });
        const transfer: Transferable[] = [];
        options.send(
          {
            id,
            method,
            args: registry.encode(
              args,
              transfer,
              options.transport,
            ) as unknown[],
          },
          transfer,
        );
      });
    },
    deliver(response): void {
      const call = pending.get(response.id);
      if (!call) return; // unknown id: possibly a late response after close
      pending.delete(response.id);
      if (response.ok) {
        call.resolve(registry.decode(response.value, options.transport));
      } else call.reject(new RemoteError(response.error));
    },
    rejectAll(reason): void {
      for (const call of pending.values()) call.reject(reason);
      pending.clear();
    },
  };
}
