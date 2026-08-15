/**
 * Main-thread entry point: wraps a Worker into a type-safe Actor proxy.
 *
 * Usage:
 *   // worker.ts —— export the agreed-upon rpc object; serveWorker() registers it
 *   export const rpc = { add(a: number, b: number) { return a + b; } };
 *
 *   // main.ts —— import type only grabs types, no worker module side effects
 *   import type * as WorkerModule from "./worker.ts";
 *   const actor = await spawn<typeof WorkerModule.rpc>(new Worker(url, { type: "module" }));
 *   const sum = await actor.add(1, 2); // sum: number, type-safe
 */

import {
  ActorDiedError,
  Frame,
  PROTOCOL_VERSION,
  RemoteError,
} from "./core/protocol.ts";
import { type Codec, PayloadCodecRegistry } from "./core/codec.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";

// The RPC boundary is inherently dynamic: type safety comes from Remote<T>
// deriving the worker's concrete signatures, and this any only serves shape
// matching — it never leaks into the user-visible proxy type.
// deno-lint-ignore no-explicit-any
export type RpcFn = (...args: any[]) => any;

/** Return values are normalized to promises (already-promises are not re-wrapped). */
type Resolved<T> = T extends Promise<unknown> ? Awaited<T> : T;

/**
 * Derives the remote proxy type from the worker-side API shape:
 * only function members survive, and return values are wrapped in Promise.
 * Non-function members (constants, classes, ...) resolve to never and fail
 * at compile time.
 */
export type Remote<T> = {
  [K in keyof T]: T[K] extends RpcFn
    ? (...args: Parameters<T[K]>) => Promise<Resolved<ReturnType<T[K]>>>
    : never;
};

export interface SpawnOptions {
  /** Timeout waiting for the worker handshake (module load), default 10s. */
  handshakeTimeoutMs?: number;
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal. Both sides must register the
   * same tag list; the handshake validates it and a mismatch kills the actor.
   */
  codecs?: Codec<unknown>[];
}

interface PendingCall {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

const HANDSHAKE_TIMEOUT = 10_000;

/** Lifecycle methods attached to the proxy (not part of Remote<T> itself). */
export interface ActorHandle {
  /** Sends a dispose frame and terminates the worker; all in-flight calls reject. */
  dispose(): Promise<void>;
}

export async function spawn<T>(
  worker: Worker,
  options: SpawnOptions = {},
): Promise<Remote<T> & ActorHandle> {
  const pending = new Map<number, PendingCall>();
  const registry = new PayloadCodecRegistry();
  // User codecs register first (can override a built-in of the same tag); built-ins fill in after.
  for (const codec of options.codecs ?? []) registry.register(codec);
  for (const codec of [iterableCodec, errorCodec, abortSignalCodec]) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  let nextId = 1;
  let dead = false;
  let resolveHandshake: (() => void) | undefined;
  let rejectHandshake: ((reason: unknown) => void) | undefined;

  // The handshake can succeed or fail: version/codec mismatches reject it on the kill path
  const handshake = new Promise<void>((resolve, reject) => {
    resolveHandshake = resolve;
    rejectHandshake = reject;
  });

  worker.onmessage = (ev: MessageEvent<Frame>) => {
    const frame = ev.data;
    if (frame.type === "handshake") {
      if (frame.version !== PROTOCOL_VERSION) {
        kill(
          new Error(
            `Protocol version mismatch: worker speaks ${frame.version}, host expects ${PROTOCOL_VERSION}`,
          ),
        );
        return;
      }
      // Codec registration mismatch: a startup failure instead of silently wrong values
      const local = registry.tags;
      const remote = frame.codecs;
      const missing = remote.filter((t) => !local.includes(t));
      const extra = local.filter((t) => !remote.includes(t));
      if (missing.length > 0 || extra.length > 0) {
        kill(
          new Error(
            `Codec mismatch: worker has extra codecs [${
              missing.join(", ")
            }], ` +
              `host has extra codecs [${extra.join(", ")}]`,
          ),
        );
        return;
      }
      resolveHandshake?.();
      return;
    }
    if (frame.type === "response") {
      const call = pending.get(frame.id);
      if (!call) return; // unknown id: possibly a late response after dispose
      pending.delete(frame.id);
      if (frame.ok) call.resolve(registry.decode(frame.value));
      else call.reject(new RemoteError(frame.error));
    }
  };

  worker.onerror = (ev) =>
    kill(
      new Error(
        `Worker crashed: ${ev.message}`,
        { cause: ev.error },
      ),
    );
  worker.onmessageerror = () => kill(new Error("Worker deserialization error"));

  function kill(reason: unknown): void {
    if (dead) return;
    dead = true;
    registry.failAll();
    for (const call of pending.values()) call.reject(reason);
    pending.clear();
    // If the handshake hasn't finished yet (version/codec mismatch, worker crash),
    // reject it so spawn() fails instead of hanging.
    rejectHandshake?.(reason);
    worker.terminate();
  }

  function invoke(method: string, args: unknown[]): Promise<unknown> {
    // Always return a rejected promise, never throw synchronously:
    // await and .catch behave identically for callers.
    if (dead) return Promise.reject(new ActorDiedError());
    return new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      const transfer: Transferable[] = [];
      const payload: Frame = {
        type: "request",
        id,
        method,
        args: registry.encode(args, transfer) as unknown[],
      };
      worker.postMessage(payload, { transfer });
    });
  }

  const dispose = (): Promise<void> => {
    if (dead) return Promise.resolve();
    dead = true;
    registry.failAll();
    try {
      worker.postMessage({ type: "dispose" } satisfies Frame);
    } finally {
      worker.terminate();
    }
    for (const call of pending.values()) call.reject(new ActorDiedError());
    pending.clear();
    return Promise.resolve();
  };

  const actor = new Proxy({} as Remote<T> & ActorHandle, {
    get(_target, prop) {
      if (prop === "dispose") return dispose;
      if (prop === Symbol.dispose) return () => void dispose();
      // The proxy must not be detected as a thenable, or await behavior breaks.
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => invoke(prop, args);
      }
      return undefined;
    },
  });

  // Handshake timeout: the worker never reported ready (module load failure or
  // serveWorker() not called) — treat as dead.
  const timer = setTimeout(() => {
    kill(
      new Error(
        "Worker handshake timed out: the worker never reported ready " +
          "(did it call serveWorker()?)",
      ),
    );
  }, options.handshakeTimeoutMs ?? HANDSHAKE_TIMEOUT);

  await handshake;
  clearTimeout(timer);
  return actor;
}
