/**
 * Callback codec: raw functions travel by reference (byref).
 *
 * A bare function cannot be structured-cloned, so any function value in a
 * payload is automatically turned into a remote callback: the caller gets a
 * directly callable reference, and invoking it executes the function in the
 * context where it was registered (its closure) and marshals the result back.
 *
 * Mechanism-wise a callback is a single-function Actor:
 *   - owner side: makeRpcHandler({ [CALL]: fn }, registry) — exactly the RPC
 *     machinery the main channel and worker links use, over a per-callback
 *     MessageChannel;
 *   - calling side: createRpcProxy + a function-targeted Proxy whose apply
 *     trap marshals the call.
 *
 * Callbacks are behavior, not identity: there is no refId, no hand-off, no
 * restore (unlike remote-ref object references). A callback reference cannot
 * be re-encoded (a proxy holder must not re-route the connection to a third
 * party) — encoding one fails loudly.
 *
 * Lifecycle: explicit dispose(), GC-based release (FinalizationRegistry), and
 * failAll (the registry closes the channel via registerChannel).
 *
 * Wire protocol (one channel per callback):
 *   caller → owner { type: "call"; id; method: "call"; args }
 *   owner → caller { type: "result"; id; ok; value | error }
 *   caller → owner { type: "dispose" }
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
  getCodecState,
} from "../codec.ts";
import { connectChannel, openChannel, registerRelease } from "../channel.ts";
import { createRpcProxy, makeRpcHandler } from "../rpc.ts";

/** The single method name on the owner-side API surface. */
const CALL = "call";

/** Marks a callback reference (so re-encoding it can be refused loudly). */
const CALLBACK_BRAND = Symbol.for("worker-actor.callback");

/** Any callable (shape-only; avoids the ban-types Function lint). */
type AnyFunction = (...args: never[]) => unknown;

/**
 * A remote callback: directly callable (apply executes the function at its
 * registration point and resolves with the result), plus dispose().
 */
export type RemoteCallback<A extends unknown[] = unknown[], R = unknown> =
  & ((...args: A) => Promise<Awaited<R>>)
  & { dispose(): Promise<void> };

/**
 * Type helper for worker-side callback parameters: a plain function whose
 * return value may be a value or a Promise of it (`R | Promise<R>`).
 *
 * Declaring a callback as `Callback<A, R>` makes the CALLING side accept both
 * sync and async functions, while the worker body stays honest: `await cb(x)`
 * types as `R`, and using `cb(x)` as a value is rejected by the compiler
 * (it is `R | Promise<R>`). This mirrors the runtime exactly — the callback
 * reference always returns a Promise regardless of how the caller wrote it.
 */
export type Callback<A extends unknown[] = unknown[], R = unknown> = (
  ...args: A
) => R | Promise<R>;

interface CallbackHandle {
  [CODEC_PLACEHOLDER_KEY]: "callback";
  port: MessagePort;
}

type CallbackFrame =
  | { type: "call"; id: number; method: string; args: unknown[] }
  | { type: "result"; id: number; ok: true; value: unknown }
  | {
    type: "result";
    id: number;
    ok: false;
    error: { name: string; message: string; stack?: string };
  }
  | { type: "dispose" };

interface CallbackCodecState {
  /** Live callback proxies; disposed on failAll (channels close via registerChannel). */
  proxies: Set<RemoteCallback>;
}

function matches(v: unknown): v is AnyFunction {
  return typeof v === "function";
}

function getState(
  ctx: { codecState: Map<Codec, unknown> },
): CallbackCodecState {
  return getCodecState<CallbackCodecState>(ctx, callbackCodec, () => ({
    proxies: new Set(),
  }));
}

function encode(fn: AnyFunction, ctx: EncodeContext): unknown {
  if (
    (fn as unknown as { [CALLBACK_BRAND]?: unknown })[CALLBACK_BRAND] === true
  ) {
    // A callback reference must not be re-encoded: the connection belongs to
    // its owner, and a proxy holder cannot re-route it to a third party.
    throw new Error(
      "A callback reference cannot be re-encoded; only fresh functions are " +
        "transmittable (functions travel by reference)",
    );
  }
  const { channel, peerPort } = openChannel(ctx);
  ctx.registry.registerChannel(channel);
  // The owner side is exactly a single-function Actor: one method, the same
  // makeRpcHandler machinery the main channel and links use. The function runs
  // in this context (its closure), results/errors round-trip via the registry.
  const handler = makeRpcHandler(
    { [CALL]: fn as (...args: never[]) => unknown },
    ctx.registry,
  );
  channel.onMessage((message) => {
    const frame = message as CallbackFrame;
    if (frame.type === "call") {
      void handler(frame).then((res) => {
        if (res.ok) {
          channel.send(
            {
              type: "result",
              id: res.id,
              ok: true,
              value: res.value,
            } satisfies CallbackFrame,
            res.transfer,
          );
        } else {
          channel.send(
            {
              type: "result",
              id: res.id,
              ok: false,
              error: res.error,
            } satisfies CallbackFrame,
          );
        }
      });
    } else if (frame.type === "dispose") {
      // No [Symbol.dispose] hook for plain functions; close the channel.
      channel.close();
    }
  });
  return {
    [CODEC_PLACEHOLDER_KEY]: "callback",
    port: peerPort,
  } satisfies CallbackHandle;
}

function decode(
  placeholder: CallbackHandle,
  ctx: DecodeContext,
): RemoteCallback {
  const channel = connectChannel(placeholder.port);
  ctx.registry.registerChannel(channel);
  const state = getState(ctx);
  const proxy = createRpcProxy(ctx.registry, {
    send: (request, transfer) =>
      channel.send(
        {
          type: "call",
          id: request.id,
          method: request.method,
          args: request.args,
        } satisfies CallbackFrame,
        transfer,
      ),
    isDead: () => channel.closed,
    deadReason: () => new Error("Callback disposed"),
  });
  // Route inbound result frames to the pending calls (createRpcProxy owns the
  // pending map but not the transport — the channel handler must feed it).
  channel.onMessage((message) => {
    const frame = message as CallbackFrame;
    if (frame.type === "result") proxy.deliver(frame);
  });
  let unregisterRelease: () => void = () => {};

  const dispose = (): void => {
    channel.send({ type: "dispose" } satisfies CallbackFrame);
    proxy.rejectAll(new Error("Callback disposed"));
    channel.close();
    state.proxies.delete(cb);
    unregisterRelease();
  };

  // Function-targeted proxy: `await cb(x)` hits the apply trap and marshals
  // the call to the registration point.
  const cb = new Proxy((() => {}) as RemoteCallback, {
    apply(_target, _thisArg, args) {
      return proxy.call(CALL, args);
    },
    get(_target, prop) {
      if (prop === CALLBACK_BRAND) return true;
      if (prop === "dispose") return dispose;
      if (prop === Symbol.dispose) return () => void dispose();
      // The proxy must not be detected as a thenable, or await behavior breaks.
      if (prop === "then") return undefined;
      return undefined;
    },
  });

  // Best-effort release on GC: the finalizer captures only the closures, never
  // the proxy itself, so registration cannot keep it alive.
  unregisterRelease = registerRelease(cb, () => {
    channel.send({ type: "dispose" } satisfies CallbackFrame);
    proxy.rejectAll(new Error("Callback garbage-collected"));
    channel.close();
    state.proxies.delete(cb);
  });

  state.proxies.add(cb);
  return cb;
}

function onRegistryFail(state: CallbackCodecState | undefined): void {
  if (!state) return;
  for (const cb of state.proxies) cb.dispose();
  state.proxies.clear();
  // open channels are closed by the registry's failAll() via registerChannel.
}

export const callbackCodec: Codec<AnyFunction> = {
  tag: "callback",
  matches,
  encode,
  decode,
  onRegistryFail,
};
