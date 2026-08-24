/**
 * iterable codec: transports AsyncIterable/Iterable over a dedicated Channel.
 *
 * Matching and handling (in codec registration order):
 *   - AsyncIterable (incl. async generator results): goes straight onto a channel.
 *   - Sync iterables (generators and custom Iterables other than arrays/Map/Set):
 *     wrapped as async-iterable, then onto a channel.
 *   - Stateful sync iterators (objects whose [Symbol.iterator]() returns this):
 *     iterated by reusing the same iterator, no new state.
 *   - Natively cloneable containers (arrays/Map/Set/TypedArray/ArrayBuffer) don't
 *     match this codec and travel via structured clone as-is.
 *
 * Stream protocol and semantics (lazy/backpressure/release/error/death) are
 * implemented by core/stream.ts on top of the generic Channel abstraction;
 * this codec only does matching and placeholder translation.
 *
 * State is isolated per registry instance through the registry's state slot —
 * each actor has its own registry, so one actor's death never closes another's
 * streams. Channels are additionally tracked via ctx.registry.registerChannel
 * so failAll() closes every open stream channel.
 */

import {
  type Codec,
  CODEC_PLACEHOLDER_KEY,
  type DecodeContext,
  type EncodeContext,
  getCodecState,
} from "../codec.ts";
import {
  connectChannel,
  connectToken,
  openChannel,
  registerRelease,
} from "../channel.ts";
import { createRemoteIterable, startStreamProducer } from "../stream.ts";

interface StreamHandle {
  [CODEC_PLACEHOLDER_KEY]: "iterable";
  /** Messageport transports: the transferred peer port. */
  port?: MessagePort;
  /** Mux transports: the channel-establishment token. */
  token?: unknown;
}

interface IterableCodecState {
  /** Producer-side stops (release the generator) for failAll. */
  producerStops: Set<() => void>;
  /** Consumer-side fails (reject pending next()) for failAll. */
  consumerFails: Set<() => void>;
}

function isAsyncIterable(v: unknown): v is AsyncIterable<unknown> {
  return (
    v !== null && typeof v === "object" &&
    typeof (v as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] ===
      "function"
  );
}

/** Wrap a sync iterable (incl. stateful iterators, where v[Symbol.iterator]() returns this) as async-iterable. */
function toAsyncIterable(v: Iterable<unknown>): AsyncIterable<unknown> {
  return {
    [Symbol.asyncIterator]() {
      const it = v[Symbol.iterator]();
      return {
        next: () => Promise.resolve(it.next()),
        return: (value?: unknown) => {
          const r = it.return?.(value);
          return r
            ? Promise.resolve(r)
            : Promise.resolve({ value, done: true });
        },
      };
    },
  };
}

function matches(v: unknown): v is AsyncIterable<unknown> {
  if (v === null || typeof v !== "object") return false;
  if (
    Array.isArray(v) || v instanceof Map || v instanceof Set ||
    ArrayBuffer.isView(v) || v instanceof ArrayBuffer
  ) {
    return false; // natively cloneable containers don't get channels
  }
  return isAsyncIterable(v) ||
    typeof (v as { [Symbol.iterator]?: unknown })[Symbol.iterator] ===
      "function";
}

function getState(
  ctx: { codecState: Map<Codec, unknown> },
): IterableCodecState {
  return getCodecState<IterableCodecState>(ctx, iterableCodec, () => ({
    producerStops: new Set(),
    consumerFails: new Set(),
  }));
}

function encode(value: AsyncIterable<unknown>, ctx: EncodeContext): unknown {
  // Sync iterables (incl. stateful iterators) go through toAsyncIterable:
  // the wrapper calls [Symbol.iterator]() once on first iteration, and a stateful
  // iterator returns itself — so the same iterator is reused, no new state.
  const iterable = isAsyncIterable(value)
    ? value
    : toAsyncIterable(value as Iterable<unknown>);
  const { channel, peerPort, token } = openChannel(ctx);
  ctx.registry.registerChannel(channel);
  const state = getState(ctx);
  // stopFn self-removes from the registry once the stream ends (done/error/
  // release), so the original iterable's object graph becomes collectable even
  // while the actor stays alive.
  let stopFn: () => void = () => {};
  stopFn = startStreamProducer(channel, iterable, () => {
    state.producerStops.delete(stopFn);
  });
  state.producerStops.add(stopFn);
  // Messageport transports hand over a port; Mux transports hand over a token.
  const handle: StreamHandle = {
    [CODEC_PLACEHOLDER_KEY]: "iterable",
    ...(peerPort !== undefined ? { port: peerPort } : { token }),
  };
  return handle;
}

function decode(
  placeholder: { port?: MessagePort; token?: unknown },
  ctx: DecodeContext,
): AsyncIterable<unknown> {
  const channel = placeholder.port !== undefined
    ? connectChannel(placeholder.port)
    : connectToken(
      ctx.transport,
      placeholder.token as { __mux: "open"; ch: number },
    );
  ctx.registry.registerChannel(channel);
  const state = getState(ctx);
  let failFn: () => void = () => {};
  let unregister: () => void = () => {};
  const { iterable, fail, detach } = createRemoteIterable(
    channel,
    () => {
      state.consumerFails.delete(failFn);
      unregister(); // explicit path: done/error/return/fail — no GC release later
    },
  );
  failFn = fail;
  state.consumerFails.add(failFn);
  // Best-effort release on GC: the finalizer captures only the channel and
  // detach — never the iterable itself — so it cannot keep the target alive.
  unregister = registerRelease(iterable as object, () => {
    channel.send({ type: "release" });
    channel.close();
    detach();
  });
  return iterable;
}

function onRegistryFail(state: IterableCodecState | undefined): void {
  if (!state) return;
  for (const fail of state.consumerFails) fail();
  for (const stop of state.producerStops) stop();
  state.consumerFails.clear();
  state.producerStops.clear();
  // open channels are closed by the registry's failAll() via registerChannel.
}

export const iterableCodec: Codec<AsyncIterable<unknown>> = {
  tag: "iterable",
  matches,
  encode,
  decode,
  onRegistryFail,
};
