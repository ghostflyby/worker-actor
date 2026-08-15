/**
 * iterable codec: transports AsyncIterable/Iterable over a dedicated MessageChannel.
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
 * Channel protocol and semantics (lazy/backpressure/release/error/death) are
 * implemented by core/stream.ts primitives; this codec only does matching and
 * placeholder translation.
 *
 * State (channel sets) is isolated per registry instance through the registry's
 * state slot — each actor has its own registry, so one actor's death never
 * closes another's streams.
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
  getCodecState,
} from "../codec.ts";
import { createRemoteIterable, startStreamProducer } from "../stream.ts";

interface StreamHandle {
  [CODEC_PLACEHOLDER_KEY]: "iterable";
  port: MessagePort;
}

interface IterableCodecState {
  activePorts: Set<MessagePort>;
  producerStops: Set<() => void>;
  consumerFails: Set<() => void>;
}

/**
 * Finalizer for rebuilt iterables: when one is garbage-collected on this side,
 * tell the producer side (via the stream's own channel) to release the original
 * object graph. Best-effort only — explicit return()/done/error/fail are the
 * deterministic release paths; the finalizer never holds a strong ref to the
 * registered target.
 */
const releaseRegistry = new FinalizationRegistry<() => void>((fn) => fn());

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
    activePorts: new Set(),
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
  const { port1, port2 } = new MessageChannel();
  const state = getState(ctx);
  // stopFn self-removes from the registry once the stream ends (done/error/
  // release), so the original iterable's object graph becomes collectable even
  // while the actor stays alive.
  let stopFn: () => void = () => {};
  stopFn = startStreamProducer(port1, iterable, () => {
    state.producerStops.delete(stopFn);
    state.activePorts.delete(port2);
  });
  state.activePorts.add(port2);
  state.producerStops.add(stopFn);
  ctx.transfer.push(port2);
  return {
    [CODEC_PLACEHOLDER_KEY]: "iterable",
    port: port2,
  } satisfies StreamHandle;
}

function decode(
  placeholder: { port: MessagePort },
  ctx: DecodeContext,
): AsyncIterable<unknown> {
  const state = getState(ctx);
  let failFn: () => void = () => {};
  const token = {};
  const { iterable, fail, detach } = createRemoteIterable(
    placeholder.port,
    () => {
      state.consumerFails.delete(failFn);
      releaseRegistry.unregister(token);
    },
  );
  failFn = fail;
  state.consumerFails.add(failFn);
  state.activePorts.delete(placeholder.port);
  // Best-effort release on GC: the finalizer only captures the port and detach —
  // never the iterable itself — so it cannot keep the target alive.
  releaseRegistry.register(iterable as object, () => {
    placeholder.port.postMessage({ type: "release" });
    detach();
  }, token);
  return iterable;
}

function onRegistryFail(state: IterableCodecState | undefined): void {
  if (!state) return;
  for (const fail of state.consumerFails) fail();
  for (const stop of state.producerStops) stop();
  for (const port of state.activePorts) port.close();
  state.consumerFails.clear();
  state.producerStops.clear();
  state.activePorts.clear();
}

export const iterableCodec: Codec<AsyncIterable<unknown>> = {
  tag: "iterable",
  matches,
  encode,
  decode,
  onRegistryFail,
};
