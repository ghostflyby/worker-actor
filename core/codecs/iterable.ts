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
 * Channel protocol and semantics (lazy/backpressure/cancel/error/death) are
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
  const stop = startStreamProducer(port1, iterable);
  const state = getState(ctx);
  state.activePorts.add(port2);
  state.producerStops.add(stop);
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
  const { iterable, fail } = createRemoteIterable(placeholder.port);
  const state = getState(ctx);
  state.activePorts.delete(placeholder.port);
  state.consumerFails.add(fail);
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
