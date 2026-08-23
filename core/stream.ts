/**
 * Stream channel primitives: pump an AsyncIterable into a Channel, and rebuild a
 * local AsyncIterable from one. The iterable codec (core/codecs/iterable.ts)
 * owns matching and placeholder translation; this file only provides the
 * type-agnostic stream protocol on top of the generic Channel abstraction.
 *
 * Stream protocol (consumer ↔ producer):
 *   consumer → producer  { type: "start" }    lazy start on first next(); no iteration, no producer work
 *   producer → consumer  { type: "item", value } | { type: "done" } | { type: "error", error }
 *   consumer → producer  { type: "release" }   consumer GC'd or explicitly returned the stream; producer releases
 *
 * Semantics:
 *   - Lazy: iteration starts only after "start".
 *   - Backpressure: items are delivered only while the consumer's next() is pending;
 *     the generator awaits between messages.
 *   - Release: calls iterator.return() (runs generator finally).
 *   - Death: fail() from createRemoteIterable rejects all pending next() calls.
 */

import {
  ActorDiedError,
  RemoteError,
  type SerializedError,
  serializeError,
} from "./protocol.ts";
import type { Channel } from "./channel.ts";

type StreamFrame =
  | { type: "start" }
  | { type: "item"; value: unknown }
  | { type: "done" }
  | { type: "error"; error: SerializedError }
  | { type: "release" };

/**
 * Producer side: pump an AsyncIterable into a channel. The returned stop()
 * cancels/closes and is safe to call repeatedly. onStopped fires exactly once
 * (on the first stop) and lets the caller remove registry bookkeeping.
 *
 * When `encode` is given, every item is passed through it before sending, so
 * codec values (actor references, nested AsyncIterables) travel as encoded
 * placeholders instead of being structured-cloned (which would fail for
 * proxies). The default sends items as-is. `encode` returns the encoded value
 * plus any transferable ports to move with the message.
 */
export function startStreamProducer(
  channel: Channel,
  iterable: AsyncIterable<unknown>,
  onStopped?: () => void,
  encode?: (value: unknown) => { value: unknown; transfer: Transferable[] },
): () => void {
  const iterator = iterable[Symbol.asyncIterator]();
  let started = false;
  let stopped = false;

  channel.onMessage((message) => {
    const frame = message as StreamFrame;
    if (frame.type === "start") {
      if (!started) {
        started = true;
        void pump();
      }
    } else if (frame.type === "release") {
      stop();
    }
  });

  function stop(): void {
    if (stopped) return;
    stopped = true;
    channel.close();
    const ret = iterator.return?.();
    if (ret) void ret.catch(() => {});
    // Self-remove from the codec's bookkeeping; idempotent thanks to `stopped`.
    onStopped?.();
  }

  async function pump(): Promise<void> {
    try {
      while (!stopped) {
        const result = await iterator.next();
        if (stopped) break;
        if (result.done) {
          channel.send({ type: "done" } satisfies StreamFrame);
          break;
        }
        if (encode === undefined) {
          channel.send({ type: "item", value: result.value } satisfies StreamFrame);
        } else {
          const encoded = encode(result.value);
          channel.send(
            { type: "item", value: encoded.value } satisfies StreamFrame,
            encoded.transfer,
          );
        }
      }
    } catch (e) {
      if (!stopped) {
        channel.send(
          {
            type: "error",
            error: serializeError(e),
          } satisfies StreamFrame,
        );
      }
    } finally {
      stop();
    }
  }

  return stop;
}

/**
 * Consumer side: rebuild a local AsyncIterable from a channel.
 * fail() rejects all pending next() calls when the actor dies, so they don't hang.
 * onReleased fires exactly once (when the stream ends by any path: done/error,
 * explicit return(), or fail) and lets the caller remove registry bookkeeping.
 *
 * When `decode` is given, every item's value is passed through it before
 * delivery, rebuilding codec values (actor references, nested AsyncIterables)
 * that the producer encoded.
 */
export function createRemoteIterable(
  channel: Channel,
  onReleased?: () => void,
  decode?: (value: unknown) => unknown,
): { iterable: AsyncIterable<unknown>; fail: () => void; detach: () => void } {
  const queue: StreamFrame[] = [];
  const waiters: Array<{
    resolve: (result: IteratorResult<unknown>) => void;
    reject: (reason: unknown) => void;
  }> = [];
  let closed = false;
  let released = false;
  let failure: SerializedError | undefined;

  const detach = (): void => {
    if (released) return;
    released = true;
    onReleased?.();
  };

  const deliver = (frame: StreamFrame): void => {
    const waiter = waiters.shift();
    if (!waiter) {
      // Only item/done/error are consumed; start/release never reach the consumer
      if (
        frame.type === "item" || frame.type === "done" || frame.type === "error"
      ) {
        queue.push(frame);
      }
      return;
    }
    if (frame.type === "item") {
      waiter.resolve({
        done: false,
        value: decode === undefined ? frame.value : decode(frame.value),
      });
    } else if (frame.type === "done") {
      waiter.resolve({ done: true, value: undefined });
    } else if (frame.type === "error") {
      waiter.reject(new RemoteError(frame.error));
    } else waiter.resolve({ done: true, value: undefined });
  };

  channel.onMessage((message) => {
    const frame = message as StreamFrame;
    deliver(frame);
    if (frame.type === "done" || frame.type === "error") {
      if (frame.type === "error") failure = frame.error;
      closed = true;
      channel.close();
      detach();
    }
  });

  const fail = (): void => {
    if (closed) return;
    failure = serializeError(new ActorDiedError());
    closed = true;
    channel.close();
    while (waiters.length) {
      const waiter = waiters.shift()!;
      waiter.reject(new RemoteError(failure!));
    }
    detach();
  };

  const toResult = (frame: StreamFrame): IteratorResult<unknown> => {
    if (frame.type === "item") {
      return {
        done: false,
        value: decode === undefined ? frame.value : decode(frame.value),
      };
    }
    if (frame.type === "done") return { done: true, value: undefined };
    if (frame.type === "error") throw new RemoteError(frame.error);
    return { done: true, value: undefined }; // start/release never appear in the consumer queue
  };

  const iterable: AsyncIterable<unknown> = {
    [Symbol.asyncIterator]() {
      let started = false;
      return {
        next(): Promise<IteratorResult<unknown>> {
          if (!started) {
            started = true;
            channel.send({ type: "start" } satisfies StreamFrame);
          }
          if (failure) return Promise.reject(new RemoteError(failure));
          const frame = queue.shift();
          if (frame) return Promise.resolve(toResult(frame));
          if (closed) return Promise.resolve({ done: true, value: undefined });
          return new Promise((resolve, reject) =>
            waiters.push({ resolve, reject })
          );
        },
        return(): Promise<IteratorResult<unknown>> {
          // Explicit abandon: tell the producer to release, then detach locally.
          // A finalizer path (see the iterable codec) may also send "release";
          // stop() on the producer side is idempotent.
          channel.send({ type: "release" } satisfies StreamFrame);
          channel.close();
          closed = true;
          detach();
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    },
  };

  return { iterable, fail, detach };
}
