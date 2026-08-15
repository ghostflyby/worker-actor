/**
 * Channel primitives for stream transport: pump an AsyncIterable into a
 * MessageChannel, and rebuild a local AsyncIterable from the channel. The
 * iterable codec (core/codecs/iterable.ts) owns matching and placeholder
 * translation; this file only provides the type-agnostic channel machinery.
 *
 * Channel protocol (consumer ↔ producer):
 *   consumer → producer  { type: "start" }    lazy start on first next(); no iteration, no producer work
 *   producer → consumer  { type: "item", value } | { type: "done" } | { type: "error", error }
 *   consumer → producer  { type: "cancel" }    early stop; triggers the producer's generator finally
 *
 * Semantics:
 *   - Lazy: iteration starts only after "start".
 *   - Backpressure: items are delivered only while the consumer's next() is pending;
 *     the generator awaits between messages.
 *   - Cancel: "cancel" calls iterator.return() (runs generator finally).
 *   - Death: fail() from createRemoteIterable rejects all pending next() calls.
 */

import {
  ActorDiedError,
  RemoteError,
  type SerializedError,
  serializeError,
} from "./protocol.ts";

type StreamFrame =
  | { type: "start" }
  | { type: "item"; value: unknown }
  | { type: "done" }
  | { type: "error"; error: SerializedError }
  | { type: "cancel" };

/**
 * Producer side: pump an AsyncIterable into the channel. The returned stop()
 * cancels/closes and is safe to call repeatedly.
 */
export function startStreamProducer(
  port: MessagePort,
  iterable: AsyncIterable<unknown>,
): () => void {
  const iterator = iterable[Symbol.asyncIterator]();
  let started = false;
  let stopped = false;

  port.onmessage = (ev: MessageEvent<StreamFrame>) => {
    const frame = ev.data;
    if (frame.type === "start") {
      if (!started) {
        started = true;
        void pump();
      }
    } else if (frame.type === "cancel") {
      stop();
    }
  };

  function stop(): void {
    if (stopped) return;
    stopped = true;
    port.close();
    const ret = iterator.return?.();
    if (ret) void ret.catch(() => {});
  }

  async function pump(): Promise<void> {
    try {
      while (!stopped) {
        const result = await iterator.next();
        if (stopped) break;
        if (result.done) {
          port.postMessage({ type: "done" } satisfies StreamFrame);
          break;
        }
        port.postMessage(
          { type: "item", value: result.value } satisfies StreamFrame,
        );
      }
    } catch (e) {
      if (!stopped) {
        port.postMessage(
          { type: "error", error: serializeError(e) } satisfies StreamFrame,
        );
      }
    } finally {
      stop();
    }
  }

  return stop;
}

/**
 * Consumer side: rebuild a local AsyncIterable from the channel.
 * fail() rejects all pending next() calls when the actor dies, so they don't hang.
 */
export function createRemoteIterable(
  port: MessagePort,
): { iterable: AsyncIterable<unknown>; fail: () => void } {
  const queue: StreamFrame[] = [];
  const waiters: Array<{
    resolve: (result: IteratorResult<unknown>) => void;
    reject: (reason: unknown) => void;
  }> = [];
  let closed = false;
  let failure: SerializedError | undefined;

  const deliver = (frame: StreamFrame): void => {
    const waiter = waiters.shift();
    if (!waiter) {
      // Only item/done/error are consumed; start/cancel never reach the consumer port
      if (
        frame.type === "item" || frame.type === "done" || frame.type === "error"
      ) {
        queue.push(frame);
      }
      return;
    }
    if (frame.type === "item") {
      waiter.resolve({ done: false, value: frame.value });
    } else if (frame.type === "done") {
      waiter.resolve({ done: true, value: undefined });
    } else if (frame.type === "error") {
      waiter.reject(new RemoteError(frame.error));
    } else waiter.resolve({ done: true, value: undefined });
  };

  port.onmessage = (ev: MessageEvent<StreamFrame>) => {
    const frame = ev.data;
    deliver(frame);
    if (frame.type === "done" || frame.type === "error") {
      if (frame.type === "error") failure = frame.error;
      closed = true;
      port.close();
    }
  };
  port.onmessageerror = () => {
    failure = {
      name: "Error",
      message: "stream message deserialization failed",
    };
    closed = true;
    port.close();
    deliver({ type: "error", error: failure });
  };

  const fail = (): void => {
    if (closed) return;
    failure = serializeError(new ActorDiedError());
    closed = true;
    port.close();
    while (waiters.length) {
      const waiter = waiters.shift()!;
      waiter.reject(new RemoteError(failure!));
    }
  };

  const toResult = (frame: StreamFrame): IteratorResult<unknown> => {
    if (frame.type === "item") return { done: false, value: frame.value };
    if (frame.type === "done") return { done: true, value: undefined };
    if (frame.type === "error") throw new RemoteError(frame.error);
    return { done: true, value: undefined }; // start/cancel never appear in the consumer queue
  };

  const iterable: AsyncIterable<unknown> = {
    [Symbol.asyncIterator]() {
      let started = false;
      return {
        next(): Promise<IteratorResult<unknown>> {
          if (!started) {
            started = true;
            port.postMessage({ type: "start" } satisfies StreamFrame);
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
          port.postMessage({ type: "cancel" } satisfies StreamFrame);
          port.close();
          closed = true;
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    },
  };

  return { iterable, fail };
}
