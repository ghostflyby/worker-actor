/**
 * Wire protocol: frames exchanged between the main thread and the worker via postMessage.
 *
 * Frames are the only language on the channel. Every RPC call maps to a
 * `request`/`response` pair, correlated by a monotonically increasing id
 * (responses may arrive out of order; a single worker thread naturally processes
 * requests serially, matching the Actor model's "one actor processes messages in order").
 *
 * Values ride on postMessage's structured clone: deeply nested objects, Map/Set, Date,
 * TypedArray, ArrayBuffer (copied) and more are supported natively — no custom serialization.
 */

/** Protocol version, checked at handshake; a mismatch kills the actor. */
export const PROTOCOL_VERSION = 1;

/**
 * Cross-thread errors are serialized manually rather than relying on structured clone.
 * Modern runtimes (V8/Deno/browsers) can clone built-in Error types and DOMException,
 * preserving subtype identity, name, message and stack. But two gaps are confirmed:
 * custom properties are dropped, and custom Error subclasses degrade to plain Error
 * (instanceof fails, name becomes "Error"). Manual serialization keeps the wire format
 * self-contained and consistent across implementations, preserves custom subclass names,
 * and leaves room for future fields such as cause/code.
 */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
}

export type Frame =
  /** Sent by the worker once the module is loaded and its API is ready; spawn() waits for it.
   *  codecs is the worker-side registered codec tag list, checked against the host's. */
  | { type: "handshake"; version: number; codecs: string[] }
  /** Main thread → worker: a single RPC call. args must be structured-cloneable. */
  | { type: "request"; id: number; method: string; args: unknown[] }
  /** Worker → main thread: call result; ok=false carries the serialized error. */
  | {
    type: "response";
    id: number;
    ok: true;
    value: unknown;
  }
  | { type: "response"; id: number; ok: false; error: SerializedError }
  /** Main thread → worker: graceful shutdown; the worker calls self.close() on receipt. */
  | { type: "dispose" };

export function serializeError(e: unknown): SerializedError {
  if (e instanceof Error) {
    return { name: e.name, message: e.message, stack: e.stack };
  }
  return { name: "Error", message: String(e) };
}

/** Error rebuilt on the main thread: keeps the worker's name/message/stack, instanceof Error. */
export class RemoteError extends Error {
  constructor(serialized: SerializedError) {
    super(serialized.message);
    this.name = serialized.name;
    if (serialized.stack) this.stack = serialized.stack;
  }
}

/** Thrown when the channel is dead (worker crashed / disposed) and a call is still made. */
export class ActorDiedError extends Error {
  constructor() {
    super("Actor is dead: the worker has terminated or crashed");
    this.name = "ActorDiedError";
  }
}
