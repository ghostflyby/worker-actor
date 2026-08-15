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
  /**
   * The original error, structured-cloned alongside the manual fields. Only
   * attached for known natively-cloneable types (built-in Error subclasses and
   * DOMException): the clone preserves instanceof identity and DOMException's
   * `code`. Custom subclasses and errors with custom properties stay
   * manual-only (their clone would degrade, so it adds nothing). Best-effort:
   * if the clone would fail, the field is dropped and the manual fields still
   * carry the error.
   */
  native?: Error | DOMException;
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
  | { type: "dispose" }
  /** Worker-runtime internal: establish a direct link channel between two workers (label-scoped). */
  | { type: "__link"; label: string; port: MessagePort }
  /** Worker-runtime internal: tear down a link channel by label. */
  | { type: "__link-close"; label: string }
  /** Main → worker: this worker's stable id (embedded in refIds for acquire routing). */
  | { type: "__worker-id"; id: string }
  /** Worker → main: request a channel to the owner of a reference (refId embeds the owner id). */
  | { type: "__acquire-ref"; refId: string }
  /** Main → owner: serve this reference's object over the given fresh port (per-holder channel). */
  | { type: "__serve-ref"; refId: string; port: MessagePort }
  /** Main → requester: here is the acquired channel; materialize the proxy. */
  | { type: "__ref-acquired"; refId: string; port: MessagePort };

/** Built-in Error constructors whose instances structured-clone natively (identity preserved). */
const NATIVE_ERROR_CONSTRUCTORS = new Set<ErrorConstructor>([
  Error,
  EvalError,
  RangeError,
  ReferenceError,
  SyntaxError,
  TypeError,
  URIError,
]);

/** Known natively-cloneable error types: the clone adds real value (identity / code). */
function isNativelyCloneableError(e: Error | DOMException): boolean {
  return e instanceof DOMException ||
    NATIVE_ERROR_CONSTRUCTORS.has(e.constructor as ErrorConstructor);
}

export function serializeError(e: unknown): SerializedError {
  if (e instanceof Error) {
    const serialized: SerializedError = {
      name: e.name,
      message: e.message,
      stack: e.stack,
    };
    if (isNativelyCloneableError(e)) {
      try {
        serialized.native = e;
      } catch {
        // The clone would fail on this runtime: drop the field, the manual
        // name/message/stack still carry the error.
      }
    }
    return serialized;
  }
  if (e instanceof DOMException) {
    const serialized: SerializedError = { name: e.name, message: e.message };
    try {
      serialized.native = e;
    } catch {
      // same best-effort degradation
    }
    return serialized;
  }
  return { name: "Error", message: String(e) };
}

/** Error rebuilt on the main thread: keeps the worker's name/message/stack, instanceof Error. */
export class RemoteError extends Error {
  /**
   * The structured-clone of the original error, when the worker attached one
   * (built-in Error subclasses / DOMException). Use it for instanceof checks
   * and DOMException.code; fall back to `name` otherwise.
   */
  readonly inner: Error | DOMException | undefined;

  constructor(serialized: SerializedError) {
    super(serialized.message);
    this.name = serialized.name;
    if (serialized.stack) this.stack = serialized.stack;
    this.inner = serialized.native;
  }
}

/** Thrown when the channel is dead (worker crashed / disposed) and a call is still made. */
export class ActorDiedError extends Error {
  constructor() {
    super("Actor is dead: the worker has terminated or crashed");
    this.name = "ActorDiedError";
  }
}
