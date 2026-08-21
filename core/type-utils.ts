/**
 * Type-level transformation for callback parameters.
 *
 * A callback reference always returns a Promise at runtime, no matter how the
 * caller wrote the function. To let the CALLING side pass either a sync or an
 * async function while the worker DECLARES the Promise form (the honest
 * runtime shape), the proxy parameter type widens every Promise-returning
 * function to `R | Promise<R>` — deep-recursively through tuples, arrays and
 * plain objects.
 *
 * This is applied at the Remote<T> projection point (spawn.ts):
 *   `(...args: TransformCallbacks<Parameters<T[K]>>) => Promise<...>`
 * Only the caller-facing parameter shape changes; the worker-side declaration
 * is untouched.
 *
 * Safety rules (verified by type probes):
 *   - built-in containers (Promise/AsyncIterable/Map/Set/Date/ArrayBuffer/
 *     views) pass through unchanged — the recursion must not turn a Map into
 *     an object-shaped type;
 *   - type-predicate functions and other edge shapes are preserved by the
 *     catch-all function branch, so `Codec.matches` etc. are never mapped into
 *     empty objects;
 *   - tuples keep their length/optionality through the mapped array branch.
 */

import type { Codec } from "./codec.ts";

/** Widen one Promise-returning function to accept sync or async callers. */
type AcceptSyncOrAsync<F> = F extends (...args: infer A) => Promise<infer R>
  ? (...args: A) => R | Promise<R>
  : F;

/** Deep-recursive: widen Promise-returning functions anywhere in T. */
export type TransformCallbacks<T> = T extends Promise<unknown> ? T
  : T extends AsyncIterable<unknown> ? T
  : T extends Map<unknown, unknown> ? T
  : T extends Set<unknown> ? T
  : T extends Date ? T
  : T extends ArrayBuffer ? T
  : T extends ArrayBufferView ? T
  : T extends (...args: infer A) => Promise<infer R>
    ? (...args: A) => R | Promise<R>
  : T extends (...args: infer A) => infer R ? (...args: A) => R
  : T extends (...args: never[]) => unknown ? T // catch-all: predicates & edges stay callable
  : T extends readonly unknown[] ? { [K in keyof T]: TransformCallbacks<T[K]> }
  : T extends object ? { [K in keyof T]: TransformCallbacks<T[K]> }
  : T;

/** Shorthand for the accepted caller-side function shape. */
export type SyncOrAsync<A extends unknown[] = unknown[], R = unknown> = (
  ...args: A
) => R | Promise<R>;

/**
 * A return type whose async semantics are already declared by the worker: it
 * implements PromiseLike (awaitable) OR AsyncIterable (iterable). Remote<T>
 * treats such a return type as "the writer already said how it behaves" and
 * projects it to a non-nested native Promise (thenable side: the RPC handler
 * awaits the method's return value, so the crossing value is the resolution
 * X) or to the AsyncIterable<E> interface (iterable side: the iterable codec
 * rebuilds a local stream on the far side).
 */
export type AsyncSemantics = PromiseLike<unknown> | AsyncIterable<unknown>;

/**
 * Extract the value types of a const codec tuple, so a runtime `codecs`
 * registration is reflected in the type projection:
 *
 *   const codecs = [remoteRefCodec] as const;  // Codec<RemoteRef<unknown>>
 *   CodecValueTypes<typeof codecs>             // RemoteRef<unknown>
 *
 * Entries typed `Codec<unknown>` (an empty generic, e.g. a widened array) are
 * filtered out: a Pass of `unknown` would swallow every return type, so an
 * untyped codec contributes nothing and the projection falls back to the
 * built-in async rules — still honest, just not identity-preserving.
 */
export type CodecValueTypes<C extends readonly unknown[]> = C extends
  readonly [infer Head, ...infer Tail] ?
    | (Head extends Codec<infer V> ? [unknown] extends [V] ? never : V
      : never)
    | CodecValueTypes<Tail>
  : never;
