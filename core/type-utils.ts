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
