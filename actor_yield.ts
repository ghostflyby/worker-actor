/**
 * actorYield: explicit actor yield — a worker method running inside a
 * per-reference serial queue (a remote-ref object or a callback owner)
 * suspends itself: its current segment completes, the queue is released (the
 * next queued call on that reference starts, so a long IO wait never blocks
 * the address), and once `p` settles the task is re-queued — the promise
 * resolves only when the queue serves it again.
 *
 * Outside a per-ref queue (a plain main-channel RPC call) this degrades to
 * `await p` — the main channel is reentrant by design.
 */

import { getCurrentQueue } from "./core/task-queue.ts";

/** Explicit actor yield: see the module docs. */
export function actorYield<T>(p: Promise<T>): Promise<T> {
  const queue = getCurrentQueue();
  if (!queue) return p; // main-channel call: no per-ref queue, plain await
  return queue.yieldTo(p);
}
