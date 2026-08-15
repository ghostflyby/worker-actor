/**
 * A library that wraps Web Workers into type-safe Actors.
 *
 * Default export: the common surface — creation (spawn / link / actor pool),
 * the worker-side runtime, errors, and the core proxy types. Advanced
 * capabilities live on orthogonal sub-paths and are imported on demand:
 *
 *   codec/      — custom transport codecs (Codec, PayloadCodecRegistry)
 *   channel/    — channel primitives for codec authors
 *   rpc/        — channel-agnostic RPC machinery
 *   stream/     — stream channel primitives
 *   codecs/     — the built-in codecs
 *   types/      — type-level projections
 *   protocol/   — wire protocol details
 */
export { attachLazyIterator, link, spawn } from "./spawn.ts";
export type { ActorHandle, Remote, SpawnOptions } from "./spawn.ts";
export { createActorPool } from "./pool.ts";
export type { ActorPool, ActorPoolOptions } from "./pool.ts";
export { serveWorker } from "./worker_runtime.ts";
export { actorYield } from "./actor_yield.ts";
export type {
  LinkHandle,
  ServeWorkerOptions,
  WorkerApi,
} from "./worker_runtime.ts";
export { ActorDiedError, RemoteError } from "./core/protocol.ts";
export type { RemoteCallback } from "./core/codecs/callback.ts";
