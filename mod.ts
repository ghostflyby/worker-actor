/**
 * A library that wraps Web Workers (and, via the Transport abstraction, child
 * processes and WebSocket connections) into type-safe Actors.
 *
 * Default export: the common surface — creation (spawn / link / actor pool),
 * the runtimes (serveWorker / serveProcess / serveNode), transport adapters
 * (fromMessagePort / fromNodeIpc / fromWebSocket), errors, and the core proxy
 * types. Advanced capabilities live on orthogonal sub-paths and are imported
 * on demand:
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
export { spawnNode, spawnProcess } from "./spawn.ts";
export type {
  ActorHandle,
  Remote,
  SpawnNodeOptions,
  SpawnOptions,
  SpawnProcessOptions,
} from "./spawn.ts";
export { createActorPool } from "./pool.ts";
export type { ActorPool, ActorPoolOptions } from "./pool.ts";
export { serveNode, serveProcess, serveWorker } from "./worker_runtime.ts";
export type {
  LinkHandle,
  ServeWorkerOptions,
  WorkerApi,
} from "./worker_runtime.ts";
export {
  fromMessagePort,
  fromNodeIpc,
  fromWebSocket,
  messageTransport,
} from "./core/transport.ts";
export type {
  MessageTransport,
  Transport,
  TransportKind,
  TransportMessage,
} from "./core/transport.ts";
export { ActorDiedError, RemoteError } from "./core/protocol.ts";
export type { RemoteCallback } from "./core/codecs/callback.ts";
