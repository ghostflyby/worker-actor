/** A library that wraps Web Workers into type-safe Actors. */
export { spawn } from "./spawn.ts";
export type { ActorHandle, Remote, SpawnOptions } from "./spawn.ts";
export { serveWorker } from "./worker_runtime.ts";
export type { ServeWorkerOptions, WorkerApi } from "./worker_runtime.ts";
export { ActorDiedError, RemoteError } from "./core/protocol.ts";
export type { Frame, SerializedError } from "./core/protocol.ts";
export { createRemoteIterable, startStreamProducer } from "./core/stream.ts";

// —— 通用 Codec 机制 ——
export {
  CODEC_PLACEHOLDER_KEY,
  getCodecState,
  PayloadCodecRegistry,
} from "./core/codec.ts";
export type { Codec, DecodeContext, EncodeContext } from "./core/codec.ts";
export { iterableCodec } from "./core/codecs/iterable.ts";
export { createErrorCodec, errorCodec } from "./core/codecs/error.ts";
export type { ErrorCodecOptions } from "./core/codecs/error.ts";
export { abortSignalCodec } from "./core/codecs/abort_signal.ts";
