/**
 * Built-in codecs. Registered automatically by spawn/serveWorker; import them
 * here to build custom codec sets (e.g. `serveWorker(api, { codecs: […] })`)
 * or to override a built-in tag.
 *
 *   import { createErrorCodec } from "…/codecs";
 */
export { abortSignalCodec } from "./core/codecs/abort_signal.ts";
export { callbackCodec, releaseCallback } from "./core/codecs/callback.ts";
export type { RemoteCallback } from "./core/codecs/callback.ts";
export { createErrorCodec, errorCodec } from "./core/codecs/error.ts";
export type { ErrorCodecOptions } from "./core/codecs/error.ts";
export { iterableCodec } from "./core/codecs/iterable.ts";
