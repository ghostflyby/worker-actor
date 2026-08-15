/**
 * Custom transport extension point: write your own Codec and register it on
 * both sides via spawn/serveWorker `{ codecs }`.
 *
 *   import { PayloadCodecRegistry, type Codec } from "…/codec";
 */
export {
  CODEC_PLACEHOLDER_KEY,
  getCodecState,
  isNativelyClonable,
  isPlaceholder,
  isPlainObject,
  PayloadCodecRegistry,
} from "./core/codec.ts";
export type { Codec, DecodeContext, EncodeContext } from "./core/codec.ts";
