/**
 * error codec: custom transport for Error values nested inside payloads.
 *
 * Design trade-off: built-in Error types (Error/RangeError/TypeError/...) and
 * DOMException survive structured clone natively (instanceof, name/message/stack)
 * and don't need handling; but custom Error subclasses degrade (instanceof fails,
 * name becomes "Error") and custom properties are always dropped. This codec only
 * takes over custom subclasses, serializing them into a placeholder:
 *
 *   { __wCodec: "error", name, message, stack?, cause?, props? }
 *
 * decode rebuilds a RemoteError (name keeps the custom subclass name) plus any
 * extra properties. By default only name/message/stack/cause are kept, matching
 * top-level RPC errors; createErrorCodec({ keepOwnProperties: true }) additionally
 * keeps enumerable own properties.
 *
 * Note: the placeholder is the end of encoding — walk does not recurse into it,
 * so nested values inside cause/custom properties are not codec-processed and
 * travel as plain structured-clone values.
 */

import { type Codec, CODEC_PLACEHOLDER_KEY } from "../codec.ts";
import { RemoteError } from "../protocol.ts";

export interface ErrorCodecOptions {
  /** Keep the error's enumerable own properties (default false, matching top-level RPC errors). */
  keepOwnProperties?: boolean;
}

interface ErrorPlaceholder {
  [CODEC_PLACEHOLDER_KEY]: "error";
  name: string;
  message: string;
  stack?: string;
  cause?: unknown;
  props?: Record<string, unknown>;
}

const SKIP_KEYS = new Set(["name", "message", "stack", "cause"]);

/** Built-in Error constructors: structured clone preserves them natively (instanceof/name/stack). */
const NATIVE_ERROR_CONSTRUCTORS = new Set<ErrorConstructor>([
  Error,
  EvalError,
  RangeError,
  ReferenceError,
  SyntaxError,
  TypeError,
  URIError,
]);

function matches(v: unknown): v is Error {
  if (!(v instanceof Error)) return false; // DOMException doesn't inherit Error; native cloning preserves it
  return !NATIVE_ERROR_CONSTRUCTORS.has(v.constructor as ErrorConstructor);
}

export function createErrorCodec(
  options: ErrorCodecOptions = {},
): Codec<Error> {
  const keepOwnProperties = options.keepOwnProperties ?? false;

  const codec: Codec<Error> = {
    tag: "error",

    matches,

    encode(v: Error): ErrorPlaceholder {
      const placeholder: ErrorPlaceholder = {
        [CODEC_PLACEHOLDER_KEY]: "error",
        name: v.name,
        message: v.message,
      };
      if (v.stack) placeholder.stack = v.stack;
      const cause = (v as { cause?: unknown }).cause;
      if (cause !== undefined) placeholder.cause = cause;
      if (keepOwnProperties) {
        const props: Record<string, unknown> = {};
        for (const key of Object.keys(v)) {
          if (SKIP_KEYS.has(key)) continue;
          props[key] = (v as unknown as Record<string, unknown>)[key];
        }
        if (Object.keys(props).length > 0) placeholder.props = props;
      }
      return placeholder;
    },

    decode(placeholder: ErrorPlaceholder): Error {
      const err = new RemoteError({
        name: placeholder.name,
        message: placeholder.message,
        stack: placeholder.stack,
      });
      if (placeholder.cause !== undefined) {
        (err as { cause?: unknown }).cause = placeholder.cause;
      }
      if (placeholder.props) {
        for (const [key, value] of Object.entries(placeholder.props)) {
          (err as unknown as Record<string, unknown>)[key] = value;
        }
      }
      return err;
    },
  };

  return codec;
}

/** 默认实例：只保留 name/message/stack/cause。 */
export const errorCodec: Codec<Error> = createErrorCodec();
