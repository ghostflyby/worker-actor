/**
 * Pluggable Codec mechanism: a registry for custom transport of RPC payloads.
 *
 * Background: most values (deeply nested objects, Map/Set/Date/TypedArray, built-in
 * Error types, DOMException, etc.) structured-clone cleanly and ride postMessage
 * directly. But one class of values cannot be cloned reliably — entities carrying
 * runtime state, closures, or prototypes: AsyncIterables, stateful Iterators,
 * AbortSignal in Deno (prototype and aborted state are lost, verified by probe),
 * and custom class instances (prototype chain degrades). Codecs give these values
 * a single escape hatch.
 *
 * Each Codec owns the transport for one tag:
 *   - encode: turn the value into a cloneable placeholder `{ __wCodec: tag, ... }`;
 *     when cross-thread signaling is needed, create a MessageChannel and push its
 *     port2 into the placeholder and ctx.transfer (transferred with the message).
 *   - decode: rebuild the original type from the placeholder.
 *   - onRegistryFail: cleanup channels/listeners created by this codec when the
 *     registry's failAll() runs (actor terminated or crashed).
 *
 * The registry deep-walks RPC payloads (arrays/Map/Set/plain objects/cycles) and
 * picks the first codec whose matches() fires, in registration order. Placeholders
 * are dispatched by tag on decode; an unknown tag throws — a registration mismatch
 * becomes a startup failure instead of silently producing garbage.
 */

/** Field name inside a placeholder that identifies the codec tag. */
export const CODEC_PLACEHOLDER_KEY = "__wCodec";

export interface EncodeContext {
  /** structured clone transfer list: ports etc. created by codecs are pushed here. */
  transfer: Transferable[];
  seen: WeakMap<object, unknown>;
  /** Per-codec state slot held by the registry; codecs read/write it themselves. */
  codecState: Map<Codec, unknown>;
}

export interface DecodeContext {
  seen: WeakMap<object, unknown>;
  codecState: Map<Codec, unknown>;
}

export interface Codec<T = unknown> {
  /** Wire identifier for the placeholder; both sides verify their tag lists at handshake. */
  readonly tag: string;
  /** Brand check: whether this value should be handled by this codec. */
  matches(value: unknown): value is T;
  /** Encode into a cloneable placeholder; create a channel and transfer its port when needed. */
  encode(value: T, ctx: EncodeContext): unknown;
  /** Rebuild the original type from the placeholder. */
  decode(placeholder: unknown, ctx: DecodeContext): T;
  /** Cleanup codec resources (channels, listeners) on failAll(); state is the registry-held slot. */
  onRegistryFail?(state: unknown): void;
}

/**
 * Read this codec's instance state from the registry state slot, initializing it
 * with init() on first use. State is isolated per codec instance (each actor's
 * registry is independent).
 */
export function getCodecState<T>(
  ctx: { codecState: Map<Codec, unknown> },
  codec: Codec,
  init: () => T,
): T {
  let state = ctx.codecState.get(codec) as T | undefined;
  if (state === undefined) {
    state = init();
    ctx.codecState.set(codec, state);
  }
  return state;
}

export function isPlaceholder(
  v: object,
): v is { [CODEC_PLACEHOLDER_KEY]: string } {
  return typeof (v as { [CODEC_PLACEHOLDER_KEY]?: unknown })[
    CODEC_PLACEHOLDER_KEY
  ] ===
    "string";
}

export function isPlainObject(v: object): boolean {
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}

/** Natively structured-cloneable containers (arrays, Map, Set, TypedArray, ArrayBuffer) don't get channels. */
export function isNativelyClonable(v: object): boolean {
  return (
    Array.isArray(v) ||
    v instanceof Map ||
    v instanceof Set ||
    ArrayBuffer.isView(v) ||
    v instanceof ArrayBuffer
  );
}

export class PayloadCodecRegistry {
  #codecs = new Map<string, Codec>();
  #order: Codec[] = [];
  #codecState = new Map<Codec, unknown>();

  register(codec: Codec): this {
    if (this.#codecs.has(codec.tag)) {
      throw new Error(`Codec tag "${codec.tag}" is already registered`);
    }
    this.#codecs.set(codec.tag, codec);
    this.#order.push(codec);
    return this;
  }

  unregister(tag: string): void {
    const codec = this.#codecs.get(tag);
    if (codec) {
      this.#codecs.delete(tag);
      this.#order = this.#order.filter((c) => c !== codec);
    }
  }

  /** Whether the tag is registered (a user codec registered first can override a built-in of the same tag). */
  has(tag: string): boolean {
    return this.#codecs.has(tag);
  }

  /** Registered codec tag list (in registration order), exchanged at handshake. */
  get tags(): string[] {
    return this.#order.map((c) => c.tag);
  }

  /** Before sending: replace deeply nested custom values with placeholders; transferred ports go into `transfer`. */
  encode(value: unknown, transfer: Transferable[]): unknown {
    return this.#encodeWalk(value, transfer, new WeakMap());
  }

  #encodeWalk(
    v: unknown,
    transfer: Transferable[],
    seen: WeakMap<object, unknown>,
  ): unknown {
    if (v === null || typeof v !== "object") return v;
    const cached = seen.get(v);
    if (cached !== undefined) return cached;
    // Custom codecs take precedence over container recursion: the first match takes over.
    for (const codec of this.#order) {
      if (codec.matches(v)) {
        const placeholder = codec.encode(v, {
          transfer,
          seen,
          codecState: this.#codecState,
        });
        seen.set(v, placeholder);
        return placeholder;
      }
    }
    if (Array.isArray(v)) {
      const out = new Array<unknown>(v.length);
      seen.set(v, out);
      for (let i = 0; i < v.length; i++) {
        out[i] = this.#encodeWalk(v[i], transfer, seen);
      }
      return out;
    }
    if (v instanceof Map) {
      const out = new Map<unknown, unknown>();
      seen.set(v, out);
      for (const [k, val] of v) {
        out.set(
          this.#encodeWalk(k, transfer, seen),
          this.#encodeWalk(val, transfer, seen),
        );
      }
      return out;
    }
    if (v instanceof Set) {
      const out = new Set<unknown>();
      seen.set(v, out);
      for (const val of v) out.add(this.#encodeWalk(val, transfer, seen));
      return out;
    }
    if (isPlainObject(v)) {
      const out: Record<string, unknown> = {};
      seen.set(v, out);
      for (const key of Object.keys(v)) {
        out[key] = this.#encodeWalk(
          (v as Record<string, unknown>)[key],
          transfer,
          seen,
        );
      }
      return out;
    }
    // Date/RegExp/TypedArray/ArrayBuffer/class instances: hand over to structured clone as-is.
    return v;
  }

  /** After receiving: rebuild placeholders into original values (deeply nested, expanded automatically). */
  decode(value: unknown): unknown {
    return this.#decodeWalk(value, new WeakMap());
  }

  #decodeWalk(v: unknown, seen: WeakMap<object, unknown>): unknown {
    if (v === null || typeof v !== "object") return v;
    const cached = seen.get(v);
    if (cached !== undefined) return cached;
    if (isPlaceholder(v)) {
      const codec = this.#codecs.get(v[CODEC_PLACEHOLDER_KEY]);
      if (!codec) {
        // Loud fail: a registration mismatch on either side should not silently produce wrong values.
        throw new Error(
          `Unknown codec tag in payload: "${v[CODEC_PLACEHOLDER_KEY]}"`,
        );
      }
      const decoded = codec.decode(v, {
        seen,
        codecState: this.#codecState,
      });
      seen.set(v, decoded);
      return decoded;
    }
    if (Array.isArray(v)) {
      const out = new Array<unknown>(v.length);
      seen.set(v, out);
      for (let i = 0; i < v.length; i++) out[i] = this.#decodeWalk(v[i], seen);
      return out;
    }
    if (v instanceof Map) {
      const out = new Map<unknown, unknown>();
      seen.set(v, out);
      for (const [k, val] of v) {
        out.set(this.#decodeWalk(k, seen), this.#decodeWalk(val, seen));
      }
      return out;
    }
    if (v instanceof Set) {
      const out = new Set<unknown>();
      seen.set(v, out);
      for (const val of v) out.add(this.#decodeWalk(val, seen));
      return out;
    }
    if (isPlainObject(v)) {
      const out: Record<string, unknown> = {};
      seen.set(v, out);
      for (const key of Object.keys(v)) {
        out[key] = this.#decodeWalk((v as Record<string, unknown>)[key], seen);
      }
      return out;
    }
    return v;
  }

  /** Actor terminated/crashed: ask every codec to clean up its resources (channels, listeners). */
  failAll(): void {
    for (const codec of this.#order) {
      codec.onRegistryFail?.(this.#codecState.get(codec));
    }
  }
}
