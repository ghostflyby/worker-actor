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

import type { Channel } from "./channel.ts";
import type { Transport } from "./transport.ts";

export interface EncodeContext {
  /** structured clone transfer list: ports etc. created by codecs are pushed here. */
  transfer: Transferable[];
  seen: WeakMap<object, unknown>;
  /** Per-codec state slot held by the registry; codecs read/write it themselves. */
  codecState: Map<Codec, unknown>;
  /**
   * The registry driving this encode. Codecs use it to recurse into nested
   * payloads (e.g. frames on their own channel whose args/results may contain
   * streams or other codec values) and to track channels for failAll cleanup.
   */
  registry: PayloadCodecRegistry;
  /**
   * The transport this encode runs over. Codecs that need a logical channel
   * (streams, abort signals, by-ref references) use transport.openChannel()
   * — via core/channel.ts openChannel(ctx) — instead of assuming they can
   * transfer a MessagePort. Defaults to a messageport-type transport so
   * existing codecs keep working without a transport in context.
   */
  transport: Transport;
}

export interface DecodeContext {
  seen: WeakMap<object, unknown>;
  codecState: Map<Codec, unknown>;
  registry: PayloadCodecRegistry;
  /** The transport this decode runs over; codecs use it to open/connect sub-channels. */
  transport: Transport;
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
  #channels = new Set<Channel>();

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

  /**
   * Track a channel opened by a codec so failAll() closes it when the actor
   * dies. close() is idempotent, so a channel that already ended is a no-op.
   */
  registerChannel(channel: Channel): void {
    this.#channels.add(channel);
  }

  /**
   * Remove a channel from failAll tracking. A codec calls this when its
   * channel closes (via onClosed), so the registry no longer pins the
   * channel's closure chain once it is done.
   */
  unregisterChannel(channel: Channel): void {
    this.#channels.delete(channel);
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
  encode(
    value: unknown,
    transfer: Transferable[],
    transport?: Transport,
  ): unknown {
    return this.#encodeWalk(
      value,
      transfer,
      transport ?? defaultMessagePortTransport(),
      new WeakMap(),
    );
  }

  #encodeWalk(
    v: unknown,
    transfer: Transferable[],
    transport: Transport,
    seen: WeakMap<object, unknown>,
  ): unknown {
    // Functions are objects too: consult the seen map for them, and run the
    // codec matches BEFORE the primitive short-circuit — the callback codec
    // turns bare functions into byref references, so they must not fall
    // through to structured clone (which throws DataCloneError for functions).
    if (v !== null && (typeof v === "object" || typeof v === "function")) {
      const cached = seen.get(v as object);
      if (cached !== undefined) return cached;
    }
    // Custom codecs take precedence over container recursion: the first match takes over.
    for (const codec of this.#order) {
      if (codec.matches(v)) {
        const placeholder = codec.encode(v, {
          transfer,
          seen,
          codecState: this.#codecState,
          registry: this,
          transport,
        });
        if (v !== null && (typeof v === "object" || typeof v === "function")) {
          seen.set(v as object, placeholder);
        }
        return placeholder;
      }
    }
    if (v === null || typeof v !== "object") return v;
    if (Array.isArray(v)) {
      const out = new Array<unknown>(v.length);
      seen.set(v, out);
      for (let i = 0; i < v.length; i++) {
        out[i] = this.#encodeWalk(v[i], transfer, transport, seen);
      }
      return out;
    }
    if (v instanceof Map) {
      const out = new Map<unknown, unknown>();
      seen.set(v, out);
      for (const [k, val] of v) {
        out.set(
          this.#encodeWalk(k, transfer, transport, seen),
          this.#encodeWalk(val, transfer, transport, seen),
        );
      }
      return out;
    }
    if (v instanceof Set) {
      const out = new Set<unknown>();
      seen.set(v, out);
      for (const val of v) {
        out.add(this.#encodeWalk(val, transfer, transport, seen));
      }
      return out;
    }
    if (isPlainObject(v)) {
      const out: Record<string, unknown> = {};
      seen.set(v, out);
      for (const key of Object.keys(v)) {
        out[key] = this.#encodeWalk(
          (v as Record<string, unknown>)[key],
          transfer,
          transport,
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
    return this.#decodeWalk(
      value,
      defaultMessagePortTransport(),
      new WeakMap(),
    );
  }

  #decodeWalk(
    v: unknown,
    transport: Transport,
    seen: WeakMap<object, unknown>,
  ): unknown {
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
        registry: this,
        transport,
      });
      seen.set(v, decoded);
      return decoded;
    }
    if (Array.isArray(v)) {
      const out = new Array<unknown>(v.length);
      seen.set(v, out);
      for (let i = 0; i < v.length; i++) {
        out[i] = this.#decodeWalk(v[i], transport, seen);
      }
      return out;
    }
    if (v instanceof Map) {
      const out = new Map<unknown, unknown>();
      seen.set(v, out);
      for (const [k, val] of v) {
        out.set(
          this.#decodeWalk(k, transport, seen),
          this.#decodeWalk(val, transport, seen),
        );
      }
      return out;
    }
    if (v instanceof Set) {
      const out = new Set<unknown>();
      seen.set(v, out);
      for (const val of v) out.add(this.#decodeWalk(val, transport, seen));
      return out;
    }
    if (isPlainObject(v)) {
      const out: Record<string, unknown> = {};
      seen.set(v, out);
      for (const key of Object.keys(v)) {
        out[key] = this.#decodeWalk(
          (v as Record<string, unknown>)[key],
          transport,
          seen,
        );
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
    for (const channel of this.#channels) channel.close();
    this.#channels.clear();
  }
}

/**
 * Default transport for codec encode/decode paths that do not carry a
 * transport in context (e.g. decode in the RPC layer). A messageport-type
 * no-op transport: existing codecs that only use openChannel/transfer keep
 * working unchanged; a transport-aware codec only activates on a real
 * transport.
 */
let defaultTransport: Transport | undefined;
function defaultMessagePortTransport(): Transport {
  if (!defaultTransport) {
    const { port2 } = new MessageChannel();
    // Keep port2 referenced so the port pair stays open.
    void port2;
    defaultTransport = {
      kind: "messageport",
      send() {},
      onMessage() {},
      openChannel() {
        const { port1: p1, port2: p2 } = new MessageChannel();
        return {
          channel: {
            closed: false,
            port: p1,
            kind: "messageport",
            send() {},
            onMessage() {},
            close() {
              p1.close();
            },
          },
          token: p2,
        };
      },
      onChannel() {},
      close() {},
    };
  }
  return defaultTransport;
}
