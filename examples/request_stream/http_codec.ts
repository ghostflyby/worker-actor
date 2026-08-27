/**
 * A composite codec: moves fetch Request/Response objects across actors by
 * moving their BODY (a ReadableStream) through the native transfer list, then
 * rebuilding the object around the received live stream.
 *
 * Deno streams are transfer-ONLY, not cloneable: postMessage without the
 * transfer list rejects them ("DataCloneError"), while listing them moves
 * ownership — the receiver gets a live stream at the payload position and the
 * sender's copy is spent (bodyUsed flips to true, further reads throw).
 * Verified on Deno 2.9.5 for MessageChannel and real Workers, nested at any
 * payload depth.
 *
 * Dual mode:
 *   - messageport transports (the default worker link): ctx.transfer carries
 *     the body; the wire holds it in place — zero plumbing, zero copies.
 *   - Mux transports (fork IPC / WebSocket / framed): there is no transfer
 *     list on those byte/message channels, so encode falls back to delegating
 *     the body to the built-in iterable codec's channel pump via
 *     ctx.registry.encode(...). Decode accepts both wire shapes.
 *
 * Byte-stream assumption: bodies are consumed as Uint8Array chunks (fetch spec
 * behavior); producers enqueueing raw strings will surface errors at read time.
 */

import {
  type Codec,
  CODEC_PLACEHOLDER_KEY,
  type DecodeContext,
  type EncodeContext,
} from "@ghostflyby/worker-actor/codec";

interface HttpPlaceholder {
  [CODEC_PLACEHOLDER_KEY]: "demo/http";
  kind: "request" | "response";
  /** Request-only wire fields. */
  url?: string;
  method?: string;
  /** Response-only wire fields. */
  status?: number;
  statusText?: string;
  /** Shared wire fields. */
  headers: [string, string][];
  /**
   * messageport mode: the body stream itself (natively transferred in place).
   * Mux mode: an iterable-codec placeholder. null for bodiless values.
   */
  body: unknown;
}

function headerEntries(h: Headers): [string, string][] {
  return [...h.entries()];
}

function encodeBody(
  value: Request | Response,
  ctx: EncodeContext,
): unknown {
  const body = value.body;
  if (!body) return null;
  if (ctx.transport.kind === "messageport") {
    // Native ownership transfer: keep the stream at its payload slot and list
    // it for the transfer that this very message is sent with.
    if (!ctx.transfer.includes(body)) ctx.transfer.push(body);
    return body;
  }
  // No native transfer list over Mux: delegate to the iterable pump instead.
  return ctx.registry.encode(body, ctx.transfer, ctx.transport);
}

function toByteStream(
  iterable: AsyncIterable<Uint8Array>,
): ReadableStream<Uint8Array> {
  // Adapter over the rebuilt remote iterable. The tempting shortcut
  // `new Response(iterable).body` works for plain reads, but its WebIDL
  // closeIterator bridge demands a spec-perfect iterator protocol; cancelling
  // such a stream throws ("return() did not return an object"). A hand-rolled
  // controller keeps early-return working end to end.
  const iterator = iterable[Symbol.asyncIterator]();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const next = await iterator.next();
        if (next.done) controller.close();
        else controller.enqueue(next.value);
      } catch (e) {
        controller.error(e);
      }
    },
    async cancel() {
      try {
        await iterator.return?.(undefined);
      } catch {
        // producer already gone — cancellation is best-effort
      }
    },
  });
}

function decodeBody(
  placeholder: unknown,
  ctx: DecodeContext,
): ReadableStream<Uint8Array> | null {
  if (placeholder === null) return null;
  // messageport mode: the wire already carries the live, natively transferred
  // stream — nothing to rebuild.
  if (placeholder instanceof ReadableStream) return placeholder;
  // Mux mode: expand the iterable-codec placeholder, then adapt.
  const iterable = ctx.registry.decode(
    placeholder,
    ctx.transport,
  ) as AsyncIterable<Uint8Array>;
  return toByteStream(iterable);
}

export const httpCodec: Codec<Request | Response> = {
  tag: "demo/http",

  matches(value): value is Request | Response {
    return value instanceof Request || value instanceof Response;
  },

  encode(value, ctx): HttpPlaceholder {
    if (value instanceof Request) {
      return {
        [CODEC_PLACEHOLDER_KEY]: "demo/http",
        kind: "request",
        url: value.url,
        method: value.method,
        headers: headerEntries(value.headers),
        body: encodeBody(value, ctx),
      };
    }
    return {
      [CODEC_PLACEHOLDER_KEY]: "demo/http",
      kind: "response",
      status: value.status,
      statusText: value.statusText,
      headers: headerEntries(value.headers),
      body: encodeBody(value, ctx),
    };
  },

  decode(raw, ctx): Request | Response {
    const p = raw as HttpPlaceholder;
    const body = decodeBody(p.body, ctx);
    if (p.kind === "request") {
      return new Request(p.url ?? "https://invalid/", {
        method: p.method ?? "GET",
        headers: p.headers,
        ...(body ? { body } : {}),
      });
    }
    return new Response(body, {
      status: p.status ?? 200,
      statusText: p.statusText ?? "",
      headers: p.headers,
    });
  },
};
