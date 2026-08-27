/**
 * A composite codec: moves fetch Request/Response objects across actors by
 * delegating their BODY (a ReadableStream) to the built-in iterable machinery,
 * then rebuilding the object around the received stream.
 *
 * Why delegation instead of native support?
 *   Deno's structured clone cannot carry a bare ReadableStream over
 *   postMessage ("DataCloneError: Cannot clone object of unsupported type"),
 *   so there is nothing to piggyback on. But every stream IS an AsyncIterable,
 *   and the library's iterable codec already knows how to pump one across any
 *   transport (MessagePort pair or Mux token) with lazy start, backpressure,
 *   early-return cancellation and death cleanup. So this codec treats the body
 *   as just another payload value: ctx.registry.encode(body, ...) hands it to
 *   whatever codec claims it, and the placeholder nests inside ours.
 *
 * Move semantics: iterating request.body locks and drains the sender's stream,
 * so once the other side starts reading, the original object is spent — the
 * envelope (url/method/headers) is cloned metadata, the bytes are moved.
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
  /** Encoded iterable-codec placeholder, or null for bodiless values. */
  body: unknown;
}

function headerEntries(h: Headers): [string, string][] {
  return [...h.entries()];
}

function encodeBody(
  value: Request | Response,
  ctx: EncodeContext,
): unknown {
  if (value.body === null) return null;
  // Delegate to the registry walk: the built-in iterable codec matches the
  // stream (ReadableStream is an AsyncIterable) and owns the channel setup.
  return ctx.registry.encode(value.body, ctx.transfer, ctx.transport);
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
