/**
 * Actor side of the Request-movement demo. Serves the HTTP-ish surface whose
 * methods accept and return real fetch objects; the httpCodec on both ends
 * makes the bodies stream instead of clone.
 *
 * Run from the repo root: `deno task demo:request`
 */
import { serveWorker } from "@ghostflyby/worker-actor";
import { httpCodec } from "./http_codec.ts";

const encoder = new TextEncoder();

export const rpc = {
  /** Consume a moved-in Request and answer with a rebuilt Response. */
  async echo(request: Request): Promise<Response> {
    const text = await request.text();
    return new Response(
      JSON.stringify({
        url: request.url,
        method: request.method,
        contentType: request.headers.get("content-type"),
        bytes: encoder.encode(text).length,
        text,
      }),
      {
        status: 202,
        statusText: "Accepted",
        headers: { "x-demo": "request-stream" },
      },
    );
  },

  /** Produce a Response whose body streams out after the call resolves. */
  poll(): Response {
    const ticks = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode("tick-1"));
        setTimeout(() => {
          controller.enqueue(encoder.encode("tick-2"));
          controller.close();
        }, 30);
      },
    });
    return new Response(ticks, {
      status: 200,
      headers: { "content-type": "text/plain" },
    });
  },
};

serveWorker(rpc, { codecs: [httpCodec] });
