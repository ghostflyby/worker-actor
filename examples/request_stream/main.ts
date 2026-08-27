/**
 * Main-thread demo of moving fetch Request/Response across workers.
 *
 * `deno run --allow-read examples/request_stream/main.ts` (or deno task demo:request)
 *
 * What to watch for:
 *   - Types stay exact: with the codec tuple passed `as const`, the proxy
 *     surface really is (Request) => Promise<Response> — no casts anywhere.
 *   - The outgoing Request is MOVED, not copied: after the call the sender's
 *     body is spent, and reading it again fails.
 *   - The returned Response resolves eagerly while its body keeps streaming;
 *     the two ticks arrive over the live stream.
 */
import type * as WorkerModule from "./worker.ts";
import { spawn } from "@ghostflyby/worker-actor";
import { httpCodec } from "./http_codec.ts";

const actor = await spawn<typeof WorkerModule.rpc>(
  new Worker(import.meta.resolve("./worker.ts"), { type: "module" }),
  { codecs: [httpCodec] as const },
);

// —— Request → worker: body streams over, original is consumed by moving ——
const chunks = new ReadableStream<Uint8Array>({
  start(controller) {
    const encoder = new TextEncoder();
    controller.enqueue(encoder.encode('{"hello":"worker"}'));
    controller.close();
  },
});
const request = new Request("https://demo.local/upload", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: chunks,
});

const response = await actor.echo(request);
console.log("echo response:", response.status, response.statusText);
console.log("echo headers:  x-demo =", response.headers.get("x-demo"));
const meta = await response.json();
console.log("worker saw:   ", meta);

try {
  await request.text();
  console.log("UNEXPECTED: original request was still readable");
} catch (e) {
  console.log(
    `original request moved → ${(e as Error).name}: ${
      (e as Error).message.slice(0, 60)
    }…`,
  );
}

// —— Response → main: eager result, lazily streamed body ——
const polled = await actor.poll();
console.log("poll status:  ", polled.status);
const reader = polled.body!.getReader();
for (let i = 0; i < 2; i++) {
  const { value, done } = await reader.read();
  if (done) break;
  console.log(`tick ${i + 1}:      `, new TextDecoder().decode(value));
}
await reader.cancel();

await actor.dispose();
