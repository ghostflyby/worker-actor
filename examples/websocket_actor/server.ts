// WebSocket actor server: upgrades a WS connection to a Transport and serves
// an RPC api over it. Any WS client (Deno, browser, Node) that speaks the
// frame protocol can call the api.
//
// Run server:  deno run --allow-net examples/websocket_actor/server.ts
// Run client:  deno run --allow-net examples/websocket_actor/client.ts
import { fromWebSocket } from "@ghostflyby/worker-actor";
import {
  makeRpcHandler,
  PayloadCodecRegistry,
} from "@ghostflyby/worker-actor/codec";
import { PROTOCOL_VERSION } from "../../core/protocol.ts";
import {
  abortSignalCodec,
  callbackCodec,
  errorCodec,
  iterableCodec,
} from "@ghostflyby/worker-actor/codecs";

// The api served over every WS connection.
const api = {
  echo(x: unknown): unknown {
    return x;
  },
  add(a: number, b: number): number {
    return a + b;
  },
  greet(name: string): string {
    return `hello ${name}`;
  },
};

// Each connection gets its own registry + handler. Register the same built-in
// codecs as spawn() so the handshake codec lists match (a mismatch kills the
// connection).
const registry = new PayloadCodecRegistry();
for (
  const codec of [iterableCodec, errorCodec, abortSignalCodec, callbackCodec]
) {
  if (!registry.has(codec.tag)) registry.register(codec);
}
const handler = makeRpcHandler(api, registry);

Deno.serve({ port: 8080 }, (req) => {
  if (req.headers.get("upgrade") !== "websocket") {
    return new Response("expected a websocket upgrade", { status: 400 });
  }
  const { socket, response } = Deno.upgradeWebSocket(req);
  const transport = fromWebSocket(socket); // WS → message-kind Transport
  socket.onmessage = (ev) => transport.deliver(ev.data);
  transport.onMessage((ev) => {
    const frame = ev.data as {
      type: string;
      id: number;
      method: string;
      args: unknown[];
    };
    if (frame.type === "request") {
      void handler(frame).then((res) => {
        transport.send({
          type: "response",
          id: res.id,
          ok: res.ok,
          value: (res as { value?: unknown }).value,
          error: (res as { error?: unknown }).error,
        });
      });
    }
  });
  // Handshake: spawn() (the unified actor entry) waits for this before
  // resolving — same frame the worker/process runtimes send. Send it once the
  // socket is OPEN (after the upgrade response is returned).
  socket.onopen = () => {
    transport.send({
      type: "handshake",
      version: PROTOCOL_VERSION,
      codecs: registry.tags,
      kind: transport.kind,
    });
  };
  return response;
});

console.log("websocket actor server listening on ws://localhost:8080");
