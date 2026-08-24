import { assertEquals } from "@std/assert";
import { fromWebSocket } from "./core/transport.ts";
import { createRpcProxy, makeRpcHandler } from "./core/rpc.ts";
import { PayloadCodecRegistry } from "./core/codec.ts";

/**
 * Local WebSocket round-trip: a server and a client, each side wrapped in
 * fromWebSocket (message-kind Transport). The server serves an RPC api, the
 * client proxies calls to it — the same machinery as a process actor, over a
 * WebSocket connection.
 */

Deno.test("WebSocket: RPC round-trip over a message-kind transport", async () => {
  // Watchdog: fail if the test hangs.
  const watchdog = setTimeout(() => {
    throw new Error("websocket test hung");
  }, 5000);
  try {
    // Start a WS server that serves an api on its transport.
    const registry = new PayloadCodecRegistry();
    const serverApi = {
      echo(x: unknown): unknown {
        return x;
      },
      add(a: number, b: number): number {
        return a + b;
      },
    };
    const serverHandler = makeRpcHandler(serverApi, registry);

    const ac = new AbortController();
    const server = Deno.serve(
      { port: 0, signal: ac.signal, onListen: () => {} },
      (req) => {
        if (req.headers.get("upgrade") !== "websocket") {
          return new Response("no", { status: 400 });
        }
        const { socket, response } = Deno.upgradeWebSocket(req);
        const transport = fromWebSocket(socket);
        socket.onmessage = (ev) => {
          transport.deliver(ev.data);
        };
        transport.onMessage((ev) => {
          const frame = ev.data as {
            type: string;
            id: number;
            method: string;
            args: unknown[];
          };
          if (frame.type === "request") {
            void serverHandler(frame).then((res) => {
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
        return response;
      },
    );
    await new Promise<void>((r) => setTimeout(r, 50)); // let the server bind
    const { port } = server.addr;

    // Client: connect, wrap transport, proxy RPC calls.
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = (e) => reject(new Error("ws connect failed"));
    });
    const clientTransport = fromWebSocket(ws);
    ws.onmessage = (ev) => clientTransport.deliver(ev.data);

    const clientRegistry = new PayloadCodecRegistry();
    const proxy = createRpcProxy(clientRegistry, {
      send: (request, transfer) =>
        clientTransport.send({
          type: "request",
          id: request.id,
          method: request.method,
          args: request.args,
        }),
      transport: clientTransport,
    });
    // Feed main-channel responses into the proxy.
    clientTransport.onMessage((ev) => {
      const frame = ev.data as { type?: string };
      if (frame.type === "response") {
        proxy.deliver(frame as Parameters<typeof proxy.deliver>[0]);
      }
    });

    const echo = await proxy.call("echo", ["hello"]);
    assertEquals(echo, "hello");
    const sum = await proxy.call("add", [2, 3]);
    assertEquals(sum, 5);

    // Teardown.
    clientTransport.close();
    ws.close();
    ac.abort();
    await server.shutdown();
  } finally {
    clearTimeout(watchdog);
  }
});
