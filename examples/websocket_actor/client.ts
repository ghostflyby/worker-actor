// WebSocket actor client: connect to the WS server, wrap the socket as a
// Transport, and spawn() an actor on it — the SAME unified entry point as a
// local Worker or a child process. The server's api is not statically known
// across the network, so the remote type is declared inline (or imported from
// a shared contract module).
//
// Run server first:  deno run --allow-net examples/websocket_actor/server.ts
// Run client:        deno run --allow-net examples/websocket_actor/client.ts
import { fromWebSocket, spawn } from "@ghostflyby/worker-actor";

interface RemoteServerApi {
  echo(x: unknown): Promise<unknown>;
  add(a: number, b: number): Promise<number>;
  greet(name: string): Promise<string>;
}

const ws = new WebSocket("ws://localhost:8080");
await new Promise<void>((resolve, reject) => {
  ws.onopen = () => resolve();
  ws.onerror = () => reject(new Error("ws connect failed"));
});

const transport = fromWebSocket(ws); // WS → message-kind Transport
ws.onmessage = (ev) => transport.deliver(ev.data);

// spawn() accepts the Transport directly — same as a Worker or process actor.
const actor = await spawn<RemoteServerApi>(transport);
console.log("echo(hello)  =", await actor.echo("hello"));
console.log("add(2, 3)    =", await actor.add(2, 3));
console.log("greet(world) =", await actor.greet("world"));

await actor.dispose();
ws.close();
