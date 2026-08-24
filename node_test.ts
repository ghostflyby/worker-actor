import { assertEquals } from "@std/assert";
import type * as NodeWorker from "./test_fixtures/node_worker.ts";
import { spawnNode } from "./spawn.ts";
import { connectActor, openNodeActor } from "./core/connect.ts";
import { createRpcProxy, makeRpcHandler } from "./core/rpc.ts";
import { PayloadCodecRegistry } from "./core/codec.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";
import { callbackCodec } from "./core/codecs/callback.ts";
import { messageTransport } from "./core/transport.ts";
import type { Channel } from "./core/channel.ts";
import type { Transport } from "./core/transport.ts";
import type { RpcProxy } from "./core/rpc.ts";

/** A callable RPC proxy over an actor channel opened via connectActor/openNodeActor. */
function rpcProxyFor(
  opened: { channel: Channel; transport: Transport },
): RpcProxy {
  const registry = new PayloadCodecRegistry();
  for (
    const codec of [iterableCodec, errorCodec, abortSignalCodec, callbackCodec]
  ) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  const proxy = createRpcProxy(registry, {
    send: (request, transfer) =>
      opened.channel.send(
        {
          type: "request",
          id: request.id,
          method: request.method,
          args: request.args,
        },
        transfer,
      ),
    isDead: () => opened.channel.closed,
    transport: opened.transport,
  });
  opened.channel.onMessage((message) => {
    const frame = message as { type?: string };
    if (frame.type === "response") proxy.deliver(frame as never);
  });
  return proxy;
}

Deno.test("spawnNode: multiple named actors on one node process", async () => {
  const node = await spawnNode<typeof NodeWorker.actors>(
    "./test_fixtures/node_worker.ts",
  );
  try {
    assertEquals(await node.counter.inc(1), 2);
    assertEquals(await node.counter.inc(10), 11);
    assertEquals(await node.counter.reset(), 0);
    assertEquals(await node.greeter.hello("world"), "hi world");
  } finally {
    await node.dispose();
  }
});

Deno.test("spawnNode: dispose closes the node", async () => {
  const node = await spawnNode<typeof NodeWorker.actors>(
    "./test_fixtures/node_worker.ts",
  );
  await node.counter.inc(0);
  await node.dispose();
  const result = await node.counter
    .inc(1)
    .then(() => "ok", () => "rejected");
  assertEquals(result, "rejected");
});

Deno.test(
  "spawnNode: connectActor opens the SECOND actor directly over the node transport",
  async () => {
    const node = await spawnNode<typeof NodeWorker.actors>(
      "./test_fixtures/node_worker.ts",
    );
    try {
      // The surface already opened counter+greeter. Open a NEW channel to the
      // greeter directly on the node transport (bypassing the surface), using
      // the same __open-actor protocol frame serveNode understands.
      const opened = openNodeActor(node.transport, "greeter");
      const greeter = rpcProxyFor(opened);
      assertEquals(await greeter.call("hello", ["connect"]), "hi connect");
      // The actor is stateful per call site: a fresh channel serves the same
      // api, and the pre-opened surface channel still works.
      assertEquals(await node.greeter.hello("surface"), "hi surface");
    } finally {
      await node.dispose();
    }
  },
);

Deno.test(
  "connectActor: single-actor transport needs no name (open + serve the api)",
  async () => {
    // Two message-kind transports wired back-to-back (fork-IPC style). One end
    // acts as the single-actor runtime (serves the api on any channel the peer
    // opens), the other is the connectActor client — no __open-actor name is
    // needed because the address is just the transport.
    const client = messageTransport({ send: (m) => server.deliver(m) });
    const server = messageTransport({ send: (m) => client.deliver(m) });
    const registry = new PayloadCodecRegistry();
    for (
      const codec of [
        iterableCodec,
        errorCodec,
        abortSignalCodec,
        callbackCodec,
      ]
    ) {
      if (!registry.has(codec.tag)) registry.register(codec);
    }
    const handler = makeRpcHandler(
      {
        add: (a: number, b: number) => a + b,
      } as import("./worker_runtime.ts").WorkerApi,
      registry,
      server,
    );
    server.onChannel((channel) => {
      channel.onMessage(async (message) => {
        const frame = message as { type?: string };
        if (frame.type !== "request") return;
        const res = await handler(frame as never);
        if (res.ok) {
          channel.send(
            { type: "response", id: res.id, ok: true, value: res.value },
            res.transfer,
          );
        } else {
          channel.send(
            { type: "response", id: res.id, ok: false, error: res.error },
          );
        }
      });
    });

    const opened = connectActor(client);
    const proxy = rpcProxyFor(opened);
    assertEquals(await proxy.call("add", [1, 2]), 3);
    assertEquals(await proxy.call("add", [40, 2]), 42);
    client.close();
    server.close();
  },
);
