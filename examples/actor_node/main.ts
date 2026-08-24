// Multi-actor node: one process serves several named actors (model B). The
// node's transport can also be used to open additional actor channels directly
// via connectActor — no new spawn needed.
//
// Run: deno run --allow-read --allow-run --allow-env examples/actor_node/main.ts
import type * as NodeModule from "./node.ts";
import { spawnNode } from "@ghostflyby/worker-actor";
import { connectActor } from "@ghostflyby/worker-actor/codec";
import { createRpcProxy } from "@ghostflyby/worker-actor/codec";
import { PayloadCodecRegistry } from "@ghostflyby/worker-actor/codec";

const node = await spawnNode<typeof NodeModule.actors>(
  "./examples/actor_node/node.ts",
);

try {
  // 1. The node exposes its named actors directly.
  console.log("counter.inc(1)    =", await node.counter.inc(1));
  console.log("greeter.hello(x)  =", await node.greeter.hello("x"));

  // 2. Open an ADDITIONAL counter channel on the node's transport directly
  //    (bypassing spawnNode's surface) and RPC to it via createRpcProxy.
  const { channel, transport } = connectActor(node.transport, "counter");
  const registry = new PayloadCodecRegistry();
  const proxy = createRpcProxy(registry, {
    send: (request, transfer) =>
      channel.send(
        {
          type: "request",
          id: request.id,
          method: request.method,
          args: request.args,
        },
        transfer,
      ),
    transport,
  });
  // Feed responses back into the proxy.
  channel.onMessage((message) => {
    const frame = message as { type: string };
    if (frame.type === "response") proxy.deliver(message as never);
  });
  console.log("connectActor inc  =", await proxy.call("inc", [10])); // 11 (same shared counter)
} finally {
  await node.dispose();
}
