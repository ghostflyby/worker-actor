import { assertEquals } from "@std/assert";
import type * as NodeWorker from "./test_fixtures/node_worker.ts";
import { spawnNode } from "./spawn.ts";

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
