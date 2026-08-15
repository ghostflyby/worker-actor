import { assertEquals } from "@std/assert";
import { spawn } from "./spawn.ts";
import {
  type RemoteRef,
  remoteRefCodec,
} from "./examples/remote_ref/ref_codec.ts";
import type * as YieldModule from "./test_fixtures/yield_worker.ts";

const URL = import.meta.resolve("./test_fixtures/yield_worker.ts");

function makeActor() {
  return spawn<typeof YieldModule.rpc>(
    new Worker(URL, { type: "module" }),
    { codecs: [remoteRefCodec] },
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface YieldRef {
  longIo(tag: string, ms: number): Promise<string>;
  ping(tag: string): Promise<string>;
  doubleYield(tag: string): Promise<string>;
}

Deno.test("per-ref serialization: calls on one reference never interleave", async () => {
  const actor = await makeActor();
  const ref = await actor.make() as RemoteRef<YieldRef>;
  // Same reference: two calls. Per-ref queue guarantees the first completes
  // before the second starts.
  await ref.ping("a");
  await ref.ping("b");
  assertEquals(await actor.getLog(), ["a:ping", "b:ping"]);
  await actor.dispose();
});

Deno.test("actorYield: long IO releases the queue (next call is served during the wait)", async () => {
  const actor = await makeActor();
  const ref = await actor.make() as RemoteRef<YieldRef>;
  // Start a long IO call, then a quick ping on the SAME reference.
  const slow = ref.longIo("slow", 80);
  await sleep(10); // let the slow call start and yield
  const fast = await ref.ping("fast"); // served while slow is waiting
  assertEquals(fast, "fast:pong");
  assertEquals(await slow, "slow:done");
  const log = await actor.getLog();
  // The ping ran between slow:start and slow:done — the queue was released.
  assertEquals(log.indexOf("slow:start") < log.indexOf("fast:ping"), true);
  assertEquals(log.indexOf("fast:ping") < log.indexOf("slow:done"), true);
  await actor.dispose();
});

Deno.test("actorYield: the caller's await resolves only after the continuation", async () => {
  const actor = await makeActor();
  const ref = await actor.make() as RemoteRef<YieldRef>;
  // doubleYield yields twice; the caller awaits the final result.
  const result = await ref.doubleYield("dy");
  assertEquals(result, "dy:end");
  const log = await actor.getLog();
  assertEquals(log, ["dy:first", "dy:second", "dy:third"]);
  await actor.dispose();
});

Deno.test("actorYield on the main channel degrades to a plain await", async () => {
  const actor = await makeActor();
  const r = await actor.mainYield(5);
  assertEquals(r, "main-yield");
  await actor.dispose();
});

Deno.test("per-ref queues are independent across references", async () => {
  const actor = await makeActor();
  const refA = await actor.make() as RemoteRef<YieldRef>;
  const refB = await actor.make() as RemoteRef<YieldRef>;
  // Long IO on A; B's call runs without waiting for A.
  const slowA = refA.longIo("A", 80);
  await sleep(10);
  const fastB = await refB.ping("B"); // independent queue: no wait
  assertEquals(fastB, "B:pong");
  assertEquals(await slowA, "A:done");
  await actor.dispose();
});
