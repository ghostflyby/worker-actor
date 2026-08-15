import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { ActorDiedError, RemoteError } from "./core/protocol.ts";
import { spawn } from "./spawn.ts";
import type * as WorkerModule from "./examples/calculator/worker.ts";

function makeActor() {
  return spawn<typeof WorkerModule.rpc>(
    new Worker(
      import.meta.resolve("./examples/calculator/worker.ts"),
      { type: "module" },
    ),
  );
}

Deno.test("basic typed RPC", async () => {
  const actor = await makeActor();
  assertEquals(await actor.add(1, 2), 3);
  assertEquals(await actor.add(5, -3), 2);
  await actor.dispose();
});

Deno.test("deeply nested value transfer via structured clone", async () => {
  const actor = await makeActor();
  const report = await actor.report({ id: "u-42", name: "Ada" }) as {
    generatedAt: Date;
    user: { id: string; name: string };
    balances: Map<string, number>;
    tags: Set<string>;
    samples: Uint8Array;
    nested: { level1: { level2: { level3: { at: string } } } };
  };
  assertInstanceOf(report.generatedAt, Date);
  assertEquals(report.user.name, "Ada");
  assertEquals(report.balances.get("bank"), 250.5);
  assert(report.tags.has("premium"));
  assertEquals([...report.samples], [1, 2, 3]);
  assertEquals(report.nested.level1.level2.level3.at, "u-42");
  await actor.dispose();
});

Deno.test("out-of-order responses resolve to their own request", async () => {
  const actor = await makeActor();
  const [slow, fast] = await Promise.all([
    actor.delay(120, "slow"),
    actor.delay(10, "fast"),
  ]);
  assertEquals(fast, "fast");
  assertEquals(slow, "slow");
  await actor.dispose();
});

Deno.test("worker error propagates as RemoteError with name/message/stack", async () => {
  const actor = await makeActor();
  const outcome = await actor.divide(1, 0).then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, RemoteError);
  assertEquals(outcome.name, "RangeError");
  assertEquals(outcome.message, "division by zero");
  assert(typeof outcome.stack === "string");
  await actor.dispose();
});

Deno.test("calls after dispose reject with ActorDiedError", async () => {
  const actor = await makeActor();
  await actor.dispose();
  const outcome = await actor.add(1, 2).then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, ActorDiedError);
});

Deno.test("unknown method yields RemoteError", async () => {
  const actor = await makeActor();
  // A call that doesn't exist in the type system: cast around static checking to simulate the runtime edge case
  const outcome = await (actor as unknown as { noSuch(): Promise<unknown> })
    .noSuch().then(
      () => "unexpectedly resolved" as const,
      (e: unknown) => e,
    );
  assertInstanceOf(outcome, RemoteError);
  assert(outcome.message.includes("noSuch"));
  await actor.dispose();
});

// —— AsyncIterable 传输 ——

async function* numbers(...vals: number[]): AsyncGenerator<number> {
  for (const v of vals) yield v;
}

Deno.test("async iterable args are transferred and consumed in worker", async () => {
  const actor = await makeActor();
  assertEquals(await actor.sumIterable(numbers(1, 2, 3)), 6);
  await actor.dispose();
});

Deno.test("async iterable nested inside object payload", async () => {
  const actor = await makeActor();
  assertEquals(await actor.sumNested({ items: numbers(10, 20) }), 30);
  await actor.dispose();
});

Deno.test("async iterable return value is rebuilt on main side", async () => {
  const actor = await makeActor();
  const stream = await actor.stream("chunk", 3);
  const out: string[] = [];
  for await (const s of stream) out.push(s);
  assertEquals(out, ["chunk-0", "chunk-1", "chunk-2"]);
  await actor.dispose();
});

Deno.test("producer error mid-stream propagates as RemoteError", async () => {
  const actor = await makeActor();
  const out: number[] = [];
  const outcome = await (async () => {
    try {
      for await (const v of await actor.failingStream()) out.push(v);
      return "resolved" as const;
    } catch (e) {
      return e as unknown;
    }
  })();
  assertEquals(out, [1, 2]);
  assertInstanceOf(outcome, RemoteError);
  assert(outcome.message.includes("stream exploded"));
  await actor.dispose();
});

Deno.test("early return cancels stream and runs producer finally", async () => {
  const actor = await makeActor();
  const inf = await actor.infiniteStream();
  const it = inf[Symbol.asyncIterator]();
  const first = await it.next();
  assertEquals(first.done, false);
  await it.return?.();
  // cancel travels on its own stream channel with no ordering guarantee against the main
  // channel; poll until the producer's finally has run
  const deadline = Date.now() + 2_000;
  let cancels = 0;
  while (Date.now() < deadline) {
    cancels = await actor.getStreamCancelCount();
    if (cancels > 0) break;
    await new Promise((r) => setTimeout(r, 10));
  }
  assertEquals(cancels, 1);
  await actor.dispose();
});

Deno.test("disposing actor mid-stream rejects pending pulls", async () => {
  const actor = await makeActor();
  const inf = await actor.infiniteStream();
  const it = inf[Symbol.asyncIterator]();
  const first = it.next(); // pending on the first element, then dispose immediately
  await actor.dispose();
  const outcome = await first.then(
    () => "resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, RemoteError);
  assertEquals(outcome.name, "ActorDiedError");
});
