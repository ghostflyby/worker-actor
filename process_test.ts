import { assertEquals } from "@std/assert";
import type * as ProcessWorker from "./test_fixtures/process_worker.ts";
import { spawnProcess } from "./spawn.ts";

Deno.test("spawnProcess: RPC round-trip over fork IPC", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { permissions: { read: true } },
  );
  try {
    assertEquals(await actor.add(1, 2), 3);
    assertEquals(await actor.greet("world"), "hello world");
    const m = await actor.map();
    assertEquals(m.get("a"), 1);
    assertEquals(m.get("b"), 2);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: dispose terminates the child process", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
  );
  await actor.add(1, 1);
  await actor.dispose();
  // After dispose, further calls reject with ActorDiedError.
  await assertEquals(
    await actor.add(1, 1).then(() => "ok", () => "rejected"),
    "rejected",
  );
});

Deno.test("spawnProcess: signal aborts creation", async () => {
  const aborted = AbortSignal.timeout(1);
  await aborted; // let it fire
  const result = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { signal: aborted },
  ).then(() => "resolved", (e) => `rejected:${e.name}`);
  assertEquals(result.startsWith("rejected"), true);
});

Deno.test("spawnProcess: AsyncIterable streams across processes (Mux token)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
  );
  try {
    const stream = await Promise.race([
      actor.count(5),
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("count() timeout")), 2000)
      ),
    ]) as AsyncIterable<number>;
    const got: number[] = [];
    for await (const n of stream) got.push(n);
    assertEquals(got, [0, 1, 2, 3, 4]);
  } finally {
    await actor.dispose();
  }
});
