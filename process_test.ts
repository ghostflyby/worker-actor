import { assertEquals } from "@std/assert";
import type * as ProcessWorker from "./test_fixtures/process_worker.ts";
import { spawnProcess } from "./spawn.ts";

// The process fixture serves remote-ref codec; every spawn must register it
// so the handshake codec lists match.
const refModule = await import("./examples/remote_ref/ref_codec.ts");
const REF_CODEC = [refModule.remoteRefCodec];

Deno.test("spawnProcess: RPC round-trip over fork IPC", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { permissions: { read: true }, codecs: REF_CODEC },
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
    { codecs: REF_CODEC },
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
    { signal: aborted, codecs: REF_CODEC },
  ).then(() => "resolved", (e) => `rejected:${e.name}`);
  assertEquals(result.startsWith("rejected"), true);
});

Deno.test("spawnProcess: AsyncIterable streams across processes (Mux token)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
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

Deno.test("spawnProcess: AbortSignal crosses processes and cancels work", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const controller = new AbortController();
    const running = actor.spin(5000, controller.signal);
    await new Promise((r) => setTimeout(r, 50));
    controller.abort();
    const iterations = await running;
    assertEquals(typeof iterations, "number");
    // The child saw the abort and stopped well before the 5s budget.
    assertEquals(iterations < 5000, true);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: a pre-aborted signal still cancels remote work (status frame not lost)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    // The signal is already aborted before the call: the abort-signal codec
    // sends its status frame immediately, which can race the placeholder
    // decode on a Mux transport. The rebuilt signal must still be aborted.
    const controller = new AbortController();
    controller.abort("already");
    const iterations = await actor.spin(5000, controller.signal);
    // The child saw the pre-aborted state and returned quickly (0 iterations).
    assertEquals(iterations, 0);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: callbacks cross processes and return results", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const result = await actor.apply((x: number) => x * 2, 21);
    assertEquals(result, 42);
  } finally {
    await actor.dispose();
  }
});

Deno.test("spawnProcess: remote references cross processes (Mux token)", async () => {
  const actor = await spawnProcess<typeof ProcessWorker.rpc>(
    "./test_fixtures/process_worker.ts",
    { codecs: REF_CODEC },
  );
  try {
    const counter = await actor
      .getCounter() as unknown as import("./examples/remote_ref/ref_codec.ts").RemoteRef<
        { inc(): number; get(): number }
      >;
    assertEquals(await counter.inc(), 1);
    assertEquals(await counter.inc(), 2);
    assertEquals(await counter.get(), 2);
  } finally {
    await actor.dispose();
  }
});
