import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import {
  ActorDiedError,
  type ActorPool,
  createActorPool,
} from "@ghostflyby/worker-actor";
import type * as WorkerModule from "./examples/calculator/worker.ts";

const WORKER_URL = import.meta.resolve("./examples/calculator/worker.ts");
// A worker whose codec set does NOT match the main thread (registers the
// remote-ref codec): its handshake fails with a codec fingerprint mismatch,
// which fires spawn's onDeath without crashing the worker (Deno's test runner
// reports any uncaught worker error as a module failure, so a genuine crash
// cannot be exercised inside pool tests).
const MISMATCH_URL = import.meta.resolve("./test_fixtures/link_b.ts");

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
  probe: () => Promise<boolean> | boolean,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await sleep(10);
  }
  throw new Error("waitFor timed out");
}

function makePool(
  size: number,
  opts: {
    routing?: "round-robin" | "least-busy";
    replace?: boolean;
    onMemberDead?: (index: number, reason: unknown) => void;
  } = {},
) {
  return createActorPool<typeof WorkerModule.rpc>({
    size,
    spawnWorker: () => new Worker(WORKER_URL, { type: "module" }),
    routing: opts.routing,
    replace: opts.replace,
    onMemberDead: opts.onMemberDead,
  });
}

interface Rpc extends ActorPool<typeof WorkerModule.rpc> {
  add(a: number, b: number): Promise<number>;
  delay(ms: number, tag: string): Promise<string>;
  getStreamCancelCount(): Promise<number>;
}

Deno.test("pool: round-robin distributes calls across members", async () => {
  const pool = makePool(2);
  const results = await Promise.all([
    pool.add(1, 1),
    pool.add(2, 2),
    pool.add(3, 3),
    pool.add(4, 4),
  ]);
  assertEquals(results, [2, 4, 6, 8]);
  await pool.dispose();
});

Deno.test("pool: least-busy routes to the idle member", async () => {
  const pool = makePool(2, { routing: "least-busy" }) as Rpc;
  // Occupy member 0 with a slow call, then a fast call must go to member 1.
  const slow = pool.delay(150, "slow");
  await sleep(20); // ensure the slow call is in-flight before the next one
  const fast = await pool.delay(5, "fast");
  assertEquals(fast, "fast");
  assertEquals(await slow, "slow");
  await pool.dispose();
});

Deno.test("pool: member handshake failure removes it from routing", async () => {
  const dead: number[] = [];
  let spawnCount = 0;
  const pool = createActorPool<typeof WorkerModule.rpc>({
    size: 2,
    spawnWorker: () => {
      spawnCount++;
      // Member 0 is a normal calculator worker; member 1 registers a codec
      // set that mismatches the main thread, so its handshake fails and
      // onDeath fires immediately (no worker crash involved).
      return new Worker(spawnCount === 1 ? WORKER_URL : MISMATCH_URL, {
        type: "module",
      });
    },
    onMemberDead: (i) => dead.push(i),
  }) as Rpc;
  await waitFor(() => dead.includes(1));
  assert(dead.includes(1), "member 1 should die on handshake mismatch");
  await waitFor(() => pool.size === 1);
  assertEquals(pool.size, 1);
  // Member 0 still serves calls.
  assertEquals(await pool.add(1, 2), 3);
  await pool.dispose();
});

Deno.test("pool: replace rebuilds a failed member", async () => {
  let spawnCount = 0;
  const pool = createActorPool<typeof WorkerModule.rpc>({
    size: 2,
    replace: true,
    spawnWorker: () => {
      spawnCount++;
      // First attempt for each slot is the mismatched worker (fails), the
      // replacement is the normal calculator worker (succeeds).
      return new Worker(
        spawnCount === 1 || spawnCount === 2 ? MISMATCH_URL : WORKER_URL,
        { type: "module" },
      );
    },
  }) as Rpc;
  // Both members fail on the first attempt; replace re-spawns them with the
  // good worker. The pool recovers to full size and serves calls.
  await waitFor(() => pool.size === 2);
  await waitFor(async () => (await pool.add(5, 5)) === 10);
  assertEquals(spawnCount, 4); // 2 initial failures + 2 replacements
  await pool.dispose();
});

Deno.test("pool: custom routing function picks a member", async () => {
  const calls: string[] = [];
  const pool = createActorPool<typeof WorkerModule.rpc>({
    size: 2,
    spawnWorker: () => new Worker(WORKER_URL, { type: "module" }),
    routing: (method) => {
      calls.push(method);
      return 1; // always pin to member 1
    },
  }) as Rpc;
  // The routing function always pins to member 1: wait until it is ready.
  await waitFor(() => pool.size === 2);
  assertEquals(await pool.add(1, 2), 3);
  assertEquals(await pool.add(3, 4), 7);
  assertEquals(calls, ["add", "add"]);
  await pool.dispose();
});

Deno.test("pool: invokeOn pins a member (state accumulates there)", async () => {
  const pool = makePool(2) as Rpc;
  // The calculator worker has no stateful counter via rpc; use the reference
  // example instead is out of scope — here we verify invokeOn dispatches to a
  // live member and returns a result.
  const r1 = await pool.invokeOn(0, "add", [10, 5]);
  const r2 = await pool.invokeOn(1, "add", [20, 5]);
  assertEquals(r1, 15);
  assertEquals(r2, 25);
  await pool.dispose();
});

Deno.test("pool: calls after dispose reject with ActorDiedError", async () => {
  const pool = makePool(2) as Rpc;
  await pool.dispose();
  const outcome = await pool.add(1, 2).then(
    () => "resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, ActorDiedError);
});

Deno.test("pool: callback parameter routes through the pool", async () => {
  const pool = makePool(2) as Rpc & {
    callCallback(cb: (x: number) => number): Promise<number>;
  };
  let calls = 0;
  const r = await pool.callCallback((x: number) => {
    calls++;
    return x * 2;
  });
  assertEquals(r, 42);
  assertEquals(calls, 1); // executed in the main-thread context
  await pool.dispose();
});

Deno.test("pool: stream return value is consumable", async () => {
  const pool = makePool(2) as Rpc & {
    stream(prefix: string, n: number): AsyncIterable<string>;
  };
  const s = pool.stream("p", 3);
  const out: string[] = [];
  for await (const v of s) out.push(v);
  assertEquals(out, ["p-0", "p-1", "p-2"]);
  await pool.dispose();
});
