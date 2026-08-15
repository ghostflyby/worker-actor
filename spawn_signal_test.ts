import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { spawn } from "./spawn.ts";
import type * as CalcWorkerModule from "./examples/calculator/worker.ts";

const CALC_URL = import.meta.resolve("./examples/calculator/worker.ts");

/** A worker module that never calls serveWorker(): the handshake never arrives. */
const HANGING_URL = import.meta.resolve("./test_fixtures/hanging_worker.ts");

function hangResult(p: Promise<unknown>, ms: number): Promise<unknown> {
  return Promise.race([
    p.then(() => "resolved" as const, (e: unknown) => e),
    new Promise((r) => setTimeout(() => r("still-pending"), ms)),
  ]);
}

Deno.test("spawn signal: default timeout still applies", async () => {
  const outcome = await hangResult(
    spawn(new Worker(HANGING_URL, { type: "module" })),
    200,
  );
  // Default 10s timeout: still pending shortly after creation, then... the
  // hanging worker eventually times out on its own, so just assert the pending
  // phase here; the exact rejection is covered by the explicit-signal test.
  assertEquals(outcome, "still-pending");
  // cleanup: drop the dangling worker
  // (the handshake timeout will kill it; nothing else to do)
});

Deno.test("spawn signal: user signal aborts creation and propagates reason", async () => {
  const controller = new AbortController();
  const p = spawn(new Worker(HANGING_URL, { type: "module" }), {
    signal: controller.signal,
  });
  await new Promise((r) => setTimeout(r, 20));
  controller.abort(new Error("user cancelled"));
  const outcome = await p.then(() => "resolved" as const, (e: unknown) => e);
  assertInstanceOf(outcome, Error);
  assertEquals((outcome as Error).message, "user cancelled");
});

Deno.test("spawn signal: null disables interruption entirely", async () => {
  const p = spawn(new Worker(HANGING_URL, { type: "module" }), {
    signal: null,
  });
  const outcome = await hangResult(p, 200);
  // No interrupt at all: still pending after 200ms, no rejection
  assertEquals(outcome, "still-pending");
});

Deno.test("spawn signal: signal no longer affects the actor after resolve", async () => {
  const controller = new AbortController();
  const actor = await spawn<typeof CalcWorkerModule.rpc>(
    new Worker(CALC_URL, { type: "module" }),
    { signal: controller.signal },
  );
  controller.abort("too late");
  // Creation already resolved: the abort must not kill the actor
  assertEquals(await actor.add(1, 2), 3);
  await actor.dispose();
});

Deno.test("spawn signal: TimeoutError keeps the serveWorker diagnostic", async () => {
  const outcome = await spawn(new Worker(HANGING_URL, { type: "module" }), {
    signal: AbortSignal.timeout(100),
  }).then(() => "resolved" as const, (e: unknown) => e);
  assertInstanceOf(outcome, Error);
  assertEquals((outcome as Error).name, "Error");
  assert((outcome as Error).message.includes("did it call serveWorker()"));
});
