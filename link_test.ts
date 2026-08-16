import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { link, RemoteError, spawn } from "@ghostflyby/worker-actor";
import { remoteRefCodec } from "./examples/remote_ref/ref_codec.ts";
import type * as LinkBModule from "./test_fixtures/link_b.ts";
import type * as LinkCModule from "./test_fixtures/link_c.ts";

const B_URL = import.meta.resolve("./test_fixtures/link_b.ts");
const C_URL = import.meta.resolve("./test_fixtures/link_c.ts");

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Spawn B and C, then link them directly (bypassing the main thread). */
async function spawnLinked(label = "b-c") {
  // The handshake is not buffered: spawn() must be called right after `new
  // Worker(...)` — an awaiting gap (e.g. spawning the other worker first) would
  // let the handshake arrive before the onmessage handler is set, and it is lost.
  const workerB = new Worker(B_URL, { type: "module" });
  // The main thread registers remote-ref too: handshake fingerprints must match,
  // and B/C may also hand references back over the main RPC channel.
  const actorB = await spawn<typeof LinkBModule.rpc>(workerB, {
    codecs: [remoteRefCodec],
  });
  const workerC = new Worker(C_URL, { type: "module" });
  const actorC = await spawn<typeof LinkCModule.rpc>(workerC, {
    codecs: [remoteRefCodec],
  });
  const unlink = link(workerB, workerC, label);
  return { workerB, workerC, actorB, actorC, unlink };
}

async function waitFor(
  probe: () => Promise<boolean>,
  timeoutMs = 2_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe()) return true;
    await sleep(10);
  }
  return false;
}

Deno.test("link: reference handed B→C bypasses the main thread and is callable", async () => {
  const { actorB, actorC } = await spawnLinked();
  await actorB.createCounterAndSendToC();

  const gotRef = await waitFor(() => actorC.getLastIsRef());
  assert(
    gotRef,
    "C should receive the reference as a proxy (not a plain value)",
  );

  // the call executes in B (owner), over the direct link
  assertEquals(await actorC.callLastIncrement(), 1);
  assertEquals(await actorC.callLastIncrement(), 2);
  // nothing disposed yet: the reference is still alive on C's side
  assertEquals(await actorB.getDisposedCount(), 0);

  await actorB.dispose();
  await actorC.dispose();
});

Deno.test("link: bidirectional — C hands its own object back to B over the same link", async () => {
  const { actorB, actorC } = await spawnLinked();
  await actorB.createCounterAndSendToC();
  await waitFor(() => actorC.getLastIsRef());

  await actorC.sendGreeterToB();
  const gotFromC = await waitFor(() => actorB.gotFromC());
  assert(gotFromC, "B should receive C's object over the same link");

  // call executes in C (owner of the greeter object)
  assertEquals(await actorB.callLastFromC("world"), "hello world from C");

  await actorB.dispose();
  await actorC.dispose();
});

Deno.test("link: unlink closes the channel; further sends are inert", async () => {
  const { actorB, actorC, unlink } = await spawnLinked();
  await actorB.createCounterAndSendToC();
  await waitFor(() => actorC.getLastIsRef());

  unlink();
  // sending after close must not throw (channel.send is inert once closed)
  const outcome = await actorB.createCounterAndSendToC().then(
    () => "resolved" as const,
    (e: unknown) => e,
  );
  assertEquals(outcome, "resolved");
  // the closed link delivered nothing new: C still sees the first value
  assertEquals(await actorC.callLastIncrement(), 1);

  await actorB.dispose();
  await actorC.dispose();
});

// —— Direct peer RPC over the link ——
// Contract types are exported by the fixture defining the surface; the caller
// imports them type-only (no runtime import cycle between workers).

Deno.test("link rpc: C calls B's served surface directly", async () => {
  const { actorB, actorC } = await spawnLinked();
  await actorC.callBEcho("hi");
  assertEquals(await actorC.callBEcho("hi"), "echo:hi");
  await actorB.dispose();
  await actorC.dispose();
});

Deno.test("link rpc: peer error propagates as RemoteError", async () => {
  const { actorB, actorC } = await spawnLinked();
  const outcome = await actorC.callBBoom().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, RemoteError);
  assertEquals(outcome.name, "RangeError");
  assert(outcome.message.includes("peer boom"));
  await actorB.dispose();
  await actorC.dispose();
});

Deno.test("link rpc: served surface hides management methods", async () => {
  const { actorB, actorC } = await spawnLinked();
  const outcome = await actorC.callBMissing().then(
    () => "unexpectedly resolved" as const,
    (e: unknown) => e,
  );
  assertInstanceOf(outcome, RemoteError);
  assert(outcome.message.includes("getDisposedCount"));
  await actorB.dispose();
  await actorC.dispose();
});

Deno.test("link rpc: bidirectional — B calls C's served surface", async () => {
  const { actorB, actorC } = await spawnLinked();
  assertEquals(await actorB.callCPing(), "pong");
  await actorB.dispose();
  await actorC.dispose();
});

// —— Reference round-trip over the link: B→C→B restores at the owner ——

Deno.test("link ref round-trip: B hands shared ref to C, C hands it back, B restores", async () => {
  const { actorB, actorC } = await spawnLinked();
  await actorB.sendSharedToC();
  const gotRef = await waitFor(() => actorC.getLastIsRef());
  assert(gotRef, "C should receive the reference over the link");
  assertEquals(await actorC.callLastIncrement(), 1); // executes in B

  // C hands the same reference back to B over the link → B restores it locally.
  assertEquals(await actorC.returnSharedToB(), "local");

  // B still owns the same object: a fresh ref round-trips again and increments.
  await actorB.sendSharedToC();
  await waitFor(() => actorC.getLastIsRef());
  assertEquals(await actorC.callLastIncrement(), 2);

  await actorB.dispose();
  await actorC.dispose();
});
