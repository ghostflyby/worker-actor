import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import { spawn } from "./spawn.ts";
import {
  actionFor,
  argPolicyFor,
  collectMoveArgs,
  collectMoveReturn,
  type TransferArgs,
} from "./core/transfer.ts";
import { messageTransport } from "./core/transport.ts";
import type * as WorkerModule from "./test_fixtures/transfer_worker.ts";

const WORKER_URL = import.meta.resolve("./test_fixtures/transfer_worker.ts");

type TransferApi = typeof WorkerModule.rpc;

function makeActor(transferArgs?: TransferArgs) {
  return spawn<TransferApi>(
    new Worker(WORKER_URL, { type: "module" }),
    { transferArgs },
  );
}

Deno.test("transfer policy resolves shorthand, defaults, and slot precedence", () => {
  assertEquals(actionFor(undefined, "x"), "clone");
  assertEquals(actionFor("move", "x"), "move");
  assertEquals(actionFor({ default: "move" }, "x"), "move");
  assertEquals(actionFor({ x: "clone", default: "move" }, "x"), "clone");

  assertEquals(argPolicyFor("move", "x"), "move");
  assertEquals(argPolicyFor({ default: "move" }, "x"), "move");
  assertEquals(
    argPolicyFor({ x: { "params[*]": "move", "params[0]": "clone" } }, "x"),
    { "params[*]": "move", "params[0]": "clone" },
  );
});

Deno.test("transfer helpers move direct values and deduplicate shared backing buffers", () => {
  const transfer: Transferable[] = [];
  const buffer = new ArrayBuffer(8);
  const view = new Uint8Array(buffer);
  const dataView = new DataView(buffer);
  collectMoveArgs(
    [buffer, view, dataView],
    { "params[*]": "move" },
    transfer,
  );
  assertEquals(transfer.length, 1);
  assertEquals(transfer[0], buffer);

  const returned: Transferable[] = [];
  collectMoveReturn(buffer, "move", returned);
  assertEquals(returned, [buffer]);
});

Deno.test("transfer helpers ignore move on non-messageport transports", () => {
  const transport = messageTransport({ send: () => {} });
  const argument = new ArrayBuffer(1);
  const argumentTransfer: Transferable[] = [];
  collectMoveArgs([argument], "move", argumentTransfer, transport);
  assertEquals(argumentTransfer, []);
  assertEquals(argument.byteLength, 1);

  const returnTransfer: Transferable[] = [];
  collectMoveReturn(new ArrayBuffer(1), "move", returnTransfer, transport);
  assertEquals(returnTransfer, []);

  transport.close();
});

Deno.test("transfer helpers reject non-movable direct values", () => {
  assertThrows(
    () => collectMoveArgs([{ data: new ArrayBuffer(1) }], "move", []),
    TypeError,
    "non-transferable value",
  );
  assertThrows(
    () => collectMoveReturn({ data: new ArrayBuffer(1) }, "move", []),
    TypeError,
    "non-transferable value",
  );
});

Deno.test("transferArgs move moves ArrayBuffer and TypedArray backing buffers", async () => {
  const actor = await makeActor({
    bufferLength: { "params[0]": "move" },
    viewInfo: { "params[0]": "move" },
    dataViewInfo: { "params[0]": "move" },
  });
  try {
    const buffer = new ArrayBuffer(16);
    assertEquals(await actor.bufferLength(buffer), 16);
    assertEquals(buffer.byteLength, 0);

    const view = new Uint8Array([4, 5, 6]);
    assertEquals(await actor.viewInfo(view), { byteLength: 3, first: 4 });
    assertEquals(view.byteLength, 0);
    assertEquals(view.buffer.byteLength, 0);

    const dataView = new DataView(new Uint8Array([9, 10]).buffer);
    assertEquals(await actor.dataViewInfo(dataView), {
      byteLength: 2,
      first: 9,
    });
    assertThrows(() => dataView.byteLength, TypeError);
  } finally {
    await actor.dispose();
  }
});

Deno.test("transferArgs wildcard moves one shared ArrayBuffer only once", async () => {
  const actor = await makeActor({
    twoBufferLengths: { "params[*]": "move" },
  });
  try {
    const buffer = new ArrayBuffer(12);
    assertEquals(await actor.twoBufferLengths(buffer, buffer), [12, 12]);
    assertEquals(buffer.byteLength, 0);
  } finally {
    await actor.dispose();
  }
});

Deno.test("clone leaves the direct argument attached", async () => {
  const actor = await makeActor({ bufferLength: "clone" });
  try {
    const buffer = new ArrayBuffer(7);
    assertEquals(await actor.bufferLength(buffer), 7);
    assertEquals(buffer.byteLength, 7);
  } finally {
    await actor.dispose();
  }
});

Deno.test("move policy does not discover nested transferables", async () => {
  const actor = await makeActor({
    nestedBufferLength: { "params[0]": "move" },
  });
  try {
    const outcome = await actor.nestedBufferLength({
      data: new ArrayBuffer(2),
    }).then(
      () => "resolved" as const,
      (error: unknown) => error,
    );
    assertEquals((outcome as Error).name, "TypeError");
    assert((outcome as Error).message.includes("non-transferable"));
  } finally {
    await actor.dispose();
  }
});

Deno.test("MessagePort move transfers the direct port", async () => {
  const actor = await makeActor({
    usePort: { "params[0]": "move" },
  });
  const { port1, port2 } = new MessageChannel();
  const received = new Promise<unknown>((resolve) => {
    port1.onmessage = (event) => resolve(event.data);
  });
  try {
    assertEquals(await actor.usePort(port2), "sent");
    assertEquals(await received, "from-worker");
  } finally {
    port1.close();
    await actor.dispose();
  }
});

Deno.test("transferReturn moves a direct return and rejects an unmovable direct return", async () => {
  const actor = await makeActor();
  try {
    const moved = await actor.makeBuffer();
    assertEquals([...new Uint8Array(moved)], [7, 8, 9]);

    const outcome = await actor.makeNestedBuffer().then(
      () => "resolved" as const,
      (error: unknown) => error,
    );
    assertEquals((outcome as Error).name, "TypeError");
    assert((outcome as Error).message.includes("non-transferable"));
  } finally {
    await actor.dispose();
  }
});

Deno.test("default return policy clones a direct ArrayBuffer", async () => {
  const actor = await makeActor();
  try {
    const buffer = await actor.makeCloneBuffer();
    assertEquals(buffer.byteLength, 2);
  } finally {
    await actor.dispose();
  }
});

Deno.test("invalid move rejects locally before publishing an RPC call", async () => {
  const actor = await makeActor({
    bufferLength: { "params[0]": "move" },
  });
  try {
    await assertRejects(
      () => actor.bufferLength({} as ArrayBuffer),
      TypeError,
      "non-transferable",
    );
    assertEquals(await actor.bufferLength(new ArrayBuffer(1)), 1);
  } finally {
    await actor.dispose();
  }
});
