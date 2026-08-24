import { assertEquals, assertRejects } from "@std/assert";
import {
  createDecoder,
  createEncoder,
  decodeFramePayload,
  encodeFrame,
} from "./core/frame.ts";

/** Serialize values through encoder+decoder and collect the output objects. */
async function roundTrip(values: unknown[]): Promise<unknown[]> {
  const enc = createEncoder();
  const dec = createDecoder();
  // Start the reader side first so the stream pipeline keeps flowing, then
  // write, then await the pipe (which drains on writer.close()).
  const outP = readAll(dec.readable);
  const piped = enc.readable.pipeTo(dec.writable);
  const writer = enc.writable.getWriter();
  for (const v of values) await writer.write(v);
  await writer.close();
  await piped;
  return await outP;
}

async function readAll<T>(stream: ReadableStream<T>): Promise<T[]> {
  const out: T[] = [];
  const reader = stream.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    out.push(value);
  }
  return out;
}

Deno.test("encodeFrame produces length-prefixed v8 bytes; decode round-trips", () => {
  const value = { a: 1, m: new Map([["k", 2n]]), t: new Uint8Array([1, 2]) };
  const frame = encodeFrame(value);
  assertEquals(frame.byteLength >= 4, true);
  const len = frame[0] | (frame[1] << 8) | (frame[2] << 16) | (frame[3] << 24);
  assertEquals(len, frame.byteLength - 4);
  const back = decodeFramePayload(frame.subarray(4));
  assertEquals(back, value);
  assertEquals((back as { m: Map<string, bigint> }).m.get("k"), 2n);
});

Deno.test("encoder + decoder round-trip through a byte stream", async () => {
  const got = await roundTrip([{ x: 1 }, "hello", new Set([1, 2]), null]);
  assertEquals(got.length, 4);
  assertEquals(got[0], { x: 1 });
  assertEquals(got[1], "hello");
  assertEquals(got[2], new Set([1, 2]));
  assertEquals(got[3], null);
});

Deno.test("decoder reassembles arbitrarily segmented frames", async () => {
  const frames = [encodeFrame({ n: 1 }), encodeFrame({ n: 2 })];
  const segmented = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const frame of frames) {
        for (let i = 0; i < frame.byteLength; i++) {
          controller.enqueue(frame.subarray(i, i + 1));
        }
      }
      controller.close();
    },
  });
  const dec = createDecoder();
  const outP = readAll(dec.readable);
  await segmented.pipeTo(dec.writable);
  assertEquals(await outP, [{ n: 1 }, { n: 2 }]);
});

Deno.test("decoder handles a zero-length payload", async () => {
  const got = await roundTrip([undefined]);
  assertEquals(got, [undefined]);
});

Deno.test("decoder errors on an oversized length prefix", async () => {
  const dec = createDecoder({ maxFrame: 4 });
  const bad = new Uint8Array([255, 255, 255, 127]); // > 4 GiB
  const reader = dec.readable.getReader();
  const w = dec.writable.getWriter();
  const readP = reader.read().then(
    () => "resolved",
    () => "rejected",
  );
  await w.write(bad);
  // The transform errored the stream; close() would throw on an errored
  // writable, which is expected.
  await w.close().catch(() => {});
  assertEquals(await readP, "rejected");
});

Deno.test("encodeFrame throws when payload exceeds maxFrame", async () => {
  await assertRejects(
    () =>
      Promise.resolve().then(() =>
        encodeFrame("x".repeat(100), { maxFrame: 4 })
      ),
    RangeError,
  );
});
