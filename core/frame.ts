/**
 * Framing layer: v8 serialization + a 4-byte LE length prefix, exposed as two
 * WHATWG TransformStreams (object stream ⇄ byte stream). This is the only new
 * wire protocol in the transport work — message-type transports (MessagePort,
 * fork IPC advanced) skip this layer entirely; byte-stream transports (TCP,
 * WebSocket binary, process stdio) must run every message through it to get
 * message boundaries.
 *
 * Frame format (uv-style):
 *   ┌──────────────┬──────────────────────────────┐
 *   │ len: UInt32LE │ payload: v8.deserialize 载荷 │
 *   └──────────────┴──────────────────────────────┘
 *
 * Frame payloads are plain v8-serialized values: the codec registry walk
 * (placeholder substitution) happens before serialization and after
 * deserialization, exactly like the message-channel path.
 */

import { deserialize, serialize } from "node:v8";

const HEADER = 4; // UInt32LE length prefix

/** Cap on a single frame payload; guards against corrupt/oversized length prefixes. */
export const DEFAULT_MAX_FRAME = 64 * 1024 * 1024; // 64 MiB

export interface FrameOptions {
  maxFrame?: number;
}

function readU32LE(bytes: Uint8Array): number {
  return (
    bytes[0] |
    (bytes[1] << 8) |
    (bytes[2] << 16) |
    (bytes[3] << 24)
  ) >>> 0;
}

function writeU32LE(out: Uint8Array, value: number): void {
  out[0] = value & 0xff;
  out[1] = (value >>> 8) & 0xff;
  out[2] = (value >>> 16) & 0xff;
  out[3] = (value >>> 24) & 0xff;
}

/** Serialize one value into its length-prefixed frame bytes (header + payload). */
export function encodeFrame(
  value: unknown,
  options: FrameOptions = {},
): Uint8Array {
  const payload = serialize(value);
  const max = options.maxFrame ?? DEFAULT_MAX_FRAME;
  if (payload.length > max) {
    throw new RangeError(
      `frame payload ${payload.length} bytes exceeds maxFrame ${max}`,
    );
  }
  const out = new Uint8Array(HEADER + payload.length);
  writeU32LE(out, payload.length);
  out.set(payload, HEADER);
  return out;
}

/** Deserialize one already-assembled frame's payload (header already consumed). */
export function decodeFramePayload(payload: Uint8Array): unknown {
  // v8.deserialize can mis-handle non-buffer-aligned views (Deno's polyfill
  // builds the result from payload.buffer); copy to a standalone buffer first.
  const copy = payload.byteOffset === 0 &&
      payload.byteLength === payload.buffer.byteLength
    ? payload
    : payload.slice();
  return deserialize(copy);
}

/**
 * Stream-wise encoder: values in, framed byte chunks out (one Uint8Array per
 * frame). Writes are queued by the WritableStream; frames are never split.
 */
export function createEncoder(options: FrameOptions = {}): TransformStream<
  unknown,
  Uint8Array
> {
  return new TransformStream<unknown, Uint8Array>({
    transform(chunk, controller) {
      try {
        controller.enqueue(encodeFrame(chunk, options));
      } catch (e) {
        controller.error(e);
      }
    },
    flush(controller) {
      controller.terminate();
    },
  });
}

/**
 * Stream-wise decoder: raw byte chunks in (arbitrarily segmented), framed
 * values out. A corrupt length prefix (> maxFrame) errors the stream.
 */
export function createDecoder(options: FrameOptions = {}): TransformStream<
  Uint8Array,
  unknown
> {
  const max = options.maxFrame ?? DEFAULT_MAX_FRAME;
  let stage: "header" | "payload" = "header";
  const headerBuf = new Uint8Array(HEADER);
  let headerFilled = 0;
  let payload: Uint8Array | undefined; // accumulating current frame's payload
  let payloadFilled = 0;
  let payloadLen = 0;

  return new TransformStream<Uint8Array, unknown>({
    transform(chunk, controller) {
      if (chunk.byteLength === 0) return;
      let offset = 0;
      while (offset < chunk.byteLength) {
        if (stage === "header") {
          const take = Math.min(
            HEADER - headerFilled,
            chunk.byteLength - offset,
          );
          headerBuf.set(chunk.subarray(offset, offset + take), headerFilled);
          headerFilled += take;
          offset += take;
          if (headerFilled === HEADER) {
            payloadLen = readU32LE(headerBuf);
            headerFilled = 0;
            if (payloadLen > max) {
              controller.error(
                new RangeError(
                  `frame payload ${payloadLen} bytes exceeds maxFrame ${max}`,
                ),
              );
              return;
            }
            if (payloadLen === 0) {
              controller.enqueue(decodeFramePayload(new Uint8Array(0)));
              continue; // stay in header stage for the next frame
            }
            payload = new Uint8Array(payloadLen);
            payloadFilled = 0;
            stage = "payload";
          }
          continue;
        }
        // stage === "payload"
        const take = Math.min(
          payloadLen - payloadFilled,
          chunk.byteLength - offset,
        );
        payload!.set(chunk.subarray(offset, offset + take), payloadFilled);
        payloadFilled += take;
        offset += take;
        if (payloadFilled === payloadLen) {
          controller.enqueue(decodeFramePayload(payload!));
          payload = undefined;
          payloadFilled = 0;
          payloadLen = 0;
          stage = "header";
        }
      }
    },
    // A half-frame at close (truncated stream) is an error, not a silent drop.
    flush(controller) {
      if (stage === "payload" && payloadFilled < payloadLen) {
        controller.error(new Error("truncated frame at stream end"));
        return;
      }
      controller.terminate();
    },
  });
}

/** Mux frames: the multiplexing protocol on a framed transport. A data frame
 * carries a logical channel id; open/close are channel-establishment controls
 * exchanged on the main channel. The `__mux` discriminator keeps data and
 * control frames distinguishable in one stream.
 */
export type MuxFrame =
  | { __mux: "data"; ch: number; value: unknown }
  | { __mux: "open"; ch: number }
  | { __mux: "close"; ch: number };

export type MuxControl = Extract<MuxFrame, { __mux: "open" | "close" }>;

export function isMuxControl(v: unknown): v is MuxControl {
  return (
    typeof v === "object" && v !== null &&
      (v as { __mux?: unknown }).__mux === "open" ||
    typeof v === "object" && v !== null &&
      (v as { __mux?: unknown }).__mux === "close"
  );
}
