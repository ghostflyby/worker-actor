import { serveWorker } from "../worker_runtime.ts";

export const rpc = {
  bufferLength(value: ArrayBuffer): number {
    return value.byteLength;
  },
  viewInfo(value: Uint8Array): { byteLength: number; first: number } {
    return { byteLength: value.byteLength, first: value[0] ?? -1 };
  },
  dataViewInfo(value: DataView): { byteLength: number; first: number } {
    return { byteLength: value.byteLength, first: value.getUint8(0) };
  },
  twoBufferLengths(
    first: ArrayBuffer,
    second: ArrayBuffer,
  ): [number, number] {
    return [first.byteLength, second.byteLength];
  },
  nestedBufferLength(value: { data: ArrayBuffer }): number {
    return value.data.byteLength;
  },
  usePort(port: MessagePort): Promise<string> {
    port.postMessage("from-worker");
    port.close();
    return Promise.resolve("sent");
  },
  makeBuffer(): ArrayBuffer {
    return new Uint8Array([7, 8, 9]).buffer;
  },
  makeCloneBuffer(): ArrayBuffer {
    return new Uint8Array([1, 2]).buffer;
  },
  makeNestedBuffer(): { data: ArrayBuffer } {
    return { data: new ArrayBuffer(4) };
  },
};

serveWorker(rpc, {
  transferReturn: {
    makeBuffer: "move",
    makeNestedBuffer: "move",
    default: "clone",
  },
});
