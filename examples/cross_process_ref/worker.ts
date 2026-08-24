// Cross-process reference owner: produces a remoteRef() to a shared counter.
// Run via: deno run --allow-read --allow-run --allow-env examples/cross_process_ref/main.ts
import { serveProcess } from "@ghostflyby/worker-actor";
import { remoteRef, remoteRefCodec } from "../remote_ref/ref_codec.ts";

// A shared object this process owns. A remoteRef() of it can be handed to any
// other actor (process / worker / websocket); callers reach THIS object.
const counter = {
  value: 0,
  inc(): number {
    return ++this.value;
  },
  get(): number {
    return this.value;
  },
};

export const rpc = {
  /** A fresh reference to the shared counter, owned by this process. */
  getCounter(): ReturnType<typeof remoteRef> {
    return remoteRef(counter);
  },
  /** Hold a handed-off reference (from another actor) for later calls. */
  holdRef(ref: unknown): string {
    heldRef = ref;
    return "held";
  },
  /** Call increment on the held reference (first call may trigger acquire). */
  callHeld(): Promise<number> {
    return (heldRef as unknown as { inc(): Promise<number> }).inc();
  },
  /** Call a Worker-owned ref's `increment` method (mixed-topology example). */
  callHeldIncrement(): Promise<number> {
    return (heldRef as unknown as { increment(): Promise<number> }).increment();
  },
};

let heldRef: unknown | undefined;

serveProcess(rpc, { codecs: [remoteRefCodec] });
