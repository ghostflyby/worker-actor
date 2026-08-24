// Process-actor fixture for cross-process reference hand-off: serves the
// remote-ref codec and exposes an object whose reference can be handed off to
// other actors (refId-only hand-off → acquire over the Mux transport).
import { serveProcess } from "../worker_runtime.ts";
import { remoteRef, remoteRefCodec } from "./remote_ref_helper.ts";

// Shared counter object: a remoteRef() of it is produced at startup and can be
// handed out or returned (re-encoded as a refId-only token on hand-off).
const counter = {
  value: 0,
  inc(): number {
    return ++this.value;
  },
  get(): number {
    return this.value;
  },
};

let heldRef: unknown | undefined;

export const rpc = {
  add(a: number, b: number): number {
    return a + b;
  },
  /** Fresh reference to this process's shared counter. */
  getCounter(): ReturnType<typeof remoteRef> {
    return remoteRef(counter);
  },
  /** Hold a reference (fresh or handed-off proxy) for later return/hand-off. */
  holdRef(ref: unknown): string {
    heldRef = ref;
    return "held";
  },
  /** Return the held reference: re-encoding a proxy is a refId-only hand-off. */
  getHeldRef(): unknown {
    return heldRef;
  },
  /** Call increment on the held reference (first call may trigger acquire). */
  callHeld(): Promise<number> {
    return (heldRef as unknown as {
      inc(): Promise<number>;
    }).inc();
  },
  /** Call a Worker-owned ref's `increment` method (mixed-topology test). */
  callHeldIncrement(): Promise<number> {
    return (heldRef as unknown as {
      increment(): Promise<number>;
    }).increment();
  },
};

serveProcess(rpc, { codecs: [remoteRefCodec] });
