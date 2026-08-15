/** Test fixture: worker B — holds a real Counter object and hands its reference to C over the link. */
import { serveWorker } from "../mod.ts";
import type { LinkHandle } from "../worker_runtime.ts";
import type { PeerRpc } from "../core/rpc.ts";
import type { CPeerApi } from "./link_c.ts"; // type-only: no runtime import cycle
import {
  isRemoteRef,
  type RemoteRef,
  remoteRef,
  remoteRefCodec,
} from "../examples/remote_ref/ref_codec.ts";

let linkHandle: LinkHandle | undefined;
let lastFromC: unknown;
let disposed = 0;

class Counter {
  #value = 0;
  increment(): number {
    return ++this.#value;
  }
  /** Cleanup hook: runs when the peer disposes or GC-releases the reference. */
  [Symbol.dispose](): void {
    disposed++;
  }
}

export const rpc = {
  /** Create a Counter and hand its reference directly to worker C over the link. */
  createCounterAndSendToC(): string {
    if (!linkHandle) throw new Error("link not established");
    const c = new Counter();
    heldObjects.push(c); // mode 1: the worker holds the object
    linkHandle.send(remoteRef(c));
    return "sent";
  },
  getDisposedCount(): number {
    return disposed;
  },
  gotFromC(): boolean {
    return lastFromC !== undefined;
  },
  /** Call a reference that worker C sent back over the link (bidirectional). */
  callLastFromC(name: string): Promise<string> {
    return (lastFromC as RemoteRef<{ hello(n: string): Promise<string> }>)
      .hello(name);
  },
  /** Direct peer RPC: B calls C's served surface (executes in C). */
  callCPing(): Promise<string> {
    return (linkHandle!.rpc as unknown as CPeerApi).ping();
  },
  /** Send the shared Counter's reference to C over the link (fresh owner token). */
  sendSharedToC(): string {
    if (!linkHandle) throw new Error("link not established");
    linkHandle.send(remoteRef(sharedCounter));
    return "sent";
  },
};

/**
 * Peer-facing surface: what worker C may call on B over the link. Deliberately
 * narrower than the main-thread rpc — management methods (getDisposedCount)
 * stay hidden from the peer. The contract type lives in the test.
 */
export const peerApi = {
  echo(s: string): string {
    return `echo:${s}`;
  },
  boom(): never {
    throw new RangeError("peer boom");
  },
  describe(): string {
    return "b";
  },
  /** Accept a reference that traveled back (C → B): restored or still proxy? */
  acceptRef(ref: RemoteRef<unknown>): string {
    return isRemoteRef(ref) ? "proxy" : "local";
  },
};

/** A Counter shared across refs so the test can observe identity/restore.
 *  Held explicitly (mode 1): worker-module top-level consts do not reliably
 *  keep objects alive against GC in Deno workers, so the reference must not
 *  be the only thing holding the object. */
const sharedCounter = new Counter();
// Array-rooted (module-level): keeps the shared object alive against GC in
// this worker (verified: globalThis property anchoring is not a reliable root).
const heldObjects: object[] = [sharedCounter];

/** Contract type for B's served surface, exported for C's calls. */
export type BPeerApi = PeerRpc<typeof peerApi>;

serveWorker(rpc, {
  codecs: [remoteRefCodec],
  onLink(link) {
    linkHandle = link;
    link.serve(peerApi);
    link.onValue((v) => {
      lastFromC = v;
    });
  },
});
