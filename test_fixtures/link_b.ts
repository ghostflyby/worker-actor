/** Test fixture: worker B — holds a real Counter object and hands its reference to C over the link. */
import { serveWorker } from "../mod.ts";
import type { LinkHandle } from "../worker_runtime.ts";
import type { PeerRpc } from "../core/rpc.ts";
import type { CPeerApi } from "./link_c.ts"; // type-only: no runtime import cycle
import {
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
    linkHandle.send(remoteRef(new Counter()));
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
};

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
