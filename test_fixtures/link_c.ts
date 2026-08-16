/** Test fixture: worker C — receives B's reference over the link, and sends one back. */
import { type LinkHandle, serveWorker } from "@ghostflyby/worker-actor";
import type { PeerRpc } from "@ghostflyby/worker-actor/codec";
import type { BPeerApi } from "./link_b.ts"; // type-only: no runtime import cycle
import {
  isRemoteRef,
  type RemoteRef,
  remoteRef,
  remoteRefCodec,
} from "../examples/remote_ref/ref_codec.ts";

let linkHandle: LinkHandle | undefined;
let lastValue: unknown;

export const rpc = {
  /** Did the last link value arrive as a remote reference proxy (not a plain value)? */
  getLastIsRef(): boolean {
    return isRemoteRef(lastValue);
  },
  /** Call a method on the reference B sent over the link (executes in B). */
  callLastIncrement(): Promise<number> {
    return (lastValue as RemoteRef<{ increment(): Promise<number> }>)
      .increment();
  },
  /** Send worker C's own object back over the same link (bidirectional). */
  sendGreeterToB(): string {
    if (!linkHandle) throw new Error("link not established");
    linkHandle.send(remoteRef({
      hello(name: string): string {
        return `hello ${name} from C`;
      },
    }));
    return "sent";
  },
  /** Direct peer RPC: C calls B's served surface (executes in B). */
  callBEcho(s: string): Promise<string> {
    return (linkHandle!.rpc as unknown as BPeerApi).echo(s);
  },
  /** Direct peer RPC: B's served surface throws; error marshals back. */
  callBBoom(): Promise<unknown> {
    return (linkHandle!.rpc as unknown as BPeerApi).boom();
  },
  /** Direct peer RPC: B's management method is NOT on B's served surface. */
  callBMissing(): Promise<unknown> {
    return (linkHandle!.rpc as unknown as {
      getDisposedCount(): Promise<number>;
    })
      .getDisposedCount();
  },
  /** Hand the reference received from B back to B over the link (round-trip). */
  returnSharedToB(): Promise<string> {
    return (linkHandle!.rpc as unknown as {
      acceptRef(ref: unknown): Promise<string>;
    })
      .acceptRef(lastValue);
  },
};

/** Peer-facing surface: what worker B may call on C over the link. */
export const peerApi = {
  ping(): string {
    return "pong";
  },
  describe(): string {
    return "c";
  },
};

/** Contract type for C's served surface, exported for B's calls. */
export type CPeerApi = PeerRpc<typeof peerApi>;

serveWorker(rpc, {
  codecs: [remoteRefCodec],
  onLink(link) {
    linkHandle = link;
    link.serve(peerApi);
    link.onValue((v) => {
      lastValue = v;
    });
  },
});
