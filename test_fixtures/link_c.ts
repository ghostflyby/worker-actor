/** Test fixture: worker C — receives B's reference over the link, and sends one back. */
import { serveWorker } from "../mod.ts";
import type { LinkHandle } from "../worker_runtime.ts";
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
};

serveWorker(rpc, {
  codecs: [remoteRefCodec],
  onLink(link) {
    linkHandle = link;
    link.onValue((v) => {
      lastValue = v;
    });
  },
});
