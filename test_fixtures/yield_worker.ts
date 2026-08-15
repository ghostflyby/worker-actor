/** Test fixture: a worker exposing objects with per-ref serialization + actorYield. */
import { serveWorker } from "../mod.ts";
import { actorYield } from "../mod.ts";
import {
  type RemoteRef,
  remoteRef,
  remoteRefCodec,
} from "../examples/remote_ref/ref_codec.ts";

/** Execution log: pushed by methods; the test asserts ordering. */
const log: string[] = [];

class YieldActor {
  /** Long IO via actorYield: the queue releases during the wait. */
  async longIo(tag: string, ms: number): Promise<string> {
    log.push(`${tag}:start`);
    const v = await actorYield(
      new Promise<string>((r) => setTimeout(() => r(`${tag}:done`), ms)),
    );
    log.push(v);
    return v;
  }

  /** A quick synchronous call (probe: does it interleave during a yield?). */
  ping(tag: string): string {
    log.push(`${tag}:ping`);
    return `${tag}:pong`;
  }

  /** Two consecutive yields (loop-style resumption). */
  async doubleYield(tag: string): Promise<string> {
    log.push(`${tag}:first`);
    await actorYield(new Promise((r) => setTimeout(r, 10)));
    log.push(`${tag}:second`);
    await actorYield(new Promise((r) => setTimeout(r, 10)));
    log.push(`${tag}:third`);
    return `${tag}:end`;
  }
}

export const rpc = {
  make(): RemoteRef<YieldActor> {
    return remoteRef(new YieldActor());
  },
  getLog(): string[] {
    return [...log];
  },
  /** Main-channel call that also yields: degrades to a plain await (no queue). */
  async mainYield(ms: number): Promise<string> {
    return await actorYield(
      new Promise((r) => setTimeout(() => r("main-yield"), ms)),
    );
  },
};

serveWorker(rpc, { codecs: [remoteRefCodec] });
