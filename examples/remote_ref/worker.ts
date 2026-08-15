/** Example worker for the custom marshal-by-ref codec. */
import { serveWorker } from "../../mod.ts";
import { type RemoteRef, remoteRef, remoteRefCodec } from "./ref_codec.ts";

/** How many real objects were released via [Symbol.dispose] — observable probe. */
let disposedCount = 0;

class Counter {
  #value = 0;

  increment(): number {
    return ++this.#value;
  }

  get(): number {
    return this.#value;
  }

  boom(): never {
    throw new RangeError("counter exploded");
  }

  /** Streams through the reference channel: proves nested codec values in results. */
  countdown(n: number): AsyncIterable<number> {
    return (async function* () {
      for (let i = n; i >= 0; i--) yield i;
    })();
  }

  /** Cleanup hook: runs when the proxy disposes or is GC-released. */
  [Symbol.dispose](): void {
    disposedCount++;
  }
}

export const rpc = {
  createCounter(): RemoteRef<Counter> {
    return remoteRef(new Counter());
  },
  disposedCount(): number {
    return disposedCount;
  },
};

serveWorker(rpc, { codecs: [remoteRefCodec] });
