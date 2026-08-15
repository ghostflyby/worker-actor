/** Example worker for the custom marshal-by-ref codec. */
import { serveWorker } from "../../mod.ts";
import {
  isRemoteRef,
  ownerChannelCountFor,
  type RemoteRef,
  remoteRef,
  remoteRefCodec,
} from "./ref_codec.ts";

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

/** One shared Counter: repeated remoteRef() calls reuse its identity. */
const shared = new Counter();

export const rpc = {
  createCounter(): RemoteRef<Counter> {
    return remoteRef(new Counter());
  },
  sharedCounter(): RemoteRef<Counter> {
    return remoteRef(shared);
  },
  /** Accept a reference back into this worker (which may be the owner). */
  acceptBack(ref: RemoteRef<unknown>): string {
    return isRemoteRef(ref) ? "proxy" : "local";
  },
  /** If the returned ref restored the owner's object, calling it runs locally. */
  callBack(ref: RemoteRef<unknown>): Promise<number> {
    return (ref as RemoteRef<{ increment(): Promise<number> }>).increment();
  },
  /** Probe: owner-side channels for `shared` (closed on restore). */
  sharedOwnerChannels(): number {
    return ownerChannelCountFor(shared);
  },
  /** Hold a reference (proxy or fresh) so it can be handed off or called later. */
  holdRef(ref: RemoteRef<unknown>): string {
    heldRef = ref;
    return "held";
  },
  /** Return the held reference: re-encoding a proxy triggers a hand-off (refId-only). */
  getHeldRef(): RemoteRef<unknown> {
    return heldRef!;
  },
  /** Call a method on the held reference (may trigger acquire on first use). */
  callHeld(_x: number): Promise<number> {
    return (heldRef as unknown as RemoteRef<{ increment(): Promise<number> }>)
      .increment();
  },
  disposedCount(): number {
    return disposedCount;
  },
};

let heldRef: RemoteRef<unknown> | undefined;

serveWorker(rpc, { codecs: [remoteRefCodec] });
