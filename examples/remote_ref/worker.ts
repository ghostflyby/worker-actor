/** Example worker for the custom marshal-by-ref codec. */
import { serveWorker } from "@ghostflyby/worker-actor";
import {
  isRemoteRef,
  ownerChannelCountFor,
  ownerStrongRefCount,
  releaseRef,
  type RemoteRef,
  remoteRef,
  remoteRefCodec,
  setLivenessParams,
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
    // Mode 1: the worker holds the object, so it stays alive regardless of
    // references. The counter's [Symbol.dispose] runs on ref dispose/GC.
    const c = new Counter();
    heldObjects.push(c);
    return remoteRef(c);
  },
  /** Mode 2: the object is only reachable through its reference — it dies
   *  once the worker's reference graph drops it (remote holders don't pin it).
   *  GC probe: how many ephemeral objects were collected. */
  createEphemeral(): RemoteRef<Counter> {
    const c = new Counter();
    ephemeralFinalizer.register(c, undefined);
    return remoteRef(c);
  },
  /** How many ephemeral objects were garbage-collected (release probe). */
  ephemeralFinalized(): number {
    return ephemeralFinalizedCount;
  },
  /** Drop all held objects: mode-1 references now point at released objects. */
  releaseAllHeld(): string {
    heldObjects.length = 0;
    return "released";
  },
  /** Release a held object explicitly (owner-side releaseRef). */
  releaseRefByName(name: string): string {
    releaseRef(namedObjects.get(name)!);
    return "released";
  },
  /** Register a named object that can later be released. */
  registerNamed(name: string): RemoteRef<Counter> {
    const c = new Counter();
    namedObjects.set(name, c);
    return remoteRef(c);
  },
  /** How many objects this worker holds strongly (release probe). */
  strongRefCount(): number {
    return ownerStrongRefCount();
  },
  /** Configure liveness params (test hook). */
  setLiveness(intervalMs: number, timeoutMs: number): string {
    setLivenessParams(intervalMs, timeoutMs);
    return "set";
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
  /** Probe: owner-side channels serving the named object (main + holders). */
  holderChannelsFor(name: string): number {
    return ownerChannelCountFor(namedObjects.get(name)!);
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
const heldObjects: object[] = [];
const namedObjects = new Map<string, object>();
let ephemeralFinalizedCount = 0;
const ephemeralFinalizer = new FinalizationRegistry<void>(() => {
  ephemeralFinalizedCount++;
});

serveWorker(rpc, { codecs: [remoteRefCodec] });
