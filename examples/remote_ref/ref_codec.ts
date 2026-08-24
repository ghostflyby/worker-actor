/**
 * A custom "marshal-by-ref" codec built on the generic Channel abstraction.
 *
 * Any object can be wrapped into a cross-thread reference (the .NET MarshalByRef
 * pattern): the real object stays on its side; the peer gets a transparent proxy
 * whose method calls are marshaled over a dedicated channel.
 *
 * Identity and transfer (the "reference" semantics):
 *   - Every real object gets a stable refId (registered on the owner side).
 *     Multiple remoteRef(x) of the same object reuse that identity.
 *   - Handing a reference over is SHARING, not moving: a proxy encodes as a
 *     refId-only token, the receiver keeps its own proxy, and the new holder
 *     acquires a fresh per-holder channel to the owner on first use (bootstrapped
 *     by the main thread). Any number of holders share the same identity, each
 *     with its own direct channel to the owner (A → B → C keeps direct channels;
 *     no proxy chains).
 *   - A reference that travels back to its owner is RESTORED: the owner's
 *     registry recognizes the refId, collapses it into a local call-through
 *     reference, and the arriving channel is closed — the owner calls the real
 *     object directly, with zero indirection. Other holders' channels stay open.
 *   - Only the owner can produce fresh references (remoteRef of an object it
 *     owns); a proxy holder can only re-encode the refId. Ownership of the real
 *     object is unaffected by hand-offs — it stays where it was created.
 *
 * Wire protocol (one channel per reference):
 *   proxy → owner  { type: "call"; id; method; args }         // args go through the registry
 *   owner → proxy  { type: "result"; id; ok: true; value }    // value too (nested streams/refs work)
 *   owner → proxy  { type: "result"; id; ok: false; error }
 *   proxy → owner  { type: "dispose" }                        // explicit dispose or GC
 *
 * Liveness plane (one channel per owner↔holder WORKER pair, not per reference):
 *   owner → holder { type: "ping" }                           // the owner PULLS liveness
 *   holder → owner { type: "pong" }
 * A holder that stops ponging past the timeout is dead: the owner releases all
 * of that holder's refs in one batch and posts a single __holder-dead notice to
 * the main thread. A holder whose pings stop fails its refs of that owner
 * (reverse detection). Frames below ride on the ref channel; liveness frames
 * ride on the pair channel. The liveness plane is MESSAGEPORT-ONLY: over a Mux
 * transport (process actors) the per-holder pair channel cannot be transferred,
 * so acquires complete over a Mux ref channel without a liveness plane — the
 * ref works, it just has no owner/holder death sweep (documented limitation).
 *
 * Lifecycle: explicit dispose(), GC-based release (FinalizationRegistry), and
 * failAll (the registry closes the channel). The owner runs the real object's
 * [Symbol.dispose] hook when the reference is released.
 */

import {
  type Channel,
  type Codec,
  CODEC_PLACEHOLDER_KEY,
  connectChannel,
  connectToken,
  type ControlFrame,
  type DecodeContext,
  type EncodeContext,
  getActiveRegistry,
  getActiveTransport,
  openChannel,
  registerControlHandler,
  registerRelease,
  serializeError,
  triggerAcquire,
} from "@ghostflyby/worker-actor/codec";
import { RemoteError } from "@ghostflyby/worker-actor";

// Deno types `self` as Window; in a worker it is a DedicatedWorkerGlobalScope.
// Only postMessage is used here (the liveness death notice goes to main).
declare const self: { postMessage(message: unknown): void };

const REF_BRAND = Symbol.for("worker-actor-example.remote-ref");
/** Marks the receiving-side proxy; lets a peer detect "this is a reference, not a value". */
const REF_PROXY_BRAND = Symbol.for("worker-actor-example.remote-ref.proxy");
/** Marks a restored local call-through reference (traveled back to its owner). */
const REF_LOCAL_BRAND = Symbol.for("worker-actor-example.remote-ref.local");
/** refId of the real object this proxy refers to (for hand-off/acquire). */
const REF_ID = Symbol.for("worker-actor-example.remote-ref.id");

/** The proxy type: every method returns a Promise; non-functions are `never`. */
export type RemoteRef<T> =
  & {
    [K in keyof T]: T[K] extends (...args: infer A) => infer R
      ? (...args: A) => Promise<Awaited<R>>
      : never;
  }
  & { dispose(): Promise<void> };

/** Brand check: is this a remote reference proxy (a marshaled reference), as opposed to a plain value? */
export function isRemoteRef(v: unknown): v is RemoteRef<unknown> {
  return typeof v === "object" && v !== null &&
    (v as { [REF_PROXY_BRAND]?: unknown })[REF_PROXY_BRAND] === true;
}

/** Wrap a real object (owned by this side) into a transmittable remote reference. */
export function remoteRef<T extends object>(obj: T): RemoteRef<T> {
  return { [REF_BRAND]: true, obj } as unknown as RemoteRef<T>;
}

/**
 * Number of open owner-side channels for an object (test/observability probe):
 * a restored reference closes them all, so this drops to 0 after a round-trip.
 */
export function ownerChannelCountFor(obj: object): number {
  const id = refIdByObj.get(obj);
  if (id === undefined) return 0;
  return ownerChannelsByRefId.get(id)?.size ?? 0;
}

// —— Identity table (owner side) ——
// refId must be unique across processes and routeable to the owner: the prefix
// is this worker's main-assigned id (set by __worker-id); before that arrives,
// a random per-process fallback keeps ids unique (such ids are never
// acquire-routed before the id is set, because acquire needs a routeable
// prefix).
let localRefCount = 0;
let workerIdPrefix = Math.random().toString(36).slice(2);
const refIdByObj = new WeakMap<object, string>();
const objByRefId = new Map<string, WeakRef<object>>();
const ownerChannelsByRefId = new Map<string, Set<Channel>>();
// Owner-side strong hold while at least one holder channel is open: the referenced
// object stays alive during holder use and becomes collectable once ALL channels
// close (holders released). Without this, an object only referenced through
// refs (WeakMap + WeakRef) could be collected while holders still call it.
const ownerStrongRefs = new Map<string, object>();
// Liveness plane: one channel per (owner, holder worker) pair, independent of
// refs. The owner PULLS liveness (pings on a cadence); the holder replies pong.
// A missing pong past the timeout means the holder worker died — the owner
// releases ALL of that holder's refs in one batch and posts ONE __holder-dead
// notice to the main thread (a worker-level monitor). The holder in turn
// watches for missing pings and fails its refs of that owner (reverse
// detection: a dead owner must not leave the holder's calls hanging forever).
// Defaults suit production; tests shorten them via rpc.
let livenessIntervalMs = 2_000;
let livenessTimeoutMs = 6_000;

/** Configure liveness (ping interval / timeout) — test hook; defaults are production-safe. */
export function setLivenessParams(intervalMs: number, timeoutMs: number): void {
  livenessIntervalMs = intervalMs;
  livenessTimeoutMs = timeoutMs;
}

// Owner side: pair channels keyed by holder worker id; ref channels record
// which holder they belong to (so a dead holder is released in one batch).
const channelRefId = new Map<Channel, string>();
const pairByHolderId = new Map<
  string,
  { channel: Channel; lastPong: number }
>();
const channelHolderId = new Map<Channel, string>();
let ownerScanner: ReturnType<typeof setInterval> | undefined;

// Holder side: pair channels keyed by owner id; each carries the kill switches
// of the proxies it covers, fired together when the owner dies.
interface HolderPair {
  channel: Channel;
  lastPing: number;
  kills: Set<() => void>;
}
const pairByOwnerId = new Map<string, HolderPair>();
let holderScanner: ReturnType<typeof setInterval> | undefined;

/** Owner side of the liveness plane: one pair channel per holder worker (first serve only). */
function ensureOwnerPair(
  holderId: string | undefined,
  livenessPort: MessagePort | undefined,
): void {
  if (holderId === undefined || livenessPort === undefined) return;
  if (pairByHolderId.has(holderId)) {
    livenessPort.close();
    return;
  }
  const channel = connectChannel(livenessPort);
  pairByHolderId.set(holderId, { channel, lastPong: Date.now() });
  ensureOwnerScanner();
}

/** Holder side of the liveness plane: one pair channel per owner (first acquire only). */
function ensureHolderPair(
  ownerId: string | undefined,
  livenessPort: MessagePort | undefined,
): HolderPair | undefined {
  if (ownerId === undefined) return undefined;
  const existing = pairByOwnerId.get(ownerId);
  if (existing) {
    livenessPort?.close();
    return existing;
  }
  if (livenessPort === undefined) return undefined;
  const channel = connectChannel(livenessPort);
  const pair: HolderPair = { channel, lastPing: Date.now(), kills: new Set() };
  channel.onMessage((message) => {
    const frame = message as LivenessFrame;
    if (frame.type === "ping") {
      pair.lastPing = Date.now();
      channel.send({ type: "pong" } satisfies LivenessFrame);
    }
  });
  pairByOwnerId.set(ownerId, pair);
  ensureHolderScanner();
  return pair;
}

/** Owner sweep: ping every pair and time out holders that stopped ponging. */
function ensureOwnerScanner(): void {
  if (ownerScanner) return;
  ownerScanner = setInterval(() => {
    const now = Date.now();
    for (const [holderId, pair] of pairByHolderId) {
      pair.channel.send({ type: "ping" } satisfies LivenessFrame);
      if (now - pair.lastPong > livenessTimeoutMs) releaseHolder(holderId);
    }
  }, livenessIntervalMs);
}

/**
 * Batch-release everything held by one dead holder worker: close its ref
 * channels (onClosed untracks them and drops the strong hold once the last
 * holder of a ref is gone) and post a SINGLE death notice to the main thread —
 * one message per death, not one per reference. Live holders of the same refs
 * are unaffected (per-holder cleanup, not a broadcast).
 */
function releaseHolder(holderId: string): void {
  const pair = pairByHolderId.get(holderId);
  if (pair === undefined) return;
  pairByHolderId.delete(holderId);
  for (const set of ownerChannelsByRefId.values()) {
    for (const ch of [...set]) {
      if (channelHolderId.get(ch) === holderId) ch.close();
    }
  }
  pair.channel.close();
  self.postMessage({ type: "__holder-dead", refId: holderId });
}

/** Holder sweep: fail the refs of an owner whose pings stopped (reverse detection). */
function ensureHolderScanner(): void {
  if (holderScanner) return;
  holderScanner = setInterval(() => {
    const now = Date.now();
    for (const [ownerId, pair] of pairByOwnerId) {
      if (now - pair.lastPing > livenessTimeoutMs) {
        pairByOwnerId.delete(ownerId);
        failOwner(pair);
      }
    }
  }, livenessIntervalMs);
}

/** The owner died: every proxy its refs created fails together (no hanging calls). */
function failOwner(pair: HolderPair): void {
  for (const kill of [...pair.kills]) kill();
  pair.channel.close();
}

/**
 * Release a refId: broadcast `released` to every holder, close their channels,
 * drop the strong hold. Peers die immediately; the object becomes collectable.
 */
function releaseRefId(refId: string): void {
  const channels = ownerChannelsByRefId.get(refId);
  if (channels) {
    for (const c of channels) {
      channelRefId.delete(c);
      c.send({ type: "released" } satisfies RefFrame);
      c.close();
    }
    ownerChannelsByRefId.delete(refId);
  }
  ownerStrongRefs.delete(refId);
}

/** Owner-side explicit release: the actor decides its byref object dies. */
export function releaseRef(obj: object): void {
  const refId = refIdByObj.get(obj);
  if (refId !== undefined) releaseRefId(refId);
}

/** How many objects the owner is holding strongly (release probe). */
export function ownerStrongRefCount(): number {
  return ownerStrongRefs.size;
}

/** Tombstone finalizer: whatever caused the object's collection, sweep the
 *  owner's bookkeeping and broadcast so no peer is left with a dead ref. */
const refIdFinalizer = new FinalizationRegistry<string>((id) => {
  const channels = ownerChannelsByRefId.get(id);
  if (channels) {
    for (const c of channels) {
      channelRefId.delete(c);
      c.send({ type: "released" } satisfies RefFrame);
      c.close();
    }
    ownerChannelsByRefId.delete(id);
  }
  objByRefId.delete(id);
  ownerStrongRefs.delete(id);
});

function refIdFor(obj: object): string {
  const existing = refIdByObj.get(obj);
  if (existing !== undefined) return existing;
  const id = `${workerIdPrefix}:${++localRefCount}`;
  refIdByObj.set(obj, id);
  objByRefId.set(id, new WeakRef(obj));
  refIdFinalizer.register(obj, id);
  return id;
}

/** Transmittable token produced by remoteRef(); recognized by the codec. */
interface RefToken {
  [REF_BRAND]: true;
  obj: unknown;
}

interface RefHandle {
  [CODEC_PLACEHOLDER_KEY]: "remote-ref";
  refId: string;
  /** Present on a fresh owner-produced reference on a messageport transport. */
  port?: MessagePort;
  /** Present on a fresh owner-produced reference on a Mux transport (channel-establishment token). */
  token?: unknown;
}

type RefFrame =
  | { type: "call"; id: number; method: string; args: unknown[] }
  | { type: "result"; id: number; ok: true; value: unknown }
  | {
    type: "result";
    id: number;
    ok: false;
    error: { name: string; message: string; stack?: string };
  }
  | { type: "dispose" }
  /** Owner → all holders: the object was released; every proxy dies immediately. */
  | { type: "released" };

/** Frames on the pair liveness channel (one channel per owner↔holder worker pair). */
type LivenessFrame = { type: "ping" } | { type: "pong" };

/**
 * Owner side: run the real object's methods for calls arriving on the channel.
 * The closure captures only the refId (not the object), so the owner does NOT
 * pin the object while holders are alive: the object is deref'd on each call
 * and, once it is garbage-collected (all holders released), calls fail with a
 * clear "released" error and the channel closes — the reference's address has
 * died, the worker itself is unaffected.
 */
function startRefOwner(
  channel: Channel,
  refId: string,
  registry: EncodeContext["registry"],
): void {
  channel.onMessage(async (message) => {
    const frame = message as RefFrame;
    if (frame.type === "call") {
      const obj = objByRefId.get(refId)?.deref();
      if (obj === undefined) {
        // The referenced object was released: the address is dead.
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: false,
            error: serializeError(
              new Error(
                `Referenced object has been released (refId ${refId})`,
              ),
            ),
          } satisfies RefFrame,
        );
        channel.close();
        return;
      }
      const fn = (obj as Record<string, unknown>)[frame.method];
      if (typeof fn !== "function") {
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: false,
            error: serializeError(
              new Error(`No such method: "${frame.method}"`),
            ),
          } satisfies RefFrame,
        );
        return;
      }
      try {
        // args arrive encoded (may contain nested streams/refs); decode them
        const args = registry.decode(frame.args) as unknown[];
        const value = await (fn as (...a: unknown[]) => unknown).apply(
          obj,
          args,
        );
        const transfer: Transferable[] = [];
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: true,
            value: registry.encode(value, transfer),
          } satisfies RefFrame,
          transfer,
        );
      } catch (e) {
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: false,
            error: serializeError(e),
          } satisfies RefFrame,
        );
      }
    } else if (frame.type === "dispose") {
      // Optional cleanup hook on the real object (mirrors generator finally);
      // skipped if the object was already released.
      const obj = objByRefId.get(refId)?.deref();
      if (obj !== undefined) {
        (obj as { [Symbol.dispose]?: () => void })[Symbol.dispose]?.();
      }
      channel.close();
    }
  });
}

/**
 * Calling side: the transparent proxy over one channel. onRemoved fires once
 * when the proxy dies by any path (dispose/transfer/GC-release) so the
 * receiver-side dedupe table can drop it.
 */
function createRefProxy(
  channel: Channel,
  registry: DecodeContext["registry"],
  refId: string,
  onRemoved: () => void,
): RemoteRef<unknown> {
  const pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (reason: unknown) => void;
  }>();
  let nextCallId = 1;
  let closed = false;
  let unregisterRelease: () => void = () => {};

  const fail = (reason: unknown): void => {
    if (closed) return;
    closed = true;
    for (const call of pending.values()) call.reject(reason);
    pending.clear();
    channel.close();
    onRemoved();
  };

  channel.onMessage((message) => {
    const frame = message as RefFrame;
    if (frame.type === "result") {
      const call = pending.get(frame.id);
      if (!call) return;
      pending.delete(frame.id);
      if (frame.ok) call.resolve(registry.decode(frame.value));
      else call.reject(new RemoteError(frame.error));
    } else if (frame.type === "released") {
      fail(new Error("Reference released by its owner"));
    }
  });

  const call = (method: string, args: unknown[]): Promise<unknown> => {
    if (closed) return Promise.reject(new Error("Remote ref is disposed"));
    return new Promise((resolve, reject) => {
      const id = nextCallId++;
      pending.set(id, { resolve, reject });
      const transfer: Transferable[] = [];
      channel.send(
        {
          type: "call",
          id,
          method,
          args: registry.encode(args, transfer) as unknown[],
        } satisfies RefFrame,
        transfer,
      );
    });
  };

  const dispose = (): void => {
    if (closed) return;
    closed = true;
    channel.send({ type: "dispose" } satisfies RefFrame);
    channel.close();
    for (const call of pending.values()) {
      call.reject(new Error("Remote ref disposed"));
    }
    pending.clear();
    unregisterRelease();
    onRemoved();
  };

  const proxy = new Proxy({} as RemoteRef<unknown>, {
    get(_target, prop) {
      if (prop === REF_PROXY_BRAND) return true;
      if (prop === REF_ID) return refId;
      if (prop === "dispose") return dispose;
      if (prop === Symbol.dispose) return () => void dispose();
      // The proxy must not be detected as a thenable, or await behavior breaks.
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => call(prop, args);
      }
      return undefined;
    },
  });

  // Best-effort release on GC: the finalizer captures only channel/fail/dispose
  // closures — never the proxy — so it cannot keep the proxy alive.
  unregisterRelease = registerRelease(proxy, () => {
    channel.send({ type: "dispose" } satisfies RefFrame);
    fail(new Error("Remote ref garbage-collected"));
  });

  return proxy;
}

/**
 * Local call-through reference: the shape a restored reference takes when it
 * travels back to its owner. Method calls run directly on the local object
 * (zero indirection); dispose is a no-op (no channel to release). Deliberately
 * NOT marked as a remote proxy (isRemoteRef → false): it is a restored local
 * reference, and it cannot be re-transferred (the owner re-sends via
 * remoteRef(realObject) instead).
 */
function createLocalRef(obj: object): RemoteRef<unknown> {
  return new Proxy({} as RemoteRef<unknown>, {
    get(_target, prop) {
      if (prop === REF_LOCAL_BRAND) return true;
      if (prop === "dispose") return () => Promise.resolve();
      if (prop === Symbol.dispose) return () => {};
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => {
          const fn = (obj as Record<string, unknown>)[prop];
          if (typeof fn !== "function") {
            return Promise.reject(new Error(`No such method: "${prop}"`));
          }
          return Promise.resolve(
            (fn as (...a: unknown[]) => unknown).apply(obj, args),
          );
        };
      }
      return undefined;
    },
  });
}

// —— Indirect sharing: a refId-only hand-off arrives at a non-owner. ——
// The identity travels; the channel is per-holder and must be established via
// the main thread (the only peer that can route between workers). The pending
// proxy queues calls until __ref-acquired delivers a port, then materializes
// the real proxy and flushes them.
interface PendingCall {
  method: string;
  args: unknown[];
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

interface PendingEntry {
  proxy: RemoteRef<unknown>;
  calls: PendingCall[];
  registry: DecodeContext["registry"];
  refId: string;
  /** The materialized real proxy once a port arrives; later calls forward here. */
  real?: RemoteRef<unknown>;
}

// Module-level pending registry (worker/main: single-threaded, race-free).
const pendingByRefId = new Map<string, PendingEntry>();

// Context-wide identity: every arrival for a refId — fresh token or refId-only
// hand-off, through ANY registry of this context — resolves to ONE entity.
// Module-global like pendingByRefId: identity is a property of the context
// (thread), not of a single spawn registry (spawn creates one registry per
// worker, and refs flow across them). WeakRef values so a dropped proxy is
// still collectable (the dedupe must not pin the GC-release path).
const identityByRefId = new Map<string, WeakRef<RemoteRef<unknown>>>();

function createPendingProxy(
  refId: string,
  ctx: DecodeContext,
): RemoteRef<unknown> {
  const existing = pendingByRefId.get(refId);
  if (existing) return existing.proxy;
  const calls: PendingCall[] = [];
  const entry: PendingEntry = {
    proxy: undefined as never,
    calls,
    registry: ctx.registry,
    refId,
  };
  const proxy = new Proxy({} as RemoteRef<unknown>, {
    get(_target, prop) {
      if (prop === REF_PROXY_BRAND) return true;
      if (prop === REF_ID) return refId;
      if (prop === "dispose") return () => disposePending(entry);
      if (prop === Symbol.dispose) {
        return () => void disposePending(entry);
      }
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => {
          // Once materialized, forward to the real proxy (later calls must not
          // queue into the already-flushed pending list).
          if (entry.real) {
            return (entry.real as unknown as Record<
              string,
              (...a: unknown[]) => Promise<unknown>
            >)[
              prop
            ](...args);
          }
          // First call triggers the acquire; subsequent calls queue too.
          triggerAcquire(refId);
          return new Promise<unknown>((resolve, reject) => {
            calls.push({ method: prop, args, resolve, reject });
          });
        };
      }
      return undefined;
    },
  });
  entry.proxy = proxy;
  pendingByRefId.set(refId, entry);
  return proxy;
}

function disposePending(entry: PendingEntry): Promise<void> {
  // Materialized: disposing the entity disposes the real proxy (which closes
  // the channel and rejects in-flight calls).
  if (entry.real) entry.real.dispose();
  pendingByRefId.delete(entry.refId);
  identityByRefId.delete(entry.refId);
  for (const c of entry.calls) {
    c.reject(new Error("Remote ref disposed before acquire completed"));
  }
  entry.calls.length = 0;
  return Promise.resolve();
}

/**
 * Materialize a pending entry: a port for the ref arrived (a direct fresh
 * token, or the acquire-completion port). The pending proxy stays the single
 * entity for this refId — later calls forward to the real proxy — and is
 * recorded in the dedupe table, so ANY later arrival path (direct token or
 * refId-only hand-off) returns the same object. Identity is path-independent.
 */
function materialize(
  entry: PendingEntry,
  port: MessagePort,
  liveness?: { ownerId?: string; livenessPort?: MessagePort },
): void {
  if (entry.real) {
    port.close(); // already materialized: the extra port is redundant
    return;
  }
  const registry = entry.registry;
  const channel = connectChannel(port);
  registry.registerChannel(channel);
  const pair = liveness
    ? ensureHolderPair(liveness.ownerId, liveness.livenessPort)
    : undefined;
  const real = createRefProxy(channel, registry, entry.refId, () => {
    // The real proxy's death does not end the ENTITY: identityByRefId keeps
    // pointing at the pending proxy (the single entity), whose dispose is the
    // entity's lifetime. Nothing to clean here.
  });
  // When the OWNER dies (reverse detection: its pings stop), this proxy — and
  // every other proxy of that owner — fails together; no calls hang forever.
  pair?.kills.add(() => {
    real.dispose();
  });
  entry.real = real;
  pendingByRefId.delete(entry.refId);
  identityByRefId.set(entry.refId, new WeakRef(entry.proxy));
  const calls = entry.calls;
  entry.calls = [];
  for (const c of calls) {
    const p =
      (real as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>)
        [
          c.method
        ](...c.args);
    p.then(c.resolve, c.reject);
  }
}

/**
 * Materialize a pending entry from a Mux token (instead of a port): the channel
 * is established by the token on our transport. Liveness is skipped on Mux —
 * the per-holder pair plane is messageport-only (documented in the module
 * header); the ref still works, it just has no owner-death sweep.
 */
function materializeToken(
  entry: PendingEntry,
  transport: DecodeContext["transport"],
  token: { __mux: "open"; ch: number },
  liveness?: { ownerId?: string; livenessPort?: MessagePort },
): void {
  if (entry.real) {
    closeTokenChannel(transport, token);
    return;
  }
  const channel = connectToken(transport, token);
  materializeChannel(entry, channel, liveness);
}

/**
 * Materialize a pending entry from an already-connected Channel (the main-side
 * local end of a Mux acquire, or a token-connected channel). The pending proxy
 * stays the single entity for this refId — later calls forward to the real
 * proxy — and is recorded in the dedupe table, so ANY later arrival path
 * returns the same object. Identity is path-independent.
 */
function materializeChannel(
  entry: PendingEntry,
  channel: Channel,
  liveness?: { ownerId?: string; livenessPort?: MessagePort },
): void {
  if (entry.real) {
    channel.close(); // already materialized: the extra channel is redundant
    return;
  }
  const registry = entry.registry;
  registry.registerChannel(channel);
  const pair = liveness
    ? ensureHolderPair(liveness.ownerId, liveness.livenessPort)
    : undefined;
  const real = createRefProxy(channel, registry, entry.refId, () => {
    // The real proxy's death does not end the ENTITY: identityByRefId keeps
    // pointing at the pending proxy (the single entity), whose dispose is the
    // entity's lifetime. Nothing to clean here.
  });
  // When the OWNER dies (reverse detection: its pings stop), this proxy — and
  // every other proxy of that owner — fails together; no calls hang forever.
  pair?.kills.add(() => {
    real.dispose();
  });
  entry.real = real;
  pendingByRefId.delete(entry.refId);
  identityByRefId.set(entry.refId, new WeakRef(entry.proxy));
  const calls = entry.calls;
  entry.calls = [];
  for (const c of calls) {
    const p =
      (real as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>)
        [
          c.method
        ](...c.args);
    p.then(c.resolve, c.reject);
  }
}

/** The default (messageport-shaped) transport used by main-side decodes without a real transport. */
function defaultTransport(): DecodeContext["transport"] {
  return {
    kind: "messageport",
    send() {},
    onMessage() {},
    openChannel() {
      throw new Error("no transport");
    },
    onChannel() {},
    close() {},
  };
}

/** Close the channel established by a Mux token (best-effort; the transport owns it). */
function closeTokenChannel(
  transport: DecodeContext["transport"],
  token: unknown,
): void {
  if (typeof token !== "object" || token === null) return;
  const t = token as { __mux?: unknown; ch?: unknown };
  if (t.__mux !== "open" || typeof t.ch !== "number") return;
  const orphan = transport.claimOrphan?.(t.ch);
  if (orphan) orphan.close();
}

/** __ref-acquired: the acquire-completion port/token arrived — materialize and flush. */
function onRefAcquiredInner(
  frame: {
    refId: string;
    port?: MessagePort;
    token?: unknown;
    channel?: Channel;
    ownerId?: string;
    livenessPort?: MessagePort;
  },
  transport: DecodeContext["transport"],
  channelOverride?: Channel,
): void {
  const entry = pendingByRefId.get(frame.refId);
  if (!entry) {
    if (channelOverride !== undefined) channelOverride.close();
    else if (frame.port !== undefined) frame.port.close();
    else if (frame.token !== undefined) {
      closeTokenChannel(transport, frame.token);
    }
    return;
  }
  if (channelOverride !== undefined) {
    materializeChannel(entry, channelOverride, {
      ownerId: frame.ownerId,
      livenessPort: frame.livenessPort,
    });
    return;
  }
  if (frame.port !== undefined) {
    materialize(entry, frame.port, {
      ownerId: frame.ownerId,
      livenessPort: frame.livenessPort,
    });
    return;
  }
  // Mux acquire: a token for the per-holder channel on OUR transport.
  materializeToken(
    entry,
    transport,
    frame.token as { __mux: "open"; ch: number },
    { ownerId: frame.ownerId, livenessPort: frame.livenessPort },
  );
}

/** __serve-ref: the owner must register a fresh per-holder channel for this ref. */
function onServeRefInner(
  frame: {
    refId: string;
    port?: MessagePort;
    token?: unknown;
    holderId?: string;
    livenessPort?: MessagePort;
  },
  registry: EncodeContext["registry"] | DecodeContext["registry"],
): void {
  const obj = objByRefId.get(frame.refId)?.deref();
  if (obj === undefined) {
    // Owner no longer holds the object: nothing to serve. Close the arriving
    // channel end (port or token) so neither side leaks the channel.
    if (frame.port !== undefined) frame.port.close();
    else if (frame.token !== undefined) {
      closeTokenChannel(
        getActiveTransport() ?? defaultTransport(),
        frame.token,
      );
    }
    return;
  }
  const transport = getActiveTransport();
  if (frame.token !== undefined && transport !== undefined) {
    // Mux path: no transferred port, and the liveness plane is messageport-only
    // (per-holder pair channels). Connect the peer-opened channel on our
    // transport and serve the ref over it.
    const channel = connectToken(
      transport,
      frame.token as { __mux: "open"; ch: number },
    );
    registry.registerChannel(channel);
    startRefOwner(channel, frame.refId, registry); // closure captures only the refId
    trackOwnerChannel(frame.refId, channel, frame.holderId);
    return;
  }
  if (frame.port === undefined) return;
  // Liveness plane (first serve of this holder only): owner-pull heartbeats.
  ensureOwnerPair(frame.holderId, frame.livenessPort);
  const channel = connectChannel(frame.port, {
    onClosed: () => {
      untrackOwnerChannel(frame.refId, channel);
      registry.unregisterChannel(channel);
    },
  });
  registry.registerChannel(channel);
  startRefOwner(channel, frame.refId, registry); // closure captures only the refId
  trackOwnerChannel(frame.refId, channel, frame.holderId);
}

// Control handlers are module-global (worker/main single-threaded).
const onWorkerId = (frame: ControlFrame): void => {
  workerIdPrefix = frame.refId; // reuse the refId field as the id carrier
};
const onRefAcquiredFrame = (frame: ControlFrame): void => {
  const transport = getActiveTransport();
  // The Mux acquire path is local to the main thread: the requester is main,
  // and the channel end is handed over directly (no wire frame).
  if (frame.channel !== undefined) {
    onRefAcquiredInner(frame, transport ?? defaultTransport(), frame.channel);
    return;
  }
  if (frame.port === undefined && frame.token === undefined) return;
  onRefAcquiredInner(frame, transport ?? defaultTransport());
};
const onServeRefFrame = (frame: ControlFrame): void => {
  const registry = getActiveRegistry();
  if (!registry) return; // no active worker registry (should not happen in a worker)
  // __serve-ref carries either a transferred port or a Mux token.
  if (frame.port === undefined && frame.token === undefined) return;
  onServeRefInner({
    refId: frame.refId,
    port: frame.port,
    token: frame.token,
    holderId: frame.holderId,
    livenessPort: frame.livenessPort,
  }, registry);
};
const onHolderDead = (_frame: ControlFrame): void => {
  // Main-side only: the liveness sweep of a dead holder worker posts one
  // __holder-dead notice per death. Nothing to do here beyond acknowledging it
  // (worker-side holders are cleaned up when the owner releases their channels).
};

registerControlHandler("__worker-id", onWorkerId);
registerControlHandler("__ref-acquired", onRefAcquiredFrame);
registerControlHandler("__serve-ref", onServeRefFrame);
// Registered everywhere, meaningful on the main thread: the death notice keeps
// the main-side codec's module graph consistent (the notice frame is routed to
// every module instance).
registerControlHandler("__holder-dead", onHolderDead);

/**
 * Register an owner-side channel for a refId. Cleanup on close is wired via
 * the channel's onClosed option at creation (openChannel/connectChannel call
 * sites); this helper only maintains the set membership.
 */
function trackOwnerChannel(
  refId: string,
  channel: Channel,
  holderId: string | undefined,
): void {
  let set = ownerChannelsByRefId.get(refId);
  if (!set) {
    set = new Set();
    ownerChannelsByRefId.set(refId, set);
  }
  set.add(channel);
  channelRefId.set(channel, refId);
  // Record which holder this channel belongs to: a dead holder is then released
  // in one batch instead of per-channel sweeps.
  if (holderId !== undefined) channelHolderId.set(channel, holderId);
  // First open channel: hold the object strongly (alive during holder use).
  const obj = objByRefId.get(refId)?.deref();
  if (obj !== undefined && !ownerStrongRefs.has(refId)) {
    ownerStrongRefs.set(refId, obj);
  }
}

/**
 * Remove an owner-side channel from the refId's set (called on close).
 */
function untrackOwnerChannel(refId: string, channel: Channel): void {
  const set = ownerChannelsByRefId.get(refId);
  if (set) {
    set.delete(channel);
    channelRefId.delete(channel);
    channelHolderId.delete(channel);
    if (set.size === 0) {
      ownerChannelsByRefId.delete(refId);
      // Last channel closed: the object is now collectable (mode-2 release).
      ownerStrongRefs.delete(refId);
    }
  }
}

export const remoteRefCodec: Codec<RemoteRef<unknown>> = {
  tag: "remote-ref",
  matches(v: unknown): v is RemoteRef<unknown> {
    return typeof v === "object" && v !== null &&
      ((v as { [REF_BRAND]?: unknown })[REF_BRAND] === true ||
        (v as { [REF_PROXY_BRAND]?: unknown })[REF_PROXY_BRAND] === true);
  },
  encode(v: RemoteRef<unknown>, ctx: EncodeContext): unknown {
    const ref = v as unknown as Record<PropertyKey, unknown>;
    if (ref[REF_BRAND] === true) {
      // Fresh token: the owner produces a reference with a new channel. The
      // closure captures only the refId, not the object.
      const obj = (ref as unknown as RefToken).obj as object;
      const refId = refIdFor(obj);
      const { channel, peerPort, token } = openChannel(ctx, {
        onClosed: () => {
          untrackOwnerChannel(refId, channel);
          ctx.registry.unregisterChannel(channel);
        },
      });
      ctx.registry.registerChannel(channel);
      startRefOwner(channel, refId, ctx.registry);
      // Fresh tokens travel to the main thread only (no holder worker id).
      trackOwnerChannel(refId, channel, undefined);
      const handle: RefHandle = {
        [CODEC_PLACEHOLDER_KEY]: "remote-ref",
        refId,
        ...(peerPort !== undefined ? { port: peerPort } : { token }),
      };
      return handle;
    }
    // Sharing semantics: a proxy encodes as its refId token only. The holder's
    // proxy stays alive; the receiver acquires a fresh per-holder channel via
    // the main thread on first use. Any number of holders can share the same
    // identity, each with its own channel to the owner.
    const refId = ref[REF_ID] as string;
    return {
      [CODEC_PLACEHOLDER_KEY]: "remote-ref",
      refId,
    } satisfies RefHandle;
  },
  decode(placeholder: RefHandle, ctx: DecodeContext): RemoteRef<unknown> {
    const { refId, port, token } = placeholder;
    // Back to the owner? Restore a local call-through reference (collapse):
    // the reference completed its journey home; close any channels still open
    // for it and call the real object directly. Works for both fresh (port /
    // token present) and refId-only hand-off arrivals.
    const obj = objByRefId.get(refId)?.deref();
    if (obj !== undefined) {
      // Restore: the reference came home. Under sharing semantics other
      // holders keep their own channels, so only the arriving channel (if any)
      // is closed — never the whole per-holder set.
      if (port !== undefined) port.close();
      if (token !== undefined) closeTokenChannel(ctx.transport, token);
      return createLocalRef(obj);
    }
    // Identity is path- and registry-independent: every arrival for a refId
    // (fresh token or refId-only hand-off, through any spawn registry of this
    // context) resolves to ONE entity via the module-level identity table.
    // `===` therefore holds across all arrival paths.
    const existing = identityByRefId.get(refId)?.deref();
    if (existing) {
      if (port !== undefined) port.close();
      if (token !== undefined) closeTokenChannel(ctx.transport, token);
      return existing;
    }
    const entry = pendingByRefId.get(refId);
    if (entry) {
      // Already known as a pending (refId-only) arrival: attach this direct
      // channel if one arrived — the pending proxy becomes the single entity.
      if (port !== undefined) materialize(entry, port);
      else if (token !== undefined) {
        materializeToken(
          entry,
          ctx.transport,
          token as { __mux: "open"; ch: number },
        );
      }
      return entry.proxy;
    }
    if (port === undefined && token === undefined) {
      // A refId-only hand-off arrived at a non-owner. The identity travels;
      // the channel is a per-holder connection that must be established via
      // the main thread (the only peer that can route between workers). Return
      // a pending proxy: its first call triggers the acquire.
      return createPendingProxy(refId, ctx);
    }
    // First direct arrival: create the entity and register it in the identity
    // table (subsequent arrivals of any path return the same object).
    const channel = port !== undefined
      ? connectChannel(port)
      : connectToken(ctx.transport, token as { __mux: "open"; ch: number });
    ctx.registry.registerChannel(channel);
    const proxy = createRefProxy(channel, ctx.registry, refId, () => {
      identityByRefId.delete(refId);
    });
    identityByRefId.set(refId, new WeakRef(proxy));
    return proxy;
  },
  onRegistryFail(): void {
    // failAll: every live entity of this context dies. The module-level
    // identity table holds exactly the context's entities (pending or direct);
    // open channels are closed by the registry's failAll() via registerChannel.
    for (const weakRef of identityByRefId.values()) {
      weakRef.deref()?.dispose();
    }
    identityByRefId.clear();
    for (const entry of pendingByRefId.values()) {
      for (const c of entry.calls) {
        c.reject(new Error("Remote ref garbage-collected"));
      }
      entry.calls.length = 0;
    }
    pendingByRefId.clear();
  },
};
