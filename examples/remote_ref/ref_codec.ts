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
 *   - Handing a reference over is a MOVE: the underlying channel port is
 *     transferred to the new holder, so the reference stays single-hop direct
 *     through any number of hand-offs (A → B → C → ... keeps a direct channel
 *     to the owner; no proxy chains). The previous holder's proxy becomes dead.
 *   - A reference that travels back to its owner is RESTORED: the owner's
 *     registry recognizes the refId, collapses it into a local call-through
 *     reference, and the channel is closed — the owner calls the real object
 *     directly, with zero indirection.
 *   - Only the owner can produce fresh references (remoteRef of an object it
 *     owns); a proxy holder can only move it. Ownership of the real object is
 *     unaffected by hand-offs — it stays where it was created.
 *
 * Wire protocol (one channel per reference):
 *   proxy → owner  { type: "call"; id; method; args }         // args go through the registry
 *   owner → proxy  { type: "result"; id; ok: true; value }    // value too (nested streams/refs work)
 *   owner → proxy  { type: "result"; id; ok: false; error }
 *   proxy → owner  { type: "dispose" }                        // explicit dispose or GC
 *
 * Lifecycle: explicit dispose(), GC-based release (FinalizationRegistry), and
 * failAll (the registry closes the channel). The owner runs the real object's
 * [Symbol.dispose] hook when the reference is released.
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
  getCodecState,
} from "../../core/codec.ts";
import {
  type Channel,
  connectChannel,
  openChannel,
  registerRelease,
} from "../../core/channel.ts";
import { RemoteError, serializeError } from "../../core/protocol.ts";

const REF_BRAND = Symbol.for("worker-actor-example.remote-ref");
/** Marks the receiving-side proxy; lets a peer detect "this is a reference, not a value". */
const REF_PROXY_BRAND = Symbol.for("worker-actor-example.remote-ref.proxy");
/** Marks a restored local call-through reference (traveled back to its owner). */
const REF_LOCAL_BRAND = Symbol.for("worker-actor-example.remote-ref.local");
/** refId of the real object this proxy refers to (for hand-off/restore). */
const REF_ID = Symbol.for("worker-actor-example.remote-ref.id");
/** Detaches the underlying port for a hand-off (move semantics); the proxy then dies. */
const REF_DETACH = Symbol.for("worker-actor-example.remote-ref.detach");

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
// refId must be unique across processes: every worker is a separate process
// with its own module state, so a bare counter would collide (B's object #1
// vs C's object #1). A random per-process prefix keeps ids globally unique.
const PROCESS_ID = Math.random().toString(36).slice(2);
let localRefCount = 0;
const refIdByObj = new WeakMap<object, string>();
const objByRefId = new Map<string, WeakRef<object>>();
const ownerChannelsByRefId = new Map<string, Set<Channel>>();
const refIdFinalizer = new FinalizationRegistry<string>((id) => {
  // The real object is gone: its reference identity dies with it.
  objByRefId.delete(id);
  ownerChannelsByRefId.delete(id);
});

function refIdFor(obj: object): string {
  const existing = refIdByObj.get(obj);
  if (existing !== undefined) return existing;
  const id = `${PROCESS_ID}:${++localRefCount}`;
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
  /** Present on a fresh owner-produced reference; absent on a refId-only hand-off. */
  port?: MessagePort;
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
  | { type: "dispose" };

/** Owner side: run the real object's methods for calls arriving on the channel. */
function startRefOwner(
  channel: Channel,
  obj: unknown,
  registry: EncodeContext["registry"],
): void {
  channel.onMessage(async (message) => {
    const frame = message as RefFrame;
    if (frame.type === "call") {
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
      // Optional cleanup hook on the real object (mirrors generator finally)
      (obj as { [Symbol.dispose]?: () => void })[Symbol.dispose]?.();
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

  channel.onMessage((message) => {
    const frame = message as RefFrame;
    if (frame.type === "result") {
      const call = pending.get(frame.id);
      if (!call) return;
      pending.delete(frame.id);
      if (frame.ok) call.resolve(registry.decode(frame.value));
      else call.reject(new RemoteError(frame.error));
    }
  });

  const fail = (reason: unknown): void => {
    if (closed) return;
    closed = true;
    for (const call of pending.values()) call.reject(reason);
    pending.clear();
    channel.close();
  };

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

  // Move semantics: the reference's identity travels on; the holder's channel
  // is closed (it was a per-holder connection to the owner; Deno won't let a
  // handler-attached port be transferred, and the owner restores by refId
  // alone). The local proxy dies after this.
  const detachForTransfer = (): void => {
    if (closed) throw new Error("Remote ref is disposed; cannot transfer");
    closed = true;
    for (const call of pending.values()) {
      call.reject(new Error("Remote ref transferred"));
    }
    pending.clear();
    unregisterRelease();
    onRemoved();
    channel.close();
  };

  const proxy = new Proxy({} as RemoteRef<unknown>, {
    get(_target, prop) {
      if (prop === REF_PROXY_BRAND) return true;
      if (prop === REF_ID) return refId;
      if (prop === REF_DETACH) return detachForTransfer;
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
    onRemoved();
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

interface RefCodecState {
  /**
   * Dedupe per receiver: the same refId (same identity) reuses one proxy.
   * WeakRef values so a dropped proxy can still be garbage-collected (the GC
   * release path must not be pinned by the dedupe table).
   */
  proxyByRefId: Map<string, WeakRef<RemoteRef<unknown>>>;
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
      // Fresh token: the owner produces a reference with a new channel.
      const obj = (ref as unknown as RefToken).obj as object;
      const refId = refIdFor(obj);
      const { channel, peerPort } = openChannel(ctx);
      ctx.registry.registerChannel(channel);
      startRefOwner(channel, obj, ctx.registry);
      // Track the owner-side channel so a round-trip back to the owner can
      // close it (the channel has completed its journey).
      let set = ownerChannelsByRefId.get(refId);
      if (!set) {
        set = new Set();
        ownerChannelsByRefId.set(refId, set);
      }
      set.add(channel);
      return {
        [CODEC_PLACEHOLDER_KEY]: "remote-ref",
        refId,
        port: peerPort,
      } satisfies RefHandle;
    }
    // Move semantics: the reference's identity travels on; the holder's channel
    // is closed (it was a per-holder connection to the owner). No port is
    // transferred — Deno refuses to transfer ports that have a message handler
    // attached, and the owner restores by refId alone. The local proxy dies.
    const refId = ref[REF_ID] as string;
    const detach = ref[REF_DETACH] as () => void;
    detach();
    return {
      [CODEC_PLACEHOLDER_KEY]: "remote-ref",
      refId,
    } satisfies RefHandle;
  },
  decode(placeholder: RefHandle, ctx: DecodeContext): RemoteRef<unknown> {
    const { refId, port } = placeholder;
    // Back to the owner? Restore a local call-through reference (collapse):
    // the reference completed its journey home; close any channels still open
    // for it and call the real object directly. Works for both fresh (port
    // present) and refId-only hand-off arrivals.
    const obj = objByRefId.get(refId)?.deref();
    if (obj !== undefined) {
      port?.close();
      const channels = ownerChannelsByRefId.get(refId);
      if (channels) {
        for (const c of channels) c.close();
        ownerChannelsByRefId.delete(refId);
      }
      return createLocalRef(obj);
    }
    if (port === undefined) {
      // A refId-only hand-off arrived at a non-owner. The channel is a
      // per-holder connection to the owner; a proxy holder cannot re-establish
      // it for a third party, so this is unsupported — fail loudly instead of
      // silently producing a dead reference.
      throw new Error(
        `remote-ref hand-off for unknown refId "${refId}": only the owner can restore, ` +
          "and channel hand-offs to third parties are unsupported",
      );
    }
    // Receiver side: the same identity reuses one proxy (refs are comparable).
    const state = getCodecState<RefCodecState>(ctx, remoteRefCodec, () => ({
      proxyByRefId: new Map(),
    }));
    const existing = state.proxyByRefId.get(refId)?.deref();
    if (existing) {
      port.close();
      return existing;
    }
    const channel = connectChannel(port);
    ctx.registry.registerChannel(channel);
    const proxy = createRefProxy(channel, ctx.registry, refId, () => {
      state.proxyByRefId.delete(refId);
    });
    state.proxyByRefId.set(refId, new WeakRef(proxy));
    return proxy;
  },
  onRegistryFail(state: RefCodecState | undefined): void {
    if (!state) return;
    for (const weakRef of state.proxyByRefId.values()) {
      weakRef.deref()?.dispose();
    }
    state.proxyByRefId.clear();
    // open channels are closed by the registry's failAll() via registerChannel.
  },
};
