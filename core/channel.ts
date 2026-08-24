/**
 * High-level channel abstraction for codec authors.
 *
 * A Codec may need a dedicated cross-thread channel with its own wire protocol —
 * not just a cloneable value. Examples: streaming elements (iterable codec),
 * abort propagation (abort-signal codec), or a custom marshal-by-ref protocol
 * (see examples/remote_ref). This module is the high-level counterpart to raw
 * MessageChannel handling: it owns channel creation, port transfer, closure and
 * GC-based release, so a codec only supplies its frames.
 *
 * The library deliberately provides no automatic protocol on a channel: a codec
 * gets the channel and defines its own frames. What the library guarantees:
 *   - openChannel(): create a MessageChannel and automatically add the peer port
 *     to ctx.transfer (transferred with the placeholder).
 *   - connectChannel(): wrap a transferred port on the receiving side.
 *   - Both ends register with the registry (ctx.registry.registerChannel) so
 *     failAll() closes every open channel when the actor dies.
 *   - registerRelease(): FinalizationRegistry wrapper for GC-based release —
 *     the codec decides what releasing means (e.g. send a "release" frame and
 *     close), and unregistering on explicit close keeps release single.
 */

import type { EncodeContext } from "./codec.ts";

export interface ChannelOptions {
  /** Called once when this endpoint closes: by close(), peer closure, or an error. */
  onClosed?: () => void;
  /** Called when a message fails to deserialize; the channel then closes. */
  onMessageError?: () => void;
}

export interface Channel {
  readonly closed: boolean;
  /** The underlying MessagePort; transferring it to a peer moves the channel (reference hand-off). */
  readonly port: MessagePort;
  /**
   * How this channel crosses the boundary: "messageport" (the peer end is a
   * MessagePort, transferable with the placeholder) or "framed" (the peer end
   * is established by a Mux token over the parent transport). Codecs that can
   * work either way (e.g. iterable) use this to choose their frame format;
   * codecs that fundamentally require a MessagePort (e.g. remote-ref's
   * liveness planes) only run on "messageport".
   */
  readonly kind?: "messageport" | "framed";
  /** Send a frame to the peer; transferable ports/buffers go in the second argument. */
  send(message: unknown, transfer?: Transferable[]): void;
  /** Register the inbound frame handler (a second call replaces the first). */
  onMessage(handler: (message: unknown) => void): void;
  /** Close this endpoint; idempotent. */
  close(): void;
}

/**
 * The result of openChannel(): the local Channel plus how the peer end is
 * handed over. On a messageport context this is a transferable MessagePort
 * (`peerPort`); on a framed context `peerPort` is undefined and `token` is a
 * Mux value the peer uses to rebuild the channel. Existing codecs read
 * `peerPort` (messageport-only); transport-aware codecs branch on it.
 */
export interface ChannelPeer {
  channel: Channel;
  /** Transferable peer port (messageport transports); undefined on framed. */
  peerPort?: MessagePort;
  /** Mux token (framed transports); undefined on messageport. */
  token?: unknown;
}

/**
 * Open a new logical channel for a value crossing the boundary. On a
 * messageport context this creates a MessageChannel and transfers port2 with
 * the placeholder (current behavior). On a framed context it delegates to the
 * transport's openChannel() and returns the Mux token instead — the peer
 * rebuilds the channel from the token via onChannel.
 */
export function openChannel(
  ctx: EncodeContext,
  options?: ChannelOptions,
): ChannelPeer {
  const transport = ctx.transport;
  // Mux transports (framed byte streams and message channels) establish
  // logical channels via transport.openChannel(): the token is sent as a Mux
  // control frame on the main channel (the peer's onChannel fires and acks),
  // AND rides in the placeholder so the peer's decode can connect the token
  // to the inbound channel.
  if (transport.kind === "framed" || transport.kind === "message") {
    const opened = transport.openChannel();
    transport.send(opened.token);
    return { channel: opened.channel, token: opened.token };
  }
  const { port1, port2 } = new MessageChannel();
  ctx.transfer.push(port2);
  return { channel: wrapPort(port1, options), peerPort: port2 };
}

/** Wrap a transferred peer port as a Channel on the receiving side. */
export function connectChannel(
  port: MessagePort,
  options?: ChannelOptions,
): Channel {
  return wrapPort(port, options);
}

/** Wrap a local/transferred MessagePort as a Channel (exported for transport adapters). */
export function wrapPort(
  port: MessagePort,
  options: ChannelOptions = {},
): Channel {
  let closed = false;
  let handler: ((message: unknown) => void) | undefined;

  port.onmessage = (ev: MessageEvent<unknown>) => {
    if (!closed) handler?.(ev.data);
  };
  port.onmessageerror = () => {
    options.onMessageError?.();
    close();
  };

  const close = (): void => {
    if (closed) return;
    closed = true;
    try {
      port.close();
    } catch {
      // The port may already be detached by a transfer (reference hand-off);
      // close() on a detached port throws — closing is still "done".
    }
    options.onClosed?.();
  };

  return {
    get closed(): boolean {
      return closed;
    },
    get port(): MessagePort {
      return port;
    },
    send(message: unknown, transfer?: Transferable[]): void {
      if (closed) return;
      port.postMessage(message, transfer ? { transfer } : undefined);
    },
    onMessage(h: (message: unknown) => void): void {
      handler = h;
    },
    close,
  };
}

/**
 * A Mux channel carries an internal `_ch` id (set by createMux's makeChannel).
 * Token-based channel establishment matches an inbound channel to a pending
 * token by that id. Exported for the token-connection helper below.
 */
export interface MuxChannelShape extends Channel {
  _ch?: number;
}

/**
 * Resolve a Mux token ({ __mux: "open", ch }) to the peer's Channel on the
 * transport. The channel may already have arrived (transport.onChannel fired
 * before this call) or arrive later; the returned Channel is the same object
 * either way. The transport's onChannel is registered once and dispatches to
 * all pending tokens by channel id.
 */
export function connectToken(
  transport: {
    onChannel(h: (channel: Channel) => void): void;
    claimOrphan?(ch: number): Channel | undefined;
  },
  token: { __mux: "open"; ch: number },
): Channel {
  const chId = token.ch;
  // Per-transport dispatcher: one onChannel registration routes every inbound
  // Mux channel to the pending entry for its id. Channels that arrived before
  // any claim (orphaned in the Mux) are taken via claimOrphan.
  let dispatcher = dispatchers.get(transport);
  if (!dispatcher) {
    const pendingByCh = new Map<number, PendingToken>();
    transport.onChannel((channel) => {
      const mux = channel as MuxChannelShape;
      if (mux._ch === undefined) return;
      const entry = pendingByCh.get(mux._ch);
      if (entry) {
        pendingByCh.delete(mux._ch);
        entry.channel = channel;
        entry.resolve?.();
      }
    });
    dispatcher = { pendingByCh };
    dispatchers.set(transport, dispatcher);
  }
  // The channel may already be orphaned in the Mux (open control arrived before
  // decode). Claim it so its data isn't lost while we wait.
  const orphan = transport.claimOrphan?.(chId);
  if (orphan) {
    const existing = dispatcher.pendingByCh.get(chId);
    if (existing && !existing.channel) {
      dispatcher.pendingByCh.delete(chId);
      existing.channel = orphan;
      existing.resolve?.();
    }
    return orphan;
  }
  const existing = dispatcher.pendingByCh.get(chId);
  if (existing && existing.channel) return existing.channel;
  // Not arrived yet (or arrived but unresolved): return a proxy that resolves
  // once the channel arrives.
  const entry: PendingToken = existing ?? {
    channel: undefined,
    resolve: undefined,
  };
  if (!existing) {
    entry._ready = new Promise<void>((resolve) => {
      entry.resolve = resolve;
    });
    dispatcher.pendingByCh.set(chId, entry);
  }
  const ready = entry._ready ?? Promise.resolve();
  const proxy: Channel = {
    get closed(): boolean {
      return entry.channel?.closed ?? true;
    },
    get port(): MessagePort {
      throw new Error("Mux channels have no MessagePort");
    },
    send(message, transfer) {
      entry.channel?.send(message, transfer);
    },
    onMessage(h) {
      if (entry.channel) entry.channel.onMessage(h);
      else void ready.then(() => entry.channel!.onMessage(h));
    },
    close() {
      entry.channel?.close();
    },
  };
  return proxy;
}

interface PendingToken {
  channel: Channel | undefined;
  resolve: (() => void) | undefined;
  _ready?: Promise<void>;
}

// Transport → pending-token dispatcher (module-level; a transport has exactly
// one onChannel handler, shared by all tokens).
const dispatchers = new WeakMap<
  { onChannel(h: (channel: Channel) => void): void },
  { pendingByCh: Map<number, PendingToken> }
>();

/** GC-based release: see the module docs. Returns an unregister function (call on explicit close). */
export function registerRelease(
  target: object,
  onReleased: () => void,
): () => void {
  const token = {};
  releaseRegistry.register(target, onReleased, token);
  return () => releaseRegistry.unregister(token);
}

/**
 * Finalizers are best-effort by spec: timing is uncontrollable and engines may
 * skip them. Explicit release paths (done/error/return/dispose/failAll) are the
 * deterministic ones; this registry is a safety net. The held callback captures
 * only onReleased — never the target — so registration can't keep it alive.
 */
const releaseRegistry = new FinalizationRegistry<() => void>((fn) => fn());
