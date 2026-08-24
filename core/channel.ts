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
  if (transport.kind === "framed") {
    const opened = transport.openChannel();
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
