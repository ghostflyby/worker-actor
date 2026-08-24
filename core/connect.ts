/**
 * Cross-connection actor addressing: open a logical channel on a Transport
 * targeting a specific actor, without a spawn() call. This is the counterpart
 * to serveNode's `__open-actor` protocol frame — the peer names which served
 * api the channel should run, and the node serves that api over the channel.
 *
 *   connectActor(transport)      — single-actor transports (spawnProcess /
 *                                  serveProcess): the only api is the main one,
 *                                  no name needed.
 *   connectActor(transport, name) — a node (serveNode): name selects which of
 *                                  the served actors the channel runs.
 *
 * The channel carries the same RPC frames as spawn's main channel; run it
 * through createRpcProxy to call the actor, or makeRpcHandler to serve it.
 * Returns { channel, transport } so both the channel frames and the transport
 * (for Mux-aware codec values / sub-channel establishment) are available.
 */

import type { Channel } from "./channel.ts";
import type { Transport } from "./transport.ts";

export interface ConnectedActor {
  /** The logical channel opened on the transport. */
  channel: Channel;
  /** The transport the channel rides on (codec encode/decode needs it). */
  transport: Transport;
}

/** Open an actor channel on a transport, naming the served api on a node. */
export function connectActor(
  transport: Transport,
  name?: string,
): ConnectedActor {
  const opened = transport.openChannel();
  // Mux handshake: the token is a control frame on the main channel — the
  // peer's Mux opens its end + acks (and the channel may carry data before the
  // __open-actor frame is processed, so send token first, name second).
  transport.send(opened.token);
  if (name !== undefined) {
    transport.send({ type: "__open-actor", name, token: opened.token });
  }
  return { channel: opened.channel, transport };
}

/**
 * Open a named actor channel on a node transport (serveNode). Same as
 * connectActor(transport, name) — a convenience alias so callers state intent.
 */
export function openNodeActor(
  transport: Transport,
  name: string,
): ConnectedActor {
  return connectActor(transport, name);
}
