/**
 * Transport: the connection abstraction that unifies every actor channel type —
 * Worker MessagePort, fork IPC, process stdio, WebSocket, TCP. A Transport
 * carries the main message channel (RPC / control frames) and is responsible
 * for opening logical sub-channels via openChannel(), exactly like the
 * WebTransport shape (a connection + createBidirectionalStream()).
 *
 * Two kinds:
 *   - messageport: messages are delivered natively (structured clone);
 *     openChannel() produces a real MessagePort (transferred with the
 *     placeholder) — the natural channel-creation form in the structured-clone
 *     world.
 *   - framed: messages run through the frame layer (core/frame.ts, v8 +
 *     length prefix); openChannel() produces a { __mux: "open", ch } token and
 *     multiplexes the logical channel over the existing connection.
 *
 * Channel establishment on a framed transport:
 *   opener:  openChannel() → { channel, token }  (token = { __mux:"open", ch })
 *   opener:  sends the token to the peer inside a placeholder
 *   peer:    onMuxOpen → builds its end of channel ch → responds
 *            { __mux:"open", ch } back on the main channel
 *   opener:  receives the "open" control → resolves the pending channel
 *   either side may then send { __mux:"data", ch, value } frames
 *
 * The frame layer is pure WHATWG Streams (ReadableStream / WritableStream);
 * Node stream.Duplex is intentionally not bridged.
 */

import type { Channel } from "./channel.ts";
import { wrapPort } from "./channel.ts";
import { createDecoder, createEncoder, type MuxFrame } from "./frame.ts";

export type TransportKind = "messageport" | "framed";

export interface Transport {
  readonly kind: TransportKind;
  /** Send a frame on the main message channel; transferables only on messageport transports. */
  send(frame: unknown, transfer?: Transferable[]): void;
  /**
   * Register the main-channel inbound handler (a second call replaces the
   * first). The event is MessageEvent-shaped ({ data, ports }) so every
   * transport — including the Worker path converted via fromMessagePort —
   * delivers transferred MessagePorts uniformly.
   */
  onMessage(handler: (ev: TransportMessage) => void): void;
  /**
   * Open a new logical channel. Returns the local Channel plus the token to
   * hand to the peer: a MessagePort (transferable) on messageport transports,
   * a { __mux: "open", ch } control value on framed transports. The peer
   * rebuilds the channel via onChannel.
   */
  openChannel(): { channel: Channel; token: unknown };
  /** Register the handler for channels opened by the peer (last handler wins). */
  onChannel(handler: (channel: Channel) => void): void;
  /** Close the transport; idempotent. Closes the main channel and every sub-channel. */
  close(): void;
}

export interface TransportOptions {
  /** Called once when the transport closes (peer close, error, or local close()). */
  onClosed?: () => void;
}

/** MessageEvent-shaped inbound frame: data plus any transferred MessagePorts. */
export interface TransportMessage {
  data: unknown;
  ports: readonly MessagePort[];
}

/**
 * messageport-type Transport over a raw MessagePort (the current Worker main
 * channel, a link port, or a transferred acquire port). No framing: values ride
 * postMessage's structured clone; openChannel creates a fresh MessageChannel
 * and returns port2 as the transferable token.
 */
export function fromMessagePort(
  port: MessagePort,
  options: TransportOptions = {},
): Transport {
  const channels = new Set<Channel>();
  let handler: ((ev: TransportMessage) => void) | undefined;
  let channelHandler: ((channel: Channel) => void) | undefined;
  let closed = false;

  port.onmessage = (ev: MessageEvent<unknown>) => {
    if (closed) return;
    // A transferred port arrives as a raw MessagePort with no payload: the
    // placeholder already carried the channel's meaning — hand it to onChannel.
    if (ev.data == null && ev.ports.length === 1) {
      const channel = wrapPort(ev.ports[0]);
      channels.add(channel);
      channelHandler?.(channel);
      return;
    }
    handler?.({ data: ev.data, ports: ev.ports });
  };
  port.onmessageerror = () => close();

  function close(): void {
    if (closed) return;
    closed = true;
    try {
      port.close();
    } catch {
      // already detached by transfer
    }
    for (const ch of channels) ch.close();
    channels.clear();
    options.onClosed?.();
  }

  return {
    get kind(): TransportKind {
      return "messageport";
    },
    send(frame, transfer) {
      if (closed) return;
      port.postMessage(frame, transfer ? { transfer } : undefined);
    },
    onMessage(h) {
      handler = h;
    },
    openChannel() {
      const { port1, port2 } = new MessageChannel();
      const channel = wrapPort(port1);
      channels.add(channel);
      return { channel, token: port2 };
    },
    onChannel(h) {
      channelHandler = h;
    },
    close,
  };
}

/** Internal marker on framed sub-channels: their Mux channel id. */ interface MuxChannel
  extends Channel {
  _ch: number;
  _deliver(value: unknown): void;
}

/**
 * Create a `kind: "framed"` Transport over a WHATWG duplex byte stream
 * (ReadableStream + WritableStream<Uint8Array>, e.g. Deno.connect or process
 * stdio). The frame layer (createEncoder/createDecoder) is wired in, and
 * logical channels are multiplexed over the same connection via Mux control
 * frames. Channel establishment is opener-initiated: the local Channel is
 * completed when the peer's "open" control arrives back.
 */
export function framedTransport(
  readable: ReadableStream<Uint8Array>,
  writable: WritableStream<Uint8Array>,
  options: TransportOptions = {},
): Transport {
  const encoder = createEncoder();
  const decoder = createDecoder();
  const channels = new Map<number, MuxChannel>();
  const pending = new Map<number, MuxChannel>();
  let nextCh = 1;
  let handler: ((ev: TransportMessage) => void) | undefined;
  let channelHandler: ((channel: Channel) => void) | undefined;
  let closed = false;

  function fail(reason: unknown): void {
    if (closed) return;
    closed = true;
    for (const ch of channels.values()) ch.close();
    channels.clear();
    pending.clear();
    options.onClosed?.();
  }

  function makeChannel(ch: number): MuxChannel {
    let channelClosed = false;
    let msgHandler: ((message: unknown) => void) | undefined;
    const channel: MuxChannel = {
      _ch: ch,
      get closed(): boolean {
        return channelClosed;
      },
      get port(): MessagePort {
        throw new Error("framed channels have no MessagePort");
      },
      send(message) {
        if (channelClosed) return;
        const frame: MuxFrame = { __mux: "data", ch, value: message };
        const writer = encoder.writable.getWriter();
        void writer.write(frame);
        writer.releaseLock();
      },
      onMessage(h) {
        msgHandler = h;
      },
      close() {
        if (channelClosed) return;
        channelClosed = true;
        channels.delete(ch);
        const frame: MuxFrame = { __mux: "close", ch };
        const writer = encoder.writable.getWriter();
        void writer.write(frame);
        writer.releaseLock();
      },
      _deliver(value: unknown) {
        msgHandler?.(value);
      },
    };
    return channel;
  }

  function handleMux(frame: MuxFrame): void {
    if (frame.__mux === "open") {
      const pendingCh = pending.get(frame.ch);
      if (pendingCh) {
        // opener: our openChannel()'s channel completes when the peer's open
        // control arrives back.
        pending.delete(frame.ch);
        channels.set(frame.ch, pendingCh);
        return;
      }
      // peer-initiated: build our end, notify onChannel, and respond "open"
      // back so the opener's channel completes (bidirectional handshake).
      const channel = makeChannel(frame.ch);
      channels.set(frame.ch, channel);
      channelHandler?.(channel);
      const ack: MuxFrame = { __mux: "open", ch: frame.ch };
      const writer = encoder.writable.getWriter();
      void writer.write(ack);
      writer.releaseLock();
      return;
    }
    if (frame.__mux === "close") {
      const ch = channels.get(frame.ch) ?? pending.get(frame.ch);
      if (ch) {
        channels.delete(frame.ch);
        pending.delete(frame.ch);
        ch.close();
      }
      return;
    }
    // data
    const ch = channels.get(frame.ch);
    if (ch) {
      ch._deliver(frame.value);
      return;
    }
    // unknown channel: fall back to the main handler
    handler?.({ data: frame, ports: [] });
  }

  // Inbound: byte stream → decoder → Mux dispatch. pipeThrough coordinates
  // backpressure between the source and the decoder so the decoder's readable
  // is consumed as bytes arrive (reading it only after draining the writable
  // would deadlock under backpressure).
  const pumpIn = async (): Promise<void> => {
    try {
      const piped = readable.pipeThrough(decoder);
      const reader = piped.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        handleMux(value as MuxFrame);
      }
    } catch (e) {
      fail(e);
    }
  };
  void pumpIn();

  // Outbound: encoder → byte stream
  const pumpOut = async (): Promise<void> => {
    try {
      const reader = encoder.readable.getReader();
      const writer = writable.getWriter();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        await writer.write(value);
      }
      await writer.close();
    } catch (e) {
      fail(e);
    }
  };
  void pumpOut();

  return {
    get kind(): TransportKind {
      return "framed";
    },
    send(frame) {
      if (closed) return;
      const writer = encoder.writable.getWriter();
      void writer.write(frame);
      writer.releaseLock();
    },
    onMessage(h) {
      handler = h;
    },
    openChannel() {
      const ch = nextCh++;
      const channel = makeChannel(ch);
      pending.set(ch, channel);
      return {
        channel,
        token: { __mux: "open", ch } satisfies MuxFrame,
      };
    },
    onChannel(h) {
      channelHandler = h;
    },
    close() {
      if (closed) return;
      closed = true;
      for (const ch of channels.values()) ch.close();
      channels.clear();
      pending.clear();
      options.onClosed?.();
    },
  };
}
