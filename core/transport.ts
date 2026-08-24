/**
 * Transport: the connection abstraction that unifies every actor channel type —
 * Worker MessagePort, fork IPC, WebSocket, TCP. A Transport carries the main
 * message channel (RPC / control frames) and is responsible for opening logical
 * sub-channels via openChannel(), exactly like the WebTransport shape (a
 * connection + createBidirectionalStream()).
 *
 * Kinds:
 *   - messageport: messages are delivered natively (structured clone);
 *     openChannel() produces a real MessagePort (transferred with the
 *     placeholder) — the natural channel-creation form in the structured-clone
 *     world.
 *   - framed: messages run through the frame layer (core/frame.ts, v8 + length
 *     prefix); openChannel() produces a { __mux: "open", ch } token and
 *     multiplexes the logical channel over the existing connection.
 *   - message: values are delivered as discrete messages (fork IPC advanced,
 *     WebSocket binary frames) — each message is one value, so the Mux protocol
 *     rides directly on the messages with no byte framing.
 *
 * Channel establishment on a Mux transport (framed / message):
 *   opener:  openChannel() → { channel, token }  (token = { __mux:"open", ch })
 *   opener:  sends the token to the peer inside a placeholder
 *   peer:    onMuxOpen → builds its end of channel ch → responds
 *            { __mux:"open", ch } back on the main channel
 *   opener:  receives the "open" control → resolves the pending channel
 *   either side may then send { __mux:"data", ch, value } frames
 */

import type { Channel } from "./channel.ts";
import { wrapPort } from "./channel.ts";
import {
  createDecoder,
  createEncoder,
  deserialize,
  type MuxFrame,
  serialize,
} from "./frame.ts";

export type TransportKind = "messageport" | "framed" | "message";

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
   * a { __mux: "open", ch } control value on Mux transports. The peer
   * rebuilds the channel via onChannel.
   */
  openChannel(): { channel: Channel; token: unknown };
  /** Register the handler for channels opened by the peer (last handler wins). */
  onChannel(handler: (channel: Channel) => void): void;
  /** Claim a peer-opened channel that arrived before its consumer connected (Mux transports). */
  claimOrphan?(ch: number): Channel | undefined;
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

/** Internal marker on Mux sub-channels: their Mux channel id. */
interface MuxChannel extends Channel {
  _ch: number;
  _deliver(value: unknown): void;
}

/**
 * Mux: the multiplexing engine shared by every non-messageport transport. It
 * owns the channel table (open/close), the opener-initiated token handshake,
 * and per-channel frame delivery. The transport supplies `write` (how to emit
 * a Mux frame) and `deliver` (where inbound Mux frames come from).
 */
export interface Mux {
  readonly kind: "framed" | "message";
  onMessage(handler: (ev: TransportMessage) => void): void;
  onChannel(handler: (channel: Channel) => void): void;
  openChannel(): { channel: Channel; token: unknown };
  send(frame: unknown): void;
  deliver(frame: unknown): void;
  /** Claim a peer-opened channel that arrived before its consumer connected. */
  claimOrphan(ch: number): Channel | undefined;
  close(): void;
}

export function createMux(
  write: (frame: MuxFrame | unknown) => void,
  options: TransportOptions = {},
): Mux {
  const channels = new Map<number, MuxChannel>();
  const pending = new Map<number, MuxChannel>();
  // Channels opened by the peer whose open control arrived before any
  // consumer claimed them (decode-side connectToken runs later). connectToken
  // claims these by id; until then they stay open so data isn't lost.
  const orphaned = new Map<number, MuxChannel>();
  let nextCh = 1;
  let handler: ((ev: TransportMessage) => void) | undefined;
  let channelHandler: ((channel: Channel) => void) | undefined;
  let closed = false;

  function makeChannel(ch: number): MuxChannel {
    let channelClosed = false;
    let msgHandler: ((message: unknown) => void) | undefined;
    // Inbound data frames can arrive before the consumer attaches its handler
    // (e.g. a codec posts a status frame right after openChannel, or the
    // open/placeholder frames race). Buffer until onMessage is set.
    const buffered: unknown[] = [];
    const channel: MuxChannel = {
      _ch: ch,
      get closed(): boolean {
        return channelClosed;
      },
      kind: "framed",
      get port(): MessagePort {
        throw new Error("Mux channels have no MessagePort");
      },
      send(message) {
        if (channelClosed) return;
        write({ __mux: "data", ch, value: message } satisfies MuxFrame);
      },
      onMessage(h) {
        msgHandler = h;
        while (buffered.length) h(buffered.shift()!);
      },
      close() {
        if (channelClosed) return;
        channelClosed = true;
        channels.delete(ch);
        orphaned.delete(ch);
        write({ __mux: "close", ch } satisfies MuxFrame);
      },
      _deliver(value: unknown) {
        if (msgHandler) msgHandler(value);
        else buffered.push(value);
      },
    };
    return channel;
  }

  function handle(frame: unknown): void {
    if (frame === null || typeof frame !== "object") {
      handler?.({ data: frame, ports: [] });
      return;
    }
    const f = frame as MuxFrame;
    if (f.__mux === "open") {
      const pendingCh = pending.get(f.ch);
      if (pendingCh) {
        // opener: our openChannel()'s channel completes when the peer's open
        // control arrives back.
        pending.delete(f.ch);
        channels.set(f.ch, pendingCh);
        return;
      }
      // peer-initiated: build our end. If a consumer (connectToken) has not
      // claimed it yet, cache it as orphaned so the claim can find it later.
      const channel = makeChannel(f.ch);
      channels.set(f.ch, channel);
      orphaned.set(f.ch, channel);
      channelHandler?.(channel);
      write({ __mux: "open", ch: f.ch } satisfies MuxFrame);
      return;
    }
    if (f.__mux === "close") {
      // Look up pending (opener), channels (established), and orphaned
      // (peer-opened, unclaimed) so a close racing the decode is not dropped.
      const ch = channels.get(f.ch) ?? pending.get(f.ch) ?? orphaned.get(f.ch);
      if (ch) {
        channels.delete(f.ch);
        pending.delete(f.ch);
        orphaned.delete(f.ch);
        ch.close();
      }
      return;
    }
    if (f.__mux === "data") {
      // Established channels AND orphaned (peer-opened, not yet claimed) ones:
      // a data frame for an orphaned channel must reach it (it buffers until a
      // handler is attached), or it would be lost before the consumer claims it.
      const ch = channels.get(f.ch) ?? orphaned.get(f.ch);
      if (ch) {
        ch._deliver(f.value);
        return;
      }
    }
    // unknown channel or non-Mux value: fall back to the main handler
    handler?.({ data: frame, ports: [] });
  }

  return {
    kind: "framed",
    onMessage(h) {
      handler = h;
    },
    onChannel(h) {
      channelHandler = h;
    },
    openChannel() {
      const ch = nextCh++;
      const channel = makeChannel(ch);
      pending.set(ch, channel);
      return { channel, token: { __mux: "open", ch } satisfies MuxFrame };
    },
    send(frame) {
      if (closed) return;
      write(frame);
    },
    deliver(frame) {
      if (closed) return;
      handle(frame);
    },
    claimOrphan(ch) {
      const orphan = orphaned.get(ch);
      if (orphan) orphaned.delete(ch);
      return orphan;
    },
    close() {
      if (closed) return;
      closed = true;
      for (const ch of channels.values()) ch.close();
      channels.clear();
      pending.clear();
      orphaned.clear();
      options.onClosed?.();
    },
  };
}

/**
 * Create a `kind: "framed"` Transport over a WHATWG duplex byte stream
 * (ReadableStream + WritableStream<Uint8Array>, e.g. Deno.connect or process
 * stdio). The frame layer (createEncoder/createDecoder) is wired in, and
 * logical channels are multiplexed over the same connection via Mux control
 * frames.
 */
export function framedTransport(
  readable: ReadableStream<Uint8Array>,
  writable: WritableStream<Uint8Array>,
  options: TransportOptions = {},
): Transport {
  const encoder = createEncoder();
  const decoder = createDecoder();
  const mux = createMux((frame) => {
    const writer = encoder.writable.getWriter();
    void writer.write(frame);
    writer.releaseLock();
  }, options);
  let closed = false;

  const fail = (): void => {
    if (closed) return;
    closed = true;
    mux.close();
  };

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
        mux.deliver(value);
      }
      mux.close();
    } catch {
      fail();
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
    } catch {
      fail();
    }
  };
  void pumpOut();

  return {
    get kind(): TransportKind {
      return "framed";
    },
    send(frame, _transfer) {
      mux.send(frame);
    },
    onMessage(h) {
      mux.onMessage(h);
    },
    openChannel() {
      return mux.openChannel();
    },
    onChannel(h) {
      mux.onChannel(h);
    },
    claimOrphan(ch) {
      return mux.claimOrphan(ch);
    },
    close() {
      if (closed) return;
      closed = true;
      mux.close();
    },
  };
}

/**
 * Create a `kind: "message"` Transport over a message-oriented channel — a
 * channel that delivers discrete values (each message is one value, with no
 * byte framing needed). Examples: node:child_process fork IPC with
 * serialization 'advanced' (each message is a v8 value), and WebSocket binary
 * frames. Messages are fed in via `deliver` (called by the caller's message
 * handler) and sent out via `send`.
 *
 * The Mux protocol rides directly on the messages: open/data/close are
 * ordinary messages, so a fork-IPC or WebSocket channel multiplexes exactly
 * like a framed connection.
 */
export interface MessageTransport extends Transport {
  /** Feed an inbound message from the underlying channel into the Mux. */
  deliver(message: unknown): void;
}

export function messageTransport(
  options: {
    send: (message: unknown) => void;
    onClosed?: () => void;
    /** Transform an inbound message before Mux dispatch (may be async, e.g. Blob → bytes). */
    decode?: (message: unknown) => unknown | Promise<unknown>;
  },
): MessageTransport {
  const mux = createMux(options.send, {
    onClosed: options.onClosed,
  });
  const decode = options.decode ?? ((m: unknown) => m);
  let closed = false;
  // Async decode (e.g. WebSocket Blob→bytes) can resolve out of order; chain
  // every inbound message so Mux frames keep their arrival order.
  let inboundChain: Promise<void> = Promise.resolve();
  return {
    get kind(): TransportKind {
      return "message";
    },
    send(frame, _transfer) {
      mux.send(frame);
    },
    onMessage(h) {
      mux.onMessage(h);
    },
    openChannel() {
      return mux.openChannel();
    },
    onChannel(h) {
      mux.onChannel(h);
    },
    claimOrphan(ch) {
      return mux.claimOrphan(ch);
    },
    deliver(message) {
      if (closed) return;
      // Chain async decodes so frames stay in arrival order.
      inboundChain = inboundChain.then(async () => {
        const decoded = await decode(message);
        mux.deliver(decoded);
      }).catch(() => {});
    },
    close() {
      if (closed) return;
      closed = true;
      mux.close();
    },
  };
}

/**
 * node:child_process fork IPC Transport (message kind). Each IPC message is a
 * v8-serialized value (serialization: 'advanced'), delivered as a discrete
 * message — no byte framing needed, and the IPC channel is out-of-band from
 * stdin/stdout/stderr so child logging cannot pollute the protocol.
 *
 * `deliver` must be wired to the child's 'message' event:
 *   child.on("message", (msg) => transport.deliver(msg));
 */
export function fromNodeIpc(
  send: (message: unknown, handle?: unknown) => boolean,
  options: TransportOptions = {},
): MessageTransport {
  return messageTransport({
    send: (message) => {
      send(message);
    },
    onClosed: options.onClosed,
  });
}

/**
 * WebSocket Transport (message kind). Each binary message is one value: the
 * frame layer is not needed (WS message boundaries are frame boundaries), and
 * Mux multiplexes logical channels over the single WS connection.
 *
 * `deliver` must be wired to the socket's message handler:
 *   ws.onmessage = (ev) => transport.deliver(ev.data);
 */
export function fromWebSocket(
  socket: {
    send(data: string | ArrayBuffer | ArrayBufferView): void;
  },
  options: TransportOptions = {},
): MessageTransport {
  return messageTransport({
    send: (message) => {
      // v8-serialize the value into a binary message (the WS message is the frame).
      socket.send(serialize(message));
    },
    // Inbound WS messages are binary (v8 bytes) or text. Deno delivers binary
    // as Blob by default; convert to bytes then deserialize.
    decode: async (message) => {
      if (typeof message === "string") return message;
      let bytes: Uint8Array;
      if (message instanceof Blob) {
        bytes = new Uint8Array(await message.arrayBuffer());
      } else if (message instanceof ArrayBuffer) {
        bytes = new Uint8Array(message);
      } else {
        bytes = message as Uint8Array;
      }
      return deserialize(bytes);
    },
    onClosed: options.onClosed,
  });
}
