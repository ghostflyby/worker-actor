/**
 * abort_signal codec: bridges AbortSignal over a MessageChannel.
 *
 * Why (verified by probe + spec): structured clone has no serialization step for
 * AbortSignal in the HTML Standard, and Deno 2.9.5's implementation drops the
 * prototype and aborted state (the clone is a plain object). So the signal is
 * rebuilt over a dedicated channel:
 *
 *   Encoding side (holds the source signal): immediately sends a `status` frame
 *     (aborted + reason); if not aborted, listens for the abort event and
 *     forwards `abort` frames.
 *   Decoding side: rebuilds an AbortController, aborts it on frames, returns
 *     controller.signal.
 *
 * Channel protocol:
 *   sender → receiver  { type: "status"; aborted; reason? }   // initial state
 *   sender → receiver  { type: "abort"; reason? }             // abort propagation
 *
 * Semantics:
 *   - On failAll (actor terminated/crashed) the receiver-side rebuilt signal is
 *     aborted too — cancels long-running work within the actor context.
 *   - The aborted state lands asynchronously (within a tick); event-driven
 *     consumers are unaffected.
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
  getCodecState,
} from "../codec.ts";

interface AbortFrame {
  type: "status" | "abort";
  aborted?: boolean;
  reason?: unknown;
}

interface AbortSignalHandle {
  [CODEC_PLACEHOLDER_KEY]: "abort-signal";
  port: MessagePort;
}

interface AbortCodecState {
  /** sender 侧清理函数（停发 + 移除监听 + 关 port1）。 */
  senders: Set<() => void>;
  /** receiver 侧 abort 函数（actor 死亡时取消重建信号）。 */
  receivers: Set<() => void>;
  /** 已转移给对端的 port2，failAll 时兜底关闭。 */
  port2s: Set<MessagePort>;
}

function matches(v: unknown): v is AbortSignal {
  return v instanceof AbortSignal;
}

function getState(ctx: { codecState: Map<Codec, unknown> }): AbortCodecState {
  return getCodecState<AbortCodecState>(ctx, abortSignalCodec, () => ({
    senders: new Set(),
    receivers: new Set(),
    port2s: new Set(),
  }));
}

function encode(signal: AbortSignal, ctx: EncodeContext): unknown {
  const { port1, port2 } = new MessageChannel();
  const state = getState(ctx);
  let stopped = false;

  const post = (frame: AbortFrame): void => {
    if (!stopped) port1.postMessage(frame);
  };

  post({ type: "status", aborted: signal.aborted, reason: signal.reason });

  const onAbort = (): void => {
    post({ type: "abort", reason: signal.reason });
    stopped = true; // propagates once; the port is closed right after
    port1.close();
  };
  if (!signal.aborted) {
    signal.addEventListener("abort", onAbort, { once: true });
  }

  state.senders.add(() => {
    stopped = true;
    signal.removeEventListener("abort", onAbort);
    port1.close();
  });
  state.port2s.add(port2);
  ctx.transfer.push(port2);
  return {
    [CODEC_PLACEHOLDER_KEY]: "abort-signal",
    port: port2,
  } satisfies AbortSignalHandle;
}

function decode(
  placeholder: AbortSignalHandle,
  ctx: DecodeContext,
): AbortSignal {
  const controller = new AbortController();
  const state = getState(ctx);
  const port = placeholder.port;

  port.onmessage = (ev: MessageEvent<AbortFrame>) => {
    const frame = ev.data;
    if (frame.type === "status") {
      if (frame.aborted && !controller.signal.aborted) {
        controller.abort(frame.reason);
      }
    } else if (frame.type === "abort") {
      if (!controller.signal.aborted) controller.abort(frame.reason);
      port.close();
    }
  };
  port.onmessageerror = () => {
    if (!controller.signal.aborted) controller.abort();
    port.close();
  };

  state.receivers.add(() => {
    if (!controller.signal.aborted) controller.abort();
    port.close();
  });

  return controller.signal;
}

function onRegistryFail(state: AbortCodecState | undefined): void {
  if (!state) return;
  for (const cleanup of state.senders) cleanup();
  for (const abort of state.receivers) abort();
  for (const port of state.port2s) port.close();
  state.senders.clear();
  state.receivers.clear();
  state.port2s.clear();
}

export const abortSignalCodec: Codec<AbortSignal> = {
  tag: "abort-signal",
  matches,
  encode,
  decode,
  onRegistryFail,
};
