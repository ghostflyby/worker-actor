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
 *   receiver → sender  { type: "release" }                    // rebuilt signal was GC'd
 *
 * Semantics:
 *   - On failAll (actor terminated/crashed) the receiver-side rebuilt signal is
 *     aborted too — cancels long-running work within the actor context.
 *   - The aborted state lands asynchronously (within a tick); event-driven
 *     consumers are unaffected.
 *   - GC-based release is NOT applicable to AbortSignal: abort propagation
 *     requires holding the rebuilt AbortController, and an AbortController
 *     strongly references its signal (controller.signal), which would pin the
 *     signal forever and never fire a finalizer. Release is therefore driven by
 *     explicit frames only: abort/status teardown and failAll. A "release"
 *     frame handler is kept defensively for forward compatibility.
 *   - Sender and receiver both self-remove from the codec's bookkeeping once
 *     their channel is done, so ports/listeners don't linger past the signal's
 *     lifetime.
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
  getCodecState,
} from "../codec.ts";

interface AbortFrame {
  type: "status" | "abort" | "release";
  aborted?: boolean;
  reason?: unknown;
}

interface AbortSignalHandle {
  [CODEC_PLACEHOLDER_KEY]: "abort-signal";
  port: MessagePort;
}

interface AbortCodecState {
  /** Sender-side cleanup (stop forwarding, remove listener, close port1). */
  senders: Set<() => void>;
  /** Receiver-side abort on failAll (cancels the rebuilt signal). */
  receivers: Set<() => void>;
  /** port2 transferred to the peer; closed as a safety net on failAll. */
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
    cleanup();
  };

  // cleanup self-removes from the registry, so the source signal's graph is
  // collectable once the channel is done, even while the actor stays alive.
  let cleanup: () => void = () => {};
  cleanup = (): void => {
    if (stopped) return;
    stopped = true;
    signal.removeEventListener("abort", onAbort);
    port1.close();
    state.senders.delete(cleanup);
    state.port2s.delete(port2);
  };

  if (!signal.aborted) {
    signal.addEventListener("abort", onAbort, { once: true });
  }
  state.senders.add(cleanup);
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
  let closed = false;

  // teardown is the single idempotent release path; abortLocal decides whether
  // the rebuilt signal is also aborted (failAll / deserialization error) or the
  // channel is just torn down (peer aborted or released).
  const failAllCleanup = (): void => teardown(true);
  function teardown(abortLocal: boolean): void {
    if (closed) return;
    closed = true;
    port.close();
    state.receivers.delete(failAllCleanup);
    if (abortLocal && !controller.signal.aborted) controller.abort();
  }

  port.onmessage = (ev: MessageEvent<AbortFrame>) => {
    const frame = ev.data;
    if (frame.type === "status") {
      if (frame.aborted && !controller.signal.aborted) {
        controller.abort(frame.reason);
      }
    } else if (frame.type === "abort") {
      if (!controller.signal.aborted) controller.abort(frame.reason);
      teardown(false);
    } else if (frame.type === "release") {
      teardown(false);
    }
  };
  port.onmessageerror = () => teardown(true);

  state.receivers.add(failAllCleanup);

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
