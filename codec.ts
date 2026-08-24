/**
 * Codec author toolbox: everything needed to write a custom transport.
 *
 * This is the extension surface for the library. A codec author combines these
 * primitives to make any value cross the actor boundary — a custom marshal-by-
 * ref protocol, a streaming channel, a remote signal — without touching the
 * library core:
 *
 *   codec           — Codec, PayloadCodecRegistry, placeholder helpers,
 *                     encode/decode contexts, codec-local state
 *   channel         — Channel, openChannel/connectChannel, GC-based
 *                     registerRelease (the .NET MarshalByRef-style primitive)
 *   stream          — stream channel primitives (startStreamProducer /
 *                     createRemoteIterable) for AsyncIterable transports
 *   rpc             — channel-agnostic RPC machinery (makeRpcHandler /
 *                     createRpcProxy / PeerRpc), reusable across channels
 *   control         — the acquire control plane (registerControlHandler /
 *                     triggerAcquire / ...) for main-coordinated references
 *   protocol        — serializeError / SerializedError for error frames
 *   types           — TransformCallbacks / SyncOrAsync / CodecValueTypes
 *                     for remote type projections (CodecValueTypes derives
 *                     the pass-through set from a runtime codec tuple)
 *
 * @see examples/remote_ref/ref_codec.ts — a full custom codec built on these.
 */
export {
  CODEC_PLACEHOLDER_KEY,
  getCodecState,
  isNativelyClonable,
  isPlaceholder,
  isPlainObject,
  PayloadCodecRegistry,
} from "./core/codec.ts";
export type { Codec, DecodeContext, EncodeContext } from "./core/codec.ts";

export {
  connectChannel,
  connectToken,
  openChannel,
  registerRelease,
} from "./core/channel.ts";
export type { Channel, ChannelOptions, ChannelPeer } from "./core/channel.ts";

export { createRemoteIterable, startStreamProducer } from "./core/stream.ts";

export { createMux, messageTransport } from "./core/transport.ts";
export type {
  MessageTransport,
  Mux,
  Transport,
  TransportKind,
  TransportMessage,
} from "./core/transport.ts";

export { createRpcProxy, makeRpcHandler } from "./core/rpc.ts";
export type {
  PeerRpc,
  RpcApi,
  RpcProxy,
  RpcProxyOptions,
  RpcRequest,
  RpcResponse,
  RpcResult,
  RpcResultError,
  RpcResultOk,
} from "./core/rpc.ts";

export {
  dispatchControlFrame,
  getActiveRegistry,
  getWorkerId,
  registerControlHandler,
  setActiveRegistry,
  setMainAcquire,
  setWorkerId,
  triggerAcquire,
  unregisterControlHandler,
} from "./core/worker-context.ts";
export type { ControlFrame } from "./core/worker-context.ts";

export { serializeError } from "./core/protocol.ts";
export type { SerializedError } from "./core/protocol.ts";

export type {
  CodecValueTypes,
  SyncOrAsync,
  TransformCallbacks,
} from "./core/type-utils.ts";
