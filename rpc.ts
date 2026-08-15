/**
 * Channel-agnostic RPC machinery: the request/response protocol, pending-map
 * correlation and codec round-trips shared by the main channel and worker
 * links. Reuse these to build custom RPC channels.
 *
 *   import { createRpcProxy, makeRpcHandler } from "…/rpc";
 */
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
