/**
 * Stream channel primitives: pump an AsyncIterable into a Channel and rebuild
 * one from it (lazy start, backpressure, release, error/death semantics).
 *
 *   import { createRemoteIterable } from "…/stream";
 */
export { createRemoteIterable, startStreamProducer } from "./core/stream.ts";
