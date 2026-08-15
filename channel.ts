/**
 * Channel primitives for codec authors: dedicated cross-thread channels with
 * GC-based release, on top of raw MessageChannel.
 *
 *   import { openChannel, registerRelease } from "…/channel";
 */
export {
  connectChannel,
  openChannel,
  registerRelease,
} from "./core/channel.ts";
export type { Channel, ChannelOptions } from "./core/channel.ts";
