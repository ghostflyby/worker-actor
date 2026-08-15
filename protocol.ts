/**
 * Wire protocol details: frames, error serialization, death errors.
 *
 *   import { ActorDiedError } from "…/protocol";
 */
export {
  ActorDiedError,
  RemoteError,
  serializeError,
} from "./core/protocol.ts";
export type { Frame, SerializedError } from "./core/protocol.ts";
