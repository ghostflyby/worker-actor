// Thin re-export of the remote-ref codec for fixtures (avoids importing the
// example module's side effects directly in test fixtures).
export { remoteRef, remoteRefCodec } from "../examples/remote_ref/ref_codec.ts";
