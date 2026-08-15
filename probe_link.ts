import { link, spawn } from "./spawn.ts";
import { remoteRefCodec } from "./examples/remote_ref/ref_codec.ts";
import type * as LinkBModule from "./test_fixtures/link_b.ts";
import type * as LinkCModule from "./test_fixtures/link_c.ts";

const t0 = Date.now();
const wb = new Worker(import.meta.resolve("./test_fixtures/link_b.ts"), {
  type: "module",
});
const wc = new Worker(import.meta.resolve("./test_fixtures/link_c.ts"), {
  type: "module",
});
const b = await spawn<typeof LinkBModule.rpc>(wb, {
  codecs: [remoteRefCodec],
  handshakeTimeoutMs: 30000,
});
console.log("B ready", Date.now() - t0, "ms");
const c = await spawn<typeof LinkCModule.rpc>(wc, {
  codecs: [remoteRefCodec],
  handshakeTimeoutMs: 30000,
});
console.log("C ready", Date.now() - t0, "ms");
link(wb, wc, "b-c");
await b.createCounterAndSendToC();
await new Promise((r) => setTimeout(r, 200));
console.log("C sees ref:", await c.getLastIsRef());
console.log("increment:", await c.callLastIncrement());
b.dispose();
c.dispose();
