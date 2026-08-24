# Worker ⇄ Actor — Design

A minimal library that wraps a Web Worker — and, via the Transport abstraction,
a child process or WebSocket connection — into a **type-safe Actor**. Design
goals: static typing, deeply nested value transfer, an automatically established
message channel, and automatically established transport. Zero runtime
dependencies (only `@std/assert` in tests).

## Core insight

A Worker and an Actor are the same model:

| Actor model                            | Web Worker                                         |
| -------------------------------------- | -------------------------------------------------- |
| Addressable, message-deliverable       | `worker.postMessage()` has a stable address        |
| Mailbox processes messages in order    | worker event loop processes serially on one thread |
| Messages are unshared, passed by value | structured clone copies by value                   |
| Exceptions don't cross addresses       | errors must be serialized back explicitly          |
| Lifecycle externally controlled        | `terminate()` / `close()`                          |

So only four things are needed: a **message channel** (postMessage + frame
protocol), a **proxy** (Proxy + incrementing ids correlating request/response),
**transport** (structured clone, deep values natively), and **types**
(type-level proxy derivation).

## Project layout

```
mod.ts                    # default export: the application surface (spawn/link/pool/serveWorker/serveProcess/serveNode/transport adapters/errors/types)
codec.ts                  # sub-path: the codec author toolbox (Codec + channel + stream + rpc + transport + control plane)
codecs.ts                 # sub-path: the built-in codecs
core/protocol.ts          # frame types, error serialization, handshake version + transport kind (private)
core/frame.ts             # v8 framing: createEncoder/createDecoder TransformStreams + Mux frame types (private)
core/transport.ts         # the Transport abstraction + adapters (fromMessagePort/fromNodeIpc/fromWebSocket/messageTransport) + createMux (private)
core/registry.ts          # ActorRegistry: transport-id → Transport bootstrap table (private)
core/codec.ts             # generic Codec interface + PayloadCodecRegistry (deep walk/placeholder/lifecycle)
core/channel.ts           # high-level Channel abstraction for codec authors (open/connect/token/release)
core/codecs/              # built-in codecs: iterable / error / abort-signal / callback
core/stream.ts            # stream protocol on top of Channel (pump/rebuild/backpressure/release)
core/rpc.ts               # channel-agnostic RPC machinery (makeRpcHandler/createRpcProxy)
core/worker-context.ts    # worker-side global context + the acquire control plane (private)
spawn.ts                  # main-thread: spawn(Worker | Transport) → type-safe Proxy + lifecycle + codec validation; spawnProcess/spawnNode
pool.ts                   # actor pool: lightweight combinator over homogeneous Worker/Transport members
worker_runtime.ts         # actor-side: createRuntime shared by serveWorker/serveProcess/serveNode
examples/calculator/      # end-to-end example (worker.ts + main.ts)
examples/remote_ref/      # custom marshal-by-ref codec (the .NET MarshalByRef pattern)
examples/process_actor/   # multi-process example (serveProcess + spawnProcess)
test_fixtures/            # test-only workers/processes/nodes for the codec/link/transport mechanisms
main_test.ts / codec_test.ts / ref_test.ts / pool_test.ts / transport_test.ts / frame_test.ts / process_test.ts / node_test.ts / registry_test.ts / websocket_test.ts  # integration tests over real Workers/processes/transports
```

### Exports (three use-case surfaces)

The import surface is organized by external use case, not by internal module
structure. Only three entry points exist (declared in deno.json `exports`):

- `@ghostflyby/worker-actor` — **the application surface.** Creation (`spawn`,
  `spawnProcess`, `spawnNode`, `link`, `createActorPool`), the actor-side
  runtimes (`serveWorker`, `serveProcess`, `serveNode`), the transport adapters
  (`fromMessagePort`, `fromNodeIpc`, `fromWebSocket`, `messageTransport`),
  errors (`RemoteError`, `ActorDiedError`), and the core proxy/types (`Remote`,
  `ActorHandle`, `SpawnOptions`, `SpawnProcessOptions`, `SpawnNodeOptions`,
  `WorkerApi`, `LinkHandle`, `ActorPool`, `ActorPoolOptions`, `RemoteCallback`,
  `Transport`, `TransportKind`, `MessageTransport`).
- `@ghostflyby/worker-actor/codec` — **the codec author toolbox.** Everything
  needed to write a custom transport: the codec registry (`Codec`,
  `PayloadCodecRegistry`, `getCodecState`, placeholder helpers), the channel
  primitives (`openChannel`, `connectChannel`, `connectToken`,
  `registerRelease`, `Channel`), stream channel primitives
  (`createRemoteIterable`, `startStreamProducer`), the transport and
  multiplexing primitives (`createMux`, `messageTransport`, `Transport`,
  `ActorRegistry`), the channel-agnostic RPC machinery (`createRpcProxy`,
  `makeRpcHandler`, `PeerRpc`), the acquire control plane
  (`registerControlHandler`, `triggerAcquire`, …), `serializeError`, and the
  type projections (`TransformCallbacks`, `SyncOrAsync`). One import for the
  whole extension point — see `examples/remote_ref/ref_codec.ts` for a full
  custom codec built on it.
- `@ghostflyby/worker-actor/codecs` — **the built-in codecs**, for custom codec
  sets or overriding a built-in tag (`iterableCodec`, `errorCodec`,
  `createErrorCodec`, `abortSignalCodec`, `callbackCodec`, `releaseCallback`).

Wire-protocol details that no external use case needs (`Frame`,
`PROTOCOL_VERSION`, worker-context internals) are deliberately private: they
have no sub-path and are only reachable through `core/` relative paths. Internal
consumers (examples, fixtures, tests) import via the public surfaces, so the
declared export map is the actually-used one.

## Usage

```ts
// worker.ts — the rpc object is the Actor's API surface; export it so the main
// thread can reference its type
import { serveWorker } from "../worker_runtime.ts";

export const rpc = {
  add(a: number, b: number): number {
    return a + b;
  },
  report(): object {
    /* can return deep nesting + Map/Set/Date/TypedArray */
  },
};

serveWorker(rpc);

// main.ts — import type only grabs types, no worker module side effects
import type * as WorkerModule from "./worker.ts";
import { spawn } from "../spawn.ts";

const actor = await spawn<typeof WorkerModule.rpc>(
  new Worker(import.meta.resolve("./worker.ts"), { type: "module" }),
);
const sum = await actor.add(1, 2); // sum: number, checked at compile time
await actor.dispose();
```

## Type design (the static-typing core)

The proxy call is always a native Promise at runtime, and the RPC handler
`await`s the worker method's return value before encoding it. The projection
therefore describes **what that Promise resolves to** — the value that actually
crosses the boundary — never a nested async wrapper. Rules, in order:

```ts
export type Remote<T, Pass extends unknown = never> = {
  [K in keyof T]: T[K] extends (...args: infer A) => never
    ? (...args: A) => Promise<never>
    : T[K] extends (...args: infer A) => infer R
      ? R extends Pass ? (...args: TransformCallbacks<A>) => R
      : R extends PromiseLike<infer X>
        ? (...args: TransformCallbacks<A>) => Promise<X>
      : R extends AsyncIterable<infer E>
        ? (...args: TransformCallbacks<A>) => AsyncIterable<E>
      : T[K] extends RpcFn ? (
          ...args: TransformCallbacks<Parameters<T[K]>>
        ) => Promise<Resolved<ReturnType<T[K]>>>
      : never
    : never;
};
```

- **Codec pass-through** (`R extends Pass`): a return type covered by a runtime
  codec is kept exactly as declared — the codec rebuilds that type on the far
  side, so full identity survives. `Pass` is derived from the `as const`
  `codecs` tuple via `CodecValueTypes`:
  ```ts
  const codecs = [remoteRefCodec] as const; // Codec<RemoteRef<unknown>>
  spawn<typeof rpc>(worker, { codecs });
  // worker: createCounter(): RemoteRef<CounterRef>  →  stays RemoteRef<CounterRef>
  ```
  A `Codec<unknown>` entry (an empty generic, e.g. a widened array) contributes
  nothing (`never`) — a `Pass` of `unknown` would swallow every return type — so
  untyped codecs fall back to the built-in async rules below, still honest.
- **Thenable** (`R extends PromiseLike<infer X>`): a native `Promise<V>`, a
  custom thenable `MyTask<R>`, an explicit `Promise<AsyncIterable<E>>`, or a
  thenable-stream hybrid all map to `() => Promise<X>`. The handler awaits the
  value, so what crosses is the resolution `X` — never a nested Promise. (An
  explicit `Promise<AsyncIterable<E>>` keeps its eager Promise because `X` is
  itself `AsyncIterable<E>`; a hybrid keeps its thenable side, the runtime
  ordering being await-first.)
- **Iterable** (`R extends AsyncIterable<infer E>`): maps to
  `() =>
  AsyncIterable<E>` — the stream is its own async semantics, no Promise
  wrapper (lazy: the first `next()` triggers the remote call; the call stays a
  real promise underneath, its attached iterator resolving it). Custom
  `AsyncIterable` subclasses flatten to the interface.
- **Fallback**: every other function member → `Promise<Resolved<R>>` (sync
  returns normalize to a Promise). Non-function members resolve to `never` (a
  mistyped field fails immediately at compile time).
- **Parameters** are projected through `TransformCallbacks` in every branch (the
  single type-projection point, `core/type-utils.ts`): a worker declaring
  `cb: (x) => Promise<R>` keeps its honest runtime shape, while the CALLING side
  may pass a sync or an async function (`R | Promise<R>`). Built-in containers
  (Map/Set/Date/AsyncIterable/Promise/ArrayBuffer/views) pass through unchanged,
  and predicate/edge function shapes are preserved.
- Callers always write `spawn<typeof WorkerModule.rpc>` and never a second
  interface definition — the worker is the source of type truth.
- The proxy additionally exposes `dispose(): Promise<void>`.

**Honesty note**: a projected type describes the resolution of the native
Promise the proxy returns. Non-async members of a passed-through type are
preserved only if the codec actually rebuilds them on the far side (e.g. a
`RemoteRef` proxy's methods); a structured-cloned resolution value carries only
its cloneable surface.

## Transport design (deep values + AsyncIterable + Codec mechanism)

**Ordinary values** ride on postMessage's **structured clone** with no custom
serialization:

- Natively supported: arbitrarily deep objects/arrays, `Map`, `Set`, `Date`,
  `RegExp`, `TypedArray`, `ArrayBuffer`, built-in `Error` types and
  `DOMException` (subtype identity, name/message/stack preserved), `BigInt`,
  `Promise` (auto-converted to MessagePort).
- Naturally **copied by value**, no shared state, matching Actor message
  semantics.
- Limitations (revisit only if needed): custom class instances lose their
  prototype chain, circular references become empty objects, no compression.

**Values that cannot be cloned reliably** are handled by the **generic Codec
registry** (`core/codec.ts`) — the extension point for custom transport, not a
growing pile of if-branches:

```ts
interface Codec<T> {
  readonly tag: string; // wire placeholder id; decode looks it up by tag
  matches(value: unknown): value is T;
  encode(value: T, ctx): unknown; // → { __wCodec: tag, ... }; may open a channel and hand the peer end over (a port on messageport transports, a Mux token otherwise)
  decode(placeholder, ctx): T; // rebuild the original type
  onRegistryFail?(state): void; // cleanup when the actor terminates/crashes
}
```

- Both sides register via `spawn(source, { codecs })` and
  `serveWorker`/`serveProcess`/`serveNode` `(api, { codecs })`; user codecs
  match before built-ins (and can override a same-tag built-in). The registry
  deep-walks payloads (any nesting inside objects/arrays/Map/Set) and picks the
  first codec whose `matches()` fires, in registration order.
- **The handshake carries both sides' codec tag lists** and validates them: a
  mismatch rejects spawn() and reports the missing/extra tags — a registration
  mismatch becomes a startup failure instead of silently producing garbage.
- Encoding and decoding run against the **transport in context**
  (`EncodeContext.transport` / `DecodeContext.transport`): a codec that needs a
  logical channel calls `openChannel(ctx)` and lets the transport decide how the
  peer end crosses — a transferable `MessagePort` on messageport transports, a
  Mux `{ __mux: "open", ch }` token on framed/message ones (see **Codec
  tokenization** below). Decode branches on the placeholder: a `port` is wrapped
  with `connectChannel(port)`, a `token` with `connectToken(transport, token)`.
- Decoding a placeholder with an unknown tag throws (loud fail).
- Built-in codecs:
  - `iterable`: AsyncIterable / sync Iterable / stateful Iterator over a
    dedicated MessageChannel (below), with lazy/backpressure/release/error/death
    semantics.
  - `error`: **custom Error subclasses** nested in payloads are serialized
    manually (built-in Error types are natively preserved and not taken over).
    Only name/message/stack/cause by default;
    `createErrorCodec({
    keepOwnProperties: true })` additionally keeps
    enumerable own properties. decode rebuilds a RemoteError (name keeps the
    custom subclass name).
  - `abort-signal`: AbortSignal bridged over a channel (verified by probe and
    spec: structured clone has no AbortSignal serialization step and Deno's
    implementation drops prototype and aborted state). An already-aborted source
    sends an immediate `status` frame; otherwise the abort event is forwarded.
    On actor death the rebuilt signal is aborted too. State lands asynchronously
    (within a tick); event-driven consumers are unaffected.
- Codec state is isolated per registry instance (per-codec state slot): one
  actor's death never closes another actor's streams.

**AsyncIterable cannot be structured-cloned** (it carries closures and runtime
state); the iterable codec opens a **dedicated `new MessageChannel()`** per
stream (`core/stream.ts` channel primitives):

- The sender holds `port1` and pumps elements into it; the receiver gets `port2`
  **transferred** with the message and **rebuilds** a local AsyncIterable that
  pulls on demand. Stream channels are separate from the RPC channel, so a flood
  of stream data never head-of-line-blocks requests/responses. On a **Mux
  transport** (process/WebSocket) the same stream rides a logical sub-channel
  multiplexed over the connection, opened by a token instead of a transferred
  port (see **Codec tokenization**).
- Stream channel protocol: `start` (lazy start on first `next()`; no iteration,
  no producer work) → `item`/`done`/`error` (producer→consumer) → `release`
  (consumer abandoned or explicitly returned; triggers the producer's generator
  `finally`).
- A producer's mid-stream exception is serialized back and rebuilt as
  `RemoteError`; `failAll()` closes all in-flight streams when the actor
  terminates/crashes.
- Headless/backpressured: `item` is only delivered while the consumer's `next()`
  is pending; the generator awaits between messages, so the peer mailbox is
  never flooded.

### GC-based release (FinalizationRegistry)

When a rebuilt remote iterable is garbage-collected on the receiving side, a
`release` frame is sent back over that stream's own channel so the producer can
drop the original object graph — **recovering the counterpart's object when it
is GC'd on the other side**:

- Decode registers the rebuilt iterable in a `FinalizationRegistry`. The held
  callback captures only the port and the teardown closure — never the target
  itself — so registration cannot keep the target alive.
- On collection, the callback posts `release`; the producer's `stop()` runs
  `iterator.return()` (generator `finally`), closes the channel, and both sides
  **self-remove** from the registry's bookkeeping sets (no entries linger until
  actor death).
- Explicit `return()` sends the same `release` frame and unregisters the
  finalizer, so release fires exactly once — no double notify.
- **This is best-effort by spec**: finalizers are not guaranteed to run and
  their timing is not controllable. Explicit `return()`/`done`/`error` and actor
  death remain the deterministic release paths.
- **Not applicable to AbortSignal**: abort propagation requires holding the
  rebuilt AbortController, and an AbortController strongly references its signal
  (`controller.signal`), which would pin the signal forever and never fire a
  finalizer. Abort-signal teardown is therefore driven by explicit frames
  (abort/status/failAll); a `release` frame handler is kept defensively for
  forward compatibility.

**ReadableStream**: natively structured-cloneable, but with "one-shot
quasi-transfer" semantics — it can be cloned only once, before reading or
locking, and the original is disturbed afterwards. Use native cloning when the
stream is handed over once for consumption; to keep the original usable or hook
lifecycle, go through the iterable channel via `rs[Symbol.asyncIterator]()`.

### The Transport abstraction (unifying every actor channel)

Every actor channel — a Web Worker's MessagePort, a fork-IPC connection, a
WebSocket, a raw WHATWG byte stream — is the same object model: a **Transport**
(`core/transport.ts`) that carries the **main message channel** (RPC and control
frames) and is responsible for opening **logical sub-channels** via
`openChannel()` / `onChannel()`. This is deliberately the WebTransport shape (a
connection + `createBidirectionalStream()`), so "how actors talk" is one
abstraction and each underlying channel type is just an adapter.

```ts
type TransportKind = "messageport" | "framed" | "message";

interface Transport {
  readonly kind: TransportKind;
  send(frame: unknown, transfer?: Transferable[]): void; // main channel
  onMessage(handler: (ev: TransportMessage) => void): void; // main channel inbound ({ data, ports })
  openChannel(): { channel: Channel; token: unknown }; // new logical sub-channel
  onChannel(handler: (channel: Channel) => void): void; // peer-opened sub-channel
  claimOrphan?(ch: number): Channel | undefined; // Mux transports: claim a peer-opened channel that arrived early
  close(): void; // closes the main channel and every sub-channel (idempotent)
}
```

The three kinds describe **how the peer end of a logical channel crosses the
boundary**:

- **`messageport`** — values ride the structured clone of `postMessage`; no
  framing and no multiplexing. `openChannel()` creates a real `MessageChannel`
  and returns `port2` as the (transferable) token; the peer rebuilds the channel
  from the transferred port. This is the Web Worker world (the raw `Worker` is
  converted via `fromMessagePort`, including the worker-side `self.postMessage`
  channel, link ports, and acquire ports).
- **`framed`** — the connection is a WHATWG **byte stream** (`ReadableStream` +
  `WritableStream<Uint8Array>`, e.g. `Deno.connect` or a process stdio pipe).
  Every value is run through the frame layer (`core/frame.ts`): `node:v8`
  serialize + a 4-byte LE length prefix, exposed as two `TransformStream`s
  (object stream ⇄ byte stream). Logical channels are **multiplexed** over the
  same connection by the shared Mux engine — `openChannel()` allocates a channel
  id and returns a `{ __mux: "open", ch }` token (a plain cloneable value, no
  transferables).
- **`message`** — the connection delivers discrete values, each one a v8 value
  (fork IPC with `serialization: 'advanced'`, WebSocket binary frames). No byte
  framing is needed — the WS/IPC message boundary IS the frame boundary — and
  the Mux protocol rides directly on the messages. `fromNodeIpc` /
  `fromWebSocket` produce this kind.

**The Mux engine** (`createMux`, shared by every framed/message transport) owns
the channel table and the **opener-initiated token handshake**:

- opener: `openChannel()` → `{ channel, token: { __mux: "open", ch } }`; the
  opener sends the token to the peer inside a placeholder;
- peer: the open control hits the Mux's `handle()` → builds its end of channel
  `ch` → fires `onChannel` and acks `{ __mux: "open", ch }` back on the main
  channel (this ack resolves the opener's pending channel);
- either side may then send `{ __mux: "data", ch, value }` /
  `{ __mux: "close",
  ch }` frames. A `close` races a pending decode by looking
  up pending, established, AND orphaned channels; a `data` frame for an orphaned
  channel is buffered until a consumer claims it.
- **Channel-id uniqueness**: both ends allocate ids independently, and a data
  frame's `ch` routes into the peer's channel table — so an id collision (both
  sides starting at 1) would misroute frames across the connection. Each side
  therefore starts from a **random base** (`Math.random() * 1e9 * 1000` plus a
  counter), keeping the two ends' id spaces disjoint with negligible collision
  probability while staying a plain number on the wire. (A real bug —
  stream-then-abort frames misrouted across processes after an id collision —
  was fixed this way.)

`spawn(source: Worker | Transport)` is the single entry point: a raw `Worker` is
converted to a messageport-type Transport via `fromMessagePort` and the call
recurses on the unified abstraction. The raw `Worker` is kept alongside for
crash detection (`onerror`) and `terminate()`, which the Transport abstraction
does not model; Mux transports surface death through channel/transport close
(`onClosed`), wired to the actor's kill path by the caller (e.g.
`spawnProcess`).

### Multi-process actors (spawnProcess / serveProcess)

An actor can run in a separate Deno process. `spawnProcess(entrypoint, options)`
starts `deno run` (via `node:child_process`) and returns the usual typed
`Remote<T> & ActorHandle`; the child module calls `serveProcess(rpc)` at its top
level. `spawnProcess` is a convenience wrapper over `spawn(Transport)` — it
builds a Transport from the child's IPC channel and recurses — with extra
controls: the child's Deno permissions are granted through `permissions` (mapped
to `--allow-*` flags; omit for allow-all), `denoArgs` passes extra CLI args, and
the child is torn down (`child.kill()`) on dispose/crash.

**The channel is `node:child_process` fork IPC, not stdin/stdout.** The child is
spawned with `stdio: ["ipc", "pipe", "pipe"]` and `serialization: "advanced"`,
giving a dedicated IPC message channel on which each message is one v8 value —
wrapped as a message-kind Transport (`fromNodeIpc`, wired to the child's
`"message"` event). The rationale for IPC over stdio:

- **Out-of-band**: the IPC channel (fd 3 on the child) is separate from
  stdout/stderr, so child logging can never pollute the protocol — a stdio
  carrier would require reserving a stream and sharing it with the child's own
  output.
- **Message boundaries for free**: `serialization: "advanced"` delivers discrete
  v8 values, so the Mux protocol rides directly on the messages with no byte
  framing — the same path as WebSocket.
- **No handles**: fork IPC cannot transfer MessagePort handles (verified:
  `ERR_INVALID_HANDLE_TYPE` both ways), which is exactly why logical channels
  are tokenized — the token handshake needs no handles. (`Deno.Command` array
  stdio's extra fds are unusable for this: the parent side has no JS accessor
  for them.)

### Multi-actor nodes (serveNode / spawnNode)

A single process/connection can serve **several named actors** (model B).
`serveNode(rpcs)` announces the actor names in its handshake and serves each
named RPC object on its own logical channel; the main channel only performs the
handshake and opens actor channels. **An actor is a Mux sub-channel**: each RPC
stream runs on its own channel (a MessagePort on messageport transports, a Mux
channel on framed/message ones), so one node process hosts many actors that
dispose independently without sharing a request serialization point.

On the peer side, `spawnNode(entrypoint)` waits for the node's handshake
(`actors` list), then opens one channel per actor — it sends the open token and
a `__open-actor` frame naming the actor (both on the main channel, in order) —
and returns a `{ [name]: Remote } & { dispose() }` surface. `serveNode` builds
its transport from the environment: fork IPC when a `process.send` exists, else
the worker `self` channel.

### WebSocket transport (fromWebSocket)

`fromWebSocket(socket)` wraps a WebSocket as a message-kind Transport, enabling
actors over any network distance. Outbound values are v8-serialized into a
binary message (`serialize(message)` — the WS message IS the frame, no length
prefix needed); inbound messages are converted Blob → bytes → `deserialize` (the
transport's `decode` hook, chained so async decodes keep arrival order) and fed
to the Mux. Text messages pass through deserialization untouched. The wire
format is therefore self-describing v8 — interoperable with any peer that can
`node:v8` (or run this library), but not with arbitrary JSON-speaking servers.
There is no built-in reconnect or discovery; each socket wraps one connection.

### Codec tokenization (port vs token)

The pre-transport world established every logical channel by **transferring a
MessagePort** (`new MessageChannel()`, `port2` pushed into the message transfer
list). Fork IPC and WebSocket cannot transfer handles, so channel identity now
travels as an **establishment token** instead:

- on a **messageport** transport, `openChannel(ctx)` still returns the peer port
  (transferable); the placeholder carries `{ port }`, decode calls
  `connectChannel(port)`;
- on a **Mux** transport (framed/message), `openChannel(ctx)` returns
  `{ token: { __mux: "open", ch } }` (also sent on the main channel so the
  peer's Mux opens its end); the placeholder carries `{ token }`, decode calls
  `connectToken(transport, token)`.

Every transport-aware codec produces both forms in its placeholder and branches
on decode — iterable, abort-signal, callback, and remote-ref (fresh tokens) all
work identically across processes and workers. A codec that fundamentally
requires a MessagePort (remote-ref's liveness planes) only runs on messageport
transports. The RPC machinery (`makeRpcHandler` / `createRpcProxy`) is
channel-agnostic: it takes the transport in context and never touches ports or
tokens itself.

### ActorRegistry (reference-acquire routing)

`ActorRegistry` (`core/registry.ts`) is the bootstrap/discovery table that
resolves "who is where". Every spawned actor (a worker, a process actor, a node)
gets a **stable transport id** (`register(transport)` → `"t1"`, `"t2", …`), sent
to the actor after its handshake and embedded in refIds as the prefix, so refIds
are globally unique AND routeable back to their owner. The registry maps id →
Transport (`resolve(id)`, `idOf(transport)`), unregisters on death, and notifies
listeners (`onUnregister`) for owner-death cleanup.

Reference-acquire routing uses it: on `__acquire-ref` (or a main-side pending
ref), the coordinator resolves the refId prefix to the owner's Transport and
bootstraps a direct owner↔requester channel — transferring a fresh MessagePort
pair for a messageport owner, or relying on the token mechanism for a Mux owner.
The registry is deliberately small, synchronous, and single-coordinator
(distributed membership/failure detection is out of scope); it is pluggable, so
the bootstrap table is not hardcoded into spawn.

### Callback / function by-ref

A bare function cannot be structured-cloned, so any function value in a payload
is automatically turned into a **remote callback** (`core/codecs/callback.ts`):
the caller gets a directly callable reference, and invoking it executes the
function at its registration point (its closure) and marshals the result back.
No explicit wrapping — `longTask(n, (p) => progress(p))` just works; nested
fields (`{ onDone: fn }`) travel byref automatically too.

Mechanism-wise a callback is a **single-function Actor**, reusing the exact RPC
machinery of the main channel and links:

- owner side: `makeRpcHandler({ call: fn }, registry)` over a per-callback
  Channel — on a messageport transport the channel is a transferred
  MessageChannel; on a Mux transport it is a logical sub-channel established by
  token — the function runs in this context, results/errors round-trip through
  the registry;
- calling side: `createRpcProxy` + a function-targeted Proxy whose `apply` trap
  marshals the call; the promise stays a real promise (await/.catch work);
  `dispose()` releases it.

**Async callbacks and awaiting results**: a callback reference is typed
`(...args) => Promise<Awaited<R>>`, and the owner side awaits the function
(`makeRpcHandler` awaits `fn(...args)`), so "wait for the callback to finish and
use its return value" works naturally — a worker writes `const r = await cb(x)`
and the final value (or a `RemoteError` for a rejection) arrives back. Only the
awaiting method suspends; the worker event loop keeps processing other messages.
Not awaiting yields a Promise, exactly like native async semantics.

Lifecycle matches remote-ref: explicit `dispose()`, GC-based release
(FinalizationRegistry, best-effort), and failAll (the registry closes the
channel). Callbacks are **behavior, not identity**: there is no refId, hand-off,
or restore — and a callback reference cannot be re-encoded (encoding one fails
loudly, since a proxy holder must not re-route the owner connection to a third
party).

**Declaring callback parameters** (avoiding type/runtime mismatch): at runtime a
callback reference always returns a Promise, regardless of how the caller wrote
the function. Declare the honest form — `cb: (x) => Promise<R>` (or async) — and
let `TransformCallbacks` (applied at the `Remote<T>` projection) widen the
caller-facing shape to `R | Promise<R>`, so BOTH sync and async functions can be
passed while the worker body stays type-safe: `await cb(x)` types as `R`, and
using `cb(x)` as a value is rejected by the compiler. No separate helper type is
needed; `RemoteCallback` remains the reference type.

### Channel abstraction for custom protocols

The stream primitives are a special case of a more general need: a codec often
wants a dedicated cross-boundary channel with its own wire protocol — streaming
elements, abort propagation, or a custom **marshal-by-ref** protocol (the .NET
MarshalByRef pattern). `core/channel.ts` is the high-level counterpart to raw
MessageChannel handling, owned by the codec author and sitting on top of the
Transport abstraction:

- `openChannel(ctx)` — open a logical channel through the context's transport
  and return a `ChannelPeer`: the local `Channel` plus how the peer end crosses
  the boundary. On a messageport transport this creates a MessageChannel and
  pushes the peer port into `ctx.transfer` (`peerPort`); on a framed/message
  (Mux) transport it calls `transport.openChannel()` and returns the
  `{ __mux: "open", ch }` token instead (`token`) — the token is also sent on
  the main channel so the peer's Mux opens its end and acks.
- `connectChannel(port)` — wrap a transferred peer port as a `Channel`.
- `connectToken(transport, token)` — resolve a Mux token to the peer's Channel,
  claiming it from the transport's orphaned-channel cache if the open control
  arrived before the consumer connected, or returning a proxy that resolves once
  the channel arrives. `Channel.kind` ("messageport" | "framed") tells a codec
  which form it is talking to: codecs that fundamentally require a MessagePort
  (e.g. remote-ref's liveness planes) only run on messageport transports.
- `registerChannel(channel)` (on the registry) — failAll() closes every open
  channel when the actor dies.
- `registerRelease(target, onReleased)` — FinalizationRegistry wrapper for
  GC-based release; returns an unregister function so explicit close keeps
  release single.

The library deliberately provides **no automatic protocol on a channel**: a
codec gets the channel and defines its own frames. What the library guarantees
is channel creation, peer-end hand-over (port or token), closure and GC-based
release. `EncodeContext` and `DecodeContext` now expose the `transport` and the
`registry`, so a codec can recurse into nested payloads — frames on its own
channel whose args/results may contain streams or other codec values.

`examples/remote_ref/` demonstrates the pattern: a custom `remote-ref` codec
turns any object into a cross-boundary reference (method calls marshaled over a
dedicated channel, errors serialized back, dispose/GC release, nested streams
flowing through reference results). It is an example, not a built-in — the
library stays protocol-agnostic.

**Reference identity, restore and indirect sharing** (the ActorRef semantics):

- Every real object has a stable **refId** (the prefix is the owner worker's
  main-assigned id, so refIds are globally unique AND routeable back to the
  owner). Repeating `remoteRef(x)` reuses the identity; a receiver dedupes by
  refId, so two refs to the same object compare equal and share one proxy.
- Handing a reference over is a **share, not a move**: a proxy encodes as its
  refId token only, and the original proxy stays alive. Any number of holders
  can hold the same identity.
- A reference that travels back to its **owner** is **restored**: the owner
  recognizes the refId, collapses it into a local call-through reference (no
  proxy, no channel — method calls run directly on the real object) and closes
  the channels still open for it.
- **Indirect sharing across actors**: when a refId token arrives at a non-owner,
  a _pending proxy_ is created; its first call triggers an acquire over the main
  channel (`__acquire-ref`). The coordinator resolves the refId prefix to the
  owner transport via the **ActorRegistry** and bootstraps a fresh
  owner↔requester channel (`__serve-ref` / `__ref-acquired`) — the coordinator
  is only the one-time router; the established channel is direct. On a
  messageport transport the bootstrapped channel is a transferred MessagePort;
  on a Mux transport it is a logical sub-channel opened by token (see
  **ActorRegistry**). Each holder gets its own per-holder channel to the owner
  (per-holder FIFO), and all holders reach the SAME real object. RefIds produced
  before the actor id arrives fall back to a random prefix and are simply never
  acquire-routed.
- Only the owner can produce fresh references; holders can only share the
  identity. Channels are never transferred — the reference is the identity, not
  the wire.

### Direct worker-to-worker links

A worker cannot talk to another worker directly (DedicatedWorker has no peer
addressing), so the main thread bootstraps a link — but never relays its
contents. `link(workerB, workerC, label)` opens a MessageChannel and transfers
one port into each worker as a `__link` control frame (the frames live on the
main RPC channel; the link itself is a separate channel). Each worker's
`serveWorker` exposes the link via `onLink`:

- `link.send(value)` — send any codec value to the peer. Encoding runs through
  the sender's registry, so the payload may be a remote reference, a stream, an
  AbortSignal, or plain structured-cloneable data. The main thread never sees
  the value.
- `link.onValue(handler)` — receive decoded values from the peer.
- `link.close()` / `unlink()` — tear the link down; failAll also closes every
  open link when an actor dies.

This enables A→B→C topologies without A in the data path: worker B hands a
reference to an object it owns directly to worker C over the link, and C calls
it — the reference channel runs between B and C only. The link is
**bidirectional**, so C can hand its own objects back to B. A peer-death
detection is best-effort (MessagePort has no close event; a closed channel
surfaces as send errors / delivery failure), recorded as a limitation.

Constraints: both link endpoints must register a compatible codec set for the
values they exchange (a mismatch fails loudly on decode, like the RPC
handshake). References travel over links by identity (see the restore semantics
above): a holder may hand a reference along, and only the owner can produce
fresh ones.

**Direct peer RPC over links** is implemented and reuses the same machinery as
the main channel: `core/rpc.ts` provides channel-agnostic factories —
`makeRpcHandler(api, registry)` (lookup → decode args → await → encode result,
shared by `serveWorker` and every link) and `createRpcProxy(registry, send)`
(pending-map + id correlation + codec encoding, shared by `spawn` and every
link). A channel is just an adapter. The link frame set adds `call`/`result`,
and each endpoint is simultaneously a serving side and a calling side
(bidirectional). The peer-callable surface is **independent of the main-thread
rpc**: `link.serve(peerApi)` declares what the peer may call (defaults to the
main api, so management methods stay hidden unless re-exposed), and `link.rpc`
proxies calls to the peer. Contract types cannot be derived across modules
(workers have no import relationship), so the defining side exports
`type XPeerApi = PeerRpc<typeof peerApi>` and the caller imports it type-only —
worker-to-worker typing is an explicit contract, not inference.

**Functions/closures are not codec-ified**: that would violate structured-clone
semantics; `Remote<T>` already treats functions as RPC methods, not data.

## Protocol design

```ts
type Frame =
  | {
    type: "handshake";
    version: number;
    codecs: string[];
    kind?: string;
    actors?: string[];
  } // actor ready + codec list + transport kind (+ node actor names)
  | { type: "request"; id: number; method: string; args: unknown[] }
  | { type: "response"; id: number; ok: true; value: unknown }
  | { type: "response"; id: number; ok: false; error: SerializedError }
  | { type: "dispose" }; // graceful shutdown
```

- **The handshake frame** lets `spawn()` resolve only after the actor module is
  loaded and its runtime (`serveWorker`/`serveProcess`/`serveNode`) has run; it
  carries both sides' codec tag lists, the **transport `kind`**, and — for a
  multi-actor node — the announced actor names. A version/codec mismatch, a
  transport-kind mismatch, or an actor crash rejects the handshake, so spawn()
  never hangs.
- **Transport-kind check**: the handshake `kind` field mirrors the host's
  transport kind ("messageport" / "framed" / "message"). A mismatch rejects
  spawn() — a Mux transport must not be treated as a messageport one (there is
  no MessagePort transfer across Mux). A peer that omits `kind` (an older
  protocol) is accepted as messageport. The node handshake instead only
  announces the actor names; the peer opens per-actor channels after it (see
  **Multi-actor nodes**).
- **Creation interruption is a three-state `signal` option** (not a bare
  timeout): omitted → a default `AbortSignal.timeout(10s)`; `null` → no
  interruption; an `AbortSignal` → user-controlled, and the rejection reason is
  `signal.reason` (composable via `AbortSignal.any`, cancellable via a shared
  controller, disabled by an idle signal). A `TimeoutError` reason keeps the
  "did it call serveWorker()?" diagnostic. The signal governs creation only:
  after resolve the listener is dropped and the actor's lifecycle belongs to
  `dispose()`; an already-aborted signal fails spawn() immediately.
- **Incrementing ids** correlate requests and responses; responses may arrive
  out of order. A single worker thread naturally processes requests serially,
  matching the Actor "one actor processes messages in order" semantics;
  concurrent calls are routed through a pending Map.
- **Error serialization**: modern runtimes can structured-clone Error types and
  DOMException (subtype identity, name/message/stack preserved), but custom
  properties are lost and custom Error subclasses degrade to Error (instanceof
  fails, name becomes "Error"). The protocol therefore serializes errors
  uniformly as `{name, message, stack}`, rebuilt on the main thread as
  `RemoteError` (instanceof Error, keeps worker name/stack) — consistent across
  implementations and extensible to cause/code fields; a contrast to "Actor
  exceptions don't propagate back" — here they do, explicitly. For **known
  natively-cloneable types** (built-in Error subclasses and DOMException) the
  original error is additionally structured-cloned into `native`, exposed as
  `RemoteError.inner` — restoring instanceof identity and `DOMException.code`.
  Custom subclasses and errors with custom properties stay manual-only (their
  clone would degrade, so it adds nothing); the clone is best-effort and dropped
  if it would fail, the manual fields always carry the error.
- **Death detection**: `onerror` / `onmessageerror` / handshake timeout /
  dispose all enter the dead state; in-flight calls reject with
  `ActorDiedError`, later calls are rejected immediately. `SpawnOptions.onDeath`
  fires on the kill path (crash / handshake failure / interrupted creation) but
  NOT on dispose — the only way to observe a crash, since spawn owns the
  worker's onerror/onmessage handlers.

## Actor pooling

`createActorPool<T>(options)` is a **lightweight combinator**, not a task
scheduler: it takes a homogeneous set of members and exposes one typed call
surface (`Remote<T>`) that routes each call to a member — picking a member,
calling its spawn proxy (the same `createRpcProxy` path), and tracking liveness.
No queue, no work-stealing. Members are produced by a `spawnWorker` factory of
`Worker | Transport`, so a pool can also span process actors or any Transport
adapter.

- Routing: `"round-robin"` (default) / `"least-busy"` (fewest in-flight) / a
  custom `(method, args) => index` function (validated; must target a live
  member). All members dead → calls reject with `ActorDiedError`.
- Member lifecycle: spawn's `onDeath` marks the member dead, drops it from
  routing, fires `onMemberDead(index, reason)`, and — when `replace` is set
  (boolean or a factory) — rebuilds it. `dispose()` terminates every member
  (idempotent).
- Member-bound payloads (documented constraint): plain data routes freely; a
  main-thread-created callback routes to any member (it executes on the main
  thread regardless of holder); fresh object-reference tokens (remote-ref) route
  anywhere, but a refId-only (moved) token must not cross members — use
  `invokeOn(index, method, args)` to pin the owning member; streams bind to the
  producing member's lifetime and are consumed by the caller, never re-routed.
  Stream-returning methods stay lazy through the pooled proxy
  (`attachLazyIterator` is applied to pooled calls, same as spawn).
- `size` reports live members; the pool is usable immediately, before any
  member's handshake completes (calls wait for the first ready member).

## Lifecycle

| Event                | Behavior                                                                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `actor.dispose()`    | sends dispose frame → worker `self.close()` / process IPC channel close, then `terminate()`/`kill()` as a safety net; all in-flight calls reject |
| worker/process crash | dead state; in-flight reject; later calls throw `ActorDiedError`                                                                                 |
| handshake timeout    | dead state, rejects with "did it call serveWorker()?"                                                                                            |
| `Symbol.dispose`     | supports `using actor = ...` (TS 5.2+)                                                                                                           |

## Reference strength and release

By-ref objects (remote-ref) and functions (callbacks) have no shared memory
across workers — "cross-worker reference strength" is entirely a local closure
graph in the owning process. The library's release semantics:

- **Holder side**: a reference/callback proxy is held **weakly** (WeakRef
  dedupe + FinalizationRegistry); once the holder drops it, GC notifies the
  owner (a `dispose` frame) and the per-holder channel closes.
- **Owner side (ref)**: the owner channel closure captures only the refId, not
  the object; the object is deref'd on each call. While at least one holder
  channel is open, the owner holds the object **strongly** (a per-refId strong
  map); when the LAST channel closes, the strong ref is dropped and the object
  becomes collectable.
- **Owner side (callback)**: the function is held via WeakRef per channel; the
  channel closure derefs it per call. Releasing the callback makes the function
  (and its closure) collectable.
- **Released object / function**: calls on a reference whose object was
  collected fail with `RemoteError("… has been released")` and the channel
  closes — the reference's address died, the worker is unaffected.
- **Lifecycle contract**: an object held by the owner's own code (mode 1) stays
  alive regardless of references; an object only reachable through references
  (mode 2) dies once all holders release it. Closed channels self-remove from
  the registry and owner tables, so finished closure chains (and transitively
  released objects) are collectable immediately, not at actor death.

## Known limitations

- The actor handshake is **not buffered**: call `spawn()` right after
  `new Worker(...)` (or the process/Transport is up). If messages arrive before
  the handler is set (e.g. another await in between), the handshake is lost and
  spawn() waits until the handshake timeout.
- One RPC entry object per actor module (extendable to
  `serveWorker({ ns: { … } })` namespaces). A **multi-actor node** (`serveNode`)
  gets around this by serving one entry per logical channel.
- If the channel's send throws a `DataCloneError` (e.g. a function/class
  instance on a messageport transport), the call rejects and the actor enters
  the dead state.
- Stream elements must themselves be transportable (an iterable nested inside an
  iterable gets its own channel — correct but unusual).
- Sync iterables (generators, custom Iterable) are wrapped as async and go
  through the same channel; natively cloneable containers (arrays/Map/Set) are
  treated as plain values, no channel.
- A consumer that neither `return()`s nor drains a stream: the producer-side
  object graph stays alive until the stream is GC-released (best-effort
  finalizer), explicitly returned, or the actor dies — it does not grow, it just
  stays resident.
- **Cross-process remote-ref indirect sharing may be untested**: a refId-only
  hand-off (the shared/indirect path) requires the acquire router to bootstrap
  an owner↔requester channel over a Mux transport (token rather than port);
  direct fresh-token refs across processes are covered by tests, but the
  refId-only-acquire-over-Mux path is not verified end-to-end.
- **Multi-machine WebSocket transport is not verified**: `fromWebSocket` is
  exercised only over a local loopback WebSocket; WAN latency, reconnects and
  interop with non-v8 peers are untested.
- **Mux has no built-in backpressure on message-kind transports**: a framed
  byte-stream transport backpressures through the encoder/decoder
  TransformStreams, but on message-kind transports (fork IPC / WebSocket) each
  message is handed straight to the OS/peer with no flow control — a fast
  producer can flood a slow consumer. Stream codecs mitigate this at the
  protocol level (items are delivered only while `next()` is pending), but the
  main channel itself is unthrottled.
- Browser deployment needs
  `new Worker(new URL("./worker.ts", import.meta.url),
  { type: "module" })`
  instead of `import.meta.resolve` (no `import.meta.resolve` in browsers).
- Boundary types like non-function fields and generic overloads resolve to
  `never`/loose types under `Remote<T>`; a `satisfies` contract type can tighten
  that if needed (see below).

## Next steps

- **Namespaces/events**: `{ type: "event"; name; payload }` frame + subscribe/
  unsubscribe, events dispatched per `[name]`.
- **Pooling over processes**: a `createProcessPool` or a pool factory over
  `spawnProcess` (today the pool's `spawnWorker` factory already accepts any
  `Worker | Transport`, but no process-specific convenience exists).
- **Uploads**: `Transferable` argument lists (zero-copy ArrayBuffer/
  OffscreenCanvas), typed via `Transfer<ArrayBuffer>` — a messageport-only
  optimization; Mux transports have no transferables.
- **Bidirectional**: worker calls main-thread APIs back, a symmetric runtime
  reusing the same protocol over a MessagePort.
- **Version negotiation**: the handshake `version` becomes a list of supported
  minimum versions.
- **Contract validation**: `spawn(satisfies<Contract> …)` checks the worker API
  against a contract shape and throws on mismatch.
- **Channel reuse**: one worker registering multiple API surfaces; spawn returns
  sub-namespace proxies sharing one frame channel.
- **Push events**: wrap pull-based AsyncIterable streams so the peer pushes data
  into local event subscriptions (pull/push dual mode).
- **Distributed discovery**: the ActorRegistry is the minimal single-coordinator
  bootstrap table; a shared registry / self-discovery for multi-node and
  multi-machine deployment remains open (see TRANSPORT.md).
