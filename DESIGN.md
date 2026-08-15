# Worker ⇄ Actor — Design

A minimal library that wraps a Web Worker into a **type-safe Actor**. Design
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
mod.ts                    # public exports
core/protocol.ts          # frame types, error serialization, handshake version
core/codec.ts             # generic Codec interface + PayloadCodecRegistry (deep walk/placeholder/lifecycle)
core/channel.ts           # high-level Channel abstraction for codec authors (open/connect/release)
core/codecs/              # built-in codecs: iterable / error / abort-signal
core/stream.ts            # stream protocol on top of Channel (pump/rebuild/backpressure/release)
spawn.ts                  # main-thread: spawn() → type-safe Proxy + lifecycle + codec validation
worker_runtime.ts         # worker-side: serveWorker(api) registers RPC, handshake, dispatch
examples/calculator/      # end-to-end example (worker.ts + main.ts)
examples/remote_ref/      # custom marshal-by-ref codec (the .NET MarshalByRef pattern)
test_fixtures/            # test-only worker for the codec mechanism
main_test.ts / codec_test.ts / ref_test.ts  # integration tests over real Workers
```

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

```ts
export type Remote<T> = {
  [K in keyof T]: T[K] extends RpcFn
    ? (...args: Parameters<T[K]>) => Promise<Resolved<ReturnType<T[K]>>>
    : never;
};
```

- Only function members survive; non-function members resolve to `never` (a
  mistyped field fails immediately at compile time).
- Return values are normalized to Promise; single-parameter `Promise`
  (`Awaited`) flattens automatically, no recursive types.
- Callers always write `spawn<typeof WorkerModule.rpc>` and never a second
  interface definition — the worker is the source of type truth.
- The proxy additionally exposes `dispose(): Promise<void>`.

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
  encode(value: T, ctx): unknown; // → { __wCodec: tag, ... }; may open a channel and transfer a port
  decode(placeholder, ctx): T; // rebuild the original type
  onRegistryFail?(state): void; // cleanup when the actor terminates/crashes
}
```

- Both sides register via `spawn(worker, { codecs })` and
  `serveWorker(api, { codecs })`; user codecs match before built-ins (and can
  override a same-tag built-in). The registry deep-walks payloads (any nesting
  inside objects/arrays/Map/Set) and picks the first codec whose `matches()`
  fires, in registration order.
- **The handshake carries both sides' codec tag lists** and validates them: a
  mismatch rejects spawn() and reports the missing/extra tags — a registration
  mismatch becomes a startup failure instead of silently producing garbage.
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
  of stream data never head-of-line-blocks requests/responses.
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

### Channel abstraction for custom protocols

The stream primitives are a special case of a more general need: a codec often
wants a dedicated cross-thread channel with its own wire protocol — streaming
elements, abort propagation, or a custom **marshal-by-ref** protocol (the .NET
MarshalByRef pattern). `core/channel.ts` is the high-level counterpart to raw
MessageChannel handling, owned by the codec author:

- `openChannel(ctx)` — create a MessageChannel and automatically add the peer
  port to `ctx.transfer` (transferred with the placeholder); returns a `Channel`
  wrapping the local port.
- `connectChannel(port)` — wrap a transferred port as a `Channel`.
- `registerChannel(channel)` (on the registry) — failAll() closes every open
  channel when the actor dies.
- `registerRelease(target, onReleased)` — FinalizationRegistry wrapper for
  GC-based release; returns an unregister function so explicit close keeps
  release single.

The library deliberately provides **no automatic protocol on a channel**: a
codec gets the channel and defines its own frames. What the library guarantees
is channel creation, port transfer, closure and GC-based release.
`EncodeContext` and `DecodeContext` now expose the `registry`, so a codec can
recurse into nested payloads — frames on its own channel whose args/results may
contain streams or other codec values.

`examples/remote_ref/` demonstrates the pattern: a custom `remote-ref` codec
turns any object into a cross-thread reference (method calls marshaled over a
dedicated channel, errors serialized back, dispose/GC release, nested streams
flowing through reference results). It is an example, not a built-in — the
library stays protocol-agnostic.

**Reference identity and restore** (the ActorRef semantics):

- Every real object has a stable **refId** (registered on the owner side, with a
  random per-process prefix so ids never collide across workers). Repeating
  `remoteRef(x)` reuses the identity; a receiver dedupes by refId, so two refs
  to the same object compare equal and share one proxy.
- Handing a reference over is a **hand-off**: the identity travels on, and the
  previous holder's channel is closed (per-holder connection to the owner). The
  previous proxy becomes dead — the reference is single-holder at any time.
- A reference that travels back to its **owner** is **restored**: the owner
  recognizes the refId, collapses it into a local call-through reference (no
  proxy, no channel — method calls run directly on the real object) and closes
  the channels still open for it. This works after any number of hand-offs:
  identity travels, the owner recognizes it home.
- Only the owner can produce fresh references; a proxy holder can only hand the
  reference along. A refId-only hand-off arriving at a non-owner is refused
  loudly (the holder cannot re-establish the owner connection for a third
  party). Channels are therefore never transferred — the reference is the
  identity, not the wire.

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
  | { type: "handshake"; version: number; codecs: string[] } // worker ready + codec list
  | { type: "request"; id: number; method: string; args: unknown[] }
  | { type: "response"; id: number; ok: true; value: unknown }
  | { type: "response"; id: number; ok: false; error: SerializedError }
  | { type: "dispose" }; // graceful shutdown
```

- **The handshake frame** lets `spawn()` resolve only after the worker module is
  loaded and `serveWorker()` has run; it carries both sides' codec tag lists and
  a mismatch kills the actor. Version/codec mismatch or a worker crash rejects
  the handshake, so spawn() never hangs.
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
  exceptions don't propagate back" — here they do, explicitly.
- **Death detection**: `onerror` / `onmessageerror` / handshake timeout /
  dispose all enter the dead state; in-flight calls reject with
  `ActorDiedError`, later calls are rejected immediately.

## Lifecycle

| Event                  | Behavior                                                                                                    |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- |
| `actor.dispose()`      | sends dispose frame → worker `self.close()`, then `terminate()` as a safety net; all in-flight calls reject |
| worker crash (onerror) | dead state; in-flight reject; later calls throw `ActorDiedError`                                            |
| handshake timeout      | dead state, rejects with "did it call serveWorker()?"                                                       |
| `Symbol.dispose`       | supports `using actor = ...` (TS 5.2+)                                                                      |

## Known limitations

- The worker handshake is **not buffered**: call `spawn()` right after
  `new Worker(...)`. If messages arrive before the handler is set (e.g. another
  await in between), the handshake is lost and spawn() waits until the handshake
  timeout.
- One RPC entry object per worker module (extendable to
  `serveWorker({ ns: { … } })` namespaces).
- If `worker.postMessage` throws `DataCloneError` (e.g. a function/class
  instance), the call rejects and the actor enters the dead state.
- Stream elements must themselves be structured-cloneable (an iterable nested
  inside an iterable gets its own channel — correct but unusual).
- Sync iterables (generators, custom Iterable) are wrapped as async and go
  through the same channel; natively cloneable containers (arrays/Map/Set) are
  treated as plain values, no channel.
- A consumer that neither `return()`s nor drains a stream: the producer-side
  object graph stays alive until the stream is GC-released (best-effort
  finalizer), explicitly returned, or the actor dies — it does not grow, it just
  stays resident.
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
- **Pooling**: multiple workers with preemptive scheduling —
  `createActorPool<typeof rpc>(n, url)` on top of spawn.
- **Uploads**: `Transferable` argument lists (zero-copy ArrayBuffer/
  OffscreenCanvas), typed via `Transfer<ArrayBuffer>`.
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
