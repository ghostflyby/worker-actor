/**
 * Main-thread entry point: wraps a Worker into a type-safe Actor proxy.
 *
 * Usage:
 *   // worker.ts —— export the agreed-upon rpc object; serveWorker() registers it
 *   export const rpc = { add(a: number, b: number) { return a + b; } };
 *
 *   // main.ts —— import type only grabs types, no worker module side effects
 *   import type * as WorkerModule from "./worker.ts";
 *   const actor = await spawn<typeof WorkerModule.rpc>(new Worker(url, { type: "module" }));
 *   const sum = await actor.add(1, 2); // sum: number, type-safe
 */

import {
  ActorDiedError,
  type Frame,
  PROTOCOL_VERSION,
} from "./core/protocol.ts";
import { type Codec, PayloadCodecRegistry } from "./core/codec.ts";
import { createRpcProxy, type RpcProxy } from "./core/rpc.ts";
import type { CodecValueTypes, TransformCallbacks } from "./core/type-utils.ts";
import {
  type ControlFrame,
  dispatchControlFrame,
  setMainAcquire,
} from "./core/worker-context.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";
import { callbackCodec } from "./core/codecs/callback.ts";
import { fromMessagePort, type Transport } from "./core/transport.ts";
import { fromNodeIpc } from "./core/transport.ts";

// The RPC boundary is inherently dynamic: type safety comes from Remote<T>
// deriving the worker's concrete signatures, and this any only serves shape
// matching — it never leaks into the user-visible proxy type.
// deno-lint-ignore no-explicit-any
export type RpcFn = (...args: any[]) => any;

/** Return values are normalized to promises (already-promises are not re-wrapped). */
type Resolved<T> = T extends Promise<unknown> ? Awaited<T> : T;

/**
 * Derives the remote proxy type from the worker-side API shape. Each method's
 * return type is projected so the caller sees an honest shape for the value
 * that actually crosses the boundary — the proxy call is always a native
 * Promise at runtime, and the RPC handler `await`s the worker method's return
 * value before encoding it. Rules, in order:
 *
 *   1. never-returning (throwing) methods → Promise<never> (must come first:
 *      never structurally matches PromiseLike).
 *   2. A return type covered by a runtime codec (`Pass`, derived from the
 *      const `codecs` tuple via CodecValueTypes) → kept exactly as declared:
 *      the codec rebuilds that type on the far side, so full identity survives.
 *   3. A PromiseLike<X> return type (native Promise, a custom thenable, an
 *      explicit Promise<AsyncIterable<E>>, or a thenable-stream hybrid) →
 *      Promise<X>: the handler awaits the value, so what crosses is the
 *      resolution X — never a nested Promise.
 *   4. A bare AsyncIterable<E> return type → AsyncIterable<E>: the iterable
 *      codec rebuilds a local stream (lazy: the first next() triggers the
 *      remote call). Custom AsyncIterable subclasses flatten to the interface.
 *   5. Every other function member → Promise<Resolved<R>> (sync returns
 *      normalize to a Promise).
 *   6. Non-function members (constants, classes, ...) → never, failing at
 *      compile time.
 *
 * Parameters are projected through TransformCallbacks in every branch: a
 * Promise-returning function parameter (the worker's honest declaration)
 * widens to `R | Promise<R>` on the calling side, so BOTH sync and async
 * functions can be passed.
 */
export type Remote<T, Pass extends unknown = never> = {
  [K in keyof T]: T[K] extends (...args: infer A) => never
    ? (...args: A) => Promise<never>
    : T[K] extends (...args: infer A) => infer R
      ? R extends Pass ? (...args: TransformCallbacks<A>) => R // codec rebuilds R on the far side: keep identity
      : R extends PromiseLike<infer X>
        ? (...args: TransformCallbacks<A>) => Promise<X> // handler awaits: crossing value is X
      : R extends AsyncIterable<infer E>
        ? (...args: TransformCallbacks<A>) => AsyncIterable<E>
      : T[K] extends RpcFn ? (
          ...args: TransformCallbacks<Parameters<T[K]>>
        ) => Promise<Resolved<ReturnType<T[K]>>>
      : never
    : never;
};

export interface SpawnOptions<
  C extends readonly Codec<unknown>[] = readonly Codec<unknown>[],
> {
  /**
   * Interruption policy for creation (the handshake):
   *   - omitted (undefined): default 10s timeout
   *   - null: no interruption at all
   *   - AbortSignal: creation is aborted when the signal fires; the rejection
   *     reason is signal.reason (a TimeoutError from AbortSignal.timeout(n)
   *     keeps the "did it call serveWorker()?" diagnostic)
   * The signal only governs creation: after spawn() resolves, the actor's
   * lifecycle is owned by dispose(). An already-aborted signal fails spawn()
   * immediately.
   */
  signal?: AbortSignal | null;
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal / callback. Both sides must
   * register the same tag list; the handshake validates it and a mismatch
   * kills the actor.
   *
   * The const tuple also drives the type projection: a return type covered by
   * a registered codec (CodecValueTypes<C>) is kept exactly as declared on
   * the Remote<T> proxy, because that codec rebuilds it on the far side.
   * Pass an `as const` tuple to opt in; a plain array or Codec<unknown>
   * entries fall back to the built-in async projection rules.
   */
  codecs?: C;
  /**
   * Fired when the actor dies through the kill() path: a worker crash, a
   * handshake failure (version/codec mismatch, timeout, interrupted creation).
   * NOT fired by dispose() — a deliberate shutdown is not a death. The pool
   * uses this to remove/replace a member; spawn's own onerror/onmessage
   * handlers are assigned internally, so this is the only way to observe a
   * crash without breaking spawn.
   */
  onDeath?: (reason: unknown) => void;
}

const HANDSHAKE_TIMEOUT = 10_000;

// Reference-acquire routing: each spawned actor gets a stable transport id
// (embedded in refIds as a prefix), so the coordinator can resolve
// "refId → owner transport" and bootstrap an owner↔requester channel on
// demand. This is the pluggable ActorRegistry — the minimal discovery layer.
import { createActorRegistry } from "./core/registry.ts";

const actorRegistry = createActorRegistry();
// Established (owner, holder) pairs: the liveness channel is created once per
// pair, on the first serve; later serves reuse it on both sides.
const livenessPairs = new Set<string>();

function routeAcquire(refId: string, requester: Transport): void {
  // refId format: "<ownerId>:<localCount>"
  const ownerId = refId.slice(0, refId.indexOf(":"));
  const owner = actorRegistry.resolve(ownerId);
  if (!owner) return; // unknown or dead owner: nothing to bootstrap
  const requesterId = actorRegistry.idOf(requester);
  const { port1, port2 } = new MessageChannel();
  const transfer1: Transferable[] = [port1];
  const transfer2: Transferable[] = [port2];
  // Liveness plane: the owner pulls heartbeats over one channel per worker pair,
  // so a dead holder is detected once and its refs are released in a batch.
  let livenessPort1: MessagePort | undefined;
  let livenessPort2: MessagePort | undefined;
  const pairKey = requesterId === undefined
    ? undefined
    : `${ownerId}:${requesterId}`;
  if (pairKey !== undefined && !livenessPairs.has(pairKey)) {
    livenessPairs.add(pairKey);
    const lc = new MessageChannel();
    livenessPort1 = lc.port1;
    livenessPort2 = lc.port2;
    transfer1.push(lc.port1);
    transfer2.push(lc.port2);
  }
  owner.send(
    {
      type: "__serve-ref",
      refId,
      port: port1,
      holderId: requesterId,
      livenessPort: livenessPort1,
    } satisfies Frame,
    transfer1,
  );
  requester.send(
    {
      type: "__ref-acquired",
      refId,
      port: port2,
      ownerId,
      livenessPort: livenessPort2,
    } satisfies Frame,
    transfer2,
  );
}

/** Main-side acquire: the main thread itself is the requester (materialize locally). */
function routeAcquireMain(refId: string): void {
  const ownerId = refId.slice(0, refId.indexOf(":"));
  const owner = actorRegistry.resolve(ownerId);
  if (!owner) return;
  const { port1, port2 } = new MessageChannel();
  owner.send(
    { type: "__serve-ref", refId, port: port1 } satisfies Frame,
    [port1],
  );
  dispatchControlFrame({ type: "__ref-acquired", refId, port: port2 });
}

// Registered once per module load: any main-side pending ref (created while
// decoding responses) triggers direct routing + local materialization.
setMainAcquire(routeAcquireMain);

/**
 * Make a stream-returning method's promise directly consumable via `for await`
 * (the Remote<T> special case types it as AsyncIterable<E>). The promise stays
 * a real promise (await/.catch/.finally all work); the attached iterator
 * resolves it on the first next() and forwards the inner remote iterable.
 * Element-level laziness is preserved by the iterable codec itself: the
 * worker-side generator only runs after the first next() (the start frame).
 *
 * Exported so an actor pool (which reuses the Remote<T> shape) applies the
 * same laziness to its routed calls.
 */
export function attachLazyIterator(p: Promise<unknown>): void {
  let attached = false;
  Object.defineProperty(p, Symbol.asyncIterator, {
    configurable: true,
    value() {
      if (!attached) {
        attached = true;
        void p.catch(() => {}); // avoid unhandled rejection while awaiting later
      }
      let iterator: AsyncIterator<unknown> | undefined;
      let started = false;
      return {
        async next(): Promise<IteratorResult<unknown>> {
          if (!started) {
            started = true;
            const inner = await p as AsyncIterable<unknown>;
            iterator = inner[Symbol.asyncIterator]();
          }
          return iterator!.next();
        },
        async return(value?: unknown): Promise<IteratorResult<unknown>> {
          if (iterator) {
            const r = iterator.return?.(value);
            return r ? await r : { done: true, value };
          }
          // never started: the worker generator never ran, nothing to cancel
          return { done: true, value };
        },
      };
    },
  });
}

/** Lifecycle methods attached to the proxy (not part of Remote<T> itself). */
export interface ActorHandle {
  /** Sends a dispose frame and terminates the worker; all in-flight calls reject. */
  dispose(): Promise<void>;
}

/**
 * Establish a direct, bidirectional link channel between two workers, bypassing
 * the main thread. Values sent over the link are encoded/decoded through each
 * worker's own codec registry, so references and streams hand directly between
 * the two workers without the main thread in the path. Both workers must expose
 * the link via serveWorker's onLink option (and register compatible codecs).
 *
 * Returns a tear-down function that tells both workers to close the link.
 */
export function link(a: Worker, b: Worker, label: string): () => void {
  const { port1, port2 } = new MessageChannel();
  a.postMessage({ type: "__link", label, port: port1 } satisfies Frame, {
    transfer: [port1],
  });
  b.postMessage({ type: "__link", label, port: port2 } satisfies Frame, {
    transfer: [port2],
  });
  return () => {
    a.postMessage({ type: "__link-close", label } satisfies Frame);
    b.postMessage({ type: "__link-close", label } satisfies Frame);
  };
}

export function spawn<
  T,
  const C extends readonly Codec<unknown>[] = readonly Codec<unknown>[],
>(
  source: Worker | Transport,
  options: SpawnOptions<C> = {},
): Promise<Remote<T, CodecValueTypes<C>> & ActorHandle> {
  // Worker is the structured-clone message-port world: convert it to a
  // messageport-type Transport and recurse on the unified abstraction. The raw
  // Worker is kept alongside for crash detection and terminate() (the Transport
  // abstraction has no worker error event or termination primitive).
  if (source instanceof Worker) {
    const rawWorker = source;
    return spawnOnTransport<T, C>(
      fromMessagePort(rawWorker as unknown as MessagePort),
      options,
      rawWorker,
    );
  }
  return spawnOnTransport<T, C>(source, options);
}

async function spawnOnTransport<
  T,
  const C extends readonly Codec<unknown>[] = readonly Codec<unknown>[],
>(
  worker: Transport,
  options: SpawnOptions<C> = {},
  rawWorker?: Worker,
): Promise<Remote<T, CodecValueTypes<C>> & ActorHandle> {
  // Constraint: the worker handshake is not buffered — call spawn() right after
  // `new Worker(...)`. If messages arrive before the onmessage handler below is
  // set (e.g. another await in between), the handshake is lost and spawn()
  // waits until the handshake timeout.
  const registry = new PayloadCodecRegistry();
  // User codecs register first (can override a built-in of the same tag); built-ins fill in after.
  for (const codec of options.codecs ?? []) registry.register(codec);
  for (
    const codec of [
      iterableCodec,
      errorCodec,
      abortSignalCodec,
      callbackCodec,
    ]
  ) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  // The RPC machinery is channel-agnostic (core/rpc.ts): the main channel is
  // just another adapter, identical to a worker-to-worker link.
  const rpcProxy = createRpcProxy(registry, {
    send: (request, transfer) =>
      worker.send(
        {
          type: "request",
          id: request.id,
          method: request.method,
          args: request.args,
        } satisfies Frame,
        transfer,
      ),
    isDead: () => dead,
    transport: worker,
  });
  // Assign a stable transport id (embedded in refIds) and register it for
  // acquire routing. The id is sent after the handshake so the worker is
  // definitely ready; FIFO ordering on the channel guarantees it arrives
  // before any user request.
  const workerId = actorRegistry.register(worker);
  let dead = false;
  let resolveHandshake: (() => void) | undefined;
  let rejectHandshake: ((reason: unknown) => void) | undefined;

  // The handshake can succeed or fail: version/codec mismatches reject it on the kill path
  const handshake = new Promise<void>((resolve, reject) => {
    resolveHandshake = resolve;
    rejectHandshake = reject;
  });

  // Transport onMessage delivers { data, ports } wrappers (MessageEvent-like),
  // carrying any transferred MessagePorts (e.g. reference-acquire handshakes).
  worker.onMessage((ev) => {
    const frame = ev.data as Frame;
    if (frame.type === "handshake") {
      if (frame.version !== PROTOCOL_VERSION) {
        kill(
          new Error(
            `Protocol version mismatch: worker speaks ${frame.version}, host expects ${PROTOCOL_VERSION}`,
          ),
        );
        return;
      }
      // Codec registration mismatch: a startup failure instead of silently wrong values
      const local = registry.tags;
      const remote = frame.codecs;
      const missing = remote.filter((t) => !local.includes(t));
      const extra = local.filter((t) => !remote.includes(t));
      if (missing.length > 0 || extra.length > 0) {
        kill(
          new Error(
            `Codec mismatch: worker has extra codecs [${
              missing.join(", ")
            }], ` +
              `host has extra codecs [${extra.join(", ")}]`,
          ),
        );
        return;
      }
      // Tell the worker its id (for refIds); FIFO after the handshake.
      worker.send({ type: "__worker-id", id: workerId } satisfies Frame);
      resolveHandshake?.();
      return;
    }
    if (frame.type === "response") {
      rpcProxy.deliver(frame);
      return;
    }
    if (frame.type === "__acquire-ref") {
      // A worker requests a channel to the owner of a reference; this worker
      // is the requester.
      routeAcquire(frame.refId, worker);
      return;
    }
    // Remaining worker→main control frames (e.g. __holder-dead death notices
    // from the liveness sweep) go to registered codec control handlers.
    dispatchControlFrame(frame as ControlFrame);
  });

  // Worker crash detection: only the raw Worker path has error events.
  // Mux transports surface death through closed channels (wired via onDeath /
  // onClose at the caller, e.g. spawnProcess).
  if (rawWorker) {
    rawWorker.onerror = (ev) =>
      kill(
        new Error(
          `Worker crashed: ${ev.message}`,
          { cause: ev.error },
        ),
      );
    rawWorker.onmessageerror = () =>
      kill(new Error("Worker deserialization error"));
  }

  function kill(reason: unknown): void {
    if (dead) return;
    dead = true;
    registry.failAll();
    rpcProxy.rejectAll(reason);
    // If the handshake hasn't finished yet (version/codec mismatch, worker crash),
    // reject it so spawn() fails instead of hanging.
    rejectHandshake?.(reason);
    // A real Worker is terminated; other transports are closed.
    if (rawWorker) rawWorker.terminate();
    else worker.close();
    // A dead worker can no longer serve references: drop it from acquire routing.
    actorRegistry.unregister(workerId);
    // Notify the owner (e.g. an actor pool) so it can remove/replace the member.
    // Deliberately NOT fired by dispose(): a deliberate shutdown is not a death.
    options.onDeath?.(reason);
  }

  // Always return a rejected promise, never throw synchronously:
  // await and .catch behave identically for callers.
  const invoke = (method: string, args: unknown[]): Promise<unknown> =>
    rpcProxy.call(method, args);

  const dispose = (): Promise<void> => {
    if (dead) return Promise.resolve();
    dead = true;
    registry.failAll();
    try {
      worker.send({ type: "dispose" } satisfies Frame);
    } finally {
      if (rawWorker) rawWorker.terminate();
      else worker.close();
    }
    rpcProxy.rejectAll(new ActorDiedError());
    return Promise.resolve();
  };

  const actor = new Proxy({} as Remote<T> & ActorHandle, {
    get(_target, prop) {
      if (prop === "dispose") return dispose;
      if (prop === Symbol.dispose) return () => void dispose();
      // The proxy must not be detected as a thenable, or await behavior breaks.
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => {
          const p = invoke(prop, args);
          attachLazyIterator(p);
          return p;
        };
      }
      return undefined;
    },
  });

  // Interruption for creation: undefined → default timeout, null → never,
  // AbortSignal → user-controlled. A TimeoutError keeps the diagnostic hint
  // for the most common failure (serveWorker() never called).
  const interrupt = options.signal === undefined
    ? AbortSignal.timeout(HANDSHAKE_TIMEOUT)
    : options.signal;

  const reasonOf = (r: unknown): unknown => {
    if (r instanceof Error) {
      if (r.name === "TimeoutError") {
        return new Error(
          "Worker handshake timed out: the worker never reported ready " +
            "(did it call serveWorker()?)",
        );
      }
      return r;
    }
    return new Error(String(r));
  };

  const onInterruptAbort = (): void => {
    if (interrupt) kill(reasonOf(interrupt.reason));
  };

  if (interrupt?.aborted) {
    kill(reasonOf(interrupt.reason));
  } else {
    interrupt?.addEventListener("abort", onInterruptAbort, { once: true });
  }

  await handshake;
  // The signal only governs creation: drop the listener once resolved (a late
  // TimeoutError must not kill a live actor). removeEventListener must receive
  // the exact registered function.
  interrupt?.removeEventListener("abort", onInterruptAbort);
  return actor;
}

export interface SpawnProcessOptions<
  C extends readonly Codec<unknown>[] = readonly Codec<unknown>[],
> {
  /** Extra codecs, matched before the built-ins (same semantics as SpawnOptions.codecs). */
  codecs?: C;
  /** Creation interruption (same semantics as SpawnOptions.signal). */
  signal?: AbortSignal | null;
  /** Fired when the process actor dies (crash / handshake failure; not dispose). */
  onDeath?: (reason: unknown) => void;
  /**
   * Deno permissions granted to the child process (passed to Deno.Command's
   * `permissions` option). The caller controls exactly what the actor process
   * can access. Omit for an allow-everything (-A) child.
   */
  permissions?: Deno.PermissionOptionsObject;
  /** Extra CLI args passed to the child `deno run` invocation (e.g. unstable flags). */
  denoArgs?: string[];
}

/**
 * Spawn an actor in a separate Deno process. The child runs the module at
 * `entrypoint` which must call serveProcess(rpc) at top level. Communication
 * uses node:child_process fork IPC (serialization 'advanced') — a dedicated
 * out-of-band channel that child stdout/stderr logging cannot pollute.
 *
 * The child's Deno permissions are controlled via `permissions`; the RPC
 * surface type is derived from `typeof module.rpc` on the child module.
 *
 *   // child.ts
 *   import { serveProcess } from ".../worker_runtime.ts";
 *   export const rpc = { add(a: number, b: number) { return a + b; } };
 *   serveProcess(rpc);
 *
 *   // main.ts
 *   import type * as ChildModule from "./child.ts";
 *   const actor = await spawnProcess<typeof ChildModule.rpc>("./child.ts", {
 *     permissions: { read: true },
 *   });
 *   await actor.add(1, 2);
 */
export async function spawnProcess<
  T,
  const C extends readonly Codec<unknown>[] = readonly Codec<unknown>[],
>(
  entrypoint: string,
  options: SpawnProcessOptions<C> = {},
): Promise<Remote<T, CodecValueTypes<C>> & ActorHandle> {
  // Dynamic import keeps node:child_process out of the worker/module graph for
  // paths that never spawn processes.
  const { spawn: cpSpawn } = await import("node:child_process");
  const args = ["run", ...permissionArgs(options.permissions), entrypoint];
  if (options.denoArgs) args.splice(1, 0, ...options.denoArgs);
  // stdio 'ipc' opens the dedicated IPC channel (fd 3 on the child); the
  // channel is v8-message-based (serialization 'advanced').
  const child = cpSpawn(
    Deno.execPath(),
    args,
    {
      stdio: ["ipc", "pipe", "pipe"],
      serialization: "advanced",
    } as never,
  );
  let dead = false;
  let disposed = false;
  // Assigned once spawn() resolves; kill() may run before that (no actor yet).
  const actorRef: { current?: Remote<T, CodecValueTypes<C>> & ActorHandle } =
    {};
  function kill(reason: unknown): void {
    if (dead) return;
    dead = true;
    options.onDeath?.(reason);
    // Tear down the inner actor so in-flight calls reject (not just the child).
    if (actorRef.current) void actorRef.current.dispose();
    try {
      child.kill();
    } catch {
      // already exited
    }
  }
  const transport = fromNodeIpc(
    (message) => {
      try {
        return child.send(message as never);
      } catch {
        return false;
      }
    },
    {
      onClosed: () => {
        // The child's IPC channel closed (process exit / crash). A deliberate
        // dispose already terminated the child — don't report it as a death.
        if (!dead && !disposed) {
          kill(new Error("Actor process exited unexpectedly"));
        }
      },
    },
  );
  child.on("message", (message) => transport.deliver(message));
  child.on("error", (err) => {
    if (!dead && !disposed) kill(err);
  });

  // Build the actor on the IPC transport; onDeath wires process death to kill.
  const actor = await spawn<T, C>(transport, {
    codecs: options.codecs,
    signal: options.signal,
    onDeath: (reason: unknown) => kill(reason),
  });
  actorRef.current = actor;
  const innerDispose = actor.dispose.bind(actor);
  actor.dispose = (): Promise<void> => {
    disposed = true;
    return innerDispose();
  };
  return actor;
}

/** Map a Deno PermissionOptionsObject to `--allow-*` CLI flags for the child. */
function permissionArgs(permissions?: Deno.PermissionOptionsObject): string[] {
  if (!permissions) return ["--allow-all"];
  const out: string[] = [];
  const kinds: (keyof Deno.PermissionOptionsObject)[] = [
    "read",
    "write",
    "net",
    "env",
    "run",
    "sys",
    "ffi",
  ];
  for (const kind of kinds) {
    const value = permissions[kind];
    if (value === true) out.push(`--allow-${kind}`);
    else if (typeof value === "string" || Array.isArray(value)) {
      const list = Array.isArray(value) ? value : [value];
      out.push(`--allow-${kind}=${list.join(",")}`);
    }
    // undefined / false: deny (no flag)
  }
  return out;
}

export interface SpawnNodeOptions {
  /** Creation interruption (same semantics as SpawnOptions.signal). */
  signal?: AbortSignal | null;
  /** Fired when the node process dies (crash / handshake failure; not dispose). */
  onDeath?: (reason: unknown) => void;
  /** Deno permissions granted to the child node process. */
  permissions?: Deno.PermissionOptionsObject;
  /** Extra CLI args for the child `deno run` invocation. */
  denoArgs?: string[];
}

/**
 * Spawn a multi-actor node process (model B): the child module calls
 * serveNode(actors) and announces its actor names; this returns a
 * { [name]: Remote } surface where each actor runs on its own logical
 * channel over the fork IPC connection.
 *
 *   // node.ts
 *   import { serveNode } from ".../worker_runtime.ts";
 *   export const actors = { counter: { inc(n) { return n + 1; } } };
 *   serveNode(actors);
 *
 *   // main.ts
 *   import type * as NodeModule from "./node.ts";
 *   const node = await spawnNode<typeof NodeModule.actors>("./node.ts");
 *   await node.counter.inc(1);
 *   await node.dispose();
 */
export async function spawnNode<
  T extends Record<string, object>,
>(
  entrypoint: string,
  options: SpawnNodeOptions = {},
): Promise<{ [K in keyof T]: Remote<T[K]> } & { dispose(): Promise<void> }> {
  const { spawn: cpSpawn } = await import("node:child_process");
  const args = ["run", ...permissionArgs(options.permissions), entrypoint];
  if (options.denoArgs) args.splice(1, 0, ...options.denoArgs);
  const child = cpSpawn(
    Deno.execPath(),
    args,
    {
      stdio: ["ipc", "pipe", "pipe"],
      serialization: "advanced",
    } as never,
  );

  let dead = false;
  let disposed = false;
  const rpcProxies: RpcProxy[] = [];
  function kill(reason: unknown): void {
    if (dead) return;
    dead = true;
    options.onDeath?.(reason);
    // Reject every actor channel's in-flight calls so callers don't hang.
    for (const proxy of rpcProxies) proxy.rejectAll(reason);
    try {
      child.kill();
    } catch {
      // already exited
    }
  }

  const transport = fromNodeIpc(
    (message) => {
      try {
        return child.send(message as never);
      } catch {
        return false;
      }
    },
    {
      onClosed: () => {
        if (!dead && !disposed) kill(new Error("Node process exited"));
      },
    },
  );
  child.on("message", (message) => transport.deliver(message));
  child.on("error", (err) => !dead && !disposed && kill(err));

  // Handshake: the node announces its actor names. Failures kill the child.
  const actors = await new Promise<string[]>((resolve, reject) => {
    const timeout = setTimeout(() => {
      kill(new Error("Node handshake timed out"));
      reject(new Error("Node handshake timed out"));
    }, 10_000);
    transport.onMessage((ev) => {
      const frame = ev.data as { type?: string; actors?: string[] };
      if (frame.type === "handshake") {
        clearTimeout(timeout);
        resolve(frame.actors ?? []);
      }
    });
    child.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });
    child.on("exit", () => {
      clearTimeout(timeout);
      reject(new Error("Node exited before handshake"));
    });
  });

  // Open one channel per actor; each runs standard RPC frames over it.
  const registry = new PayloadCodecRegistry();
  for (
    const codec of [iterableCodec, errorCodec, abortSignalCodec, callbackCodec]
  ) {
    if (!registry.has(codec.tag)) registry.register(codec);
  }
  const surface = {} as Record<string, unknown>;
  for (const name of actors) {
    const opened = transport.openChannel();
    // Tell the node which actor this channel serves: the open token rides as
    // a Mux control frame (the node's Mux opens the channel + acks), and the
    // __open-actor frame names it. Both go on the main channel in order.
    transport.send(opened.token);
    transport.send({
      type: "__open-actor",
      name,
      token: opened.token,
    } as unknown as Frame);
    const channel = opened.channel;
    const proxy = createRpcProxy(registry, {
      send: (request, transfer) =>
        channel.send(
          {
            type: "request",
            id: request.id,
            method: request.method,
            args: request.args,
          } satisfies Frame,
          transfer,
        ),
      isDead: () => channel.closed,
      deadReason: () => new Error(`Actor "${name}" channel closed`),
      transport,
    });
    rpcProxies.push(proxy);
    channel.onMessage((message) => {
      const frame = message as Frame;
      if (frame.type === "response") proxy.deliver(frame);
    });
    surface[name] = new Proxy({} as object, {
      get(_t, prop) {
        if (prop === "then") return undefined;
        if (typeof prop === "string") {
          return (...args: unknown[]) => {
            const p = proxy.call(prop, args);
            attachLazyIterator(p);
            return p;
          };
        }
        return undefined;
      },
    });
  }

  const dispose = (): Promise<void> => {
    if (dead) return Promise.resolve();
    disposed = true;
    dead = true;
    try {
      transport.send({ type: "dispose" } satisfies Frame);
    } finally {
      transport.close();
      // Deliberate shutdown: not a death — reject in-flight, don't fire onDeath.
      for (const proxy of rpcProxies) proxy.rejectAll(new ActorDiedError());
      try {
        child.kill();
      } catch {
        // already exited
      }
    }
    return Promise.resolve();
  };

  return Object.assign(surface, { dispose }) as never;
}
