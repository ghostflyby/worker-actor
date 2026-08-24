/**
 * Actor-side runtime: registers the module's top-level rpc object as an Actor,
 * handles request frames and replies with responses. Two entry points share
 * one RPC machinery:
 *
 *   - serveWorker(api, options): the Web Worker runtime. The main channel is
 *     the worker's self.postMessage; direct worker-to-worker links (link()
 *     from the main thread) get the same machinery over a transferred port.
 *   - serveProcess(api, options): the multi-process runtime. The main channel
 *     is node:child_process fork IPC (serialization 'advanced' — each message
 *     is a v8 value), giving a dedicated out-of-band channel that child
 *     logging on stdout/stderr cannot pollute.
 *
 * Usage (call once at the module's top level):
 *   import { serveWorker } from "./worker_runtime.ts";
 *   export const rpc = { async add(a: number, b: number) { return a + b; } };
 *   serveWorker(rpc);
 *
 * Conventions:
 *   - The module has exactly one RPC entry object (name is free, but the main
 *     thread's `typeof WorkerModule.rpc` must refer to it).
 *   - Methods and return values must be structured-cloneable (plain data +
 *     Map/Set/Date/TypedArray, ...); functions/class instances/prototype chains
 *     can't cross threads — Remote<T> enforces at compile time that only
 *     functions are exposed.
 *   - Exceptions inside the worker are serialized back and rebuilt as RemoteError.
 */

import { type Frame, PROTOCOL_VERSION } from "./core/protocol.ts";
import { type Codec, PayloadCodecRegistry } from "./core/codec.ts";
import { type Channel, connectChannel, connectToken } from "./core/channel.ts";
import {
  fromMessagePort,
  fromNodeIpc,
  type Transport,
} from "./core/transport.ts";
import {
  dispatchControlFrame,
  setActiveRegistry,
  setActiveTransport,
  setWorkerId,
} from "./core/worker-context.ts";
import {
  createRpcProxy,
  makeRpcHandler,
  type PeerRpc,
  type RpcResponse,
} from "./core/rpc.ts";
import { iterableCodec } from "./core/codecs/iterable.ts";
import { errorCodec } from "./core/codecs/error.ts";
import { abortSignalCodec } from "./core/codecs/abort_signal.ts";
import { callbackCodec } from "./core/codecs/callback.ts";

// Deno types `self` as Window by default; in a worker script it is actually a
// DedicatedWorkerGlobalScope. Only the members used are declared here, so we
// don't depend on lib.webworker.
declare const self: {
  postMessage(message: unknown, options?: { transfer?: Transferable[] }): void;
  onmessage: ((ev: MessageEvent<Frame>) => void) | null;
  close(): void;
};

export interface WorkerApi {
  // The RPC boundary accepts any signature; concrete worker method types are
  // derived and checked by Remote<T> on the main thread.
  // deno-lint-ignore no-explicit-any
  [method: string]: (...args: any[]) => any;
}

/** A direct, bidirectional link between this worker and a peer worker. */
export interface LinkHandle {
  /** Label of the link (shared by both endpoints). */
  label: string;
  /**
   * Send any codec value to the peer: references, streams, AbortSignals and
   * plain structured-cloneable values all work. Encoding runs through this
   * worker's registry, so the peer must register a compatible codec set.
   */
  send(value: unknown): void;
  /** Register the handler for values arriving from the peer (last handler wins). */
  onValue(handler: (value: unknown) => void): void;
  /**
   * Declare the RPC surface the peer can call over this link. Defaults to the
   * main-thread api; call serve() to expose a narrower peer-facing surface
   * (the main-thread api usually contains management methods the peer
   * shouldn't see). Calling serve() again replaces the surface.
   */
  serve(api: WorkerApi): void;
  /**
   * Proxy for calling the peer's RPC surface. The peer type cannot be derived
   * across modules, so cast to your contract: `link.rpc as PeerRpc<Contract>`.
   */
  rpc: PeerRpc<object>;
  /** Close this endpoint of the link; in-flight RPC calls reject. */
  close(): void;
}

/** Frame carried on a link channel (not on the main RPC channel). */
type LinkFrame =
  | { type: "__link-value"; value: unknown }
  | { type: "call"; id: number; method: string; args: unknown[] }
  | { type: "result"; id: number; ok: true; value: unknown }
  | {
    type: "result";
    id: number;
    ok: false;
    error: { name: string; message: string; stack?: string };
  };

export interface ServeWorkerOptions {
  /**
   * Extra codecs to register, matched before the built-ins.
   * Built-ins: iterable / error / abort-signal / callback. The tag list must
   * match spawn()'s; the handshake frame carries and validates it.
   */
  codecs?: Codec<unknown>[];
  /**
   * Called when the main thread links this worker to a peer via link().
   * The handle exposes value send/onValue and peer RPC (serve/rpc) over a
   * direct channel that bypasses the main thread; the peer must register a
   * compatible codec set for the values exchanged. Link channels are closed
   * by failAll when the actor dies.
   */
  onLink?: (link: LinkHandle) => void;
}

/**
 * Shared RPC runtime over a Transport main channel. Handles request
 * frames (decode args → await → encode result), reference-acquire control
 * frames, the worker-id handshake and dispose. `linkHandlers` lets the
 * Worker-specific link mechanism register itself (process runtimes have no
 * MessagePort transfer and skip links).
 */
function createRuntime(
  api: WorkerApi,
  options: ServeWorkerOptions,
  transport: Transport,
  close: () => void,
): void {
  const registry = new PayloadCodecRegistry();
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
  const links = new Map<string, Channel>();
  setActiveRegistry(registry);
  // Acquire control frames must ride this runtime's own transport (a process
  // actor has no self.postMessage for the __acquire-ref request).
  setActiveTransport(transport);
  const mainHandler = makeRpcHandler(api, registry, transport);

  transport.onMessage(async (ev) => {
    const frame = ev.data as Frame;
    if (frame.type === "request") {
      const res = await mainHandler(frame);
      if (res.ok) {
        transport.send(
          { type: "response", id: res.id, ok: true, value: res.value },
          res.transfer,
        );
      } else {
        transport.send(
          { type: "response", id: res.id, ok: false, error: res.error },
        );
      }
      return;
    }
    if (frame.type === "__link") {
      // Direct link to a peer worker, bypassing the main thread. Values and
      // RPC calls on it are encoded/decoded through this worker's registry.
      const channel = connectChannel(frame.port);
      registry.registerChannel(channel); // failAll closes every open link too
      links.set(frame.label, channel);
      let valueHandler: ((value: unknown) => void) | undefined;
      // Peer-callable surface: defaults to the main-thread api; serve() overrides.
      let linkApi: WorkerApi = api;
      let linkHandler = makeRpcHandler(linkApi, registry);
      // Calling side toward the peer (bidirectional RPC).
      const proxy = createRpcProxy(registry, {
        send: (request, transfer) =>
          channel.send(
            {
              type: "call",
              id: request.id,
              method: request.method,
              args: request.args,
            } satisfies LinkFrame,
            transfer,
          ),
        isDead: () => channel.closed,
        deadReason: () => new Error("Link closed"),
      });
      const peerRpc = new Proxy({} as PeerRpc<object>, {
        get(_target, prop) {
          if (prop === "then") return undefined;
          if (typeof prop === "string") {
            return (...args: unknown[]) => proxy.call(prop, args);
          }
          return undefined;
        },
      });
      channel.onMessage((message) => {
        const linkFrame = message as LinkFrame;
        if (linkFrame.type === "__link-value") {
          valueHandler?.(registry.decode(linkFrame.value));
        } else if (linkFrame.type === "call") {
          void linkHandler(linkFrame).then((res) => {
            if (res.ok) {
              channel.send(
                {
                  type: "result",
                  id: res.id,
                  ok: true,
                  value: res.value,
                } satisfies LinkFrame,
                res.transfer,
              );
            } else {
              channel.send(
                {
                  type: "result",
                  id: res.id,
                  ok: false,
                  error: res.error,
                } satisfies LinkFrame,
              );
            }
          });
        } else if (linkFrame.type === "result") {
          proxy.deliver(linkFrame as RpcResponse);
        }
      });
      options.onLink?.({
        label: frame.label,
        send(value: unknown): void {
          const transfer: Transferable[] = [];
          channel.send(
            {
              type: "__link-value",
              value: registry.encode(value, transfer),
            } satisfies LinkFrame,
            transfer,
          );
        },
        onValue(handler: (value: unknown) => void): void {
          valueHandler = handler;
        },
        serve(newApi: WorkerApi): void {
          linkApi = newApi;
          linkHandler = makeRpcHandler(linkApi, registry);
        },
        rpc: peerRpc,
        close(): void {
          proxy.rejectAll(new Error("Link closed"));
          channel.close();
          links.delete(frame.label);
        },
      });
      return;
    }
    if (frame.type === "__link-close") {
      links.get(frame.label)?.close();
      links.delete(frame.label);
      return;
    }
    if (frame.type === "__worker-id") {
      setWorkerId(frame.id);
      dispatchControlFrame({ type: "__worker-id", refId: frame.id });
      return;
    }
    if (frame.type === "__serve-ref" || frame.type === "__ref-acquired") {
      dispatchControlFrame(frame);
      return;
    }
    if (frame.type === "dispose") {
      registry.failAll();
      close();
    }
  });

  // Module loaded, API ready — spawn() wakes up from pending on this frame.
  transport.send({
    type: "handshake",
    version: PROTOCOL_VERSION,
    codecs: registry.tags,
    kind: transport.kind,
  });
}

/** Worker runtime: main channel = self.postMessage, links over transferred ports. */
export function serveWorker(
  api: WorkerApi,
  options: ServeWorkerOptions = {},
): void {
  // self is a DedicatedWorkerGlobalScope: wrap it as a MessagePort-shaped
  // source so fromMessagePort yields a messageport-type Transport — the same
  // abstraction every actor channel runs on (openChannel hands transferred
  // ports, exactly like the main-thread Worker path).
  const port = {
    onmessage: null as ((ev: MessageEvent<unknown>) => void) | null,
    postMessage(
      message: unknown,
      opts?: { transfer?: Transferable[] },
    ): void {
      self.postMessage(message, opts);
    },
    close(): void {
      self.close();
    },
  } as unknown as MessagePort;
  const transport = fromMessagePort(port);
  // Bridge the worker's inbound messages into the fake port's onmessage
  // (fromMessagePort reads the port's onmessage).
  self.onmessage = (ev) => port.onmessage?.(ev);
  createRuntime(api, options, transport, () => self.close());
}

/**
 * Multi-process runtime: main channel = node:child_process fork IPC
 * (serialization 'advanced'). Each IPC message is a v8 value; the Mux protocol
 * rides directly on the messages (message-kind Transport). The IPC channel is
 * out-of-band from stdout/stderr, so child logging cannot pollute the protocol.
 *
 * Usage in the child process module (called once at top level):
 *   import { serveProcess } from "./worker_runtime.ts";
 *   export const rpc = { ... };
 *   serveProcess(rpc);
 */
export function serveProcess(
  api: WorkerApi,
  options: Omit<ServeWorkerOptions, "onLink"> = {},
): void {
  // node:child_process environment: process.send / process.on("message").
  const proc = (globalThis as { process?: unknown }).process as {
    send(message: unknown): void;
    on(event: "message", handler: (message: unknown) => void): void;
  } | undefined;
  if (!proc || typeof proc.send !== "function") {
    throw new Error(
      "serveProcess() must run inside a node:child_process (Deno.Command spawn with ipc)",
    );
  }
  const transport = fromNodeIpc((message) => {
    proc.send(message);
    return true;
  });
  // Wire the IPC channel into the message-kind transport: inbound IPC messages
  // feed the Mux deliver.
  proc.on("message", (message) => transport.deliver(message));
  createRuntime(
    api,
    options,
    transport,
    () => transport.close(),
  );
}

/**
 * Multi-actor node runtime (model B): one process/connection serves several
 * named actors. The main channel only performs the handshake (announcing the
 * actor names) and opens actor channels; each actor's RPC runs on its own
 * logical channel (a Mux sub-channel on framed/message transports, or a
 * MessagePort on messageport transports). Actor = channel.
 *
 * Usage in the node module (called once at top level):
 *   import { serveNode } from "./worker_runtime.ts";
 *   export const actors = { counter: { inc(n) { ... } }, logger: { ... } };
 *   serveNode(actors);
 *
 * The peer (spawnNodeProcess / connectNode) opens one channel per actor and
 * gets a { [name]: Remote } surface.
 */
export function serveNode(
  rpcs: Record<string, WorkerApi>,
  options: Omit<ServeWorkerOptions, "onLink"> = {},
): void {
  // Build a registry with the same codec setup as the single-actor runtimes.
  const registry = new PayloadCodecRegistry();
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
  setActiveRegistry(registry);

  // Resolve the transport from the environment: node:child_process fork IPC if
  // present, else self (Web Worker).
  const proc = (globalThis as { process?: unknown }).process as {
    send(message: unknown): void;
    on(event: "message", handler: (message: unknown) => void): void;
  } | undefined;
  const transport: Transport = proc && typeof proc.send === "function"
    ? (() => {
      const t = fromNodeIpc((message) => {
        proc.send(message);
        return true;
      });
      proc.on("message", (message) => t.deliver(message));
      return t;
    })()
    : fromMessagePort(self as unknown as MessagePort);

  // Node actors also post acquire frames on the node's own transport (a
  // node process has no self.postMessage for the __acquire-ref request).
  setActiveTransport(transport);

  const names = Object.keys(rpcs);
  const actorChannels = new Set<Channel>();

  const serveChannel = (name: string, channel: Channel): void => {
    if (!rpcs[name]) {
      channel.close();
      return;
    }
    registry.registerChannel(channel);
    actorChannels.add(channel);
    const handler = makeRpcHandler(rpcs[name], registry, transport);
    channel.onMessage((message) => {
      const frame = message as Frame;
      if (frame.type === "request") {
        void handler(frame).then((res) => {
          if (res.ok) {
            channel.send(
              { type: "response", id: res.id, ok: true, value: res.value },
              res.transfer,
            );
          } else {
            channel.send(
              { type: "response", id: res.id, ok: false, error: res.error },
            );
          }
        });
      } else if (frame.type === "dispose") {
        channel.close();
        actorChannels.delete(channel);
      }
    });
  };

  transport.onMessage((ev) => {
    const frame = ev.data as { type: string };
    if (frame.type === "handshake") {
      // Peer-initiated? No — the node announces itself. Handled below.
      return;
    }
    if (frame.type === "__open-actor") {
      const f = frame as unknown as {
        type: "__open-actor";
        name: string;
        port?: MessagePort;
        token?: unknown;
      };
      const channel = f.port !== undefined
        ? connectChannel(f.port)
        : connectToken(transport, f.token as { __mux: "open"; ch: number });
      serveChannel(f.name, channel);
      return;
    }
    if (frame.type === "dispose") {
      registry.failAll();
      for (const ch of actorChannels) ch.close();
      actorChannels.clear();
      transport.close();
    }
  });

  // Announce ready + the actor names (handshake). The peer opens actor
  // channels after this.
  transport.send({
    type: "handshake",
    version: PROTOCOL_VERSION,
    codecs: registry.tags,
    kind: transport.kind,
    actors: names,
  } as never);
}
