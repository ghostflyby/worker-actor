/**
 * Worker-side global context and the reference-acquire control plane.
 *
 * The main thread assigns each spawned worker a stable id (__worker-id frame);
 * refIds embed that id, so the main thread can resolve "refId → owner worker"
 * to route acquire requests. A module-level control-frame registry lets codecs
 * (the remote-ref codec) register handlers for __serve-ref / __ref-acquired,
 * dispatched by the worker runtime or the main thread.
 *
 * activeRegistry: each worker runs exactly one serveWorker, so a module-level
 * "current registry" is race-free and lets codec control handlers materialize
 * values (decode args / create proxies) without threading the registry through
 * every frame.
 */

import type { PayloadCodecRegistry } from "./codec.ts";
import type { Channel } from "./channel.ts";
import type { Transport } from "./transport.ts";

// Deno types `self` as Window; in a worker it is a DedicatedWorkerGlobalScope.
// Only postMessage is used here.
declare const self: { postMessage(message: unknown): void };

let workerId: string | undefined;
let activeRegistry: PayloadCodecRegistry | undefined;
// The transport this runtime posts acquire control frames on. Set by
// createRuntime / serveNode; a process/remote actor has no self.postMessage,
// so the acquire frame must ride its own Transport main channel instead.
let activeTransport: Transport | undefined;
let mainAcquire: ((refId: string) => void) | undefined;

export function setWorkerId(id: string): void {
  workerId = id;
}

/** The main-thread-assigned id of this worker (undefined on the main thread). */
export function getWorkerId(): string | undefined {
  return workerId;
}

export function setActiveRegistry(registry: PayloadCodecRegistry): void {
  activeRegistry = registry;
}

/** The registry of this worker's single serveWorker (main thread: undefined). */
export function getActiveRegistry(): PayloadCodecRegistry | undefined {
  return activeRegistry;
}

/** The transport this runtime posts control frames on (set by the runtimes). */
export function setActiveTransport(transport: Transport): void {
  activeTransport = transport;
}

/** The current transport (an actor runtime's main channel; main thread: undefined). */
export function getActiveTransport(): Transport | undefined {
  return activeTransport;
}

export interface ControlFrame {
  type: string;
  refId: string;
  port?: MessagePort;
  /** Mux channel-establishment token (instead of a transferred port, on Mux transports). */
  token?: unknown;
  /**
   * A pre-built Channel — main-side local materialization only (the main thread
   * opens the Mux channel and owns its local end; a raw Channel cannot ride the
   * wire, so this field is never present on a transported frame).
   */
  channel?: Channel;
  /** Holder/owner worker ids and the liveness pair port (acquire control frames). */
  holderId?: string;
  ownerId?: string;
  livenessPort?: MessagePort;
}

type ControlHandler = (frame: ControlFrame) => void;

const controlHandlers = new Map<string, Set<ControlHandler>>();

/** Register a handler for a control frame type (e.g. the ref codec's acquire hooks). */
export function registerControlHandler(type: string, fn: ControlHandler): void {
  let set = controlHandlers.get(type);
  if (!set) {
    set = new Set();
    controlHandlers.set(type, set);
  }
  set.add(fn);
}

/** Remove a previously registered control handler (test/teardown use). */
export function unregisterControlHandler(
  type: string,
  fn: ControlHandler,
): void {
  controlHandlers.get(type)?.delete(fn);
}

/** Dispatch a control frame to its registered handlers (worker runtime / main). */
export function dispatchControlFrame(frame: ControlFrame): void {
  const set = controlHandlers.get(frame.type);
  if (!set) return;
  for (const fn of set) fn(frame);
}

/**
 * Main-thread acquire router: spawn registers it so a main-side pending ref
 * can bootstrap the owner↔requester channel directly (no postMessage round
 * trip — the main thread IS the router).
 */
export function setMainAcquire(fn: (refId: string) => void): void {
  mainAcquire = fn;
}

/**
 * Trigger an acquire for a refId. The side is decided by the active runtime:
 * a process/remote actor (createRuntime set a Transport) posts the request on
 * its own main channel — there is no self.postMessage off the worker thread;
 * a Web Worker (id set) posts it to the main thread via self; the main thread
 * (no id, no transport) runs the router directly. mainAcquire is registered by
 * spawn in every module instance, so it must never be the side decision — only
 * the main thread may use it.
 */
export function triggerAcquire(refId: string): void {
  if (activeTransport !== undefined) {
    activeTransport.send({ type: "__acquire-ref", refId });
    return;
  }
  if (workerId !== undefined) {
    self.postMessage({ type: "__acquire-ref", refId });
    return;
  }
  if (mainAcquire) mainAcquire(refId);
}
