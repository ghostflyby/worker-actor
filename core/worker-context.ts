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

// Deno types `self` as Window; in a worker it is a DedicatedWorkerGlobalScope.
// Only postMessage is used here.
declare const self: { postMessage(message: unknown): void };

let workerId: string | undefined;
let activeRegistry: PayloadCodecRegistry | undefined;
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

export interface ControlFrame {
  type: string;
  refId: string;
  port?: MessagePort;
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
 * Trigger an acquire for a refId. The side is decided by workerId: a worker
 * (id set) posts the request to the main thread; the main thread (no id) runs
 * the router directly. mainAcquire is registered by spawn in every module
 * instance, so it must never be the side decision — only the main thread may
 * use it.
 */
export function triggerAcquire(refId: string): void {
  if (workerId !== undefined) {
    self.postMessage({ type: "__acquire-ref", refId });
    return;
  }
  if (mainAcquire) mainAcquire(refId);
}
