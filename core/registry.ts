/**
 * Actor registry: the bootstrap/discovery table that resolves "who is where".
 *
 * Each actor channel (a spawned worker, a process actor, a node) gets a stable
 * transport id, embedded in refIds as a prefix. The registry maps that id to
 * the Transport that owns it, so a reference-acquire (or a cross-connection
 * connect) can route "refId → owner transport" and bootstrap a channel on
 * demand. This is the minimal, pluggable form of the distributed discovery
 * layer: the main thread / coordinator is the registry holder, and remote
 * transports register themselves.
 *
 * The registry is deliberately small and synchronous (single-threaded
 * coordinator). Distributed membership/failure detection is out of scope here.
 */

import type { Transport } from "./transport.ts";

export interface ActorRegistry {
  /** Assign a stable id to a transport and register it; returns the id. */
  register(transport: Transport): string;
  /** Resolve a transport id to its transport; undefined if unknown/dead. */
  resolve(id: string): Transport | undefined;
  /** Reverse lookup: the id registered for a transport (undefined if unknown). */
  idOf(transport: Transport): string | undefined;
  /** Unregister a transport by id (on death/dispose). */
  unregister(id: string): void;
  /** Callback when a transport is unregistered (for owner-death cleanup). */
  onUnregister(handler: (id: string, transport: Transport) => void): void;
}

export function createActorRegistry(): ActorRegistry {
  const byId = new Map<string, Transport>();
  const idByTransport = new WeakMap<Transport, string>();
  let nextId = 1;
  let unregisterHandler:
    | ((id: string, transport: Transport) => void)
    | undefined;

  return {
    register(transport): string {
      const existing = idByTransport.get(transport);
      if (existing !== undefined) return existing;
      const id = `t${nextId++}`;
      byId.set(id, transport);
      idByTransport.set(transport, id);
      return id;
    },
    resolve(id) {
      return byId.get(id);
    },
    idOf(transport) {
      return idByTransport.get(transport);
    },
    unregister(id) {
      const transport = byId.get(id);
      if (!transport) return;
      byId.delete(id);
      idByTransport.delete(transport);
      unregisterHandler?.(id, transport);
    },
    onUnregister(handler) {
      unregisterHandler = handler;
    },
  };
}
