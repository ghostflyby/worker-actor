# Ownership and reference model

The precise terminology and relationships for who owns what, and how release
works. This is the authority on ownership; DESIGN.md covers mechanics.

## Terminology

- **actor** — a JS execution context (a Web Worker) that hosts byref objects and
  processes messages. The unit of existence, isolation and failure.
- **creator** — the JS execution context that created an actor (the main thread,
  usually). Owns the actor's existence.
- **consumer** — a JS execution context holding a `ref` to an actor's byref
  object. Has usage rights only — no ownership.
- **object** — a plain JS object that can exist independently of this library
  (no library involvement needed to create or use it).
- **ref** — a reference to an actor-owned byref object; the generic term
  (remote-ref reference or callback reference). **Strong by default**: holding a
  ref prevents the actor from auto-releasing the object (the conditional strong
  hold). **weakref** — a weak variant that does not prevent auto-release.
- **byref** / **sub-actor** — an object an actor exposes across contexts via
  refs. Lives and dies inside its owning actor.
- **owner** — the entity that may release an entity _ignoring refs_: an actor
  releases its byref via `releaseRef` without any ref's consent; the creator
  releases the actor via `dispose`, invalidating all its byrefs at once.

## Ownership nesting

```
creator ──owns──▶ actor        release: dispose()      (ignores every ref/byref)
actor   ──owns──▶ byref        release: releaseRef()   (ignores every ref)
GC      ──owns──▶ plain object  release: no references → collected
consumer ──holds──▶ ref         release: drop/dispose   (participates in the conditional strong hold)
```

- **creator owns the actor's existence** (thread lifetime, resources): only the
  creator may terminate it. An actor that wants to shut down **requests** the
  creator (`requestShutdown`) — existence is one-way, decided by the creator.
- **actor owns its byrefs** (sub-actors): the actor decides when an object it
  exposed dies, via `releaseRef`. A creator/consumer cannot release a byref
  directly — they can only ask the actor (an rpc method), never take ownership.
- **GC owns plain objects**: the only "release" is collection when no strong
  reference path exists.

## Why every owner keeps a strong reference

An owner relationship requires the owner to be able to reach what it owns — that
is what makes release possible. Release is literally _removing the owner's
strong reference_ (plus notifying peers). Therefore:

> An object is garbage-collected only when no strong reference path exists, i.e.
> when its owner no longer holds it. **GC of an owned object is a RESULT of the
> owner relationship having already become inactive — not an independent event
> that requires a response.**

## Responding to object GC

- **ref (conditional strong)**: while any holder channel is open, the owner
  holds the object strongly. The object cannot be GC'd while the relationship is
  active. When the object is GC'd, the relationship is already inactive (the
  owner released it and broadcast `released`, or all holders released and the
  last channel closed). **No extra response needed** — GC is the tail of
  release, not a trigger.
- **callback (current)**: the function is held only via WeakRef, so it may be
  GC'd _while the relationship is still active_. This is the one case where GC
  can occur mid-relationship and must be responded to (broadcast `released` so
  peers fail immediately). This is an asymmetry with refs — the design fix is to
  align callbacks with the conditional-strong model (see below).
- **after alignment, the uniform principle holds**: any byref entity is GC'd
  only after its owner relationship is already inactive; GC never requires a
  response; responses are driven by _relationship state changes_ (`releaseRef`
  broadcast, channel close), not by collection.

## Owner-side cleanup guarantees (bounded, not infinite)

The conditional-strong model releases only on _notification_ (finalizer,
explicit release, channel close) — and notifications are best-effort. To keep an
accidental leak bounded:

- **Tombstone finalizer**: the owner registers every exposed object in a
  FinalizationRegistry; whatever caused its collection, the owner sweeps its
  bookkeeping and broadcasts `released` (so no peer keeps a dead ref). This is a
  safety net — under the conditional-strong model the object is not collectable
  while the relationship is active, so the tombstone only fires after release
  already happened or a hold failed.
- **Holder keepalive**: each holder heartbeats its channels; the owner times out
  channels with no heartbeat and releases them. This deterministically covers a
  holder worker dying (its heartbeat stops) — the most invisible leak path
  (MessagePort has no close event).
- **Known limitation**: a consumer dropping a ref whose finalizer never runs
  (FinalizationRegistry is best-effort) leaves the holder channel open and the
  object retained until the owner-side timeout sweep (keepalive) or actor death.
  The timeout bounds this; exact reclamation requires explicit `ref.dispose()`
  (deterministic) or reference counting (not provided).

## Release paths

| owner    | entity       | release            | effect on peers                                              |
| -------- | ------------ | ------------------ | ------------------------------------------------------------ |
| creator  | actor        | `dispose()`        | all byrefs invalidated (channels close, proxies die)         |
| actor    | byref object | `releaseRef(obj)`  | `released` broadcast → every holder's proxy dies immediately |
| consumer | ref          | drop / `dispose()` | conditional strong weakens; last holder → object collectable |

The fallback `deref`-undefined error on call stays as a safety net for races
(broadcast lost / timing), while the broadcast is the immediate path.
