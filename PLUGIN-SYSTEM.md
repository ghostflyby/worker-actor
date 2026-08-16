# Plugin system orchestration

A design document for the library's primary target scenario: a dynamic plugin
system. This is a usage pattern — a convention layer built on the library's
existing primitives — not a specification of new library API. Where a feature
cannot be expressed with the current library, that is called out explicitly (see
§9).

## 1. Goal and scope

One module script, two contexts, one instance:

- In the **creator** context (the main thread), the plugin module is available
  for `ensure()` — creating a worker-backed actor for it (via the module itself
  under §3.1, or via the manifests registry under §3.2).
- In any **extension** context (another plugin's worker), importing the same
  module yields — directly, at import time — a reference to the actor instance
  the creator created. No manual wiring, no id registry lookups.

The orchestration layer additionally guarantees _no incomplete states_:

- **Dependency graph** drives everything: disabling a plugin cooperatively tears
  down, in reverse topological order, every component (worker) that depends on
  it — leaf first, injected cancellation signal, then whole-worker reclamation.
  Recovery starts actors and reconnects them in topological order.
- **Hot reload** is a full cancel-and-rebuild (no intermediate states), with a
  more expensive parallel-swap variant for zero-downtime reloads.

## 2. Terminology

- **plugin module** — a module whose single script yields two contents by
  context: the real implementation in its host worker, a lazy reference in
  consumers. Two mechanisms deliver this (§3).
- **actor** — the worker-backed instance of a plugin (1 worker : 1 actor).
- **orchestrator** — the application-level component on the main thread that
  owns the registry, the dependency graph and the lifecycle state machine. It is
  convention today; its core could become library API later.
- **ensure** — the idempotent operation "make sure this actor exists and is
  running, creating the worker if needed".
- **disable** — the idempotent operation "take this actor and everything that
  transitively depends on it out of service" (cascade teardown).
- **swap** — the parallel hot-reload variant: a new instance starts next to the
  old one, the registry switches over atomically, then the old one is torn down
  (see §8.2).

## 3. Module view: same specifier, two contents

JS has no shared module instance across workers — each worker owns its module
graph. The plugin system needs "the same module script" to yield the real
implementation in the host worker and a lazy reference in consumers. Two
mechanisms achieve this; they differ only at the module-view layer. Everything
below — §4 orchestration, §5 state machine, §6 cascades, §7 recovery, §8 reload
— is mechanism-independent: the acquire protocol, registry, state machine and
cascade are identical under either view.

### 3.1 Runtime branch (defineActor)

The module itself carries both views. The convention makes the _shape_ of the
module do the work:

```ts
// plugins/foo.ts — the same file, two views depending on the context
import { defineActor } from "…";

export const actor = defineActor({
  id: "foo",
  deps: ["bar", "baz"],          // static dependency declaration
  publicSurface: { doWork, query }, // what consumers may call (use rights only)
  create: () => ({ …api }),      // creator view only
});
```

- **Import has no side effects.** The module only declares. This is the
  foundation of the whole convention: a cyclic module graph cannot deadlock,
  because nothing runs at import time. It also makes "importing the module
  yields the actor reference" safe — the reference is lazy.
- **Creator view** (`getWorkerId() === undefined`): `actor` is a service handle.
  `ensure()` spawns the worker on first use and publishes the actor in the
  registry; `disable()` is creator-only.
- **Worker view**: `defineActor` detects it is inside a worker and degrades to a
  lazy reference proxy — the first method call emits an acquire-actor control
  frame, and the reference resolves against the creator's registry. This is the
  "import directly yields the instance" behavior: importing gets the reference,
  first use gets the connection.

### 3.2 Load-hook replacement (shim entry + registerHooks)

**The mechanism.** Deno implements Node's `registerHooks()` (from
`node:module`), which lets a context intercept module resolution and loading for
its own module graph. Loader hooks do **not** propagate between contexts
(verified, see below), so every worker that imports plugins must install its own
hooks via a **shim entry**:

```
consumer worker entry (shim)                host worker entry (plain)
  registerHooks(redirect table)               import the plugin for real → serve surface
  await import(<consumer tree>)  ──hooks──▶   stub (lazy reference)
```

- The shim registers hooks first, then dynamically imports the consumer tree.
  Every static import of a plugin specifier inside that tree is intercepted and
  rewritten to a stub module (verified). The shim itself imports nothing but the
  hook machinery and a per-project **manifests** registry module, so no plugin
  resolution can happen before the hooks are live.
- The stub yields a lazy reference: the first method call emits
  `__acquire-actor`; main bootstraps the per-acquire channel (§4). Importing
  gets the reference, first use gets the connection — the same contract as §3.1.
- The host worker has no hooks: it imports the real module and serves its
  surface over the acquired channels. The creator never imports plugin modules —
  it operates the manifests registry (`ensure("foo")` spawns the host worker).

**Verified behavior (Deno 2.9.5, `deno run -A`):**

| Probe | Question                                                                 | Result                                                                                                                              |
| ----- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| A     | does `node:module.register()` (Node's async API) work?                   | no — Deno does not implement it; it is a no-op shell. Use `registerHooks()`                                                         |
| B1    | `registerHooks` intercepts `file://` imports on the main thread          | yes — plugin specifier → stub                                                                                                       |
| B2    | main-registered hooks propagate into a spawned worker                    | no — the worker resolves the real module                                                                                            |
| B3    | a worker can register hooks for itself                                   | yes — but a top-level static import evaluates before registration (real); a dynamic import after registration is intercepted (stub) |
| B4    | `--import` preload hooks propagate into a worker                         | no                                                                                                                                  |
| B5    | shim-entry pattern (register, then dynamically import the consumer tree) | yes — every static import inside the tree is intercepted                                                                            |

**Hard constraints** (consequences of the verified matrix):

1. **Hooks are per-context.** They neither inherit from the parent worker nor
   come from `--import`. Every worker that imports plugins needs a shim entry
   installing its own hooks; the redirect table comes from the manifests
   registry the shim imports, not from the entry URL.
2. **Plugins must sit below the dynamic-import boundary.** The shim is the only
   module that runs before hooks are live, so it must not statically import any
   plugin. The consumer tree is loaded dynamically after registration (B5);
   static imports inside that tree resolve during the dynamic load and are
   intercepted.
3. **Stub content.** A stub may be a real file (verified) or synthesized source
   returned by a `load` hook (virtual module). Synthesized source is **not
   verified**: the docs require `jsr:`/`npm:`/`https:` specifiers to be declared
   up front (Deno statically analyzes the graph), and a synthetic stub importing
   a runtime helper (the reference factory) needs a probe before adoption. File
   stubs have no such concern.

**Trade-off vs §3.1:**

|                  | §3.1 runtime branch               | §3.2 load-hook                                 |
| ---------------- | --------------------------------- | ---------------------------------------------- |
| plugin module    | must wrap itself in `defineActor` | pure logic, zero library awareness             |
| context dispatch | in-module `getWorkerId()` branch  | load-time rewrite, invisible to the module     |
| deployment       | plain worker entries              | every consumer worker entry must be a shim     |
| risk             | none (existing library APIs only) | experimental API + synthetic-source blind spot |

**Recommendation:** validate the orchestration layer with §3.1 first (Phase 1 —
the state machine, cascade and recovery are the real work; the module view is
swappable). Adopt §3.2 when plugin modules must be library-agnostic or the
in-module branch becomes a constraint. The orchestration layer (§4–§8) does not
change between the two.

## 4. Orchestration model

The orchestrator (main thread) is the single authority over actor existence.
This is the structural advantage of a plugin system over the library's
object-level refs: every worker was spawned by main, so main holds every
`Worker` handle and every `onDeath` hook. Consequences:

- **No liveness plane needed.** The object-level ref codec needs heartbeat pairs
  because ownership can live in any worker and the router (main) only bootstraps
  connections. Here main _is_ the lifecycle authority — spawn's `onDeath`
  already reports every unexpected exit. Death handling, cascade teardown and
  reference reclamation all run off main's registry; no ping/pong machinery is
  required.
- **Registry** — `actorId → { worker, state, surface, deps, dependents }`.
  Dependencies come from two sources: the static `deps` declaration, and runtime
  acquire records (a worker may discover real dependencies beyond the
  declaration; the declaration must cover all usage, actual connections win,
  mismatches are warned).
- **Reference resolution** — a consumer's acquire-actor request is answered by
  main: create a `MessageChannel`, ask the actor worker to serve its
  `publicSurface` over one port (`__serve-actor`), hand the other port to the
  requester (`__actor-acquired`). This is the `link()` mechanism instantiated
  per acquire — the channel-agnostic RPC factories (`makeRpcHandler` /
  `createRpcProxy`) already do this for worker links.

## 5. Lifecycle state machine (the core)

The state machine is what makes "no incomplete states" enforceable instead of
aspirational. Every operation is legal only in the states that can complete it
atomically.

### 5.1 States

```
              ensure()                ready
   stopped ───────────▶ starting ───────────▶ running
     ▲                   │   │                  │  │
     │        cancel/    │   │ fail/crash       │  │ disable() / cascade
     │        dep-fail   ▼   ▼                  ▼  ▼
     └────────────── stopped ◀─── stopping ◀──────┘
                                  (cooperative
                                   completion or
                                   timeout → terminate)

   crashed — unexpected exit (onDeath) from any state; handled like a
   completed stop for the purpose of cascades.
```

- **stopped** — no worker, no graph edges. `ensure()` is the only legal entry.
  An acquire request may lazily trigger `ensure()` or be refused, depending on
  policy. (An actor that was disabled stays in `stopped` with its metadata
  retained, so it can be re-enabled.)
- **starting** — the worker exists and is initializing (handshake done,
  dependencies confirmed running, `create()` finished). The actor is **not yet
  published**: invisible means unusable — this is the first guard against
  incomplete states. Acquire requests queue (resolved when `running`) or are
  refused. `disable()` during `starting` cancels the start: terminate the fresh
  worker, back to `stopped`. A dependency failing while starting moves the start
  to the crash path.
- **running** — published in the registry; acquire-serve is accepted. This is
  the only state in which consumers can connect.
- **stopping** — the cancellation signal has been injected (or a dispose frame
  sent); in-flight calls drain or are cancelled. New acquire-serve is refused
  with a released-style error. Graph edges are **kept** through `stopping` —
  cascade ordering depends on it (see §6). Completion (cooperative) or timeout →
  `stopped`.
- **stopped** (final) — worker terminated, edges removed, metadata retained.
- **crashed** — an unexpected exit (`onDeath` without going through `stopping`).
  Dependents cascade to `stopping`; the entry is kept for diagnostics.
  `ensure()` may restart from `crashed` if policy allows.

### 5.2 Transition matrix

| From     | To       | Trigger                           | Guard                                                         | Actions                                        |
| -------- | -------- | --------------------------------- | ------------------------------------------------------------- | ---------------------------------------------- |
| stopped  | starting | `ensure()`                        | all deps `running` (or ensured first)                         | create worker, wait for ready                  |
| starting | running  | ready                             | all deps `running`                                            | publish in registry, accept acquire-serve      |
| starting | stopped  | start cancelled                   | —                                                             | terminate worker, no publication ever happened |
| starting | crashed  | `onDeath`                         | —                                                             | cascade dependents                             |
| running  | stopping | `disable()` / cascade / swap done | no dependents still `running` (or cascading in the same wave) | inject cancel signal                           |
| stopping | stopped  | cooperative completion            | in-flight calls drained or cancelled                          | terminate, remove edges                        |
| stopping | stopped  | timeout                           | cooperative window expired                                    | terminate (bounded, not infinite)              |
| stopping | crashed  | `onDeath`                         | —                                                             | treat as `stopped`, cascade dependents         |
| any      | crashed  | `onDeath`                         | —                                                             | cascade dependents                             |

### 5.3 Invariants

1. **At most one instance per actorId reaches `running`** — except inside a swap
   window (§8.2), which is the single sanctioned moment of coexistence.
2. **Acquire-serve only from `running`.** Consumers never touch a worker that is
   not fully up (no partial visibility).
3. **A dependency may not enter `stopping` while a dependent is still
   `running`** — unless that dependent is being stopped in the same cascade
   wave. This is the ordering rule that makes teardown deterministic.
4. **An actor starts only after its dependencies are `running`** (topological
   start). Graph edges: declared at `starting`, live during `running` /
   `stopping`, removed at `stopped`.

### 5.4 Crash handling

A crash anywhere is handled by the same cascade machinery as a disable (§6): the
crashed actor counts as already stopped (its dependents were already talking to
a corpse), and its dependents cascade. The only difference is the diagnostic and
the auto-restart policy. This is what keeps the graph from "knowing" about a
half-alive actor: after `crashed`, no one can reach it.

## 6. Cascade termination (waves)

Disabling an actor computes the **dependency closure** (everything that
transitively depends on it), then tears down in reverse topological order:

- **Wave model, not linear recursion.** Each wave takes all nodes that no longer
  have running dependents (the leaves of the remaining subgraph) and
  cooperatively cancels them **in parallel**. Waves are serial with respect to
  each other; nodes within a wave are parallel. Parallel branches never wait on
  each other; the chain within a wave is always serial.
- **Cooperative cancellation has a bounded window.** The cancellation signal is
  injected, in-flight work is given a grace period (e.g. 5 s), then the worker
  is terminated regardless. One uncooperative plugin must not stall the cascade
  — bounded, not infinite, matching the library's cleanup philosophy.
- **A crash during a wave counts as completion** for that node (its `crashed`
  exit feeds the same cascade logic).
- **Effect on consumers:** live consumers of a being-stopped actor are refused
  new acquire-serve and their held references die (actor-level equivalent of the
  ref codec's `released` broadcast, propagated by the orchestrator).
- **Dependency reclamation** happens at `stopped`, when the edges leave the
  graph — so a later restart starts from a clean slate.

## 7. Dependency recovery (topological start)

Restarting a disabled subgraph runs `ensure()` in topological order:

- **Topological start** — dependencies first, per wave; all nodes whose
  dependencies are already `running` start in parallel.
- **Idempotent** — every `ensure()` is idempotent: reconnecting an existing
  reference must not re-create the worker.
- **Failure policy (must be chosen, not left blank):**
  - _rollback_ — a failed start tears down everything started in this recovery
    and returns the subgraph to the `stopped` set it started from. Nothing
    half-restored is left running. Cost: a transient failure loses the whole
    batch.
  - _degraded_ — the failure is reported, the successfully started part stays
    `running`, dependents of the failed actor cascade. Faster, but leaves a
    partially restored system by design. The default should be **rollback**: "no
    incomplete states" is the point of this design, and a partial restore is
    exactly an incomplete state.

## 8. Hot reload

Two tiers. Both are full cancel-and-rebuild or full swap — never in-place
mutation of a live worker.

### 8.1 Stop-and-start (default)

`stopping → stopped → starting → running` for the reloaded actor, cascading its
dependents per §6. Simple, no intermediate state, bounded downtime window. The
dependency set is identical before and after; only the actor's code changed.

### 8.2 Parallel swap (atomic switch-over)

The expensive tier: zero-downtime reload. It exists only because a lifecycle
state machine defines the window in which two instances of one actorId are legal
— without states, "the new one is ready, switch" is unmanageable.

Orchestrator-level trajectory (a swap is a _registry-level_ window on top of the
per-actor machine):

```
old: running
    │  swap(actorId)
    ▼
old: running  +  new: starting      ← two instances coexist (invariant 1 suspends)
    │  new ready
    ▼
old: running  +  new: running       ← both healthy
    │  atomic switch-over
    ▼
old: stopping +  new: running       ← consumers now reach only `new`
    │  cooperative / timeout
    ▼
stopped + new: running              ← back to one instance
```

- **The switch-over point** is the only moment the registry rebinds
  `actorId → instance`. Two consumer connection models:
  - _Indirect reference (recommended)_ — consumers hold a rebindable proxy; the
    switch-over updates the forwarding target, consumers are unaware. This is
    exactly the forward-once-acquired pattern the ref codec already uses for
    pending proxies (`entry.real`), lifted to the actor level. No in-flight
    calls are lost on the switch.
  - _Direct connection + reconnect broadcast_ — consumers learn of the switch
    and re-acquire. Simpler, but calls made in the switch window fail. Only
    acceptable if the surface is low-traffic and failures are retryable.
- **Failure rollback** — if `new` fails in `starting`, terminate it, `old` stays
  `running`, the registry returns to one instance. The system behaves exactly as
  if the reload never happened. No intermediate state survives a failed swap.
- **Dependencies during swap** — `new` shares `old`'s dependencies; edges are
  actorId-scoped, not instance-scoped, so no double counting and no dependent
  churn for the consumers of the reloaded actor.
- **Cancelling a swap** (disable during swap): abort the swap (terminate `new`)
  and disable `old` per the normal cascade.

The swap tier is the one place the pattern genuinely needs library support; see
§9.2.

## 9. Library modification assessment

Does this require changes to the library? Split by tier.

### 9.1 Works with the current library (no changes)

- **Module view** — two mechanisms, neither needs library changes: the runtime
  branch (`defineActor`, thin glue in `examples/plugin_system/` using the
  existing `getWorkerId()`) or the load-hook branch (shim entry + Deno's native
  `registerHooks()`, pure convention) — §3.
- **Acquire protocol** — `__acquire-actor` / `__serve-actor` /
  `__actor-acquired` are new control frame _types_, registered via the existing
  `registerControlHandler` extension point; no core protocol change.
- **Per-acquire channels** — the existing channel-agnostic RPC factories
  (`makeRpcHandler` / `createRpcProxy`) serve the acquired pair, exactly as
  `link()` does today.
- **Lifecycle monitoring** — spawn's `onDeath` covers every unexpected exit; the
  orchestrator keeps the registry and the dependency graph in application code.
- **State machine, cascade waves, topological recovery, stop-and-start reload**
  — all orchestrator logic; main thread application code.
- **Cooperative cancellation** — the orchestrator can inject a signal as a
  library value (e.g. a per-actor `AbortSignal` passed through the acquired
  channel) without new worker-side API, as long as the plugin's worker code
  knows to read it.

### 9.2 Minimal library changes (only for the swap tier + worker-side signal)

Two genuine gaps, both small:

1. **Rebindable reference (swap's indirect model).** The type-safe forwarding
   proxy — `Remote<T>`'s TransformCallbacks/AsyncIterable projection applied to
   a proxy whose target can be switched — should be library-provided, not
   hand-rolled per plugin. Candidate shapes: a `createSwitchableProxy()`
   primitive, or making the acquire protocol registry-indirect by default so
   switch-over is transparent to consumers. Reuses the pending-proxy pattern
   already in `ref_codec.ts`.
2. **Worker-side cancellation signal.** If graceful shutdown should be enforced
   by the worker (the cancellation signal is the plugin's own responsibility to
   honor), the worker needs a first-class handle — e.g. a `serveWorker` option
   or a worker-context accessor that exposes the current actor's cancellation
   signal. Without this, cancellation is convention-only (plugins must know
   where to look).

### 9.3 Phased recommendation

- **Phase 1 (no library changes):** `examples/plugin_system/` skeleton + this
  document as the contract. Module view starts with the runtime branch (§3.1) —
  zero environment risk; the load-hook branch (§3.2) is a drop-in replacement at
  the entry layer if plugin modules must be library-agnostic. State machine,
  dependency graph, cascade waves, topological recovery, stop-and-start reload,
  convention-level cancellation. This validates the orchestration shape end to
  end.
- **Phase 2 (minimal library changes):** worker-side cancellation signal, then
  the rebindable reference for swap, only if the scenario confirms zero-downtime
  reload is a real requirement (Phase 1's stop-and-start may be sufficient — the
  plugin system's own scale decides).
