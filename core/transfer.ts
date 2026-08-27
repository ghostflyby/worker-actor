/**
 * Transfer policy: how values cross the boundary — clone (structured clone
 * copy) or move (ownership transfer via the postMessage transfer list).
 *
 * Direction asymmetry: the SENDER decides. Parameters are sent main→worker,
 * so spawn (and its derived transfer views) configure the argument side; the
 * return value is encoded worker→main, so serveWorker, serveProcess, or
 * serveNode configures the return side locally. Transfer policy is not part of
 * the handshake and cannot be overridden by the caller.
 * `params[...]` slots never coexist with `return` in one policy.
 *
 * `"move"` is chosen over the web's `"transfer"` to avoid colliding with the
 * `postMessage(msg, { transfer })` option name — this is an action, not a
 * transfer-list reference.
 *
 * Move semantics (verified against Deno's MessageChannel): a value kept in the
 * payload AND listed in the transfer list is detached on the sender and
 * delivered in place on the receiver (the same object, zero copy). So "move"
 * needs no placeholder or decode-side rebuild — the encoder just collects the
 * configured values into the transfer list while leaving the payload intact.
 *
 * Scope: only messageport-type transports support transfer lists. On
 * framed/message transports a move policy is ignored, so the underlying
 * transport keeps its ordinary clone/serialization behavior.
 */

import type { Transport } from "./transport.ts";

/** Clone (copy, the default) or move (ownership transfer, sender detaches). */
export type TransferAction = "clone" | "move";

/** Argument slot: the i-th parameter, or all parameters. */
export type ArgSlot = `params[${number}]` | `params[*]`;

/** Per-method argument policy: per-slot, or a shorthand for all slots. */
export type ArgPolicy =
  | { [slot in ArgSlot]?: TransferAction }
  | TransferAction;

/** Argument-side config: keyed by method name; unlisted methods use default. */
export type TransferArgsConfig = {
  [method: string]: ArgPolicy | TransferAction | undefined;
  default?: TransferAction;
};

/** Argument-side config, or a shorthand applying to every direct parameter. */
export type TransferArgs = TransferArgsConfig | TransferAction;

/** A return value is a single slot, so the per-method policy is just the action. */
export type ReturnPolicy = TransferAction;

/** Return-side config: keyed by method name; unlisted methods use default. */
export type TransferReturnConfig = {
  [method: string]: ReturnPolicy | undefined;
  default?: TransferAction;
};

/** Return-side config, or a shorthand applying to every direct return value. */
export type TransferReturn = TransferReturnConfig | TransferAction;

/** Resolve the action for one method from a return-value config. */
export function actionFor(
  config: TransferReturn | undefined,
  method: string,
): TransferAction {
  if (config === undefined || config === "clone" || config === "move") {
    return config ?? "clone";
  }
  return config[method] ?? config.default ?? "clone";
}

/** Resolve the argument policy for one method, falling back to the global default. */
export function argPolicyFor(
  config: TransferArgs | undefined,
  method: string,
): ArgPolicy | undefined {
  if (config === undefined) return undefined;
  if (config === "clone" || config === "move") return config;
  return config[method] ?? config.default;
}

/** The directly movable values supported by the first transfer-config version. */
export type MovableValue = ArrayBuffer | ArrayBufferView | MessagePort;

/** A directly movable value; nested values are intentionally not inspected. */
export function isMovable(v: unknown): v is MovableValue {
  return typeof v === "object" && v !== null &&
    (v instanceof ArrayBuffer || ArrayBuffer.isView(v) ||
      v instanceof MessagePort);
}

function transferValue(v: MovableValue): Transferable {
  if (v instanceof MessagePort || v instanceof ArrayBuffer) return v;
  // ArrayBufferView itself is not a transferable-list entry. Its backing buffer
  // is the transferable, and a SharedArrayBuffer is rejected below.
  const buffer = v.buffer;
  if (!(buffer instanceof ArrayBuffer)) {
    throw new TypeError(
      "[transfer] move requested for a non-transferable value: " +
        "SharedArrayBuffer-backed views cannot be moved.",
    );
  }
  return buffer;
}

/** Move requires a supported transferable value; anything else fails loudly. */
function requireMovable(v: unknown, where: string): Transferable {
  if (!isMovable(v)) {
    throw new TypeError(
      `[transfer] move requested for a non-transferable value (${where}): ` +
        `expected ArrayBuffer / ArrayBufferView / MessagePort, got ` +
        `${v === null ? "null" : typeof v}. ` +
        `Use "clone" for this slot or pass a transferable.`,
    );
  }
  return transferValue(v);
}

function addTransfer(
  value: unknown,
  where: string,
  transfer: Transferable[],
): void {
  const normalized = requireMovable(value, where);
  if (!transfer.includes(normalized)) transfer.push(normalized);
}

function supportsMove(transport: Transport | undefined): boolean {
  return transport === undefined || transport.kind === "messageport";
}

function actionAt(
  policy: Exclude<ArgPolicy, TransferAction>,
  index: number,
): TransferAction {
  // A concrete slot overrides the wildcard, including an explicit "clone".
  return policy[`params[${index}]`] ?? policy["params[*]"] ?? "clone";
}

/**
 * Collect only the direct argument values configured for move into `transfer`.
 * The payload remains unchanged; no nested traversal is performed here.
 */
export function collectMoveArgs(
  args: unknown[],
  policy: ArgPolicy | undefined,
  transfer: Transferable[],
  transport?: Transport,
): void {
  if (policy === undefined || policy === "clone") return;
  // Non-messageport transports have no native transfer list. Treat move as
  // clone and let their existing serialization path handle the value.
  if (!supportsMove(transport)) return;
  if (policy === "move") {
    for (let i = 0; i < args.length; i++) {
      addTransfer(args[i], `args[${i}]`, transfer);
    }
    return;
  }
  for (let i = 0; i < args.length; i++) {
    const action = actionAt(policy, i);
    if (action === "move") {
      addTransfer(args[i], `args[${i}]`, transfer);
    }
  }
}

/** Collect a direct return value configured for move into `transfer`. */
export function collectMoveReturn(
  value: unknown,
  action: TransferAction | undefined,
  transfer: Transferable[],
  transport?: Transport,
): void {
  if (action !== "move") return;
  // A move request is transparent on transports without a native transfer
  // list; the return value follows that transport's normal clone behavior.
  if (!supportsMove(transport)) return;
  addTransfer(value, "return value", transfer);
}
