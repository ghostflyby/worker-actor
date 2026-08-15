/**
 * A custom "marshal-by-ref" codec built on the generic Channel abstraction.
 *
 * This is the high-level counterpart to the stream primitives: any object can
 * be wrapped into a cross-thread reference (the .NET MarshalByRef pattern).
 * The real object stays on its side; the peer gets a transparent proxy whose
 * method calls are marshaled over a dedicated channel.
 *
 * The library deliberately provides NO automatic behavior here: the channel,
 * the frames and the lifecycle are this codec's own design, built on top of
 * openChannel / connectChannel / registerRelease from core/channel.ts.
 *
 * Wire protocol (one channel per reference):
 *   proxy → owner  { type: "call"; id; method; args }         // args go through the registry
 *   owner → proxy  { type: "result"; id; ok: true; value }    // value too (nested streams/refs work)
 *   owner → proxy  { type: "result"; id; ok: false; error }
 *   proxy → owner  { type: "dispose" }                        // explicit dispose or GC
 *
 * Lifecycle: explicit dispose(), GC-based release (FinalizationRegistry), and
 * failAll (the registry closes the channel). The owner runs the real object's
 * [Symbol.dispose] hook when the reference is released.
 */

import {
  Codec,
  CODEC_PLACEHOLDER_KEY,
  DecodeContext,
  EncodeContext,
} from "../../core/codec.ts";
import {
  connectChannel,
  openChannel,
  registerRelease,
} from "../../core/channel.ts";
import { RemoteError, serializeError } from "../../core/protocol.ts";

const REF_BRAND = Symbol.for("worker-actor-example.remote-ref");

/** The proxy type: every method returns a Promise; non-functions are `never`. */
export type RemoteRef<T> =
  & {
    [K in keyof T]: T[K] extends (...args: infer A) => infer R
      ? (...args: A) => Promise<Awaited<R>>
      : never;
  }
  & { dispose(): Promise<void> };

/** Transmittable token produced by remoteRef(); recognized by the codec. */
interface RefToken {
  [REF_BRAND]: true;
  obj: unknown;
}

type RefFrame =
  | { type: "call"; id: number; method: string; args: unknown[] }
  | { type: "result"; id: number; ok: true; value: unknown }
  | {
    type: "result";
    id: number;
    ok: false;
    error: { name: string; message: string; stack?: string };
  }
  | { type: "dispose" };

interface RefHandle {
  [CODEC_PLACEHOLDER_KEY]: "remote-ref";
  port: MessagePort;
}

/** Wrap a real object into a transmittable remote reference (kept on this side). */
export function remoteRef<T extends object>(obj: T): RemoteRef<T> {
  return { [REF_BRAND]: true, obj } as unknown as RemoteRef<T>;
}

function startRefOwner(
  channel: ReturnType<typeof connectChannel>,
  obj: unknown,
  registry: EncodeContext["registry"],
): void {
  channel.onMessage(async (message) => {
    const frame = message as RefFrame;
    if (frame.type === "call") {
      const fn = (obj as Record<string, unknown>)[frame.method];
      if (typeof fn !== "function") {
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: false,
            error: serializeError(
              new Error(`No such method: "${frame.method}"`),
            ),
          } satisfies RefFrame,
        );
        return;
      }
      try {
        // args arrive encoded (may contain nested streams/refs); decode them
        const args = registry.decode(frame.args) as unknown[];
        const value = await (fn as (...a: unknown[]) => unknown).apply(
          obj,
          args,
        );
        const transfer: Transferable[] = [];
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: true,
            value: registry.encode(value, transfer),
          } satisfies RefFrame,
          transfer,
        );
      } catch (e) {
        channel.send(
          {
            type: "result",
            id: frame.id,
            ok: false,
            error: serializeError(e),
          } satisfies RefFrame,
        );
      }
    } else if (frame.type === "dispose") {
      // Optional cleanup hook on the real object (mirrors generator finally)
      (obj as { [Symbol.dispose]?: () => void })[Symbol.dispose]?.();
      channel.close();
    }
  });
}

function createRefProxy(
  channel: ReturnType<typeof connectChannel>,
  registry: DecodeContext["registry"],
): RemoteRef<unknown> {
  const pending = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (reason: unknown) => void;
  }>();
  let nextId = 1;
  let closed = false;

  channel.onMessage((message) => {
    const frame = message as RefFrame;
    if (frame.type === "result") {
      const call = pending.get(frame.id);
      if (!call) return;
      pending.delete(frame.id);
      if (frame.ok) call.resolve(registry.decode(frame.value));
      else call.reject(new RemoteError(frame.error));
    }
  });

  const fail = (reason: unknown): void => {
    if (closed) return;
    closed = true;
    for (const call of pending.values()) call.reject(reason);
    pending.clear();
    channel.close();
  };

  const call = (method: string, args: unknown[]): Promise<unknown> => {
    if (closed) return Promise.reject(new Error("Remote ref is disposed"));
    return new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      const transfer: Transferable[] = [];
      channel.send(
        {
          type: "call",
          id,
          method,
          args: registry.encode(args, transfer) as unknown[],
        } satisfies RefFrame,
        transfer,
      );
    });
  };

  let unregisterRelease: () => void = () => {};

  const dispose = (): void => {
    if (closed) return;
    closed = true;
    channel.send({ type: "dispose" } satisfies RefFrame);
    channel.close();
    for (const c of pending.values()) {
      c.reject(new Error("Remote ref disposed"));
    }
    pending.clear();
    unregisterRelease();
  };

  const proxy = new Proxy({} as RemoteRef<unknown>, {
    get(_target, prop) {
      if (prop === "dispose") return dispose;
      if (prop === Symbol.dispose) return () => void dispose();
      // The proxy must not be detected as a thenable, or await behavior breaks.
      if (prop === "then") return undefined;
      if (typeof prop === "string") {
        return (...args: unknown[]) => call(prop, args);
      }
      return undefined;
    },
  });

  // Best-effort release on GC: the finalizer captures only channel/fail/dispose
  // closures — never the proxy — so it cannot keep the proxy alive.
  unregisterRelease = registerRelease(proxy, () => {
    channel.send({ type: "dispose" } satisfies RefFrame);
    fail(new Error("Remote ref garbage-collected"));
  });

  return proxy;
}

export const remoteRefCodec: Codec<RemoteRef<unknown>> = {
  tag: "remote-ref",
  matches(v: unknown): v is RemoteRef<unknown> {
    return typeof v === "object" && v !== null &&
      (v as { [REF_BRAND]?: unknown })[REF_BRAND] === true;
  },
  encode(token: RemoteRef<unknown>, ctx: EncodeContext): unknown {
    // The transmitted side is the brand token { [REF_BRAND]: true, obj };
    // RemoteRef is the proxy type, so the token is recovered via the brand.
    const ref = token as unknown as RefToken;
    const { channel, peerPort } = openChannel(ctx);
    ctx.registry.registerChannel(channel);
    startRefOwner(channel, ref.obj, ctx.registry);
    return {
      [CODEC_PLACEHOLDER_KEY]: "remote-ref",
      port: peerPort,
    } satisfies RefHandle;
  },
  decode(placeholder: RefHandle, ctx: DecodeContext): RemoteRef<unknown> {
    const channel = connectChannel(placeholder.port);
    ctx.registry.registerChannel(channel);
    return createRefProxy(channel, ctx.registry);
  },
};
