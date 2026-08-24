import { assertEquals } from "@std/assert";
import { framedTransport, fromMessagePort } from "./core/transport.ts";
import { connectChannel } from "./core/channel.ts";
import type { Channel } from "./core/channel.ts";

interface Transport {
  kind: string;
  send(frame: unknown, transfer?: Transferable[]): void;
  onMessage(h: (frame: unknown) => void): void;
  openChannel(): { channel: Channel; token: unknown };
  onChannel(h: (channel: Channel) => void): void;
  close(): void;
}

/** Wait (polling) until `pred` is true or timeout; returns whether it became true. */
async function until(pred: () => boolean, timeoutMs = 2000): Promise<boolean> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) return false;
    await new Promise((r) => setTimeout(r, 5));
  }
  return true;
}

Deno.test("fromMessagePort: openChannel hands a port; peer connectChannel round-trips", async () => {
  const { port1, port2 } = new MessageChannel();
  const t = fromMessagePort(port1);
  const opened = t.openChannel();
  const peer = connectChannel(opened.token as MessagePort);
  const got: unknown[] = [];
  peer.onMessage((v) => got.push(v));
  opened.channel.send("ping");
  await until(() => got.length === 1);
  assertEquals(got, ["ping"]);
  opened.channel.send({ n: 2 });
  await until(() => got.length === 2);
  assertEquals(got, ["ping", { n: 2 }]);
  t.close();
  void port2;
});

/** Wire two framed transports through an in-memory byte pipe pair. */
function wirePair(): { a: Transport; b: Transport } {
  // Each direction is a queue + a pending pull resolver: write() queues (or
  // enqueues if a pull is waiting); pull() enqueues or parks until data.
  function makePipe(): {
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  } {
    let queue: Uint8Array[] = [];
    let waiters: Array<(chunk: Uint8Array) => void> = [];
    return {
      readable: new ReadableStream<Uint8Array>({
        pull(controller) {
          const chunk = queue.shift();
          if (chunk) {
            controller.enqueue(chunk);
            return;
          }
          return new Promise<void>((resolve) => {
            waiters.push((c) => {
              controller.enqueue(c);
              resolve();
            });
          });
        },
      }),
      writable: new WritableStream<Uint8Array>({
        write(chunk) {
          const waiter = waiters.shift();
          if (waiter) waiter(chunk.slice());
          else queue.push(chunk.slice());
        },
      }),
    };
  }
  const aToB = makePipe();
  const bToA = makePipe();
  return {
    a: framedTransport(aToB.readable, bToA.writable),
    b: framedTransport(bToA.readable, aToB.writable),
  };
}

Deno.test("framedTransport: main-channel messages round-trip over bytes", async () => {
  const { a, b } = wirePair();
  const got: unknown[] = [];
  b.onMessage((v) => got.push(v));
  a.send({ hello: "world" });
  assertEquals(await until(() => got.length === 1), true);
  assertEquals(got, [{ hello: "world" }]);
  a.close();
  b.close();
});

Deno.test("framedTransport: openChannel token handshake + data over Mux", async () => {
  const { a, b } = wirePair();
  const peerChannels: Channel[] = [];
  b.onChannel((ch) => peerChannels.push(ch));

  const opened = a.openChannel();
  assertEquals(typeof (opened.token as { ch?: unknown })?.ch, "number");

  // A hands the token to B over the main channel.
  a.send(opened.token);
  assertEquals(await until(() => peerChannels.length === 1), true);

  const gotA: unknown[] = [];
  const gotB: unknown[] = [];
  opened.channel.onMessage((v) => gotA.push(v));
  peerChannels[0].onMessage((v) => gotB.push(v));

  opened.channel.send("a->b");
  assertEquals(await until(() => gotB.length === 1), true);
  assertEquals(gotB, ["a->b"]);

  peerChannels[0].send("b->a");
  assertEquals(await until(() => gotA.length === 1), true);
  assertEquals(gotA, ["b->a"]);

  a.close();
  b.close();
});

Deno.test("framedTransport: two channels multiplex over one connection", async () => {
  const { a, b } = wirePair();
  const peerChannels: Channel[] = [];
  b.onChannel((ch) => peerChannels.push(ch));

  const c1 = a.openChannel();
  const c2 = a.openChannel();
  a.send(c1.token);
  a.send(c2.token);
  assertEquals(await until(() => peerChannels.length === 2), true);

  const gotB1: unknown[] = [];
  const gotB2: unknown[] = [];
  peerChannels[0].onMessage((v) => gotB1.push(v));
  peerChannels[1].onMessage((v) => gotB2.push(v));
  c1.channel.send(1);
  c2.channel.send(2);
  c1.channel.send(3);
  assertEquals(
    await until(() => gotB1.length === 2 && gotB2.length === 1),
    true,
  );
  assertEquals(gotB1, [1, 3]);
  assertEquals(gotB2, [2]);

  a.close();
  b.close();
});
