import { assertEquals } from "@std/assert";
import { createActorRegistry } from "./core/registry.ts";
import { fromMessagePort } from "./core/transport.ts";

Deno.test("ActorRegistry: register/resolve/idOf round-trip", () => {
  const reg = createActorRegistry();
  const { port1, port2 } = new MessageChannel();
  const t = fromMessagePort(port1);
  const id = reg.register(t);
  assertEquals(typeof id, "string");
  assertEquals(reg.resolve(id), t);
  assertEquals(reg.idOf(t), id);
  // Same transport re-registers to the same id.
  assertEquals(reg.register(t), id);
  void port2;
});

Deno.test("ActorRegistry: unregister removes the transport and fires the callback", () => {
  const reg = createActorRegistry();
  const { port1, port2 } = new MessageChannel();
  const t = fromMessagePort(port1);
  const id = reg.register(t);
  const unregistered: string[] = [];
  reg.onUnregister((uid) => unregistered.push(uid));
  reg.unregister(id);
  assertEquals(reg.resolve(id), undefined);
  assertEquals(reg.idOf(t), undefined);
  assertEquals(unregistered, [id]);
  // Idempotent.
  reg.unregister(id);
  assertEquals(unregistered, [id]);
  void port2;
});

Deno.test("ActorRegistry: two transports get distinct ids", () => {
  const reg = createActorRegistry();
  const c1 = new MessageChannel();
  const c2 = new MessageChannel();
  const t1 = fromMessagePort(c1.port1);
  const t2 = fromMessagePort(c2.port1);
  const id1 = reg.register(t1);
  const id2 = reg.register(t2);
  assertEquals(id1 !== id2, true);
  void c1.port2;
  void c2.port2;
});
