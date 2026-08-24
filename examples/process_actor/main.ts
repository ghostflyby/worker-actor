// Process-actor example: spawn an actor in a separate Deno process and call it.
// Run: deno run --allow-read --allow-run --allow-env examples/process_actor/main.ts
import type * as ProcessModule from "./worker.ts";
import { spawnProcess } from "../../spawn.ts";

const actor = await spawnProcess<typeof ProcessModule.rpc>(
  "./examples/process_actor/worker.ts",
  {
    permissions: { read: true },
  },
);

console.log("add(1, 2) =", await actor.add(1, 2));
console.log("greet(world) =", await actor.greet("world"));

const got: number[] = [];
for await (const n of await actor.count(3)) got.push(n);
console.log("count(3) =", got);

const controller = new AbortController();
const waiting = actor.wait(5000, controller.signal);
setTimeout(() => controller.abort(), 200);
console.log("wait(aborted) =", await waiting);

await actor.dispose();
console.log("disposed");
