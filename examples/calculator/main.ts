/** Main-thread demo: `deno run examples/calculator/main.ts` */
import type * as WorkerModule from "./worker.ts";
import { spawn } from "../../spawn.ts";

const actor = await spawn<typeof WorkerModule.rpc>(
  new Worker(import.meta.resolve("./worker.ts"), { type: "module" }),
);

console.log("add(1,2)   =", await actor.add(1, 2));
console.log("add(5,-3)  =", await actor.add(5, -3));

const [slow, fast] = await Promise.all([
  actor.delay(120, "slow"),
  actor.delay(10, "fast"),
]);
console.log(`out-of-order: "${fast}" arrived before "${slow}"`);

const report = await actor.report({ id: "u-42", name: "Ada" });
console.log("report:", report);

try {
  await actor.divide(1, 0);
} catch (e) {
  console.log(
    `caught ${(e as Error).constructor.name}: ${(e as Error).message}`,
  );
}

// —— AsyncIterable transport ——
async function* numbers(): AsyncGenerator<number> {
  yield 1;
  yield 2;
  yield 3;
}

console.log("sumIterable     =", await actor.sumIterable(numbers()));
console.log("sumNested       =", await actor.sumNested({ items: numbers() }));

const stream = await actor.stream("chunk", 5);
const chunks: string[] = [];
for await (const c of stream) chunks.push(c);
console.log("stream          =", chunks);

try {
  for await (const _ of await actor.failingStream()) {
    // Expected to throw mid-way; the loop body is intentionally empty
  }
} catch (e) {
  console.log(
    `stream error    = ${(e as Error).name}: ${(e as Error).message}`,
  );
}

// Early stop: the cancel signal reaches the worker, whose generator finally runs
const inf = await actor.infiniteStream();
const it = inf[Symbol.asyncIterator]();
console.log("first item      =", (await it.next()).value);
await it.return?.();
const deadline = Date.now() + 2_000;
let cancels = 0;
while (Date.now() < deadline) {
  cancels = await actor.getStreamCancelCount();
  if (cancels > 0) break;
  await new Promise((r) => setTimeout(r, 10));
}
console.log("worker cancels  =", cancels);

await actor.dispose();
console.log("disposed, done");
