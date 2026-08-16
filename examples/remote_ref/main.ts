/** Main-thread demo: `deno run examples/remote_ref/main.ts` */
import type * as WorkerModule from "./worker.ts";
import { spawn } from "@ghostflyby/worker-actor";
import { type RemoteRef, remoteRefCodec } from "./ref_codec.ts";

const actor = await spawn<typeof WorkerModule.rpc>(
  new Worker(import.meta.resolve("./worker.ts"), { type: "module" }),
  { codecs: [remoteRefCodec] },
);

const ref: RemoteRef<{ increment(): number; get(): number }> = await actor
  .createCounter();
console.log("increment →", await ref.increment());
console.log("increment →", await ref.increment());
console.log("get →", await ref.get());

try {
  await (ref as unknown as { boom(): Promise<never> }).boom();
} catch (e) {
  console.log(
    `ref error → ${(e as Error).constructor.name}: ${(e as Error).message}`,
  );
}

// nested AsyncIterable flows through the reference channel
const out: number[] = [];
for await (
  const v of await (ref as unknown as {
    countdown(n: number): Promise<AsyncIterable<number>>;
  })
    .countdown(3)
) {
  out.push(v);
}
console.log("countdown →", out);

await ref.dispose();
console.log("disposed ref; worker released →", await actor.disposedCount());
await actor.dispose();
