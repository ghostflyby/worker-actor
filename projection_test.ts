/**
 * Remote<T> return-type projection tests.
 *
 * Compile-time assertions use the repo's convention (pool_test.ts's
 * `interface Rpc extends ActorPool<...>`): each expected projected signature is
 * re-declared as a member of an interface extending the projection. A mismatch
 * fails `deno check`. Runtime tests verify the honest crossing values for the
 * two custom async-semantics shapes.
 */
import { assertEquals } from "@std/assert";
import { spawn } from "@ghostflyby/worker-actor";
import type { CodecValueTypes } from "@ghostflyby/worker-actor/codec";
import type { Codec } from "@ghostflyby/worker-actor/codec";
import type { Remote } from "@ghostflyby/worker-actor";

// —— compile-time fixture API: every return shape the projection must handle ——

/** A custom thenable: PromiseLike but not a native Promise. */
interface MyTask<R> extends PromiseLike<R> {}

/** A custom AsyncIterable subtype. */
interface MyStream<E> extends AsyncIterable<E> {}

/** A branded custom type transported by a codec (not async-structured). */
interface BrandedHandle {
  readonly brand: "handle";
  call(): Promise<number>;
}

/** A codec that transports BrandedHandle (its type feeds the Pass set). */
declare const brandedCodec: Codec<BrandedHandle>;

type Api = {
  // custom thenable → Promise<X> (rule 3, non-nested)
  task(): MyTask<number>;
  // custom AsyncIterable → AsyncIterable<E> (rule 4, flattened to interface)
  streamOf(): MyStream<string>;
  // hybrid (thenable + iterable) → Promise<X> (runtime awaits first)
  hybrid(): MyTask<number> & AsyncIterable<string>;
  // bare AsyncIterable → AsyncIterable<E>
  stream(): AsyncIterable<string>;
  // eager Promise<AsyncIterable> → Promise<AsyncIterable<E>> (X = AsyncIterable<E>)
  eager(): Promise<AsyncIterable<number>>;
  // native Promise → Promise<V>
  add(a: number, b: number): Promise<number>;
  // sync return → Promise<R>
  name(): string;
  // never-returning → Promise<never>
  boom(): never;
  // non-function member → never
  CONST: number;
};

/** The codec tuple (as const) whose value type drives the Pass set. */
declare const brandedCodecTuple: readonly [typeof brandedCodec];

type Pass = CodecValueTypes<typeof brandedCodecTuple>;

/** The projection with the codec-derived Pass set applied. */
type Projected = Remote<Api, Pass>;

// —— compile-time assertions: each member re-declared with its expected shape ——

interface ProjectedSurface extends Projected {
  // custom thenable: the resolution value is number, NOT the thenable itself
  task(): Promise<number>;
  // custom AsyncIterable: flattened to the AsyncIterable<E> interface
  streamOf(): AsyncIterable<string>;
  // hybrid: runtime awaits first → resolution is the thenable's value
  hybrid(): Promise<number>;
  // bare AsyncIterable keeps its own async semantics (no Promise wrapper)
  stream(): AsyncIterable<string>;
  // eager Promise<AsyncIterable> keeps its Promise
  eager(): Promise<AsyncIterable<number>>;
  // native Promise passes through
  add(a: number, b: number): Promise<number>;
  // sync return normalized to Promise
  name(): Promise<string>;
  // never-returning stays Promise<never>
  boom(): Promise<never>;
  // non-function member is never
  CONST: never;
}

// A value of type ProjectedSurface must be a subtype of Projected: every
// re-declared member must be assignable to the projected member. This is the
// compile-time assertion (mismatches fail `deno check`).
export const _projectionSurface: Projected =
  null as unknown as ProjectedSurface;

// —— runtime: custom thenable / custom AsyncIterable cross the boundary honestly ——

import type * as WorkerModule from "./test_fixtures/projection_worker.ts";

const WORKER_URL = import.meta.resolve(
  "./test_fixtures/projection_worker.ts",
);

function makeActor() {
  return spawn<typeof WorkerModule.rpc>(
    new Worker(WORKER_URL, { type: "module" }),
  );
}

Deno.test("projection: custom thenable resolves to its value (not re-wrapped)", async () => {
  const actor = await makeActor();
  // task(): Promise<number> — awaiting gives the resolution value 42.
  assertEquals(await actor.task(), 42);
  await actor.dispose();
});

Deno.test("projection: custom AsyncIterable flattens to a consumable stream", async () => {
  const actor = await makeActor();
  // streamOf(): AsyncIterable<string> — for-await works directly on the call result.
  const out: string[] = [];
  for await (const v of actor.streamOf()) out.push(v);
  assertEquals(out, ["a", "b", "c"]);
  await actor.dispose();
});
