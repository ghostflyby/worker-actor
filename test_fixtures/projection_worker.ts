/** Runtime fixture for projection_test.ts: custom thenable / custom AsyncIterable. */
import { serveWorker } from "@ghostflyby/worker-actor";

/** A custom thenable (PromiseLike but not a native Promise). */
class MyTask<T> {
  constructor(private readonly value: T) {}

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    _onrejected?:
      | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
      | null,
  ): PromiseLike<TResult1 | TResult2> {
    return Promise.resolve(this.value).then(onfulfilled);
  }
}

/** A custom AsyncIterable (structurally extends the protocol, not the interface). */
class MyStream<T> {
  private readonly items: T[];
  constructor(items: T[]) {
    this.items = items;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    for (const item of this.items) {
      await new Promise((r) => setTimeout(r, 1));
      yield item;
    }
  }
}

export const rpc = {
  /** Returns a custom thenable: the handler awaits it, the crossing value is 42. */
  task(): MyTask<number> {
    return new MyTask(42);
  },

  /** Returns a custom AsyncIterable: the iterable codec rebuilds a local stream. */
  streamOf(): MyStream<string> {
    return new MyStream(["a", "b", "c"]);
  },
};

serveWorker(rpc);
