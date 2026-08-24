// Multi-actor node fixture: served via serveNode (one process, many actors).
import { serveNode } from "@ghostflyby/worker-actor";

const counter = {
  value: 0,
  inc(n: number) {
    return (this.value += n);
  },
};

export const actors = {
  counter: {
    inc(n: number): number {
      return counter.inc(n);
    },
    get(): number {
      return counter.value;
    },
  },
  greeter: {
    hello(name: string): string {
      return `hi ${name}`;
    },
  },
};

serveNode(actors);
