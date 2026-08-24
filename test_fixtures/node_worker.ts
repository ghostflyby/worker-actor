// Multi-actor node fixture: served via serveNode.
import { serveNode } from "../worker_runtime.ts";

export const actors = {
  counter: {
    inc(n: number): number {
      return n + 1;
    },
    reset(): number {
      return 0;
    },
  },
  greeter: {
    hello(name: string): string {
      return `hi ${name}`;
    },
  },
};

serveNode(actors);
