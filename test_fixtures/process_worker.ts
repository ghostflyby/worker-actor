// Process-actor fixture: the child module served via serveProcess.
import { serveProcess } from "../worker_runtime.ts";

export const rpc = {
  add(a: number, b: number): number {
    return a + b;
  },
  greet(name: string): string {
    return `hello ${name}`;
  },
  map(): Map<string, number> {
    return new Map([["a", 1], ["b", 2]]);
  },
  async *count(n: number): AsyncIterable<number> {
    for (let i = 0; i < n; i++) yield i;
  },
};

serveProcess(rpc);
