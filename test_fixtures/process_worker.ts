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
  /** Loop until aborted; returns how many iterations ran. */
  async spin(ms: number, signal: AbortSignal): Promise<number> {
    const start = Date.now();
    let iterations = 0;
    while (Date.now() - start < ms) {
      if (signal.aborted) break;
      iterations++;
      await new Promise((r) => setTimeout(r, 1));
    }
    return iterations;
  },
  /** Invoke the callback with a value and return its result. */
  async apply(cb: (x: number) => Promise<number>, x: number): Promise<number> {
    return await cb(x);
  },
};

serveProcess(rpc);
