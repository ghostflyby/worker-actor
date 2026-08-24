// Process-actor example: the rpc object is served via serveProcess, so the
// actor runs in a separate Deno process (node:child_process fork IPC).
import { serveProcess } from "@ghostflyby/worker-actor";

export const rpc = {
  add(a: number, b: number): number {
    return a + b;
  },
  greet(name: string): string {
    return `hello ${name}`;
  },
  async *count(n: number): AsyncIterable<number> {
    for (let i = 0; i < n; i++) {
      await new Promise((r) => setTimeout(r, 10));
      yield i;
    }
  },
  async wait(ms: number, signal: AbortSignal): Promise<string> {
    // Poll the rebuilt signal: event-driven abort after a prior stream
    // channel close is a known gap (see TRANSPORT.md known issues).
    const start = Date.now();
    while (Date.now() - start < ms) {
      if (signal.aborted) return "aborted";
      await new Promise((r) => setTimeout(r, 10));
    }
    return "timed-out";
  },
};

serveProcess(rpc);
