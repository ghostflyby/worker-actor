/**
 * Per-address serial task queue: the actor's own mailbox discipline.
 *
 * One RefQueue per reference channel (or per callback owner) gives that
 * address actor-style serialization: only one task executes at a time, and a
 * task's segments never interleave with another task's segments. Tasks are
 * explicit segments (not promise chains): a task can suspend itself via
 * yieldTo() — its current segment completes, the queue releases (the next
 * task starts, so a long IO wait never blocks the address), and once the
 * external promise settles the task is re-queued; when its turn comes, the
 * queue resumes it.
 *
 * The resumption boundary IS gated by the queue (the yield promise resolves
 * only when the task is served again); the continuation after that resolve is
 * a microtask, so it interleaves fairly with other tasks' segments — that is
 * the documented boundary of "controlled resumption".
 *
 * Module-level "current task" context: the worker is single-threaded and the
 * queue pump drives segments synchronously, so a module-level current-task
 * pointer is race-free. actorYield() reads it to find the queue to yield to;
 * main-channel calls (no per-ref queue) degrade to a plain await.
 */

interface RefTask {
  /** The whole task body (an async segment chain); may call yieldTo() inside. */
  run: () => Promise<void>;
  /** Set once the task first starts; resolves when the whole body completes. */
  done?: Promise<void>;
  /** Set while the task is suspended (waiting for its external promise). */
  resume?: () => void;
}

/** Module-level context: the queue currently driving a segment (worker-global, race-free). */
let currentQueue: RefQueue | undefined;
/** The task whose segment is currently executing (or undefined between segments). */
let currentTask: RefTask | undefined;

/** The queue currently being serviced — read by actorYield() to yield to. */
export function getCurrentQueue(): RefQueue | undefined {
  return currentQueue;
}

/** The task currently executing (only meaningful while a queue is driving). */
export function getCurrentTask(): RefTask | undefined {
  return currentTask;
}

export class RefQueue {
  #queue: RefTask[] = [];
  #running: RefTask | null = null;

  /** Enqueue a task (one call to this reference); tasks run strictly serially. */
  enqueue(run: () => Promise<void>): void {
    this.#queue.push({ run });
    this.#pump();
  }

  /**
   * Suspend the currently executing task: its segment completes, the queue is
   * released (the next queued task starts immediately), and when `p` settles
   * the task is re-queued — the returned promise resolves only when the queue
   * serves it again. The continuation after the await is a fair microtask.
   */
  yieldTo<T>(p: Promise<T>): Promise<T> {
    const task = this.#running;
    if (!task) return p; // defensive: no running task (should not happen)
    // Release the queue: the current segment is done.
    this.#running = null;
    if (currentTask === task) currentTask = undefined;
    this.#pump(); // the next queued task starts now
    return new Promise<T>((resolve, reject) => {
      void p.then(
        (value) => {
          task.resume = () => resolve(value);
          this.#queue.push(task);
          this.#pump();
        },
        (err) => {
          task.resume = () => reject(err);
          this.#queue.push(task);
          this.#pump();
        },
      );
    });
  }

  #pump(): void {
    if (this.#running || this.#queue.length === 0) return;
    const task = this.#queue.shift()!;
    this.#running = task;
    currentQueue = this;
    currentTask = task;
    const resume = task.resume;
    const exec: Promise<void> = resume
      ? (async () => {
        // Resumed task: re-run the continuation, then wait for the whole
        // body to finish (it may yield again). resume is consumed once.
        task.resume = undefined;
        resume();
        await task.done;
      })()
      : (async () => {
        // First run: the body starts; done tracks its completion.
        const run = (async () => {
          await task.run();
        })();
        task.done = run;
        await run;
      })();
    void exec.finally(() => {
      this.#running = null;
      if (currentTask === task) currentTask = undefined;
      this.#pump();
    });
  }
}
