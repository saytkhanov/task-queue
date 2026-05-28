import { QueueClosedError } from "./errors.js";

export type TaskKey = string;

export interface TaskQueueOptions {
  concurrency: number;
  retries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;

  /**
   * Internal testing hook
   */
  clock?: {
    sleep(ms: number, signal?: AbortSignal): Promise<void>;
    random(): number;
  };
}

export interface TaskContext {
  signal: AbortSignal;
  attempt: number;
}

export interface TaskStats {
  enqueued: number;
  started: number;
  deduplicated: number;
  retried: number;
  failed: number;
  canceled: number;
}

type Clock = NonNullable<TaskQueueOptions["clock"]>;

interface QueueItem<T> {
  key: TaskKey;
  task: (ctx: TaskContext) => Promise<T>;
  controller: AbortController;
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
  started: boolean;
}

const defaultClock: Clock = {
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(timer);
        reject(signal?.reason);
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  },
  random(): number {
    return Math.random();
  }
};

export class TaskQueue {
  private readonly concurrency: number;
  private readonly retries: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly jitterRatio: number;
  private readonly clock: Clock;

  private readonly queue: QueueItem<unknown>[] = [];
  private readonly inFlight = new Map<TaskKey, QueueItem<unknown>>();

  private activeCount = 0;
  private closing = false;
  private shutdownPromise?: Promise<void>;
  private resolveShutdown?: () => void;

  private readonly counters: TaskStats = {
    enqueued: 0,
    started: 0,
    deduplicated: 0,
    retried: 0,
    failed: 0,
    canceled: 0
  };

  constructor(options: TaskQueueOptions) {
    const {
      concurrency,
      retries = 0,
      baseDelayMs = 100,
      maxDelayMs = 30_000,
      jitterRatio = 0.2,
      clock = defaultClock
    } = options;

    if (!Number.isInteger(concurrency) || concurrency <= 0) {
      throw new TypeError("concurrency must be an integer greater than 0");
    }
    if (!Number.isInteger(retries) || retries < 0) {
      throw new TypeError("retries must be a non-negative integer");
    }
    if (!(baseDelayMs >= 0)) {
      throw new TypeError("baseDelayMs must be a non-negative number");
    }
    if (!(maxDelayMs >= 0)) {
      throw new TypeError("maxDelayMs must be a non-negative number");
    }
    if (!(jitterRatio >= 0)) {
      throw new TypeError("jitterRatio must not be negative");
    }

    this.concurrency = concurrency;
    this.retries = retries;
    this.baseDelayMs = baseDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.jitterRatio = jitterRatio;
    this.clock = clock;
  }

  add<T>(key: TaskKey, task: (ctx: TaskContext) => Promise<T>): Promise<T> {
    if (this.closing) {
      return Promise.reject(new QueueClosedError());
    }

    const existing = this.inFlight.get(key);
    if (existing) {
      this.counters.deduplicated++;
      return existing.promise as Promise<T>;
    }

    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    const item: QueueItem<T> = {
      key,
      task,
      controller: new AbortController(),
      promise,
      resolve,
      reject,
      started: false
    };

    this.queue.push(item as QueueItem<unknown>);
    this.inFlight.set(key, item as QueueItem<unknown>);
    this.counters.enqueued++;

    this.drain();

    return promise;
  }

  private drain(): void {
    // Scheduling loop is implemented in a later stage.
  }

  shutdown(): Promise<void> {
    if (!this.shutdownPromise) {
      this.shutdownPromise = new Promise<void>((resolve) => {
        this.resolveShutdown = resolve;
      });
    }
    this.closing = true;
    return this.shutdownPromise;
  }

  stats(): TaskStats {
    return { ...this.counters };
  }
}
