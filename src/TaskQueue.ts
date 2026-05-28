import { QueueClosedError, TaskCanceledError } from "./errors.js";

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

export class TaskQueue {
  private readonly concurrency: number;
  private readonly retries: number;
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly jitterRatio: number;
  private readonly customClock?: Clock;

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
      clock
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
    this.customClock = clock;
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
    while (!this.closing && this.activeCount < this.concurrency && this.queue.length > 0) {
      const item = this.queue.shift();
      if (!item) return;

      item.started = true;
      this.activeCount += 1;
      this.counters.started += 1;

      void this.runItem(item);
    }

    this.maybeResolveShutdown();
  }

  private async runItem<T>(item: QueueItem<T>): Promise<void> {
    try {
      const result = await this.executeWithRetries(item);
      item.resolve(result);
    } catch (error) {
      this.counters.failed += 1;
      item.reject(error);
    } finally {
      this.inFlight.delete(item.key);
      this.activeCount -= 1;
      this.drain();
      this.maybeResolveShutdown();
    }
  }

  private async executeWithRetries<T>(item: QueueItem<T>): Promise<T> {
    const maxAttempts = 1 + this.retries;

    for (let attempt = 1; ; attempt++) {
      try {
        return await item.task({
          signal: item.controller.signal,
          attempt
        });
      } catch (error) {
        if (attempt >= maxAttempts) {
          throw error;
        }
        this.counters.retried += 1;
        const delay = this.calculateDelay(attempt);
        await this.sleep(delay, item.controller.signal);
      }
    }
  }

  private calculateDelay(attempt: number): number {
    const exponential = this.baseDelayMs * 2 ** (attempt - 1);
    const capped = Math.min(exponential, this.maxDelayMs);

    if (this.jitterRatio === 0) {
      return capped;
    }

    const random = this.customClock?.random ? this.customClock.random() : Math.random();
    // factor in [1 - jitterRatio, 1 + jitterRatio]
    const factor = 1 - this.jitterRatio + random * (2 * this.jitterRatio);
    return capped * factor;
  }

  private sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      return Promise.reject(new TaskCanceledError());
    }

    if (this.customClock) {
      return this.customClock.sleep(ms, signal).catch((error: unknown) => {
        if (signal?.aborted) {
          throw new TaskCanceledError();
        }
        throw error;
      });
    }

    return new Promise<void>((resolve, reject) => {
      const onAbort = () => {
        clearTimeout(timer);
        reject(new TaskCanceledError());
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }, ms);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  private maybeResolveShutdown(): void {
    if (this.closing && this.activeCount === 0 && this.queue.length === 0) {
      this.resolveShutdown?.();
    }
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
