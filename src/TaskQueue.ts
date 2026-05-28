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
