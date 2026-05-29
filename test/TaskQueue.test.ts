// ФИО: Shamil Saitkhanov

import test from "node:test";
import assert from "node:assert/strict";
import { TaskQueue, QueueClosedError, TaskCanceledError } from "../src/index.js";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;

  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

test("concurrency limit", async () => {
  const queue = new TaskQueue({ concurrency: 2 });

  let active = 0;
  let maxActive = 0;

  const run = (i: number) =>
    queue.add(`task-${i}`, async () => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await wait(20);
      active -= 1;
      return i;
    });

  const results = await Promise.all([0, 1, 2, 3, 4].map(run));

  assert.equal(maxActive, 2);
  assert.deepEqual(results, [0, 1, 2, 3, 4]);
  assert.equal(queue.stats().started, 5);
});

test("deduplication", async () => {
  const queue = new TaskQueue({ concurrency: 2 });

  let calls = 0;
  const task = async () => {
    calls += 1;
    await wait(10);
    return "result";
  };

  const [a, b, c] = await Promise.all([
    queue.add("same", task),
    queue.add("same", task),
    queue.add("same", task)
  ]);

  assert.equal(calls, 1);
  assert.equal(a, "result");
  assert.equal(b, "result");
  assert.equal(c, "result");
  assert.equal(queue.stats().deduplicated, 2);
  assert.equal(queue.stats().enqueued, 1);
});

test("retry with growing delays", async () => {
  const delays: number[] = [];

  const queue = new TaskQueue({
    concurrency: 1,
    retries: 2,
    baseDelayMs: 10,
    maxDelayMs: 100,
    jitterRatio: 0,
    clock: {
      random: () => 0.5,
      sleep: async (ms) => {
        delays.push(ms);
      }
    }
  });

  let attempts = 0;
  const result = await queue.add("retry", async () => {
    attempts += 1;
    if (attempts < 3) {
      throw new Error("boom");
    }
    return "ok";
  });

  assert.equal(result, "ok");
  assert.equal(attempts, 3);
  assert.deepEqual(delays, [10, 20]);
  assert.equal(queue.stats().retried, 2);
  assert.equal(queue.stats().failed, 0);
});

test("graceful shutdown", async () => {
  const queue = new TaskQueue({ concurrency: 1 });

  const gate = deferred<void>();

  const p1 = queue.add("active", async () => {
    await gate.promise;
    return "done";
  });

  const p2 = queue.add("waiting", async () => "never");

  const shutdownPromise = queue.shutdown();

  await assert.rejects(p2, TaskCanceledError);
  await assert.rejects(
    queue.add("new", async () => "x"),
    QueueClosedError
  );

  let shutdownDone = false;
  void shutdownPromise.then(() => {
    shutdownDone = true;
  });
  await wait(20);
  assert.equal(shutdownDone, false);

  gate.resolve();

  assert.equal(await p1, "done");
  await shutdownPromise;
  assert.equal(shutdownDone, true);
  assert.equal(queue.stats().canceled, 1);
});
