import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/jobQueue.js';
import { Worker } from '../src/worker.js';
import { createApp } from '../src/server.js';
import { HttpClient } from '../src/client.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(predicate, { timeoutMs = 4000, label = 'condition' } = {}) {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${label}`);
    await sleep(10);
  }
}

const fastQueue = () => new JobQueue({ rng: () => 0, backoff: { baseMs: 20, maxMs: 40 } });

test('processes jobs by type and passes the payload', async (t) => {
  const queue = fastQueue();
  const seen = [];
  const worker = new Worker({
    source: queue,
    pollIntervalMs: 5,
    concurrency: 3,
    handlers: { greet: async (payload) => seen.push(payload.name) },
  });
  t.after(() => worker.stop());
  for (const name of ['ada', 'linus', 'grace']) queue.enqueue({ type: 'greet', payload: { name } });

  worker.start();
  await waitFor(() => queue.stats().completed === 3, { label: 'all jobs completed' });
  assert.deepEqual(seen.sort(), ['ada', 'grace', 'linus']);
});

test('failing handlers are retried and finally dead-lettered', async (t) => {
  const queue = fastQueue();
  let calls = 0;
  const worker = new Worker({
    source: queue,
    pollIntervalMs: 5,
    concurrency: 1,
    handlers: {
      explode: async () => {
        calls += 1;
        throw new Error('always fails');
      },
    },
  });
  t.after(() => worker.stop());
  const { job } = queue.enqueue({ type: 'explode', maxAttempts: 3 });

  worker.start();
  await waitFor(() => queue.stats().dead === 1, { label: 'job dead-lettered' });
  assert.equal(calls, 3);
  assert.equal(queue.getJob(job.id).lastError, 'always fails');
});

test('jobs with no registered handler are not silently dropped', async (t) => {
  const queue = fastQueue();
  const worker = new Worker({ source: queue, pollIntervalMs: 5, concurrency: 1, handlers: {} });
  t.after(() => worker.stop());
  const { job } = queue.enqueue({ type: 'mystery', maxAttempts: 1 });

  worker.start();
  await waitFor(() => queue.stats().dead === 1, { label: 'job dead-lettered' });
  assert.match(queue.getJob(job.id).lastError, /no handler registered for "mystery"/);
});

test('heartbeats keep a long job from being redelivered', async (t) => {
  const queue = fastQueue();
  let calls = 0;
  const worker = new Worker({
    source: queue,
    pollIntervalMs: 5,
    concurrency: 2, // the second loop keeps calling reserve(), which reaps expired leases
    leaseMs: 90,
    handlers: {
      long: async () => {
        calls += 1;
        await sleep(400); // far longer than the 90ms lease
      },
    },
  });
  t.after(() => worker.stop());
  queue.enqueue({ type: 'long' });

  worker.start();
  await waitFor(() => queue.stats().completed === 1, { label: 'long job completed' });
  assert.equal(calls, 1, 'job must be delivered exactly once while heartbeats succeed');
});

test('stop() is graceful: in-flight jobs finish before it resolves', async (t) => {
  const queue = fastQueue();
  let finished = false;
  const worker = new Worker({
    source: queue,
    pollIntervalMs: 5,
    concurrency: 1,
    handlers: {
      slow: async () => {
        await sleep(150);
        finished = true;
      },
    },
  });
  t.after(() => worker.stop());
  queue.enqueue({ type: 'slow' });

  worker.start();
  await waitFor(() => queue.stats().active === 1, { label: 'job in flight' });
  await worker.stop();
  assert.equal(finished, true);
  assert.equal(queue.stats().completed, 1);
});

test('works end-to-end through the HTTP client', async (t) => {
  const queue = fastQueue();
  const app = createApp({ queue, authToken: 'tok', tickIntervalMs: 20 });
  const port = await app.listen(0);
  t.after(() => app.close());
  const client = new HttpClient({ baseUrl: `http://127.0.0.1:${port}`, authToken: 'tok' });

  const results = [];
  const worker = new Worker({
    source: client,
    pollIntervalMs: 10,
    concurrency: 2,
    handlers: { double: async ({ n }) => results.push(n * 2) },
  });
  t.after(() => worker.stop());

  for (const n of [1, 2, 3]) await client.enqueue({ type: 'double', payload: { n } });
  worker.start();
  await waitFor(() => results.length === 3, { label: 'three results' });
  assert.deepEqual(results.sort(), [2, 4, 6]);
  await waitFor(() => queue.stats().completed === 3, { label: 'acks processed' });
});
