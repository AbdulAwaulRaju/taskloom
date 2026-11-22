import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JobQueue } from '../src/jobQueue.js';
import { ConflictError, NotFoundError, ValidationError } from '../src/errors.js';
import { fakeClock } from '../testkit/fakeClock.js';

// rng = 0 makes backoff deterministic: attempt 1 -> 500ms, 2 -> 1000ms, 3 -> 2000ms
function makeQueue(extra = {}) {
  const clock = fakeClock();
  const queue = new JobQueue({ clock, rng: () => 0, backoff: { baseMs: 1000, maxMs: 60_000 }, ...extra });
  return { queue, clock };
}

function tmpWal(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'taskloom-q-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return path.join(dir, 'queue.wal');
}

test('higher priority first, FIFO within the same priority', () => {
  const { queue } = makeQueue();
  queue.enqueue({ type: 'a', priority: 0 });
  const b = queue.enqueue({ type: 'b', priority: 5 }).job;
  const c = queue.enqueue({ type: 'c', priority: 5 }).job;
  const d = queue.enqueue({ type: 'd', priority: 9 }).job;
  const order = [queue.reserve(), queue.reserve(), queue.reserve(), queue.reserve()].map((j) => j.type);
  assert.deepEqual(order, ['d', 'b', 'c', 'a']);
  assert.equal(queue.reserve(), null);
  assert.ok(b && c && d);
});

test('delayed jobs become available only once their time has come', () => {
  const { queue, clock } = makeQueue();
  const { job } = queue.enqueue({ type: 'later', delayMs: 5000 });
  assert.equal(job.state, 'delayed');
  assert.equal(queue.reserve(), null);
  clock.advance(4999);
  assert.equal(queue.reserve(), null);
  clock.advance(1);
  assert.equal(queue.reserve().id, job.id);
});

test('ack completes a job; stale or wrong tokens are rejected', () => {
  const { queue } = makeQueue();
  const { job } = queue.enqueue({ type: 'x', payload: { n: 1 } });
  const leased = queue.reserve({ workerId: 'w1' });
  assert.equal(leased.attempts, 1);
  assert.equal(leased.workerId, 'w1');
  assert.deepEqual(leased.payload, { n: 1 });

  assert.throws(() => queue.ack(job.id, 'wrong-token'), ConflictError);
  queue.ack(job.id, leased.token);
  assert.equal(queue.getJob(job.id).state, 'completed');
  assert.throws(() => queue.ack(job.id, leased.token), ConflictError); // double ack
  assert.throws(() => queue.ack('missing', 'x'), NotFoundError);
  assert.equal(queue.getJob(job.id).token, undefined); // tokens never leak through getJob
  assert.equal(queue.stats().completed, 1);
});

test('nack retries with backoff, then dead-letters after maxAttempts', () => {
  const { queue, clock } = makeQueue();
  const { job } = queue.enqueue({ type: 'flaky', maxAttempts: 2 });

  const first = queue.reserve();
  queue.nack(job.id, first.token, 'boom 1');
  let current = queue.getJob(job.id);
  assert.equal(current.state, 'delayed');
  assert.equal(current.lastError, 'boom 1');
  assert.equal(queue.reserve(), null); // still backing off

  clock.advance(500);
  const second = queue.reserve();
  assert.equal(second.attempts, 2);
  queue.nack(job.id, second.token, 'boom 2');

  current = queue.getJob(job.id);
  assert.equal(current.state, 'dead');
  assert.equal(queue.listDead().length, 1);
  assert.equal(queue.listDead()[0].lastError, 'boom 2');
  assert.deepEqual(
    { dead: queue.stats().dead, ready: queue.stats().ready, active: queue.stats().active },
    { dead: 1, ready: 0, active: 0 },
  );
});

test('an expired lease counts as a failed attempt and invalidates the old token', () => {
  const { queue, clock } = makeQueue();
  const { job } = queue.enqueue({ type: 'slow' });
  const leased = queue.reserve({ leaseMs: 1000 });

  clock.advance(1001);
  assert.equal(queue.reserve(), null); // reserve() ticks: lease reaped, job now backing off
  const afterExpiry = queue.getJob(job.id);
  assert.equal(afterExpiry.state, 'delayed');
  assert.equal(afterExpiry.lastError, 'lease expired');

  clock.advance(500);
  const redelivered = queue.reserve();
  assert.equal(redelivered.id, job.id);
  assert.equal(redelivered.attempts, 2);
  assert.throws(() => queue.ack(job.id, leased.token), ConflictError); // zombie worker is fenced off
  queue.ack(job.id, redelivered.token);
});

test('heartbeat extends a lease', () => {
  const { queue, clock } = makeQueue();
  const { job } = queue.enqueue({ type: 'long' });
  const leased = queue.reserve({ leaseMs: 1000 });

  clock.advance(800);
  queue.heartbeat(job.id, leased.token, 1000); // now valid until +1800
  clock.advance(800); // t = 1600
  queue.tick();
  assert.equal(queue.getJob(job.id).state, 'active');

  clock.advance(300); // t = 1900
  queue.tick();
  assert.notEqual(queue.getJob(job.id).state, 'active');
});

test('idempotency keys deduplicate enqueues', () => {
  const { queue } = makeQueue();
  const first = queue.enqueue({ type: 'charge', idempotencyKey: 'order-42' });
  const second = queue.enqueue({ type: 'charge', idempotencyKey: 'order-42' });
  assert.equal(first.duplicate, false);
  assert.equal(second.duplicate, true);
  assert.equal(second.job.id, first.job.id);
  assert.equal(queue.stats().total, 1);
});

test('requeueDead gives a dead job a fresh start', () => {
  const { queue } = makeQueue();
  const { job } = queue.enqueue({ type: 'x', maxAttempts: 1 });
  queue.nack(job.id, queue.reserve().token, 'fail');
  assert.equal(queue.getJob(job.id).state, 'dead');

  const requeued = queue.requeueDead(job.id);
  assert.equal(requeued.state, 'ready');
  assert.equal(requeued.attempts, 0);
  assert.throws(() => queue.requeueDead(job.id), ConflictError); // not dead anymore
  assert.equal(queue.reserve().id, job.id);
});

test('rejects invalid input', () => {
  const { queue } = makeQueue();
  const bad = [
    {},
    { type: '' },
    { type: 'x', priority: 10 },
    { type: 'x', priority: 1.5 },
    { type: 'x', delayMs: -1 },
    { type: 'x', maxAttempts: 0 },
    { type: 'x', idempotencyKey: 42 },
    { type: 'x', payload: 'y'.repeat(300 * 1024) },
  ];
  for (const input of bad) assert.throws(() => queue.enqueue(input), ValidationError, JSON.stringify(input).slice(0, 60));
  assert.throws(() => queue.reserve({ leaseMs: 0 }), ValidationError);
  const circular = {};
  circular.self = circular;
  assert.throws(() => queue.enqueue({ type: 'x', payload: circular }), ValidationError);
});

test('callers cannot corrupt internal state by mutating returned objects', () => {
  const { queue } = makeQueue();
  const { job } = queue.enqueue({ type: 'x', payload: { list: [1] } });
  job.payload.list.push(2);
  job.state = 'dead';
  assert.deepEqual(queue.getJob(job.id).payload, { list: [1] });
  assert.equal(queue.getJob(job.id).state, 'ready');
});

test('completed history is bounded and evicts idempotency keys with it', () => {
  const { queue } = makeQueue({ completedRetention: 1 });
  const first = queue.enqueue({ type: 'x', idempotencyKey: 'k1' }).job;
  queue.ack(first.id, queue.reserve().token);
  const second = queue.enqueue({ type: 'x' }).job;
  queue.ack(second.id, queue.reserve().token);

  assert.equal(queue.getJob(first.id), null); // evicted
  assert.equal(queue.stats().completed, 1);
  assert.equal(queue.enqueue({ type: 'x', idempotencyKey: 'k1' }).duplicate, false);
});

test('state survives a restart, including live leases', (t) => {
  const walPath = tmpWal(t);
  const clock = fakeClock();
  const options = { walPath, clock, rng: () => 0, backoff: { baseMs: 1000 } };

  const q1 = new JobQueue(options);
  const done = q1.enqueue({ type: 'done' }).job;
  const inFlight = q1.enqueue({ type: 'in-flight', priority: 9 }).job;
  const waiting = q1.enqueue({ type: 'waiting' }).job;
  const doomed = q1.enqueue({ type: 'doomed', maxAttempts: 1, priority: 8 }).job;

  const a = q1.reserve(); // in-flight (priority 9)
  const b = q1.reserve(); // doomed (priority 8)
  q1.nack(doomed.id, b.token, 'nope');
  const c = q1.reserve(); // done / waiting (FIFO among priority 0)
  q1.ack(c.id, c.token);
  const before = q1.stats();
  q1.close();

  const q2 = new JobQueue(options); // simulated crash + restart
  assert.deepEqual(q2.stats(), before);
  assert.equal(q2.getJob(doomed.id).state, 'dead');
  assert.equal(q2.getJob(c.id).state, 'completed');

  q2.ack(a.id, a.token); // the lease token issued before the "crash" is still valid
  assert.equal(q2.getJob(inFlight.id).state, 'completed');

  const next = q2.reserve();
  assert.ok([done.id, waiting.id].includes(next.id));
  q2.close();
});

test('log compaction bounds the log and preserves state exactly', (t) => {
  const walPath = tmpWal(t);
  const options = { walPath, compactMinRecords: 10, completedRetention: 5, fsync: false };

  const q1 = new JobQueue(options);
  for (let i = 0; i < 50; i++) {
    const { job } = q1.enqueue({ type: 'x', payload: { i } });
    q1.ack(job.id, q1.reserve().token);
  }
  const keep = q1.enqueue({ type: 'pending', payload: 'still here' }).job;
  const stats = q1.stats();
  assert.ok(stats.walRecords < 60, `log was not compacted: ${stats.walRecords} records for 151 events`);
  q1.close();

  const q2 = new JobQueue(options);
  assert.equal(q2.stats().completed, 5);
  assert.equal(q2.stats().ready, 1);
  assert.deepEqual(q2.getJob(keep.id).payload, 'still here');
  q2.close();
});
