// Run with: npm run demo
// Starts a server in-process, runs a worker against it over HTTP, and shows retries,
// priorities, delayed jobs and the dead-letter queue in action.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { JobQueue, createApp, HttpClient, Worker } from '../src/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (msg) => console.log(`  ${msg}`);

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'taskloom-demo-'));
const queue = new JobQueue({ walPath: path.join(dir, 'queue.wal'), backoff: { baseMs: 100, maxMs: 800 } });
const app = createApp({ queue, tickIntervalMs: 50 });
const port = await app.listen(0);
const client = new HttpClient({ baseUrl: `http://127.0.0.1:${port}` });
console.log(`\nTaskloom demo - server on :${port}, log in ${dir}\n`);

const worker = new Worker({
  source: client,
  concurrency: 2,
  leaseMs: 2000,
  pollIntervalMs: 40,
  handlers: {
    email: async ({ to }) => {
      await sleep(60);
      log(`sent email to ${to}`);
    },
    flaky: async (_payload, { job }) => {
      if (job.attempts < 3) throw new Error(`transient failure on attempt ${job.attempts}`);
      log(`flaky job finally succeeded on attempt ${job.attempts}`);
    },
    poison: async () => {
      throw new Error('this job can never succeed');
    },
    report: async () => log('delayed report generated'),
  },
  onEvent: (e) => {
    if (e.type === 'nack') log(`x ${e.jobType} failed (attempt ${e.attempt}) -> retry or dead-letter`);
  },
});

console.log('Enqueueing jobs...');
await client.enqueue({ type: 'email', payload: { to: 'low@example.com' }, priority: 1 });
await client.enqueue({ type: 'email', payload: { to: 'vip@example.com' }, priority: 9 });
await client.enqueue({ type: 'flaky', maxAttempts: 5 });
await client.enqueue({ type: 'poison', maxAttempts: 3 });
await client.enqueue({ type: 'report', delayMs: 600 });
const dup = await client.enqueue({ type: 'email', payload: { to: 'once@example.com' }, idempotencyKey: 'welcome-1' });
const dup2 = await client.enqueue({ type: 'email', payload: { to: 'once@example.com' }, idempotencyKey: 'welcome-1' });
log(`idempotency: second enqueue was a duplicate = ${dup2.duplicate} (same id: ${dup.job.id === dup2.job.id})\n`);

console.log('Processing:');
worker.start();
for (;;) {
  const s = await client.stats();
  if (s.ready + s.delayed + s.active === 0) break;
  await sleep(100);
}
await worker.stop();

const stats = await client.stats();
const dead = await client.listDead();
console.log('\nFinal stats:', JSON.stringify(stats));
console.log('Dead-letter queue:', dead.map((j) => `${j.type} (${j.attempts} attempts): ${j.lastError}`));

await app.close();
fs.rmSync(dir, { recursive: true, force: true });
