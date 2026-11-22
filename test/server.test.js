import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JobQueue } from '../src/jobQueue.js';
import { createApp } from '../src/server.js';

const TOKEN = 's3cret';

async function start(t, options = {}) {
  const queue = new JobQueue({ rng: () => 0, backoff: { baseMs: 10 } });
  const app = createApp({ queue, authToken: TOKEN, ...options });
  const port = await app.listen(0);
  t.after(() => app.close());
  const base = `http://127.0.0.1:${port}`;

  async function api(path, { method = 'GET', body, token = TOKEN, raw } = {}) {
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    if (body !== undefined || raw !== undefined) headers['Content-Type'] = 'application/json';
    const res = await fetch(base + path, {
      method,
      headers,
      body: raw ?? (body === undefined ? undefined : JSON.stringify(body)),
    });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      /* plain-text response */
    }
    return { status: res.status, json, text };
  }
  return { api, queue };
}

test('healthz is public, everything else needs the bearer token', async (t) => {
  const { api } = await start(t);
  assert.equal((await api('/healthz', { token: null })).status, 200);
  assert.equal((await api('/stats', { token: null })).status, 401);
  assert.equal((await api('/stats', { token: 'wrong' })).status, 401);
  assert.equal((await api('/stats')).status, 200);
});

test('full job lifecycle over HTTP', async (t) => {
  const { api } = await start(t);

  const created = await api('/jobs', { method: 'POST', body: { type: 'email', payload: { to: 'a@b.c' }, priority: 3 } });
  assert.equal(created.status, 201);
  assert.equal(created.json.state, 'ready');

  assert.equal((await api(`/jobs/${created.json.id}`)).json.type, 'email');

  const leased = await api('/jobs/reserve', { method: 'POST', body: { workerId: 'w1', leaseMs: 5000 } });
  assert.equal(leased.status, 200);
  assert.equal(leased.json.id, created.json.id);
  assert.ok(leased.json.token);

  const hb = await api(`/jobs/${leased.json.id}/heartbeat`, { method: 'POST', body: { token: leased.json.token, extendMs: 5000 } });
  assert.equal(hb.status, 200);

  const acked = await api(`/jobs/${leased.json.id}/ack`, { method: 'POST', body: { token: leased.json.token } });
  assert.equal(acked.status, 200);
  assert.equal((await api(`/jobs/${leased.json.id}`)).json.state, 'completed');

  assert.equal((await api('/jobs/reserve', { method: 'POST', body: {} })).status, 204);
});

test('idempotent enqueue returns 200 with the original job', async (t) => {
  const { api } = await start(t);
  const a = await api('/jobs', { method: 'POST', body: { type: 'x', idempotencyKey: 'k' } });
  const b = await api('/jobs', { method: 'POST', body: { type: 'x', idempotencyKey: 'k' } });
  assert.equal(a.status, 201);
  assert.equal(b.status, 200);
  assert.equal(b.json.id, a.json.id);
});

test('nack leads to dead letters that can be requeued', async (t) => {
  const { api } = await start(t);
  const { json: job } = await api('/jobs', { method: 'POST', body: { type: 'x', maxAttempts: 1 } });
  const leased = (await api('/jobs/reserve', { method: 'POST', body: {} })).json;
  await api(`/jobs/${job.id}/nack`, { method: 'POST', body: { token: leased.token, error: 'kaput' } });

  const dead = await api('/dead');
  assert.equal(dead.json.length, 1);
  assert.equal(dead.json[0].lastError, 'kaput');

  const requeued = await api(`/dead/${job.id}/requeue`, { method: 'POST', body: {} });
  assert.equal(requeued.json.state, 'ready');
});

test('errors map to the right status codes with a JSON error body', async (t) => {
  const { api } = await start(t, { maxBodyBytes: 1024 });

  const badJson = await api('/jobs', { method: 'POST', raw: '{not json' });
  assert.equal(badJson.status, 400);
  assert.equal(badJson.json.error.code, 'VALIDATION');

  assert.equal((await api('/jobs', { method: 'POST', body: { type: '' } })).status, 400);
  assert.equal((await api('/jobs', { method: 'POST', body: [1, 2] })).status, 400);
  assert.equal((await api('/jobs/does-not-exist')).status, 404);
  assert.equal((await api('/nope')).status, 404);

  const { json: job } = await api('/jobs', { method: 'POST', body: { type: 'x' } });
  await api('/jobs/reserve', { method: 'POST', body: {} });
  assert.equal((await api(`/jobs/${job.id}/ack`, { method: 'POST', body: { token: 'wrong' } })).status, 409);
  assert.equal((await api(`/jobs/${job.id}/ack`, { method: 'POST', body: {} })).status, 400); // token missing

  const huge = await api('/jobs', { method: 'POST', body: { type: 'x', payload: 'z'.repeat(5000) } });
  assert.equal(huge.status, 413);
});

test('/metrics exposes Prometheus-style gauges', async (t) => {
  const { api } = await start(t);
  await api('/jobs', { method: 'POST', body: { type: 'x' } });
  const metrics = await api('/metrics');
  assert.equal(metrics.status, 200);
  assert.match(metrics.text, /taskloom_jobs\{state="ready"\} 1/);
  assert.match(metrics.text, /# TYPE taskloom_jobs gauge/);
});


test('job listing is authenticated and never exposes lease tokens', async (t) => {
  const { api } = await start(t);
  const created = await api('/jobs', { method: 'POST', body: { type: 'report' } });
  const leased = await api('/jobs/reserve', { method: 'POST', body: {} });
  assert.equal(leased.status, 200);
  const listing = await api('/jobs?state=all&limit=10');
  assert.equal(listing.status, 200);
  assert.equal(listing.json.total, 1);
  assert.equal(listing.json.jobs[0].id, created.json.id);
  assert.equal(Object.hasOwn(listing.json.jobs[0], 'token'), false);
  assert.equal((await api('/jobs', { token: null })).status, 401);
  assert.equal((await api('/jobs?state=invalid')).status, 400);
});

test('dashboard is served without external assets', async (t) => {
  const { api } = await start(t);
  const response = await api('/dashboard');
  assert.equal(response.status, 200);
  assert.match(response.text, /Queue Control Center/);
});
