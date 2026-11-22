import { test } from 'node:test';
import assert from 'node:assert/strict';
import { backoffDelay } from '../src/backoff.js';

const opts = { baseMs: 100, maxMs: 1000, factor: 2 };

test('doubles the ceiling each attempt (lower bound with rng=0)', () => {
  const rng = () => 0;
  assert.equal(backoffDelay(1, { ...opts, rng }), 50);
  assert.equal(backoffDelay(2, { ...opts, rng }), 100);
  assert.equal(backoffDelay(3, { ...opts, rng }), 200);
});

test('never exceeds maxMs and never goes below half the ceiling', () => {
  for (let attempt = 1; attempt <= 30; attempt++) {
    const ceiling = Math.min(1000, 100 * 2 ** (attempt - 1));
    for (const r of [0, 0.5, 0.999999]) {
      const delay = backoffDelay(attempt, { ...opts, rng: () => r });
      assert.ok(delay >= Math.floor(ceiling / 2), `attempt ${attempt}: ${delay} too small`);
      assert.ok(delay <= ceiling, `attempt ${attempt}: ${delay} too large`);
    }
  }
});

test('jitter actually varies the delay', () => {
  const low = backoffDelay(4, { ...opts, rng: () => 0 });
  const high = backoffDelay(4, { ...opts, rng: () => 0.99 });
  assert.ok(high > low);
});
