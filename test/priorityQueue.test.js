import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MinHeap } from '../src/priorityQueue.js';

test('pops items in ascending order', () => {
  const heap = new MinHeap((a, b) => a - b);
  const input = Array.from({ length: 500 }, (_, i) => (i * 7919) % 1009);
  for (const n of input) heap.push(n);
  const out = [];
  while (heap.size > 0) out.push(heap.pop());
  assert.deepEqual(out, [...input].sort((a, b) => a - b));
});

test('peek does not remove, empty pop returns undefined', () => {
  const heap = new MinHeap((a, b) => a - b);
  assert.equal(heap.pop(), undefined);
  assert.equal(heap.peek(), undefined);
  heap.push(3);
  heap.push(1);
  assert.equal(heap.peek(), 1);
  assert.equal(heap.size, 2);
});

test('supports object items with a custom comparator', () => {
  const heap = new MinHeap((a, b) => b.p - a.p || a.seq - b.seq);
  heap.push({ p: 1, seq: 1 });
  heap.push({ p: 9, seq: 3 });
  heap.push({ p: 9, seq: 2 });
  assert.deepEqual([heap.pop().seq, heap.pop().seq, heap.pop().seq], [2, 3, 1]);
});
