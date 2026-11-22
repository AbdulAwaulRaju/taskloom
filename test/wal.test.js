import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { WriteAheadLog, crc32 } from '../src/wal.js';
import { CorruptLogError } from '../src/errors.js';

function tmpFile(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'taskloom-wal-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return path.join(dir, 'test.wal');
}

test('crc32 matches the well-known check value', () => {
  assert.equal(crc32(Buffer.from('123456789')), 0xcbf43926);
});

test('appended records survive a reopen', (t) => {
  const file = tmpFile(t);
  const { wal } = WriteAheadLog.open(file);
  wal.append([{ n: 1 }, { n: 2 }]);
  wal.append([{ n: 3 }]);
  wal.close();

  const reopened = WriteAheadLog.open(file);
  assert.deepEqual(reopened.records, [{ n: 1 }, { n: 2 }, { n: 3 }]);
  assert.equal(reopened.wal.records, 3);
  assert.equal(reopened.recoveredTornTail, false);
  reopened.wal.close();
});

test('a torn final write is dropped and the log stays appendable', (t) => {
  const file = tmpFile(t);
  const { wal } = WriteAheadLog.open(file);
  wal.append([{ n: 1 }, { n: 2 }]);
  wal.close();
  fs.appendFileSync(file, '0badc0de\t{"n":3,"trunc'); // crash mid-write: no newline

  const recovered = WriteAheadLog.open(file);
  assert.deepEqual(recovered.records, [{ n: 1 }, { n: 2 }]);
  assert.equal(recovered.recoveredTornTail, true);
  recovered.wal.append([{ n: 4 }]);
  recovered.wal.close();

  const final = WriteAheadLog.open(file);
  assert.deepEqual(final.records, [{ n: 1 }, { n: 2 }, { n: 4 }]);
  final.wal.close();
});

test('a bad checksum on the last line is treated as a torn write', (t) => {
  const file = tmpFile(t);
  const { wal } = WriteAheadLog.open(file);
  wal.append([{ n: 1 }]);
  wal.close();
  fs.appendFileSync(file, '00000000\t{"n":2}\n');

  const recovered = WriteAheadLog.open(file);
  assert.deepEqual(recovered.records, [{ n: 1 }]);
  assert.equal(recovered.recoveredTornTail, true);
  recovered.wal.close();
});

test('corruption in the middle is an error, never silently skipped', (t) => {
  const file = tmpFile(t);
  const { wal } = WriteAheadLog.open(file);
  wal.append([{ n: 1 }, { n: 2 }, { n: 3 }]);
  wal.close();
  const lines = fs.readFileSync(file, 'utf8').split('\n');
  lines[1] = lines[1].replace('"n":2', '"n":9'); // flip a byte: checksum no longer matches
  fs.writeFileSync(file, lines.join('\n'));

  assert.throws(() => WriteAheadLog.open(file), CorruptLogError);
});

test('compact atomically replaces the contents and keeps working', (t) => {
  const file = tmpFile(t);
  const { wal } = WriteAheadLog.open(file);
  wal.append(Array.from({ length: 100 }, (_, i) => ({ n: i })));
  wal.compact([{ snapshot: true }]);
  assert.equal(wal.records, 1);
  wal.append([{ n: 'after' }]);
  wal.close();
  assert.equal(fs.existsSync(`${file}.tmp`), false);

  const reopened = WriteAheadLog.open(file);
  assert.deepEqual(reopened.records, [{ snapshot: true }, { n: 'after' }]);
  reopened.wal.close();
});
