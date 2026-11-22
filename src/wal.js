import fs from 'node:fs';
import path from 'node:path';
import { CorruptLogError } from './errors.js';

/*
 * Write-ahead log: one record per line, formatted as
 *
 *     <crc32 hex, 8 chars> <TAB> <JSON> <LF>
 *
 * - A torn final line (crash mid-write) is detected and truncated on open.
 * - A damaged line in the MIDDLE is never silently skipped: that means real
 *   corruption, so we refuse to start rather than lose data quietly.
 */

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32(buffer) {
  let c = 0xffffffff;
  for (const byte of buffer) c = CRC_TABLE[(c ^ byte) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function encodeLine(record) {
  const json = JSON.stringify(record);
  const crc = crc32(Buffer.from(json, 'utf8')).toString(16).padStart(8, '0');
  return `${crc}\t${json}\n`;
}

function encodeLines(records) {
  return Buffer.from(records.map(encodeLine).join(''), 'utf8');
}

function decodeLine(line) {
  const tab = line.indexOf('\t');
  if (tab !== 8) return null;
  const json = line.slice(tab + 1);
  const expected = crc32(Buffer.from(json, 'utf8')).toString(16).padStart(8, '0');
  if (line.slice(0, 8) !== expected) return null;
  try {
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function writeAll(fd, buffer) {
  let offset = 0;
  while (offset < buffer.length) offset += fs.writeSync(fd, buffer, offset, buffer.length - offset);
}

function readLog(filePath) {
  if (!fs.existsSync(filePath)) return { records: [], goodBytes: 0, size: 0 };
  const buf = fs.readFileSync(filePath);
  const records = [];
  let offset = 0;
  let goodBytes = 0;
  while (offset < buf.length) {
    const nl = buf.indexOf(0x0a, offset);
    if (nl === -1) break; // no newline: torn final write
    const record = decodeLine(buf.subarray(offset, nl).toString('utf8'));
    if (record === null) {
      if (nl + 1 === buf.length) break; // damaged final line: treat as torn write
      throw new CorruptLogError(`corrupt record at byte ${offset} of ${filePath}`);
    }
    records.push(record);
    offset = nl + 1;
    goodBytes = offset;
  }
  return { records, goodBytes, size: buf.length };
}

export class WriteAheadLog {
  #fd;
  #path;
  #fsync;

  /** Number of records currently in the file. */
  records = 0;

  constructor(filePath, { fsync = true } = {}) {
    this.#path = filePath;
    this.#fsync = fsync;
    this.#fd = fs.openSync(filePath, 'a');
  }

  /** Open (creating if needed), recover from a torn tail, and return all valid records. */
  static open(filePath, { fsync = true } = {}) {
    fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
    const { records, goodBytes, size } = readLog(filePath);
    const recoveredTornTail = goodBytes < size;
    if (recoveredTornTail) fs.truncateSync(filePath, goodBytes);
    const wal = new WriteAheadLog(filePath, { fsync });
    wal.records = records.length;
    return { wal, records, recoveredTornTail };
  }

  /** Append records with a single write + fsync (a tiny "group commit"). */
  append(records) {
    if (records.length === 0) return;
    writeAll(this.#fd, encodeLines(records));
    if (this.#fsync) fs.fsyncSync(this.#fd);
    this.records += records.length;
  }

  /**
   * Atomically replace the log with `records`:
   * write temp file -> fsync -> rename over the original -> fsync directory.
   * A crash at any point leaves either the complete old log or the complete new one.
   */
  compact(records) {
    const tmp = `${this.#path}.tmp`;
    const tmpFd = fs.openSync(tmp, 'w');
    try {
      writeAll(tmpFd, encodeLines(records));
      if (this.#fsync) fs.fsyncSync(tmpFd);
    } finally {
      fs.closeSync(tmpFd);
    }
    fs.closeSync(this.#fd);
    try {
      fs.renameSync(tmp, this.#path);
      this.#syncDirectory();
    } finally {
      this.#fd = fs.openSync(this.#path, 'a');
    }
    this.records = records.length;
  }

  close() {
    fs.closeSync(this.#fd);
  }

  #syncDirectory() {
    if (!this.#fsync) return;
    try {
      const dirFd = fs.openSync(path.dirname(path.resolve(this.#path)), 'r');
      try {
        fs.fsyncSync(dirFd);
      } finally {
        fs.closeSync(dirFd);
      }
    } catch {
      // Directory fsync is not supported on every platform (e.g. Windows).
    }
  }
}
