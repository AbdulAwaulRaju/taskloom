import { randomUUID } from 'node:crypto';
import { MinHeap } from './priorityQueue.js';
import { WriteAheadLog } from './wal.js';
import { backoffDelay } from './backoff.js';
import { ConflictError, NotFoundError, ValidationError } from './errors.js';

const STATES = ['ready', 'delayed', 'active', 'completed', 'dead'];
const MAX_PAYLOAD_BYTES = 256 * 1024;
const MAX_DELAY_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_LEASE_MS = 60 * 60 * 1000;

/*
 * Job lifecycle (see docs/ARCHITECTURE.md):
 *
 *   enqueue --> ready --reserve--> active --ack--------------> completed
 *                 ^                  |
 *                 |                  +--nack / lease expired--> delayed --(time)--> ready
 *                 |                  |                            (attempts left)
 *               requeue              +--nack / lease expired--> dead
 *                 |                                             (no attempts left)
 *                 +---------------------------------------------+
 *
 * Every state change is an immutable event. Events are written to the WAL first
 * (write-ahead) and then applied to memory by one reducer, #apply. Recovery
 * replays the same events through the same reducer, so live and recovered state
 * cannot drift apart.
 */
export class JobQueue {
  #jobs = new Map(); // id -> job
  #ready = new MinHeap((a, b) => b.priority - a.priority || a.seq - b.seq);
  #delayed = new MinHeap((a, b) => a.at - b.at || a.seq - b.seq);
  #leases = new MinHeap((a, b) => a.at - b.at);
  #keys = new Map(); // idempotencyKey -> id
  #completed = new Set(); // insertion-ordered ids, for bounded retention
  #counts = Object.fromEntries(STATES.map((s) => [s, 0]));
  #seq = 1;
  #wal = null;
  #opts;

  /**
   * @param {object}   [options]
   * @param {string}   [options.walPath]            enable durability by giving a log file path
   * @param {boolean}  [options.fsync=true]         fsync after every commit
   * @param {Function} [options.clock=Date.now]     injectable time source (for tests)
   * @param {Function} [options.rng=Math.random]    injectable randomness (for tests)
   * @param {object}   [options.backoff]            { baseMs, maxMs, factor }
   * @param {number}   [options.completedRetention=1000]  how many finished jobs stay queryable
   * @param {number}   [options.compactMinRecords=1000]   don't compact logs smaller than this
   */
  constructor({
    walPath = null,
    fsync = true,
    clock = Date.now,
    rng = Math.random,
    backoff = {},
    completedRetention = 1000,
    compactMinRecords = 1000,
  } = {}) {
    this.#opts = { clock, rng, backoff, completedRetention, compactMinRecords };
    if (walPath) {
      const { wal, records } = WriteAheadLog.open(walPath, { fsync });
      this.#wal = wal;
      for (const record of records) this.#apply(record);
      for (const job of this.#jobs.values()) this.#index(job);
    }
  }

  // ---------------------------------------------------------------- public API

  enqueue(input = {}) {
    const spec = this.#validateEnqueue(input);

    if (spec.idempotencyKey !== null) {
      const existing = this.#jobs.get(this.#keys.get(spec.idempotencyKey));
      if (existing) return { job: this.#view(existing), duplicate: true };
    }

    const now = this.#now();
    const delayed = spec.delayMs > 0;
    const job = {
      id: randomUUID(),
      seq: this.#seq,
      type: spec.type,
      payload: spec.payload,
      priority: spec.priority,
      maxAttempts: spec.maxAttempts,
      attempts: 0,
      state: delayed ? 'delayed' : 'ready',
      runAt: delayed ? now + spec.delayMs : null,
      leaseUntil: null,
      token: null,
      workerId: null,
      lastError: null,
      idempotencyKey: spec.idempotencyKey,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
    };
    const [created] = this.#commit([{ t: 'enqueue', job }]);
    return { job: this.#view(created), duplicate: false };
  }

  /** Lease the highest-priority ready job, or return null. The result includes the lease token. */
  reserve({ workerId = 'anonymous', leaseMs = 30_000 } = {}) {
    if (typeof workerId !== 'string' || workerId.length === 0 || workerId.length > 200) {
      throw new ValidationError('workerId must be a non-empty string (max 200 chars)');
    }
    this.#validateDuration(leaseMs, 'leaseMs', MAX_LEASE_MS);

    this.tick();

    let entry;
    let job;
    for (;;) {
      entry = this.#ready.pop();
      if (!entry) return null;
      job = this.#jobs.get(entry.id);
      if (job && job.state === 'ready') break; // skip stale heap entries
    }

    const now = this.#now();
    try {
      const [reserved] = this.#commit([
        { t: 'reserve', id: job.id, token: randomUUID(), leaseUntil: now + leaseMs, workerId, at: now },
      ]);
      return this.#view(reserved, { includeToken: true });
    } catch (err) {
      this.#ready.push(entry); // the WAL write failed: the job is still ready, don't lose it
      throw err;
    }
  }

  ack(id, token) {
    const job = this.#claim(id, token);
    this.#commit([{ t: 'ack', id: job.id, at: this.#now() }]);
  }

  nack(id, token, error = 'unknown error') {
    const job = this.#claim(id, token);
    this.#commit([this.#failureEvent(job, error, this.#now())]);
  }

  /** Extend a lease so a long-running job is not mistaken for a crashed one. */
  heartbeat(id, token, extendMs = 30_000) {
    this.#validateDuration(extendMs, 'extendMs', MAX_LEASE_MS);
    const job = this.#claim(id, token);
    const now = this.#now();
    this.#commit([{ t: 'heartbeat', id: job.id, leaseUntil: now + extendMs, at: now }]);
  }

  /**
   * Advance time-based transitions:
   *  - delayed jobs whose time has come become ready (derived state, not logged)
   *  - expired leases count as a failed attempt (logged)
   * Cheap to call often: it only touches heap entries that are actually due.
   */
  tick() {
    const now = this.#now();

    while (this.#delayed.size > 0 && this.#delayed.peek().at <= now) {
      const entry = this.#delayed.pop();
      const job = this.#jobs.get(entry.id);
      if (job && job.state === 'delayed' && job.runAt === entry.at) {
        this.#setState(job, 'ready');
        this.#ready.push({ id: job.id, priority: job.priority, seq: job.seq });
      }
    }

    const popped = [];
    const events = [];
    while (this.#leases.size > 0 && this.#leases.peek().at <= now) {
      const entry = this.#leases.pop();
      popped.push(entry);
      const job = this.#jobs.get(entry.id);
      // Heartbeats leave old entries behind; only the entry matching the job's current lease counts.
      if (job && job.state === 'active' && job.token === entry.token && job.leaseUntil === entry.at) {
        events.push(this.#failureEvent(job, 'lease expired', now));
      }
    }
    if (events.length > 0) {
      try {
        this.#commit(events);
      } catch (err) {
        for (const entry of popped) this.#leases.push(entry); // retry on the next tick
        throw err;
      }
    }
    return events.length;
  }

  getJob(id) {
    const job = this.#jobs.get(id);
    return job ? this.#view(job) : null;
  }

  listDead({ limit = 100 } = {}) {
    const out = [];
    for (const job of this.#jobs.values()) {
      if (job.state === 'dead') {
        out.push(this.#view(job));
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  /** List jobs without exposing lease tokens. Newest jobs are returned first. */
  listJobs({ state = null, limit = 50, offset = 0 } = {}) {
    const allowed = new Set([...STATES, 'all']);
    if (state !== null && !allowed.has(state)) throw new ValidationError('state must be ready, delayed, active, completed, dead, or all');
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) throw new ValidationError('limit must be an integer from 1 to 200');
    if (!Number.isInteger(offset) || offset < 0 || offset > 1_000_000) throw new ValidationError('offset must be an integer from 0 to 1000000');
    const jobs = [...this.#jobs.values()]
      .filter((job) => state === null || state === 'all' || job.state === state)
      .sort((a, b) => b.createdAt - a.createdAt || b.seq - a.seq);
    return { jobs: jobs.slice(offset, offset + limit).map((job) => this.#view(job)), total: jobs.length, limit, offset };
  }

  /** Give a dead job a fresh set of attempts. */
  requeueDead(id) {
    const job = this.#jobs.get(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    if (job.state !== 'dead') throw new ConflictError(`job ${id} is ${job.state}, not dead`);
    const [requeued] = this.#commit([{ t: 'requeue', id, at: this.#now() }]);
    return this.#view(requeued);
  }

  stats() {
    return { ...this.#counts, total: this.#jobs.size, walRecords: this.#wal ? this.#wal.records : 0 };
  }

  /** Rewrite the log as a compact snapshot of current state. Called automatically when the log bloats. */
  compact() {
    if (!this.#wal) return false;
    const completed = [...this.#completed].map((id) => this.#jobs.get(id));
    const others = [...this.#jobs.values()].filter((j) => j.state !== 'completed').sort((a, b) => a.seq - b.seq);
    this.#wal.compact([...completed, ...others].map((job) => ({ t: 'snapshot', job })));
    return true;
  }

  close() {
    if (this.#wal) this.#wal.close();
    this.#wal = null;
  }

  // ------------------------------------------------------------------ internals

  #now() {
    return this.#opts.clock();
  }

  #setState(job, state) {
    if (job.state) this.#counts[job.state] -= 1;
    job.state = state;
    this.#counts[state] += 1;
  }

  /** The single place where events change state. Used for both live commits and log replay. */
  #apply(e) {
    switch (e.t) {
      case 'enqueue':
      case 'snapshot': {
        const job = { ...e.job };
        const state = job.state;
        job.state = null;
        this.#jobs.set(job.id, job);
        this.#setState(job, state);
        if (job.idempotencyKey) this.#keys.set(job.idempotencyKey, job.id);
        if (state === 'completed') this.#completed.add(job.id);
        this.#seq = Math.max(this.#seq, job.seq + 1);
        return job;
      }
      case 'reserve': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        this.#setState(job, 'active');
        job.attempts += 1;
        job.token = e.token;
        job.leaseUntil = e.leaseUntil;
        job.workerId = e.workerId;
        job.updatedAt = e.at;
        return job;
      }
      case 'heartbeat': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        job.leaseUntil = e.leaseUntil;
        job.updatedAt = e.at;
        return job;
      }
      case 'ack': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        this.#setState(job, 'completed');
        job.token = null;
        job.leaseUntil = null;
        job.completedAt = e.at;
        job.updatedAt = e.at;
        this.#completed.add(job.id);
        this.#evictCompleted();
        return job;
      }
      case 'retry': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        this.#setState(job, 'delayed');
        job.runAt = e.runAt;
        job.lastError = e.error;
        job.token = null;
        job.leaseUntil = null;
        job.updatedAt = e.at;
        return job;
      }
      case 'dead': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        this.#setState(job, 'dead');
        job.lastError = e.error;
        job.token = null;
        job.leaseUntil = null;
        job.updatedAt = e.at;
        return job;
      }
      case 'requeue': {
        const job = this.#jobs.get(e.id);
        if (!job) return null;
        this.#setState(job, 'ready');
        job.attempts = 0;
        job.runAt = null;
        job.updatedAt = e.at;
        return job;
      }
      default:
        return null; // unknown event types are ignored (forward compatibility)
    }
  }

  /** Put a job into the heap that matches its state. */
  #index(job) {
    if (job.state === 'ready') this.#ready.push({ id: job.id, priority: job.priority, seq: job.seq });
    else if (job.state === 'delayed') this.#delayed.push({ id: job.id, at: job.runAt, seq: job.seq });
    else if (job.state === 'active') this.#leases.push({ id: job.id, at: job.leaseUntil, token: job.token });
  }

  /** Write-ahead: persist first, then change memory. If the write throws, memory is untouched. */
  #commit(events) {
    if (this.#wal) this.#wal.append(events);
    const touched = [];
    for (const event of events) {
      const job = this.#apply(event);
      if (job) {
        this.#index(job);
        touched.push(job);
      }
    }
    this.#maybeCompact();
    return touched;
  }

  #maybeCompact() {
    if (!this.#wal) return;
    const records = this.#wal.records;
    if (records >= this.#opts.compactMinRecords && records > 4 * Math.max(1, this.#jobs.size)) this.compact();
  }

  #evictCompleted() {
    while (this.#completed.size > this.#opts.completedRetention) {
      const id = this.#completed.values().next().value;
      this.#completed.delete(id);
      const job = this.#jobs.get(id);
      if (!job) continue;
      this.#counts.completed -= 1;
      this.#jobs.delete(id);
      if (job.idempotencyKey && this.#keys.get(job.idempotencyKey) === id) this.#keys.delete(job.idempotencyKey);
    }
  }

  #claim(id, token) {
    const job = this.#jobs.get(id);
    if (!job) throw new NotFoundError(`job ${id} not found`);
    if (job.state !== 'active' || job.token !== token) {
      throw new ConflictError(`lease for job ${id} is not held by this caller`);
    }
    return job;
  }

  #failureEvent(job, error, now) {
    const message = String(error ?? 'unknown error').slice(0, 1000);
    if (job.attempts >= job.maxAttempts) return { t: 'dead', id: job.id, error: message, at: now };
    const delay = backoffDelay(job.attempts, { ...this.#opts.backoff, rng: this.#opts.rng });
    return { t: 'retry', id: job.id, runAt: now + delay, error: message, at: now };
  }

  #view(job, { includeToken = false } = {}) {
    const { token, ...rest } = job;
    return structuredClone(includeToken ? { ...rest, token } : rest);
  }

  #validateEnqueue({ type, payload = null, priority = 0, delayMs = 0, maxAttempts = 5, idempotencyKey = null }) {
    if (typeof type !== 'string' || type.length === 0 || type.length > 200) {
      throw new ValidationError('type must be a non-empty string (max 200 chars)');
    }
    if (!Number.isInteger(priority) || priority < 0 || priority > 9) {
      throw new ValidationError('priority must be an integer from 0 (lowest) to 9 (highest)');
    }
    if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > MAX_DELAY_MS) {
      throw new ValidationError('delayMs must be between 0 and 30 days');
    }
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 100) {
      throw new ValidationError('maxAttempts must be an integer from 1 to 100');
    }
    if (idempotencyKey !== null && (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0 || idempotencyKey.length > 200)) {
      throw new ValidationError('idempotencyKey must be a non-empty string (max 200 chars)');
    }
    let json;
    try {
      json = JSON.stringify(payload);
    } catch {
      throw new ValidationError('payload must be JSON-serializable');
    }
    if (json === undefined) json = 'null';
    if (Buffer.byteLength(json) > MAX_PAYLOAD_BYTES) {
      throw new ValidationError(`payload exceeds ${MAX_PAYLOAD_BYTES} bytes`);
    }
    // Round-trip so what we keep in memory is exactly what a log replay would produce.
    return { type, payload: JSON.parse(json), priority, delayMs, maxAttempts, idempotencyKey };
  }

  #validateDuration(value, name, max) {
    if (!Number.isInteger(value) || value < 1 || value > max) {
      throw new ValidationError(`${name} must be an integer from 1 to ${max}`);
    }
  }
}
