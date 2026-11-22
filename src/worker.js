import { randomUUID } from 'node:crypto';

/**
 * Runs jobs from a `source` (a JobQueue or an HttpClient) using type-keyed handlers.
 *
 *   handlers: { 'send-email': async (payload, { job, signal }) => { ... } }
 *
 * - `concurrency` independent loops each reserve one job at a time.
 * - While a handler runs, a heartbeat keeps the lease alive. If the lease is lost
 *   (e.g. we were partitioned away), `signal` is aborted so the handler can stop early.
 * - A thrown error means nack (the queue decides: retry with backoff or dead-letter).
 * - stop() is graceful: no new jobs are taken, in-flight jobs finish.
 */
export class Worker {
  #source;
  #handlers;
  #concurrency;
  #leaseMs;
  #pollIntervalMs;
  #workerId;
  #onEvent;
  #running = false;
  #loops = [];
  #sleepers = new Set();

  constructor({
    source,
    handlers,
    concurrency = 4,
    leaseMs = 30_000,
    pollIntervalMs = 250,
    workerId = `worker-${randomUUID().slice(0, 8)}`,
    onEvent = () => {},
  }) {
    this.#source = source;
    this.#handlers = handlers;
    this.#concurrency = concurrency;
    this.#leaseMs = leaseMs;
    this.#pollIntervalMs = pollIntervalMs;
    this.#workerId = workerId;
    this.#onEvent = onEvent;
  }

  get workerId() {
    return this.#workerId;
  }

  start() {
    if (this.#running) return;
    this.#running = true;
    for (let i = 0; i < this.#concurrency; i++) this.#loops.push(this.#loop());
  }

  async stop() {
    this.#running = false;
    for (const wake of [...this.#sleepers]) wake();
    await Promise.all(this.#loops);
    this.#loops = [];
  }

  #emit(event) {
    try {
      this.#onEvent({ workerId: this.#workerId, ...event });
    } catch {
      // observers must never break the worker
    }
  }

  #sleep(ms) {
    return new Promise((resolve) => {
      let timer;
      const wake = () => {
        clearTimeout(timer);
        this.#sleepers.delete(wake);
        resolve();
      };
      timer = setTimeout(wake, ms);
      this.#sleepers.add(wake);
    });
  }

  async #loop() {
    while (this.#running) {
      let job;
      try {
        job = await this.#source.reserve({ workerId: this.#workerId, leaseMs: this.#leaseMs });
      } catch (err) {
        this.#emit({ type: 'reserve_error', error: err.message });
        await this.#sleep(this.#pollIntervalMs * 4); // back off while the source is unhealthy
        continue;
      }
      if (!job) {
        await this.#sleep(this.#pollIntervalMs);
        continue;
      }
      await this.#run(job);
    }
  }

  async #run(job) {
    const handler = this.#handlers[job.type];
    if (!handler) {
      await this.#settle(() => this.#source.nack(job.id, job.token, `no handler registered for "${job.type}"`), job, 'nack');
      return;
    }

    const controller = new AbortController();
    let heartbeatInFlight = false;
    const heartbeat = setInterval(async () => {
      if (heartbeatInFlight) return;
      heartbeatInFlight = true;
      try {
        await this.#source.heartbeat(job.id, job.token, this.#leaseMs);
      } catch (err) {
        this.#emit({ type: 'lease_lost', jobId: job.id, error: err.message });
        controller.abort(new Error('lease lost'));
      } finally {
        heartbeatInFlight = false;
      }
    }, Math.max(10, Math.floor(this.#leaseMs / 3)));

    try {
      await handler(job.payload, { job, signal: controller.signal });
    } catch (err) {
      clearInterval(heartbeat);
      await this.#settle(() => this.#source.nack(job.id, job.token, err?.message ?? String(err)), job, 'nack');
      return;
    }
    clearInterval(heartbeat);
    await this.#settle(() => this.#source.ack(job.id, job.token), job, 'ack');
  }

  async #settle(action, job, kind) {
    try {
      await action();
      this.#emit({ type: kind, jobId: job.id, jobType: job.type, attempt: job.attempts });
    } catch (err) {
      // Typically a lost lease: someone else owns the job now. Safe to ignore, delivery is at-least-once.
      this.#emit({ type: `${kind}_failed`, jobId: job.id, error: err.message });
    }
  }
}
