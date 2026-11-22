# Taskloom

[![CI](https://github.com/AbdulAwaulRaju/taskloom/actions/workflows/ci.yml/badge.svg)](https://github.com/AbdulAwaulRaju/taskloom/actions/workflows/ci.yml)

A durable, **zero-dependency** job queue for Node.js, written from scratch.
Priorities, delayed jobs, visibility-timeout leases, retries with jittered exponential backoff,
a dead-letter queue, idempotent enqueue, and a checksummed write-ahead log that survives crashes.

```
npm run demo     # watch retries, priorities, delayed jobs and the dead-letter queue
npm test         # 39 tests, no dependencies to install
```

## Why this exists

Almost every backend needs background work (emails, webhooks, video processing), and
"just use a queue" hides real distributed-systems questions. Taskloom is a small, readable
implementation that answers them explicitly:

| Question | Taskloom's answer |
|---|---|
| What if a worker crashes mid-job? | Jobs are *leased*. If the lease expires the job is retried. Heartbeats extend leases for long jobs. |
| What if the *queue* crashes? | Every state change is written to a write-ahead log (fsync'd, CRC32-protected) before it is applied. Restart replays the log. |
| What if a crash tears the last write in half? | The torn tail is detected by checksum and truncated. Damage in the *middle* of the log is a hard error, never silently skipped. |
| What if a zombie worker finishes after its lease expired? | Each lease has a unique fencing token; stale `ack`/`nack` calls are rejected with `409`. |
| What if a job keeps failing? | Exponential backoff with jitter, then the dead-letter queue after `maxAttempts`. Dead jobs can be inspected and requeued. |
| What if a client retries an enqueue? | `idempotencyKey` makes enqueue safe to repeat. |
| Does the log grow forever? | No. It is atomically compacted into a snapshot (write temp, fsync, rename). |

**Delivery guarantee: at-least-once.** A job may run more than once (e.g. a worker finishes
but crashes before `ack`), so handlers should be idempotent. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

Requires Node.js 18.17+.

```bash
node bin/taskloom.js serve --port 8080 --data ./data --token my-secret
```

```bash
# enqueue
curl -X POST localhost:8080/jobs -H 'Authorization: Bearer my-secret' \
  -d '{"type":"email","payload":{"to":"ada@example.com"},"priority":5,"maxAttempts":3}'

# inspect
curl localhost:8080/stats -H 'Authorization: Bearer my-secret'
```

### Use it as a library

```js
import { JobQueue, Worker } from './src/index.js';

const queue = new JobQueue({ walPath: './data/queue.wal' });

const worker = new Worker({
  source: queue, // or an HttpClient pointing at a remote server
  concurrency: 4,
  handlers: {
    email: async ({ to }, { job, signal }) => {
      await sendEmail(to, { signal }); // `signal` aborts if the lease is lost
    },
  },
});
worker.start();

queue.enqueue({ type: 'email', payload: { to: 'ada@example.com' }, priority: 9 });

// graceful shutdown: finish in-flight jobs, then exit
await worker.stop();
queue.close();
```

## HTTP API

| Method & path | Purpose | Success |
|---|---|---|
| `POST /jobs` | Enqueue `{type, payload?, priority?, delayMs?, maxAttempts?, idempotencyKey?}` | `201` (`200` if duplicate) |
| `POST /jobs/reserve` | Lease the next job `{workerId?, leaseMs?}` | `200` + job with `token`, or `204` |
| `GET /jobs?state=all&limit=50&offset=0` | List retained jobs (no lease tokens) | `200` |
| `GET /jobs/:id` | Inspect a job | `200` |
| `POST /jobs/:id/ack` | Mark done `{token}` | `200` |
| `POST /jobs/:id/nack` | Report failure `{token, error?}` | `200` |
| `POST /jobs/:id/heartbeat` | Extend lease `{token, extendMs?}` | `200` |
| `GET /dead` | List dead-letter queue | `200` |
| `POST /dead/:id/requeue` | Retry a dead job | `200` |
| `GET /stats`, `GET /metrics` | JSON counts / Prometheus gauges | `200` |
| `GET /healthz` | Liveness (no auth) | `200` |

Errors are JSON: `{"error": {"code": "VALIDATION", "message": "..."}}` with `400`, `401`, `404`, `409`, `413`.
`priority` is `0` (lowest) to `9` (highest); equal priorities are FIFO.

## Web dashboard

Start the server and open `http://127.0.0.1:8080/` (or `/dashboard`). The built-in, dependency-free dashboard provides live queue statistics, job browsing, job creation, and dead-letter requeue controls. When bearer authentication is enabled, enter the token in the dashboard connection screen; it is held only in the current browser tab and is not persisted to local storage.

The dashboard is served by the same Node.js process. It does not require a separate frontend build or external CDN.

## Project layout

```
src/jobQueue.js      core state machine (events -> single reducer)
src/wal.js           checksummed write-ahead log, torn-write recovery, atomic compaction
src/priorityQueue.js binary min-heap
src/backoff.js       exponential backoff with jitter
src/server.js        HTTP API, auth, validation, dashboard, body limits, graceful shutdown
public/index.html    responsive operations dashboard
src/client.js        HTTP client (same interface as JobQueue)
src/worker.js        worker with concurrency, heartbeats, abort signal, graceful stop
bin/taskloom.js      CLI
test/                node:test suites (unit, crash-recovery, HTTP, worker integration)
examples/demo.js     end-to-end demo
```

## Testing philosophy

Time and randomness are injected (`clock`, `rng`), so lease expiry, backoff and delayed jobs
are tested deterministically with a fake clock instead of `sleep`. Crash recovery is tested by
reopening the log (including with deliberately torn and corrupted bytes). Worker tests run
against both an in-process queue and the real HTTP server.

## Known limitations / roadmap

- Single node. Replication (Raft or leader/follower log shipping) is the natural next step.
- All live jobs are held in memory; the log is the durability layer, not an on-disk index.
- Named queues and per-type worker routing (today any worker can reserve any job type).
- Group commit across concurrent requests to amortize `fsync`.

## License

MIT
