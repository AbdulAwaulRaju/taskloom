import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, timingSafeEqual } from 'node:crypto';
import { ConflictError, NotFoundError, TaskloomError, ValidationError } from './errors.js';

class HttpError extends TaskloomError {
  constructor(status, code, message) {
    super(message, code);
    this.status = status;
  }
}

const sha256 = (s) => createHash('sha256').update(s).digest();
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooLarge = false;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) tooLarge = true; // keep draining so we can still send a clean 413
      else chunks.push(chunk);
    });
    req.on('end', () => {
      if (tooLarge) return reject(new HttpError(413, 'PAYLOAD_TOO_LARGE', `body exceeds ${limit} bytes`));
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new ValidationError('request body is not valid JSON'));
      }
    });
    req.on('error', reject);
  });
}

function requireObject(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new ValidationError('request body must be a JSON object');
  }
  return body;
}

function requireToken(body) {
  if (typeof body.token !== 'string' || body.token.length === 0) {
    throw new ValidationError('token is required');
  }
  return body.token;
}

/**
 * Creates the HTTP front-end for a JobQueue.
 *
 *   POST /jobs                      enqueue
 *   POST /jobs/reserve              lease the next job (204 if none)
 *   GET  /jobs/:id                  inspect a job
 *   POST /jobs/:id/ack|nack|heartbeat
 *   GET  /dead                      dead-letter queue
 *   POST /dead/:id/requeue          retry a dead job
 *   GET  /stats | /metrics | /healthz
 */
export function createApp({ queue, authToken = null, maxBodyBytes = 1024 * 1024, tickIntervalMs = 250 } = {}) {
  if (!queue) throw new TypeError('createApp requires a queue');
  const expectedAuth = authToken ? sha256(`Bearer ${authToken}`) : null;
  let ticker = null;

  function authorize(req) {
    if (!expectedAuth) return;
    const given = sha256(req.headers.authorization ?? '');
    if (!timingSafeEqual(given, expectedAuth)) throw new HttpError(401, 'UNAUTHORIZED', 'missing or invalid bearer token');
  }

  async function route(req, url) {
    const method = req.method;
    const segs = url.pathname.split('/').filter(Boolean);

    if (method === 'GET' && segs.length === 1 && (segs[0] === 'healthz')) return { status: 200, body: { ok: true } };
    if (method === 'GET' && (url.pathname === '/' || url.pathname === '/dashboard')) {
      const html = await readFile(path.join(ROOT, 'public', 'index.html'), 'utf8');
      return { status: 200, html };
    }

    authorize(req);

    if (method === 'GET' && segs.length === 1 && segs[0] === 'stats') return { status: 200, body: queue.stats() };

    if (method === 'GET' && segs.length === 1 && segs[0] === 'jobs') {
      const state = url.searchParams.get('state');
      const limit = Number(url.searchParams.get('limit') ?? 50);
      const offset = Number(url.searchParams.get('offset') ?? 0);
      return { status: 200, body: queue.listJobs({ state, limit, offset }) };
    }

    if (method === 'GET' && segs.length === 1 && segs[0] === 'metrics') {
      const { total, walRecords, ...states } = queue.stats();
      const lines = ['# HELP taskloom_jobs Jobs currently held, by state.', '# TYPE taskloom_jobs gauge'];
      for (const [state, n] of Object.entries(states)) lines.push(`taskloom_jobs{state="${state}"} ${n}`);
      lines.push('# TYPE taskloom_wal_records gauge', `taskloom_wal_records ${walRecords}`);
      return { status: 200, text: `${lines.join('\n')}\n` };
    }

    if (method === 'GET' && segs.length === 1 && segs[0] === 'dead') {
      const limit = Math.min(Number(url.searchParams.get('limit')) || 100, 1000);
      return { status: 200, body: queue.listDead({ limit }) };
    }

    if (method === 'POST' && segs.length === 3 && segs[0] === 'dead' && segs[2] === 'requeue') {
      return { status: 200, body: queue.requeueDead(segs[1]) };
    }

    if (segs[0] === 'jobs') {
      if (method === 'POST' && segs.length === 1) {
        const { job, duplicate } = queue.enqueue(requireObject(await readBody(req, maxBodyBytes)));
        return { status: duplicate ? 200 : 201, body: job };
      }
      if (method === 'POST' && segs.length === 2 && segs[1] === 'reserve') {
        const job = queue.reserve(requireObject(await readBody(req, maxBodyBytes)));
        return job ? { status: 200, body: job } : { status: 204 };
      }
      if (method === 'GET' && segs.length === 2) {
        const job = queue.getJob(segs[1]);
        if (!job) throw new NotFoundError(`job ${segs[1]} not found`);
        return { status: 200, body: job };
      }
      if (method === 'POST' && segs.length === 3) {
        const [, id, action] = segs;
        if (action === 'ack') {
          const body = requireObject(await readBody(req, maxBodyBytes));
          queue.ack(id, requireToken(body));
          return { status: 200, body: { ok: true } };
        }
        if (action === 'nack') {
          const body = requireObject(await readBody(req, maxBodyBytes));
          queue.nack(id, requireToken(body), body.error);
          return { status: 200, body: { ok: true } };
        }
        if (action === 'heartbeat') {
          const body = requireObject(await readBody(req, maxBodyBytes));
          queue.heartbeat(id, requireToken(body), body.extendMs);
          return { status: 200, body: { ok: true } };
        }
      }
    }

    throw new HttpError(404, 'NOT_FOUND', `no route for ${method} ${url.pathname}`);
  }

  function send(res, status, headers, payload) {
    res.writeHead(status, { ...headers, 'Content-Length': Buffer.byteLength(payload) });
    res.end(payload);
  }

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://localhost');
      const result = await route(req, url);
      if (result.status === 204) {
        res.writeHead(204);
        res.end();
      } else if (result.html !== undefined) {
        send(res, result.status, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }, result.html);
      } else if (result.text !== undefined) {
        send(res, result.status, { 'Content-Type': 'text/plain; version=0.0.4' }, result.text);
      } else {
        send(res, result.status, { 'Content-Type': 'application/json' }, JSON.stringify(result.body));
      }
    } catch (err) {
      let status = 500;
      if (err instanceof HttpError) status = err.status;
      else if (err instanceof ValidationError) status = 400;
      else if (err instanceof NotFoundError) status = 404;
      else if (err instanceof ConflictError) status = 409;
      else console.error('[taskloom] unexpected error:', err);

      const code = err instanceof TaskloomError ? err.code : 'INTERNAL';
      const message = status === 500 ? 'internal server error' : err.message;
      send(res, status, { 'Content-Type': 'application/json' }, JSON.stringify({ error: { code, message } }));
    }
  });

  return {
    server,
    queue,
    /** Start listening (use port 0 for a random free port). Resolves with the bound port. */
    listen(port = 0, host = '127.0.0.1') {
      return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => {
          server.off('error', reject);
          ticker = setInterval(() => {
            try {
              queue.tick();
            } catch (err) {
              console.error('[taskloom] tick failed:', err);
            }
          }, tickIntervalMs);
          ticker.unref();
          resolve(server.address().port);
        });
      });
    },
    /** Graceful shutdown: stop accepting, finish in-flight requests, close the log. */
    close() {
      clearInterval(ticker);
      return new Promise((resolve) => {
        server.close(() => {
          queue.close();
          resolve();
        });
        server.closeIdleConnections?.();
      });
    },
  };
}
