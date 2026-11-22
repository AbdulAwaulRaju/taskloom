import { TaskloomError } from './errors.js';

export class ClientError extends TaskloomError {
  constructor(message, status, code) {
    super(message, code ?? 'HTTP_ERROR');
    this.status = status;
  }
}

/**
 * HTTP client with the same method names as JobQueue, so a Worker can use
 * either an in-process queue or a remote server interchangeably.
 */
export class HttpClient {
  #baseUrl;
  #authToken;
  #timeoutMs;

  constructor({ baseUrl, authToken = null, timeoutMs = 10_000 }) {
    this.#baseUrl = baseUrl.replace(/\/+$/, '');
    this.#authToken = authToken;
    this.#timeoutMs = timeoutMs;
  }

  async #request(method, path, body) {
    const headers = {};
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (this.#authToken) headers.Authorization = `Bearer ${this.#authToken}`;
    const res = await fetch(`${this.#baseUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(this.#timeoutMs),
    });
    if (res.status === 204) return { status: 204, data: null };
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) throw new ClientError(data?.error?.message ?? res.statusText, res.status, data?.error?.code);
    return { status: res.status, data };
  }

  async enqueue(input) {
    const { status, data } = await this.#request('POST', '/jobs', input);
    return { job: data, duplicate: status === 200 };
  }

  async reserve({ workerId, leaseMs } = {}) {
    return (await this.#request('POST', '/jobs/reserve', { workerId, leaseMs })).data;
  }

  async ack(id, token) {
    await this.#request('POST', `/jobs/${encodeURIComponent(id)}/ack`, { token });
  }

  async nack(id, token, error) {
    await this.#request('POST', `/jobs/${encodeURIComponent(id)}/nack`, { token, error });
  }

  async heartbeat(id, token, extendMs) {
    await this.#request('POST', `/jobs/${encodeURIComponent(id)}/heartbeat`, { token, extendMs });
  }

  async getJob(id) {
    try {
      return (await this.#request('GET', `/jobs/${encodeURIComponent(id)}`)).data;
    } catch (err) {
      if (err.status === 404) return null;
      throw err;
    }
  }

  async stats() {
    return (await this.#request('GET', '/stats')).data;
  }

  async listDead({ limit = 100 } = {}) {
    return (await this.#request('GET', `/dead?limit=${limit}`)).data;
  }

  async requeueDead(id) {
    return (await this.#request('POST', `/dead/${encodeURIComponent(id)}/requeue`, {})).data;
  }
}
