#!/usr/bin/env node
import { parseArgs } from 'node:util';
import path from 'node:path';
import { JobQueue, createApp } from '../src/index.js';

const HELP = `taskloom - durable job queue server

Usage:
  taskloom serve [options]

Options:
  --port <n>      port to listen on            (env PORT, default 8080)
  --host <addr>   address to bind              (env HOST, default 127.0.0.1)
  --data <dir>    directory for the log file   (env TASKLOOM_DATA, default ./data)
  --token <str>   require "Authorization: Bearer <str>" (env TASKLOOM_TOKEN)
  --no-fsync      skip fsync (faster, can lose the last writes on power failure)
  -h, --help      show this help
`;

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    port: { type: 'string' },
    host: { type: 'string' },
    data: { type: 'string' },
    token: { type: 'string' },
    'no-fsync': { type: 'boolean' },
    help: { type: 'boolean', short: 'h' },
  },
});

if (values.help || positionals[0] !== 'serve') {
  console.log(HELP);
  process.exit(values.help ? 0 : 1);
}

const port = Number(values.port ?? process.env.PORT ?? 8080);
const host = values.host ?? process.env.HOST ?? '127.0.0.1';
const dataDir = values.data ?? process.env.TASKLOOM_DATA ?? './data';
const authToken = values.token ?? process.env.TASKLOOM_TOKEN ?? null;

const queue = new JobQueue({ walPath: path.join(dataDir, 'queue.wal'), fsync: !values['no-fsync'] });
const app = createApp({ queue, authToken });
const boundPort = await app.listen(port, host);
console.log(`taskloom listening on http://${host}:${boundPort} (data: ${path.resolve(dataDir)})`);
if (!authToken && host !== '127.0.0.1') console.warn('warning: listening on a non-local address without --token');

let shuttingDown = false;
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n${signal} received, shutting down...`);
    await app.close();
    process.exit(0);
  });
}
