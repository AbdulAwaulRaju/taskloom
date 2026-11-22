
import { HttpClient, Worker } from './src/index.js';

const client = new HttpClient({
  baseUrl: 'http://127.0.0.1:8080'
});

const worker = new Worker({
  source: client,
  concurrency: 2,
  leaseMs: 10000,
  pollIntervalMs: 500,

  handlers: {
    'test-email': async (payload, { job }) => {
      console.log('\n----------------------------');
      console.log('JOB RECEIVED BY WORKER');
      console.log('Job ID:', job.id);
      console.log('Job Type:', job.type);
      console.log('Email:', payload.to);
      console.log('Subject:', payload.subject);
      console.log('Processing email...');

      await new Promise(resolve => setTimeout(resolve, 2000));

      console.log('Email processing successful!');
      console.log('----------------------------\n');
    },

    'test-fail': async (payload, { job }) => {
      console.log('\n----------------------------');
      console.log('JOB FAILURE TEST');
      console.log('Job ID:', job.id);
      console.log('Job Type:', job.type);
      console.log('Attempt:', job.attempts);
      console.log('Simulating failure...');

      throw new Error('Intentional failure for retry testing');
    }
  },

  onEvent: (event) => {
    console.log(
      '[WORKER EVENT]',
      event.type,
      event.jobId || '',
      event.error || ''
    );
  }
});

console.log('Taskloom Worker is starting...');
console.log('Worker ID:', worker.workerId);

worker.start();

process.on('SIGINT', async () => {
  console.log('\nStopping worker...');
  await worker.stop();
  console.log('Worker stopped safely.');
  process.exit(0);
});

