import { spawn } from 'node:child_process';

// What load/send-emails.js prints on its K6_RESULT line
export interface K6Result {
  requests: number;
  rate: number;
  failedRate: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  dropped: number;
  accepted: number;
  rejected: number;
  clientRetries: number;
}

// Runs k6 in Docker (nothing to install), streams its output, and picks out the K6_RESULT line.
// `env` sets the k6 options read by load/send-emails.js: SCENARIO, RATE, DURATION, API_URL...
// `composeFiles` are extra -f arguments for docker compose (the stress tests use one).
export function runK6(env: Record<string, string>, composeFiles: string[] = []): Promise<K6Result | null> {
  return new Promise((resolve, reject) => {
    const k6 = spawn('docker', ['compose', ...composeFiles, '--profile', 'load', 'run', '--rm', 'k6'], {
      env: { ...process.env, ...env },
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let output = '';
    k6.stdout.on('data', (chunk: Buffer) => {
      output += chunk.toString();
      process.stdout.write(chunk);
    });
    k6.on('error', reject);
    k6.on('close', () => {
      const line = output.split('\n').find((l) => l.startsWith('K6_RESULT '));
      resolve(line ? (JSON.parse(line.slice('K6_RESULT '.length)) as K6Result) : null);
    });
  });
}
