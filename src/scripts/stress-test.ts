/**
 * Stress tests. One command each; the script starts everything itself:
 *
 *   npm run stress -- steady   RATE emails/s for DURATION         (default 1000/s for 5m)
 *   npm run stress -- spike    BASE_RATE, a burst of PEAK_RATE, back to BASE_RATE (200 → 2000 → 200)
 *   npm run stress -- chaos    steady load while we crash things  (default 1000/s for 4m):
 *                              kill -9 a worker, kill -9 the relay, kill RabbitMQ, kill -9 an API process
 *   npm run stress -- soak     a lower rate for a long time, watching memory (default 300/s for 30m)
 *
 * What it does:
 *   1. starts Postgres, RabbitMQ and Redis (Docker), and builds the app
 *   2. uses its OWN database (relay_load) and RabbitMQ vhost (load-test), wiped each run,
 *      so it never touches your dev data, even if `npm run dev` is running
 *   3. starts several API processes, the relay and the workers, and restarts any that die
 *      (like Kubernetes would), with the mock provider recording every email it gets
 *   4. runs k6, and every couple of seconds records accepted/s, delivered/s, backlog, memory
 *   5. waits for every email to be delivered, then checks: was anything lost? sent twice?
 *   6. writes load-results/<time>-<scenario>/report.md (plus timeline.csv and process logs)
 *
 * Any setting can be changed with an environment variable, e.g.
 *   RATE=1500 DURATION=10m WORKERS=3 npm run stress -- steady
 */
// Keep this import FIRST: it copies the shell's variables before config loads .env (see the file)
import { shellEnv } from './shell-env';
import { ChildProcess, execFile, spawn } from 'node:child_process';
import { appendFileSync, closeSync, mkdirSync, openSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { Pool } from 'pg';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { runMigrations } from '../shared/migrations';
import { createRedis, waitForRedis } from '../shared/rate-limiter';
import { RedisSendRecorder } from '../worker/mock-provider';
import { K6Result, runK6 } from './k6';

const run = promisify(execFile);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------- settings

type Scenario = 'steady' | 'spike' | 'chaos' | 'soak';
const SCENARIOS: Record<Scenario, Record<string, string>> = {
  steady: { RATE: '1000', DURATION: '5m' },
  spike: { BASE_RATE: '200', PEAK_RATE: '2000', DURATION: '140s' }, // the k6 stages add up to 140s
  chaos: { RATE: '1000', DURATION: '4m' },
  soak: { RATE: '300', DURATION: '30m' },
};

const scenario = process.argv[2] as Scenario;
if (!(scenario in SCENARIOS)) {
  console.error(`Usage: npm run stress -- <${Object.keys(SCENARIOS).join('|')}>`);
  process.exit(1);
}
const setting = (name: string, fallback?: string): string =>
  shellEnv[name] ?? SCENARIOS[scenario][name] ?? fallback ?? '';

const cores = os.availableParallelism();
const RATE = Number(setting('RATE', '0'));
const BASE_RATE = Number(setting('BASE_RATE', '0'));
const PEAK_RATE = Number(setting('PEAK_RATE', '0'));
const DURATION = scenario === 'spike' ? SCENARIOS.spike.DURATION : setting('DURATION');
const DURATION_S = parseDuration(DURATION);
// Rough split of the machine: k6, Postgres and RabbitMQ need cores too
const API_PROCESSES = Number(setting('API_PROCESSES', String(Math.max(1, Math.floor(cores / 2)))));
const WORKERS = Number(setting('WORKERS', String(Math.max(1, Math.floor(cores / 4)))));
const RELAYS = Number(setting('RELAYS', '1'));
const SAMPLE_S = Number(setting('SAMPLE_S', scenario === 'soak' ? '10' : '2'));
const DRAIN_TIMEOUT_S = Number(setting('DRAIN_TIMEOUT_S', '600'));
const API_PORT = setting('STRESS_API_PORT', '3100');
const API_URL = `http://localhost:${API_PORT}`;
const VHOST = 'load-test';
const LOAD_DB = 'relay_load';

const appSettings = {
  API_PROCESSES: String(API_PROCESSES),
  DB_POOL_SIZE: setting('DB_POOL_SIZE', '20'),
  WORKER_PREFETCH: setting('WORKER_PREFETCH', '100'),
  RELAY_BATCH_SIZE: setting('RELAY_BATCH_SIZE', '500'),
  RELAY_POLL_INTERVAL_MS: setting('RELAY_POLL_INTERVAL_MS', '100'),
  // Short delays and lease, so emails stuck by a crash are picked up in seconds, not minutes
  RETRY_DELAYS_MS: setting('RETRY_DELAYS_MS', '1000,2000,4000'),
  SENDING_LEASE_MS: setting('SENDING_LEASE_MS', '10000'),
  MOCK_LATENCY_MS: setting('MOCK_LATENCY_MS', '50'),
  MOCK_FAILURE_RATE: setting('MOCK_FAILURE_RATE', '0'),
  PROVIDER_RATE_LIMIT_PER_SEC: setting('PROVIDER_RATE_LIMIT_PER_SEC', '0'),
};

const rabbitUrl = new URL(config.RABBITMQ_URL);
const rabbitAuth = `Basic ${Buffer.from(`${decodeURIComponent(rabbitUrl.username)}:${decodeURIComponent(rabbitUrl.password)}`).toString('base64')}`;
const MANAGEMENT_URL = setting('RABBITMQ_MANAGEMENT_URL', `http://${rabbitUrl.hostname}:15672`);
const loadDbUrl = withPath(config.DATABASE_URL, `/${LOAD_DB}`);

const childEnv: NodeJS.ProcessEnv = {
  ...process.env,
  ...appSettings,
  PORT: API_PORT,
  DATABASE_URL: loadDbUrl,
  RABBITMQ_URL: withPath(config.RABBITMQ_URL, `/${VHOST}`),
  REDIS_URL: config.REDIS_URL,
  EMAIL_PROVIDER: 'mock',
  MOCK_RECORD_SENDS: 'true',
  LOG_LEVEL: 'warn',
};

const startedAt = new Date();
const stamp = startedAt.toISOString().slice(0, 16).replace('T', '-').replace(':', '');
const outDir = path.join('load-results', `${stamp}-${scenario}`);
const logDir = path.join(outDir, 'logs');

// ---------------------------------------------------------------- processes

const RESTART_DELAY_MS = 1000;

// A process we keep alive: if it dies, start it again after a second, like Kubernetes would.
class Supervised {
  child?: ChildProcess;
  restarts = 0;
  private stopping = false;

  constructor(
    readonly name: string,
    private script: string,
  ) {}

  start() {
    const log = openSync(path.join(logDir, `${this.name}.log`), 'a');
    this.child = spawn(process.execPath, [this.script], { env: childEnv, stdio: ['ignore', log, log] });
    closeSync(log); // the child has its own copy
    this.child.on('exit', () => {
      if (this.stopping) return;
      this.restarts++;
      setTimeout(() => !this.stopping && this.start(), RESTART_DELAY_MS);
    });
  }

  get running() {
    return !!this.child && this.child.exitCode === null && this.child.signalCode === null;
  }

  kill9() {
    this.child?.kill('SIGKILL');
  }

  async stop() {
    this.stopping = true;
    if (!this.running) return;
    const exited = new Promise((resolve) => this.child!.once('exit', resolve));
    this.child!.kill('SIGTERM');
    if (!(await Promise.race([exited.then(() => true), sleep(15_000).then(() => false)]))) this.kill9();
  }
}

const api = new Supervised('api', 'dist/api/server.js');
const relays = Array.from({ length: RELAYS }, (_, i) => new Supervised(`relay-${i + 1}`, 'dist/relay/main.js'));
const workers = Array.from({ length: WORKERS }, (_, i) => new Supervised(`worker-${i + 1}`, 'dist/worker/main.js'));
const allProcesses = [api, ...relays, ...workers];

// ---------------------------------------------------------------- setup

async function sh(cmd: string, args: string[]) {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: 'inherit' });
    child.on('error', reject);
    child.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} ${args.join(' ')} exited with ${code}`)),
    );
  });
}

async function rabbitApi(method: string, apiPath: string, body?: object) {
  return fetch(`${MANAGEMENT_URL}/api${apiPath}`, {
    method,
    headers: { Authorization: rabbitAuth, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(3000),
  });
}

// A fresh, empty RabbitMQ "virtual host": its own set of queues, separate from the dev ones
async function resetVhost() {
  const vhost = encodeURIComponent(VHOST);
  for (let attempt = 1; ; attempt++) {
    try {
      await rabbitApi('DELETE', `/vhosts/${vhost}`);
      const created = await rabbitApi('PUT', `/vhosts/${vhost}`);
      if (!created.ok) throw new Error(`creating the vhost: HTTP ${created.status}`);
      const user = encodeURIComponent(decodeURIComponent(rabbitUrl.username));
      await rabbitApi('PUT', `/permissions/${vhost}/${user}`, { configure: '.*', write: '.*', read: '.*' });
      return;
    } catch (err) {
      if (attempt >= 30) throw err; // the management plugin starts a little after RabbitMQ itself
      await sleep(1000);
    }
  }
}

async function resetDatabase() {
  const admin = createPool(config.DATABASE_URL, 1);
  const { rowCount } = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [LOAD_DB]);
  if (!rowCount) await admin.query(`CREATE DATABASE ${LOAD_DB}`);
  await admin.end();

  const pool = createPool(loadDbUrl, 4);
  await runMigrations(pool);
  await pool.query('TRUNCATE emails, outbox RESTART IDENTITY');
  return pool;
}

async function waitFor(what: string, check: () => Promise<boolean>, timeoutS = 60) {
  const deadline = Date.now() + timeoutS * 1000;
  while (Date.now() < deadline) {
    if (await check().catch(() => false)) return;
    await sleep(500);
  }
  throw new Error(`Timed out waiting for ${what} (see ${logDir})`);
}

async function consumers(): Promise<number> {
  const res = await rabbitApi('GET', `/queues/${encodeURIComponent(VHOST)}/emails.transactional`);
  return res.ok ? ((await res.json()) as { consumers: number }).consumers : 0;
}

// ---------------------------------------------------------------- measuring

interface Sample {
  t: number; // seconds since k6 started (negative = before)
  accepted: number;
  delivered: number;
  outbox: number;
  rabbit: number | null; // messages in the main and retry queues
  rssMb: Record<string, number>; // memory per kind of process
}

const samples: Sample[] = [];
// measure = which rate a chaos event is expected to hit, so the report can show its effect
type Measure = 'accepted' | 'delivered';
const events: { t: number; text: string; measure?: Measure }[] = [];
let k6StartedAt = 0;
const now = () => (k6StartedAt ? (Date.now() - k6StartedAt) / 1000 : 0);

function note(text: string, measure?: Measure) {
  events.push({ t: now(), text, measure });
  console.log(`\n>>> [${fmtTime(now())}] ${text}\n`);
}

async function rabbitBacklog(): Promise<number | null> {
  try {
    const res = await rabbitApi('GET', `/queues/${encodeURIComponent(VHOST)}?columns=name,messages`);
    if (!res.ok) return null;
    const queues = (await res.json()) as { name: string; messages?: number }[];
    return queues.filter((q) => !q.name.endsWith('dead-letter')).reduce((sum, q) => sum + (q.messages ?? 0), 0);
  } catch {
    return null; // RabbitMQ is down (that's the chaos test)
  }
}

async function memory(): Promise<Record<string, number>> {
  try {
    const groups: Record<string, number[]> = { api: [], relay: [], workers: [] };
    if (api.child?.pid) {
      groups.api.push(api.child.pid);
      const { stdout } = await run('pgrep', ['-P', String(api.child.pid)]).catch(() => ({ stdout: '' }));
      groups.api.push(...stdout.split('\n').filter(Boolean).map(Number));
    }
    for (const r of relays) if (r.child?.pid && r.running) groups.relay.push(r.child.pid);
    for (const w of workers) if (w.child?.pid && w.running) groups.workers.push(w.child.pid);

    const pids = Object.values(groups).flat();
    const { stdout } = await run('ps', ['-o', 'pid=,rss=', '-p', pids.join(',')]);
    const rss = new Map(
      stdout
        .trim()
        .split('\n')
        .map((line) => line.trim().split(/\s+/).map(Number) as [number, number]),
    );
    const result: Record<string, number> = {};
    for (const [group, list] of Object.entries(groups)) {
      result[group] = Math.round(list.reduce((sum, pid) => sum + (rss.get(pid) ?? 0), 0) / 1024);
    }
    return result;
  } catch {
    return {};
  }
}

function startSampler(pool: Pool, redis: ReturnType<typeof createRedis>) {
  let stopped = false;
  const loop = (async () => {
    while (!stopped) {
      sampleCpu();
      const [acceptedRes, outboxRes, delivered, rabbit, rssMb] = await Promise.all([
        pool.query<{ n: string }>('SELECT coalesce(max(id), 0) AS n FROM outbox'), // ids count up from 1
        pool.query<{ n: number }>('SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL'),
        redis.scard(RedisSendRecorder.SENT_KEY),
        rabbitBacklog(),
        memory(),
      ]).catch(() => [null, null, 0, null, {}] as const);
      if (acceptedRes && outboxRes) {
        const s: Sample = {
          t: now(),
          accepted: Number(acceptedRes.rows[0].n),
          delivered: Number(delivered),
          outbox: outboxRes.rows[0].n,
          rabbit,
          rssMb,
        };
        const prev = samples.at(-1);
        samples.push(s);
        if (prev) {
          const dt = s.t - prev.t || SAMPLE_S;
          console.log(
            `[${fmtTime(s.t)}] accepted ${pad((s.accepted - prev.accepted) / dt)}/s | delivered ${pad((s.delivered - prev.delivered) / dt)}/s | ` +
              `waiting: outbox ${s.outbox}, RabbitMQ ${s.rabbit ?? 'down'}`,
          );
        }
      }
      await sleep(SAMPLE_S * 1000);
    }
  })();
  return async () => {
    stopped = true;
    await loop;
  };
}

// ---------------------------------------------------------------- CPU (Linux only)

// How much CPU each part used during the load: is one part the bottleneck, or is the whole
// machine full? Every sample reads each process's CPU counter (/proc/<pid>/stat) and adds
// what it used since the previous sample to its part. Sampling all along (not just at the
// start and end) also counts processes that exit before the end, like k6.

let clockTicks = 100; // CPU counters are in "ticks" (usually 100 per second)
const cpuByPart = new Map<string, number>(); // ticks
const otherByName = new Map<string, number>(); // what's inside "Other", by process name
let lastTicks = new Map<number, { comm: string; ticks: number; startedAt: number }>();
let machineStart: { busy: number; total: number } | null = null;
let machineEnd: { busy: number; total: number } | null = null;
let cpuStartedAt = 0;
let cpuSeconds = 0;

function machineTicks() {
  // first line of /proc/stat: cpu  user nice system idle iowait irq softirq steal
  const [user, nice, system, idle, iowait, irq, softirq, steal] = readFileSync('/proc/stat', 'utf8')
    .split('\n')[0]
    .trim()
    .split(/\s+/)
    .slice(1)
    .map(Number);
  const busy = user + nice + system + irq + softirq;
  return { busy, total: busy + idle + iowait + steal };
}

function processTicks() {
  const result = new Map<number, { comm: string; ticks: number; startedAt: number }>();
  for (const entry of readdirSync('/proc')) {
    if (!/^\d+$/.test(entry)) continue;
    try {
      const stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
      const comm = stat.slice(stat.indexOf('(') + 1, stat.lastIndexOf(')'));
      const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' '); // fields[0] is field 3 (state)
      // utime + stime = CPU used so far. starttime tells a reused pid apart from the old process.
      result.set(Number(entry), {
        comm,
        ticks: Number(fields[11]) + Number(fields[12]),
        startedAt: Number(fields[19]),
      });
    } catch {
      // the process exited while we were reading
    }
  }
  return result;
}

function part(pid: number, comm: string): string {
  if (comm === 'node') {
    let cmdline = '';
    try {
      cmdline = readFileSync(`/proc/${pid}/cmdline`, 'utf8');
    } catch {
      // exited
    }
    if (cmdline.includes('dist/api/server.js')) return 'API';
    if (cmdline.includes('dist/relay/main.js')) return 'Relay';
    if (cmdline.includes('dist/worker/main.js')) return 'Workers';
    if (pid === process.pid) return 'This test script';
  }
  if (comm === 'postgres') return 'Postgres';
  if (['beam.smp', 'erl_child_setup', 'epmd', 'inet_gethost'].includes(comm)) return 'RabbitMQ';
  if (comm === 'k6') return 'k6 (the load generator)';
  if (comm === 'redis-server') return 'Redis';
  if (comm === 'docker-proxy') return "Docker's port forwarding (docker-proxy)";
  if (/^(dockerd|containerd)/.test(comm)) return 'Docker';
  // Kernel threads: mostly writing data to disk (Postgres and RabbitMQ save every commit) and networking
  if (/^(kworker|ksoftirqd|jbd2|kswapd|rcu_|migration|irq\/)/.test(comm))
    return 'Linux kernel (disk writes, networking)';
  return 'Other';
}

async function startCpu() {
  try {
    clockTicks = Number((await run('getconf', ['CLK_TCK'])).stdout.trim()) || 100;
    machineStart = machineTicks();
    lastTicks = processTicks();
    cpuStartedAt = Date.now();
  } catch {
    machineStart = null; // not Linux (e.g. a Mac): no CPU table
  }
}

function sampleCpu() {
  if (!machineStart || machineEnd) return;
  const now = processTicks();
  for (const [pid, { comm, ticks, startedAt }] of now) {
    const before = lastTicks.get(pid);
    // A process that started since the last sample: count all of its CPU time
    const used = before && before.startedAt === startedAt ? ticks - before.ticks : ticks;
    if (used <= 0) continue;
    const name = part(pid, comm);
    cpuByPart.set(name, (cpuByPart.get(name) ?? 0) + used);
    if (name === 'Other') otherByName.set(comm, (otherByName.get(comm) ?? 0) + used);
  }
  lastTicks = now;
}

function stopCpu() {
  if (!machineStart) return;
  sampleCpu();
  machineEnd = machineTicks();
  cpuSeconds = (Date.now() - cpuStartedAt) / 1000;
}

// e.g. "ksoftirqd/0 0.10, tsx 0.05": the biggest processes inside "Other"
function topOthers() {
  return [...otherByName.entries()]
    .filter(([, ticks]) => ticks / clockTicks / cpuSeconds >= 0.01)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4)
    .map(([name, ticks]) => `${name} ${(ticks / clockTicks / cpuSeconds).toFixed(2)}`)
    .join(', ');
}

function cpuReport(): string[] {
  if (!machineStart || !machineEnd || cpuSeconds <= 0) return [];
  const busyShare = (machineEnd.busy - machineStart.busy) / (machineEnd.total - machineStart.total);
  // The kernel's interrupt handling (e.g. network packets) isn't charged to any process
  const charged = [...cpuByPart.values()].reduce((sum, ticks) => sum + ticks, 0) / clockTicks / cpuSeconds;
  const uncharged = busyShare * cores - charged;
  if (uncharged >= 0.01) cpuByPart.set('Kernel interrupts (network, disk)', uncharged * clockTicks * cpuSeconds);
  return [
    '### CPU during the load',
    '',
    `The whole machine was **${Math.round(busyShare * 100)}% busy** (${(busyShare * cores).toFixed(1)} of ${cores} cores).`,
    '',
    '| Part | CPU cores used |',
    '|---|---|',
    ...[...cpuByPart.entries()]
      .map(([name, ticks]) => [name, ticks / clockTicks / cpuSeconds] as const)
      .filter(([, used]) => used >= 0.01)
      .sort((a, b) => b[1] - a[1])
      .map(
        ([name, used]) =>
          `| ${name === 'Other' && topOthers() ? `Other (${topOthers()})` : name} | ${used.toFixed(2)} |`,
      ),
    '',
  ];
}

// ---------------------------------------------------------------- chaos

let rabbitKilled = false;

async function killOneApiProcess() {
  if (API_PROCESSES === 1) return api.kill9(); // our supervisor restarts it
  const { stdout } = await run('pgrep', ['-P', String(api.child!.pid)]);
  const pid = Number(stdout.split('\n')[0]); // the cluster primary starts a replacement
  process.kill(pid, 'SIGKILL');
}

const chaosPlan: { at: number; text: string; measure: Measure; action: () => Promise<void> | void }[] = [
  { at: 0.2, text: 'kill -9 worker-1', measure: 'delivered', action: () => workers[0].kill9() },
  { at: 0.4, text: 'kill -9 the outbox relay', measure: 'delivered', action: () => relays[0].kill9() },
  {
    at: 0.6,
    text: 'killed RabbitMQ (docker kill), restarting it in 10s',
    measure: 'delivered',
    action: async () => {
      rabbitKilled = true;
      await run('docker', ['compose', 'kill', 'rabbitmq']);
      await sleep(10_000);
      await run('docker', ['compose', 'start', 'rabbitmq']);
      rabbitKilled = false;
      note('RabbitMQ started again');
    },
  },
  { at: 0.8, text: 'kill -9 one API process', measure: 'accepted', action: killOneApiProcess },
];

function scheduleChaos(): Promise<unknown> {
  return Promise.all(
    chaosPlan.map(
      ({ at, text, measure, action }) =>
        new Promise<void>((resolve) =>
          setTimeout(
            async () => {
              note(text, measure);
              await Promise.resolve(action()).catch((err) => note(`chaos step failed: ${(err as Error).message}`));
              resolve();
            },
            at * DURATION_S * 1000,
          ),
        ),
    ),
  );
}

// ---------------------------------------------------------------- after the load

async function waitForDrain(pool: Pool): Promise<number | null> {
  const loadEnded = Date.now();
  const deadline = loadEnded + DRAIN_TIMEOUT_S * 1000;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{ pending: number; outbox: number }>(`
      SELECT (SELECT count(*)::int FROM emails WHERE status IN ('queued', 'sending')) AS pending,
             (SELECT count(*)::int FROM outbox WHERE published_at IS NULL) AS outbox`);
    if (rows[0].pending === 0 && rows[0].outbox === 0) return (Date.now() - loadEnded) / 1000;
    await sleep(1000);
  }
  return null;
}

async function results(pool: Pool, redis: ReturnType<typeof createRedis>) {
  const { rows } = await pool.query(`
    SELECT count(*)::int                                        AS total,
           count(*) FILTER (WHERE status = 'sent')::int         AS sent,
           count(*) FILTER (WHERE status = 'failed')::int       AS failed,
           count(*) FILTER (WHERE status IN ('queued', 'sending'))::int AS pending,
           count(*) FILTER (WHERE attempts > 1)::int            AS retried,
           extract(epoch FROM max(sent_at) - min(created_at))   AS span_s,
           percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
             FILTER (WHERE status = 'sent') AS e2e
      FROM emails`);
  const [deliveredUnique, duplicates] = await Promise.all([
    redis.scard(RedisSendRecorder.SENT_KEY),
    redis.get(RedisSendRecorder.DUPLICATES_KEY),
  ]);
  return { ...rows[0], deliveredUnique, duplicates: Number(duplicates ?? 0) };
}

// For each chaos event: how far did the rate it hits drop, and how long until it was back to normal?
// Only looks until the next chaos event, so one event's effect isn't blamed on another.
function chaosEffect(index: number): string {
  const event = events[index];
  if (!event.measure || !RATE) return '';
  const nextEvent = events.slice(index + 1).find((e) => e.measure);
  const end = Math.min(DURATION_S, nextEvent?.t ?? Infinity);
  const field = event.measure;
  const rates = samples
    .map((s, i) => (i === 0 ? null : { t: s.t, rate: (s[field] - samples[i - 1][field]) / (s.t - samples[i - 1].t) }))
    .filter((r): r is { t: number; rate: number } => !!r && r.t > event.t && r.t <= end);
  if (!rates.length) return '';
  const what = field === 'delivered' ? 'delivery' : 'emails accepted by the API';
  const low = rates.reduce((a, b) => (b.rate < a.rate ? b : a));
  if (low.rate >= 0.9 * RATE) return `no visible drop in ${what} (lowest ${Math.round(low.rate)}/s)`;
  const back = rates.find((r) => r.t > low.t && r.rate >= 0.9 * RATE);
  return `${what} dropped to ${Math.round(low.rate)}/s, ${back ? `back to normal ${Math.round(back.t - event.t)}s after` : 'not back to normal before the next event'}`;
}

function report(k6: K6Result | null, d: Awaited<ReturnType<typeof results>>, drainS: number | null, cpu: string[]) {
  const e2e: number[] = d.e2e ?? [];
  const k6Accepted = k6?.accepted ?? d.total;
  // Accepted by the API (202) but not saved, or saved but never delivered or dead-lettered
  const lost = Math.max(0, k6Accepted - d.total) + Math.max(0, d.total - d.deliveredUnique - d.failed);
  const spanS = Number(d.span_s) || 0;
  const rates = samples.slice(1).map((s, i) => (s.delivered - samples[i].delivered) / (s.t - samples[i].t));
  const peakDelivery = Math.max(0, ...rates);
  const maxOutbox = Math.max(0, ...samples.map((s) => s.outbox));
  const maxRabbit = Math.max(0, ...samples.map((s) => s.rabbit ?? 0));
  const during = samples.filter((s) => s.t >= 0 && s.t <= DURATION_S);
  const mem = (group: string) => {
    const values = during.map((s) => s.rssMb[group]).filter((v): v is number => !!v);
    return values.length ? `${values[0]} MB | ${Math.max(...values)} MB | ${values.at(-1)} MB` : '- | - | -';
  };

  const load =
    scenario === 'spike'
      ? `${BASE_RATE} → ${PEAK_RATE} → ${BASE_RATE} emails/s (${DURATION})`
      : `${RATE} emails/s for ${DURATION}`;
  const n = (v: number) => Math.round(v).toLocaleString('en-US');
  const ms = (v: number | undefined | null) => (v == null ? '-' : `${n(v)} ms`);

  const lines = [
    `## Stress test: ${scenario}, ${load}`,
    '',
    `**${n(k6Accepted)} emails accepted, ${n(d.deliveredUnique)} delivered, ${lost === 0 ? '0 lost ✅' : `${n(lost)} LOST ❌`}, ` +
      `${n(d.duplicates)} duplicate sends. API p99 ${ms(k6?.p99)}.**`,
    '',
    `Machine: ${cores} CPU cores (${os.cpus()[0]?.model.trim() ?? 'unknown'}), ${Math.round(os.totalmem() / 1024 ** 3)} GB RAM. ` +
      `App: ${API_PROCESSES} API process(es), ${RELAYS} relay(s), ${WORKERS} worker(s) (prefetch ${appSettings.WORKER_PREFETCH}). ` +
      `Mock provider: ${appSettings.MOCK_LATENCY_MS} ms per email, ${Number(appSettings.MOCK_FAILURE_RATE) * 100}% failures.`,
    '',
    '### API (POST /v1/emails)',
    '',
    '| Metric | Value |',
    '|---|---|',
    ...(k6
      ? [
          `| Requests | ${n(k6.requests)} (${k6.rate.toFixed(1)}/s) |`,
          `| Accepted (202) | ${n(k6.accepted)} |`,
          `| Rejected, even after client retries | ${n(k6.rejected)} |`,
          `| Client retries (same Idempotency-Key) | ${n(k6.clientRetries)} |`,
          `| Latency p50 / p95 / p99 / max | ${ms(k6.p50)} / ${ms(k6.p95)} / ${ms(k6.p99)} / ${ms(k6.max)} |`,
          `| Requests k6 couldn't start in time | ${n(k6.dropped)} |`,
        ]
      : ['| k6 result | not available (see the k6 output) |']),
    '',
    '### Delivery',
    '',
    '| Metric | Value |',
    '|---|---|',
    `| Delivered (unique emails the provider got) | ${n(d.deliveredUnique)} |`,
    `| **Lost** | **${n(lost)}** |`,
    `| **Duplicate sends** | **${n(d.duplicates)}** |`,
    `| Dead-lettered | ${n(d.failed)} |`,
    `| Still in progress at the end | ${n(d.pending)} |`,
    `| Needed more than one attempt | ${n(d.retried)} |`,
    `| Delivery throughput | ${spanS > 0 ? n(d.sent / spanS) : '-'}/s average, ${n(peakDelivery)}/s peak |`,
    `| End-to-end latency p50 / p95 / p99 | ${ms(e2e[0])} / ${ms(e2e[1])} / ${ms(e2e[2])} |`,
    `| Most emails waiting at once | outbox ${n(maxOutbox)}, RabbitMQ ${n(maxRabbit)} |`,
    `| All delivered, after the load stopped, within | ${drainS == null ? `not within ${DRAIN_TIMEOUT_S}s` : `${drainS.toFixed(0)}s`} |`,
    '',
  ];

  if (scenario === 'chaos') {
    lines.push('### Chaos events', '', '| Time | What happened | Effect |', '|---|---|---|');
    events.forEach((e, i) => lines.push(`| ${fmtTime(e.t)} | ${e.text} | ${chaosEffect(i)} |`));
    const restarts = allProcesses.filter((p) => p.restarts > 0).map((p) => `${p.name} ×${p.restarts}`);
    lines.push('', `Process restarts: ${restarts.join(', ') || 'none'}.`, '');
  }

  lines.push(
    ...cpu,
    '### Memory (RSS) during the load',
    '',
    '| Processes | Start | Max | End |',
    '|---|---|---|---|',
    `| API (all processes) | ${mem('api')} |`,
    `| Relay | ${mem('relay')} |`,
    `| Workers (all) | ${mem('workers')} |`,
    '',
    `_Lost = accepted by the API but never delivered or dead-lettered. Duplicate sends = the provider got the same email twice. ` +
      `End-to-end latency = from the API accepting the email to the provider accepting it._`,
  );
  return lines.join('\n');
}

// ---------------------------------------------------------------- main

async function main() {
  mkdirSync(logDir, { recursive: true });
  console.log(`Stress test "${scenario}" on ${cores} CPU cores. Results will be in ${outDir}`);
  console.log(`Settings: WORKERS=${WORKERS} RELAYS=${RELAYS}`, appSettings, '\n');

  await sh('docker', ['compose', 'up', '-d', '--wait', 'postgres', 'rabbitmq', 'redis']);
  await sh('npm', ['run', 'build', '--silent']);
  const pool = await resetDatabase();
  await resetVhost();
  const redis = createRedis(config.REDIS_URL);
  await waitForRedis(redis);
  await redis.del(RedisSendRecorder.SENT_KEY, RedisSendRecorder.DUPLICATES_KEY);

  console.log(`Starting ${API_PROCESSES} API process(es), ${RELAYS} relay(s), ${WORKERS} worker(s)...`);
  for (const p of allProcesses) p.start();
  await waitFor('the API', async () => (await fetch(`${API_URL}/health`)).ok);
  await waitFor('the workers to connect to RabbitMQ', async () => (await consumers()) >= WORKERS);

  const stopSampler = startSampler(pool, redis);
  k6StartedAt = Date.now();
  await startCpu();
  const chaos = scenario === 'chaos' ? scheduleChaos() : Promise.resolve();
  const k6 = await runK6({
    SCENARIO: scenario === 'spike' ? 'spike' : 'steady',
    RATE: String(RATE),
    DURATION,
    BASE_RATE: String(BASE_RATE),
    PEAK_RATE: String(PEAK_RATE),
    API_URL,
  });
  stopCpu();
  await chaos;

  console.log('\nLoad finished. Waiting for every email to be delivered...');
  const drainS = await waitForDrain(pool);
  await sleep(SAMPLE_S * 1000); // one last sample
  await stopSampler();
  const d = await results(pool, redis);

  const text = report(k6, d, drainS, cpuReport());
  writeFileSync(path.join(outDir, 'report.md'), `${text}\n`);
  writeFileSync(path.join(outDir, 'timeline.csv'), timelineCsv());
  console.log(`\n\n${text}\n\nSaved to ${outDir}/report.md (and timeline.csv, logs/)`);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);

  await cleanup();
  redis.disconnect();
  await pool.end();
}

function timelineCsv() {
  const header =
    'seconds,accepted_total,delivered_total,waiting_in_outbox,waiting_in_rabbitmq,api_mb,relay_mb,workers_mb,event';
  const rows = samples.map((s) => {
    const event = events
      .filter((e) => e.t > s.t - SAMPLE_S && e.t <= s.t)
      .map((e) => e.text)
      .join('; ');
    return [
      s.t.toFixed(1),
      s.accepted,
      s.delivered,
      s.outbox,
      s.rabbit ?? '',
      s.rssMb.api ?? '',
      s.rssMb.relay ?? '',
      s.rssMb.workers ?? '',
      `"${event}"`,
    ].join(',');
  });
  return `${[header, ...rows].join('\n')}\n`;
}

async function cleanup() {
  await Promise.all(allProcesses.map((p) => p.stop()));
  if (rabbitKilled) await run('docker', ['compose', 'start', 'rabbitmq']).catch(() => {});
}

process.on('SIGINT', async () => {
  console.log('\nStopping...');
  await cleanup();
  process.exit(130);
});

main().catch(async (err) => {
  console.error('\nStress test failed:', err);
  await cleanup();
  process.exit(1);
});

// ---------------------------------------------------------------- helpers

function parseDuration(text: string): number {
  let seconds = 0;
  for (const [, value, unit] of text.matchAll(/(\d+)(ms|s|m|h)/g)) {
    seconds += Number(value) * { ms: 0.001, s: 1, m: 60, h: 3600 }[unit as 'ms' | 's' | 'm' | 'h'];
  }
  if (!seconds) throw new Error(`Can't read DURATION "${text}" (use e.g. 90s, 5m, 1h)`);
  return seconds;
}

function withPath(url: string, pathname: string): string {
  const u = new URL(url);
  u.pathname = pathname;
  return u.toString();
}

function fmtTime(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

function pad(v: number): string {
  return String(Math.round(v)).padStart(5);
}
