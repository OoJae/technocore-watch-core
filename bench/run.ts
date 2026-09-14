// Benchmark: workload W1 against a LOCAL technocore-chat at production-like settings
// (CHAT_WAIT_POLL=0.5, CHAT_RATE_READ=600, CHAT_MAX_WAITERS_PER_IP=4), through the counting
// proxy. Never touches production.
//
//   node bench/run.ts                 scale 0.2 (3 minutes)
//   node bench/run.ts --full          scale 1   (15 minutes, the #765 W1 run)
//   node bench/run.ts --scale 0.05    quick smoke
//   node bench/run.ts --runs 3        repeat and keep every run plus the median summary
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { cpus, platform, release, tmpdir, totalmem } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { HostPool } from '../src/engine/host-pool.ts';
import { createWatcher, type Watcher } from '../src/engine/watcher.ts';
import { DEFAULT_COALESCE } from '../src/engine/types.ts';
import { renderNotice } from '../src/protocol/render.ts';
import { FileStateStore } from '../src/state/file-store.ts';
import { startProxy } from '../tests/harness/counting-proxy.ts';
import { startTechnocore } from '../tests/harness/technocore-server.ts';
import { post } from '../tests/harness/writer.ts';
import { type BenchTrace, markdown, summarize } from './report.ts';
import { DURATION_MIN, FLOOD_PER_SECOND, SESSIONS, workloadW1 } from './workload.ts';

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const flag = (n: string) => args.includes(`--${n}`);
const value = (n: string) => {
  const i = args.indexOf(`--${n}`);
  return i >= 0 ? args[i + 1] : undefined;
};
const scale = flag('full') ? 1 : Number(value('scale') ?? '0.2');
const runs = Math.max(1, Number(value('runs') ?? '1'));
if (!(scale > 0 && scale <= 1)) throw new Error('--scale must be in (0, 1]');

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function runOnce(): Promise<ReturnType<typeof summarize> & { trace: BenchTrace }> {
  const server = await startTechnocore({ waitPoll: 0.5, rateRead: 600, maxWaitersPerIp: 4 });
  const proxy = await startProxy(server.origin);
  const dataDir = await mkdtemp(join(tmpdir(), 'tcw bench '));
  const trace: BenchTrace = { startedAt: 0, endedAt: 0, scale, subscriptions: SESSIONS, posts: [], seen: [], notices: [], requests: [], waitNotHeld: 0, rateLimited: 0 };
  let pool: HostPool | undefined;
  let watchers: Watcher[] = [];
  const boot = async () => {
    pool = new HostPool({ origin: proxy.origin });
    const store = new FileStateStore(dataDir);
    watchers = [];
    for (const [scope, rooms] of Object.entries(SESSIONS)) {
      const w = createWatcher({ origin: proxy.origin, scopeKey: scope, store, pool });
      w.onNotice(async (n) => {
        trace.notices.push({ scope, id: n.id, t: Date.now(), chars: renderNotice(n).length, rooms: n.rooms.map((r) => ({ room: r.room, seqFrom: r.seqFrom, seqTo: r.seqTo, newCount: r.newCount, ...(r.generation !== undefined ? { generation: r.generation } : {}) })) });
        await w.markNotified(n.id);
      });
      w.onEvent((e) => {
        if (e.type === 'activity' && e.room) trace.seen.push({ scope, room: e.room, from: Number(e.detail!.from), to: Number(e.detail!.to), t: Date.now() });
        if (e.type === 'wait-not-held') trace.waitNotHeld++;
        if (e.type === 'rate-limited') trace.rateLimited++;
      });
      for (const r of rooms) await w.subscribe(r);
      watchers.push(w);
    }
  };
  const shutdown = async () => {
    await Promise.all(watchers.map((w) => w.dispose()));
    await pool?.dispose();
  };

  try {
    for (const room of new Set(Object.values(SESSIONS).flat())) await post(server.origin, room, 'bench seed');
    await boot();
    trace.startedAt = Date.now();
    const requestsFrom = proxy.records.length;
    const minute = 60_000 * scale;
    let flood: Promise<void> | undefined;
    let flooding = false;
    const posting: Promise<void>[] = [];
    const doPost = async (room: string, kind: string, text: string) => {
      const seq = await post(server.origin, room, text);
      trace.posts.push({ room, seq, kind, tPostOk: Date.now() });
    };
    for (const ev of workloadW1()) {
      const due = trace.startedAt + ev.atMin * minute;
      if (due > Date.now()) await sleep(due - Date.now());
      switch (ev.kind) {
        case 'burst':
          for (let i = 0; i < (ev.count ?? 8); i++) await doPost(ev.room, 'burst', `burst ${i}`);
          break;
        case 'flood-start':
          flooding = true;
          flood = (async () => {
            let i = 0;
            while (flooding) {
              const t = Date.now();
              posting.push(doPost(ev.room, 'flood', `flood ${i++}`));
              await sleep(Math.max(0, 1000 / FLOOD_PER_SECOND - (Date.now() - t)));
            }
          })();
          break;
        case 'flood-stop':
          flooding = false;
          await flood;
          break;
        case 'trickle':
          posting.push(doPost(ev.room, 'trickle', `trickle at ${ev.atMin}`));
          break;
        case 'single':
          posting.push(doPost(ev.room, 'single', `single at ${ev.atMin}`));
          break;
        case 'restart':
          await shutdown();
          trace.restartAt = Date.now();
          await boot();
          break;
      }
    }
    const end = trace.startedAt + DURATION_MIN * minute;
    if (end > Date.now()) await sleep(end - Date.now());
    await Promise.all(posting);
    await sleep(35_000); // let the last coalesced notice (<= minWakeInterval) land
    trace.endedAt = Date.now();
    trace.requests = proxy.records.slice(requestsFrom).map((r) => ({ class: r.class, startedAt: r.startedAt, ...(r.status !== undefined ? { status: r.status } : {}) }));
    await shutdown();
    return { ...summarize(trace), trace };
  } finally {
    await proxy.close();
    await server.dispose();
    await rm(dataDir, { recursive: true, force: true });
  }
}

function sha(): string {
  try {
    return execFileSync('git', ['rev-parse', '--short', 'HEAD'], { cwd: here, encoding: 'utf8' }).trim();
  } catch {
    return 'nogit';
  }
}

const results = [];
for (let i = 0; i < runs; i++) {
  process.stderr.write(`bench: run ${i + 1}/${runs} at scale ${scale} (~${Math.round(DURATION_MIN * scale * 60 + 45)} s)\n`);
  results.push(await runOnce());
}
const pick = [...results].sort((a, b) => (a.wakeLatencyMs.all.p50 ?? 0) - (b.wakeLatencyMs.all.p50 ?? 0))[Math.floor(results.length / 2)]!;
const date = new Date().toISOString().slice(0, 10);
const meta = {
  date,
  commit: sha(),
  scale: `${scale} (${DURATION_MIN * scale} workload minutes)`,
  runs: String(runs),
  hardware: `${cpus()[0]?.model ?? 'cpu'} x${cpus().length}, ${Math.round(totalmem() / 2 ** 30)} GiB, ${platform()} ${release()}`,
  node: process.version,
  technocore: 'technocore-chat v0.13.0 @20a4457 (local, CHAT_WAIT_POLL=0.5, CHAT_RATE_READ=600, CHAT_MAX_WAITERS_PER_IP=4)',
  coalescer: `quiet ${DEFAULT_COALESCE.quietMs / 1000} s / max ${DEFAULT_COALESCE.maxDelayMs / 1000} s / min wake ${DEFAULT_COALESCE.minWakeIntervalMs / 1000} s (defaults)`,
  workload: 'W1 without harness: 2 scopes x 3 subs (1 shared), idle, 8-burst, 5 msg/s flood, 45 s trickle, restart, singles',
};
const outDir = join(here, 'results');
await mkdir(outDir, { recursive: true });
const base = join(outDir, `${date}-${meta.commit}-scale${scale}`);
const { trace: _trace, ...summary } = pick;
await writeFile(`${base}.json`, JSON.stringify({ meta, summary, runs: results.map(({ trace, ...s }) => ({ summary: s, trace })) }, null, 1) + '\n');
await writeFile(`${base}.md`, markdown(summary, meta));
process.stdout.write(markdown(summary, meta) + '\n');
process.stderr.write(`bench: wrote ${base}.{json,md}\n`);
