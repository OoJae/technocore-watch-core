// technocore-watch — harness-neutral CLI over technocore-watch-core.
// Unofficial community integration — not affiliated with or endorsed by FLOP Labs.
//
//   technocore-watch run         [--scope KEY]... [--print line|framed|json|none]
//   technocore-watch monitor     [--scope KEY]...
//   technocore-watch subscribe   <room> [--from now|retained] [--offline]
//   technocore-watch unsubscribe <room>
//   technocore-watch status      [--json]
//   technocore-watch inbox       [room] [--max N] [--peek] [--format framed|json]
//
// Common: --data <dir> --scope <key> --origin <url> --verbose
import { homedir } from 'node:os';
import { join } from 'node:path';
import { open, stat } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { createWatcher, type Watcher } from '../engine/watcher.ts';
import { HostPool } from '../engine/host-pool.ts';
import type { CoalesceOptions, Limits } from '../engine/types.ts';
import { isValidRoomName } from '../protocol/names.ts';
import { asciiJson, renderNotice, renderNoticeLine, renderPage } from '../protocol/render.ts';
import type { Notice } from '../protocol/types.ts';
import { FileStateStore } from '../state/file-store.ts';
import { emptyState } from '../state/model.ts';
import type { StoreLock } from '../state/store.ts';
import { normalizeOrigin } from '../transport/client.ts';

export const DEFAULT_ORIGIN = 'https://technocore.chat';

export interface CliIo {
  out: (s: string) => void;
  err: (s: string) => void;
  env: Record<string, string | undefined>;
  /** Resolves when the process should stop (signals). Tests pass their own. */
  stopSignal?: AbortSignal;
}

interface Parsed {
  command: string | undefined;
  positionals: string[];
  flags: Map<string, string[]>;
}

const BOOLEAN_FLAGS = new Set(['json', 'peek', 'offline', 'verbose', 'help']);

export function parseArgs(argv: string[]): Parsed {
  const flags = new Map<string, string[]>();
  const positionals: string[] = [];
  let command: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === '--') {
      positionals.push(...argv.slice(i + 1));
      break;
    }
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      const name = (eq >= 0 ? a.slice(2, eq) : a.slice(2)).toLowerCase();
      let value: string;
      if (eq >= 0) value = a.slice(eq + 1);
      else if (BOOLEAN_FLAGS.has(name)) value = 'true';
      else {
        const nextArg = argv[i + 1];
        if (nextArg === undefined) throw new UsageError(`--${name} needs a value`);
        value = nextArg;
        i++;
      }
      flags.set(name, [...(flags.get(name) ?? []), value]);
    } else if (command === undefined) {
      command = a;
    } else {
      positionals.push(a);
    }
  }
  return { command, positionals, flags };
}

class UsageError extends Error {
  override readonly name = 'UsageError';
}

const USAGE = `technocore-watch — read-only Technocore room watcher (unofficial; not affiliated with or endorsed by FLOP Labs)

usage:
  technocore-watch run         [--scope KEY]... [--print line|framed|json|none]
  technocore-watch monitor     [--scope KEY]...
  technocore-watch subscribe   <room> [--from now|retained] [--offline]
  technocore-watch unsubscribe <room>
  technocore-watch status      [--json]
  technocore-watch inbox       [room] [--max N] [--peek] [--format framed|json]

common flags:
  --data <dir>     state directory (default $TECHNOCORE_WATCH_DATA or ~/.local/state/technocore-watch)
  --scope <key>    scope key (default "default")
  --origin <url>   Technocore origin (default $TECHNOCORE_URL or ${DEFAULT_ORIGIN})
  --quiet-ms/--max-delay-ms/--min-wake-ms  coalescer timing for run/monitor
  --wait-seconds <n>  long-poll wait (<= 10)

Room names and message text are untrusted third-party data; output frames them as such.`;

function one(p: Parsed, name: string): string | undefined {
  const v = p.flags.get(name);
  return v ? v[v.length - 1] : undefined;
}

function num(p: Parsed, name: string, min: number, max: number): number | undefined {
  const v = one(p, name);
  if (v === undefined) return undefined;
  const n = Number(v);
  if (!Number.isFinite(n) || n < min || n > max) throw new UsageError(`--${name} must be a number in [${min}, ${max}]`);
  return n;
}

function dataDir(p: Parsed, io: CliIo): string {
  return one(p, 'data') ?? io.env.TECHNOCORE_WATCH_DATA ?? join(io.env.XDG_STATE_HOME ?? join(homedir(), '.local', 'state'), 'technocore-watch');
}

function originOf(p: Parsed, io: CliIo): string {
  return normalizeOrigin(one(p, 'origin') ?? io.env.TECHNOCORE_URL ?? DEFAULT_ORIGIN);
}

function scopesOf(p: Parsed): string[] {
  return p.flags.get('scope') ?? [];
}

function limitsOf(p: Parsed): Partial<Limits> {
  const l: Partial<Limits> = {};
  const w = num(p, 'wait-seconds', 0.5, 10);
  if (w !== undefined) l.waitSeconds = w;
  const sync = num(p, 'sync-ms', 0, 3_600_000);
  if (sync !== undefined) l.syncIntervalMs = sync;
  return l;
}

function coalesceOf(p: Parsed): Partial<CoalesceOptions> {
  const c: Partial<CoalesceOptions> = {};
  const q = num(p, 'quiet-ms', 0, 3_600_000);
  if (q !== undefined) c.quietMs = q;
  const m = num(p, 'max-delay-ms', 0, 3_600_000);
  if (m !== undefined) c.maxDelayMs = m;
  const w = num(p, 'min-wake-ms', 0, 86_400_000);
  if (w !== undefined) c.minWakeIntervalMs = w;
  return c;
}

async function withWatcher<T>(p: Parsed, io: CliIo, fn: (w: Watcher, store: FileStateStore) => Promise<T>): Promise<T> {
  const store = new FileStateStore(dataDir(p, io));
  const scope = scopesOf(p)[0] ?? 'default';
  const w = createWatcher({ origin: originOf(p, io), scopeKey: scope, store, poll: false, limits: { ...limitsOf(p), syncIntervalMs: 0 } });
  try {
    await w.ready;
    return await fn(w, store);
  } finally {
    await w.dispose();
  }
}

function printNotice(n: Notice, mode: string, io: CliIo): void {
  if (mode === 'none') return;
  if (mode === 'json') io.out(JSON.stringify(n));
  else if (mode === 'framed') io.out(renderNotice(n));
  else io.out(renderNoticeLine(n));
}

function waitForStop(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) resolve();
    else signal.addEventListener('abort', () => resolve(), { once: true });
  });
}

async function runPoller(p: Parsed, io: CliIo, lock: StoreLock, store: FileStateStore, printMode: string, stop: AbortSignal): Promise<void> {
  const origin = originOf(p, io);
  const verbose = one(p, 'verbose') === 'true';
  const pools = new Map<string, HostPool>();
  const watchers = new Map<string, Watcher>();
  const explicit = scopesOf(p);
  const limits = limitsOf(p);
  const coalesce = coalesceOf(p);

  const poolFor = (o: string) => {
    let pool = pools.get(o);
    if (!pool) {
      pool = new HostPool({ origin: o, limits });
      pools.set(o, pool);
    }
    return pool;
  };

  const addScope = async (scope: string) => {
    if (watchers.has(scope)) return;
    const existing = await store.load(scope).catch(() => null);
    const scopeOrigin = existing ? normalizeOrigin(existing.origin) : origin;
    if (!existing) await store.save(emptyState(scopeOrigin, scope), 0).catch(() => undefined);
    const w = createWatcher({ origin: scopeOrigin, scopeKey: scope, store, pool: poolFor(scopeOrigin), limits, coalesce, journalNotices: true });
    watchers.set(scope, w);
    w.onNotice(async (n) => {
      printNotice(n, printMode, io);
      await w.markNotified(n.id);
    });
    if (verbose) {
      w.onEvent((e) => {
        if (e.type !== 'poll-start' && e.type !== 'poll-end') io.err(`event ${asciiJson(e)}`);
      });
    }
    await w.ready.catch((e: unknown) => io.err(`scope ${asciiJson(scope)}: ${String((e as Error).message)}`));
  };

  const discover = async () => {
    const scopes = explicit.length ? explicit : await store.listScopes();
    if (!scopes.length && !explicit.length) scopes.push('default');
    for (const s of scopes) await addScope(s);
  };

  await discover();
  const timer = setInterval(() => void discover().catch(() => undefined), 5000);
  if (verbose) io.err(`technocore-watch: polling ${watchers.size} scope(s) (lock pid ${lock.info.pid})`);
  try {
    await waitForStop(stop);
  } finally {
    clearInterval(timer);
    await Promise.allSettled([...watchers.values()].map((w) => w.dispose()));
    await Promise.allSettled([...pools.values()].map((pl) => pl.dispose()));
    await lock.release();
  }
}

async function followNotices(
  p: Parsed,
  io: CliIo,
  store: FileStateStore,
  stop: AbortSignal,
  offsets: Map<string, number>,
): Promise<'stopped' | 'lock'> {
  const explicit = scopesOf(p);
  const paths = async () => {
    const scopes = explicit.length ? explicit : await store.listScopes();
    return scopes.map((s) => store.noticesPath(s));
  };
  for (const path of await paths()) {
    if (offsets.has(path)) continue; // keep our place across lock attempts
    try {
      offsets.set(path, (await stat(path)).size);
    } catch {
      offsets.set(path, 0);
    }
  }
  let tick = 0;
  while (!stop.aborted) {
    await new Promise<void>((r) => {
      const t = setTimeout(r, 1000);
      stop.addEventListener('abort', () => {
        clearTimeout(t);
        r();
      }, { once: true });
    });
    if (stop.aborted) break;
    for (const path of await paths()) {
      let size = 0;
      try {
        size = (await stat(path)).size;
      } catch {
        continue;
      }
      let from = offsets.get(path) ?? 0;
      if (size < from) from = 0; // ring rewritten
      if (size === from) continue;
      const h = await open(path, 'r');
      try {
        const buf = Buffer.alloc(size - from);
        await h.read(buf, 0, buf.length, from);
        const text = buf.toString('utf8');
        const lastNl = text.lastIndexOf('\n');
        if (lastNl < 0) continue;
        offsets.set(path, from + Buffer.byteLength(text.slice(0, lastNl + 1)));
        for (const line of text.slice(0, lastNl).split('\n')) {
          if (!line.trim()) continue;
          try {
            io.out(renderNoticeLine(JSON.parse(line) as Notice));
          } catch {
            // torn or foreign line
          }
        }
      } finally {
        await h.close();
      }
    }
    if (++tick % 10 === 0) return 'lock';
  }
  return 'stopped';
}

export async function main(argv: string[], io: CliIo): Promise<number> {
  let p: Parsed;
  try {
    p = parseArgs(argv);
  } catch (e) {
    io.err(String((e as Error).message));
    io.err(USAGE);
    return 2;
  }
  if (!p.command || one(p, 'help') === 'true' || p.command === 'help') {
    io.out(USAGE);
    return p.command ? 0 : 2;
  }
  const stop = io.stopSignal ?? new AbortController().signal;
  try {
    switch (p.command) {
      case 'subscribe': {
        const room = p.positionals[0];
        if (!isValidRoomName(room)) throw new UsageError('subscribe needs a valid room name');
        const from = one(p, 'from') ?? 'now';
        if (from !== 'now' && from !== 'retained') throw new UsageError('--from must be now or retained');
        const offline = one(p, 'offline') === 'true';
        const sub = await withWatcher(p, io, (w) => w.subscribe(room, { startFrom: from, resolveNow: !offline }));
        io.out(JSON.stringify({ subscribed: sub.room, headSeq: sub.headSeq, generation: sub.generation ?? null, pendingStart: sub.startFrom ?? null }));
        return 0;
      }
      case 'unsubscribe': {
        const room = p.positionals[0];
        if (!isValidRoomName(room)) throw new UsageError('unsubscribe needs a valid room name');
        await withWatcher(p, io, (w) => w.unsubscribe(room));
        io.out(JSON.stringify({ unsubscribed: room }));
        return 0;
      }
      case 'status': {
        const st = await withWatcher(p, io, async (w) => w.status());
        if (one(p, 'json') === 'true') {
          io.out(JSON.stringify(st));
        } else {
          io.out(`scope ${asciiJson(st.scopeKey)} origin ${asciiJson(st.origin)} subscriptions ${st.totals.subscriptions} unread ${st.totals.unread} errors ${st.totals.errors}`);
          for (const s of st.subscriptions) {
            io.out(
              `  ${s.room.padEnd(24)} ${s.status.padEnd(7)} gen ${String(s.generation ?? '-').padEnd(4)} head ${String(s.headSeq).padEnd(7)} accepted ${String(s.acceptedSeq).padEnd(7)} unread ${String(s.unread).padEnd(6)} gaps ${s.gaps.length}${s.pendingStart ? ` pending:${s.pendingStart}` : ''}`,
            );
          }
        }
        return 0;
      }
      case 'inbox': {
        const room = p.positionals[0];
        if (room !== undefined && !isValidRoomName(room)) throw new UsageError('inbox room name is invalid');
        const max = num(p, 'max', 1, 200) ?? 50;
        const format = one(p, 'format') ?? 'framed';
        if (format !== 'framed' && format !== 'json') throw new UsageError('--format must be framed or json');
        const peek = one(p, 'peek') === 'true';
        await withWatcher(p, io, async (w) => {
          const rooms = room ? [room] : w.status().subscriptions.filter((s) => s.unread > 0).map((s) => s.room);
          if (room && !w.status().subscriptions.some((s) => s.room === room)) throw new UsageError(`not subscribed to ${room}`);
          if (!rooms.length && format === 'framed') io.out('(no unread Technocore messages)');
          for (const r of rooms) {
            const page = await w.readPage(r, { max });
            if (format === 'json') io.out(JSON.stringify(page));
            else io.out(renderPage(page));
            if (!peek && page.toSeq >= page.fromSeq) await w.ack(r, page.toSeq);
          }
        });
        return 0;
      }
      case 'run':
      case 'monitor': {
        const store = new FileStateStore(dataDir(p, io));
        const printMode = one(p, 'print') ?? (p.command === 'monitor' ? 'line' : 'line');
        if (!['line', 'framed', 'json', 'none'].includes(printMode)) throw new UsageError('--print must be line, framed, json or none');
        const offsets = new Map<string, number>();
        for (;;) {
          const lock = await store.acquireLock('poller');
          if (lock) {
            await runPoller(p, io, lock, store, printMode, stop);
            return 0;
          }
          if (p.command === 'run') {
            io.err('technocore-watch: another poller holds the lock for this data directory');
            return 3;
          }
          const why = await followNotices(p, io, store, stop, offsets);
          if (why === 'stopped') return 0;
        }
      }
      default:
        throw new UsageError(`unknown command ${asciiJson(p.command)}`);
    }
  } catch (e) {
    if (e instanceof UsageError) {
      io.err(e.message);
      return 2;
    }
    io.err(`technocore-watch: ${String((e as Error)?.message ?? e).slice(0, 500)}`);
    return 1;
  }
}

function isEntrypoint(): boolean {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return import.meta.url === pathToFileURL(entry).href || /technocore-watch$/.test(entry);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  const ctrl = new AbortController();
  const onSignal = () => ctrl.abort();
  process.once('SIGINT', onSignal);
  process.once('SIGTERM', onSignal);
  main(process.argv.slice(2), {
    out: (s) => process.stdout.write(s + '\n'),
    err: (s) => process.stderr.write(s + '\n'),
    env: process.env,
    stopSignal: ctrl.signal,
  }).then(
    (code) => {
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
      process.exitCode = code;
    },
    (e) => {
      process.stderr.write(`technocore-watch: ${String(e)}\n`);
      process.exitCode = 1;
    },
  );
}
