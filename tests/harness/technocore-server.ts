// Disposable LOCAL technocore-chat server for integration tests and benchmarks.
// Never points at production: it always binds 127.0.0.1 with a throwaway CHAT_ROOT.
import { type ChildProcess, spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import * as http from 'node:http';
import * as net from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export interface ServerOptions {
  waitPoll?: number;
  rateRead?: number;
  rateWrite?: number;
  maxWaitersPerIp?: number;
  maxWait?: number;
  env?: Record<string, string>;
}

export interface TechnocoreServer {
  readonly origin: string;
  readonly port: number;
  readonly root: string;
  readonly checkout: string;
  /** SIGTERM the process group (then SIGKILL). Keeps CHAT_ROOT. */
  stop(): Promise<void>;
  /** SIGKILL the process group immediately (simulates a crash). */
  kill(): Promise<void>;
  /** Start again on the same port and CHAT_ROOT. */
  start(): Promise<void>;
  /** Stop and delete CHAT_ROOT. */
  dispose(): Promise<void>;
  /** Run a Python snippet in the checkout with this server's CHAT_ROOT (server should be stopped). */
  python(code: string, args?: string[]): Promise<string>;
  logs(): string;
}

const here = dirname(fileURLToPath(import.meta.url));
const live = new Set<ChildProcess>();

process.once('exit', () => {
  for (const c of live) {
    try {
      if (c.pid) process.kill(-c.pid, 'SIGKILL');
    } catch {
      // already gone
    }
  }
});

export function findCheckout(): string {
  const candidates = [process.env.TECHNOCORE_CHECKOUT, resolve(here, '..', '..', '..', '.cache', 'technocore-chat')].filter(Boolean) as string[];
  for (const c of candidates) {
    if (existsSync(join(c, 'src', 'app.py')) && existsSync(join(c, 'pyproject.toml'))) return c;
  }
  throw new Error(
    'technocore-chat checkout not found. Set TECHNOCORE_CHECKOUT to a clone of flop-labs/technocore-chat at v0.13.0 (20a4457) with `uv sync --frozen` done, or place it at ../.cache/technocore-chat.',
  );
}

async function portFree(port: number): Promise<boolean> {
  return new Promise((res) => {
    const srv = net.createServer();
    srv.once('error', () => res(false));
    srv.listen(port, '127.0.0.1', () => srv.close(() => res(true)));
  });
}

export async function freePort(): Promise<number> {
  for (let i = 0; i < 200; i++) {
    const port = 20000 + Math.floor(Math.random() * 40000);
    if (await portFree(port)) return port;
  }
  throw new Error('no free port in 20000-60000');
}

function healthy(port: number): Promise<boolean> {
  return new Promise((res) => {
    const req = http.get({ host: '127.0.0.1', port, path: '/healthz', agent: false, timeout: 1000 }, (r) => {
      r.resume();
      res(r.statusCode === 200);
    });
    req.on('error', () => res(false));
    req.on('timeout', () => {
      req.destroy();
      res(false);
    });
  });
}

export async function startTechnocore(o: ServerOptions = {}): Promise<TechnocoreServer> {
  const checkout = findCheckout();
  const root = await mkdtemp(join(tmpdir(), 'technocore-root-'));
  const port = await freePort();
  let child: ChildProcess | undefined;
  let exited: Promise<void> = Promise.resolve();
  let output = '';

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    CHAT_ROOT: root,
    CHAT_RATE_READ: String(o.rateRead ?? 1_000_000),
    CHAT_RATE_WRITE: String(o.rateWrite ?? 1_000_000),
    CHAT_RATE_ROOMS_PER_DAY: '1000000',
    CHAT_DUPE_FILTER_SECONDS: '0',
    CHAT_EDGE_CACHE_SECONDS: '0',
    CHAT_FSYNC: '0',
    CHAT_WAIT_POLL: String(o.waitPoll ?? 0.05),
    CHAT_MAX_WAITERS_PER_IP: String(o.maxWaitersPerIp ?? 4),
    CHAT_MAX_WAIT: String(o.maxWait ?? 10),
    // Never write bytecode into the read-only upstream checkout.
    PYTHONDONTWRITEBYTECODE: '1',
    UV_NO_PROGRESS: '1',
    ...o.env,
  };

  const start = async () => {
    const c = spawn('uv', ['run', 'uvicorn', '--app-dir', 'src', 'app:app', '--host', '127.0.0.1', '--port', String(port), '--log-level', 'warning'], {
      cwd: checkout,
      env,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child = c;
    live.add(c);
    const onData = (d: Buffer) => {
      output = (output + d.toString('utf8')).slice(-20_000);
    };
    c.stdout?.on('data', onData);
    c.stderr?.on('data', onData);
    exited = new Promise<void>((res) => c.once('exit', () => {
      live.delete(c);
      res();
    }));
    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      if (c.exitCode !== null) throw new Error(`technocore server exited early (${c.exitCode}):\n${output}`);
      if (await healthy(port)) return;
      await new Promise((r) => setTimeout(r, 150));
    }
    throw new Error(`technocore server did not become healthy on :${port}\n${output}`);
  };

  const signalGroup = (sig: NodeJS.Signals) => {
    if (!child?.pid || child.exitCode !== null) return;
    try {
      process.kill(-child.pid, sig);
    } catch {
      // gone
    }
  };

  const stop = async () => {
    if (!child || child.exitCode !== null) return;
    signalGroup('SIGTERM');
    const t = setTimeout(() => signalGroup('SIGKILL'), 5000);
    await exited;
    clearTimeout(t);
  };

  const kill = async () => {
    if (!child || child.exitCode !== null) return;
    signalGroup('SIGKILL');
    await exited;
  };

  await start();
  return {
    origin: `http://127.0.0.1:${port}`,
    port,
    root,
    checkout,
    stop,
    kill,
    start,
    async dispose() {
      await stop();
      await rm(root, { recursive: true, force: true });
    },
    python(code: string, args: string[] = []) {
      return new Promise<string>((res, rej) => {
        const p = spawn('uv', ['run', 'python', '-c', code, ...args], { cwd: checkout, env, stdio: ['ignore', 'pipe', 'pipe'] });
        let out = '';
        let err = '';
        p.stdout.on('data', (d: Buffer) => (out += d.toString()));
        p.stderr.on('data', (d: Buffer) => (err += d.toString()));
        p.on('exit', (code) => (code === 0 ? res(out) : rej(new Error(`python exited ${code}: ${err}`))));
      });
    },
    logs: () => output,
  };
}
