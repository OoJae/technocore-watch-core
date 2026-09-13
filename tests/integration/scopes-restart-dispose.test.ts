// Scenarios 10 (two scopes in one pool), 11 (restart: no re-notify) and 12 (dispose leaves no
// active handles and the proxy sees the connection closed).
import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { HostPool } from '../../src/engine/host-pool.ts';
import { createWatcher } from '../../src/engine/watcher.ts';
import { FileStateStore } from '../../src/state/file-store.ts';
import type { Notice } from '../../src/protocol/types.ts';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post } from '../harness/writer.ts';
import { harness, sleepMs, tempDir, waitFor } from './helpers.ts';

const here = dirname(fileURLToPath(import.meta.url));
let server: TechnocoreServer;
let proxy: CountingProxy;
let data: Awaited<ReturnType<typeof tempDir>>;

beforeAll(async () => {
  server = await startTechnocore();
  proxy = await startProxy(server.origin);
  data = await tempDir();
});

afterAll(async () => {
  await proxy?.close();
  await server?.dispose();
  await data?.cleanup();
});

describe('scopes, restart and dispose', () => {
  it('scenario 10: two scopes share one upstream poll per room, keep independent cursors, <=3 long-polls', async () => {
    const rooms = ['shared', 'a-1', 'a-2', 'b-1', 'b-2'];
    for (const r of rooms) await post(server.origin, r, 'seed');
    const pool = new HostPool({ origin: proxy.origin });
    const store = new FileStateStore(data.path);
    const mk = (scopeKey: string) => {
      const w = createWatcher({ origin: proxy.origin, scopeKey, store, pool, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 } });
      const notices: Notice[] = [];
      w.onNotice(async (n) => {
        notices.push(n);
        await w.markNotified(n.id);
      });
      return { w, notices };
    };
    const A = mk('scope-A');
    const B = mk('scope-B');
    for (const r of ['shared', 'a-1', 'a-2']) await A.w.subscribe(r);
    for (const r of ['shared', 'b-1', 'b-2']) await B.w.subscribe(r);
    const t0 = Date.now();
    await sleepMs(4000);
    await post(server.origin, 'shared', 'to both scopes');
    await waitFor(() => A.notices.length >= 1 && B.notices.length >= 1, 60_000, 100);
    expect(A.notices[0]!.rooms.map((r) => r.room)).toEqual(['shared']);
    expect(B.notices[0]!.rooms.map((r) => r.room)).toEqual(['shared']);
    await A.w.ack('shared', 2);
    expect(A.w.status().subscriptions.find((s) => s.room === 'shared')).toMatchObject({ acceptedSeq: 2, unread: 0 });
    expect(B.w.status().subscriptions.find((s) => s.room === 'shared')).toMatchObject({ acceptedSeq: 1, unread: 1 });
    expect(proxy.maxInflightLongPolls).toBeLessThanOrEqual(3);
    expect(proxy.maxInflightByRoom.get('shared') ?? 0).toBe(1);
    for (const [, n] of proxy.maxInflightByRoom) expect(n).toBeLessThanOrEqual(1);
    const sharedPolls = proxy.records.filter((r) => r.room === 'shared' && r.class === 'read-wait' && r.startedAt >= t0);
    const endedWithin = (a: number, b: number) => sharedPolls.filter((r) => r.startedAt < b && (r.endedAt ?? Infinity) > a).length;
    for (const r of sharedPolls) expect(endedWithin(r.startedAt, r.startedAt + 1)).toBeLessThanOrEqual(1);
    await A.w.dispose();
    await B.w.dispose();
    await pool.dispose();
  });

  it('scenario 11: dispose then recreate with the same store -> no re-notify', async () => {
    await post(server.origin, 'restart', 'seed');
    const first = harness({ origin: proxy.origin, scopeKey: 'restart', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 } });
    await first.watcher.subscribe('restart');
    await post(server.origin, 'restart', 'one');
    await post(server.origin, 'restart', 'two');
    await waitFor(() => first.notices.length === 1, 15_000);
    await first.watcher.dispose();

    const second = harness({ origin: proxy.origin, scopeKey: 'restart', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 } });
    await second.watcher.ready;
    const since = Date.now();
    await sleepMs(4000);
    expect(second.notices).toHaveLength(0);
    expect(second.watcher.status().subscriptions[0]).toMatchObject({ headSeq: 3, notifiedSeq: 3, unread: 2 });
    // It resumed the long-poll from headSeq, not from scratch.
    const resumed = proxy.records.find((r) => r.room === 'restart' && r.class === 'read-wait' && r.startedAt >= since);
    expect(resumed?.query.get('since')).toBe('3');
    await post(server.origin, 'restart', 'three');
    await waitFor(() => second.notices.length === 1, 15_000);
    expect(second.notices[0]!.rooms[0]).toMatchObject({ seqFrom: 4, seqTo: 4, newCount: 1 });
    await second.watcher.dispose();
  });

  it('scenario 12: dispose leaves no timers or sockets (the process exits by itself) and the proxy sees the close', async () => {
    const child = spawn(process.execPath, [join(here, '..', 'harness', 'dispose-child.ts'), proxy.origin, join(data.path, 'child')], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    child.stdout.on('data', (d: Buffer) => (out += d.toString()));
    child.stderr.on('data', (d: Buffer) => (err += d.toString()));
    const exited = new Promise<number | null>((r) => child.once('exit', (code) => r(code)));
    await waitFor(() => out.includes('"ready":true'), 30_000);
    await post(server.origin, 'dispose-a', 'wake the child');
    await waitFor(() => out.includes('"notice"'), 15_000);
    await waitFor(() => proxy.inflightLongPolls >= 1, 15_000);
    const socketsBefore = proxy.openSockets;
    expect(socketsBefore).toBeGreaterThanOrEqual(1);
    child.stdin.write('dispose\n');
    await waitFor(() => out.includes('"disposed":true'), 15_000);
    const report = JSON.parse(out.split('\n').find((l) => l.includes('"disposed"'))!) as { before: string[]; after: string[] };
    expect(report.before).toEqual(expect.arrayContaining(['TCPSocketWrap', 'Timeout']));
    expect(report.after.filter((r) => r === 'Timeout' || r === 'TCPSocketWrap' || r === 'TCPServerWrap')).toEqual([]);
    const code = await Promise.race([exited, sleepMs(5000).then(() => 'still running' as const)]);
    if (code === 'still running') child.kill('SIGKILL');
    expect(code, `child did not exit on its own; stderr: ${err}`).toBe(0);
    await waitFor(() => proxy.openSockets === 0, 5000);
    expect(proxy.records.filter((r) => r.class === 'read-wait').some((r) => r.clientAborted)).toBe(true);
  });
});
