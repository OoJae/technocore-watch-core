import { spawn } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm, writeFile, stat } from 'node:fs/promises';
import { hostname, tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { FileStateStore, scopeHash } from '../../src/state/file-store.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import { RevisionConflictError, emptyState, migrateState } from '../../src/state/model.ts';
import { newSubscription } from '../../src/protocol/reconcile.ts';
import type { StateStore } from '../../src/state/store.ts';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'tcw store '));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

for (const kind of ['file', 'memory'] as const) {
  describe(`${kind} store`, () => {
    const make = (): StateStore => (kind === 'file' ? new FileStateStore(dir) : new MemoryStateStore());

    it('compare-and-set saves bump the revision and reject stale writers', async () => {
      const store = make();
      const s0 = emptyState('https://technocore.chat', 'scope/with spaces');
      const s1 = await store.save(s0, 0);
      expect(s1.revision).toBe(1);
      await expect(store.save(s0, 0)).rejects.toBeInstanceOf(RevisionConflictError);
      s1.subscriptions.lobby = newSubscription('lobby', new Date());
      const s2 = await store.save(s1, 1);
      expect(s2.revision).toBe(2);
      const loaded = await store.load('scope/with spaces');
      expect(loaded?.subscriptions.lobby?.room).toBe('lobby');
      expect(await store.revision('scope/with spaces')).toBe(2);
      expect(await store.listScopes()).toEqual(['scope/with spaces']);
    });

    it('only one of two concurrent writers at the same revision wins', async () => {
      const store = make();
      await store.save(emptyState('https://x.test', 's'), 0);
      const results = await Promise.allSettled([store.save(emptyState('https://x.test', 's'), 1), store.save(emptyState('https://x.test', 's'), 1)]);
      expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
      expect(results.filter((r) => r.status === 'rejected')).toHaveLength(1);
    });

    it('single-poller lock', async () => {
      const store = make();
      const a = await store.acquireLock();
      expect(a).not.toBeNull();
      expect(await store.acquireLock()).toBeNull();
      await a!.release();
      const b = await store.acquireLock();
      expect(b).not.toBeNull();
      await b!.release();
    });

    it('notices journal round-trips', async () => {
      const store = make();
      await store.save(emptyState('https://x.test', 's'), 0);
      await store.appendNotice('s', { id: 'n1', scopeKey: 's', createdAt: 'c', rooms: [], totalNew: 0, provenance: { origin: 'o', transport: 'http-longpoll', watcher: 'w', fetchedAt: 'f' } });
      expect((await store.readNotices('s')).map((n) => n.id)).toEqual(['n1']);
    });
  });
}

describe('FileStateStore specifics', () => {
  it('writes atomically (no temp files left, 0600 files) under <data>/v1/<sha256[:16]>/', async () => {
    const store = new FileStateStore(dir);
    await store.save(emptyState('https://x.test', 'k'), 0);
    const scopeDir = join(dir, 'v1', scopeHash('k'));
    const files = await readdir(scopeDir);
    expect(files).toEqual(['state.json']);
    expect((await stat(join(scopeDir, 'state.json'))).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(join(scopeDir, 'state.json'), 'utf8')).revision).toBe(1);
  });

  it('steals a stale lock (dead pid) but not a live one', async () => {
    const store = new FileStateStore(dir, { heartbeatMs: 50 });
    await store.acquireLock().then((l) => l?.release());
    await writeFile(join(dir, 'v1', 'poller.lock'), JSON.stringify({ pid: 2 ** 22 + 12345, startedAt: 'x', heartbeatAt: new Date().toISOString(), owner: 'dead' }));
    const stolen = await store.acquireLock();
    expect(stolen).not.toBeNull();
    const other = new FileStateStore(dir, { heartbeatMs: 50 });
    expect(await other.acquireLock()).toBeNull();
    await new Promise((r) => setTimeout(r, 180)); // heartbeats keep it fresh
    expect(await other.acquireLock()).toBeNull();
    await stolen!.release();
  });

  it('never steals a live holder\'s lock just because its heartbeat is old (laptop asleep)', async () => {
    const lockPath = join(dir, 'v1', 'poller.lock');
    // This process holds it; its heartbeat is aged by 60 s as if the machine had slept.
    const daemon = new FileStateStore(dir); // default 10 s heartbeat: nothing refreshes it during the test
    const held = await daemon.acquireLock();
    expect(held).not.toBeNull();
    const info = JSON.parse(await readFile(lockPath, 'utf8'));
    await writeFile(lockPath, JSON.stringify({ ...info, heartbeatAt: new Date(Date.now() - 60_000).toISOString() }));
    const monitor = new FileStateStore(dir, { heartbeatMs: 50 });
    expect(await monitor.acquireLock()).toBeNull();
    expect(held!.isHeld!()).toBe(true);
    await held!.release();

    // Another live process on this host with a stale heartbeat: still not stale...
    const child = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 30000)'], { stdio: 'ignore' });
    try {
      await new Promise((r) => setTimeout(r, 1500));
      const quick = new FileStateStore(dir, { heartbeatMs: 50, staleAfterMs: 200 });
      const other = { pid: child.pid, startedAt: new Date().toISOString(), owner: 'other-process', hostname: hostname() };
      await writeFile(lockPath, JSON.stringify({ ...other, heartbeatAt: new Date(Date.now() - 500).toISOString() }));
      expect(await quick.acquireLock()).toBeNull();
      // ...unless that pid provably started after the last heartbeat (pid reuse).
      await writeFile(lockPath, JSON.stringify({ ...other, heartbeatAt: new Date(Date.now() - 10_000).toISOString() }));
      const taken = await quick.acquireLock();
      expect(taken).not.toBeNull();
      await taken!.release();
      // A lock written by a previous process that had our pid is a leftover.
      await writeFile(lockPath, JSON.stringify({ pid: process.pid, startedAt: 'x', heartbeatAt: new Date().toISOString(), owner: 'previous-life', hostname: hostname() }));
      const again = await quick.acquireLock();
      expect(again).not.toBeNull();
      await again!.release();
    } finally {
      child.kill('SIGKILL');
    }
  });

  it('a holder whose lock is taken over is told so and never overwrites the new holder', async () => {
    const store = new FileStateStore(dir, { heartbeatMs: 30 });
    const lock = (await store.acquireLock())!;
    let lost = 0;
    lock.onLost!(() => lost++);
    const lockPath = join(dir, 'v1', 'poller.lock');
    const thief = JSON.stringify({ pid: process.pid, startedAt: 'x', heartbeatAt: new Date().toISOString(), owner: 'thief' });
    await writeFile(lockPath, thief);
    await new Promise((r) => setTimeout(r, 250));
    expect(lost).toBe(1);
    expect(lock.isHeld!()).toBe(false);
    expect(await readFile(lockPath, 'utf8')).toBe(thief);
    await lock.release();
    expect(await readFile(lockPath, 'utf8')).toBe(thief);
  });

  it('bounds the notices journal as a ring', async () => {
    const store = new FileStateStore(dir);
    await store.save(emptyState('https://x.test', 's'), 0);
    const big = 'x'.repeat(4000);
    for (let i = 0; i < 100; i++) {
      await store.appendNotice('s', { id: `n${i}`, scopeKey: 's', createdAt: big, rooms: [], totalNew: 0, provenance: { origin: 'o', transport: 'http-longpoll', watcher: 'w', fetchedAt: 'f' } });
    }
    expect((await stat(store.noticesPath('s'))).size).toBeLessThanOrEqual(256 << 10);
    const ids = (await store.readNotices('s')).map((n) => n.id);
    expect(ids[ids.length - 1]).toBe('n99');
    expect(ids.length).toBeLessThan(100);
  });

  it('migrateState drops invalid rooms and clamps cursors', () => {
    const s = migrateState({
      version: 1,
      origin: 'o',
      scopeKey: 'k',
      revision: 3,
      subscriptions: {
        'BAD NAME': { headSeq: 1 },
        ok: { headSeq: 5, notifiedSeq: 9, acceptedSeq: -1, gaps: [{ from: 3, to: 1, reason: 'ring-dropped' }, { from: 1, to: 2, reason: 'ring-dropped' }], status: 'weird' },
      },
    });
    expect(Object.keys(s.subscriptions)).toEqual(['ok']);
    expect(s.subscriptions.ok).toMatchObject({ headSeq: 5, notifiedSeq: 5, acceptedSeq: 0, status: 'active' });
    expect(s.subscriptions.ok!.gaps).toHaveLength(1);
    expect(() => migrateState({ version: 2 })).toThrow();
  });
});
