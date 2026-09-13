// CLI processes around the poller lock, in-process and without any network: the `monitor`
// follower across notice-journal rewrites, and a `run` poller that loses its lock.
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { main } from '../../src/cli/main.ts';
import { FileStateStore, MAX_NOTICES_BYTES } from '../../src/state/file-store.ts';
import { emptyState } from '../../src/state/model.ts';
import { NOTICE_HEADER } from '../../src/protocol/render.ts';
import type { Notice } from '../../src/protocol/types.ts';

// Nothing listens here; neither test ever subscribes to a room, so no request is made.
const DEAD_ORIGIN = 'http://127.0.0.1:9';
let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'tcw cli lock '));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function notice(id: string, pad = 0): Notice {
  const rooms = Array.from({ length: pad }, (_, i) => ({
    room: `padding-room-${i}`,
    newCount: 1,
    seqFrom: 1,
    seqTo: 1,
    gaps: [],
    signedSenders: 0,
    unsignedSenders: 1,
  }));
  return {
    id,
    scopeKey: 'default',
    createdAt: new Date().toISOString(),
    rooms,
    totalNew: rooms.length,
    provenance: { origin: DEAD_ORIGIN, transport: 'http-longpoll', watcher: 'test', fetchedAt: new Date().toISOString() },
  };
}

function capture() {
  const out: string[] = [];
  const err: string[] = [];
  return { out, err, io: { out: (s: string) => out.push(s), err: (s: string) => err.push(s), env: {} as Record<string, string | undefined> } };
}

describe('CLI around the poller lock', () => {
  it('monitor follower prints each notice exactly once across journal ring rewrites', async () => {
    const poller = new FileStateStore(dir);
    await poller.save(emptyState(DEAD_ORIGIN, 'default'), 0);
    const lock = await poller.acquireLock('poller');
    expect(lock).not.toBeNull();
    // A journal just under its bound, written before the follower starts.
    const old: string[] = [];
    for (let i = 0, size = 0; ; i++) {
      const line = JSON.stringify(notice(`old-${i}`, 20)) + '\n';
      if (size + line.length > MAX_NOTICES_BYTES - 3000) break;
      old.push(line);
      size += line.length;
    }
    await writeFile(poller.noticesPath('default'), old.join(''));

    const c = capture();
    const stop = new AbortController();
    const run = main(['monitor', '--data', dir, '--origin', DEAD_ORIGIN], { ...c.io, stopSignal: stop.signal });
    await sleep(1300); // the follower has started and ticked once
    expect(c.out).toEqual([]);

    // One small notice fits under the bound and is printed normally.
    const fresh: string[] = ['new-0'];
    await poller.appendNotice('default', notice('new-0', 1));
    await sleep(1500);
    expect(c.out).toHaveLength(1);
    // The next one crosses the bound: the ring is rewritten (new inode, about half the size)
    // and still holds new-0 and many old notices, none of which may be printed again.
    fresh.push('new-1');
    await poller.appendNotice('default', notice('new-1', 40));
    await sleep(1500);
    expect(c.out).toHaveLength(2);
    // Keep writing through further rewrites, with and without follower ticks in between.
    for (let i = 2; i < 60; i++) {
      fresh.push(`new-${i}`);
      await poller.appendNotice('default', notice(`new-${i}`, 40));
      if (i % 20 === 0) await sleep(1100);
    }
    await sleep(2500);
    stop.abort();
    expect(await run).toBe(0);
    await lock!.release();

    const printedIds = c.out.map((l) => {
      expect(l.startsWith(NOTICE_HEADER)).toBe(true);
      return /notice_id_json="([^"]+)"/.exec(l)?.[1];
    });
    expect(printedIds).toEqual(fresh);
    // Sanity: the ring really was rewritten (the oldest lines are gone).
    expect(await readFile(poller.noticesPath('default'), 'utf8')).not.toContain('"old-0"');
  }, 30_000);

  it('run stops polling and exits 3 when another process takes the lock over', async () => {
    const c = capture();
    c.io.env.TECHNOCORE_WATCH_LOCK_HEARTBEAT_MS = '50';
    const stop = new AbortController();
    const run = main(['run', '--data', dir, '--origin', DEAD_ORIGIN], { ...c.io, stopSignal: stop.signal });
    const lockPath = join(dir, 'v1', 'poller.lock');
    for (let i = 0; i < 100; i++) {
      try {
        if (JSON.parse(await readFile(lockPath, 'utf8')).pid === process.pid) break;
      } catch {
        // not yet
      }
      await sleep(20);
    }
    await sleep(200);
    const thief = JSON.stringify({ pid: process.pid, startedAt: 'x', heartbeatAt: new Date().toISOString(), owner: 'another-process' });
    await writeFile(lockPath, thief);
    const result = await Promise.race([run, sleep(3000).then(() => 'still running' as const)]);
    stop.abort();
    expect(result).toBe(3);
    expect(c.err.join('\n')).toContain('took over the poller lock');
    expect(await readFile(lockPath, 'utf8')).toBe(thief);
  }, 20_000);
});
