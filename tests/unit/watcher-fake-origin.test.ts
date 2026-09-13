// Engine tests against the in-memory origin model under fake time: the scheduler, coalescer,
// wait_held:false handling, 429 penalties, shared polls and restart semantics, deterministically.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { HostPool } from '../../src/engine/host-pool.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import type { Notice } from '../../src/protocol/types.ts';
import { FakeOrigin } from '../harness/fake-transport.ts';

const ORIGIN = 'http://fake.test';

function watcher(origin: FakeOrigin, over: Partial<WatcherOptions> = {}): Watcher {
  return createWatcher({
    origin: ORIGIN,
    scopeKey: 'scope-a',
    store: new MemoryStateStore(),
    transport: origin,
    limits: { syncIntervalMs: 0, ...over.limits },
    coalesce: { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000, ...over.coalesce },
    ...over,
  } as WatcherOptions);
}

async function until(cond: () => boolean, stepMs = 50, maxMs = 120_000): Promise<void> {
  for (let t = 0; t < maxMs && !cond(); t += stepMs) await vi.advanceTimersByTimeAsync(stepMs);
  expect(cond()).toBe(true);
}

beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
afterEach(() => vi.useRealTimers());

describe('watcher on a fake origin (fake time)', () => {
  it('idle: long-polls at ~6/min per room and never notifies', async () => {
    const origin = new FakeOrigin();
    origin.model.post('lobby', 'a', 'hello');
    const w = watcher(origin);
    const notices: Notice[] = [];
    w.onNotice((n) => void notices.push(n));
    await w.subscribe('lobby');
    const before = origin.counts()['read-wait'];
    await vi.advanceTimersByTimeAsync(60_000);
    const polls = origin.counts()['read-wait'] - before;
    expect(polls).toBeGreaterThanOrEqual(5);
    expect(polls).toBeLessThanOrEqual(7);
    expect(notices).toHaveLength(0);
    await w.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('8-message burst with pageSize 3: one notice new=8, pages 1..3, 4..6, 7..8', async () => {
    const origin = new FakeOrigin();
    const w = watcher(origin, { limits: { pageSize: 3, syncIntervalMs: 0 } });
    const notices: Notice[] = [];
    w.onNotice(async (n) => {
      notices.push(n);
      await w.markNotified(n.id);
    });
    await w.subscribe('burst');
    await vi.advanceTimersByTimeAsync(100);
    for (let i = 1; i <= 8; i++) origin.model.post('burst', 'a', `m${i}`);
    await until(() => notices.length === 1);
    await vi.advanceTimersByTimeAsync(40_000);
    expect(notices).toHaveLength(1);
    expect(notices[0]!.rooms[0]).toMatchObject({ room: 'burst', newCount: 8, seqFrom: 1, seqTo: 8, gaps: [] });
    const ranges: number[][] = [];
    for (;;) {
      const page = await w.readPage('burst');
      if (!page.messages.length) break;
      ranges.push(page.messages.map((m) => m.seq));
      await w.ack('burst', page.toSeq);
    }
    expect(ranges).toEqual([[1, 2, 3], [4, 5, 6], [7, 8]]);
    expect(w.status().subscriptions[0]).toMatchObject({ acceptedSeq: 8, notifiedSeq: 8, unread: 0 });
    await w.dispose();
  });

  it('wait_held:false sleeps about the requested wait instead of re-polling at wire speed', async () => {
    const origin = new FakeOrigin(undefined, { maxWaitersPerIp: 1 });
    origin.model.post('busy', 'a', 'x');
    // Another client holds the only waiter slot for the whole test.
    const hog = new AbortController();
    const hold = async () => {
      while (!hog.signal.aborted) {
        await origin.get({ url: `${ORIGIN}/r/busy?since=1&wait=10&format=json`, signal: hog.signal }).catch(() => undefined);
      }
    };
    void hold();
    await vi.advanceTimersByTimeAsync(10);
    const w = watcher(origin, { limits: { waitSeconds: 5, syncIntervalMs: 0 } });
    const events: string[] = [];
    w.onEvent((e) => events.push(e.type));
    await w.subscribe('busy');
    const start = origin.requests.filter((r) => r.kind === 'read-wait' && r.waitHeld === false).length;
    await vi.advanceTimersByTimeAsync(60_000);
    const refused = origin.requests.filter((r) => r.kind === 'read-wait' && r.waitHeld === false).length - start;
    // One request per ~5 s (±20%) => at most 60 / 4 + 1.
    expect(refused).toBeGreaterThanOrEqual(9);
    expect(refused).toBeLessThanOrEqual(16);
    expect(events).toContain('wait-not-held');
    hog.abort();
    await w.dispose();
  });

  it('429: honours the retry the body names and slows the whole pool', async () => {
    const origin = new FakeOrigin(undefined, { rateRead: 10 });
    origin.model.post('slow', 'a', 'x');
    const w = watcher(origin, { limits: { readBudgetFraction: 1, waitSeconds: 1, syncIntervalMs: 0 } });
    const events: { type: string; at: number }[] = [];
    w.onEvent((e) => events.push({ type: e.type, at: Date.now() }));
    // Spend the origin bucket from "another process" on the same IP.
    for (let i = 0; i < 10; i++) await origin.get({ url: `${ORIGIN}/r/slow?format=json`, signal: new AbortController().signal });
    const t0 = Date.now();
    await w.subscribe('slow');
    await vi.advanceTimersByTimeAsync(120_000);
    const limited = origin.requests.filter((r) => r.status === 429 && r.startedAt >= t0);
    expect(limited.length).toBeGreaterThanOrEqual(1);
    expect(events.some((e) => e.type === 'rate-limited')).toBe(true);
    // After the first 429, the watcher's own request rate stays within 10/min (+ small burst).
    const first429 = limited[0]!.startedAt;
    const next = origin.requests.find((r) => r.startedAt > first429);
    expect(next!.startedAt - first429).toBeGreaterThanOrEqual(5000);
    const after = origin.requests.filter((r) => r.startedAt > first429 && r.kind !== 'config');
    const minutes = (Date.now() - first429) / 60_000;
    expect(after.length / minutes).toBeLessThanOrEqual(10 + 2);
    await w.dispose();
  });

  it('two scopes in one pool: independent cursors, one upstream long-poll per shared room, <=3 long-polls', async () => {
    const origin = new FakeOrigin();
    for (const r of ['shared', 'a-only', 'b-only', 'extra-1', 'extra-2']) origin.model.post(r, 'x', 'seed');
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    const mk = (scopeKey: string) =>
      createWatcher({ origin: ORIGIN, scopeKey, store: new MemoryStateStore(), pool, coalesce: { quietMs: 100, maxDelayMs: 500, minWakeIntervalMs: 0 } });
    const a = mk('A');
    const b = mk('B');
    const na: Notice[] = [];
    const nb: Notice[] = [];
    a.onNotice((n) => void na.push(n));
    b.onNotice((n) => void nb.push(n));
    await a.subscribe('shared');
    await a.subscribe('a-only');
    await a.subscribe('extra-1');
    await b.subscribe('shared');
    await b.subscribe('b-only');
    await b.subscribe('extra-2');
    await vi.advanceTimersByTimeAsync(30_000);
    origin.model.post('shared', 'x', 'hello both');
    await until(() => na.length >= 1 && nb.length >= 1);
    expect(na[0]!.rooms.map((r) => r.room)).toEqual(['shared']);
    expect(nb[0]!.rooms.map((r) => r.room)).toEqual(['shared']);
    await a.ack('shared', 2);
    expect(a.status().subscriptions.find((s) => s.room === 'shared')!.acceptedSeq).toBe(2);
    expect(b.status().subscriptions.find((s) => s.room === 'shared')!.acceptedSeq).toBe(1);
    expect(origin.maxInflightWaits).toBeLessThanOrEqual(3);
    for (const [, n] of origin.maxInflightPerRoom) expect(n).toBeLessThanOrEqual(1);
    expect(pool.longPolls.maxObserved).toBeLessThanOrEqual(3);
    // Swept rooms still deliver.
    origin.model.post('extra-2', 'x', 'swept');
    await until(() => nb.some((n) => n.rooms.some((r) => r.room === 'extra-2')), 100, 120_000);
    await a.dispose();
    await b.dispose();
    await pool.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('restart with the same store: no re-notify once marked; at-least-once when not marked', async () => {
    const origin = new FakeOrigin();
    const store = new MemoryStateStore();
    const w1 = watcher(origin, { store });
    const n1: Notice[] = [];
    w1.onNotice(async (n) => {
      n1.push(n);
      await w1.markNotified(n.id);
    });
    await w1.subscribe('r');
    origin.model.post('r', 'a', '1');
    origin.model.post('r', 'a', '2');
    await until(() => n1.length === 1);
    await w1.dispose();

    const w2 = watcher(origin, { store });
    const n2: Notice[] = [];
    w2.onNotice((n) => void n2.push(n)); // deliberately never marks
    await w2.ready;
    await vi.advanceTimersByTimeAsync(45_000);
    expect(n2).toHaveLength(0);
    expect(w2.status().subscriptions[0]).toMatchObject({ headSeq: 2, notifiedSeq: 2, unread: 2 });
    origin.model.post('r', 'a', '3');
    await until(() => n2.length === 1);
    await w2.dispose();

    const w3 = watcher(origin, { store });
    const n3: Notice[] = [];
    w3.onNotice((n) => void n3.push(n));
    await w3.ready;
    await until(() => n3.length === 1);
    expect(n3[0]!.rooms[0]).toMatchObject({ seqFrom: 3, seqTo: 3, newCount: 1 });
    await w3.dispose();
  });

  it('paused backlog of 450 resumes through export with no loss; readPage pages it contiguously', async () => {
    const origin = new FakeOrigin();
    const w = watcher(origin, { coalesce: { quietMs: 100, maxDelayMs: 1000, minWakeIntervalMs: 0 } });
    const notices: Notice[] = [];
    w.onNotice((n) => void notices.push(n));
    origin.model.post('big', 'a', 'seed');
    await w.subscribe('big');
    await w.pause('big');
    for (let i = 0; i < 450; i++) origin.model.post('big', 'a', `m${i}`);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(notices).toHaveLength(0);
    await w.resume('big');
    await until(() => notices.reduce((n, x) => n + x.totalNew, 0) === 450);
    expect(origin.counts().export).toBeGreaterThanOrEqual(1);
    expect(notices.flatMap((n) => n.rooms.flatMap((r) => r.gaps))).toEqual([]);
    let seen = 1;
    for (;;) {
      const page = await w.readPage('big', { max: 200 });
      if (!page.messages.length) break;
      for (const m of page.messages) expect(m.seq).toBe(++seen);
      await w.ack('big', page.toSeq);
    }
    expect(seen).toBe(451);
    await w.dispose();
  });

  it('a 400 for a room stops polling it and records the error', async () => {
    const origin = new FakeOrigin();
    origin.intercept = (rec) =>
      rec.room === 'nope' ? { status: 400, headers: { get: () => null }, body: (async function* () {})(), text: async () => "400 bad name 'nope'", cancel: () => undefined } : undefined;
    const w = watcher(origin);
    await w.subscribe('nope');
    await vi.advanceTimersByTimeAsync(5000);
    expect(w.status().subscriptions[0]).toMatchObject({ status: 'error', lastError: { kind: 'bad-request' } });
    const n = origin.requests.length;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(origin.requests.length - n).toBeLessThanOrEqual(1);
    await w.dispose();
  });

  it('picks up subscriptions written by another process through the store', async () => {
    const origin = new FakeOrigin();
    origin.model.post('late', 'a', 'x');
    const store = new MemoryStateStore();
    const daemon = watcher(origin, { store, limits: { syncIntervalMs: 500 }, coalesce: { quietMs: 50, maxDelayMs: 100, minWakeIntervalMs: 0 } });
    const notices: Notice[] = [];
    daemon.onNotice((n) => void notices.push(n));
    await daemon.ready;
    const cli = watcher(origin, { store, poll: false });
    await cli.subscribe('late', { resolveNow: true });
    await cli.dispose();
    await vi.advanceTimersByTimeAsync(2000);
    expect(daemon.status().subscriptions.map((s) => s.room)).toEqual(['late']);
    origin.model.post('late', 'a', 'y');
    await until(() => notices.length === 1);
    expect(notices[0]!.rooms[0]).toMatchObject({ seqFrom: 2, seqTo: 2 });
    await daemon.dispose();
  });
});
