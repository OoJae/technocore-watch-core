// Room discontinuities on the fake origin under fake time: a recreation whose export fails once,
// an origin whose generation went backwards (data root reset), and `startFrom: 'now'` on rooms
// whose read view cannot name the real high-water mark (reaped with a floor, fully expired).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { HostPool } from '../../src/engine/host-pool.ts';
import type { WatchEvent } from '../../src/engine/types.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import type { Notice } from '../../src/protocol/types.ts';
import type { TransportResponse } from '../../src/transport/http.ts';
import { FakeOrigin } from '../harness/fake-transport.ts';

const ORIGIN = 'http://fake.test';

function watcher(origin: FakeOrigin, over: Partial<WatcherOptions> = {}): Watcher {
  return createWatcher({
    origin: ORIGIN,
    scopeKey: 'scope-d',
    store: new MemoryStateStore(),
    transport: origin,
    limits: { syncIntervalMs: 0, ...over.limits },
    coalesce: { quietMs: 100, maxDelayMs: 500, minWakeIntervalMs: 0, ...over.coalesce },
    ...over,
  } as WatcherOptions);
}

function status503(): TransportResponse {
  return { status: 503, headers: { get: () => null }, body: (async function* () {})(), text: async () => '503 unavailable', cancel: () => undefined };
}

async function until(cond: () => boolean, stepMs = 50, maxMs = 180_000): Promise<void> {
  for (let t = 0; t < maxMs && !cond(); t += stepMs) await vi.advanceTimersByTimeAsync(stepMs);
  expect(cond()).toBe(true);
}

beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
afterEach(() => vi.useRealTimers());

describe('discontinuities (fake origin, fake time)', () => {
  it('a recreation whose export fails once is still processed on a later empty poll', async () => {
    const origin = new FakeOrigin();
    for (let i = 1; i <= 5; i++) origin.model.post('regress', 'a', `g1 ${i}`);
    const w = watcher(origin, { limits: { waitSeconds: 2, syncIntervalMs: 0 } });
    const notices: Notice[] = [];
    w.onNotice((n) => void notices.push(n));
    await w.subscribe('regress');
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 1, headSeq: 5 });
    await vi.advanceTimersByTimeAsync(3000);
    let failExports = 1;
    origin.intercept = (rec) => {
      if (rec.kind === 'other' && rec.path.endsWith('/export') && failExports > 0) {
        failExports--;
        return status503();
      }
      return undefined;
    };
    origin.model.deleteFile('regress');
    origin.model.post('regress', 'b', 'g2 1');
    origin.model.post('regress', 'b', 'g2 2');
    await until(() => notices.some((n) => n.rooms.some((r) => r.regression)));
    expect(failExports).toBe(0);
    const room = notices.flatMap((n) => n.rooms).find((r) => r.regression)!;
    expect(room).toMatchObject({ generation: 2, regression: { fromHead: 5, toLast: 2 }, recreated: { from: 1, to: 2 }, newCount: 2 });
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 2 });
    await w.dispose();
  });

  it('a restarted scope with stored generation 1 still sees the recreation when another scope already follows generation 2', async () => {
    const origin = new FakeOrigin();
    for (let i = 1; i <= 5; i++) origin.model.post('shared-gen', 'a', `g1 ${i}`);
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { waitSeconds: 2, syncIntervalMs: 0 } });
    const storeA = new MemoryStateStore();
    const mk = (scopeKey: string, store: MemoryStateStore) =>
      createWatcher({ origin: ORIGIN, scopeKey, store, pool, limits: { syncIntervalMs: 0 }, coalesce: { quietMs: 100, maxDelayMs: 500, minWakeIntervalMs: 0 } });
    const a1 = mk('A', storeA);
    await a1.subscribe('shared-gen');
    await a1.dispose(); // stored at generation 1, head 5
    origin.model.deleteFile('shared-gen');
    origin.model.post('shared-gen', 'b', 'g2 1');
    const b = mk('B', new MemoryStateStore());
    await b.subscribe('shared-gen'); // follows generation 2 from the start
    await vi.advanceTimersByTimeAsync(5000);
    const a2 = mk('A', storeA);
    const notices: Notice[] = [];
    a2.onNotice((n) => void notices.push(n));
    await a2.ready;
    await until(() => notices.some((n) => n.rooms.some((r) => r.regression)));
    expect(a2.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 1 });
    await a2.dispose();
    await b.dispose();
    await pool.dispose();
  });

  it('an origin whose generation went backwards (data root reset) is resynced, not polled in a tight loop', async () => {
    const origin = new FakeOrigin(undefined, { rateRead: 600 });
    // Generation 3 at head 3.
    origin.model.post('g', 'a', 'gen1');
    origin.model.deleteFile('g');
    origin.model.post('g', 'a', 'gen2');
    origin.model.deleteFile('g');
    origin.model.post('g', 'a', 'gen3 1');
    origin.model.post('g', 'a', 'gen3 2');
    origin.model.post('g', 'a', 'gen3 3');
    const w = watcher(origin);
    const notices: Notice[] = [];
    const events: WatchEvent[] = [];
    w.onNotice((n) => void notices.push(n));
    w.onEvent((e) => events.push(e));
    await w.subscribe('g');
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 3, headSeq: 3 });
    await vi.advanceTimersByTimeAsync(1000);
    origin.model.resetAll();
    for (let i = 1; i <= 10; i++) origin.model.post('g', 'x', `fresh ${i}`);
    const t0 = Date.now();
    await vi.advanceTimersByTimeAsync(60_000);
    const polls = origin.requests.filter((r) => r.room === 'g' && r.startedAt >= t0 && r.kind !== 'export').length;
    expect(polls).toBeLessThanOrEqual(30);
    expect(events.some((e) => e.type === 'stale-generation')).toBe(true);
    const room = notices.flatMap((n) => n.rooms).find((r) => r.regression);
    expect(room).toMatchObject({ room: 'g', generation: 1, recreated: { from: 3, to: 1 }, regression: { fromHead: 3, toLast: 10 }, newCount: 10 });
    expect(w.status().subscriptions[0]).toMatchObject({ status: 'active', generation: 1, headSeq: 10, acceptedSeq: 0, unread: 10 });
    const page = await w.readPage('g');
    expect(page.messages.map((m) => m.text)).toEqual(Array.from({ length: 10 }, (_, i) => `fresh ${i + 1}`));
    await w.dispose();
  });

  it('a single replayed response from an older generation is still ignored', async () => {
    const origin = new FakeOrigin();
    origin.model.post('replayed', 'a', 'gen1');
    const old = origin.model.read('replayed', 0, 200);
    origin.model.deleteFile('replayed');
    origin.model.post('replayed', 'a', 'gen2 1');
    const w = watcher(origin, { limits: { waitSeconds: 2, syncIntervalMs: 0 } });
    const notices: Notice[] = [];
    w.onNotice((n) => void notices.push(n));
    await w.subscribe('replayed');
    let replays = 1;
    origin.intercept = (rec) => {
      if (rec.room === 'replayed' && rec.kind === 'other' && !rec.path.endsWith('/export') && replays > 0) {
        replays--;
        return { status: 200, headers: { get: () => null }, body: (async function* () {})(), text: async () => JSON.stringify(old), cancel: () => undefined };
      }
      return undefined;
    };
    await vi.advanceTimersByTimeAsync(30_000);
    expect(replays).toBe(0);
    expect(notices).toHaveLength(0);
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 1 });
    await w.dispose();
  });

  it("startFrom 'now' on a reaped room (floor kept, file gone) does not report the history as lost", async () => {
    const origin = new FakeOrigin();
    for (let i = 1; i <= 50; i++) origin.model.post('reaped', 'a', `old ${i}`);
    origin.model.reap('reaped');
    expect(origin.model.read('reaped', undefined, 1)).toMatchObject({ last_seq: 0, generation: 1, count: 0 });
    const w = watcher(origin);
    const notices: Notice[] = [];
    w.onNotice(async (n) => {
      notices.push(n);
      await w.markNotified(n.id);
    });
    await w.subscribe('reaped');
    await vi.advanceTimersByTimeAsync(2000);
    origin.model.post('reaped', 'b', 'first after subscribe');
    await until(() => notices.length === 1);
    expect(notices[0]!.rooms[0]).toMatchObject({ room: 'reaped', seqFrom: 51, seqTo: 51, newCount: 1, gaps: [], recreated: { from: 1, to: 2 } });
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 51, notifiedSeq: 51, acceptedSeq: 50, unread: 1, gaps: [] });
    const page = await w.readPage('reaped');
    expect(page).toMatchObject({ fromSeq: 51, toSeq: 51, gaps: [] });
    expect(page.messages.map((m) => m.text)).toEqual(['first after subscribe']);
    await w.dispose();
  });

  it("startFrom 'now' on an e- room whose records all expired does not report the history as lost", async () => {
    const origin = new FakeOrigin();
    for (let i = 1; i <= 30; i++) origin.model.post('e-expired', 'a', `old ${i}`);
    origin.model.expireAll('e-expired');
    const w = watcher(origin);
    const notices: Notice[] = [];
    w.onNotice(async (n) => {
      notices.push(n);
      await w.markNotified(n.id);
    });
    await w.subscribe('e-expired');
    await vi.advanceTimersByTimeAsync(2000);
    origin.model.post('e-expired', 'b', 'new 1');
    origin.model.post('e-expired', 'b', 'new 2');
    await until(() => notices.length === 1);
    expect(notices[0]!.rooms[0]).toMatchObject({ room: 'e-expired', seqFrom: 31, seqTo: 32, newCount: 2, gaps: [] });
    expect(w.status().subscriptions[0]).toMatchObject({ generation: 1, headSeq: 32, acceptedSeq: 30, unread: 2, gaps: [] });
    await w.dispose();
  });
});
