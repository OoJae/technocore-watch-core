// Flood re-poll pacing (Limits.floodRepollPacing) under fake time against the in-memory origin: a
// flood costs about one lead read per notice window instead of one read per message, wake() and a
// flushed or dropped notice cut the nap, nothing is lost, and latency outside floods is unchanged.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HostPool } from '../../src/engine/host-pool.ts';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { DEFAULT_LIMITS, validateLimits, type WatchEvent } from '../../src/engine/types.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import type { Notice } from '../../src/protocol/types.ts';
import { FakeOrigin } from '../harness/fake-transport.ts';

const ORIGIN = 'http://fake.test';
/** One notice window: maxDelay and minWake both 10 s, the same as the long-poll wait cap. */
const COALESCE = { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 };

interface Scope {
  w: Watcher;
  notices: Notice[];
  events: WatchEvent[];
}

function scope(origin: FakeOrigin, over: Partial<WatcherOptions> = {}): Scope {
  const w = createWatcher({
    origin: ORIGIN,
    scopeKey: 'scope-a',
    store: new MemoryStateStore(),
    transport: origin,
    coalesce: COALESCE,
    ...over,
    limits: { syncIntervalMs: 0, ...over.limits },
  });
  const notices: Notice[] = [];
  const events: WatchEvent[] = [];
  w.onNotice(async (n) => {
    notices.push(n);
    await w.markNotified(n.id);
  });
  w.onEvent((e) => events.push(e));
  return { w, notices, events };
}

/** Room reads (long-polls and plain reads) that started in [from, to). */
function reads(origin: FakeOrigin, room: string, from = 0, to = Number.POSITIVE_INFINITY): number {
  return origin.requests.filter((r) => r.room === room && (r.kind === 'read-wait' || r.kind === 'read') && r.startedAt >= from && r.startedAt < to).length;
}

/** Posts `count` messages, one every `everyMs`, advancing fake time as it goes. */
async function flood(origin: FakeOrigin, room: string, count: number, everyMs: number, perTick = 1): Promise<void> {
  for (let i = 0; i < count; i++) {
    for (let k = 0; k < perTick; k++) origin.model.post(room, 'flooder', `m${i}.${k}`);
    await vi.advanceTimersByTimeAsync(everyMs);
  }
}

async function until(cond: () => boolean, stepMs = 10, maxMs = 120_000): Promise<void> {
  for (let t = 0; t < maxMs && !cond(); t += stepMs) await vi.advanceTimersByTimeAsync(stepMs);
  expect(cond()).toBe(true);
}

/** Notices must cover (from, to] exactly once, in order, without gaps. */
function expectContiguous(notices: Notice[], room: string, from: number, to: number): void {
  let next = from + 1;
  let total = 0;
  for (const n of notices) {
    for (const r of n.rooms.filter((x) => x.room === room)) {
      expect(r.gaps).toEqual([]);
      expect(r.seqFrom).toBe(next);
      expect(r.newCount).toBe(r.seqTo - r.seqFrom + 1);
      next = r.seqTo + 1;
      total += r.newCount;
    }
  }
  expect(next - 1).toBe(to);
  expect(total).toBe(to - from);
}

beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-14T00:00:00Z') }));
afterEach(() => vi.useRealTimers());

describe('flood re-poll pacing (fake time)', () => {
  it('a 60 s flood at 5 msg/s costs a few reads per notice window, not one per message, and loses nothing', async () => {
    const results: Record<string, { reads: number; notices: number }> = {};
    for (const pacing of [true, false]) {
      const origin = new FakeOrigin();
      const s = scope(origin, { limits: { floodRepollPacing: pacing } });
      await s.w.subscribe('flood');
      await vi.advanceTimersByTimeAsync(100);
      const t0 = Date.now();
      await flood(origin, 'flood', 300, 200);
      const floodReads = reads(origin, 'flood', t0, Date.now());
      await vi.advanceTimersByTimeAsync(30_000);
      expectContiguous(s.notices, 'flood', 0, 300);
      expect(s.w.status().subscriptions[0]).toMatchObject({ headSeq: 300, notifiedSeq: 300, gaps: [] });
      expect(origin.counts().export).toBe(0);
      results[pacing ? 'paced' : 'unpaced'] = { reads: floodReads, notices: s.notices.length };
      await s.w.dispose();
      expect(vi.getTimerCount()).toBe(0);
    }
    const paced = results.paced!;
    const unpaced = results.unpaced!;
    // Without pacing every message wakes the long-poll: one read per message.
    expect(unpaced.reads).toBeGreaterThanOrEqual(280);
    // With it: the first window's quiet-driven reads, then one lead read before each flush and one
    // right after it (the flush cuts the nap), with 6 windows in 60 s.
    expect(paced.reads).toBeLessThanOrEqual(2 * paced.notices + 8);
    expect(paced.reads * 10).toBeLessThan(unpaced.reads);
    // The same notices either way: pacing changes when the poll lands, not what the scope is told.
    expect(Math.abs(paced.notices - unpaced.notices)).toBeLessThanOrEqual(1);
  });

  it('the paced read lands just before the notice is due, so the flush carries fresh data', async () => {
    const origin = new FakeOrigin();
    const s = scope(origin);
    await s.w.subscribe('flood');
    await vi.advanceTimersByTimeAsync(100);
    await flood(origin, 'flood', 250, 200);
    const paced = s.events.filter((e) => e.type === 'poll-paced');
    expect(paced.length).toBeGreaterThan(0);
    const leads: number[] = [];
    for (const n of s.notices.slice(1)) {
      const at = Date.parse(n.createdAt);
      const before = origin.requests.filter((r) => r.room === 'flood' && r.startedAt < at).at(-1);
      if (before) leads.push(at - before.startedAt);
    }
    expect(leads.length).toBeGreaterThanOrEqual(3);
    // repollLeadMs (500) before the flush, plus at most the fake origin's wake latency.
    for (const lead of leads) expect(lead).toBeLessThanOrEqual(DEFAULT_LIMITS.repollLeadMs + 100);
    await s.w.dispose();
  });

  it('wake() cuts a paced nap (attach of another scope, slot change)', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    const polls: number[] = [];
    pool.onEvent((e) => {
      if (e.type === 'poll-start' && e.room === 'flood') polls.push(Date.now());
    });
    let longNapAt = 0;
    let longNapMs = 0;
    pool.onEvent((e) => {
      if (e.type === 'poll-paced' && e.room === 'flood' && Number(e.detail?.ms) >= 5000) {
        longNapAt = Date.now();
        longNapMs = Number(e.detail?.ms);
      }
    });
    const a = scope(origin, { pool });
    await a.w.subscribe('flood');
    await vi.advanceTimersByTimeAsync(100);
    for (let i = 0; i < 200 && !longNapAt; i++) await flood(origin, 'flood', 1, 200);
    expect(longNapAt).toBeGreaterThan(0);
    const napStart = longNapAt;

    // Nothing polls during the nap on its own...
    await vi.advanceTimersByTimeAsync(2000);
    expect(polls.filter((t) => t > napStart)).toEqual([]);
    // ...until the pool is woken (what a slot change does), which re-polls at once.
    const woke = Date.now();
    pool.wake('flood');
    await vi.advanceTimersByTimeAsync(20);
    expect(polls.filter((t) => t >= woke)).toHaveLength(1);
    expect(woke + 20).toBeLessThan(napStart + longNapMs);

    // Attach: a second scope subscribing to the same room during a paced nap.
    longNapAt = 0;
    for (let i = 0; i < 200 && !longNapAt; i++) await flood(origin, 'flood', 1, 200);
    expect(longNapAt).toBeGreaterThan(0);
    const b = scope(origin, { pool, scopeKey: 'scope-b' });
    const subscribing = Date.now();
    await b.w.subscribe('flood');
    await vi.advanceTimersByTimeAsync(20);
    expect(polls.filter((t) => t >= subscribing).length).toBeGreaterThanOrEqual(1);
    expect(Date.now()).toBeLessThan(longNapAt + longNapMs);

    await a.w.dispose();
    await b.w.dispose();
    await pool.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('shared room: a notice flushing during the nap cuts it; a scope leaving with a notice still pending elsewhere does not', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    const polls: number[] = [];
    let longNapAt = 0;
    pool.onEvent((e) => {
      if (e.room !== 'shared') return;
      if (e.type === 'poll-start') polls.push(Date.now());
      if (e.type === 'poll-paced' && Number(e.detail?.ms) >= 5000) longNapAt = Date.now();
    });
    const a = scope(origin, { pool, scopeKey: 'scope-a' });
    const b = scope(origin, { pool, scopeKey: 'scope-b' });
    await a.w.subscribe('shared');
    await b.w.subscribe('shared');
    await vi.advanceTimersByTimeAsync(100);
    for (let i = 0; i < 200 && !longNapAt; i++) await flood(origin, 'shared', 1, 200);
    expect(longNapAt).toBeGreaterThan(0);
    expect(a.w.status().pendingNotice && b.w.status().pendingNotice).toBe(true);

    // B leaves: A still has a pending notice, so the nap goes on.
    const left = Date.now();
    await b.w.unsubscribe('shared');
    await vi.advanceTimersByTimeAsync(1500);
    expect(polls.filter((t) => t >= left)).toEqual([]);

    // A's notice goes out early: nothing is pending any more, so the room is polled right away.
    const flushed = Date.now();
    a.w.flushNotices();
    await vi.advanceTimersByTimeAsync(20);
    expect(polls.filter((t) => t >= flushed)).toHaveLength(1);

    await a.w.dispose();
    await b.w.dispose();
    await pool.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('shared room: no pacing while any scope following it has nothing pending', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    const a = scope(origin, { pool, scopeKey: 'scope-a' });
    // B notifies on every observation, so it never holds a notice when the poller decides.
    const b = scope(origin, { pool, scopeKey: 'scope-b', coalesce: { quietMs: 0, maxDelayMs: 0, minWakeIntervalMs: 0 } });
    await a.w.subscribe('shared');
    await b.w.subscribe('shared');
    await vi.advanceTimersByTimeAsync(100);
    const t0 = Date.now();
    await flood(origin, 'shared', 100, 200);
    // Each paced nap B's immediate flush cuts at once, so the room still reads once per message.
    expect(reads(origin, 'shared', t0, Date.now())).toBeGreaterThanOrEqual(90);
    await vi.advanceTimersByTimeAsync(30_000);
    expectContiguous(a.notices, 'shared', 0, 100);
    expectContiguous(b.notices, 'shared', 0, 100);
    await a.w.dispose();
    await b.w.dispose();
    await pool.dispose();
  });

  it('dispose or unsubscribe during a paced nap stops the poller and leaves no timers', async () => {
    for (const how of ['dispose', 'unsubscribe'] as const) {
      const origin = new FakeOrigin();
      const s = scope(origin);
      let longNap = false;
      s.w.onEvent((e) => {
        if (e.type === 'poll-paced' && Number(e.detail?.ms) >= 5000) longNap = true;
      });
      await s.w.subscribe('flood');
      await vi.advanceTimersByTimeAsync(100);
      for (let i = 0; i < 200 && !longNap; i++) await flood(origin, 'flood', 1, 200);
      expect(longNap).toBe(true);
      const t = Date.now();
      if (how === 'dispose') {
        await s.w.dispose();
        expect(vi.getTimerCount()).toBe(0);
      } else {
        await s.w.unsubscribe('flood');
      }
      origin.model.post('flood', 'flooder', 'after');
      await vi.advanceTimersByTimeAsync(30_000);
      expect(reads(origin, 'flood', t)).toBe(0);
      if (how === 'unsubscribe') {
        expect(s.w.status().subscriptions).toEqual([]);
        await s.w.dispose();
        expect(vi.getTimerCount()).toBe(0);
      }
    }
  });

  it('seen-by-poll latency outside floods is the same with pacing on and off', async () => {
    const seen: Record<string, number[]> = {};
    for (const pacing of [true, false]) {
      const origin = new FakeOrigin();
      const s = scope(origin, { limits: { floodRepollPacing: pacing } });
      await s.w.subscribe('trickle');
      await vi.advanceTimersByTimeAsync(100);
      const latencies: number[] = [];
      // Isolated messages (each starts a notice) and a short burst of 3 in one poll.
      for (let i = 0; i < 6; i++) {
        const posted = Date.now();
        const seq = origin.model.post('trickle', 'a', `single ${i}`).seq;
        if (i === 3) {
          origin.model.post('trickle', 'a', 'burst b');
          origin.model.post('trickle', 'a', 'burst c');
        }
        await until(() => s.events.some((e) => e.type === 'activity' && Number(e.detail?.to) >= seq));
        latencies.push(Date.now() - posted);
        await vi.advanceTimersByTimeAsync(15_000);
      }
      seen[pacing ? 'paced' : 'unpaced'] = latencies;
      expect(s.events.filter((e) => e.type === 'poll-paced').every((e) => Number(e.detail?.ms) <= DEFAULT_LIMITS.floodRepollMinMs)).toBe(true);
      await s.w.dispose();
    }
    expect(seen.paced).toEqual(seen.unpaced);
    for (const ms of seen.paced!) expect(ms).toBeLessThanOrEqual(100);
  });

  it('never naps long enough for the next read to overflow one page (no export backfill at 250 msg/s)', async () => {
    const origin = new FakeOrigin();
    const s = scope(origin);
    await s.w.subscribe('fast');
    await vi.advanceTimersByTimeAsync(100);
    await flood(origin, 'fast', 150, 200, 50); // 50 messages every 200 ms for 30 s
    await vi.advanceTimersByTimeAsync(30_000);
    expectContiguous(s.notices, 'fast', 0, 7500);
    // Too fast to nap even the shortest pace without overflowing a page: no nap, no export.
    expect(origin.counts().export).toBe(0);
    expect(s.events.filter((e) => e.type === 'poll-paced')).toEqual([]);
    await s.w.dispose();
  });

  it('a flood that speeds up during a long nap costs one export and still loses nothing', async () => {
    const origin = new FakeOrigin();
    const s = scope(origin);
    let longNap = false;
    s.w.onEvent((e) => {
      if (e.type === 'poll-paced' && Number(e.detail?.ms) >= 5000) longNap = true;
    });
    await s.w.subscribe('surge');
    await vi.advanceTimersByTimeAsync(100);
    let posted = 0;
    for (let i = 0; i < 200 && !longNap; i++, posted++) await flood(origin, 'surge', 1, 200);
    expect(longNap).toBe(true);
    await flood(origin, 'surge', 10, 200, 100); // 1,000 messages in 2 s, all inside the nap
    posted += 1000;
    await vi.advanceTimersByTimeAsync(40_000);
    expect(origin.counts().export).toBe(1);
    expectContiguous(s.notices, 'surge', 0, posted);
    await s.w.dispose();
  });

  it('never splits a burst or adds notices, including when quietMs is shorter than lead + floor', async () => {
    const configs = [
      { name: 'quiet shorter than lead + floor', coalesce: { quietMs: 1000, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 }, burst: [15, 200] },
      { name: 'defaults', coalesce: { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000 }, burst: [15, 200] },
      { name: 'tiny windows, no min wake', coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 }, burst: [200, 100] },
    ] as const;
    for (const c of configs) {
      const shapes: Record<string, string[]> = {};
      for (const pacing of [true, false]) {
        const origin = new FakeOrigin();
        const s = scope(origin, { coalesce: c.coalesce, limits: { floodRepollPacing: pacing } });
        await s.w.subscribe('burst');
        await vi.advanceTimersByTimeAsync(100);
        await flood(origin, 'burst', c.burst[0], c.burst[1]);
        await vi.advanceTimersByTimeAsync(60_000);
        expectContiguous(s.notices, 'burst', 0, c.burst[0]);
        shapes[pacing ? 'paced' : 'unpaced'] = s.notices.map((n) => `${n.rooms[0]!.seqFrom}-${n.rooms[0]!.seqTo}`);
        await s.w.dispose();
      }
      expect(shapes.paced, c.name).toEqual(shapes.unpaced);
    }
  });

  it('validates the pacing limits', () => {
    expect(() => new HostPool({ origin: ORIGIN, limits: { repollLeadMs: -1 } })).toThrow(RangeError);
    expect(() => new HostPool({ origin: ORIGIN, limits: { floodRepollMinMs: 50 } })).toThrow(RangeError);
    expect(() => new HostPool({ origin: ORIGIN, limits: { floodRepollMinMs: Number.NaN } })).toThrow(RangeError);
    expect(() => new HostPool({ origin: ORIGIN, limits: { floodRepollPacing: 'yes' as unknown as boolean } })).toThrow(TypeError);
    expect(() => createWatcher({ origin: ORIGIN, scopeKey: 's', store: new MemoryStateStore(), limits: { repollLeadMs: 10_001 } })).toThrow(RangeError);
    // An explicit undefined keeps the default instead of erasing it.
    const pool = new HostPool({ origin: ORIGIN, limits: { repollLeadMs: undefined, floodRepollMinMs: 2000 } });
    expect(pool.limits).toMatchObject({ floodRepollPacing: true, repollLeadMs: 500, floodRepollMinMs: 2000 });
    expect(validateLimits({ repollLeadMs: 0, floodRepollMinMs: 10_000 })).toEqual({ repollLeadMs: 0, floodRepollMinMs: 10_000 });
    void pool.dispose();
  });
});
