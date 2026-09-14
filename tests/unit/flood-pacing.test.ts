// Flood re-poll pacing (Limits.floodRepollPacing) under fake time against the in-memory origin: once a
// notice is held by maxDelayMs/minWakeIntervalMs a flood costs a few reads per notice window instead of
// one read per message, wake() and a flushed or dropped notice cut the nap, nothing is lost, and
// notices go out at the same times with the same messages as without pacing.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HostPool } from '../../src/engine/host-pool.ts';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { DEFAULT_COALESCE, DEFAULT_LIMITS, validateLimits, type Observation, type PendingNoticeTiming, type RoomMember, type WatchEvent } from '../../src/engine/types.ts';
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
  it('a 60 s flood at 5 msg/s costs a few reads per notice window once maxDelayMs holds the notice, and loses nothing', async () => {
    const results: Record<string, { reads: number; perWindow: number[]; notices: string[] }> = {};
    for (const pacing of [true, false]) {
      const origin = new FakeOrigin();
      const s = scope(origin, { limits: { floodRepollPacing: pacing } });
      await s.w.subscribe('flood');
      await vi.advanceTimersByTimeAsync(100);
      const t0 = Date.now();
      await flood(origin, 'flood', 300, 200);
      const end = Date.now();
      await vi.advanceTimersByTimeAsync(30_000);
      expectContiguous(s.notices, 'flood', 0, 300);
      expect(s.w.status().subscriptions[0]).toMatchObject({ headSeq: 300, notifiedSeq: 300, gaps: [] });
      expect(origin.counts().export).toBe(0);
      // Reads per notice window: [flood start, first notice), [first notice, second notice), ...
      const bounds = [t0, ...s.notices.map((n) => Date.parse(n.createdAt)).filter((at) => at < end), end];
      results[pacing ? 'paced' : 'unpaced'] = {
        reads: reads(origin, 'flood', t0, end),
        perWindow: bounds.slice(1).map((to, i) => reads(origin, 'flood', bounds[i]!, to)),
        notices: s.notices.map((n) => `${n.rooms[0]!.seqFrom}-${n.rooms[0]!.seqTo}@${Date.parse(n.createdAt) - t0}`),
      };
      await s.w.dispose();
      expect(vi.getTimerCount()).toBe(0);
    }
    const paced = results.paced!;
    const unpaced = results.unpaced!;
    // Without pacing every message wakes the long-poll: one read per message.
    expect(unpaced.reads).toBeGreaterThanOrEqual(280);
    // The same notices, at the same times: pacing changes when the poll lands, not what the scope is told.
    expect(paced.notices).toEqual(unpaced.notices);
    expect(paced.notices).toHaveLength(6);
    // The first window is held open by quietMs until maxDelayMs caps it, so it is read live (never more
    // than unpaced). Every later window: a paced read or two, then live reads in the 500 ms lead window.
    const [first, ...later] = paced.perWindow;
    expect(first).toBeLessThanOrEqual(unpaced.perWindow[0]!);
    expect(later).toHaveLength(5);
    for (const n of later) expect(n, paced.perWindow.join(', ')).toBeLessThanOrEqual(6);
    expect(paced.reads * 4).toBeLessThan(unpaced.reads);
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

/** Offsets (ms) for `count` posts, one every `everyMs`, starting at `from`. */
function every(count: number, everyMs: number, from = 0): number[] {
  return Array.from({ length: count }, (_, i) => from + i * everyMs);
}

interface ScheduleRun {
  /** `seqFrom-seqTo@createdAt` per notice, createdAt relative to the first post slot. */
  notices: string[];
  /** `to@at` per activity event (when the poll saw each step), relative to the first post slot. */
  seen: string[];
  paced: number;
}

/** Posts one message at each offset (ms after t0), then lets every notice go out. */
async function runSchedule(pacing: boolean, coalesce: WatcherOptions['coalesce'], schedule: readonly number[]): Promise<ScheduleRun> {
  const origin = new FakeOrigin();
  const s = scope(origin, { coalesce, limits: { floodRepollPacing: pacing } });
  await s.w.subscribe('r');
  await vi.advanceTimersByTimeAsync(100);
  const t0 = Date.now();
  for (const [i, off] of [...schedule].sort((a, b) => a - b).entries()) {
    const wait = t0 + off - Date.now();
    if (wait > 0) await vi.advanceTimersByTimeAsync(wait);
    origin.model.post('r', 'a', `m${i}`);
  }
  await vi.advanceTimersByTimeAsync(90_000);
  expectContiguous(s.notices, 'r', 0, schedule.length);
  const run: ScheduleRun = {
    notices: s.notices.map((n) => `${n.rooms[0]!.seqFrom}-${n.rooms[0]!.seqTo}@${Date.parse(n.createdAt) - t0}`),
    seen: s.events.filter((e) => e.type === 'activity').map((e) => `${e.detail?.to}@${Date.parse(e.at) - t0}`),
    paced: s.events.filter((e) => e.type === 'poll-paced').length,
  };
  await s.w.dispose();
  expect(vi.getTimerCount()).toBe(0);
  return run;
}

/** A RoomMember driven by the test: head follows what it observes; `timing` is its pending notice. */
class ScriptedMember implements RoomMember {
  readonly id: string;
  readonly room: string;
  headSeq: number | undefined;
  gen: number | undefined;
  prepares = 0;
  pendingNotice?: () => PendingNoticeTiming | undefined;

  constructor(room: string, head: number | undefined, timing?: (m: ScriptedMember) => PendingNoticeTiming | undefined) {
    this.room = room;
    this.id = `scripted#${room}#${Math.random()}`;
    this.headSeq = head;
    if (timing) this.pendingNotice = () => timing(this);
  }

  prepare(): Promise<boolean> {
    this.prepares++;
    return Promise.resolve(this.headSeq !== undefined);
  }

  head(): number | undefined {
    return this.headSeq;
  }

  generation(): number | undefined {
    return this.gen;
  }

  active(): boolean {
    return this.headSeq !== undefined;
  }

  onObservation(obs: Observation): Promise<'ok' | 'retry'> {
    this.gen = obs.view.generation;
    const last = obs.view.messages.at(-1)?.seq;
    if (last !== undefined && this.headSeq !== undefined) this.headSeq = Math.max(this.headSeq, last);
    return Promise.resolve('ok');
  }

  onFatal(): void {}
}

/** A notice due at `dueAt` that no activity can move. */
function fixedAt(dueAt: number): PendingNoticeTiming {
  return { dueAt, movableFrom: dueAt };
}

describe('flood re-poll pacing fidelity (fake time)', () => {
  it('never changes when notices go out or what they cover while quietMs can still move the due time', async () => {
    const defaults = { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000 };
    const cases: { name: string; coalesce: WatcherOptions['coalesce']; schedule: number[]; sameSeen?: boolean }[] = [
      // Uneven traffic outside floods: pacing must not stretch the quiet window or merge notices.
      { name: 'defaults, posts at 0/100/2400', coalesce: DEFAULT_COALESCE, schedule: [0, 100, 2400], sameSeen: true },
      // (Posts 4..6 arrive while a notice held by minWakeIntervalMs is pending: those are read at the paced rate.)
      { name: 'defaults, posts at 0/100/2400/4700/7000/9300', coalesce: DEFAULT_COALESCE, schedule: [0, 100, 2400, 4700, 7000, 9300] },
      { name: 'defaults, posts at 0/300/600/3000', coalesce: DEFAULT_COALESCE, schedule: [0, 300, 600, 3000], sameSeen: true },
      // Short bursts from an idle room: the notice must not go out later.
      { name: '8 posts 150 ms apart', coalesce: { ...defaults }, schedule: every(8, 150), sameSeen: true },
      { name: '5 posts 300 ms apart', coalesce: { ...defaults }, schedule: every(5, 300), sameSeen: true },
      { name: '30 posts 200 ms apart', coalesce: { ...defaults }, schedule: every(30, 200), sameSeen: true },
      { name: '8 posts 400 ms apart', coalesce: { ...defaults }, schedule: every(8, 400), sameSeen: true },
      // quietMs <= repollLeadMs, and a burst that runs across a due time held by minWakeIntervalMs.
      ...[5000, 5130, 5270, 5410, 5555, 6000].flatMap((start) => [
        { name: `quiet 500, burst from ${start} ms across the min-wake due`, coalesce: { quietMs: 500, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 }, schedule: [0, ...every(80, 100, start)] },
        { name: `quiet 200, burst from ${start} ms across the min-wake due`, coalesce: { quietMs: 200, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 }, schedule: [0, ...every(80, 100, start)] },
      ]),
    ];
    for (const c of cases) {
      const paced = await runSchedule(true, c.coalesce, c.schedule);
      const unpaced = await runSchedule(false, c.coalesce, c.schedule);
      expect(paced.notices, c.name).toEqual(unpaced.notices);
      if (c.sameSeen) {
        // No notice held by maxDelayMs/minWakeIntervalMs was pending: the poll sees every message when it would unpaced.
        expect(paced.seen, c.name).toEqual(unpaced.seen);
      }
    }
  });

  it('a paced read lands at most repollLeadMs before a fixed flush, also when the flood rate limits the naps', async () => {
    const rates: [everyMs: number, perTick: number][] = [
      [200, 1], // 5 msg/s: naps are not rate-limited
      [50, 3], // 60 msg/s
      [40, 3], // 75 msg/s
      [50, 4], // 80 msg/s
      [100, 10], // 100 msg/s
    ];
    for (const [everyMs, perTick] of rates) {
      const origin = new FakeOrigin();
      const s = scope(origin);
      await s.w.subscribe('flood');
      await vi.advanceTimersByTimeAsync(100);
      const postedAt: number[] = [];
      for (let t = 0; t < 60_000; t += everyMs) {
        for (let k = 0; k < perTick; k++) postedAt[origin.model.post('flood', 'flooder', `m${t}.${k}`).seq] = Date.now();
        await vi.advanceTimersByTimeAsync(everyMs);
      }
      const posted = postedAt.length - 1;
      await vi.advanceTimersByTimeAsync(30_000);
      expectContiguous(s.notices, 'flood', 0, posted);
      expect(origin.counts().export).toBe(0);
      expect(s.events.some((e) => e.type === 'poll-paced'), `${perTick}/${everyMs} ms`).toBe(true);
      // For each notice: how long before it went out was the oldest message it does not cover posted?
      const ages = s.notices.map((n) => {
        const at = Date.parse(n.createdAt);
        const next = n.rooms[0]!.seqTo + 1;
        return next <= posted && postedAt[next]! < at ? at - postedAt[next]! : 0;
      });
      // repollLeadMs, plus at most the fake origin's wake latency (50 ms) and one posting tick.
      for (const age of ages) expect(age, `${perTick} every ${everyMs} ms: ${ages.join(', ')}`).toBeLessThanOrEqual(DEFAULT_LIMITS.repollLeadMs + 50 + everyMs);
      // Waits shorter than floodRepollMinMs are read live, and a nap the rate cuts short leaves the rest
      // as one more nap rather than a stretch of live reads (80 msg/s: 276 reads; 341 without that).
      const naps = s.events.filter((e) => e.type === 'poll-paced').map((e) => Number(e.detail?.ms));
      expect(Math.min(...naps)).toBeGreaterThanOrEqual(DEFAULT_LIMITS.floodRepollMinMs);
      expect(reads(origin, 'flood'), `${perTick} every ${everyMs} ms`).toBeLessThanOrEqual(290);
      await s.w.dispose();
    }
  });

  it('a wake from before the poll started (slot assignment on attach) does not skip the first paced nap', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin });
    const t0 = Date.now();
    const polls: number[] = [];
    const naps: number[] = [];
    pool.onEvent((e) => {
      if (e.room !== 'solo') return;
      if (e.type === 'poll-start') polls.push(Date.now());
      if (e.type === 'poll-paced') naps.push(Date.now());
    });
    const member = new ScriptedMember('solo', 0, (m) => (m.headSeq ? fixedAt(t0 + 60_000) : undefined));
    const detach = pool.attach(member);
    await vi.advanceTimersByTimeAsync(100);
    origin.model.post('solo', 'a', 'one');
    await until(() => naps.length === 1);
    const napAt = naps[0]!;
    origin.model.post('solo', 'a', 'two');
    await vi.advanceTimersByTimeAsync(900);
    // The first paced nap (floodRepollMinMs) runs in full: no poll until it ends.
    expect(polls.filter((t) => t > napAt)).toEqual([]);
    await vi.advanceTimersByTimeAsync(200);
    expect(polls.filter((t) => t > napAt)).toHaveLength(1);
    detach();
    await pool.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('no pacing while another member of the room is still resolving its start', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin });
    const events: WatchEvent[] = [];
    pool.onEvent((e) => events.push(e));
    const t0 = Date.now();
    const ready = new ScriptedMember('mixed', 0, (m) => (m.headSeq ? fixedAt(t0 + 120_000) : undefined));
    // Its start never resolves (prepare() keeps returning false), so it has no head and is not active.
    const starting = new ScriptedMember('mixed', undefined, () => fixedAt(t0 + 120_000));
    const offA = pool.attach(ready);
    const offB = pool.attach(starting);
    await vi.advanceTimersByTimeAsync(100);
    const before = starting.prepares;
    const t1 = Date.now();
    await flood(origin, 'mixed', 40, 200);
    expect(ready.headSeq).toBe(40);
    expect(events.filter((e) => e.type === 'poll-paced')).toEqual([]);
    // Every pass retries the pending start, so it is not held back by a nap.
    expect(starting.prepares - before).toBeGreaterThanOrEqual(35);
    expect(reads(origin, 'mixed', t1, Date.now())).toBeGreaterThanOrEqual(35);
    offA();
    offB();
    await pool.dispose();
  });

  it('unsubscribing the room that held a scope\'s only pending notice cuts a paced nap on its other room', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    // quietMs above maxDelayMs: each notice is due 15 s after its first activity, whatever follows.
    const fixed = { quietMs: 20_000, maxDelayMs: 15_000, minWakeIntervalMs: 0 };
    const t = scope(origin, { pool, scopeKey: 'scope-t', coalesce: fixed });
    const s = scope(origin, { pool, scopeKey: 'scope-s', coalesce: fixed });
    const r2Polls: number[] = [];
    const r2Naps: { at: number; ms: number }[] = [];
    pool.onEvent((e) => {
      if (e.room !== 'r2') return;
      if (e.type === 'poll-start') r2Polls.push(Date.now());
      if (e.type === 'poll-paced') r2Naps.push({ at: Date.now(), ms: Number(e.detail?.ms) });
    });
    await t.w.subscribe('r2');
    await s.w.subscribe('r1');
    await vi.advanceTimersByTimeAsync(100);
    origin.model.post('r1', 'a', 'x'); // S: a notice pending for r1 only
    origin.model.post('r2', 'a', 'm1'); // T: a notice pending for r2
    await vi.advanceTimersByTimeAsync(500);
    origin.model.post('r2', 'a', 'm2');
    await until(() => r2Naps.some((n) => n.ms >= 5000));
    // Posted during r2's nap, so T has not seen them; S then joins r2 at its head (seq 4).
    origin.model.post('r2', 'a', 'm3');
    origin.model.post('r2', 'a', 'm4');
    await vi.advanceTimersByTimeAsync(100);
    const joined = Date.now();
    await s.w.subscribe('r2');
    await vi.advanceTimersByTimeAsync(200);
    // The wake re-read r2 from T's head: seqs 3..4 went to T's notice only, and r2 naps again because
    // S still has its r1 notice pending.
    expect(t.w.status().subscriptions[0]).toMatchObject({ room: 'r2', headSeq: 4 });
    expect(r2Naps.filter((n) => n.at >= joined && n.ms >= 5000)).toHaveLength(1);
    const napAt = r2Naps.at(-1)!.at;
    expect(r2Polls.filter((p) => p > napAt)).toEqual([]);
    // S leaves r1: S has nothing pending any more, so r2 must be read again at once.
    const left = Date.now();
    await s.w.unsubscribe('r1');
    expect(s.w.status().pendingNotice).toBe(false);
    await vi.advanceTimersByTimeAsync(20);
    expect(r2Polls.filter((p) => p >= left)).toHaveLength(1);
    await s.w.dispose();
    await t.w.dispose();
    await pool.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('a member without pendingNotice(), or one that throws, never stops the room from being read', async () => {
    for (const pacing of [true, false]) {
      for (const kind of ['missing', 'throws'] as const) {
        const origin = new FakeOrigin();
        const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { floodRepollPacing: pacing } });
        const errors: WatchEvent[] = [];
        pool.onEvent((e) => {
          if (e.type === 'error') errors.push(e);
        });
        const member = new ScriptedMember('plain', 0);
        if (kind === 'throws') {
          member.pendingNotice = () => {
            throw new Error('boom');
          };
        }
        const off = pool.attach(member);
        await vi.advanceTimersByTimeAsync(100);
        await flood(origin, 'plain', 5, 1000);
        await vi.advanceTimersByTimeAsync(1000);
        expect(errors, `${kind}, pacing ${pacing}`).toEqual([]);
        expect(member.headSeq, `${kind}, pacing ${pacing}`).toBe(5);
        off();
        await pool.dispose();
      }
    }
  });

  it('a pending notice due beyond the Date range never stops polling', async () => {
    // Direct: a member whose notice is due at Number.MAX_SAFE_INTEGER.
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin });
    const events: WatchEvent[] = [];
    pool.onEvent((e) => events.push(e));
    const member = new ScriptedMember('far', 0, (m) => (m.headSeq ? fixedAt(Number.MAX_SAFE_INTEGER) : undefined));
    const off = pool.attach(member);
    await vi.advanceTimersByTimeAsync(100);
    await flood(origin, 'far', 5, 3000);
    await vi.advanceTimersByTimeAsync(12_000);
    expect(events.filter((e) => e.type === 'error')).toEqual([]);
    expect(member.headSeq).toBe(5);
    const paced = events.filter((e) => e.type === 'poll-paced');
    expect(paced.length).toBeGreaterThan(0);
    expect(paced[0]!.detail?.noticeAt).toBe(Number.MAX_SAFE_INTEGER);
    off();
    await pool.dispose();

    // Through createWatcher: "effectively never" coalescer timings.
    const origin2 = new FakeOrigin();
    const s = scope(origin2, { coalesce: { quietMs: Number.MAX_SAFE_INTEGER, maxDelayMs: Number.MAX_SAFE_INTEGER, minWakeIntervalMs: 0 } });
    await s.w.subscribe('far');
    await vi.advanceTimersByTimeAsync(100);
    await flood(origin2, 'far', 5, 3000);
    await vi.advanceTimersByTimeAsync(12_000);
    expect(s.events.filter((e) => e.type === 'error')).toEqual([]);
    const st = s.w.status();
    expect(st.subscriptions[0]).toMatchObject({ headSeq: 5 });
    expect(st.pendingNotice).toBe(true);
    expect(s.notices).toEqual([]);
    await s.w.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });
});
