import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Coalescer, type FlushBatch, type RoomDelta } from '../../src/engine/coalescer.ts';
import { DEFAULT_COALESCE } from '../../src/engine/types.ts';
import { renderNotice } from '../../src/protocol/render.ts';
import type { Message } from '../../src/protocol/types.ts';

const msgs = (from: number, n: number, sender = 'a'): Message[] => Array.from({ length: n }, (_, i) => ({ seq: from + i, ts: 't', from: sender, text: `m${from + i}` }));

function delta(room: string, from: number, n: number, over: Partial<RoomDelta> = {}): RoomDelta {
  return { room, generation: 1, delivered: msgs(from + 1, n), gaps: [], rangeFrom: from, rangeTo: from + n, transport: 'http-longpoll', fetchedAt: new Date().toISOString(), ...over };
}

describe('Coalescer (fake time)', () => {
  beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
  afterEach(() => vi.useRealTimers());

  it('flushes after quietMs of silence, merging a burst into one notice', async () => {
    const out: FlushBatch[] = [];
    const c = new Coalescer((b) => out.push(b));
    for (let i = 0; i < 8; i++) {
      c.add(delta('lobby', i, 1));
      await vi.advanceTimersByTimeAsync(100);
    }
    await vi.advanceTimersByTimeAsync(1800);
    expect(out).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(200);
    expect(out).toHaveLength(1);
    expect(out[0]!.rooms[0]).toMatchObject({ room: 'lobby', newCount: 8, seqFrom: 1, seqTo: 8, unsignedSenders: 1, signedSenders: 0 });
    expect(out[0]!.totalNew).toBe(8);
  });

  it('flushes by maxDelayMs under continuous activity, then respects minWakeIntervalMs', async () => {
    const out: { at: number; b: FlushBatch }[] = [];
    const c = new Coalescer((b) => out.push({ at: Date.now(), b }), { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000 });
    const start = Date.now();
    let seq = 0;
    const durationMs = 5 * 60_000;
    while (Date.now() - start < durationMs) {
      c.add(delta('flood', seq, 5));
      seq += 5;
      await vi.advanceTimersByTimeAsync(1000); // 5 msg/s
    }
    const during = out.filter((o) => o.at - start <= durationMs);
    expect(out[0]!.at - start).toBe(10_000);
    expect(during.length).toBeLessThanOrEqual(Math.ceil(durationMs / 30_000));
    for (let i = 1; i < out.length; i++) expect(out[i]!.at - out[i - 1]!.at).toBeGreaterThanOrEqual(30_000);
    await vi.advanceTimersByTimeAsync(60_000);
    const total = out.reduce((n, o) => n + o.b.totalNew, 0);
    expect(total).toBe(seq); // nothing lost, nothing double counted
    for (const o of out) expect(renderNotice({ id: 'x', scopeKey: 's', createdAt: 'now', rooms: o.b.rooms, totalNew: o.b.totalNew, provenance: { origin: 'o', transport: 'http-longpoll', watcher: 'w', fetchedAt: 'f' } }).length).toBeLessThanOrEqual(1500);
  });

  it('never emits for empty activity and keeps one pending notice with bounded rooms', async () => {
    const out: FlushBatch[] = [];
    const c = new Coalescer((b) => out.push(b), { maxRoomsPerNotice: 16 });
    c.add({ room: 'a', delivered: [], gaps: [], rangeFrom: 3, rangeTo: 3, transport: 'http-longpoll', fetchedAt: 'x' });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(out).toHaveLength(0);
    for (let i = 0; i < 20; i++) c.add(delta(`room-${i}`, 0, 2));
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(2000);
    expect(out).toHaveLength(1);
    expect(out[0]!.rooms).toHaveLength(16);
    expect(out[0]!.moreRooms).toBe(4);
    expect(out[0]!.overflow).toHaveLength(4);
    expect(out[0]!.totalNew).toBe(40);
  });

  it('defaults: a flood gets its first notice after maxDelayMs (7 s), then one every minWakeIntervalMs (8 s), nothing lost', async () => {
    expect(DEFAULT_COALESCE).toEqual({ quietMs: 2000, maxDelayMs: 7000, minWakeIntervalMs: 8000 });
    const out: { at: number; b: FlushBatch }[] = [];
    const c = new Coalescer((b) => out.push({ at: Date.now(), b }));
    const start = Date.now();
    let seq = 0;
    while (Date.now() - start < 60_000) {
      c.add(delta('flood', seq, 1));
      seq += 1;
      await vi.advanceTimersByTimeAsync(200); // 5 msg/s
    }
    await vi.advanceTimersByTimeAsync(20_000);
    expect(out[0]!.at - start).toBe(7000);
    // Held by minWakeIntervalMs: activity keeps arriving, yet each later window is exactly 8 s.
    for (let i = 1; i < out.length - 1; i++) expect(out[i]!.at - out[i - 1]!.at).toBe(8000);
    expect(out.length).toBe(8);
    expect(out.reduce((n, o) => n + o.b.totalNew, 0)).toBe(seq);
    // A single message after a quiet spell still goes out after quietMs.
    const quietAt = Date.now();
    c.add(delta('flood', seq, 1));
    await vi.advanceTimersByTimeAsync(2000);
    expect(out.at(-1)!.at - quietAt).toBe(2000);
    c.dispose();
  });

  it('surfaces gaps and recreation even with no new messages; dispose cancels timers', async () => {
    const out: FlushBatch[] = [];
    const c = new Coalescer((b) => out.push(b));
    c.add({ room: 'r', generation: 2, delivered: [], gaps: [{ from: 4, to: 9, reason: 'ring-dropped', generation: 2, detectedAt: 'x' }], rangeFrom: 3, rangeTo: 9, recreated: { from: 1, to: 2 }, transport: 'http-sweep', fetchedAt: 'x' });
    await vi.advanceTimersByTimeAsync(2000);
    expect(out[0]!.rooms[0]).toMatchObject({ newCount: 0, recreated: { from: 1, to: 2 }, gaps: [{ from: 4, to: 9 }] });
    c.add(delta('r', 9, 1));
    expect(vi.getTimerCount()).toBe(1);
    c.dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('pendingTiming: movable while quietMs can still push the due time, fixed once maxDelayMs or minWakeIntervalMs holds it', async () => {
    const t0 = Date.now();
    const c = new Coalescer(() => undefined, { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 });
    expect(c.pendingTiming()).toBeUndefined();
    c.add(delta('r', 0, 1));
    // quietMs drives it: any later activity moves it.
    expect(c.pendingTiming()).toEqual({ dueAt: t0 + 2000, movableFrom: t0 });
    for (let i = 1; i <= 9; i++) {
      await vi.advanceTimersByTimeAsync(1000);
      c.add(delta('r', i, 1));
    }
    // t0 + 9 s: maxDelayMs caps it at t0 + 10 s, whatever arrives.
    expect(c.pendingTiming()).toEqual({ dueAt: t0 + 10_000, movableFrom: t0 + 10_000 });
    await vi.advanceTimersByTimeAsync(1000);
    expect(c.pendingTiming()).toBeUndefined();
    // Flushed at t0 + 10 s. Activity at t0 + 11 s: minWakeIntervalMs holds it at t0 + 20 s, but maxDelayMs
    // (t0 + 21 s) is later, so activity after t0 + 18 s can still push it out.
    await vi.advanceTimersByTimeAsync(1000);
    c.add(delta('r', 10, 1));
    expect(c.pendingTiming()).toEqual({ dueAt: t0 + 20_000, movableFrom: t0 + 18_000 });
    c.dispose();
    // Now t0 + 11 s.
    const d = new Coalescer(() => undefined, { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000 });
    d.add(delta('r', 0, 1));
    d.flushNow();
    await vi.advanceTimersByTimeAsync(1000);
    d.add(delta('r', 1, 1));
    // minWakeIntervalMs holds it at t0 + 41 s, later than activity could ever push it (maxDelayMs: t0 + 22 s): fixed.
    expect(d.pendingTiming()).toEqual({ dueAt: t0 + 41_000, movableFrom: t0 + 41_000 });
    d.dispose();
    const q = new Coalescer(() => undefined, { quietMs: 0, maxDelayMs: 10_000, minWakeIntervalMs: 10_000 });
    q.add(delta('r', 0, 1));
    q.flushNow();
    q.add(delta('r', 1, 1));
    // Without quietMs nothing moves it (minWakeIntervalMs holds it at t0 + 22 s, maxDelayMs is no later).
    expect(q.pendingTiming()).toEqual({ dueAt: t0 + 22_000, movableFrom: t0 + 22_000 });
    q.dispose();
  });

  it('a due time beyond the longest timer arms the longest timer instead of one that fires at once', () => {
    const delays: number[] = [];
    const fns: (() => void)[] = [];
    let now = 1_000_000;
    const clock = {
      now: () => now,
      setTimeout: (fn: () => void, ms: number) => {
        delays.push(ms);
        fns.push(fn);
        return fns.length;
      },
      clearTimeout: () => undefined,
    };
    const out: FlushBatch[] = [];
    const c = new Coalescer((b) => out.push(b), { quietMs: Number.MAX_SAFE_INTEGER, maxDelayMs: Number.MAX_SAFE_INTEGER, clock });
    c.add(delta('r', 0, 1));
    expect(delays).toEqual([2 ** 31 - 1]);
    // When it fires the notice is still not due: it re-arms (bounded again) and does not flush.
    now += 2 ** 31 - 1;
    fns.at(-1)!();
    expect(delays).toEqual([2 ** 31 - 1, 2 ** 31 - 1]);
    expect(out).toEqual([]);
    expect(c.hasPending).toBe(true);
    c.dispose();
  });
});
