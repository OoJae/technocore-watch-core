import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Coalescer, type FlushBatch, type RoomDelta } from '../../src/engine/coalescer.ts';
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
    const c = new Coalescer((b) => out.push({ at: Date.now(), b }));
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
});
