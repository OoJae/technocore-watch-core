import { describe, expect, it } from 'vitest';
import { applyAck, boundGaps, newSubscription, reconcile, unreadCount } from '../../src/protocol/reconcile.ts';
import type { GapRecord } from '../../src/protocol/types.ts';
import type { ExportScan, SubscriptionState } from '../../src/protocol/types.ts';
import { ServerModel } from '../harness/model.ts';

const now = new Date('2026-09-13T12:00:00Z');

function scan(model: ServerModel, room: string, afterSeq: number, beforeSeq: number, maxRecords = 2000): ExportScan {
  const e = model.exportRoom(room);
  const inRange = e.records.filter((m) => m.seq > afterSeq && m.seq < beforeSeq);
  const out: ExportScan = { generation: e.generation, records: inRange.slice(0, maxRecords), bounded: inRange.length > maxRecords, afterSeq, beforeSeq };
  if (e.records[0]) out.oldestRetainedSeq = e.records[0].seq;
  if (out.bounded) out.lastScannedSeq = out.records[out.records.length - 1]!.seq;
  else if (e.records.length) out.lastScannedSeq = Math.min(e.records[e.records.length - 1]!.seq, beforeSeq);
  return out;
}

function sub(room: string, over: Partial<SubscriptionState> = {}): SubscriptionState {
  return { ...newSubscription(room, now), ...over };
}

function step(s: SubscriptionState, model: ServerModel, room: string, since: number, limit: number, maxRecords = 2000) {
  const view = model.read(room, since, limit);
  let r = reconcile(s, since, view, now);
  for (let i = 0; r.needsBackfill && i < 3; i++) {
    const nb = r.needsBackfill;
    if (nb.reason === 'catch-up') r = reconcile(s, s.headSeq, model.read(room, s.headSeq, 200), now);
    else r = reconcile(s, since, view, now, { backfill: scan(model, room, nb.afterSeq, nb.beforeSeq, maxRecords) });
  }
  return r;
}

describe('reconcile', () => {
  it('8 messages read with limit 3 are never taken as 3 (the #765 "8 -> 3" bug)', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 8; i++) m.post('r', 'a', `m${i}`);
    const s = sub('r', { generation: 1 });
    const view = m.read('r', 0, 3);
    expect(view.messages.map((x) => x.seq)).toEqual([6, 7, 8]);
    const first = reconcile(s, 0, view, now);
    expect(first.needsBackfill).toEqual({ afterSeq: 0, beforeSeq: 6, generation: 1, reason: 'tail-gap' });
    expect(first.next).toEqual(s);
    const r = reconcile(s, 0, view, now, { backfill: scan(m, 'r', 0, 6) });
    expect(r.delivered.map((x) => x.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(r.gaps).toEqual([]);
    expect(r.next.headSeq).toBe(8);
    expect(r.activity).toMatchObject({ seqFrom: 1, seqTo: 8, count: 8 });
  });

  it('is idempotent for replayed and duplicate responses', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 5; i++) m.post('r', 'a', `m${i}`);
    const old = m.read('r', 0, 50);
    const r1 = reconcile(sub('r'), 0, old, now);
    expect(r1.delivered).toHaveLength(5);
    m.post('r', 'a', 'm6');
    const r2 = reconcile(r1.next, 5, m.read('r', 5, 50), now);
    expect(r2.delivered.map((x) => x.seq)).toEqual([6]);
    const replay = reconcile(r2.next, 0, old, now);
    expect(replay.delivered).toEqual([]);
    expect(replay.next).toEqual(r2.next);
  });

  it('records ring drops with the exact range', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 3; i++) m.post('r', 'a', `m${i}`);
    const r1 = step(sub('r'), m, 'r', 0, 200);
    for (let i = 4; i <= 400; i++) m.post('r', 'a', `m${i}`);
    m.compact('r', 150); // keeps 251..400
    const r = step(r1.next, m, 'r', 3, 200);
    expect(r.gaps).toEqual([{ from: 4, to: 250, reason: 'ring-dropped', generation: 1, detectedAt: now.toISOString() }]);
    expect(r.delivered[0]!.seq).toBe(251);
    expect(r.delivered).toHaveLength(150);
    expect(r.next.headSeq).toBe(400);
  });

  it('marks what a bounded export could not scan as backfill-bounded', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 450; i++) m.post('r', 'a', `m${i}`);
    const r = step(sub('r', { generation: 1 }), m, 'r', 0, 200, 100);
    expect(r.delivered.map((x) => x.seq).slice(0, 3)).toEqual([1, 2, 3]);
    expect(r.gaps).toEqual([{ from: 101, to: 250, reason: 'backfill-bounded', generation: 1, detectedAt: now.toISOString() }]);
    expect(r.delivered).toHaveLength(300);
    expect(r.next.headSeq).toBe(450);
  });

  it('detects recreation with a continued sequence (reap kept the floor)', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 5; i++) m.post('r', 'a', `m${i}`);
    const s = step(sub('r'), m, 'r', 0, 200).next;
    m.reap('r');
    m.post('r', 'a', 'again');
    const r = step(s, m, 'r', 5, 200);
    expect(r.recreated).toEqual({ from: 1, to: 2 });
    expect(r.regression).toBeUndefined();
    expect(r.delivered.map((x) => x.seq)).toEqual([6]);
    expect(r.next).toMatchObject({ generation: 2, headSeq: 6 });
  });

  it('detects a seq regression (file deleted, floor lost) even when the long-poll view is empty', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 5; i++) m.post('r', 'a', `m${i}`);
    let s = step(sub('r'), m, 'r', 0, 200).next;
    s = applyAck(s, 3);
    m.deleteFile('r');
    m.post('r', 'b', 'new1');
    m.post('r', 'b', 'new2');
    const view = m.read('r', 5, 200);
    expect(view.messages).toEqual([]);
    expect(view.last_seq).toBe(5); // the server echoes `since` — a regression is invisible without generation
    const need = reconcile(s, 5, view, now);
    expect(need.needsBackfill).toMatchObject({ reason: 'generation-change', afterSeq: 0, beforeSeq: 6 });
    const r = reconcile(s, 5, view, now, { backfill: scan(m, 'r', 0, 6) });
    expect(r.regression).toEqual({ fromHead: 5, toLast: 2 });
    expect(r.recreated).toEqual({ from: 1, to: 2 });
    expect(r.delivered.map((x) => x.text)).toEqual(['new1', 'new2']);
    expect(r.gaps).toEqual([{ from: 4, to: 5, reason: 'seq-regression', generation: 1, detectedAt: now.toISOString() }]);
    expect(r.next).toMatchObject({ generation: 2, headSeq: 2, notifiedSeq: 0, acceptedSeq: 0 });
    expect(unreadCount(r.next)).toBe(2);
  });

  it('ignores responses from an older generation', () => {
    const r = reconcile(sub('r', { generation: 3, headSeq: 10 }), 0, { room: 'r', count: 1, first_seq: 1, last_seq: 1, generation: 2, messages: [{ seq: 1, ts: 't', from: 'a', text: 'x' }] }, now);
    expect(r.ignored).toBe('stale-generation');
  });

  it('asks for a catch-up read when a shared poll is ahead of this subscriber', () => {
    const r = reconcile(sub('r', { generation: 1, headSeq: 2 }), 5, { room: 'r', count: 0, first_seq: null, last_seq: 5, generation: 1, messages: [] }, now);
    expect(r.needsBackfill).toMatchObject({ reason: 'catch-up', afterSeq: 2, beforeSeq: 6 });
  });

  it('the first creation of a room that never existed (generation 0) is not reported as a recreation', () => {
    const m = new ServerModel();
    const empty = m.read('fresh', 0, 200);
    expect(empty.generation).toBe(0);
    const s = reconcile(sub('fresh'), 0, empty, now).next;
    expect(s.generation).toBe(0);
    m.post('fresh', 'a', 'first');
    const r = step(s, m, 'fresh', 0, 200);
    expect(r.recreated).toBeUndefined();
    expect(r.delivered.map((x) => x.seq)).toEqual([1]);
    expect(r.next.generation).toBe(1);
  });

  it('an origin reset (confirmed lower generation) restarts the cursors; unconfirmed it is ignored', () => {
    const m = new ServerModel();
    for (let i = 1; i <= 10; i++) m.post('r', 'x', `fresh ${i}`); // generation 1 on the reset origin
    const stored = sub('r', { generation: 3, headSeq: 5, notifiedSeq: 5, acceptedSeq: 2 });
    const view = m.read('r', 5, 200);
    expect(reconcile(stored, 5, view, now).ignored).toBe('stale-generation');
    const need = reconcile(stored, 5, view, now, { originReset: true });
    expect(need.needsBackfill).toMatchObject({ reason: 'generation-change', afterSeq: 0, beforeSeq: 6 });
    const r = reconcile(stored, 5, view, now, { originReset: true, backfill: scan(m, 'r', 0, 6) });
    expect(r).toMatchObject({ originReset: true, recreated: { from: 3, to: 1 }, regression: { fromHead: 5, toLast: 10 } });
    expect(r.delivered.map((x) => x.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(r.gaps).toEqual([{ from: 3, to: 5, reason: 'seq-regression', generation: 3, detectedAt: now.toISOString() }]);
    expect(r.next).toMatchObject({ generation: 1, headSeq: 10, notifiedSeq: 0, acceptedSeq: 0 });
    // Reset to an origin that holds nothing for the room: cursors restart unsettled.
    const empty = new ServerModel().read('r', 5, 200);
    const e = reconcile(stored, 5, empty, now, { originReset: true, backfill: { records: [], bounded: false, afterSeq: 0, beforeSeq: 6, generation: 0 } });
    expect(e.next).toMatchObject({ generation: 0, headSeq: 0, acceptedSeq: 0, headUnsettled: true });
    expect(e).toMatchObject({ originReset: true, regression: { fromHead: 5, toLast: 0 } });
  });

  it('boundGaps never widens a gap over delivered or retained seqs, and never relabels one', () => {
    const at = now.toISOString();
    const gap = (from: number, to: number, reason: GapRecord['reason']): GapRecord => ({ from, to, reason, generation: 1, detectedAt: at });
    // An old ring drop, then backfill-bounded gaps from repeated large backlogs (11..19 were delivered between).
    const gaps = [gap(1, 10, 'ring-dropped'), gap(20, 30, 'backfill-bounded'), ...Array.from({ length: 7 }, (_, i) => gap(100 + i * 20, 105 + i * 20, 'backfill-bounded'))];
    const bounded = boundGaps(gaps, { acceptedSeq: 0, generation: 1 }, 8);
    expect(bounded).toHaveLength(8);
    for (const g of bounded) {
      const original = gaps.find((o) => o.from <= g.from && g.to <= o.to);
      expect(original, `gap ${g.from}..${g.to} ${g.reason} is not inside one recorded gap`).toBeDefined();
      expect(g.reason).toBe(original!.reason);
    }
    // The backfill-bounded record (still retained) is dropped before the ring drop.
    expect(bounded[0]).toMatchObject({ from: 1, to: 10, reason: 'ring-dropped' });
    expect(bounded.some((g) => g.from === 20)).toBe(false);
    // Touching or overlapping gaps of the same reason coalesce; different reasons stay apart.
    expect(boundGaps([gap(1, 4, 'ring-dropped'), gap(5, 9, 'ring-dropped'), gap(10, 12, 'backfill-bounded')], { acceptedSeq: 0, generation: 1 }, 8)).toEqual([
      gap(1, 9, 'ring-dropped'),
      gap(10, 12, 'backfill-bounded'),
    ]);
    // unread never under-counts because of bounding.
    const s = sub('r', { generation: 1, headSeq: 250, gaps: bounded });
    const exact = sub('r', { generation: 1, headSeq: 250, gaps });
    expect(unreadCount(s)).toBeGreaterThanOrEqual(unreadCount(exact));
  });

  it('ack is monotonic within a generation and clamped to head', () => {
    const s = sub('r', { generation: 2, headSeq: 10 });
    const a = applyAck(s, 7);
    expect(a.acceptedSeq).toBe(7);
    expect(applyAck(a, 3).acceptedSeq).toBe(7);
    expect(applyAck(a, 99).acceptedSeq).toBe(10);
    expect(applyAck(a, 9, 1).acceptedSeq).toBe(7); // stale generation ignored
  });
});
