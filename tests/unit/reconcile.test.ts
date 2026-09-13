import { describe, expect, it } from 'vitest';
import { applyAck, newSubscription, reconcile, unreadCount } from '../../src/protocol/reconcile.ts';
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

  it('ack is monotonic within a generation and clamped to head', () => {
    const s = sub('r', { generation: 2, headSeq: 10 });
    const a = applyAck(s, 7);
    expect(a.acceptedSeq).toBe(7);
    expect(applyAck(a, 3).acceptedSeq).toBe(7);
    expect(applyAck(a, 99).acceptedSeq).toBe(10);
    expect(applyAck(a, 9, 1).acceptedSeq).toBe(7); // stale generation ignored
  });
});
