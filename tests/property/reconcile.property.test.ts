// Property tests: random interleavings of posts, ring compaction, reaps (floor kept), hand
// deletions (seq regression), polls with random limits, stale/replayed responses, duplicate
// deliveries, bounded exports and acks. After every settled step:
//   * every seq in (old head, new head] is delivered exactly once or covered by one gap
//   * delivered text matches what the server stored for that generation and seq
//   * a ring-dropped gap never covers a record the export still retained
//   * acceptedSeq never decreases within a generation
//   * re-applying the identical input is a no-op (duplicate delivery is idempotent)
//   * notice counts equal delivered and gapped totals
import fc from 'fast-check';
import { describe, expect, it } from 'vitest';
import { applyAck, newSubscription, reconcile, type ReconcileResult } from '../../src/protocol/reconcile.ts';
import type { ExportScan, ReadView, SubscriptionState } from '../../src/protocol/types.ts';
import { Coalescer, type FlushBatch } from '../../src/engine/coalescer.ts';
import { ServerModel } from '../harness/model.ts';

const ROOM = 'r';
const now = new Date('2026-09-13T12:00:00Z');

type Op =
  | { t: 'post'; n: number }
  | { t: 'compact'; keep: number }
  | { t: 'reap' }
  | { t: 'delete' }
  | { t: 'poll'; limit: number; exportMax: number }
  | { t: 'stale'; back: number; limit: number; exportMax: number }
  | { t: 'dup' }
  | { t: 'ack'; frac: number };

const opArb = (allowRegression: boolean): fc.Arbitrary<Op> =>
  fc.oneof(
    { weight: 5, arbitrary: fc.record({ t: fc.constant('post' as const), n: fc.integer({ min: 1, max: 40 }) }) },
    { weight: 2, arbitrary: fc.record({ t: fc.constant('compact' as const), keep: fc.integer({ min: 1, max: 60 }) }) },
    { weight: 1, arbitrary: fc.constant({ t: 'reap' as const }) },
    { weight: allowRegression ? 1 : 0, arbitrary: fc.constant({ t: 'delete' as const }) },
    {
      weight: 6,
      arbitrary: fc.record({ t: fc.constant('poll' as const), limit: fc.constantFrom(1, 3, 7, 50, 200), exportMax: fc.constantFrom(1, 5, 30, 2000) }),
    },
    {
      weight: 2,
      arbitrary: fc.record({
        t: fc.constant('stale' as const),
        back: fc.integer({ min: 0, max: 6 }),
        limit: fc.constantFrom(1, 3, 50, 200),
        exportMax: fc.constantFrom(5, 2000),
      }),
    },
    { weight: 1, arbitrary: fc.constant({ t: 'dup' as const }) },
    { weight: 2, arbitrary: fc.record({ t: fc.constant('ack' as const), frac: fc.double({ min: 0, max: 1, noNaN: true }) }) },
  );

function exportScan(model: ServerModel, afterSeq: number, beforeSeq: number, maxRecords: number): ExportScan {
  const e = model.exportRoom(ROOM);
  const out: ExportScan = { generation: e.generation, records: [], bounded: false, afterSeq, beforeSeq };
  if (e.records[0]) out.oldestRetainedSeq = e.records[0].seq;
  for (const m of e.records) {
    out.lastScannedSeq = m.seq;
    if (m.seq >= beforeSeq) break;
    if (m.seq <= afterSeq) continue;
    if (out.records.length >= maxRecords) {
      out.bounded = true;
      out.lastScannedSeq = out.records[out.records.length - 1]!.seq;
      break;
    }
    out.records.push({ ...m });
  }
  return out;
}

interface Truth {
  text: Map<string, string>; // `${gen}:${seq}` -> text
}

function settle(
  sub: SubscriptionState,
  since: number,
  view: ReadView,
  model: ServerModel,
  exportMax: number,
): { r: ReconcileResult; since: number; view: ReadView; backfill?: ExportScan; exportAt?: ExportScan } {
  let curSince = since;
  let curView = view;
  let backfill: ExportScan | undefined;
  let r = reconcile(sub, curSince, curView, now);
  for (let i = 0; i < 4 && r.needsBackfill; i++) {
    const nb = r.needsBackfill;
    if (nb.reason === 'catch-up') {
      curSince = sub.headSeq;
      curView = model.read(ROOM, sub.headSeq, 200);
      backfill = undefined;
    } else {
      backfill = exportScan(model, nb.afterSeq, nb.beforeSeq, exportMax);
    }
    r = reconcile(sub, curSince, curView, now, backfill ? { backfill } : {});
  }
  const out: { r: ReconcileResult; since: number; view: ReadView; backfill?: ExportScan } = { r, since: curSince, view: curView };
  if (backfill) out.backfill = backfill;
  return out;
}

function runScenario(ops: Op[], opts: { checkNotices: boolean }) {
  const model = new ServerModel();
  const truth: Truth = { text: new Map() };
  let textCounter = 0;
  const post = (n: number) => {
    for (let i = 0; i < n; i++) {
      const text = `t${textCounter++}`;
      const m = model.post(ROOM, 'a', text);
      truth.text.set(`${model.generation(ROOM)}:${m.seq}`, text);
    }
  };
  post(1);
  let sub = newSubscription(ROOM, now);
  const history: { since: number; view: ReadView }[] = [];
  let last: { since: number; view: ReadView; backfill?: ExportScan } | undefined;
  // Seqs settled (delivered or gapped) in the current epoch; reset on regression.
  let settled = new Set<number>();
  let epochGeneration: number | undefined;
  let deliveredTotal = 0;
  let gappedTotal = 0;
  const flushed: FlushBatch[] = [];
  const coalescer = new Coalescer((b) => flushed.push(b), { quietMs: 0, maxDelayMs: 0, minWakeIntervalMs: 0, maxGapsPerRoom: 1_000_000, maxRoomsPerNotice: 16 });

  const check = (prev: SubscriptionState, res: ReconcileResult, exportUsed: ExportScan | undefined) => {
    if (res.needsBackfill || res.ignored) {
      expect(res.next).toEqual(prev);
      return;
    }
    const from = res.regression ? 0 : prev.headSeq;
    const to = res.next.headSeq;
    if (!res.regression && prev.generation !== undefined && res.next.generation === prev.generation) {
      expect(to).toBeGreaterThanOrEqual(from);
      expect(res.next.acceptedSeq).toBeGreaterThanOrEqual(prev.acceptedSeq);
    }
    if (res.regression) {
      settled = new Set();
    }
    if (res.next.generation !== epochGeneration && !res.regression) {
      // continued recreation keeps numbering; nothing to reset
    }
    epochGeneration = res.next.generation;
    const covered: number[] = [];
    let prevSeq = -Infinity;
    for (const m of res.delivered) {
      expect(m.seq).toBeGreaterThan(prevSeq);
      prevSeq = m.seq;
      covered.push(m.seq);
      const want = truth.text.get(`${res.next.generation}:${m.seq}`);
      expect(m.text).toBe(want);
    }
    const currentGaps = res.gaps.filter((g) => g.reason !== 'seq-regression');
    for (const g of currentGaps) {
      expect(g.to).toBeGreaterThanOrEqual(g.from);
      for (let s = g.from; s <= g.to; s++) covered.push(s);
      if (g.reason === 'ring-dropped') {
        // Checked against what the origin retains right now, whether or not an export was used:
        // a tail window taken at face value would show up here as a fake ring drop.
        const retained = new Set(model.exportRoom(ROOM).records.map((m) => m.seq));
        for (let s = g.from; s <= g.to; s++) expect(retained.has(s)).toBe(false);
      }
    }
    covered.sort((a, b) => a - b);
    const expected: number[] = [];
    for (let s = from + 1; s <= to; s++) expected.push(s);
    expect(covered).toEqual(expected);
    for (const s of covered) {
      expect(settled.has(s)).toBe(false);
      settled.add(s);
    }
    deliveredTotal += res.delivered.length;
    gappedTotal += currentGaps.reduce((n, g) => n + g.to - g.from + 1, 0);
    if (opts.checkNotices && (res.range || res.recreated)) {
      coalescer.add({
        room: ROOM,
        ...(res.next.generation !== undefined ? { generation: res.next.generation } : {}),
        delivered: res.delivered,
        gaps: res.gaps,
        rangeFrom: res.range?.from ?? res.next.headSeq,
        rangeTo: res.range?.to ?? res.next.headSeq,
        transport: 'http-longpoll',
        fetchedAt: now.toISOString(),
      });
      if (flushed.length % 2 === 0) coalescer.flushNow();
    }
  };

  for (const op of ops) {
    switch (op.t) {
      case 'post':
        post(op.n);
        history.push({ since: sub.headSeq, view: model.read(ROOM, sub.headSeq, 200) });
        break;
      case 'compact':
        model.compact(ROOM, op.keep);
        break;
      case 'reap':
        model.reap(ROOM);
        break;
      case 'delete':
        model.deleteFile(ROOM);
        break;
      case 'poll':
      case 'stale': {
        let since = sub.headSeq;
        let view: ReadView;
        if (op.t === 'stale' && history.length) {
          const h = history[Math.max(0, history.length - 1 - op.back)]!;
          since = h.since;
          view = h.view;
        } else {
          view = model.read(ROOM, since, op.limit);
        }
        const prev = sub;
        const s = settle(sub, since, view, model, op.exportMax);
        check(prev, s.r, s.backfill);
        if (!s.r.needsBackfill && !s.r.ignored) {
          sub = s.r.next;
          last = s;
        }
        history.push({ since, view });
        break;
      }
      case 'dup': {
        if (!last) break;
        const again = reconcile(sub, last.since, last.view, now, last.backfill ? { backfill: last.backfill } : {});
        if (!again.needsBackfill) {
          expect(again.delivered).toEqual([]);
          expect(again.gaps).toEqual([]);
          expect(again.next).toEqual(sub);
        }
        break;
      }
      case 'ack': {
        const target = Math.floor(sub.acceptedSeq + (sub.headSeq - sub.acceptedSeq) * op.frac);
        const before = sub.acceptedSeq;
        sub = applyAck(sub, target);
        expect(sub.acceptedSeq).toBeGreaterThanOrEqual(before);
        expect(sub.acceptedSeq).toBeLessThanOrEqual(sub.headSeq);
        break;
      }
    }
  }

  // A final fresh poll must bring the head to the origin's newest seq when the room exists.
  const final = settle(sub, sub.headSeq, model.read(ROOM, sub.headSeq, 200), model, 1_000_000);
  check(sub, final.r, final.backfill);
  if (!final.r.needsBackfill && !final.r.ignored) sub = final.r.next;
  const room = model.rooms.get(ROOM)!;
  if (room.exists && (sub.generation === room.generation || sub.generation === undefined)) {
    expect(sub.headSeq).toBe(model.lastSeq(ROOM));
  }
  if (opts.checkNotices) {
    coalescer.flushNow();
    const noticed = flushed.reduce((n, b) => n + b.totalNew, 0);
    expect(noticed).toBe(deliveredTotal);
    const noticedGaps = flushed.reduce((n, b) => n + b.rooms.reduce((k, r) => k + r.gaps.filter((g) => g.reason !== 'seq-regression').reduce((j, g) => j + g.to - g.from + 1, 0), 0), 0);
    expect(noticedGaps).toBe(gappedTotal);
  }
}

describe('reconcile properties', () => {
  it('every seq is delivered or gapped, exactly once, across random interleavings (with regressions)', () => {
    fc.assert(
      fc.property(fc.array(opArb(true), { minLength: 1, maxLength: 60 }), (ops) => runScenario(ops, { checkNotices: false })),
      { numRuns: 400 },
    );
  });

  it('notice counts equal delivered and gapped totals (no regressions)', () => {
    fc.assert(
      fc.property(fc.array(opArb(false), { minLength: 1, maxLength: 60 }), (ops) => runScenario(ops, { checkNotices: true })),
      { numRuns: 300 },
    );
  });
});
