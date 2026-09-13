// reconcile: fold one origin observation into a subscription's cursors. Pure.
//
// INVARIANT (property-tested): after a successful step, every seq in (old headSeq,
// new headSeq] — or (0, new headSeq] after a seq regression — is either delivered
// (present in `delivered`, ascending, exactly once) or covered by exactly one GapRecord
// in `gaps`. Nothing is skipped silently. This is the "8 messages, limit 3" fix: the read
// view returns the NEWEST window, so a tail that starts above headSeq+1 is never taken
// at face value — it asks for a backfill (export) first.
//
// When the observation alone cannot establish the range, reconcile returns
// `needsBackfill` with the state UNCHANGED; the caller fetches and calls again:
//   - 'tail-gap'          export (afterSeq, beforeSeq) then reconcile(sub, since, view, {backfill})
//   - 'generation-change' export from 0 (the oldest retained record decides regression)
//   - 'catch-up'          the view was fetched with since > headSeq (a shared poll ahead of
//                         this subscriber): re-read with since = headSeq and reconcile that.

import type { ExportScan, GapReason, GapRecord, Message, ReadView, SubscriptionState } from './types.ts';

export const DEFAULT_MAX_GAPS = 8;

export interface BackfillRequest {
  afterSeq: number;
  beforeSeq: number;
  generation?: number;
  reason: 'tail-gap' | 'generation-change' | 'catch-up';
}

export interface ReconcileResult {
  next: SubscriptionState;
  /** Messages newly delivered, ascending, each seq exactly once. */
  delivered: Message[];
  /** Gaps newly detected by this step. */
  gaps: GapRecord[];
  /** (from, to] seq range this step settled; present when headSeq moved or reset. */
  range?: { from: number; to: number };
  /** Design-compat summary of `delivered` + `range`. */
  activity?: { seqFrom: number; seqTo: number; count: number; messages: Message[] };
  needsBackfill?: BackfillRequest;
  recreated?: { from?: number; to: number };
  regression?: { fromHead: number; toLast: number };
  ignored?: 'room-mismatch' | 'stale-generation';
  /**
   * Set when a `headUnsettled` subscription settled its start: seqs <= settledStart predate the
   * subscription (neither delivered nor gapped) and the cursors were raised to it.
   */
  settledStart?: number;
  /** The confirmed-lower generation was applied as a restart (see ReconcileOptions.originReset). */
  originReset?: boolean;
}

export interface ReconcileOptions {
  backfill?: ExportScan;
  maxGaps?: number;
  /**
   * The caller confirmed with fresh, cache-busted reads that the origin now reports a LOWER
   * generation than the stored one (its data root was reset or restored). The observation is
   * then applied as a restart (recreated + regression, cursors reset) instead of being ignored
   * as a stale response.
   */
  originReset?: boolean;
}

function cloneSub(sub: SubscriptionState): SubscriptionState {
  return { ...sub, gaps: sub.gaps.map((g) => ({ ...g })) };
}

/** First seq the view proves exactly: everything >= coverStart that exists is in view.messages. */
export function coverStart(since: number, view: ReadView): number {
  const first = view.messages[0];
  if (!first) return since + 1;
  return first.seq <= since + 1 ? since + 1 : first.seq;
}

function covers(bf: ExportScan | undefined, afterSeq: number, beforeSeq: number, generation: number | undefined): bf is ExportScan {
  if (!bf) return false;
  if (bf.afterSeq > afterSeq || bf.beforeSeq < beforeSeq) return false;
  if (bf.generation !== undefined && generation !== undefined && bf.generation !== generation) return false;
  return true;
}

function mergeAscending(a: readonly Message[], b: readonly Message[]): Message[] {
  const bySeq = new Map<number, Message>();
  for (const m of a) bySeq.set(m.seq, m);
  for (const m of b) bySeq.set(m.seq, m); // the view (b) wins on overlap: it is the newer fetch
  return [...bySeq.values()].sort((x, y) => x.seq - y.seq);
}

interface Assembly {
  delivered: Message[];
  gaps: GapRecord[];
  head: number;
}

/**
 * Walk records above `head`, delivering contiguous runs and recording every hole.
 * Holes above the export's scan boundary (when a budget stopped it) are 'backfill-bounded';
 * holes the export proves absent are 'ring-dropped'.
 */
function assemble(
  head: number,
  records: readonly Message[],
  bf: ExportScan | undefined,
  boundaryBefore: number,
  generation: number | undefined,
  detectedAt: string,
): Assembly {
  const delivered: Message[] = [];
  const gaps: GapRecord[] = [];
  let expected = head + 1;
  const scannedThrough = bf && bf.bounded ? (bf.lastScannedSeq ?? bf.afterSeq) : Number.POSITIVE_INFINITY;
  const push = (from: number, to: number, reason: GapReason) => {
    if (to < from) return;
    const g: GapRecord = { from, to, reason, detectedAt };
    if (generation !== undefined) g.generation = generation;
    gaps.push(g);
  };
  for (const r of records) {
    if (r.seq < expected) continue;
    if (r.seq > expected) {
      const holeTo = r.seq - 1;
      if (bf && bf.bounded && holeTo < boundaryBefore) {
        // Split at what the export actually scanned.
        const droppedTo = Math.min(holeTo, scannedThrough);
        push(expected, droppedTo, 'ring-dropped');
        push(Math.max(expected, scannedThrough + 1), holeTo, 'backfill-bounded');
      } else {
        push(expected, holeTo, 'ring-dropped');
      }
    }
    delivered.push(r);
    expected = r.seq + 1;
  }
  return { delivered, gaps, head: expected - 1 };
}

/**
 * Keep gaps bounded without ever widening a gap over seqs that were delivered or are still
 * retained: drop settled records (at or below acceptedSeq; older generations beyond the newest
 * two), coalesce records that touch or overlap AND share reason and generation, then drop whole
 * records while over the bound — older generations first (informational only), then the oldest
 * `backfill-bounded` record (its seqs are still retained and readPage fetches them anyway), then
 * the oldest record. A dropped `ring-dropped` record only makes `unread` over-count until
 * readPage re-detects the hole; nothing is ever reported lost that was not.
 */
export function boundGaps(gaps: GapRecord[], sub: Pick<SubscriptionState, 'acceptedSeq' | 'generation'>, max = DEFAULT_MAX_GAPS): GapRecord[] {
  const isCurrent = (g: GapRecord) => g.generation === undefined || sub.generation === undefined || g.generation === sub.generation;
  const live = gaps.filter((g) => !isCurrent(g) || g.to > sub.acceptedSeq);
  const older = live.filter((g) => !isCurrent(g)).slice(-2).map((g) => ({ ...g }));
  const current = live
    .filter(isCurrent)
    .map((g) => ({ ...g }))
    .sort((a, b) => a.from - b.from || a.to - b.to);
  const merged: GapRecord[] = [];
  for (const g of current) {
    const last = merged[merged.length - 1];
    if (last && last.reason === g.reason && last.generation === g.generation && g.from <= last.to + 1) {
      last.to = Math.max(last.to, g.to);
    } else {
      merged.push(g);
    }
  }
  const out = [...older, ...merged];
  while (out.length > Math.max(0, max)) {
    let i = out.findIndex((g) => !isCurrent(g));
    if (i < 0) i = out.findIndex((g) => g.reason === 'backfill-bounded');
    if (i < 0) i = 0;
    out.splice(i, 1);
  }
  return out;
}

/** Cursors restart at 0; the old generation's unread range is recorded as a seq-regression gap. */
function lostOnRestart(sub: SubscriptionState, G: number | undefined, detectedAt: string): GapRecord[] {
  if (sub.acceptedSeq >= sub.headSeq) return [];
  const r: GapRecord = { from: sub.acceptedSeq + 1, to: sub.headSeq, reason: 'seq-regression', detectedAt };
  if (G !== undefined) r.generation = G;
  return [r];
}

/** A `headUnsettled` subscription's first records settle its start: seqs <= base predate it. */
function settleStart(next: SubscriptionState, base: number): void {
  next.notifiedSeq = Math.max(next.notifiedSeq, base);
  next.acceptedSeq = Math.max(next.acceptedSeq, base);
  delete next.headUnsettled;
}

export function reconcile(
  sub: SubscriptionState,
  since: number,
  view: ReadView,
  now: Date,
  opts: ReconcileOptions = {},
): ReconcileResult {
  const detectedAt = now.toISOString();
  const maxGaps = opts.maxGaps ?? DEFAULT_MAX_GAPS;
  const unchanged = (extra: Partial<ReconcileResult> = {}): ReconcileResult => ({
    next: cloneSub(sub),
    delivered: [],
    gaps: [],
    ...extra,
  });

  if (view.room !== sub.room) return unchanged({ ignored: 'room-mismatch' });
  // Generation 0 means "never existed" (store.room_generation): its first creation is not a recreation.
  const G = sub.generation === 0 ? undefined : sub.generation;
  const g = view.generation;
  // A lower generation is normally a stale or replayed response. Only a caller that confirmed it
  // with fresh reads (the origin's data root was reset or restored) may pass originReset.
  const backwards = g !== undefined && G !== undefined && g < G;
  if (backwards && !opts.originReset) return unchanged({ ignored: 'stale-generation' });
  const H = sub.headSeq;
  const start = coverStart(since, view);
  const bf = opts.backfill;
  const genChanged = g !== undefined && G !== undefined && g !== G;
  const unsettled = sub.headUnsettled === true;

  if (!genChanged) {
    if (since > H) {
      return unchanged({ needsBackfill: { afterSeq: H, beforeSeq: since + 1, ...(g !== undefined ? { generation: g } : {}), reason: 'catch-up' } });
    }
    const fresh = view.messages.filter((m) => m.seq > H);
    if (fresh.length === 0) {
      const next = cloneSub(sub);
      if (G === undefined && g !== undefined) next.generation = g;
      return { next, delivered: [], gaps: [] };
    }
    let records: Message[] = fresh;
    let usedBf: ExportScan | undefined;
    if (start > H + 1) {
      if (!covers(bf, H, start, g)) {
        return unchanged({ needsBackfill: { afterSeq: H, beforeSeq: start, ...(g !== undefined ? { generation: g } : {}), reason: 'tail-gap' } });
      }
      usedBf = bf;
      records = mergeAscending(bf.records.filter((m) => m.seq > H && m.seq < start), fresh);
    }
    // An unsettled 'now' start: the oldest record readable above H is the first one posted
    // after the subscription, so nothing below it was lost while watching.
    const base = unsettled && records[0] ? Math.max(H, records[0].seq - 1) : H;
    const genForGaps = g ?? G;
    const a = assemble(base, records, usedBf, start, genForGaps, detectedAt);
    const next = cloneSub(sub);
    if (genForGaps !== undefined) next.generation = genForGaps;
    next.headSeq = a.head;
    next.lastActivityAt = detectedAt;
    if (unsettled) settleStart(next, base);
    next.gaps = boundGaps([...next.gaps, ...a.gaps], next, maxGaps);
    const res = finish(next, a, base);
    if (unsettled) res.settledStart = base;
    return res;
  }

  // Generation changed: the room was (re)created. Only the oldest retained record of the new
  // generation can tell a continued sequence (reap with floor) from a restarted one.
  if (!covers(bf, 0, start, g)) {
    return unchanged({ needsBackfill: { afterSeq: 0, beforeSeq: start, generation: g, reason: 'generation-change' } });
  }
  const all = mergeAscending(bf.records.filter((m) => m.seq < start), view.messages);
  const oldest = bf.oldestRetainedSeq ?? all[0]?.seq;
  const recreated: { from?: number; to: number } = { to: g };
  if (G !== undefined) recreated.from = G;
  if (oldest === undefined) {
    const next = cloneSub(sub);
    next.generation = g;
    if (!backwards) return { next, delivered: [], gaps: [], recreated };
    // The reset origin holds nothing for this room yet: restart the cursors and let the first
    // records it serves settle the start (its floor is unknowable from an empty view).
    const lost = lostOnRestart(sub, G, detectedAt);
    next.headSeq = 0;
    next.notifiedSeq = 0;
    next.acceptedSeq = 0;
    next.headUnsettled = true;
    next.lastActivityAt = detectedAt;
    next.gaps = boundGaps([...next.gaps, ...lost], next, maxGaps);
    return { next, delivered: [], gaps: lost, recreated, regression: { fromHead: H, toLast: 0 }, originReset: true };
  }
  if (oldest <= H || backwards) {
    // Seq regression: the new generation restarted numbering at or below our head — or the
    // origin itself was reset, where no numbering carries over whatever the seqs look like.
    const lost = lostOnRestart(sub, G, detectedAt);
    const a = assemble(0, all, bf, start, g, detectedAt);
    const next = cloneSub(sub);
    next.generation = g;
    next.headSeq = a.head;
    next.notifiedSeq = 0;
    next.acceptedSeq = 0;
    delete next.headUnsettled;
    next.lastActivityAt = detectedAt;
    next.gaps = boundGaps([...next.gaps, ...lost, ...a.gaps], next, maxGaps);
    const res = finish(next, a, 0);
    res.gaps = [...lost, ...a.gaps];
    res.recreated = recreated;
    res.regression = { fromHead: H, toLast: a.head };
    if (backwards) res.originReset = true;
    return res;
  }
  // Continued sequence: cursors stay (min(current, oldest - 1) == current); anything between
  // our head and the new generation's oldest record is gone — unless the start is unsettled,
  // in which case everything below the oldest record predates the subscription.
  const base = unsettled ? Math.max(H, oldest - 1) : H;
  const a = assemble(base, all.filter((m) => m.seq > base), bf, start, g, detectedAt);
  const next = cloneSub(sub);
  next.generation = g;
  next.headSeq = a.head;
  if (a.delivered.length || a.gaps.length) next.lastActivityAt = detectedAt;
  if (unsettled) settleStart(next, base);
  next.gaps = boundGaps([...next.gaps, ...a.gaps], next, maxGaps);
  const res = finish(next, a, base);
  res.recreated = recreated;
  if (unsettled) res.settledStart = base;
  return res;
}

function finish(next: SubscriptionState, a: Assembly, from: number): ReconcileResult {
  const res: ReconcileResult = { next, delivered: a.delivered, gaps: a.gaps };
  if (a.head > from || a.gaps.length) {
    res.range = { from, to: a.head };
    res.activity = { seqFrom: from + 1, seqTo: a.head, count: a.delivered.length, messages: a.delivered };
  }
  return res;
}

/** Advance acceptedSeq monotonically within the current generation, clamped to headSeq. */
export function applyAck(sub: SubscriptionState, throughSeq: number, generation?: number): SubscriptionState {
  if (generation !== undefined && sub.generation !== undefined && generation !== sub.generation) return sub;
  const target = Math.min(Math.max(0, Math.floor(throughSeq)), sub.headSeq);
  if (target <= sub.acceptedSeq) return sub;
  const next = cloneSub(sub);
  next.acceptedSeq = target;
  next.gaps = boundGaps(next.gaps, next);
  return next;
}

/** Advance notifiedSeq monotonically within the current generation, clamped to headSeq. */
export function applyNotified(sub: SubscriptionState, throughSeq: number, generation?: number): SubscriptionState {
  if (generation !== undefined && sub.generation !== undefined && generation !== sub.generation) return sub;
  const target = Math.min(Math.max(0, Math.floor(throughSeq)), sub.headSeq);
  if (target <= sub.notifiedSeq) return sub;
  return { ...cloneSub(sub), notifiedSeq: target };
}

/** Distinct seqs in [from, to] covered by gaps of `generation` (overlapping records count once). */
export function gappedCount(gaps: readonly GapRecord[], generation: number | undefined, from: number, to: number): number {
  const ranges = gaps
    .filter((g) => g.generation === undefined || generation === undefined || g.generation === generation)
    .map((g) => [Math.max(g.from, from), Math.min(g.to, to)] as const)
    .filter(([a, b]) => b >= a)
    .sort((x, y) => x[0] - y[0]);
  let total = 0;
  let curFrom = 0;
  let curTo = -1;
  for (const [a, b] of ranges) {
    if (a > curTo + 1 || curTo < curFrom) {
      if (curTo >= curFrom) total += curTo - curFrom + 1;
      curFrom = a;
      curTo = b;
    } else {
      curTo = Math.max(curTo, b);
    }
  }
  if (curTo >= curFrom) total += curTo - curFrom + 1;
  return total;
}

/** Seqs in (acceptedSeq, headSeq] not covered by a current-generation gap. */
export function unreadCount(sub: SubscriptionState): number {
  const gapped = gappedCount(sub.gaps, sub.generation, sub.acceptedSeq + 1, sub.headSeq);
  return Math.max(0, sub.headSeq - sub.acceptedSeq - gapped);
}

export function newSubscription(room: string, now: Date, startFrom?: 'now' | 'retained'): SubscriptionState {
  const s: SubscriptionState = {
    room,
    headSeq: 0,
    notifiedSeq: 0,
    acceptedSeq: 0,
    gaps: [],
    status: 'active',
    createdAt: now.toISOString(),
  };
  if (startFrom) s.startFrom = startFrom;
  return s;
}
