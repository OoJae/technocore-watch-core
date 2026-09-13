// Persistent state model and validation/migration of whatever is on disk.
import { isValidRoomName } from '../protocol/names.ts';
import type { GapReason, GapRecord, SubscriptionState, WatchStateV1 } from '../protocol/types.ts';

export type { SubscriptionState, WatchStateV1 };

export const MAX_STATE_BYTES = 256 << 10;

export class StateError extends Error {
  override readonly name = 'StateError';
}

export class RevisionConflictError extends Error {
  override readonly name = 'RevisionConflictError';
  readonly expected: number;
  readonly actual: number;
  constructor(expected: number, actual: number) {
    super(`state revision conflict: expected ${expected}, found ${actual}`);
    this.expected = expected;
    this.actual = actual;
  }
}

export function emptyState(origin: string, scopeKey: string, now = new Date()): WatchStateV1 {
  return { version: 1, origin, scopeKey, revision: 0, subscriptions: {}, updatedAt: now.toISOString() };
}

const nonNeg = (v: unknown): v is number => typeof v === 'number' && Number.isSafeInteger(v) && v >= 0;
const REASONS: GapReason[] = ['ring-dropped', 'backfill-bounded', 'seq-regression'];

function migrateGap(raw: unknown): GapRecord | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const g = raw as Record<string, unknown>;
  if (!nonNeg(g.from) || !nonNeg(g.to) || g.to < g.from) return null;
  if (!REASONS.includes(g.reason as GapReason)) return null;
  const out: GapRecord = {
    from: g.from,
    to: g.to,
    reason: g.reason as GapReason,
    detectedAt: typeof g.detectedAt === 'string' ? g.detectedAt : new Date(0).toISOString(),
  };
  if (nonNeg(g.generation)) out.generation = g.generation;
  return out;
}

function migrateSub(room: string, raw: unknown): SubscriptionState | null {
  if (!isValidRoomName(room) || typeof raw !== 'object' || raw === null) return null;
  const s = raw as Record<string, unknown>;
  const headSeq = nonNeg(s.headSeq) ? s.headSeq : 0;
  const sub: SubscriptionState = {
    room,
    headSeq,
    notifiedSeq: nonNeg(s.notifiedSeq) ? Math.min(s.notifiedSeq, headSeq) : 0,
    acceptedSeq: nonNeg(s.acceptedSeq) ? Math.min(s.acceptedSeq, headSeq) : 0,
    gaps: Array.isArray(s.gaps) ? (s.gaps.map(migrateGap).filter(Boolean) as GapRecord[]).slice(-32) : [],
    status: s.status === 'paused' || s.status === 'error' ? s.status : 'active',
  };
  if (nonNeg(s.generation)) sub.generation = s.generation;
  if (typeof s.lastActivityAt === 'string') sub.lastActivityAt = s.lastActivityAt;
  if (typeof s.createdAt === 'string') sub.createdAt = s.createdAt;
  if (s.startFrom === 'now' || s.startFrom === 'retained') sub.startFrom = s.startFrom;
  if (s.headUnsettled === true) sub.headUnsettled = true;
  if (typeof s.lastError === 'object' && s.lastError !== null) {
    const e = s.lastError as Record<string, unknown>;
    if (typeof e.kind === 'string' && typeof e.at === 'string') sub.lastError = { kind: e.kind.slice(0, 64), at: e.at };
  }
  return sub;
}

/** Validates and upgrades a parsed state document. Throws StateError for unusable input. */
export function migrateState(raw: unknown): WatchStateV1 {
  if (typeof raw !== 'object' || raw === null) throw new StateError('state is not an object');
  const r = raw as Record<string, unknown>;
  if (r.version !== 1) throw new StateError(`unsupported state version ${String(r.version)}`);
  if (typeof r.origin !== 'string' || typeof r.scopeKey !== 'string') throw new StateError('state lacks origin/scopeKey');
  const subs: Record<string, SubscriptionState> = {};
  if (typeof r.subscriptions === 'object' && r.subscriptions !== null) {
    for (const [room, v] of Object.entries(r.subscriptions as Record<string, unknown>)) {
      const s = migrateSub(room, v);
      if (s) subs[room] = s;
    }
  }
  return {
    version: 1,
    origin: r.origin,
    scopeKey: r.scopeKey,
    revision: nonNeg(r.revision) ? r.revision : 0,
    subscriptions: subs,
    updatedAt: typeof r.updatedAt === 'string' ? r.updatedAt : new Date(0).toISOString(),
  };
}

export function cloneState(s: WatchStateV1): WatchStateV1 {
  return structuredClone(s);
}
