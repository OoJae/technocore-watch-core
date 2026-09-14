import type { Refusal } from '../protocol/parse.ts';
import type { GapRecord, Message, ReadView, SubscriptionState } from '../protocol/types.ts';

export interface Limits {
  maxSubscriptionsPerScope: number;
  maxSubscriptionsPerHost: number;
  /** Concurrent requests per origin (long-polls included). */
  maxConcurrentRequests: number;
  /** Concurrent long-polls per origin; also capped at the origin's waiters-per-IP minus one. */
  maxConcurrentLongPolls: number;
  /** Fraction of the origin's published read budget this process may spend. */
  readBudgetFraction: number;
  /** Override the origin's reads/minute (otherwise read from /config). */
  readsPerMinute?: number;
  waitSeconds: number;
  pageSize: number;
  maxPageChars: number;
  maxExportBytes: number;
  maxBackfillRecords: number;
  sweepMinIntervalMs: number;
  minSlotTenureMs: number;
  backoffBaseMs: number;
  backoffCapMs: number;
  noticeMaxChars: number;
  previewChars: number;
  maxGaps: number;
  maxRoomsPerNotice: number;
  cacheMessages: number;
  subscriptionTtlDays: number;
  /** How often a watcher checks the store for changes made by other processes (0 = never). */
  syncIntervalMs: number;
  /**
   * Flood re-poll pacing. After a poll that returned messages, when EVERY subscription following the
   * room already has a pending notice, the room's poller does not re-poll at once: new messages would
   * only merge into notices that are not due yet. It naps until `repollLeadMs` before the earliest
   * moment a later read could change one of those notices, and at most the long-poll wait. For a notice
   * held by `maxDelayMs` or `minWakeIntervalMs` that moment is its due time; for one arriving activity
   * can still push out (`quietMs`) it is `quietMs` before its due time, so while `quietMs` drives a
   * notice there is no nap at all. From `repollLeadMs` before that moment the room is read live again.
   * As long as a read completes within `repollLeadMs`, notices go out at the same times with the same
   * messages as without pacing; what changes is when the poll sees messages while a held notice is
   * pending (`headSeq`/`unread` advance at the paced rate).
   *
   * A wake (new subscriber, slot change), a notice that flushes or is dropped early, unsubscribe and
   * dispose all cut the nap. A nap is at least `floodRepollMinMs` (shorter waits are read live) and never
   * so long that, at the rate the last reads showed, more than half a 200-message read page piles up
   * (which would need an export to fill in); a flood too fast for even the shortest nap is not paced.
   * Messages stay on the origin meanwhile, so nothing is lost. false = re-poll immediately. Pool-level:
   * read from the HostPool's limits.
   */
  floodRepollPacing: boolean;
  /**
   * How long before a pending notice is due (or could start to move) a paced nap ends and live reads
   * resume (ms, 0..10000). Must cover a read's round trip: a read that completes later than this can
   * move messages into the next notice.
   */
  repollLeadMs: number;
  /** Shortest paced nap (ms, 100..10000); a wait shorter than this is read live. */
  floodRepollMinMs: number;
}

export const DEFAULT_LIMITS: Limits = {
  maxSubscriptionsPerScope: 16,
  maxSubscriptionsPerHost: 64,
  maxConcurrentRequests: 4,
  maxConcurrentLongPolls: 3,
  readBudgetFraction: 0.5,
  waitSeconds: 10,
  pageSize: 50,
  maxPageChars: 16_000,
  maxExportBytes: 12 << 20,
  maxBackfillRecords: 2000,
  sweepMinIntervalMs: 15_000,
  minSlotTenureMs: 60_000,
  backoffBaseMs: 500,
  backoffCapMs: 60_000,
  noticeMaxChars: 1500,
  previewChars: 0,
  maxGaps: 8,
  maxRoomsPerNotice: 16,
  cacheMessages: 2000,
  subscriptionTtlDays: 7,
  syncIntervalMs: 2000,
  floodRepollPacing: true,
  repollLeadMs: 500,
  floodRepollMinMs: 1000,
};

/** Largest epoch ms a Date can hold; toISOString() throws beyond it. */
export const MAX_DATE_MS = 8.64e15;

/** Bounds for the flood re-poll pacing limits (see Limits). */
export const PACING_BOUNDS = {
  repollLeadMs: { min: 0, max: 10_000 },
  floodRepollMinMs: { min: 100, max: 10_000 },
} as const;

/** Drops keys whose value is undefined, so merging over DEFAULT_LIMITS never erases a default. */
export function definedLimits(l: Partial<Limits> | undefined): Partial<Limits> {
  return Object.fromEntries(Object.entries(l ?? {}).filter(([, v]) => v !== undefined)) as Partial<Limits>;
}

/**
 * Throws a RangeError/TypeError for limits that would break polling. Only the fields validated here
 * are checked; returns the same object.
 */
export function validateLimits<T extends Partial<Limits>>(l: T): T {
  if (l.floodRepollPacing !== undefined && typeof l.floodRepollPacing !== 'boolean') {
    throw new TypeError('limits.floodRepollPacing must be a boolean');
  }
  for (const key of ['repollLeadMs', 'floodRepollMinMs'] as const) {
    const v = l[key];
    if (v === undefined) continue;
    const { min, max } = PACING_BOUNDS[key];
    if (typeof v !== 'number' || !Number.isFinite(v) || v < min || v > max) {
      throw new RangeError(`limits.${key} must be a number in [${min}, ${max}]`);
    }
  }
  return l;
}

export interface CoalesceOptions {
  quietMs: number;
  maxDelayMs: number;
  minWakeIntervalMs: number;
}

/**
 * Tuned on W1 (dsh-technocore-watch bench/results/sweep-2026-09-14): in a steady flood a notice window is
 * max(minWakeIntervalMs, maxDelayMs), so wake latency there is roughly uniform over 8 s. minWakeIntervalMs
 * above maxDelayMs keeps a flood's pending notice held by minWakeIntervalMs, which is what lets flood
 * re-poll pacing nap.
 */
export const DEFAULT_COALESCE: CoalesceOptions = { quietMs: 2000, maxDelayMs: 7000, minWakeIntervalMs: 8000 };

export type TransportKind = 'http-longpoll' | 'http-sweep';

export interface Observation {
  room: string;
  since: number;
  view: ReadView;
  fetchedAt: string;
  transport: TransportKind;
}

export type WatchEventType =
  | 'poll-start'
  | 'poll-end'
  | 'poll-paced'
  | 'rate-limited'
  | 'wait-not-held'
  | 'network-error'
  | 'server-error'
  | 'protocol-error'
  | 'refused'
  | 'backfill'
  | 'activity'
  | 'gap'
  | 'recreated'
  | 'regression'
  | 'stale-generation'
  | 'notice'
  | 'slot-change'
  | 'state-conflict'
  | 'subscription-expired'
  | 'error';

export interface WatchEvent {
  type: WatchEventType;
  at: string;
  room?: string;
  scopeKey?: string;
  detail?: Record<string, unknown>;
}

/** When a pending notice is due, and when newly observed activity could still move that (epoch ms). */
export interface PendingNoticeTiming {
  /** When the notice is due. */
  dueAt: number;
  /**
   * Activity first observed after this moment pushes `dueAt` later (quietMs); `dueAt` itself when
   * nothing can move it any more (maxDelayMs or minWakeIntervalMs holds it). Activity observed up to
   * this moment leaves the notice's timing where it is.
   */
  movableFrom: number;
}

export interface RoomMember {
  readonly id: string;
  readonly room: string;
  /** Resolve a pending start (probe read). Returns false to retry later. */
  prepare(): Promise<boolean>;
  /** Current head, or undefined while not ready. */
  head(): number | undefined;
  /** The generation this member last settled; a view reporting another one is always forwarded. */
  generation(): number | undefined;
  active(): boolean;
  /**
   * Timing of the notice this member already has pending, or undefined when none is pending. Optional:
   * the room's poller paces flood re-polls only while every active member reports one (a member without
   * this method, or one that throws or returns non-finite times, simply never allows pacing). A member
   * whose pending notice goes away early (flushed, dropped) should call HostPool.noticeChanged(room).
   */
  pendingNotice?(): PendingNoticeTiming | undefined;
  onObservation(obs: Observation): Promise<'ok' | 'retry'>;
  onFatal(refusal: Refusal): void;
}

export interface Page {
  room: string;
  generation?: number;
  messages: Message[];
  fromSeq: number;
  toSeq: number;
  gaps: GapRecord[];
  hasMore: boolean;
  untrusted: true;
}

export interface SubscriptionStatusView {
  room: string;
  status: SubscriptionState['status'];
  generation?: number;
  headSeq: number;
  notifiedSeq: number;
  acceptedSeq: number;
  unread: number;
  gaps: GapRecord[];
  lastActivityAt?: string;
  lastError?: { kind: string; at: string };
  pendingStart?: 'now' | 'retained';
  mode: 'long-poll' | 'sweep' | 'idle';
}

export interface WatchStatus {
  scopeKey: string;
  origin: string;
  subscriptions: SubscriptionStatusView[];
  totals: { subscriptions: number; unread: number; errors: number; paused: number };
  pendingNotice: boolean;
  nextWakeAt?: string;
  revision: number;
}

export interface Logger {
  debug?(msg: string, detail?: Record<string, unknown>): void;
  warn?(msg: string, detail?: Record<string, unknown>): void;
}
