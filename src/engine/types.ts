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
};

export interface CoalesceOptions {
  quietMs: number;
  maxDelayMs: number;
  minWakeIntervalMs: number;
}

export const DEFAULT_COALESCE: CoalesceOptions = { quietMs: 2000, maxDelayMs: 10_000, minWakeIntervalMs: 30_000 };

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
  | 'rate-limited'
  | 'wait-not-held'
  | 'network-error'
  | 'server-error'
  | 'protocol-error'
  | 'refused'
  | 'backfill'
  | 'gap'
  | 'recreated'
  | 'regression'
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

export interface RoomMember {
  readonly id: string;
  readonly room: string;
  /** Resolve a pending start (probe read). Returns false to retry later. */
  prepare(): Promise<boolean>;
  /** Current head, or undefined while not ready. */
  head(): number | undefined;
  active(): boolean;
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
