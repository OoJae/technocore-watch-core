// createWatcher(): one scope's subscriptions, cursors, coalesced notices and paging.
//
// Cursors per subscription:
//   headSeq     highest seq observed from the origin (upstream polls resume here)
//   notifiedSeq highest seq covered by a notice the harness confirmed (markNotified)
//   acceptedSeq highest seq the consumer acknowledged (ack) — the "handled" cursor
//
// Delivery is at-least-once: a notice emitted but not marked before a crash is emitted
// again on restart (headSeq > notifiedSeq). When headSeq <= notifiedSeq nothing re-notifies,
// while unread counts (headSeq - acceptedSeq - gapped) stay visible in status().
import { randomUUID } from 'node:crypto';
import { type Clock, isAbortError, systemClock } from '../limits/clock.ts';
import { assertRoomName } from '../protocol/names.ts';
import type { Refusal } from '../protocol/parse.ts';
import { asciiJson, messageJson, renderNotice, renderNoticeLine } from '../protocol/render.ts';
import { applyAck, applyNotified, boundGaps, gappedCount, newSubscription, reconcile, unreadCount, type ReconcileOptions, type ReconcileResult } from '../protocol/reconcile.ts';
import type { ExportScan, GapRecord, Message, Notice, SubscriptionState, WatchStateV1 } from '../protocol/types.ts';
import { RevisionConflictError, emptyState } from '../state/model.ts';
import type { StateStore } from '../state/store.ts';
import { normalizeOrigin } from '../transport/client.ts';
import type { Transport } from '../transport/http.ts';
import { fetchForward } from './backfill.ts';
import { Coalescer, type FlushBatch } from './coalescer.ts';
import { HostPool } from './host-pool.ts';
import {
  type CoalesceOptions,
  DEFAULT_LIMITS,
  type Limits,
  type Logger,
  MAX_DATE_MS,
  type Observation,
  type Page,
  type PendingNoticeTiming,
  type RoomMember,
  type SubscriptionStatusView,
  type WatchEvent,
  type WatchStatus,
  definedLimits,
  validateLimits,
} from './types.ts';

export const WATCHER_ID = 'technocore-watch-core/0.1.0';

export interface WatcherOptions {
  origin: string;
  scopeKey: string;
  store: StateStore;
  /** Share upstream polls, budget and waiter slots with other watchers on the same origin. */
  pool?: HostPool;
  /** Used only when no pool is given. */
  transport?: Transport;
  limits?: Partial<Limits>;
  coalesce?: Partial<CoalesceOptions>;
  clock?: Clock;
  logger?: Logger;
  /** false = never poll (status/inbox/ack tools in a second process). Default true. */
  poll?: boolean;
  /** Append every notice to the store's notice journal before listeners run. */
  journalNotices?: boolean;
  /** Mark notices notified as soon as listeners ran without throwing. Default false. */
  autoMarkNotified?: boolean;
}

export interface SubscribeOptions {
  startFrom?: 'now' | 'retained';
  /** Resolve the start position with one read even when this watcher does not poll. */
  resolveNow?: boolean;
}

export interface Watcher extends AsyncDisposable {
  readonly scopeKey: string;
  readonly origin: string;
  readonly ready: Promise<void>;
  subscribe(room: string, o?: SubscribeOptions): Promise<SubscriptionState>;
  unsubscribe(room: string): Promise<void>;
  pause(room?: string): Promise<void>;
  resume(room?: string): Promise<void>;
  status(): WatchStatus;
  /**
   * Messages after acceptedSeq. `maxChars` (default limits.maxPageChars) bounds the RENDERED size:
   * the escaped JSON line each message becomes in renderPage, not its raw text length. The first
   * message of a page is always included, so a page can exceed it by at most one message.
   */
  readPage(room: string, o?: { max?: number; maxChars?: number }): Promise<Page>;
  /** renderNotice / renderNoticeLine bounded by this watcher's limits.noticeMaxChars. */
  renderNotice(n: Notice, form?: 'framed' | 'line'): string;
  ack(room: string, throughSeq: number): Promise<void>;
  markNotified(noticeId: string): Promise<void>;
  onNotice(fn: (n: Notice) => void | Promise<void>): () => void;
  onEvent(fn: (e: WatchEvent) => void): () => void;
  /** Force the pending notice out now (tests, shutdown hooks). */
  flushNotices(): void;
  /** Reload the store if another process changed it. */
  sync(): Promise<void>;
  dispose(): Promise<void>;
}

type Op = (s: WatchStateV1) => void;

interface EmittedNotice {
  rooms: { room: string; generation?: number; seqTo: number }[];
}

class SubscriptionMember implements RoomMember {
  readonly id: string;
  readonly room: string;
  private readonly w: WatcherImpl;
  private chain: Promise<unknown> = Promise.resolve();
  private staleStreak = 0;

  constructor(w: WatcherImpl, room: string) {
    this.w = w;
    this.room = room;
    this.id = `${w.scopeKey}#${room}`;
  }

  private sub(): SubscriptionState | undefined {
    return this.w.state.subscriptions[this.room];
  }

  head(): number | undefined {
    const s = this.sub();
    if (!s || s.startFrom) return undefined;
    return s.headSeq;
  }

  generation(): number | undefined {
    return this.sub()?.generation;
  }

  active(): boolean {
    const s = this.sub();
    return !!s && s.status === 'active' && !s.startFrom && !this.w.disposed;
  }

  pendingNotice(): PendingNoticeTiming | undefined {
    return this.w.pendingNotice();
  }

  prepare(): Promise<boolean> {
    return this.w.resolveStart(this.room);
  }

  onFatal(refusal: Refusal): void {
    this.w.markError(this.room, refusal.kind);
  }

  onObservation(obs: Observation): Promise<'ok' | 'retry'> {
    const run = this.chain.then(() => this.observe(obs));
    this.chain = run.catch(() => undefined);
    return run;
  }

  private async observe(obs: Observation): Promise<'ok' | 'retry'> {
    const w = this.w;
    const pool = w.pool;
    let since = obs.since;
    let view = obs.view;
    let backfill: ExportScan | undefined;
    let originReset = false;
    for (let attempt = 0; attempt < 5; attempt++) {
      if (w.disposed) return 'ok';
      const sub = this.sub();
      if (!sub || sub.status !== 'active' || sub.startFrom) return 'ok';
      const opts: ReconcileOptions = { maxGaps: w.limits.maxGaps };
      if (backfill) opts.backfill = backfill;
      if (originReset) opts.originReset = true;
      const r = reconcile(sub, since, view, new Date(w.clock.now()), opts);
      if (r.ignored === 'stale-generation') {
        // Usually a replayed or edge-cached response: retry with backoff (never re-poll at wire
        // speed). If the lower generation persists across STALE_CONFIRMATIONS observations AND a
        // fresh cache-busted read still reports it, the origin's data root was reset or restored:
        // resync from it as a restart instead of staying stuck on a generation that is gone.
        this.staleStreak++;
        w.noteStaleGeneration(this.room, sub.generation, view.generation, this.staleStreak);
        if (this.staleStreak < STALE_CONFIRMATIONS) return 'retry';
        const res = await pool.read({ room: this.room, since: sub.headSeq, limit: 200, cacheBust: true });
        if (!res.ok) return 'retry';
        pool.cache.add(this.room, res.view.generation, res.view.messages);
        since = sub.headSeq;
        view = res.view;
        backfill = undefined;
        const g = res.view.generation;
        originReset = g !== undefined && sub.generation !== undefined && sub.generation !== 0 && g < sub.generation;
        if (!originReset) this.staleStreak = 0;
        continue;
      }
      if (r.ignored) return 'ok';
      if (view.generation === undefined || sub.generation === undefined || view.generation >= sub.generation) this.staleStreak = 0;
      const nb = r.needsBackfill;
      if (nb) {
        if (nb.reason === 'catch-up') {
          const res = await pool.read({ room: this.room, since: sub.headSeq, limit: 200, cacheBust: true });
          if (!res.ok) return 'retry';
          pool.cache.add(this.room, res.view.generation, res.view.messages);
          since = sub.headSeq;
          view = res.view;
          backfill = undefined;
          continue;
        }
        const bf = await pool.backfill({ room: this.room, afterSeq: nb.afterSeq, beforeSeq: nb.beforeSeq });
        if (!bf.ok) return 'retry';
        backfill = bf;
        continue;
      }
      if (r.originReset) this.staleStreak = 0;
      w.applyReconcile(this.room, r, obs);
      return 'ok';
    }
    return 'retry';
  }
}

/** Consecutive lower-generation observations before a fresh read may confirm an origin reset. */
const STALE_CONFIRMATIONS = 3;

function gapKey(g: GapRecord): string {
  return `${g.generation ?? ''}|${g.from}|${g.to}|${g.reason}`;
}

function mergeGapLists(a: GapRecord[], b: GapRecord[]): GapRecord[] {
  const seen = new Set(a.map(gapKey));
  const out = a.map((g) => ({ ...g }));
  for (const g of b) if (!seen.has(gapKey(g))) out.push({ ...g });
  return out;
}

class WatcherImpl implements Watcher {
  readonly scopeKey: string;
  readonly origin: string;
  readonly store: StateStore;
  readonly limits: Limits;
  readonly clock: Clock;
  readonly pool: HostPool;
  readonly ready: Promise<void>;
  state: WatchStateV1;
  disposed = false;
  private readonly ownsPool: boolean;
  private readonly polling: boolean;
  private readonly journal: boolean;
  private readonly autoMark: boolean;
  private readonly coalescer: Coalescer;
  private savedRevision = 0;
  private unsaved: Op[] = [];
  private flushChain: Promise<void> = Promise.resolve();
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private syncTimer: ReturnType<typeof setInterval> | undefined;
  private readonly members = new Map<string, { member: SubscriptionMember; detach: () => void }>();
  private readonly noticeListeners = new Set<(n: Notice) => void | Promise<void>>();
  private readonly eventListeners = new Set<(e: WatchEvent) => void>();
  private readonly emitted = new Map<string, EmittedNotice>();
  private readonly offPool: () => void;
  private readonly logger: Logger | undefined;
  private syncing = false;

  constructor(o: WatcherOptions) {
    this.origin = normalizeOrigin(o.origin);
    if (typeof o.scopeKey !== 'string' || !o.scopeKey || o.scopeKey.length > 512) throw new TypeError('scopeKey must be a non-empty string (max 512 chars)');
    this.scopeKey = o.scopeKey;
    this.store = o.store;
    this.clock = o.clock ?? systemClock;
    this.logger = o.logger;
    this.polling = o.poll ?? true;
    this.journal = o.journalNotices ?? false;
    this.autoMark = o.autoMarkNotified ?? false;
    if (o.pool) {
      if (o.pool.origin !== this.origin) throw new TypeError('pool origin does not match watcher origin');
      this.pool = o.pool;
      this.ownsPool = false;
      this.limits = validateLimits({ ...o.pool.limits, ...definedLimits(o.limits) });
    } else {
      const poolOpts: ConstructorParameters<typeof HostPool>[0] = { origin: this.origin, clock: this.clock };
      if (o.transport) poolOpts.transport = o.transport;
      if (o.limits) poolOpts.limits = o.limits;
      this.pool = new HostPool(poolOpts);
      this.ownsPool = true;
      this.limits = { ...DEFAULT_LIMITS, ...definedLimits(o.limits) };
    }
    this.state = emptyState(this.origin, this.scopeKey, new Date(this.clock.now()));
    const coalesceOpts: ConstructorParameters<typeof Coalescer>[1] = {
      ...o.coalesce,
      clock: this.clock,
      maxRoomsPerNotice: this.limits.maxRoomsPerNotice,
      previewChars: this.limits.previewChars,
      maxGapsPerRoom: this.limits.maxGaps,
    };
    this.coalescer = new Coalescer((b) => this.emitNotice(b), coalesceOpts);
    this.offPool = this.pool.onEvent((e) => {
      if (e.room !== undefined && !this.state.subscriptions[e.room]) return;
      if (e.room === undefined && e.type !== 'slot-change') return;
      this.emitEvent({ ...e, scopeKey: this.scopeKey });
    });
    this.ready = this.init();
    this.ready.catch(() => undefined);
  }

  private async init(): Promise<void> {
    const loaded = await this.store.load(this.scopeKey);
    if (loaded) {
      if (normalizeOrigin(loaded.origin) !== this.origin) {
        throw new Error(`state for scope was created for a different origin (${JSON.stringify(loaded.origin).slice(0, 80)})`);
      }
      this.state = loaded;
      this.savedRevision = loaded.revision;
    }
    if (this.disposed) return;
    this.expireIdle();
    this.syncMembers();
    // At-least-once: re-announce anything observed but never confirmed notified.
    for (const sub of Object.values(this.state.subscriptions)) {
      if (sub.status !== 'active' || sub.startFrom || sub.headSeq <= sub.notifiedSeq) continue;
      const gaps = sub.gaps.filter(
        (g) => (g.generation === undefined || g.generation === sub.generation) && g.to > sub.notifiedSeq && g.from <= sub.headSeq,
      );
      const gapped = gappedCount(gaps, sub.generation, sub.notifiedSeq + 1, sub.headSeq);
      const delta: Parameters<Coalescer['add']>[0] = {
        room: sub.room,
        delivered: [],
        gaps,
        rangeFrom: sub.notifiedSeq,
        rangeTo: sub.headSeq,
        countOverride: Math.max(0, sub.headSeq - sub.notifiedSeq - gapped),
        transport: 'http-longpoll',
        fetchedAt: sub.lastActivityAt ?? new Date(this.clock.now()).toISOString(),
      };
      if (sub.generation !== undefined) delta.generation = sub.generation;
      this.coalescer.add(delta);
    }
    if (this.limits.syncIntervalMs > 0) {
      this.syncTimer = setInterval(() => void this.sync().catch(() => undefined), this.limits.syncIntervalMs);
    }
  }

  // ---------------------------------------------------------------- persistence

  private mutate(op: Op, persist: 'now' | 'soon' = 'soon'): Promise<void> {
    op(this.state);
    this.unsaved.push(op);
    if (persist === 'now') return this.flush();
    if (!this.flushTimer && !this.disposed) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = undefined;
        void this.flush().catch((e) => this.emitEvent({ type: 'error', at: this.nowIso(), detail: { message: String(e?.message ?? e) } }));
      }, 250);
    }
    return Promise.resolve();
  }

  flush(): Promise<void> {
    const run = this.flushChain.then(() => this.flushOnce());
    this.flushChain = run.catch(() => undefined);
    return run;
  }

  private async flushOnce(): Promise<void> {
    for (let attempt = 0; attempt < 10 && this.unsaved.length; attempt++) {
      const batch = this.unsaved.length;
      const snapshot = structuredClone(this.state);
      try {
        const saved = await this.store.save(snapshot, this.savedRevision);
        this.savedRevision = saved.revision;
        this.state.revision = saved.revision;
        this.unsaved.splice(0, batch);
        return;
      } catch (e) {
        if (!(e instanceof RevisionConflictError)) throw e;
        this.emitEvent({ type: 'state-conflict', at: this.nowIso(), detail: { expected: e.expected, actual: e.actual } });
        await this.reloadFromStore();
      }
    }
    if (this.unsaved.length) throw new Error('could not persist watcher state after repeated revision conflicts');
  }

  private async reloadFromStore(): Promise<void> {
    const disk = (await this.store.load(this.scopeKey)) ?? emptyState(this.origin, this.scopeKey, new Date(this.clock.now()));
    this.state = disk;
    this.savedRevision = disk.revision;
    for (const op of this.unsaved) op(this.state);
    this.syncMembers();
  }

  async sync(): Promise<void> {
    if (this.disposed || this.syncing) return;
    this.syncing = true;
    try {
      await this.ready;
      const rev = await this.store.revision(this.scopeKey);
      if (rev < 0 || rev === this.savedRevision) return;
      await this.flushChain;
      if ((await this.store.revision(this.scopeKey)) === this.savedRevision) return;
      const run = this.flushChain.then(() => this.reloadFromStore());
      this.flushChain = run.catch(() => undefined);
      await run;
    } finally {
      this.syncing = false;
    }
  }

  // ---------------------------------------------------------------- membership

  private syncMembers(): void {
    if (this.disposed) return;
    let dropped = false;
    for (const [room, entry] of [...this.members]) {
      const sub = this.state.subscriptions[room];
      if (!sub || sub.status !== 'active') {
        entry.detach();
        this.members.delete(room);
        if (!sub) {
          this.coalescer.remove(room);
          dropped = true;
        }
      }
    }
    if (dropped) this.noticeStateChanged();
    if (!this.polling) return;
    for (const sub of Object.values(this.state.subscriptions)) {
      if (sub.status !== 'active' || this.members.has(sub.room)) continue;
      const member = new SubscriptionMember(this, sub.room);
      try {
        const detach = this.pool.attach(member);
        this.members.set(sub.room, { member, detach });
      } catch (e) {
        this.emitEvent({ type: 'error', at: this.nowIso(), room: sub.room, detail: { message: String((e as Error).message) } });
      }
    }
  }

  private expireIdle(): void {
    const ttlMs = this.limits.subscriptionTtlDays * 86_400_000;
    if (!(ttlMs > 0)) return;
    const now = this.clock.now();
    for (const sub of Object.values(this.state.subscriptions)) {
      const last = Date.parse(sub.lastActivityAt ?? sub.createdAt ?? '');
      if (!Number.isFinite(last) || now - last <= ttlMs || unreadCount(sub) > 0) continue;
      const room = sub.room;
      void this.mutate((s) => {
        delete s.subscriptions[room];
      });
      this.emitEvent({ type: 'subscription-expired', at: this.nowIso(), room, detail: { idleDays: this.limits.subscriptionTtlDays } });
    }
  }

  async resolveStart(room: string): Promise<boolean> {
    const sub = this.state.subscriptions[room];
    if (!sub || !sub.startFrom) return true;
    let res;
    try {
      res = await this.pool.read({ room, limit: 1, cacheBust: true });
    } catch (e) {
      if (isAbortError(e) && this.pool.signal.aborted) throw e;
      return false;
    }
    if (!res.ok) {
      if (res.refusal.kind === 'bad-request' || res.refusal.kind === 'forbidden') {
        this.markError(room, res.refusal.kind);
        return true;
      }
      return false;
    }
    const view = res.view;
    const mode = sub.startFrom;
    await this.mutate((s) => {
      const cur = s.subscriptions[room];
      if (!cur || !cur.startFrom) return;
      if (view.generation !== undefined) cur.generation = view.generation;
      if (mode === 'now') {
        cur.headSeq = view.last_seq;
        cur.notifiedSeq = view.last_seq;
        cur.acceptedSeq = view.last_seq;
        // An empty view names no high-water mark (last_seq echoes 0 for a reaped room with a
        // floor, or an e- room whose records all expired): the first records settle it.
        if (view.messages.length === 0) cur.headUnsettled = true;
      }
      delete cur.startFrom;
    }, 'now');
    return true;
  }

  markError(room: string, kind: string): void {
    const at = this.nowIso();
    this.mutate((s) => {
      const cur = s.subscriptions[room];
      if (!cur) return;
      cur.status = 'error';
      cur.lastError = { kind, at };
    }, 'now').catch((e: unknown) => this.emitEvent({ type: 'error', at, room, detail: { message: String((e as Error)?.message ?? e).slice(0, 200) } }));
    this.emitEvent({ type: 'refused', at, room, detail: { kind } });
  }

  noteStaleGeneration(room: string, stored: number | undefined, observed: number | undefined, streak: number): void {
    this.emitEvent({ type: 'stale-generation', at: this.nowIso(), room, detail: { stored: stored ?? null, observed: observed ?? null, streak } });
  }

  applyReconcile(room: string, r: ReconcileResult, obs: Observation): void {
    if (this.disposed) return; // an observation that raced dispose is re-read after restart
    const next = r.next;
    const reset = Boolean(r.regression);
    void this.mutate((s) => {
      const cur = s.subscriptions[room];
      if (!cur) return;
      // A lower generation only ever lands through a confirmed origin reset, and only over the
      // generation it was confirmed against.
      const backwards = cur.generation !== undefined && next.generation !== undefined && cur.generation > next.generation;
      if (backwards && !(r.originReset && r.recreated?.from === cur.generation)) return;
      if (reset) {
        cur.generation = next.generation;
        cur.headSeq = next.headSeq;
        cur.notifiedSeq = next.notifiedSeq;
        cur.acceptedSeq = next.acceptedSeq;
        cur.gaps = next.gaps.map((g) => ({ ...g }));
        if (next.headUnsettled) cur.headUnsettled = true;
        else delete cur.headUnsettled;
      } else {
        if (next.generation !== undefined) cur.generation = next.generation;
        cur.headSeq = Math.max(cur.headSeq, next.headSeq);
        if (r.settledStart !== undefined && cur.headUnsettled) {
          cur.notifiedSeq = Math.max(cur.notifiedSeq, r.settledStart);
          cur.acceptedSeq = Math.max(cur.acceptedSeq, r.settledStart);
          delete cur.headUnsettled;
        }
        cur.gaps = boundGaps(mergeGapLists(cur.gaps, r.gaps), cur, this.limits.maxGaps);
      }
      if (next.lastActivityAt) cur.lastActivityAt = next.lastActivityAt;
      cur.notifiedSeq = Math.min(cur.notifiedSeq, cur.headSeq);
      cur.acceptedSeq = Math.min(cur.acceptedSeq, cur.headSeq);
      if (cur.lastError && cur.status === 'active') delete cur.lastError;
    });
    const at = this.nowIso();
    if (r.range) {
      this.emitEvent({ type: 'activity', at, room, detail: { from: r.range.from, to: r.range.to, delivered: r.delivered.length, transport: obs.transport, fetchedAt: obs.fetchedAt } });
    }
    for (const g of r.gaps) this.emitEvent({ type: 'gap', at, room, detail: { ...g } });
    if (r.recreated) this.emitEvent({ type: 'recreated', at, room, detail: { ...r.recreated } });
    if (r.regression) this.emitEvent({ type: 'regression', at, room, detail: { ...r.regression } });
    if (r.range || r.recreated) {
      const delta: Parameters<Coalescer['add']>[0] = {
        room,
        delivered: r.delivered,
        gaps: r.gaps,
        rangeFrom: r.range?.from ?? next.headSeq,
        rangeTo: r.range?.to ?? next.headSeq,
        transport: obs.transport,
        fetchedAt: obs.fetchedAt,
      };
      if (next.generation !== undefined) delta.generation = next.generation;
      if (r.recreated) delta.recreated = r.recreated;
      if (r.regression) delta.regression = r.regression;
      this.coalescer.add(delta);
    }
  }

  // ---------------------------------------------------------------- notices & events

  /** When this scope's pending notice is due and whether activity can still move that; undefined when none is pending. */
  pendingNotice(): PendingNoticeTiming | undefined {
    if (this.disposed) return undefined;
    return this.coalescer.pendingTiming();
  }

  /** The pending notice flushed or lost rooms: let flood-paced pollers of this scope's rooms re-check. */
  private noticeStateChanged(): void {
    if (this.disposed) return;
    for (const room of this.members.keys()) this.pool.noticeChanged(room);
  }

  private nowIso(): string {
    return new Date(this.clock.now()).toISOString();
  }

  private emitEvent(e: WatchEvent): void {
    for (const fn of this.eventListeners) {
      try {
        fn(e);
      } catch {
        // never let a listener break the watcher
      }
    }
  }

  private emitNotice(b: FlushBatch): void {
    if (this.disposed) return;
    this.noticeStateChanged();
    const notice: Notice = {
      id: `tcw-${randomUUID()}`,
      scopeKey: this.scopeKey,
      createdAt: this.nowIso(),
      rooms: b.rooms,
      totalNew: b.totalNew,
      provenance: { origin: this.origin, transport: b.transport, watcher: WATCHER_ID, fetchedAt: b.fetchedAt },
    };
    if (b.moreRooms) notice.moreRooms = b.moreRooms;
    // Record every room in the batch (including ones summarised as "+N more") for markNotified.
    const covered = [
      ...b.rooms.map((r) => (r.generation === undefined ? { room: r.room, seqTo: r.seqTo } : { room: r.room, generation: r.generation, seqTo: r.seqTo })),
      ...b.overflow,
    ];
    this.emitted.set(notice.id, { rooms: covered });
    while (this.emitted.size > 64) this.emitted.delete(this.emitted.keys().next().value as string);
    this.emitEvent({ type: 'notice', at: notice.createdAt, detail: { id: notice.id, totalNew: notice.totalNew, rooms: notice.rooms.length } });
    void this.deliver(notice);
  }

  private async deliver(notice: Notice): Promise<void> {
    try {
      if (this.journal) await this.store.appendNotice(this.scopeKey, notice);
      let failed = false;
      for (const fn of [...this.noticeListeners]) {
        try {
          await fn(structuredClone(notice));
        } catch (e) {
          failed = true;
          this.emitEvent({ type: 'error', at: this.nowIso(), detail: { message: `notice listener failed: ${String((e as Error)?.message ?? e).slice(0, 200)}` } });
        }
      }
      if (this.autoMark && !failed) await this.markNotified(notice.id);
    } catch (e) {
      this.emitEvent({ type: 'error', at: this.nowIso(), detail: { message: String((e as Error)?.message ?? e).slice(0, 200) } });
    }
  }

  onNotice(fn: (n: Notice) => void | Promise<void>): () => void {
    this.noticeListeners.add(fn);
    return () => this.noticeListeners.delete(fn);
  }

  onEvent(fn: (e: WatchEvent) => void): () => void {
    this.eventListeners.add(fn);
    return () => this.eventListeners.delete(fn);
  }

  flushNotices(): void {
    this.coalescer.flushNow();
  }

  renderNotice(n: Notice, form: 'framed' | 'line' = 'framed'): string {
    const opts = { maxChars: this.limits.noticeMaxChars };
    return form === 'line' ? renderNoticeLine(n, opts) : renderNotice(n, opts);
  }

  async markNotified(noticeId: string): Promise<void> {
    await this.ready;
    const entry = this.emitted.get(noticeId);
    if (!entry) return;
    this.emitted.delete(noticeId);
    await this.mutate((s) => {
      for (const r of entry.rooms) {
        const cur = s.subscriptions[r.room];
        if (!cur) continue;
        s.subscriptions[r.room] = applyNotified(cur, r.seqTo, r.generation);
      }
    }, 'now');
  }

  // ---------------------------------------------------------------- public API

  private assertLive(): void {
    if (this.disposed) throw new Error('watcher disposed');
  }

  async subscribe(room: string, o: SubscribeOptions = {}): Promise<SubscriptionState> {
    assertRoomName(room);
    await this.ready;
    this.assertLive();
    const existing = this.state.subscriptions[room];
    if (existing) return structuredClone(existing);
    const count = Object.keys(this.state.subscriptions).length;
    if (count >= this.limits.maxSubscriptionsPerScope) {
      throw new RangeError(`subscription limit reached (${this.limits.maxSubscriptionsPerScope} per scope)`);
    }
    const sub = newSubscription(room, new Date(this.clock.now()), o.startFrom ?? 'now');
    await this.mutate((s) => {
      if (!s.subscriptions[room]) s.subscriptions[room] = structuredClone(sub);
    }, 'now');
    if (this.polling || o.resolveNow) {
      await this.resolveStart(room).catch(() => false);
      this.syncMembers();
    }
    return structuredClone(this.state.subscriptions[room] ?? sub);
  }

  async unsubscribe(room: string): Promise<void> {
    await this.ready;
    this.assertLive();
    const entry = this.members.get(room);
    if (entry) {
      entry.detach();
      this.members.delete(room);
    }
    this.coalescer.remove(room);
    this.noticeStateChanged();
    await this.mutate((s) => {
      delete s.subscriptions[room];
    }, 'now');
  }

  private async setStatus(room: string | undefined, status: 'active' | 'paused'): Promise<void> {
    await this.ready;
    this.assertLive();
    if (room !== undefined) assertRoomName(room);
    await this.mutate((s) => {
      for (const sub of Object.values(s.subscriptions)) {
        if (room !== undefined && sub.room !== room) continue;
        if (status === 'paused' && sub.status === 'active') sub.status = 'paused';
        if (status === 'active' && (sub.status === 'paused' || sub.status === 'error')) {
          sub.status = 'active';
          delete sub.lastError;
        }
      }
    }, 'now');
    this.syncMembers();
  }

  pause(room?: string): Promise<void> {
    return this.setStatus(room, 'paused');
  }

  resume(room?: string): Promise<void> {
    return this.setStatus(room, 'active');
  }

  status(): WatchStatus {
    const subs: SubscriptionStatusView[] = Object.values(this.state.subscriptions)
      .sort((a, b) => a.room.localeCompare(b.room))
      .map((s) => {
        const v: SubscriptionStatusView = {
          room: s.room,
          status: s.status,
          headSeq: s.headSeq,
          notifiedSeq: s.notifiedSeq,
          acceptedSeq: s.acceptedSeq,
          unread: unreadCount(s),
          gaps: s.gaps.map((g) => ({ ...g })),
          mode: s.status === 'active' && this.polling ? this.pool.pollerMode(s.room) : 'idle',
        };
        if (s.generation !== undefined) v.generation = s.generation;
        if (s.lastActivityAt) v.lastActivityAt = s.lastActivityAt;
        if (s.lastError) v.lastError = { ...s.lastError };
        if (s.startFrom) v.pendingStart = s.startFrom;
        return v;
      });
    const out: WatchStatus = {
      scopeKey: this.scopeKey,
      origin: this.origin,
      subscriptions: subs,
      totals: {
        subscriptions: subs.length,
        unread: subs.reduce((n, s) => n + s.unread, 0),
        errors: subs.filter((s) => s.status === 'error').length,
        paused: subs.filter((s) => s.status === 'paused').length,
      },
      pendingNotice: this.coalescer.hasPending,
      revision: this.state.revision,
    };
    const next = this.coalescer.nextFlushAt;
    // A due time beyond the Date range ("effectively never" coalescer timings) has no ISO form.
    if (next !== undefined && Math.abs(next) <= MAX_DATE_MS) out.nextWakeAt = new Date(next).toISOString();
    return out;
  }

  async readPage(room: string, o: { max?: number; maxChars?: number } = {}): Promise<Page> {
    assertRoomName(room);
    await this.ready;
    this.assertLive();
    const sub = this.state.subscriptions[room];
    if (!sub) throw new Error(`not subscribed to ${room}`);
    const max = Math.min(200, Math.max(1, Math.floor(o.max ?? this.limits.pageSize)));
    const maxChars = Math.max(1, Math.floor(o.maxChars ?? this.limits.maxPageChars));
    const generation = sub.generation;
    const end = sub.headSeq;
    const messages: Message[] = [];
    const gaps: GapRecord[] = [];
    let seq = sub.acceptedSeq + 1;
    let chars = 0;
    let fetches = 0;
    let stoppedEarly = false;
    const stateGap = (n: number) =>
      sub.gaps.find(
        (g) => (g.generation === undefined || generation === undefined || g.generation === generation) && g.reason !== 'backfill-bounded' && g.from <= n && n <= g.to,
      );
    const newGaps: GapRecord[] = [];
    while (seq <= end && messages.length < max) {
      const cached = this.pool.cache.get(room, generation, seq);
      if (cached) {
        // Count what renderPage emits (every non-ASCII unit becomes \uXXXX), sender included.
        const size = asciiJson(messageJson(cached)).length;
        if (messages.length > 0 && chars + size > maxChars) {
          stoppedEarly = true;
          break;
        }
        messages.push(cached);
        chars += size;
        seq++;
        continue;
      }
      const g = stateGap(seq);
      if (g) {
        gaps.push({ ...g });
        seq = g.to + 1;
        continue;
      }
      if (fetches >= 3) {
        stoppedEarly = true;
        break;
      }
      fetches++;
      const f = await fetchForward(this.pool, room, seq - 1, end);
      if (!f.ok) {
        if (messages.length || gaps.length) {
          stoppedEarly = true;
          break;
        }
        throw new Error(`origin refused the read (${f.refusal.kind})`);
      }
      if (f.generation !== undefined && generation !== undefined && f.generation !== generation) {
        stoppedEarly = true; // recreated: the poller reconciles first
        break;
      }
      this.pool.cache.add(room, f.generation ?? generation, f.records);
      const firstRec = f.records[0];
      if (!firstRec) {
        if (f.bounded) stoppedEarly = true;
        // Nothing retained between seq and the head (or the head is ahead of the origin).
        if (!f.bounded && f.oldestRetainedSeq !== undefined && f.oldestRetainedSeq > seq) {
          const to = Math.min(end, f.oldestRetainedSeq - 1);
          const gap: GapRecord = { from: seq, to, reason: 'ring-dropped', detectedAt: this.nowIso() };
          if (generation !== undefined) gap.generation = generation;
          gaps.push(gap);
          newGaps.push(gap);
          seq = to + 1;
          continue;
        }
        break;
      }
      if (firstRec.seq > seq && !f.bounded) {
        const gap: GapRecord = { from: seq, to: firstRec.seq - 1, reason: 'ring-dropped', detectedAt: this.nowIso() };
        if (generation !== undefined) gap.generation = generation;
        gaps.push(gap);
        newGaps.push(gap);
        seq = firstRec.seq;
      } else if (firstRec.seq > seq) {
        stoppedEarly = true;
        break;
      }
    }
    if (newGaps.length) {
      await this.mutate((s) => {
        const cur = s.subscriptions[room];
        if (!cur || cur.generation !== generation) return;
        cur.gaps = boundGaps(mergeGapLists(cur.gaps, newGaps), cur, this.limits.maxGaps);
      });
    }
    const toSeq = seq - 1;
    const page: Page = {
      room,
      messages: messages.map((m) => ({ ...m })),
      fromSeq: sub.acceptedSeq + 1,
      toSeq,
      gaps,
      hasMore: toSeq < end || stoppedEarly,
      untrusted: true,
    };
    if (generation !== undefined) page.generation = generation;
    return page;
  }

  async ack(room: string, throughSeq: number): Promise<void> {
    assertRoomName(room);
    await this.ready;
    this.assertLive();
    if (!Number.isFinite(throughSeq)) throw new TypeError('throughSeq must be a finite number');
    const generation = this.state.subscriptions[room]?.generation;
    await this.mutate((s) => {
      const cur = s.subscriptions[room];
      if (!cur) return;
      s.subscriptions[room] = applyAck(cur, throughSeq, generation);
    }, 'now');
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    await this.ready.catch(() => undefined);
    if (this.syncTimer) clearInterval(this.syncTimer);
    this.syncTimer = undefined;
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
    this.coalescer.dispose();
    for (const { detach } of this.members.values()) detach();
    this.members.clear();
    this.offPool();
    try {
      await this.flush();
    } catch (e) {
      this.logger?.warn?.('final state flush failed', { message: String((e as Error)?.message ?? e) });
    }
    if (this.ownsPool) await this.pool.dispose();
    this.noticeListeners.clear();
    this.eventListeners.clear();
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.dispose();
  }
}

export function createWatcher(o: WatcherOptions): Watcher {
  return new WatcherImpl(o);
}
