// HostPool: everything shared per origin within one process — the rate governor, the
// request/long-poll/export semaphores, the slot scheduler, the message cache, and ONE
// upstream poller per room whose results fan out to every subscribed scope.
import { DecorrelatedJitter, jitter } from '../limits/backoff.ts';
import { AbortError, type Clock, isAbortError, sleep, systemClock } from '../limits/clock.ts';
import { RateGovernor } from '../limits/governor.ts';
import { WaiterSemaphore } from '../limits/semaphore.ts';
import { ProtocolError, type Refusal } from '../protocol/parse.ts';
import {
  type ExportScanResult,
  type OriginConfig,
  type ReadRoomResult,
  TechnocoreReader,
  DEFAULT_ORIGIN_CONFIG,
} from '../transport/client.ts';
import { TransportError, type Transport } from '../transport/http.ts';
import { MessageCache } from './cache.ts';
import { SlotScheduler } from './scheduler.ts';
import { DEFAULT_LIMITS, type Limits, type Observation, type RoomMember, type WatchEvent, type WatchEventType, definedLimits, validateLimits } from './types.ts';

export interface HostPoolOptions {
  origin: string;
  transport?: Transport;
  limits?: Partial<Limits>;
  clock?: Clock;
  random?: () => number;
  /** Skip GET /config and use these values instead. */
  originConfig?: Partial<OriginConfig>;
}

function raceAbort<T>(p: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(new AbortError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new AbortError());
    signal.addEventListener('abort', onAbort, { once: true });
    p.then(
      (v) => {
        signal.removeEventListener('abort', onAbort);
        resolve(v);
      },
      (e: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(e);
      },
    );
  });
}

export class HostPool {
  readonly origin: string;
  readonly reader: TechnocoreReader;
  readonly limits: Limits;
  readonly governor: RateGovernor;
  readonly requests: WaiterSemaphore;
  readonly longPolls: WaiterSemaphore;
  readonly exports: WaiterSemaphore;
  readonly scheduler: SlotScheduler;
  readonly cache: MessageCache;
  readonly clock: Clock;
  readonly random: () => number;
  private readonly polls = new Map<string, RoomPoller>();
  private readonly stopping = new Set<Promise<void>>();
  private readonly ctrl = new AbortController();
  private readonly listeners = new Set<(e: WatchEvent) => void>();
  private readonly inflight = new Map<string, Promise<unknown>>();
  private configPromise: Promise<OriginConfig> | undefined;
  private readonly configOverride: Partial<OriginConfig> | undefined;
  originConfig: OriginConfig | undefined;
  private configAt = 0;
  private memberCount = 0;
  private disposed = false;

  constructor(o: HostPoolOptions) {
    this.clock = o.clock ?? systemClock;
    this.random = o.random ?? Math.random;
    this.limits = validateLimits({ ...DEFAULT_LIMITS, ...definedLimits(o.limits) });
    const readerOpts: { origin: string; transport?: Transport } = { origin: o.origin };
    if (o.transport) readerOpts.transport = o.transport;
    this.reader = new TechnocoreReader(readerOpts);
    this.origin = this.reader.origin;
    this.configOverride = o.originConfig;
    const initialRate = this.limits.readsPerMinute ?? DEFAULT_ORIGIN_CONFIG.readsPerMinute;
    this.governor = new RateGovernor({ perMinute: initialRate * this.limits.readBudgetFraction, clock: this.clock });
    this.requests = new WaiterSemaphore(this.limits.maxConcurrentRequests);
    this.longPolls = new WaiterSemaphore(Math.min(this.limits.maxConcurrentLongPolls, this.limits.maxConcurrentRequests));
    this.exports = new WaiterSemaphore(1);
    this.scheduler = new SlotScheduler({
      slots: this.longPolls.available,
      minTenureMs: this.limits.minSlotTenureMs,
      minSweepMs: this.limits.sweepMinIntervalMs,
      clock: this.clock,
    });
    this.scheduler.onChange((rooms) => {
      for (const room of rooms) this.polls.get(room)?.wake();
      this.emit({ type: 'slot-change', detail: { rooms, slotted: this.scheduler.slotted() } });
    });
    this.cache = new MessageCache(this.limits.cacheMessages, this.limits.maxSubscriptionsPerHost);
  }

  get signal(): AbortSignal {
    return this.ctrl.signal;
  }

  get isDisposed(): boolean {
    return this.disposed;
  }

  onEvent(fn: (e: WatchEvent) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  emit(e: Omit<WatchEvent, 'at'> & { at?: string }): void {
    const ev: WatchEvent = { ...e, at: e.at ?? new Date(this.clock.now()).toISOString() };
    for (const fn of this.listeners) {
      try {
        fn(ev);
      } catch {
        // listeners never break polling
      }
    }
  }

  /** Reads /config once and applies the budget and waiter caps. Never throws (defaults on failure). */
  ensureConfig(): Promise<OriginConfig> {
    // A /config that failed (origin down at startup) is retried after a minute rather than
    // pinning conservative defaults for the life of the process.
    if (this.configPromise && this.originConfig?.fallback && this.clock.now() - this.configAt > 60_000) {
      this.configPromise = undefined;
    }
    if (!this.configPromise) {
      this.configPromise = (async () => {
        let cfg: OriginConfig;
        if (this.configOverride && this.configOverride.readsPerMinute !== undefined) {
          cfg = { ...DEFAULT_ORIGIN_CONFIG, ...this.configOverride, fallback: false };
        } else {
          try {
            cfg = await this.reader.config(this.ctrl.signal);
          } catch {
            cfg = { ...DEFAULT_ORIGIN_CONFIG };
          }
          if (this.configOverride) cfg = { ...cfg, ...this.configOverride };
        }
        this.originConfig = cfg;
        this.configAt = this.clock.now();
        const rpm = this.limits.readsPerMinute ?? cfg.readsPerMinute;
        this.governor.configure(rpm * this.limits.readBudgetFraction);
        // Leave one of the origin's per-IP waiter slots for other clients on this host.
        const waiterCap = Math.max(1, cfg.waitersPerIp - 1);
        const slots = Math.max(1, Math.min(this.limits.maxConcurrentLongPolls, waiterCap, this.limits.maxConcurrentRequests));
        this.longPolls.setCapacity(slots);
        this.scheduler.setSlots(slots);
        return cfg;
      })();
    }
    return this.configPromise;
  }

  budgetPerMinute(): number {
    return this.governor.ratePerMinute;
  }

  waitSeconds(): number {
    const max = this.originConfig?.maxWaitS ?? 10;
    return Math.max(0.5, Math.min(this.limits.waitSeconds, max, 10));
  }

  memberTotal(): number {
    return this.memberCount;
  }

  attach(member: RoomMember): () => void {
    if (this.disposed) throw new Error('host pool disposed');
    if (this.memberCount >= this.limits.maxSubscriptionsPerHost) {
      throw new RangeError(`host pool subscription limit reached (${this.limits.maxSubscriptionsPerHost})`);
    }
    let poller = this.polls.get(member.room);
    if (!poller) {
      poller = new RoomPoller(this, member.room);
      this.polls.set(member.room, poller);
      this.scheduler.add(member.room);
      poller.members.add(member);
      poller.start();
    } else {
      poller.members.add(member);
      poller.wake();
    }
    this.memberCount++;
    let detached = false;
    return () => {
      if (detached) return;
      detached = true;
      this.memberCount--;
      const p = this.polls.get(member.room);
      if (!p) return;
      p.members.delete(member);
      if (p.members.size > 0) p.reconsiderPacing();
      if (p.members.size === 0) {
        this.polls.delete(member.room);
        this.scheduler.remove(member.room);
        this.cache.invalidate(member.room);
        const done = p.stop();
        this.stopping.add(done);
        void done.finally(() => {
          this.stopping.delete(done);
          // An observation that raced the stop may have cached the room again.
          if (!this.polls.has(member.room)) this.cache.invalidate(member.room);
        });
      }
    };
  }

  wake(room: string): void {
    this.polls.get(room)?.wake();
  }

  /**
   * A member's pending notice for `room` went away or moved earlier (flushed, dropped): a poller that is
   * pacing a flood re-checks its nap and re-polls now if the reason for the nap is gone. Unlike wake(),
   * this never skips any other sleep.
   */
  noticeChanged(room: string): void {
    this.polls.get(room)?.reconsiderPacing();
  }

  pollerMode(room: string): 'long-poll' | 'sweep' | 'idle' {
    const p = this.polls.get(room);
    if (!p) return 'idle';
    return this.scheduler.hasSlot(room) ? 'long-poll' : 'sweep';
  }

  /** Shared requests run under the pool's signal only; each caller races its own signal outside. */
  private dedupe<T>(key: string, fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    let p = this.inflight.get(key) as Promise<T> | undefined;
    if (!p) {
      p = fn().finally(() => this.inflight.delete(key));
      this.inflight.set(key, p);
    }
    return signal ? raceAbort(p, signal) : p;
  }

  /** A governed, non-waiting read. Identical concurrent reads share one request. */
  read(a: { room: string; since?: number; limit?: number; cacheBust?: boolean }, signal?: AbortSignal): Promise<ReadRoomResult> {
    const key = `read|${a.room}|${a.since ?? ''}|${a.limit ?? ''}|${a.cacheBust ? 1 : 0}`;
    return this.dedupe(key, async () => {
      await this.ensureConfig();
      const sig = this.ctrl.signal;
      await this.governor.acquire(sig);
      const release = await this.requests.acquire(sig);
      try {
        const args: Parameters<TechnocoreReader['readRoom']>[0] = { room: a.room, signal: sig };
        if (a.since !== undefined) args.since = a.since;
        if (a.limit !== undefined) args.limit = a.limit;
        if (a.cacheBust) args.cacheBust = true;
        const res = await this.reader.readRoom(args);
        if (!res.ok) this.noteRefusal(a.room, res.refusal);
        return res;
      } finally {
        release();
      }
    }, signal);
  }

  /**
   * The long-poll: one of at most maxConcurrentLongPolls per origin. `signal` must already
   * follow the pool (RoomPoller passes its own controller, which dispose() aborts), so no
   * composite signal is built per request.
   */
  async longPoll(room: string, since: number, signal: AbortSignal): Promise<ReadRoomResult> {
    await this.ensureConfig();
    const sig = this.ctrl.signal.aborted ? this.ctrl.signal : signal;
    const slot = await this.longPolls.acquire(sig);
    try {
      await this.governor.acquire(sig);
      const release = await this.requests.acquire(sig);
      try {
        const res = await this.reader.readRoom({ room, since, limit: 200, waitS: this.waitSeconds(), signal: sig });
        if (!res.ok) this.noteRefusal(room, res.refusal);
        return res;
      } finally {
        release();
      }
    } finally {
      slot();
    }
  }

  /** Bounded export scan; one export at a time per origin; identical scans share one request. */
  backfill(a: { room: string; afterSeq: number; beforeSeq: number }, signal?: AbortSignal): Promise<ExportScanResult> {
    const key = `export|${a.room}|${a.afterSeq}|${a.beforeSeq}`;
    return this.dedupe(key, async () => {
      await this.ensureConfig();
      const sig = this.ctrl.signal;
      const exportSlot = await this.exports.acquire(sig);
      try {
        await this.governor.acquire(sig);
        const release = await this.requests.acquire(sig);
        try {
          this.emit({ type: 'backfill', room: a.room, detail: { afterSeq: a.afterSeq, beforeSeq: a.beforeSeq } });
          const res = await this.reader.exportScan({
            room: a.room,
            afterSeq: a.afterSeq,
            beforeSeq: a.beforeSeq,
            maxRecords: this.limits.maxBackfillRecords,
            maxBytes: this.limits.maxExportBytes,
            signal: sig,
          });
          if (!res.ok) this.noteRefusal(a.room, res.refusal);
          else this.cache.add(a.room, res.generation, res.records);
          return res;
        } finally {
          release();
        }
      } finally {
        exportSlot();
      }
    }, signal);
  }

  private noteRefusal(room: string, refusal: Refusal): void {
    if (refusal.kind === 'rate') {
      this.governor.penalize(refusal.retryAfterS);
      this.emit({ type: 'rate-limited', room, detail: { retryAfterS: refusal.retryAfterS, bucket: refusal.bucket } });
    } else if (refusal.kind === 'server') {
      this.emit({ type: 'server-error', room, detail: { status: refusal.status } });
    } else {
      this.emit({ type: 'refused', room, detail: { kind: refusal.kind } });
    }
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.ctrl.abort();
    const loops = [...this.polls.values()].map((p) => p.stop());
    this.polls.clear();
    await Promise.allSettled([...loops, ...this.stopping]);
    await Promise.allSettled([...this.inflight.values()]);
    this.cache.clear();
    await this.reader.close();
    this.listeners.clear();
  }

  /** Test hook: the transport-level abort signal is shared by everything in the pool. */
  get activeRooms(): string[] {
    return [...this.polls.keys()];
  }
}

/** True when the view names a generation that some member has not settled yet. */
function generationUnsettled(members: readonly RoomMember[], generation: number | undefined): boolean {
  if (generation === undefined) return false;
  return members.some((m) => m.generation() !== generation);
}

/**
 * A paced nap never lets more than this many messages pile up on the origin at the rate the last
 * response showed: half of the 200-message read page, so the paced re-poll still gets everything in
 * one page instead of a tail window that needs an export backfill.
 */
const PACED_PAGE_TARGET = 100;

export class RoomPoller {
  readonly room: string;
  readonly members = new Set<RoomMember>();
  private readonly pool: HostPool;
  private readonly ctrl = new AbortController();
  private wakeCtrl: AbortController | undefined;
  private woken = false;
  /** Set while a flood-paced nap runs: aborting `cut` ends it; `until` is when it would end. */
  private pacing: { cut: AbortController; until: number } | undefined;
  /** When the last successful read came back: messages above its cursor have accrued since then. */
  private lastResponseAt: number | undefined;
  /** Whether that read carried messages, i.e. whether the time since it measures a flow, not idling. */
  private lastResponseHadMessages = false;
  private loop: Promise<void> | undefined;
  private readonly backoff: DecorrelatedJitter;
  private nextSweepAt = 0;

  constructor(pool: HostPool, room: string) {
    this.pool = pool;
    this.room = room;
    this.backoff = new DecorrelatedJitter({
      baseMs: pool.limits.backoffBaseMs,
      capMs: pool.limits.backoffCapMs,
      random: pool.random,
    });
  }

  start(): void {
    this.loop = this.run().catch((e) => {
      if (!isAbortError(e)) this.pool.emit({ type: 'error', room: this.room, detail: { message: String((e as Error)?.message ?? e) } });
    });
  }

  stop(): Promise<void> {
    this.ctrl.abort();
    return this.loop ?? Promise.resolve();
  }

  wake(): void {
    this.woken = true;
    this.wakeCtrl?.abort();
  }

  private get signal(): AbortSignal {
    return this.ctrl.signal; // HostPool.dispose() stops (aborts) every poller
  }

  /** Sleep that a wake() (or `cut`) interrupts; only a stop/dispose rejects. */
  private async nap(ms: number, cut?: AbortSignal): Promise<void> {
    if (this.woken) {
      this.woken = false;
      return;
    }
    const w = new AbortController();
    this.wakeCtrl = w;
    const signals = [this.ctrl.signal, this.pool.signal, w.signal];
    if (cut) signals.push(cut);
    try {
      await sleep(ms, signals, this.pool.clock);
    } catch (e) {
      if (this.ctrl.signal.aborted || this.pool.signal.aborted) throw e;
    } finally {
      this.wakeCtrl = undefined;
      this.woken = false;
    }
  }

  /**
   * The earliest pending-notice due time over the members this poller serves, or undefined when any of
   * them has no pending notice (or is still resolving its start, which the loop has to retry).
   * Paused, errored or disposed members take no observations, so they neither allow nor block pacing.
   */
  private earliestNoticeAt(): number | undefined {
    let earliest: number | undefined;
    for (const m of this.members) {
      if (m.head() === undefined) return undefined;
      if (!m.active()) continue;
      const at = m.nextNoticeAt();
      if (at === undefined || !Number.isFinite(at)) return undefined;
      earliest = earliest === undefined ? at : Math.min(earliest, at);
    }
    return earliest;
  }

  /**
   * How long to pace the next re-poll, or undefined to re-poll now. The nap aims `repollLeadMs` before
   * the earliest pending notice is due, at least `floodRepollMinMs` and at most the long-poll wait.
   *
   * Rounding a short nap up to the floor lets the poll land after that due time. That is harmless when
   * the due time is `fixed` (maxDelayMs or minWakeIntervalMs holds it where it is whatever arrives): the
   * flush cuts the nap and the room is read right after it. When the last read moved the due time
   * later, it is being held open by arriving activity (quietMs), and a poll that lands after it would
   * let the notice go out early with part of a burst, so there is no nap at all then.
   */
  private pacedNapMs(fixed: boolean): { ms: number; noticeAt: number } | undefined {
    const l = this.pool.limits;
    if (!l.floodRepollPacing) return undefined;
    const noticeAt = this.earliestNoticeAt();
    if (noticeAt === undefined) return undefined;
    const target = noticeAt - l.repollLeadMs - this.pool.clock.now();
    if (target < l.floodRepollMinMs && !fixed) return undefined;
    const ms = Math.min(this.pool.waitSeconds() * 1000, Math.max(l.floodRepollMinMs, target));
    return { ms, noticeAt };
  }

  /** Called when members or their pending notices change: cut a paced nap whose reason is gone. */
  reconsiderPacing(): void {
    const p = this.pacing;
    if (!p) return;
    const next = this.pacedNapMs(true);
    // Pending notices only move later while they stay pending, so only a notice that went away (or a
    // member set whose earliest notice is sooner than the nap) needs the poll now.
    if (!next || this.pool.clock.now() + next.ms < p.until) p.cut.abort();
  }

  /**
   * After a poll that returned messages: nap while every member's pending notice absorbs them.
   * `accrued` messages above the cursor piled up over the `accrualMs` before the response came back at
   * `respondedAt` (accrualMs is undefined when that span includes idle time, e.g. the first messages
   * after a quiet long-poll). The nap stays short enough that the next read still fits in one page at
   * that rate, and is skipped when even the shortest nap would not. Without a rate yet, the first paced
   * nap is the shortest one, which measures it. `dueBefore` is the earliest pending notice's due time
   * before this observation: unchanged means it is fixed (see pacedNapMs).
   */
  private async paceFlood(accrued: number, accrualMs: number | undefined, respondedAt: number, dueBefore: number | undefined): Promise<void> {
    const dueAfter = this.earliestNoticeAt();
    const next = this.pacedNapMs(dueBefore !== undefined && dueAfter === dueBefore);
    if (!next) return;
    const l = this.pool.limits;
    let ms: number;
    if (accrualMs === undefined) {
      // No flow measured yet. Messages that woke a long-poll arrived within about one origin
      // wait_poll, so that is the slowest rate they can mean: skip the nap (and measure on the next
      // read) when even that rate would overflow the page target during the shortest nap.
      const wakeSpanMs = Math.max(1, (this.pool.originConfig?.waitPollS ?? 0.5) * 1000);
      if (accrued * l.floodRepollMinMs > PACED_PAGE_TARGET * wakeSpanMs) return;
      ms = Math.min(next.ms, l.floodRepollMinMs);
    } else {
      // Messages also pile up while the observation is processed (an export backfill can take a while).
      const byRate = (PACED_PAGE_TARGET * Math.max(1, accrualMs)) / Math.max(1, accrued) - (this.pool.clock.now() - respondedAt);
      if (byRate < l.floodRepollMinMs) return;
      ms = Math.min(next.ms, Math.floor(byRate));
    }
    const cut = new AbortController();
    const pacing = { cut, until: this.pool.clock.now() + ms };
    this.pacing = pacing;
    this.pool.emit({ type: 'poll-paced', room: this.room, detail: { ms, noticeAt: new Date(next.noticeAt).toISOString() } });
    try {
      await this.nap(ms, cut.signal);
    } finally {
      if (this.pacing === pacing) this.pacing = undefined;
    }
  }

  private async readyMembers(): Promise<{ ready: RoomMember[]; pending: boolean }> {
    const ready: RoomMember[] = [];
    let pending = false;
    for (const m of [...this.members]) {
      if (m.head() === undefined) {
        let ok = false;
        try {
          ok = await m.prepare();
        } catch (e) {
          if (isAbortError(e) && (this.ctrl.signal.aborted || this.pool.signal.aborted)) throw e;
          ok = false;
        }
        if (!ok) {
          pending = true;
          continue;
        }
      }
      if (m.active() && m.head() !== undefined && this.members.has(m)) ready.push(m);
    }
    return { ready, pending };
  }

  private async run(): Promise<void> {
    const pool = this.pool;
    const room = this.room;
    while (!this.ctrl.signal.aborted && !pool.signal.aborted) {
      // This pass reads members, slots and heads afresh, so a wake from before it is already served;
      // only a wake that lands from here on may skip the next nap. (A stale wake, e.g. the slot
      // assignment on attach, used to skip the first paced or wait_held:false nap.)
      this.woken = false;
      const { ready, pending } = await this.readyMembers();
      if (ready.length === 0) {
        await this.nap(pending ? this.backoff.next() : 60_000);
        continue;
      }
      // The lowest head drives the shared poll: members ahead of it simply ignore seqs they
      // already hold (reconcile filters by their own head), so nobody needs a separate catch-up.
      const since = Math.min(...ready.map((m) => m.head()!));
      let result: ReadRoomResult;
      let transport: Observation['transport'];
      try {
        await pool.ensureConfig();
        if (pool.scheduler.hasSlot(room)) {
          transport = 'http-longpoll';
          pool.emit({ type: 'poll-start', room, detail: { since, mode: 'long-poll' } });
          result = await pool.longPoll(room, since, this.signal);
        } else {
          transport = 'http-sweep';
          const now = pool.clock.now();
          if (this.nextSweepAt > now) {
            await this.nap(this.nextSweepAt - now);
            continue;
          }
          this.nextSweepAt = now + jitter(pool.scheduler.sweepIntervalMs(pool.budgetPerMinute()), 0.2, pool.random);
          pool.emit({ type: 'poll-start', room, detail: { since, mode: 'sweep' } });
          result = await pool.read({ room, since, limit: 1 }, this.signal);
          if (result.ok) {
            const v = result.view;
            if (v.messages.length > 0 || generationUnsettled(ready, v.generation)) {
              if (v.messages.length > 0) pool.scheduler.noteActivity(room);
              result = await pool.read({ room, since, limit: 200, cacheBust: true }, this.signal);
            }
          }
        }
      } catch (e) {
        if (this.ctrl.signal.aborted || pool.signal.aborted) return;
        const type = e instanceof ProtocolError ? 'protocol-error' : 'network-error';
        pool.emit({ type, room, detail: { message: String((e as Error)?.message ?? e).slice(0, 200), kind: e instanceof TransportError ? e.kind : undefined } });
        await this.nap(this.backoff.next());
        continue;
      }
      pool.emit({ type: 'poll-end', room, detail: { since, mode: transport, ok: result.ok, status: result.status } });
      const respondedAt = pool.clock.now();
      const flowingSince = this.lastResponseHadMessages ? this.lastResponseAt : undefined;
      if (result.ok) {
        this.lastResponseAt = respondedAt;
        this.lastResponseHadMessages = result.view.messages.length > 0;
      }
      if (!result.ok) {
        const r = result.refusal;
        if (r.kind === 'rate') {
          const waitMs = Math.max(r.retryAfterS * 1000, this.backoff.next());
          await this.nap(waitMs + jitter(Math.min(1000, waitMs * 0.1), 1, pool.random));
          continue;
        }
        if (r.kind === 'bad-request' || r.kind === 'forbidden') {
          for (const m of this.members) m.onFatal(r);
          await this.nap(this.backoff.next());
          continue;
        }
        await this.nap(this.backoff.next());
        continue;
      }
      const view = result.view;
      // An empty view still matters when any member has not settled the generation it reports
      // (a recreation, a regression, a reset origin, or a member that joined or restarted with an
      // older stored generation). Compared per member, so a failed step is retried on the next
      // empty view instead of being remembered as handled.
      if (view.messages.length === 0 && !generationUnsettled(ready, view.generation)) {
        this.backoff.reset();
        if (transport === 'http-longpoll') {
          if (view.wait_held === false) {
            pool.emit({ type: 'wait-not-held', room, detail: { since } });
            await this.nap(jitter(pool.waitSeconds() * 1000, 0.2, pool.random));
          } else if (view.wait_held === undefined) {
            // The origin did not say it waited: never re-poll at wire speed.
            await this.nap(1000);
          }
        }
        continue;
      }
      if (view.messages.length) {
        pool.scheduler.noteActivity(room);
        pool.cache.add(room, view.generation, view.messages);
      }
      const obs: Observation = { room, since, view, fetchedAt: result.fetchedAt, transport };
      const dueBefore = this.earliestNoticeAt();
      const statuses = await Promise.all(
        ready
          .filter((m) => this.members.has(m) && m.active())
          .map((m) =>
            m.onObservation(obs).catch((e: unknown): 'retry' => {
              if (isAbortError(e) && (this.ctrl.signal.aborted || pool.signal.aborted)) throw e;
              pool.emit({ type: 'error', room, detail: { message: String((e as Error)?.message ?? e).slice(0, 200) } });
              return 'retry';
            }),
          ),
      );
      if (statuses.includes('retry')) {
        await this.nap(this.backoff.next());
      } else {
        this.backoff.reset();
        // Without pacing a flood costs one read per message the origin wakes on, all merged into
        // notices that are not due yet; see Limits.floodRepollPacing.
        if (view.messages.length > 0) {
          const accrued = Math.max(view.messages.length, view.last_seq - since);
          await this.paceFlood(accrued, flowingSince === undefined ? undefined : respondedAt - flowingSince, respondedAt, dueBefore);
        }
      }
    }
  }
}

export { AbortError };
