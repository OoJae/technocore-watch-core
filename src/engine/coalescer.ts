// Coalescer: at most ONE pending notice per scope; new activity merges into it.
// Flush after quietMs without new activity or maxDelayMs after the first event, whichever
// comes first — but never sooner than minWakeIntervalMs after the previous notice.
// Empty polls never reach here, so they never produce a notice.
import { isDidKey } from '../protocol/names.ts';
import { previewText } from '../protocol/render.ts';
import type { GapRecord, Message, NoticeRoom } from '../protocol/types.ts';
import { type Clock, type TimerHandle, systemClock } from '../limits/clock.ts';
import { type CoalesceOptions, DEFAULT_COALESCE, type TransportKind } from './types.ts';

export interface RoomDelta {
  room: string;
  generation?: number;
  delivered: Message[];
  gaps: GapRecord[];
  /** (from, to] settled by this delta. */
  rangeFrom: number;
  rangeTo: number;
  /** For restart re-notification: count without message bodies. */
  countOverride?: number;
  recreated?: { from?: number; to: number };
  regression?: { fromHead: number; toLast: number };
  transport: TransportKind;
  fetchedAt: string;
}

interface PendingRoom {
  room: string;
  generation?: number;
  newCount: number;
  seqFrom: number;
  seqTo: number;
  gaps: GapRecord[];
  recreated?: { from?: number; to: number };
  regression?: { fromHead: number; toLast: number };
  signed: Set<string>;
  unsigned: Set<string>;
  preview: { from: string; text: string }[];
  lastAt: number;
}

export interface FlushBatch {
  rooms: NoticeRoom[];
  /** Rooms beyond maxRoomsPerNotice: counted in totalNew/moreRooms, listed for markNotified. */
  overflow: { room: string; generation?: number; seqTo: number }[];
  moreRooms: number;
  totalNew: number;
  transport: TransportKind;
  fetchedAt: string;
}

export interface CoalescerOptions extends Partial<CoalesceOptions> {
  maxRoomsPerNotice?: number;
  previewChars?: number;
  maxGapsPerRoom?: number;
  clock?: Clock;
}

const MAX_SENDERS = 256;
const MAX_PREVIEW_ITEMS = 3;

export class Coalescer {
  readonly opts: CoalesceOptions;
  private readonly clock: Clock;
  private readonly maxRooms: number;
  private readonly previewChars: number;
  private readonly maxGaps: number;
  private readonly onFlush: (batch: FlushBatch) => void;
  private pending = new Map<string, PendingRoom>();
  private firstAt = 0;
  private lastAt = 0;
  private lastFlushAt = Number.NEGATIVE_INFINITY;
  private timer: TimerHandle | undefined;
  private dueAt: number | undefined;
  private transport: TransportKind = 'http-longpoll';
  private fetchedAt = '';
  private disposed = false;
  flushes = 0;

  constructor(onFlush: (batch: FlushBatch) => void, o: CoalescerOptions = {}) {
    this.onFlush = onFlush;
    this.opts = {
      quietMs: o.quietMs ?? DEFAULT_COALESCE.quietMs,
      maxDelayMs: o.maxDelayMs ?? DEFAULT_COALESCE.maxDelayMs,
      minWakeIntervalMs: o.minWakeIntervalMs ?? DEFAULT_COALESCE.minWakeIntervalMs,
    };
    this.clock = o.clock ?? systemClock;
    this.maxRooms = o.maxRoomsPerNotice ?? 16;
    this.previewChars = Math.min(280, Math.max(0, o.previewChars ?? 0));
    this.maxGaps = o.maxGapsPerRoom ?? 8;
  }

  get hasPending(): boolean {
    return this.pending.size > 0;
  }

  get nextFlushAt(): number | undefined {
    return this.dueAt;
  }

  add(d: RoomDelta): void {
    if (this.disposed) return;
    const count = d.countOverride ?? d.delivered.length;
    if (count === 0 && d.gaps.length === 0 && !d.recreated && !d.regression) return;
    const now = this.clock.now();
    if (this.pending.size === 0) this.firstAt = now;
    this.lastAt = now;
    this.transport = d.transport;
    if (d.fetchedAt > this.fetchedAt) this.fetchedAt = d.fetchedAt;
    let p = this.pending.get(d.room);
    if (!p || d.regression) {
      // A regression renumbers the room, so earlier pending ranges no longer mean anything;
      // the notice reports the regression itself instead.
      const carried = p;
      p = {
        room: d.room,
        newCount: 0,
        seqFrom: d.rangeFrom + 1,
        seqTo: d.rangeTo,
        gaps: [],
        signed: new Set(),
        unsigned: new Set(),
        preview: [],
        lastAt: now,
      };
      if (carried?.recreated) p.recreated = { ...carried.recreated };
      this.pending.set(d.room, p);
    }
    if (d.generation !== undefined) p.generation = d.generation;
    p.newCount += count;
    p.seqFrom = Math.min(p.seqFrom, d.rangeFrom + 1);
    p.seqTo = Math.max(p.seqTo, d.rangeTo);
    p.lastAt = now;
    if (d.recreated) {
      p.recreated = p.recreated ? { ...(p.recreated.from !== undefined ? { from: p.recreated.from } : {}), to: d.recreated.to } : { ...d.recreated };
    }
    if (d.regression) p.regression = { ...d.regression };
    for (const g of d.gaps) p.gaps.push({ ...g });
    while (p.gaps.length > this.maxGaps) {
      const [a, b] = p.gaps;
      if (!a || !b) break;
      p.gaps.splice(0, 2, { ...a, from: Math.min(a.from, b.from), to: Math.max(a.to, b.to) });
    }
    for (const m of d.delivered) {
      const bucket = m.sig && isDidKey(m.from) ? p.signed : p.unsigned;
      if (bucket.size < MAX_SENDERS) bucket.add(m.from);
    }
    if (this.previewChars > 0 && d.delivered.length) {
      for (const m of d.delivered.slice(-MAX_PREVIEW_ITEMS)) {
        p.preview.push({ from: previewText(m.from, 80), text: previewText(m.text, this.previewChars) });
      }
      if (p.preview.length > MAX_PREVIEW_ITEMS) p.preview.splice(0, p.preview.length - MAX_PREVIEW_ITEMS);
    }
    this.schedule();
  }

  /** Drop a room's pending activity (unsubscribe). */
  remove(room: string): void {
    this.pending.delete(room);
    if (this.pending.size === 0) this.cancelTimer();
  }

  private computeDue(): number {
    const byActivity = Math.min(this.firstAt + this.opts.maxDelayMs, this.lastAt + this.opts.quietMs);
    return Math.max(this.lastFlushAt + this.opts.minWakeIntervalMs, byActivity);
  }

  private cancelTimer(): void {
    if (this.timer !== undefined) this.clock.clearTimeout(this.timer);
    this.timer = undefined;
    this.dueAt = undefined;
  }

  private schedule(): void {
    const due = this.computeDue();
    if (this.timer !== undefined && this.dueAt === due) return;
    this.cancelTimer();
    this.dueAt = due;
    this.timer = this.clock.setTimeout(() => {
      this.timer = undefined;
      this.dueAt = undefined;
      const now = this.clock.now();
      const again = this.computeDue();
      if (again > now + 1) {
        this.schedule();
        return;
      }
      this.flushNow();
    }, Math.max(0, due - this.clock.now()));
  }

  /** Emit the pending notice immediately (ignores timing). No-op when nothing is pending. */
  flushNow(): void {
    if (this.pending.size === 0 || this.disposed) return;
    this.cancelTimer();
    const all = [...this.pending.values()].sort((a, b) => b.lastAt - a.lastAt || a.room.localeCompare(b.room));
    this.pending = new Map();
    this.lastFlushAt = this.clock.now();
    this.flushes++;
    const rooms: NoticeRoom[] = all.slice(0, this.maxRooms).map((p) => {
      const r: NoticeRoom = {
        room: p.room,
        newCount: p.newCount,
        seqFrom: p.seqFrom,
        seqTo: p.seqTo,
        gaps: p.gaps,
        signedSenders: p.signed.size,
        unsignedSenders: p.unsigned.size,
      };
      if (p.generation !== undefined) r.generation = p.generation;
      if (p.recreated) r.recreated = p.recreated;
      if (p.regression) r.regression = p.regression;
      if (p.preview.length) r.preview = p.preview;
      return r;
    });
    const totalNew = all.reduce((n, p) => n + p.newCount, 0);
    const overflow = all.slice(this.maxRooms).map((p) =>
      p.generation === undefined ? { room: p.room, seqTo: p.seqTo } : { room: p.room, generation: p.generation, seqTo: p.seqTo },
    );
    this.onFlush({
      rooms,
      overflow,
      moreRooms: Math.max(0, all.length - this.maxRooms),
      totalNew,
      transport: this.transport,
      fetchedAt: this.fetchedAt || new Date(this.clock.now()).toISOString(),
    });
  }

  dispose(): void {
    this.disposed = true;
    this.cancelTimer();
    this.pending.clear();
  }
}
