// Long-poll slot assignment. Up to `slots` rooms per origin hold a long-poll slot; the rest
// are swept (wait=0&limit=1) at an interval derived from the read budget. A swept room
// that shows activity is promoted, displacing the slot holder that has been quiet longest
// once that holder has kept its slot for minTenureMs.
import { type Clock, systemClock } from '../limits/clock.ts';

interface RoomSlotState {
  room: string;
  addedAt: number;
  lastActivityAt: number;
  slotSince?: number;
}

export class SlotScheduler {
  private slots: number;
  private readonly clock: Clock;
  private readonly minTenureMs: number;
  private readonly minSweepMs: number;
  private readonly rooms = new Map<string, RoomSlotState>();
  private readonly listeners = new Set<(changed: string[]) => void>();
  private seq = 0;

  constructor(o: { slots: number; minTenureMs?: number; minSweepMs?: number; clock?: Clock }) {
    this.slots = Math.max(1, Math.floor(o.slots));
    this.clock = o.clock ?? systemClock;
    this.minTenureMs = o.minTenureMs ?? 60_000;
    this.minSweepMs = o.minSweepMs ?? 15_000;
  }

  onChange(fn: (changed: string[]) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  get slotCount(): number {
    return this.slots;
  }

  setSlots(n: number): void {
    this.slots = Math.max(1, Math.floor(n));
    this.rebalance(true);
  }

  add(room: string): void {
    if (this.rooms.has(room)) return;
    // addedAt breaks ties in insertion order even when the clock does not move.
    this.rooms.set(room, { room, addedAt: this.clock.now() + this.seq++ / 1e6, lastActivityAt: Number.NEGATIVE_INFINITY });
    this.rebalance(false);
  }

  remove(room: string): void {
    if (!this.rooms.delete(room)) return;
    this.rebalance(true);
  }

  has(room: string): boolean {
    return this.rooms.has(room);
  }

  hasSlot(room: string): boolean {
    return this.rooms.get(room)?.slotSince !== undefined;
  }

  slotted(): string[] {
    return [...this.rooms.values()].filter((r) => r.slotSince !== undefined).map((r) => r.room);
  }

  overflowCount(): number {
    return Math.max(0, this.rooms.size - this.slotted().length);
  }

  noteActivity(room: string): void {
    const r = this.rooms.get(room);
    if (!r) return;
    r.lastActivityAt = this.clock.now();
    if (r.slotSince === undefined) this.rebalance(false);
  }

  /** max(minSweep, overflowRooms x 60000 / (budgetPerMin - slots x 6)). */
  sweepIntervalMs(budgetPerMin: number): number {
    const overflow = this.overflowCount();
    if (overflow === 0) return this.minSweepMs;
    const spare = Math.max(1, budgetPerMin - this.slotted().length * 6);
    return Math.max(this.minSweepMs, Math.ceil((overflow * 60_000) / spare));
  }

  private rebalance(freed: boolean): void {
    const now = this.clock.now();
    const changed: string[] = [];
    const all = [...this.rooms.values()];
    const holders = all.filter((r) => r.slotSince !== undefined);
    // Too many holders (slots shrank): demote the quietest.
    holders.sort((a, b) => a.lastActivityAt - b.lastActivityAt || b.addedAt - a.addedAt);
    while (holders.length > this.slots) {
      const h = holders.shift()!;
      delete h.slotSince;
      changed.push(h.room);
    }
    const candidates = all
      .filter((r) => r.slotSince === undefined)
      .sort((a, b) => b.lastActivityAt - a.lastActivityAt || a.addedAt - b.addedAt);
    for (const c of candidates) {
      if (holders.length < this.slots) {
        c.slotSince = now;
        holders.push(c);
        changed.push(c.room);
        continue;
      }
      if (freed || c.lastActivityAt === Number.NEGATIVE_INFINITY) continue;
      // Promote over the quietest holder that has served its tenure and is quieter.
      holders.sort((a, b) => a.lastActivityAt - b.lastActivityAt || b.addedAt - a.addedAt);
      const victim = holders.find((h) => now - (h.slotSince ?? now) >= this.minTenureMs && h.lastActivityAt < c.lastActivityAt);
      if (!victim) continue;
      delete victim.slotSince;
      holders.splice(holders.indexOf(victim), 1);
      c.slotSince = now;
      holders.push(c);
      changed.push(victim.room, c.room);
    }
    if (changed.length) for (const fn of this.listeners) fn(changed);
  }
}
