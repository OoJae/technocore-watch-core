// In-memory model of technocore-chat v0.13.0 store semantics that matter to a reader:
// seq/floor/generation (store.append, _set_seq_entry, last_seq), tail reads that return the
// NEWEST window (read_messages), ring compaction, reaping with a floor, and hand deletion
// that loses the floor (seq regression). Used by the property tests and the fake transport.
import type { Message, ReadView } from '../../src/protocol/types.ts';

interface RoomModel {
  records: Message[];
  exists: boolean;
  generation: number;
  floor: number;
  /** `e-` expiry: records with seq <= this are unreadable (read and export), but still count for last_seq. */
  expiredThrough?: number;
}

export class ServerModel {
  readonly rooms = new Map<string, RoomModel>();
  private tsCounter = 0;
  private readonly listeners = new Set<(room: string) => void>();

  private room(name: string): RoomModel {
    let r = this.rooms.get(name);
    if (!r) {
      r = { records: [], exists: false, generation: 0, floor: 0 };
      this.rooms.set(name, r);
    }
    return r;
  }

  onPost(fn: (room: string) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  lastSeq(name: string): number {
    const r = this.room(name);
    if (r.exists) return r.records[r.records.length - 1]?.seq ?? 0;
    return r.floor;
  }

  generation(name: string): number {
    return this.room(name).generation;
  }

  post(name: string, from: string, text: string, extra: { nonce?: string; sig?: string } = {}): Message {
    const r = this.room(name);
    const created = !r.exists;
    const seq = this.lastSeq(name) + 1;
    const ts = `2026-09-13T00:00:${String(this.tsCounter++).padStart(6, '0')}Z`;
    const m: Message = { seq, ts, from, text, ...extra };
    r.records.push(m);
    r.exists = true;
    if (created) {
      r.generation += 1;
      r.floor = 0;
      delete r.expiredThrough;
    }
    for (const fn of this.listeners) fn(name);
    return m;
  }

  /** Ring compaction: keep only the newest `keep` records (at least one). */
  compact(name: string, keep: number): void {
    const r = this.room(name);
    const k = Math.max(1, keep);
    if (r.records.length > k) r.records = r.records.slice(-k);
  }

  /** Proper reap: the floor keeps the sequence going after recreation. */
  reap(name: string): void {
    const r = this.room(name);
    r.floor = this.lastSeq(name);
    r.records = [];
    r.exists = false;
  }

  /** File deleted by hand: floor lost, the next write restarts at seq 1 (regression). */
  deleteFile(name: string): void {
    const r = this.room(name);
    r.records = [];
    r.exists = false;
    r.floor = 0;
  }

  /** Every current record of an `e-` room expires: unreadable, but seq keeps advancing past them. */
  expireAll(name: string): void {
    const r = this.room(name);
    r.expiredThrough = this.lastSeq(name);
  }

  /** The origin's data root is replaced (a restore, or a dev server restarted on a fresh CHAT_ROOT). */
  resetAll(): void {
    this.rooms.clear();
  }

  private readable(r: RoomModel): Message[] {
    const through = r.expiredThrough ?? 0;
    return through ? r.records.filter((m) => m.seq > through) : r.records;
  }

  read(name: string, since: number | undefined, limit = 50): ReadView {
    const r = this.room(name);
    const lim = Math.max(1, Math.min(200, limit));
    let out = this.readable(r).filter((m) => since === undefined || m.seq > since);
    out = out.slice(-lim);
    return {
      room: name,
      count: out.length,
      first_seq: out[0]?.seq ?? null,
      last_seq: out.length ? out[out.length - 1]!.seq : (since ?? 0),
      generation: r.generation,
      messages: out.map((m) => ({ ...m })),
    };
  }

  exportRoom(name: string): { generation: number; records: Message[] } {
    const r = this.room(name);
    return { generation: r.generation, records: this.readable(r).map((m) => ({ ...m })) };
  }

  /** A deep snapshot, for replaying stale responses. */
  snapshot(): ServerModel {
    const s = new ServerModel();
    for (const [k, v] of this.rooms) s.rooms.set(k, { ...v, records: v.records.map((m) => ({ ...m })) });
    s.tsCounter = this.tsCounter;
    return s;
  }
}
