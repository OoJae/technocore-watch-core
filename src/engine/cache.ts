// Bounded per-room message cache shared by every scope in a host pool, so readPage can
// usually page what the upstream poll already fetched without another origin request.
import type { Message } from '../protocol/types.ts';

interface RoomEntry {
  generation: number | undefined;
  bySeq: Map<number, Message>;
}

export class MessageCache {
  private readonly rooms = new Map<string, RoomEntry>();
  private readonly maxPerRoom: number;

  constructor(maxPerRoom: number) {
    this.maxPerRoom = Math.max(0, maxPerRoom);
  }

  add(room: string, generation: number | undefined, messages: readonly Message[]): void {
    if (this.maxPerRoom === 0 || messages.length === 0) return;
    let e = this.rooms.get(room);
    if (!e || (generation !== undefined && e.generation !== undefined && e.generation !== generation)) {
      e = { generation, bySeq: new Map() };
      this.rooms.set(room, e);
    }
    if (generation !== undefined) e.generation = generation;
    for (const m of messages) e.bySeq.set(m.seq, m);
    if (e.bySeq.size > this.maxPerRoom) {
      const seqs = [...e.bySeq.keys()].sort((a, b) => a - b);
      for (const s of seqs.slice(0, e.bySeq.size - this.maxPerRoom)) e.bySeq.delete(s);
    }
  }

  get(room: string, generation: number | undefined, seq: number): Message | undefined {
    const e = this.rooms.get(room);
    if (!e) return undefined;
    if (generation !== undefined && e.generation !== undefined && generation !== e.generation) return undefined;
    return e.bySeq.get(seq);
  }

  invalidate(room: string): void {
    this.rooms.delete(room);
  }

  clear(): void {
    this.rooms.clear();
  }
}
