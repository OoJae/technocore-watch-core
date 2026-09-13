// Contiguous forward paging for readPage: the read view only returns the NEWEST window, so
// a page that starts below that window comes from a bounded export scan instead.
import type { Refusal } from '../protocol/parse.ts';
import type { Message } from '../protocol/types.ts';
import type { HostPool } from './host-pool.ts';

export type ForwardFetch =
  | {
      ok: true;
      generation?: number;
      /** Ascending records with afterSeq < seq <= endSeq. */
      records: Message[];
      /** When an export was used: its oldest retained seq. */
      oldestRetainedSeq?: number;
      /** A budget stopped the export; seqs above lastScannedSeq are unknown, not missing. */
      bounded: boolean;
      lastScannedSeq?: number;
    }
  | { ok: false; refusal: Refusal };

export async function fetchForward(
  pool: HostPool,
  room: string,
  afterSeq: number,
  endSeq: number,
  signal?: AbortSignal,
): Promise<ForwardFetch> {
  const res = await pool.read({ room, since: afterSeq, limit: 200, cacheBust: true }, signal);
  if (!res.ok) return { ok: false, refusal: res.refusal };
  const v = res.view;
  pool.cache.add(room, v.generation, v.messages);
  const first = v.messages[0];
  const base: { generation?: number } = {};
  if (v.generation !== undefined) base.generation = v.generation;
  if (!first) return { ok: true, ...base, records: [], bounded: false };
  if (first.seq === afterSeq + 1) {
    return { ok: true, ...base, records: v.messages.filter((m) => m.seq <= endSeq), bounded: false };
  }
  const beforeSeq = Math.min(endSeq + 1, first.seq);
  const bf = await pool.backfill({ room, afterSeq, beforeSeq }, signal);
  if (!bf.ok) return { ok: false, refusal: bf.refusal };
  if (bf.generation !== undefined && v.generation !== undefined && bf.generation !== v.generation) {
    // Recreated between the two requests: let the poller reconcile before paging further.
    return { ok: true, generation: bf.generation, records: [], bounded: true };
  }
  const records = bf.records.filter((m) => m.seq > afterSeq && m.seq < beforeSeq);
  if (!bf.bounded && beforeSeq === first.seq) {
    for (const m of v.messages) if (m.seq <= endSeq) records.push(m);
  }
  const out: ForwardFetch = { ok: true, ...base, records, bounded: bf.bounded };
  if (bf.oldestRetainedSeq !== undefined) out.oldestRetainedSeq = bf.oldestRetainedSeq;
  if (bf.lastScannedSeq !== undefined) out.lastScannedSeq = bf.lastScannedSeq;
  return out;
}
