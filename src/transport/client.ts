// TechnocoreReader — read-only by construction. It builds exactly three kinds of URL
// (room read, room export, /config) and has no method that could post, sign or set.
import { assertRoomName } from '../protocol/names.ts';
import { ProtocolError, parseExportLine, parseReadView, parseRefusal, type Refusal } from '../protocol/parse.ts';
import type { ExportScan, Message, ReadView } from '../protocol/types.ts';
import { NodeHttpTransport, type Transport } from './http.ts';

export const USER_AGENT = 'technocore-watch-core/0.1.0 (unofficial community integration)';

export interface ReadRoomArgs {
  room: string;
  since?: number;
  limit?: number;
  waitS?: number;
  signal: AbortSignal;
  /** Adds a unique query parameter so an edge cache cannot answer a reconcile read. */
  cacheBust?: boolean;
}

export type ReadRoomResult =
  | { ok: true; view: ReadView; status: number; fetchedAt: string; url: string }
  | { ok: false; refusal: Refusal; status: number; fetchedAt: string; url: string };

export interface ExportScanArgs {
  room: string;
  afterSeq: number;
  beforeSeq: number;
  maxRecords: number;
  maxBytes: number;
  signal: AbortSignal;
}

export type ExportScanResult = ({ ok: true } & ExportScan) | { ok: false; refusal: Refusal; status: number };

export interface OriginConfig {
  maxWaitS: number;
  readsPerMinute: number;
  waitersPerIp: number;
  waitPollS?: number;
  edgeCacheSeconds?: number;
  version?: string;
  /** True when /config could not be read and defaults were used. */
  fallback: boolean;
}

export const DEFAULT_ORIGIN_CONFIG: OriginConfig = {
  maxWaitS: 10,
  readsPerMinute: 120,
  waitersPerIp: 4,
  fallback: true,
};

export function normalizeOrigin(origin: string): string {
  let u: URL;
  try {
    u = new URL(origin);
  } catch {
    throw new TypeError(`invalid origin URL: ${JSON.stringify(origin).slice(0, 120)}`);
  }
  if (u.protocol !== 'http:' && u.protocol !== 'https:') throw new TypeError('origin must be http(s)');
  if (u.username || u.password) throw new TypeError('origin must not carry credentials');
  if (u.search || u.hash) throw new TypeError('origin must not carry a query or fragment');
  return (u.origin + u.pathname).replace(/\/+$/, '');
}

let bustCounter = 0;

export class TechnocoreReader {
  readonly origin: string;
  readonly transport: Transport;
  private readonly userAgent: string;
  private readonly ownsTransport: boolean;

  constructor(o: { origin: string; transport?: Transport; userAgent?: string }) {
    this.origin = normalizeOrigin(o.origin);
    this.ownsTransport = !o.transport;
    this.transport = o.transport ?? new NodeHttpTransport();
    this.userAgent = o.userAgent ?? USER_AGENT;
  }

  roomUrl(room: string, q: { since?: number; limit?: number; waitS?: number; cacheBust?: boolean } = {}): string {
    assertRoomName(room);
    const p = new URLSearchParams();
    if (q.since !== undefined) p.set('since', String(Math.max(0, Math.floor(q.since))));
    if (q.limit !== undefined) p.set('limit', String(Math.min(200, Math.max(1, Math.floor(q.limit)))));
    if (q.waitS !== undefined && q.waitS > 0) p.set('wait', String(Math.min(10, q.waitS)));
    p.set('format', 'json');
    if (q.cacheBust) p.set('cb', `${Date.now().toString(36)}${(bustCounter++).toString(36)}`);
    return `${this.origin}/r/${room}?${p.toString()}`;
  }

  exportUrl(room: string): string {
    assertRoomName(room);
    return `${this.origin}/r/${room}/export`;
  }

  async readRoom(a: ReadRoomArgs): Promise<ReadRoomResult> {
    const q: { since?: number; limit?: number; waitS?: number; cacheBust?: boolean } = {};
    if (a.since !== undefined) q.since = a.since;
    if (a.limit !== undefined) q.limit = a.limit;
    if (a.waitS !== undefined) q.waitS = a.waitS;
    if (a.cacheBust) q.cacheBust = true;
    const url = this.roomUrl(a.room, q);
    const waitMs = (a.waitS ?? 0) * 1000;
    const res = await this.transport.get({
      url,
      headers: { 'user-agent': this.userAgent },
      signal: a.signal,
      timeoutMs: waitMs + 20_000,
    });
    const fetchedAt = new Date().toISOString();
    const body = await res.text(16 << 20);
    if (res.status !== 200) {
      return { ok: false, refusal: parseRefusal(res.status, body, res.headers), status: res.status, fetchedAt, url };
    }
    const view = parseReadView(body);
    if (view.room !== a.room) throw new ProtocolError('read view names a different room');
    return { ok: true, view, status: res.status, fetchedAt, url };
  }

  /**
   * Streams the NDJSON export and stops as soon as it has what it needs: a record with
   * seq >= beforeSeq, maxRecords records inside the range, or maxBytes read. The first
   * record of the export is reported as oldestRetainedSeq whatever the range.
   */
  async exportScan(a: ExportScanArgs): Promise<ExportScanResult> {
    const url = this.exportUrl(a.room);
    const res = await this.transport.get({
      url,
      headers: { 'user-agent': this.userAgent },
      signal: a.signal,
      timeoutMs: 120_000,
    });
    if (res.status !== 200) {
      const body = await res.text(64 << 10).catch(() => '');
      return { ok: false, refusal: parseRefusal(res.status, body, res.headers), status: res.status };
    }
    const genHeader = res.headers.get('x-room-generation');
    const records: Message[] = [];
    let oldest: number | undefined;
    let lastScanned: number | undefined;
    let bytes = 0;
    let bounded = false;
    let done = false;
    let buffered = '';
    const decoder = new TextDecoder('utf-8');
    const take = (line: string): void => {
      const rec = parseExportLine(line);
      if (!rec) return;
      if (oldest === undefined) oldest = rec.seq;
      lastScanned = rec.seq;
      if (rec.seq >= a.beforeSeq) {
        done = true;
        return;
      }
      if (rec.seq <= a.afterSeq) return;
      if (records.length >= a.maxRecords) {
        bounded = true;
        done = true;
        lastScanned = records[records.length - 1]!.seq;
        return;
      }
      records.push(rec);
    };
    try {
      for await (const chunk of res.body) {
        bytes += chunk.byteLength;
        buffered += decoder.decode(chunk, { stream: true });
        let nl: number;
        while (!done && (nl = buffered.indexOf('\n')) >= 0) {
          take(buffered.slice(0, nl));
          buffered = buffered.slice(nl + 1);
        }
        if (done) break;
        if (bytes >= a.maxBytes) {
          bounded = true;
          break;
        }
      }
      if (!done && !bounded) {
        buffered += decoder.decode();
        if (buffered.trim()) take(buffered);
      }
    } finally {
      res.cancel();
    }
    const out: ExportScanResult = { ok: true, records, bounded, afterSeq: a.afterSeq, beforeSeq: a.beforeSeq };
    if (genHeader && /^\d+$/.test(genHeader)) out.generation = Number(genHeader);
    if (oldest !== undefined) out.oldestRetainedSeq = oldest;
    if (lastScanned !== undefined) out.lastScannedSeq = lastScanned;
    return out;
  }

  /** GET /config (never rate limited by the origin). Falls back to conservative defaults. */
  async config(signal: AbortSignal): Promise<OriginConfig> {
    try {
      const res = await this.transport.get({
        url: `${this.origin}/config`,
        headers: { 'user-agent': this.userAgent },
        signal,
        timeoutMs: 15_000,
      });
      const body = await res.text(1 << 20);
      if (res.status !== 200) return { ...DEFAULT_ORIGIN_CONFIG };
      const doc = JSON.parse(body) as { version?: unknown; settings?: Record<string, unknown> };
      const s = doc.settings ?? {};
      const num = (v: unknown, d: number) => (typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : d);
      const cfg: OriginConfig = {
        maxWaitS: Math.min(10, num(s.max_wait, 10)),
        readsPerMinute: Math.max(1, num(s.rate_read, 120)),
        waitersPerIp: num(s.max_waiters_per_ip, 4),
        fallback: false,
      };
      if (typeof s.wait_poll === 'number') cfg.waitPollS = s.wait_poll;
      if (typeof s.edge_cache_seconds === 'number') cfg.edgeCacheSeconds = s.edge_cache_seconds;
      if (typeof doc.version === 'string') cfg.version = doc.version.slice(0, 40);
      return cfg;
    } catch (e) {
      if (signal.aborted) throw e;
      return { ...DEFAULT_ORIGIN_CONFIG };
    }
  }

  async close(): Promise<void> {
    if (this.ownsTransport) await this.transport.close?.();
  }
}
