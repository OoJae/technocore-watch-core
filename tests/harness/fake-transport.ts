// A Transport backed by ServerModel: long-poll waits (fake-timer friendly), waiter caps with
// wait_held:false, a per-minute read bucket producing the real 429 body, and exports.
import type { Transport, TransportRequest, TransportResponse } from '../../src/transport/http.ts';
import { AbortError } from '../../src/limits/clock.ts';
import { ServerModel } from './model.ts';

export interface FakeRequestRecord {
  url: string;
  path: string;
  kind: 'read-wait' | 'read' | 'export' | 'config' | 'other';
  room?: string;
  startedAt: number;
  endedAt?: number;
  status?: number;
  waitHeld?: boolean;
  aborted?: boolean;
}

export interface FakeOriginOptions {
  rateRead?: number;
  maxWait?: number;
  maxWaitersPerIp?: number;
  waitPoll?: number;
}

function respond(status: number, body: string, headers: Record<string, string> = {}): TransportResponse {
  const buf = Buffer.from(body, 'utf8');
  const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), v]));
  let cancelled = false;
  const iterable: AsyncIterable<Uint8Array> = {
    [Symbol.asyncIterator]: async function* () {
      for (let i = 0; i < buf.length && !cancelled; i += 4096) yield buf.subarray(i, i + 4096);
    },
  };
  return {
    status,
    headers: { get: (n: string) => lower[n.toLowerCase()] ?? null },
    body: iterable,
    text: async () => body,
    cancel: () => {
      cancelled = true;
    },
  };
}

export class FakeOrigin implements Transport {
  readonly model: ServerModel;
  readonly requests: FakeRequestRecord[] = [];
  readonly opts: Required<FakeOriginOptions>;
  private tokens: number;
  private lastRefill = Date.now();
  private waiters = 0;
  inflightWaits = 0;
  maxInflightWaits = 0;
  readonly maxInflightPerRoom = new Map<string, number>();
  private readonly inflightPerRoom = new Map<string, number>();
  closed = false;
  /** Optional hook to replace a response (fault injection). */
  intercept?: (rec: FakeRequestRecord) => TransportResponse | undefined;

  constructor(model = new ServerModel(), o: FakeOriginOptions = {}) {
    this.model = model;
    this.opts = { rateRead: o.rateRead ?? 1_000_000, maxWait: o.maxWait ?? 10, maxWaitersPerIp: o.maxWaitersPerIp ?? 4, waitPoll: o.waitPoll ?? 0.05 };
    this.tokens = this.opts.rateRead;
  }

  counts(): Record<FakeRequestRecord['kind'], number> {
    const c = { 'read-wait': 0, read: 0, export: 0, config: 0, other: 0 };
    for (const r of this.requests) c[r.kind]++;
    return c;
  }

  private take(): number {
    const now = Date.now();
    const per = this.opts.rateRead;
    this.tokens = Math.min(per, this.tokens + ((now - this.lastRefill) * per) / 60_000);
    this.lastRefill = now;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return 0;
    }
    return ((1 - this.tokens) * 60) / per;
  }

  private limited(retry: number): TransportResponse {
    const wait = Math.max(1, Math.round(retry));
    const body = `429 rate limited: the read budget for your IP (${this.opts.rateRead}/min) is spent.\nretry after: ${wait}s — the bucket refills continuously.\n`;
    return respond(429, body, { 'retry-after': String(wait) });
  }

  async get(req: TransportRequest): Promise<TransportResponse> {
    if (this.closed) throw new Error('closed');
    if (req.signal.aborted) throw new AbortError();
    const u = new URL(req.url);
    const rec: FakeRequestRecord = { url: req.url, path: u.pathname, kind: 'other', startedAt: Date.now() };
    this.requests.push(rec);
    const finish = (r: TransportResponse) => {
      rec.status = r.status;
      rec.endedAt = Date.now();
      return r;
    };
    if (u.pathname === '/config') {
      rec.kind = 'config';
      return finish(
        respond(200, JSON.stringify({ version: '0.13.0', settings: { rate_read: this.opts.rateRead, max_wait: this.opts.maxWait, max_waiters_per_ip: this.opts.maxWaitersPerIp, wait_poll: this.opts.waitPoll } })),
      );
    }
    const exp = /^\/r\/([^/]+)\/export$/.exec(u.pathname);
    const read = /^\/r\/([^/]+)$/.exec(u.pathname);
    const room = exp?.[1] ?? read?.[1];
    if (room) rec.room = room;
    const injected = this.intercept?.(rec);
    if (exp) {
      rec.kind = 'export';
      if (injected) return finish(injected);
      const retry = this.take();
      if (retry) return finish(this.limited(retry));
      const e = this.model.exportRoom(exp[1]!);
      return finish(respond(200, e.records.map((m) => JSON.stringify(m)).join('\n') + (e.records.length ? '\n' : ''), { 'x-room-generation': String(e.generation) }));
    }
    if (read) {
      const since = u.searchParams.has('since') ? Number(u.searchParams.get('since')) : undefined;
      const limit = u.searchParams.has('limit') ? Number(u.searchParams.get('limit')) : 50;
      const wait = Math.min(this.opts.maxWait, Number(u.searchParams.get('wait') ?? '0') || 0);
      rec.kind = wait > 0 ? 'read-wait' : 'read';
      if (injected) return finish(injected);
      const retry = this.take();
      if (retry) return finish(this.limited(retry));
      let view = this.model.read(read[1]!, since, limit);
      if (wait > 0 && since !== undefined && view.messages.length === 0) {
        if (this.waiters >= this.opts.maxWaitersPerIp) {
          rec.waitHeld = false;
          return finish(respond(200, JSON.stringify({ ...view, wait_held: false })));
        }
        this.waiters++;
        this.inflightWaits++;
        this.maxInflightWaits = Math.max(this.maxInflightWaits, this.inflightWaits);
        const n = (this.inflightPerRoom.get(read[1]!) ?? 0) + 1;
        this.inflightPerRoom.set(read[1]!, n);
        this.maxInflightPerRoom.set(read[1]!, Math.max(this.maxInflightPerRoom.get(read[1]!) ?? 0, n));
        try {
          view = await new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
              off();
              req.signal.removeEventListener('abort', onAbort);
              resolve({ ...this.model.read(read[1]!, since, limit), wait_held: true });
            }, wait * 1000);
            const off = this.model.onPost((r) => {
              if (r !== read[1]) return;
              // The real server notices within wait_poll; resolve on the next tick.
              setTimeout(() => {
                const v = this.model.read(read[1]!, since, limit);
                if (!v.messages.length) return;
                clearTimeout(timer);
                off();
                req.signal.removeEventListener('abort', onAbort);
                resolve(v);
              }, this.opts.waitPoll * 1000);
            });
            const onAbort = () => {
              clearTimeout(timer);
              off();
              rec.aborted = true;
              reject(new AbortError());
            };
            req.signal.addEventListener('abort', onAbort, { once: true });
          });
        } finally {
          this.waiters--;
          this.inflightWaits--;
          this.inflightPerRoom.set(read[1]!, (this.inflightPerRoom.get(read[1]!) ?? 1) - 1);
        }
        if (view.wait_held) rec.waitHeld = true;
      }
      return finish(respond(200, JSON.stringify(view)));
    }
    return finish(respond(404, '404 not found'));
  }

  close(): void {
    this.closed = true;
  }
}
