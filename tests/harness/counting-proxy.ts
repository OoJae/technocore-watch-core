// Counting, fault-injecting HTTP proxy placed between a watcher and the local server.
// Classifies every request, tracks concurrent long-polls per room, and can replay an old
// response, drop a long-poll mid-flight, or add latency.
import * as http from 'node:http';
import type { AddressInfo, Socket } from 'node:net';

export type ProxyClass = 'read-wait' | 'read' | 'export' | 'post' | 'config' | 'other';

export interface ProxyRecord {
  method: string;
  path: string;
  query: URLSearchParams;
  class: ProxyClass;
  room?: string;
  startedAt: number;
  endedAt?: number;
  status?: number;
  bytes: number;
  replayed?: boolean;
  dropped?: boolean;
  upstreamError?: string;
  clientAborted?: boolean;
}

export function classify(method: string, url: URL): { class: ProxyClass; room?: string } {
  if (method !== 'GET' && method !== 'HEAD') return { class: 'post' };
  if (url.pathname === '/config') return { class: 'config' };
  const exp = /^\/r\/([^/]+)\/export$/.exec(url.pathname);
  if (exp) return { class: 'export', room: exp[1]! };
  const read = /^\/r\/([^/]+)$/.exec(url.pathname);
  if (read) return { class: Number(url.searchParams.get('wait') ?? 0) > 0 ? 'read-wait' : 'read', room: read[1]! };
  return { class: 'other' };
}

export class CountingProxy {
  readonly records: ProxyRecord[] = [];
  target: string;
  latencyMs = 0;
  private server: http.Server;
  private readonly sockets = new Set<Socket>();
  private readonly lastReadBody = new Map<string, { status: number; headers: http.IncomingHttpHeaders; body: Buffer }>();
  private replayCount = 0;
  private dropCount = 0;
  private dropAfterMs = 300;
  private readonly inflightByRoom = new Map<string, number>();
  readonly maxInflightByRoom = new Map<string, number>();
  inflightLongPolls = 0;
  maxInflightLongPolls = 0;
  openSockets = 0;
  port = 0;

  constructor(target: string) {
    this.target = target;
    this.server = http.createServer((req, res) => void this.handle(req, res));
    this.server.keepAliveTimeout = 30_000;
    this.server.on('connection', (s) => {
      this.sockets.add(s);
      this.openSockets++;
      s.on('close', () => {
        this.sockets.delete(s);
        this.openSockets--;
      });
    });
  }

  get origin(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  async listen(): Promise<this> {
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', () => r()));
    this.port = (this.server.address() as AddressInfo).port;
    return this;
  }

  /** The next `n` room reads are answered with the last 200 body recorded for that room. */
  replayNext(n = 1): void {
    this.replayCount += n;
  }

  /** The next `n` long-polls have their client connection destroyed after `afterMs`. */
  dropNextLongPoll(n = 1, afterMs = 300): void {
    this.dropCount += n;
    this.dropAfterMs = afterMs;
  }

  count(cls: ProxyClass, since = 0): number {
    return this.records.filter((r) => r.class === cls && r.startedAt >= since).length;
  }

  counts(since = 0): Record<ProxyClass, number> {
    const c: Record<ProxyClass, number> = { 'read-wait': 0, read: 0, export: 0, post: 0, config: 0, other: 0 };
    for (const r of this.records) if (r.startedAt >= since) c[r.class]++;
    return c;
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://proxy.local');
    const { class: cls, room } = classify(req.method ?? 'GET', url);
    const rec: ProxyRecord = { method: req.method ?? 'GET', path: url.pathname, query: url.searchParams, class: cls, startedAt: Date.now(), bytes: 0 };
    if (room) rec.room = room;
    this.records.push(rec);
    if (this.latencyMs) await new Promise((r) => setTimeout(r, this.latencyMs));
    let clientGone = false;
    res.on('close', () => {
      if (!res.writableEnded) {
        clientGone = true;
        rec.clientAborted = true;
      }
    });

    if ((cls === 'read' || cls === 'read-wait') && room && this.replayCount > 0) {
      const last = this.lastReadBody.get(room);
      if (last) {
        this.replayCount--;
        rec.replayed = true;
        rec.status = last.status;
        rec.bytes = last.body.length;
        res.writeHead(last.status, { 'content-type': String(last.headers['content-type'] ?? 'application/json'), 'cache-control': 'no-store' });
        res.end(last.body);
        rec.endedAt = Date.now();
        return;
      }
    }

    if (cls === 'read-wait' && room) {
      this.inflightLongPolls++;
      this.maxInflightLongPolls = Math.max(this.maxInflightLongPolls, this.inflightLongPolls);
      const n = (this.inflightByRoom.get(room) ?? 0) + 1;
      this.inflightByRoom.set(room, n);
      this.maxInflightByRoom.set(room, Math.max(this.maxInflightByRoom.get(room) ?? 0, n));
    }
    const done = () => {
      if (cls === 'read-wait' && room && rec.endedAt === undefined) {
        this.inflightLongPolls--;
        this.inflightByRoom.set(room, (this.inflightByRoom.get(room) ?? 1) - 1);
      }
      rec.endedAt ??= Date.now();
    };

    const upstream = http.request(
      new URL(url.pathname + url.search, this.target),
      { method: req.method, headers: { ...req.headers, host: new URL(this.target).host }, agent: false },
      (up) => {
        rec.status = up.statusCode ?? 0;
        const chunks: Buffer[] = [];
        res.writeHead(up.statusCode ?? 502, up.headers);
        up.on('data', (c: Buffer) => {
          rec.bytes += c.length;
          if (cls === 'read' || cls === 'read-wait') chunks.push(c);
          res.write(c);
        });
        up.on('end', () => {
          if ((cls === 'read' || cls === 'read-wait') && room && up.statusCode === 200) {
            this.lastReadBody.set(room, { status: 200, headers: up.headers, body: Buffer.concat(chunks) });
          }
          res.end();
          done();
        });
        up.on('error', () => {
          res.destroy();
          done();
        });
      },
    );
    upstream.on('error', (e) => {
      rec.upstreamError = e.message;
      // Mirror a dead origin: the client sees its connection reset, not a synthetic 502.
      res.destroy();
      req.socket.destroy();
      done();
    });
    res.on('close', () => {
      if (clientGone) upstream.destroy();
      done();
    });
    if (cls === 'read-wait' && this.dropCount > 0) {
      this.dropCount--;
      rec.dropped = true;
      setTimeout(() => {
        upstream.destroy();
        req.socket.destroy();
      }, this.dropAfterMs);
    }
    req.pipe(upstream);
  }

  async close(): Promise<void> {
    for (const s of this.sockets) s.destroy();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

export async function startProxy(target: string): Promise<CountingProxy> {
  return new CountingProxy(target).listen();
}
