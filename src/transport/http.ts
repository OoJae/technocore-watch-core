// Transports are GET-only by construction: the interface has no method, body or verb
// parameter, so nothing built on it can write to an origin.
import * as http from 'node:http';
import * as https from 'node:https';
import { AbortError } from '../limits/clock.ts';
import type { HeaderLike } from '../protocol/parse.ts';

export interface TransportRequest {
  url: string;
  headers?: Record<string, string>;
  signal: AbortSignal;
  /** Whole-request deadline including body, in ms. */
  timeoutMs?: number;
}

export interface TransportResponse {
  status: number;
  headers: HeaderLike;
  /** The body stream; consume once. */
  body: AsyncIterable<Uint8Array>;
  /** Read the whole body as UTF-8, failing past maxBytes. */
  text(maxBytes?: number): Promise<string>;
  /** Stop reading and close the underlying connection. */
  cancel(): void;
}

export interface Transport {
  get(req: TransportRequest): Promise<TransportResponse>;
  close?(): void | Promise<void>;
}

export class TransportError extends Error {
  override readonly name = 'TransportError';
  readonly kind: 'network' | 'timeout' | 'too-large';
  constructor(kind: 'network' | 'timeout' | 'too-large', message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.kind = kind;
  }
}

export const DEFAULT_MAX_TEXT_BYTES = 16 << 20;

async function collect(body: AsyncIterable<Uint8Array>, maxBytes: number, cancel: () => void): Promise<string> {
  const chunks: Uint8Array[] = [];
  let size = 0;
  for await (const chunk of body) {
    size += chunk.byteLength;
    if (size > maxBytes) {
      cancel();
      throw new TransportError('too-large', `response body exceeded ${maxBytes} bytes`);
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

class MapHeaders implements HeaderLike {
  private readonly h: http.IncomingHttpHeaders;
  constructor(h: http.IncomingHttpHeaders) {
    this.h = h;
  }
  get(name: string): string | null {
    const v = this.h[name.toLowerCase()];
    if (v === undefined) return null;
    return Array.isArray(v) ? v.join(', ') : String(v);
  }
}

/**
 * node:http(s) transport with a private keep-alive agent. `close()` destroys every socket
 * the agent holds, which is what lets a disposed watcher leave no handles behind.
 */
export class NodeHttpTransport implements Transport {
  private readonly httpAgent: http.Agent;
  private readonly httpsAgent: https.Agent;
  private readonly inflight = new Set<http.ClientRequest>();
  private closed = false;

  constructor(o: { maxSockets?: number; keepAlive?: boolean } = {}) {
    const opts = { keepAlive: o.keepAlive ?? true, maxSockets: o.maxSockets ?? 8, keepAliveMsecs: 1000 };
    this.httpAgent = new http.Agent(opts);
    this.httpsAgent = new https.Agent(opts);
  }

  get(req: TransportRequest): Promise<TransportResponse> {
    if (this.closed) return Promise.reject(new TransportError('network', 'transport closed'));
    if (req.signal.aborted) return Promise.reject(new AbortError());
    const url = new URL(req.url);
    const mod = url.protocol === 'https:' ? https : url.protocol === 'http:' ? http : null;
    if (!mod) return Promise.reject(new TypeError(`unsupported protocol ${url.protocol}`));
    return new Promise<TransportResponse>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const r = mod.request(url, {
        method: 'GET',
        agent: url.protocol === 'https:' ? this.httpsAgent : this.httpAgent,
        headers: { accept: 'application/json, application/x-ndjson;q=0.9, text/plain;q=0.5', ...req.headers },
      });
      this.inflight.add(r);
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        timer = undefined;
        req.signal.removeEventListener('abort', onAbort);
        this.inflight.delete(r);
      };
      const fail = (e: unknown) => {
        cleanup();
        if (!settled) {
          settled = true;
          reject(e);
        }
      };
      const onAbort = () => {
        r.destroy(new AbortError());
        fail(new AbortError());
      };
      req.signal.addEventListener('abort', onAbort, { once: true });
      if (req.timeoutMs && req.timeoutMs > 0) {
        timer = setTimeout(() => {
          const e = new TransportError('timeout', `request timed out after ${req.timeoutMs}ms`);
          r.destroy(e);
          fail(e);
        }, req.timeoutMs);
      }
      r.on('error', (e) => {
        if (e instanceof AbortError || e instanceof TransportError) fail(e);
        else fail(new TransportError('network', (e as Error).message, { cause: e }));
      });
      r.on('response', (res) => {
        settled = true;
        const cancel = () => {
          res.destroy();
          r.destroy();
          cleanup();
        };
        res.on('end', cleanup);
        res.on('close', cleanup);
        res.on('error', () => cleanup());
        const body: AsyncIterable<Uint8Array> = {
          [Symbol.asyncIterator]: async function* () {
            try {
              for await (const chunk of res) yield chunk as Uint8Array;
            } catch (e) {
              if (req.signal.aborted) throw new AbortError();
              if (e instanceof TransportError) throw e;
              throw new TransportError('network', (e as Error).message, { cause: e });
            }
          },
        };
        resolve({
          status: res.statusCode ?? 0,
          headers: new MapHeaders(res.headers),
          body,
          text: (maxBytes = DEFAULT_MAX_TEXT_BYTES) => collect(body, maxBytes, cancel),
          cancel,
        });
      });
      r.end();
    });
  }

  close(): void {
    this.closed = true;
    for (const r of this.inflight) r.destroy(new AbortError('transport closed'));
    this.inflight.clear();
    this.httpAgent.destroy();
    this.httpsAgent.destroy();
  }
}

/** Global-fetch transport, for runtimes without node:http. */
export class FetchTransport implements Transport {
  async get(req: TransportRequest): Promise<TransportResponse> {
    const ctrl = new AbortController();
    const onAbort = () => ctrl.abort();
    req.signal.addEventListener('abort', onAbort, { once: true });
    const timer = req.timeoutMs ? setTimeout(() => ctrl.abort(), req.timeoutMs) : undefined;
    const done = () => {
      if (timer) clearTimeout(timer);
      req.signal.removeEventListener('abort', onAbort);
    };
    let res: Response;
    try {
      res = await fetch(req.url, { method: 'GET', headers: req.headers ?? {}, signal: ctrl.signal, redirect: 'manual' });
    } catch (e) {
      done();
      if (req.signal.aborted) throw new AbortError();
      throw new TransportError(timer && ctrl.signal.aborted ? 'timeout' : 'network', (e as Error).message, { cause: e });
    }
    const stream = res.body;
    const cancel = () => {
      ctrl.abort();
      done();
    };
    const body: AsyncIterable<Uint8Array> = {
      [Symbol.asyncIterator]: async function* () {
        if (!stream) return;
        const reader = stream.getReader();
        try {
          for (;;) {
            const { done: end, value } = await reader.read();
            if (end) break;
            yield value;
          }
        } catch (e) {
          if (req.signal.aborted) throw new AbortError();
          throw new TransportError('network', (e as Error).message, { cause: e });
        } finally {
          done();
        }
      },
    };
    return {
      status: res.status,
      headers: res.headers,
      body,
      text: (maxBytes = DEFAULT_MAX_TEXT_BYTES) => collect(body, maxBytes, cancel),
      cancel,
    };
  }
}

export type RequestClass = 'read-wait' | 'read' | 'export' | 'config' | 'other';

export function classifyUrl(url: string): RequestClass {
  const u = new URL(url);
  if (u.pathname === '/config') return 'config';
  if (/^\/r\/[^/]+\/export$/.test(u.pathname)) return 'export';
  if (/^\/r\/[^/]+$/.test(u.pathname)) {
    const w = Number(u.searchParams.get('wait') ?? '0');
    return w > 0 ? 'read-wait' : 'read';
  }
  return 'other';
}

export interface RecordedRequest {
  url: string;
  class: RequestClass;
  startedAt: number;
  endedAt?: number;
  status?: number;
  error?: string;
}

/** Wraps a transport and records every request (used by tests and the benchmark). */
export class CountingTransport implements Transport {
  readonly requests: RecordedRequest[] = [];
  private readonly inner: Transport;
  private readonly maxRecords: number;
  constructor(inner: Transport, o: { maxRecords?: number } = {}) {
    this.inner = inner;
    this.maxRecords = o.maxRecords ?? 100_000;
  }

  async get(req: TransportRequest): Promise<TransportResponse> {
    const rec: RecordedRequest = { url: req.url, class: classifyUrl(req.url), startedAt: Date.now() };
    if (this.requests.length < this.maxRecords) this.requests.push(rec);
    try {
      const res = await this.inner.get(req);
      rec.status = res.status;
      rec.endedAt = Date.now();
      return res;
    } catch (e) {
      rec.error = (e as Error).name;
      rec.endedAt = Date.now();
      throw e;
    }
  }

  counts(): Record<RequestClass, number> {
    const c: Record<RequestClass, number> = { 'read-wait': 0, read: 0, export: 0, config: 0, other: 0 };
    for (const r of this.requests) c[r.class]++;
    return c;
  }

  close(): void | Promise<void> {
    return this.inner.close?.();
  }
}
