import { describe, expect, it } from 'vitest';
import { TechnocoreReader, normalizeOrigin } from '../../src/transport/client.ts';
import { CountingTransport, classifyUrl } from '../../src/transport/http.ts';
import { createWatcher } from '../../src/engine/watcher.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import { FakeOrigin } from '../harness/fake-transport.ts';

const WRITE_PATH = /\/(say|say-signed|set|set-signed)\//;

describe('TechnocoreReader is read-only by construction', () => {
  it('exposes no write-shaped methods', () => {
    const names = Object.getOwnPropertyNames(TechnocoreReader.prototype);
    expect(names.sort()).toEqual(['close', 'config', 'constructor', 'exportScan', 'exportUrl', 'readRoom', 'roomUrl'].sort());
    for (const n of names) expect(n).not.toMatch(/say|post|sign|set|write|claim|put|delete/i);
  });

  it('refuses invalid room names before building any URL', () => {
    const r = new TechnocoreReader({ origin: 'http://127.0.0.1:1', transport: new FakeOrigin() });
    expect(() => r.roomUrl('../kv/x/y/set/z')).toThrow(TypeError);
    expect(() => r.roomUrl('lobby/say/x/y')).toThrow(TypeError);
    expect(r.roomUrl('lobby', { since: 3, limit: 999, waitS: 60 })).toBe('http://127.0.0.1:1/r/lobby?since=3&limit=200&wait=10&format=json');
  });

  it('validates origins', () => {
    expect(normalizeOrigin('https://technocore.chat/')).toBe('https://technocore.chat');
    expect(() => normalizeOrigin('ftp://x')).toThrow();
    expect(() => normalizeOrigin('https://u:p@x')).toThrow();
    expect(() => normalizeOrigin('https://x/?q=1')).toThrow();
  });

  it('a busy watcher only ever issues GET reads, exports and /config', async () => {
    const origin = new FakeOrigin();
    for (let i = 0; i < 20; i++) origin.model.post('lobby', 'a', `m${i}`);
    const counting = new CountingTransport(origin);
    const w = createWatcher({
      origin: 'http://fake.test',
      scopeKey: 's',
      store: new MemoryStateStore(),
      transport: counting,
      limits: { waitSeconds: 0.5, syncIntervalMs: 0 },
      coalesce: { quietMs: 10, maxDelayMs: 20, minWakeIntervalMs: 0 },
    });
    await w.subscribe('lobby', { startFrom: 'retained' });
    const notices: number[] = [];
    w.onNotice((n) => void notices.push(n.totalNew));
    for (let i = 0; i < 300; i++) origin.model.post('lobby', 'a', `x${i}`);
    for (let i = 0; i < 100 && notices.reduce((a, b) => a + b, 0) < 320; i++) await new Promise((r) => setTimeout(r, 20));
    const page = await w.readPage('lobby', { max: 5 });
    expect(page.messages).toHaveLength(5);
    await w.dispose();
    expect(counting.requests.length).toBeGreaterThan(2);
    for (const r of counting.requests) {
      expect(WRITE_PATH.test(new URL(r.url).pathname)).toBe(false);
      expect(['read', 'read-wait', 'export', 'config']).toContain(classifyUrl(r.url));
    }
    expect(notices.reduce((a, b) => a + b, 0)).toBe(320);
  });
});
