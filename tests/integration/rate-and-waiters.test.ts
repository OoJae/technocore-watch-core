// Scenarios 8 (CHAT_RATE_READ=10: follow the 429 body) and 9 (CHAT_MAX_WAITERS_PER_IP=1 while
// another client holds the slot: wait_held:false leads to a sleep, not a tight loop).
import * as http from 'node:http';
import { afterAll, describe, expect, it } from 'vitest';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post } from '../harness/writer.ts';
import { harness, sleepMs, tempDir, waitFor } from './helpers.ts';

const cleanups: (() => Promise<void>)[] = [];
afterAll(async () => {
  for (const c of cleanups.reverse()) await c().catch(() => undefined);
});

function get(url: string, signal?: AbortSignal): Promise<{ status: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = http.get(url, { agent: false, signal }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (c: string) => (body += c));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, body }));
    });
    req.on('error', reject);
  });
}

async function setup(o: Parameters<typeof startTechnocore>[0]): Promise<{ server: TechnocoreServer; proxy: CountingProxy; dataDir: string }> {
  const server = await startTechnocore(o);
  const proxy = await startProxy(server.origin);
  const data = await tempDir();
  cleanups.push(async () => {
    await proxy.close();
    await server.dispose();
    await data.cleanup();
  });
  return { server, proxy, dataDir: data.path };
}

describe('origin limits', () => {
  it('scenario 8: CHAT_RATE_READ=10 -> the watcher honours the 429 body and stays within budget afterwards', async () => {
    const { server, proxy, dataDir } = await setup({ rateRead: 10 });
    await post(server.origin, 'limited', 'seed');
    const h = harness({
      origin: proxy.origin,
      scopeKey: 'rate',
      dataDir,
      // Spend the whole published budget so the server, not the client governor, is the wall.
      limits: { readBudgetFraction: 1, waitSeconds: 1 },
      coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 },
    });
    // Another client on the same IP spends the read bucket first.
    for (let i = 0; i < 12; i++) await get(`${server.origin}/r/limited?format=json`);
    const t0 = Date.now();
    await h.watcher.subscribe('limited', { startFrom: 'now' }).catch(() => undefined);
    await waitFor(() => proxy.records.some((r) => r.status === 429 && r.startedAt >= t0), 30_000);
    const first429 = proxy.records.find((r) => r.status === 429 && r.startedAt >= t0)!;
    await waitFor(() => h.events.some((e) => e.type === 'rate-limited'), 10_000);
    const ev = h.events.find((e) => e.type === 'rate-limited')!;
    const retryAfterS = Number(ev.detail!.retryAfterS);
    expect(retryAfterS).toBeGreaterThanOrEqual(1);
    await sleepMs(25_000);
    const after = proxy.records.filter((r) => r.startedAt > first429.startedAt && r.class !== 'config');
    expect(after.length).toBeGreaterThanOrEqual(1);
    // The next request waited out the retry the body named.
    expect(after[0]!.startedAt - first429.startedAt).toBeGreaterThanOrEqual(retryAfterS * 1000 - 250);
    // And the watcher's rate after the 429 stays within 10/min (plus a small client burst).
    const windowMin = (Date.now() - first429.startedAt) / 60_000;
    expect(after.length).toBeLessThanOrEqual(Math.ceil(10 * windowMin) + 2);
    // It still works: a post is delivered once budget allows.
    await post(server.origin, 'limited', 'eventually');
    await waitFor(() => h.notices.some((n) => n.totalNew >= 1), 90_000, 200);
    await h.watcher.dispose();
  });

  it('scenario 9: CHAT_MAX_WAITERS_PER_IP=1 held elsewhere -> wait_held:false sleeps instead of spinning', async () => {
    const { server, proxy, dataDir } = await setup({ maxWaitersPerIp: 1 });
    await post(server.origin, 'held', 'seed');
    const hog = new AbortController();
    let hogging = true;
    void (async () => {
      while (hogging) await get(`${server.origin}/r/held?since=1&wait=10&format=json`, hog.signal).catch(() => undefined);
    })();
    await sleepMs(300);
    const h = harness({ origin: proxy.origin, scopeKey: 'held', dataDir, limits: { waitSeconds: 2 } });
    await h.watcher.subscribe('held');
    const t0 = Date.now();
    await sleepMs(12_000);
    const polls = proxy.records.filter((r) => r.class === 'read-wait' && r.startedAt >= t0);
    const events = h.events.filter((e) => e.type === 'wait-not-held' && Date.parse(e.at) >= t0);
    expect(events.length).toBeGreaterThanOrEqual(3);
    // ~one request per 2 s (±20%), certainly not hundreds.
    expect(polls.length).toBeLessThanOrEqual(Math.ceil(12 / 1.6) + 2);
    expect(polls.length).toBeGreaterThanOrEqual(3);
    hogging = false;
    hog.abort();
    await h.watcher.dispose();
  });
});
