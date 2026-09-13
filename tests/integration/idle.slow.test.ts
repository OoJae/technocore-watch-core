// Scenario 1 (slow, 60 s real time): an idle watcher produces 0 notices and costs about
// rooms x 6 long-polls per minute at the origin.
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post } from '../harness/writer.ts';
import { SKIP_SLOW, harness, sleepMs, tempDir } from './helpers.ts';

let server: TechnocoreServer;
let proxy: CountingProxy;
let data: Awaited<ReturnType<typeof tempDir>>;

beforeAll(async () => {
  if (SKIP_SLOW) return;
  server = await startTechnocore();
  proxy = await startProxy(server.origin);
  data = await tempDir();
});

afterAll(async () => {
  await proxy?.close();
  await server?.dispose();
  await data?.cleanup();
});

describe.skipIf(SKIP_SLOW)('[slow] idle', () => {
  it('scenario 1: idle for 60 s -> 0 notices, long-polls ~ rooms x 6/min, nothing else', async () => {
    const rooms = ['idle-a', 'idle-b'];
    for (const r of rooms) await post(server.origin, r, 'seed');
    const h = harness({ origin: proxy.origin, scopeKey: 'idle', dataDir: data.path });
    for (const r of rooms) await h.watcher.subscribe(r);
    await sleepMs(500);
    const t0 = Date.now();
    await sleepMs(60_000);
    const c = proxy.counts(t0);
    expect(h.notices).toHaveLength(0);
    // 2 rooms x 6/min = 12; allow one in-flight poll per room at either edge of the window.
    expect(c['read-wait']).toBeGreaterThanOrEqual(10);
    expect(c['read-wait']).toBeLessThanOrEqual(14);
    expect(c.read).toBe(0);
    expect(c.export).toBe(0);
    expect(h.events.filter((e) => e.type === 'network-error' || e.type === 'rate-limited')).toEqual([]);
    await h.watcher.dispose();
  });
});
