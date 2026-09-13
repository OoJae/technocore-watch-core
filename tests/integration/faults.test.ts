// Scenarios 6 (server killed mid long-poll) and 7 (proxy replays an old response).
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post } from '../harness/writer.ts';
import { harness, sleepMs, tempDir, waitFor } from './helpers.ts';

let server: TechnocoreServer;
let proxy: CountingProxy;
let data: Awaited<ReturnType<typeof tempDir>>;

beforeAll(async () => {
  server = await startTechnocore();
  proxy = await startProxy(server.origin);
  data = await tempDir();
});

afterAll(async () => {
  await proxy?.close();
  await server?.dispose();
  await data?.cleanup();
});

describe('faults', () => {
  it('scenario 6: killing uvicorn mid long-poll -> jittered reconnect, no duplicate notice', async () => {
    for (let i = 0; i < 2; i++) await post(server.origin, 'crash', `before ${i}`);
    const h = harness({ origin: proxy.origin, scopeKey: 'crash', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 } });
    await h.watcher.subscribe('crash');
    await post(server.origin, 'crash', 'delivered once');
    await waitFor(() => h.notices.length === 1, 15_000);
    // Wait until a long-poll is parked, then kill the server process group hard.
    await waitFor(() => proxy.inflightLongPolls >= 1, 15_000);
    const killedAt = Date.now();
    await server.kill();
    await sleepMs(3000);
    const attemptsWhileDown = proxy.records.filter((r) => r.startedAt >= killedAt && r.room === 'crash');
    expect(attemptsWhileDown.length).toBeGreaterThanOrEqual(1);
    // Decorrelated jitter from 500 ms: nowhere near a tight loop while the origin is down.
    expect(attemptsWhileDown.length).toBeLessThanOrEqual(8);
    expect(h.events.some((e) => e.type === 'network-error')).toBe(true);
    await server.start();
    await post(server.origin, 'crash', 'after restart');
    await waitFor(() => h.notices.length === 2, 90_000, 100);
    await sleepMs(1500);
    expect(h.notices).toHaveLength(2);
    expect(h.notices[0]!.rooms[0]).toMatchObject({ seqFrom: 3, seqTo: 3, newCount: 1 });
    expect(h.notices[1]!.rooms[0]).toMatchObject({ seqFrom: 4, seqTo: 4, newCount: 1 });
    const gapsBetween = attemptsWhileDown.map((r) => r.startedAt).sort((a, b) => a - b);
    for (let i = 1; i < gapsBetween.length; i++) expect(gapsBetween[i]! - gapsBetween[i - 1]!).toBeGreaterThanOrEqual(100);
    await h.watcher.dispose();
  });

  it('scenario 7: a replayed (stale) response never produces a duplicate notice', async () => {
    await post(server.origin, 'replay', 'seed');
    const h = harness({ origin: proxy.origin, scopeKey: 'replay', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 }, limits: { waitSeconds: 1 } });
    await h.watcher.subscribe('replay');
    await post(server.origin, 'replay', 'one');
    await post(server.origin, 'replay', 'two');
    await waitFor(() => h.notices.length === 1, 15_000);
    expect(h.notices[0]!.totalNew).toBe(2);
    // The proxy now answers the next reads with the last 200 body it saw for the room
    // (the response that carried seq 2..3), several times over.
    proxy.replayNext(5);
    await waitFor(() => proxy.records.filter((r) => r.replayed).length >= 5, 30_000);
    await post(server.origin, 'replay', 'three');
    await waitFor(() => h.notices.length === 2, 15_000);
    await sleepMs(1500);
    expect(h.notices).toHaveLength(2);
    expect(h.notices[1]!.rooms[0]).toMatchObject({ seqFrom: 4, seqTo: 4, newCount: 1 });
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ headSeq: 4 });
    await h.watcher.dispose();
  });
});
