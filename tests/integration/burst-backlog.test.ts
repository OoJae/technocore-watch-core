// Scenarios 2 (8-message burst, pageSize 3) and 3 (450 messages while paused, via export).
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post, postMany, postSigned, testIdentity } from '../harness/writer.ts';
import { drain, harness, tempDir, waitFor } from './helpers.ts';

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

describe('burst and backlog against a local technocore-chat', () => {
  it('scenario 2: an 8-message burst read with pageSize 3 is one notice (new 8) and pages 1..3, 4..6, 7..8', async () => {
    await post(server.origin, 'burst', 'before subscribing');
    const h = harness({ origin: proxy.origin, scopeKey: 'burst', dataDir: data.path, limits: { pageSize: 3 }, coalesce: { quietMs: 500, maxDelayMs: 3000, minWakeIntervalMs: 30_000 } });
    const sub = await h.watcher.subscribe('burst');
    expect(sub.headSeq).toBe(1);
    await waitFor(() => proxy.count('read-wait') >= 1);
    const burst = [];
    for (let i = 1; i <= 7; i++) burst.push(await post(server.origin, 'burst', `burst message ${i}`));
    burst.push(await postSigned(server.origin, 'burst', 'signed burst message 8'));
    expect(burst).toEqual([2, 3, 4, 5, 6, 7, 8, 9]);
    await waitFor(() => h.notices.length === 1, 15_000);
    await new Promise((r) => setTimeout(r, 2500));
    expect(h.notices).toHaveLength(1);
    expect(h.notices[0]!.totalNew).toBe(8);
    expect(h.notices[0]!.rooms[0]).toMatchObject({ room: 'burst', newCount: 8, seqFrom: 2, seqTo: 9, gaps: [], signedSenders: 1, unsignedSenders: 1, generation: 1 });

    // Page from the start of the room the way a consumer would: 3 at a time.
    await h.watcher.ack('burst', 1);
    const pages: number[][] = [];
    for (;;) {
      const page = await h.watcher.readPage('burst');
      expect(page.untrusted).toBe(true);
      if (!page.messages.length) break;
      pages.push(page.messages.map((m) => m.seq));
      await h.watcher.ack('burst', page.toSeq);
    }
    expect(pages).toEqual([[2, 3, 4], [5, 6, 7], [8, 9]]);
    const st = h.watcher.status().subscriptions[0]!;
    expect(st).toMatchObject({ acceptedSeq: 9, headSeq: 9, notifiedSeq: 9, unread: 0 });
    const signed = (await h.watcher.readPage('burst')).messages;
    expect(signed).toEqual([]);
    expect(testIdentity().did).toMatch(/^did:key:z6Mk/);
    await h.watcher.dispose();
  });

  it('scenario 3: 450 messages posted while paused arrive via export with no loss', async () => {
    await post(server.origin, 'backlog', 'seed');
    const h = harness({ origin: proxy.origin, scopeKey: 'backlog', dataDir: data.path, coalesce: { quietMs: 300, maxDelayMs: 2000, minWakeIntervalMs: 0 } });
    await h.watcher.subscribe('backlog');
    await h.watcher.pause('backlog');
    const exportsBefore = proxy.count('export');
    await postMany(server.origin, 'backlog', 450, (i) => `backlog ${i}`, 4);
    await new Promise((r) => setTimeout(r, 1000));
    expect(h.notices).toHaveLength(0);
    await h.watcher.resume('backlog');
    await waitFor(() => h.notices.reduce((n, x) => n + x.totalNew, 0) >= 450, 30_000);
    expect(h.notices.reduce((n, x) => n + x.totalNew, 0)).toBe(450);
    expect(h.notices.flatMap((n) => n.rooms.flatMap((r) => r.gaps))).toEqual([]);
    expect(proxy.count('export') - exportsBefore).toBeGreaterThanOrEqual(1);
    await h.watcher.ack('backlog', 1);
    const pages = await drain(h.watcher, 'backlog', 200);
    const seqs = pages.flat();
    expect(seqs).toEqual(Array.from({ length: 450 }, (_, i) => i + 2));
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ headSeq: 451, acceptedSeq: 451, unread: 0 });
    await h.watcher.dispose();
  });
});
