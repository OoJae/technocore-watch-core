// Scenario 4 (slow): ~11 MiB of 4096-character posts (~2,700 POSTs) force the 10 MiB ring
// to compact while the subscription is paused. The watcher must record a ring-dropped gap
// with the exact range the export proves is gone, and deliver everything still retained.
// MAX_ROOM_BYTES is a constant in v0.13.0 (not a CHAT_ knob), so the full volume is used.
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import * as http from 'node:http';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post, postMany } from '../harness/writer.ts';
import { SKIP_SLOW, harness, tempDir, waitFor } from './helpers.ts';

let server: TechnocoreServer;
let proxy: CountingProxy;
let data: Awaited<ReturnType<typeof tempDir>>;

function firstExportSeq(origin: string, room: string): Promise<{ first: number; last: number; count: number }> {
  return new Promise((resolve, reject) => {
    http.get(`${origin}/r/${room}/export`, { agent: false }, (res) => {
      let buf = '';
      let first = -1;
      let last = -1;
      let count = 0;
      res.setEncoding('utf8');
      res.on('data', (c: string) => {
        buf += c;
        let nl: number;
        while ((nl = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, nl);
          buf = buf.slice(nl + 1);
          if (!line.trim()) continue;
          const seq = (JSON.parse(line) as { seq: number }).seq;
          if (first < 0) first = seq;
          last = seq;
          count++;
        }
      });
      res.on('end', () => resolve({ first, last, count }));
      res.on('error', reject);
    }).on('error', reject);
  });
}

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

describe.skipIf(SKIP_SLOW)('[slow] ring drop', () => {
  it('scenario 4: a compacted ring yields a ring-dropped gap with the exact range', async () => {
    for (let i = 0; i < 5; i++) await post(server.origin, 'ring', `early ${i}`);
    const h = harness({
      origin: proxy.origin,
      scopeKey: 'ring',
      dataDir: data.path,
      coalesce: { quietMs: 300, maxDelayMs: 2000, minWakeIntervalMs: 0 },
    });
    const sub = await h.watcher.subscribe('ring');
    expect(sub.headSeq).toBe(5);
    await h.watcher.pause('ring');
    const text = (i: number) => `${String(i).padStart(6, '0')} ${'x'.repeat(4089)}`;
    expect(text(1)).toHaveLength(4096);
    await postMany(server.origin, 'ring', 2700, text, 6);
    const retained = await firstExportSeq(server.origin, 'ring');
    expect(retained.first).toBeGreaterThan(6); // compaction really happened
    expect(retained.last).toBe(2705);

    await h.watcher.resume('ring');
    await waitFor(() => h.watcher.status().subscriptions[0]!.headSeq === 2705, 60_000, 100);
    await waitFor(() => h.notices.length > 0 && h.notices.reduce((n, x) => n + x.totalNew, 0) + 0 >= Math.min(2000, retained.count) - 1, 30_000, 100);
    const gaps = h.notices.flatMap((n) => n.rooms.flatMap((r) => r.gaps));
    const dropped = gaps.filter((g) => g.reason === 'ring-dropped');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({ from: 6, to: retained.first - 1, reason: 'ring-dropped', generation: 1 });
    // Retained records beyond the 2000-record backfill budget are reported, not skipped.
    const bounded = gaps.filter((g) => g.reason === 'backfill-bounded');
    const delivered = h.notices.reduce((n, x) => n + x.totalNew, 0);
    const boundedCount = bounded.reduce((n, g) => n + g.to - g.from + 1, 0);
    expect(delivered + boundedCount + (dropped[0]!.to - dropped[0]!.from + 1)).toBe(2700);
    const st = h.watcher.status().subscriptions[0]!;
    expect(st.gaps.some((g) => g.reason === 'ring-dropped' && g.from === 6 && g.to === retained.first - 1)).toBe(true);
    // The consumer's first page reports the gap and starts at the oldest retained record.
    const page = await h.watcher.readPage('ring', { max: 5, maxChars: 1_000_000 });
    expect(page.gaps[0]).toMatchObject({ from: 6, to: retained.first - 1 });
    expect(page.messages[0]!.seq).toBe(retained.first);
    await h.watcher.dispose();
  });
});
