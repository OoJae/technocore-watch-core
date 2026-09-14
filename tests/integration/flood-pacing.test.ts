// Scenario 13: flood re-poll pacing against a local technocore-chat. During a flood the room is read a
// few times per notice window instead of once per message, no seq is lost (a flood, then a fast
// surge), and a single message outside a flood is still seen by the poll at once.
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { Notice } from '../../src/protocol/types.ts';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post, postMany } from '../harness/writer.ts';
import { drain, harness, sleepMs, tempDir, waitFor } from './helpers.ts';

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

const COALESCE = { quietMs: 2000, maxDelayMs: 4000, minWakeIntervalMs: 5000 };

function roomReads(room: string, from: number, to: number): number {
  return proxy.records.filter((r) => r.room === room && (r.class === 'read-wait' || r.class === 'read') && r.startedAt >= from && r.startedAt < to).length;
}

/** Posts `n` messages at `perSecond`, on a fixed schedule so slow posts do not stretch the flood. */
async function floodRoom(room: string, n: number, perSecond: number): Promise<{ start: number; end: number; seqs: number[] }> {
  const start = Date.now();
  const seqs: number[] = [];
  for (let i = 0; i < n; i++) {
    const due = start + (i * 1000) / perSecond;
    if (due > Date.now()) await sleepMs(due - Date.now());
    seqs.push(await post(server.origin, room, `flood ${i}`));
  }
  return { start, end: Date.now(), seqs };
}

function covered(notices: Notice[], room: string): number {
  return notices.flatMap((n) => n.rooms.filter((r) => r.room === room)).reduce((max, r) => Math.max(max, r.seqTo), 0);
}

describe('flood re-poll pacing against a local technocore-chat', () => {
  it('scenario 13: a flood costs a few reads per notice window, loses nothing, and single messages are still seen at once', async () => {
    const room = 'flood';
    await post(server.origin, room, 'seed');
    const h = harness({ origin: proxy.origin, scopeKey: 'flood', dataDir: data.path, coalesce: COALESCE });
    const seen: { at: number; to: number }[] = [];
    h.watcher.onEvent((e) => {
      if (e.type === 'activity' && e.room === room) seen.push({ at: Date.now(), to: Number(e.detail?.to) });
    });
    const seenAfter = async (seq: number, postedAt: number) => {
      await waitFor(() => seen.some((s) => s.to >= seq), 15_000, 5);
      return seen.find((s) => s.to >= seq)!.at - postedAt;
    };
    await h.watcher.subscribe(room);
    await waitFor(() => proxy.count('read-wait') >= 1);
    const singles: number[] = [];

    // Isolated messages: each one starts a notice (the previous notice went out > minWake ago).
    for (let i = 0; i < 2; i++) {
      const seq = await post(server.origin, room, `single ${i}`);
      singles.push(await seenAfter(seq, Date.now()));
      await sleepMs(6500);
    }

    // A 15 s flood at 20 msg/s (paced), then a surge of 600 as fast as the server takes them.
    const flood = await floodRoom(room, 300, 20);
    const floodReads = roomReads(room, flood.start, flood.end);
    const pacedNaps = h.events.filter((e) => e.type === 'poll-paced').length;
    const surge = await postMany(server.origin, room, 600, (i) => `surge ${i}`, 8);
    const last = surge[surge.length - 1]!;
    await waitFor(() => covered(h.notices, room) >= last, 60_000, 50);

    // Outside the flood again: once the last notice is out, a single message is seen at once.
    await sleepMs(COALESCE.minWakeIntervalMs + COALESCE.quietMs + 500);
    const after = await post(server.origin, room, 'single after the flood');
    singles.push(await seenAfter(after, Date.now()));
    await waitFor(() => covered(h.notices, room) >= after, 15_000);

    // No seq lost: notices cover 2..after exactly once without gaps, and pages read back all of it.
    let next = 2;
    for (const n of h.notices) {
      for (const r of n.rooms) {
        expect(r.gaps).toEqual([]);
        expect(r.seqFrom).toBe(next);
        expect(r.newCount).toBe(r.seqTo - r.seqFrom + 1);
        next = r.seqTo + 1;
      }
    }
    expect(next - 1).toBe(after);
    await h.watcher.ack(room, 1);
    expect((await drain(h.watcher, room, 200)).flat()).toEqual(Array.from({ length: after - 1 }, (_, i) => i + 2));
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ headSeq: after, notifiedSeq: after, acceptedSeq: after, unread: 0, gaps: [] });

    console.log(
      `[scenario 13] flood: ${flood.seqs.length} posts in ${((flood.end - flood.start) / 1000).toFixed(1)} s -> ${floodReads} room reads, ` +
        `${pacedNaps} paced naps; surge: ${surge.length} posts; notices: ${h.notices.length}; exports: ${proxy.count('export')}; ` +
        `seen-by-poll outside floods (ms): ${singles.join(', ')}`,
    );
    expect(pacedNaps).toBeGreaterThan(0);
    // 15 s is three 5 s notice windows: a handful of reads each, not one per message.
    expect(floodReads).toBeLessThanOrEqual(30);
    expect(floodReads * 5).toBeLessThan(flood.seqs.length);
    for (const ms of singles) expect(ms).toBeLessThan(1000);
    await h.watcher.dispose();
  });

  it('scenario 13 control: the same flood with pacing off reads about once per message', async () => {
    const room = 'flood-unpaced';
    await post(server.origin, room, 'seed');
    const h = harness({ origin: proxy.origin, scopeKey: 'flood-unpaced', dataDir: data.path, coalesce: COALESCE, limits: { floodRepollPacing: false } });
    await h.watcher.subscribe(room);
    await waitFor(() => proxy.records.some((r) => r.room === room && r.class === 'read-wait'));
    const flood = await floodRoom(room, 160, 20);
    const floodReads = roomReads(room, flood.start, flood.end);
    const last = flood.seqs[flood.seqs.length - 1]!;
    await waitFor(() => covered(h.notices, room) >= last, 30_000, 50);
    console.log(`[scenario 13 control] pacing off: ${flood.seqs.length} posts in ${((flood.end - flood.start) / 1000).toFixed(1)} s -> ${floodReads} room reads`);
    expect(h.events.filter((e) => e.type === 'poll-paced')).toEqual([]);
    expect(floodReads).toBeGreaterThanOrEqual(flood.seqs.length / 2);
    await h.watcher.dispose();
  });
});
