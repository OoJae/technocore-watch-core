// Scenario 5: room recreation. (a) A proper reap keeps the floor, so seq continues and the
// watcher emits `recreated`; (b) deleting the room file by hand loses the floor, seq restarts
// and the watcher reports a seq regression. The server is stopped for each store mutation.
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startTechnocore, type TechnocoreServer } from '../harness/technocore-server.ts';
import { startProxy, type CountingProxy } from '../harness/counting-proxy.ts';
import { post } from '../harness/writer.ts';
import { harness, tempDir, waitFor } from './helpers.ts';

let server: TechnocoreServer;
let proxy: CountingProxy;
let data: Awaited<ReturnType<typeof tempDir>>;

const REAP = `
import sys, pathlib
sys.path.insert(0, 'src')
import store, config
root = pathlib.Path(config.ROOT)
room = sys.argv[1]
store._set_seq_entry(root, room, store.last_seq(root, room))
store.room_path(root, room).unlink()
print('reaped', room)
`;

const DELETE_FILE = `
import sys, pathlib
sys.path.insert(0, 'src')
import store, config
root = pathlib.Path(config.ROOT)
room = sys.argv[1]
store.room_path(root, room).unlink()
print('deleted', room)
`;

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

describe('recreation', () => {
  it('scenario 5a: reap with floor -> recreated notice, seq continues', async () => {
    for (let i = 0; i < 3; i++) await post(server.origin, 'reaped', `gen1 ${i}`);
    const h = harness({ origin: proxy.origin, scopeKey: 'recreate', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 }, limits: { waitSeconds: 2 } });
    await h.watcher.subscribe('reaped');
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ headSeq: 3, generation: 1 });
    await server.stop();
    expect(await server.python(REAP, ['reaped'])).toContain('reaped');
    await server.start();
    expect(await post(server.origin, 'reaped', 'generation two')).toBe(4);
    await waitFor(() => h.notices.some((n) => n.rooms.some((r) => r.recreated)), 60_000);
    const room = h.notices.flatMap((n) => n.rooms).find((r) => r.recreated)!;
    expect(room).toMatchObject({ room: 'reaped', recreated: { from: 1, to: 2 }, generation: 2, seqTo: 4, newCount: 1, gaps: [] });
    expect(room.regression).toBeUndefined();
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 4 });
    expect(h.events.some((e) => e.type === 'recreated')).toBe(true);
    await h.watcher.dispose();
  });

  it('scenario 5b: room file deleted (floor lost) -> seq regression surfaced explicitly', async () => {
    for (let i = 0; i < 3; i++) await post(server.origin, 'regress', `gen1 ${i}`);
    const h = harness({ origin: proxy.origin, scopeKey: 'regress', dataDir: data.path, coalesce: { quietMs: 200, maxDelayMs: 1000, minWakeIntervalMs: 0 }, limits: { waitSeconds: 2 } });
    await h.watcher.subscribe('regress'); // 'now': 1..3 count as handled
    for (let i = 3; i < 6; i++) await post(server.origin, 'regress', `gen1 ${i}`);
    await waitFor(() => h.watcher.status().subscriptions[0]!.headSeq === 6, 15_000);
    await h.watcher.ack('regress', 4); // 5 and 6 stay unread
    await server.stop();
    expect(await server.python(DELETE_FILE, ['regress'])).toContain('deleted');
    await server.start();
    expect(await post(server.origin, 'regress', 'restarted one')).toBe(1);
    expect(await post(server.origin, 'regress', 'restarted two')).toBe(2);
    await waitFor(() => h.notices.some((n) => n.rooms.some((r) => r.regression)), 60_000);
    const room = h.notices.flatMap((n) => n.rooms).find((r) => r.regression)!;
    expect(room).toMatchObject({ room: 'regress', regression: { fromHead: 6, toLast: 2 }, recreated: { from: 1, to: 2 }, generation: 2, seqFrom: 1, seqTo: 2, newCount: 2 });
    expect(room.gaps).toEqual([expect.objectContaining({ from: 5, to: 6, reason: 'seq-regression', generation: 1 })]);
    expect(h.watcher.status().subscriptions[0]).toMatchObject({ generation: 2, headSeq: 2, acceptedSeq: 0, unread: 2 });
    const page = await h.watcher.readPage('regress');
    expect(page.messages.map((m) => m.text)).toEqual(['restarted one', 'restarted two']);
    await h.watcher.dispose();
  });
});
