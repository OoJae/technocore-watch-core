// Output and memory bounds on the fake origin: noticeMaxChars reaches rendering, readPage's
// character budget counts the escaped output, and the pool's message cache forgets rooms.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createWatcher, type Watcher, type WatcherOptions } from '../../src/engine/watcher.ts';
import { HostPool } from '../../src/engine/host-pool.ts';
import { MessageCache } from '../../src/engine/cache.ts';
import { MemoryStateStore } from '../../src/state/memory-store.ts';
import { asciiJson, messageJson, renderNotice, renderPage } from '../../src/protocol/render.ts';
import type { Notice } from '../../src/protocol/types.ts';
import { FakeOrigin } from '../harness/fake-transport.ts';

const ORIGIN = 'http://fake.test';

function watcher(origin: FakeOrigin, over: Partial<WatcherOptions> = {}): Watcher {
  return createWatcher({
    origin: ORIGIN,
    scopeKey: 'scope-b',
    store: new MemoryStateStore(),
    transport: origin,
    limits: { syncIntervalMs: 0, ...over.limits },
    coalesce: { quietMs: 100, maxDelayMs: 500, minWakeIntervalMs: 0, ...over.coalesce },
    ...over,
  } as WatcherOptions);
}

async function until(cond: () => boolean, stepMs = 50, maxMs = 120_000): Promise<void> {
  for (let t = 0; t < maxMs && !cond(); t += stepMs) await vi.advanceTimersByTimeAsync(stepMs);
  expect(cond()).toBe(true);
}

beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
afterEach(() => vi.useRealTimers());

describe('bounds', () => {
  it('limits.noticeMaxChars bounds what the watcher renders', async () => {
    const origin = new FakeOrigin();
    const rooms = Array.from({ length: 3 }, (_, i) => `a-very-long-room-name-for-the-notice-bound-te-${i}`); // all long-polled
    for (const r of rooms) origin.model.post(r, 'seed', 'x');
    const w = watcher(origin, { limits: { noticeMaxChars: 700, syncIntervalMs: 0 } });
    const notices: Notice[] = [];
    w.onNotice((n) => void notices.push(n));
    for (const r of rooms) await w.subscribe(r);
    await vi.advanceTimersByTimeAsync(1000);
    for (const r of rooms) origin.model.post(r, 'did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK', 'y');
    await until(() => notices.reduce((n, x) => n + x.totalNew, 0) === rooms.length);
    const notice = notices.find((n) => n.rooms.length > 1)!;
    expect(renderNotice(notice).length).toBeGreaterThan(700); // the default 1,500 bound allows more
    const framed = w.renderNotice(notice);
    const line = w.renderNotice(notice, 'line');
    expect(framed.length).toBeLessThanOrEqual(700);
    expect(line.length).toBeLessThanOrEqual(700);
    expect(line).not.toContain('\n');
    expect(framed).toContain('more_rooms:');
    await w.dispose();
  });

  it('readPage counts the escaped output against maxPageChars, not raw UTF-16 length', async () => {
    const origin = new FakeOrigin();
    const w = watcher(origin);
    await w.subscribe('emoji');
    await vi.advanceTimersByTimeAsync(500);
    const text = '\u{1F600}'.repeat(1000); // 2,000 UTF-16 units; ~12,000 characters once escaped
    for (let i = 0; i < 10; i++) origin.model.post('emoji', 'x', text);
    await until(() => w.status().subscriptions[0]!.headSeq === 10);
    const lens: number[] = [];
    let pages = 0;
    for (;;) {
      const page = await w.readPage('emoji');
      if (!page.messages.length) break;
      pages++;
      const rendered = page.messages.map((m) => asciiJson(messageJson(m)).length).reduce((a, b) => a + b, 0);
      lens.push(rendered);
      // At most maxPageChars of message lines, or a single message that alone exceeds it.
      expect(rendered <= 16_000 || page.messages.length === 1).toBe(true);
      expect(renderPage(page).length).toBeLessThan(16_000 + 1_000);
      await w.ack('emoji', page.toSeq);
    }
    expect(pages).toBe(10);
    await w.dispose();
  });

  it('the pool cache forgets a room when its last member detaches, and caps rooms overall', async () => {
    const origin = new FakeOrigin();
    const pool = new HostPool({ origin: ORIGIN, transport: origin, limits: { syncIntervalMs: 0 } });
    const w = createWatcher({ origin: ORIGIN, scopeKey: 'cache', store: new MemoryStateStore(), pool, coalesce: { quietMs: 50, maxDelayMs: 100, minWakeIntervalMs: 0 } });
    for (let r = 0; r < 5; r++) {
      const room = `churn-${r}`;
      await w.subscribe(room);
      await vi.advanceTimersByTimeAsync(200);
      for (let i = 0; i < 20; i++) origin.model.post(room, 'x', `m${i}`);
      await until(() => w.status().subscriptions.find((s) => s.room === room)?.headSeq === 20);
      await w.readPage(room);
      expect(pool.cache.has(room)).toBe(true);
      await w.unsubscribe(room);
      await vi.advanceTimersByTimeAsync(100);
      expect(pool.cache.has(room)).toBe(false);
    }
    expect(pool.cache.roomCount).toBe(0);
    await w.dispose();
    await pool.dispose();

    const cache = new MessageCache(10, 4);
    for (let r = 0; r < 20; r++) cache.add(`room-${r}`, 1, [{ seq: 1, ts: 't', from: 'a', text: 'x' }]);
    expect(cache.roomCount).toBe(4);
    expect(cache.has('room-19')).toBe(true);
    expect(cache.has('room-0')).toBe(false);
  });
});
