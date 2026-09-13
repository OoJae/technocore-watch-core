import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DecorrelatedJitter, jitter } from '../../src/limits/backoff.ts';
import { AbortError, sleep } from '../../src/limits/clock.ts';
import { RateGovernor } from '../../src/limits/governor.ts';
import { WaiterSemaphore } from '../../src/limits/semaphore.ts';

describe('DecorrelatedJitter', () => {
  it('stays within [base, cap], grows, and resets', () => {
    let x = 0.999;
    const j = new DecorrelatedJitter({ baseMs: 500, capMs: 60_000, random: () => x });
    const seq = Array.from({ length: 12 }, () => j.next());
    for (const v of seq) {
      expect(v).toBeGreaterThanOrEqual(500);
      expect(v).toBeLessThanOrEqual(60_000);
    }
    expect(seq[seq.length - 1]).toBe(60_000);
    j.reset();
    x = 0;
    expect(j.next()).toBe(500);
    const f = new DecorrelatedJitter({ baseMs: 10, capMs: 20, floorMs: 15, random: () => 0 });
    expect(f.next()).toBe(15);
  });

  it('jitter spreads symmetrically', () => {
    expect(jitter(1000, 0.2, () => 0)).toBe(800);
    expect(jitter(1000, 0.2, () => 1)).toBe(1200);
  });
});

describe('RateGovernor (fake time)', () => {
  beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
  afterEach(() => vi.useRealTimers());

  it('refills continuously and never exceeds the per-minute rate after the burst', async () => {
    const g = new RateGovernor({ perMinute: 60, burst: 2 });
    let granted = 0;
    const loop = (async () => {
      for (;;) {
        await g.acquire();
        granted++;
        if (granted >= 12) return;
      }
    })();
    await vi.advanceTimersByTimeAsync(10_000);
    // 2 burst + ~1/s refill over 10 s
    expect(granted).toBeLessThanOrEqual(12);
    expect(granted).toBeGreaterThanOrEqual(11);
    await vi.advanceTimersByTimeAsync(5000);
    await loop;
  });

  it('a 429 penalty blocks the whole bucket for retryAfter', async () => {
    const g = new RateGovernor({ perMinute: 6000 });
    g.penalize(6);
    expect(g.delayMs()).toBe(6000);
    let done = false;
    void g.acquire().then(() => (done = true));
    await vi.advanceTimersByTimeAsync(5900);
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(200);
    expect(done).toBe(true);
    expect(g.penalties).toBe(1);
  });

  it('abort beats every sleep', async () => {
    const ctrl = new AbortController();
    const p = sleep(60_000, ctrl.signal);
    ctrl.abort();
    await expect(p).rejects.toBeInstanceOf(AbortError);
    const g = new RateGovernor({ perMinute: 1, burst: 1 });
    await g.acquire();
    const c2 = new AbortController();
    const q = g.acquire(c2.signal);
    c2.abort();
    await expect(q).rejects.toBeInstanceOf(AbortError);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('WaiterSemaphore', () => {
  it('caps concurrency, is FIFO, and supports abort', async () => {
    const s = new WaiterSemaphore(2);
    const r1 = await s.acquire();
    const r2 = await s.acquire();
    expect(s.tryAcquire()).toBeNull();
    const order: number[] = [];
    const p3 = s.acquire().then((r) => {
      order.push(3);
      return r;
    });
    const ctrl = new AbortController();
    const p4 = s.acquire(ctrl.signal);
    const p5 = s.acquire().then((r) => {
      order.push(5);
      return r;
    });
    ctrl.abort();
    await expect(p4).rejects.toBeInstanceOf(AbortError);
    r1();
    r1(); // double release is a no-op
    const r3 = await p3;
    r2();
    const r5 = await p5;
    expect(order).toEqual([3, 5]);
    expect(s.maxObserved).toBe(2);
    r3();
    r5();
    expect(s.active).toBe(0);
  });
});
