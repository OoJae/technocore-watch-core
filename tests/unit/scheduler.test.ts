import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SlotScheduler } from '../../src/engine/scheduler.ts';

describe('SlotScheduler (fake time)', () => {
  beforeEach(() => vi.useFakeTimers({ now: new Date('2026-09-13T00:00:00Z') }));
  afterEach(() => vi.useRealTimers());

  it('gives the first N rooms slots and sweeps the rest at the budget-derived interval', () => {
    const s = new SlotScheduler({ slots: 3, minTenureMs: 60_000 });
    for (const r of ['a', 'b', 'c', 'd', 'e']) s.add(r);
    expect(s.slotted().sort()).toEqual(['a', 'b', 'c']);
    expect(s.overflowCount()).toBe(2);
    // budget 300/min: max(15 s, 2 x 60000 / (300 - 18)) = 15 s
    expect(s.sweepIntervalMs(300)).toBe(15_000);
    // tiny budget: 2 x 60000 / (20 - 18) = 60 s
    expect(s.sweepIntervalMs(20)).toBe(60_000);
  });

  it('promotes an active swept room only over a holder that served its tenure', () => {
    const s = new SlotScheduler({ slots: 2, minTenureMs: 60_000 });
    const changes: string[][] = [];
    s.onChange((c) => changes.push(c));
    s.add('a');
    s.add('b');
    s.add('c');
    s.noteActivity('a');
    s.noteActivity('c');
    expect(s.hasSlot('c')).toBe(false); // holders are too new
    vi.advanceTimersByTime(61_000);
    s.noteActivity('c');
    expect(s.hasSlot('c')).toBe(true);
    expect(s.hasSlot('b')).toBe(false); // b was the quiet one
    expect(s.hasSlot('a')).toBe(true);
    s.remove('a');
    expect(s.slotted().sort()).toEqual(['b', 'c']);
    expect(changes.length).toBeGreaterThan(0);
  });

  it('shrinking slots demotes the quietest', () => {
    const s = new SlotScheduler({ slots: 3 });
    s.add('a');
    s.add('b');
    s.add('c');
    s.noteActivity('b');
    s.setSlots(1);
    expect(s.slotted()).toEqual(['b']);
  });
});
