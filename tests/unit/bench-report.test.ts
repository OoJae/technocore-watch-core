import { describe, expect, it } from 'vitest';
import { type BenchTrace, markdown, summarize } from '../../bench/report.ts';
import { SESSIONS } from '../../bench/workload.ts';

/** A 15-workload-minute trace at scale 1/15 (1 real minute; workload minute m starts at m * 4 s). */
function trace(): BenchTrace {
  const startedAt = 1_000_000;
  const at = (workloadMin: number) => startedAt + workloadMin * 4000;
  const requests: BenchTrace['requests'] = [{ class: 'config', startedAt }];
  // 10 reads before the flood, 40 inside workload minutes 4-9 (20 s real), 10 after it.
  for (let i = 0; i < 10; i++) requests.push({ class: 'read-wait', startedAt: at(0.1 + i * 0.3) });
  for (let i = 0; i < 40; i++) requests.push({ class: i % 4 ? 'read-wait' : 'read', startedAt: at(4 + i * 0.12) });
  for (let i = 0; i < 10; i++) requests.push({ class: 'read-wait', startedAt: at(9.1 + i * 0.5) });
  return {
    startedAt,
    endedAt: startedAt + 60_000,
    scale: 1 / 15,
    subscriptions: SESSIONS,
    posts: [],
    seen: [],
    notices: [
      { scope: 'session-1', id: 'n1', t: at(3.2), rooms: [], chars: 100 },
      { scope: 'session-1', id: 'n2', t: at(5), rooms: [], chars: 100 },
      { scope: 'session-1', id: 'n3', t: at(8.9), rooms: [], chars: 100 },
      { scope: 'session-2', id: 'n4', t: at(12), rooms: [], chars: 100 },
    ],
    requests,
    waitNotHeld: 0,
    rateLimited: 0,
  };
}

describe('core bench report', () => {
  it('counts room reads overall and in the flood window, also per room-minute over the unique rooms', () => {
    const s = summarize(trace());
    expect(s.originRequests.total).toBe(61);
    expect(s.originRequests.readsPerMinute).toBe(60);
    // 40 reads over 20 real seconds of flood.
    expect(s.originRequests.floodReadsPerMinute).toBe(120);
    expect(s.originRequests.uniqueRooms).toBe(5);
    expect(s.originRequests.readsPerRoomMinute).toBe(12);
    expect(s.originRequests.floodReadsPerRoomMinute).toBe(24);
    expect(s.noticesInFlood).toBe(2);
  });

  it('prints the flood and per-room-minute read rows', () => {
    const md = markdown(summarize(trace()), { date: 'test' });
    expect(md).toContain('- room reads/min (all but /config): whole run 60, flood (minutes 4-9) 120');
    expect(md).toContain('- room reads per room-minute (5 unique rooms followed at once): whole run 12, flood 24');
    expect(md).toContain('during the flood: 2)');
  });
});
