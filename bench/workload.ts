// Workload W1 (design §6.2) for the core, without an agent harness. Times are in workload
// minutes and scaled by `scale` (1 = the full 15-minute run). Rates (5 msg/s flood) are not.

export interface WorkloadEvent {
  atMin: number;
  kind: 'burst' | 'flood-start' | 'flood-stop' | 'trickle' | 'restart' | 'single';
  room: string;
  count?: number;
}

export const ROOMS = { A: 'bench-a', B: 'bench-b', C: 'bench-c', D: 'bench-d', E: 'bench-e' } as const;

/** Two sessions, three subscriptions each, one room (A) shared. */
export const SESSIONS: Record<string, string[]> = {
  'session-1': [ROOMS.A, ROOMS.B, ROOMS.C],
  'session-2': [ROOMS.A, ROOMS.D, ROOMS.E],
};

export const DURATION_MIN = 15;
export const FLOOD_PER_SECOND = 5;

export function workloadW1(): WorkloadEvent[] {
  const ev: WorkloadEvent[] = [];
  ev.push({ atMin: 3, kind: 'burst', room: ROOMS.A, count: 8 });
  ev.push({ atMin: 4, kind: 'flood-start', room: ROOMS.B });
  ev.push({ atMin: 9, kind: 'flood-stop', room: ROOMS.B });
  for (let t = 0.75; t < DURATION_MIN; t += 0.75) ev.push({ atMin: Number(t.toFixed(2)), kind: 'trickle', room: ROOMS.C });
  ev.push({ atMin: 10, kind: 'restart', room: '' });
  ev.push({ atMin: 11.5, kind: 'single', room: ROOMS.D });
  ev.push({ atMin: 12.75, kind: 'single', room: ROOMS.E });
  ev.push({ atMin: 14, kind: 'single', room: ROOMS.C });
  return ev.sort((a, b) => a.atMin - b.atMin);
}
