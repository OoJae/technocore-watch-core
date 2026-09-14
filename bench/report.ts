// Turns a benchmark trace into the numbers #765 asks for: wake latency (post -> notice),
// origin requests per minute by class, 429s, wait_held:false replies, notices, duplicates.

export interface PostTrace {
  room: string;
  seq: number;
  kind: string;
  tPostOk: number;
}

export interface SeenTrace {
  scope: string;
  room: string;
  from: number;
  to: number;
  t: number;
}

export interface NoticeTrace {
  scope: string;
  id: string;
  t: number;
  rooms: { room: string; seqFrom: number; seqTo: number; newCount: number; generation?: number }[];
  chars: number;
}

export interface RequestTrace {
  class: string;
  startedAt: number;
  status?: number;
}

export interface BenchTrace {
  startedAt: number;
  endedAt: number;
  scale: number;
  subscriptions: Record<string, string[]>;
  posts: PostTrace[];
  seen: SeenTrace[];
  notices: NoticeTrace[];
  requests: RequestTrace[];
  waitNotHeld: number;
  rateLimited: number;
  restartAt?: number;
}

export function percentile(values: number[], p: number): number | null {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  const idx = Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1));
  return s[idx]!;
}

function stats(values: number[]) {
  return { n: values.length, p50: percentile(values, 50), p95: percentile(values, 95), max: values.length ? Math.max(...values) : null };
}

export function summarize(trace: BenchTrace) {
  const minutes = (trace.endedAt - trace.startedAt) / 60_000;
  const wake: Record<string, number[]> = {};
  const seenLat: number[] = [];
  const all: number[] = [];
  let unnotified = 0;
  for (const p of trace.posts) {
    for (const [scope, rooms] of Object.entries(trace.subscriptions)) {
      if (!rooms.includes(p.room)) continue;
      const seen = trace.seen.find((s) => s.scope === scope && s.room === p.room && s.from < p.seq && p.seq <= s.to && s.t >= p.tPostOk - 5);
      if (seen) seenLat.push(Math.max(0, seen.t - p.tPostOk));
      const n = trace.notices.find((x) => x.scope === scope && x.t >= p.tPostOk - 5 && x.rooms.some((r) => r.room === p.room && r.seqFrom <= p.seq && p.seq <= r.seqTo));
      if (!n) {
        unnotified++;
        continue;
      }
      const lat = Math.max(0, n.t - p.tPostOk);
      (wake[p.kind] ??= []).push(lat);
      all.push(lat);
    }
  }
  // Duplicate = a (scope, room, seq) covered by more than one notice.
  let duplicates = 0;
  const covered = new Map<string, number>();
  for (const n of trace.notices) {
    for (const r of n.rooms) {
      for (let s = r.seqFrom; s <= r.seqTo; s++) {
        const k = `${n.scope}|${r.room}|${r.generation ?? ''}|${s}`;
        covered.set(k, (covered.get(k) ?? 0) + 1);
      }
    }
  }
  for (const v of covered.values()) if (v > 1) duplicates++;
  const byClass: Record<string, number> = {};
  for (const r of trace.requests) byClass[r.class] = (byClass[r.class] ?? 0) + 1;
  const perMinute = Object.fromEntries(Object.entries(byClass).map(([k, v]) => [k, Number((v / minutes).toFixed(2))]));
  const idleWindowEnd = trace.startedAt + 3 * 60_000 * trace.scale;
  const idleReq = trace.requests.filter((r) => r.startedAt >= trace.startedAt + 5000 && r.startedAt < idleWindowEnd && r.class !== 'config');
  const idleMinutes = Math.max(1e-9, (idleWindowEnd - trace.startedAt - 5000) / 60_000);
  const afterRestart = trace.restartAt ? trace.notices.filter((n) => n.t >= trace.restartAt!) : [];
  // Room reads (everything but /config) overall and in the flood (workload minutes 4-9), also per
  // room-minute over the unique rooms the pool follows at once (a shared room is read once for all scopes).
  const reads = trace.requests.filter((r) => r.class !== 'config');
  const floodFrom = trace.startedAt + 4 * 60_000 * trace.scale;
  const floodTo = trace.startedAt + 9 * 60_000 * trace.scale;
  const floodReads = reads.filter((r) => r.startedAt >= floodFrom && r.startedAt < floodTo).length;
  const floodMinutes = Math.max(1e-9, (floodTo - floodFrom) / 60_000);
  const uniqueRooms = new Set(Object.values(trace.subscriptions).flat()).size;
  return {
    minutes: Number(minutes.toFixed(2)),
    wakeLatencyMs: { all: stats(all), byKind: Object.fromEntries(Object.entries(wake).map(([k, v]) => [k, stats(v)])) },
    seenLatencyMs: stats(seenLat),
    originRequests: {
      total: trace.requests.length,
      byClass,
      perMinute,
      idlePerMinute: Number((idleReq.length / idleMinutes).toFixed(2)),
      readsPerMinute: Number((reads.length / Math.max(1e-9, minutes)).toFixed(2)),
      floodReadsPerMinute: Number((floodReads / floodMinutes).toFixed(2)),
      uniqueRooms,
      readsPerRoomMinute: Number((reads.length / Math.max(1e-9, minutes * uniqueRooms)).toFixed(2)),
      floodReadsPerRoomMinute: Number((floodReads / (floodMinutes * Math.max(1, uniqueRooms))).toFixed(2)),
    },
    noticesInFlood: trace.notices.filter((n) => n.t >= floodFrom && n.t < floodTo).length,
    status429: trace.requests.filter((r) => r.status === 429).length,
    waitNotHeld: trace.waitNotHeld,
    rateLimitedEvents: trace.rateLimited,
    notices: trace.notices.length,
    noticesAfterRestart: afterRestart.length,
    noticeChars: stats(trace.notices.map((n) => n.chars)),
    duplicateNotifiedSeqs: duplicates,
    postsUnnotified: unnotified,
    posts: trace.posts.length,
  };
}

export function markdown(summary: ReturnType<typeof summarize>, meta: Record<string, string>): string {
  const row = (name: string, s: { n: number; p50: number | null; p95: number | null; max: number | null }) => `| ${name} | ${s.n} | ${s.p50 ?? '-'} | ${s.p95 ?? '-'} | ${s.max ?? '-'} |`;
  const lines = [
    `# technocore-watch-core benchmark (${meta.date})`,
    '',
    'Unofficial community integration — not affiliated with or endorsed by FLOP Labs. Local server only.',
    '',
    ...Object.entries(meta).map(([k, v]) => `- **${k}**: ${v}`),
    '',
    '## Wake latency (post 200 -> coalesced notice), ms',
    '',
    '| events | n | p50 | p95 | max |',
    '|---|---|---|---|---|',
    row('all', summary.wakeLatencyMs.all),
    ...Object.entries(summary.wakeLatencyMs.byKind).map(([k, v]) => row(k, v)),
    row('seen by poll (stage)', summary.seenLatencyMs),
    '',
    '## Origin requests',
    '',
    `- total: ${summary.originRequests.total} over ${summary.minutes} min`,
    `- per minute by class: ${JSON.stringify(summary.originRequests.perMinute)}`,
    `- idle phase requests/min: ${summary.originRequests.idlePerMinute}`,
    `- room reads/min (all but /config): whole run ${summary.originRequests.readsPerMinute}, flood (minutes 4-9) ${summary.originRequests.floodReadsPerMinute}`,
    `- room reads per room-minute (${summary.originRequests.uniqueRooms} unique rooms followed at once): whole run ${summary.originRequests.readsPerRoomMinute}, flood ${summary.originRequests.floodReadsPerRoomMinute}`,
    `- 429 responses: ${summary.status429}; wait_held:false replies: ${summary.waitNotHeld}`,
    '',
    '## Notices',
    '',
    `- notices: ${summary.notices} (after restart: ${summary.noticesAfterRestart}; during the flood: ${summary.noticesInFlood})`,
    `- notice size chars p50/p95/max: ${summary.noticeChars.p50}/${summary.noticeChars.p95}/${summary.noticeChars.max}`,
    `- duplicate notified seqs (target 0): ${summary.duplicateNotifiedSeqs}`,
    `- posts never covered by a notice: ${summary.postsUnnotified} of ${summary.posts}`,
    '',
  ];
  return lines.join('\n');
}
