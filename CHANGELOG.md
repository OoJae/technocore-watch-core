# Changelog

## 0.1.0 — unreleased

First version. Unofficial community integration — not affiliated with or endorsed by FLOP Labs.

- `protocol/`: room names and classes, bigint-safe read view and export parsing, 429 body parsing,
  the single-line sweep, `reconcile` with the delivered-or-gapped invariant, fixed untrusted rendering.
- `transport/`: read-only `TechnocoreReader` (room read, streaming bounded export scan, `/config`).
- `limits/`: rate governor seeded from `/config`, waiter semaphore, decorrelated jitter.
- `state/`: `FileStateStore` (atomic writes, revision compare-and-set, poller lock, notice ring) and
  `MemoryStateStore`.
- `engine/`: `HostPool` (one upstream poll per room, <= 3 long-polls per origin, sweeps for overflow,
  export backfill), coalescer, `createWatcher()`.
- CLI `technocore-watch run|monitor|subscribe|unsubscribe|status|inbox`.
- Tests: unit, fast-check properties, fake-time engine tests, 12 integration scenarios against a local
  technocore-chat v0.13.0; W1 benchmark harness.

### Fixed after independent review (still 0.1.0, unreleased)

- A recreation or regression whose export failed once was never processed afterwards: the poller
  remembered the new generation as handled. Empty views are now forwarded to every subscription
  that has not settled the generation they report (also covers a scope restarting with an older
  stored generation in a shared pool).
- A lower origin generation (data root reset or restored) left the subscription stuck and polled in a
  tight loop. It now backs off, emits `stale-generation`, and after 3 observations plus a confirming
  cache-busted read resyncs as a restart (`recreated` + `regression`).
- `startFrom: 'now'` on an empty view (reaped room with a floor, fully expired `e-` room) reported the
  whole history as a `ring-dropped` gap on the first post. The first records now settle the start.
- `boundGaps` merged unrelated gaps and kept the first reason, hiding delivered and still-retained
  seqs. It now merges only touching gaps with the same reason and drops whole records otherwise.
- `monitor` re-printed the whole retained journal after every ring rewrite. It now resumes after the
  last printed notice id.
- The poller lock could be taken from a live process whose heartbeat was old, and the holder never
  noticed. Same-host live pids are no longer stale (pid reuse detected with `ps -o lstart=`), and a
  holder that loses the lock stops polling (`run` exits 3).
- `NodeHttpTransport.close()` resolved before its sockets closed, so handles could outlive `dispose()`.
- The CLI auto-ran inside any wrapper whose path ended in `technocore-watch`. It now compares real paths.
- `limits.noticeMaxChars` was ignored. It now drives `watcher.renderNotice()` and the CLI (`--notice-max-chars`).
- `readPage`'s `maxPageChars` counted raw UTF-16 units. It now counts the escaped output.
- The pool message cache kept rooms forever. Rooms are dropped when their last subscriber leaves and
  capped at 64 (LRU).
- The dispose leak check now runs a plain-ESM child against `dist/`, so it no longer needs TypeScript
  stripping (Node 20). CI matrix: Node 20, 22, 24.
- The secret scan also flags 32-byte seeds written as base64url/base64.

### Tuning pass (still 0.1.0, unreleased)

- Flood re-poll pacing (`limits.floodRepollPacing`, default on; `repollLeadMs` 500, `floodRepollMinMs`
  1,000; CLI `--flood-pacing on|off`, `--repoll-lead-ms`, `--flood-repoll-min-ms`). A room whose every
  subscribing scope already has a notice pending no longer re-polls after each message: it naps until
  just before the earliest of those notices is due (capped at the long-poll wait) and is woken early by
  a new subscriber, a slot change, a flushed or dropped notice, unsubscribe or dispose. It does not nap
  when the due time is still being pushed out by `quietMs` and is closer than lead + floor, nor for
  longer than half a read page takes to fill at the measured rate. Against a local server (integration
  scenario 13) a 15 s flood at 20 msg/s cost 8 room reads (296 before this change), with no seq lost,
  and single messages outside the flood were still seen by the poll in under 60 ms. Fake-time unit
  tests: a 60 s flood at 5 msg/s costs 17 reads instead of 300. New `poll-paced` trace event. `RoomMember` gains `nextNoticeAt()`.
- Pacing limits are validated (`RangeError`/`TypeError` from `HostPool` and `createWatcher`); `limits`
  keys set to `undefined` keep their defaults instead of erasing them. `PACING_BOUNDS` and
  `validateLimits` are exported.
- A wake that arrived before a poll started (the slot assignment on attach) used to skip the next nap
  after it; a poll now clears it, so only wakes that land during a poll skip the following nap.
