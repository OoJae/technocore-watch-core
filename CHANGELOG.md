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
