# technocore-watch-core benchmark (2026-09-14)

Unofficial community integration — not affiliated with or endorsed by FLOP Labs. Local server only.

- **date**: 2026-09-14
- **commit**: bfbdaa6
- **scale**: 1 (15 workload minutes)
- **runs**: 1
- **hardware**: Apple M5 x10, 16 GiB, darwin 25.6.0
- **node**: v26.0.0
- **technocore**: technocore-chat v0.13.0 @20a4457 (local, CHAT_WAIT_POLL=0.5, CHAT_RATE_READ=600, CHAT_MAX_WAITERS_PER_IP=4)
- **coalescer**: quiet 2 s / max 7 s / min wake 8 s (defaults)
- **pool**: defaults: 3 long-poll slots, wait 10 s, sweep >= 15 s, flood re-poll pacing on (lead 500 ms, min nap 1000 ms)
- **workload**: W1 without harness: 2 scopes x 3 subs (1 shared), idle, 8-burst, 5 msg/s flood, 45 s trickle, restart, singles

## Wake latency (post 200 -> coalesced notice), ms

| events | n | p50 | p95 | max |
|---|---|---|---|---|
| all | 1525 | 4430 | 8088 | 8679 |
| trickle | 19 | 2206 | 8167 | 8167 |
| burst | 16 | 2061 | 2147 | 2147 |
| flood | 1487 | 4515 | 8088 | 8494 |
| single | 3 | 4323 | 8679 | 8679 |
| seen by poll (stage) | 1525 | 3336 | 7088 | 7497 |

## Origin requests

- total: 474 over 15.58 min
- per minute by class: {"read":8.21,"read-wait":22.14,"config":0.06}
- idle phase requests/min: 26.4
- room reads/min (all but /config): whole run 30.35, flood (minutes 4-9) 37.2
- room reads per room-minute (5 unique rooms followed at once): whole run 6.07, flood 7.44
- 429 responses: 0; wait_held:false replies: 5

## Notices

- notices: 54 (after restart: 9; during the flood: 37)
- notice size chars p50/p95/max: 576/701/701
- duplicate notified seqs (target 0): 0
- posts never covered by a notice: 0 of 1517
