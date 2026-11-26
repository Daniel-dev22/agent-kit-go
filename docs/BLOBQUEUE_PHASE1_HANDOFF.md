# blobqueue Phase 1 stage 1 hand-off (otel-w2, 2026-11-26)

Branch `feat/blobqueue` (kit worktree `/docker_container_volumes/agent-repos/agent-kit-go-otel`). Design: `BLOBQUEUE_DESIGN.md`.

## Decision
New sibling package `blobqueue`, NOT an extension of `eventoutbox.BatchOutbox` (advisor-confirmed). BatchOutbox is
JSON-object-only with supersede/bisect/hold semantics built for four consumers; carrying binary through it means
base64 (+33% bytes). `blobqueue` owns a length-prefixed binary frame (encoder AND decoder, so otel-ingest imports
the same code). stdlib only: `go.mod`, `go.sum` and `eventoutbox/` are byte-identical to `origin/main`
(`git diff --stat origin/main -- go.mod go.sum eventoutbox` is empty).

## API (package `blobqueue`)
`New(db *sql.DB, Config) (*Queue, error)` | `Enqueue(ctx, Item) (seq uint64, error)` (returns after commit) |
`Run(ctx, Sender) error` (drain loop, backoff+jitter) | `Maintain(ctx)` | `Stats(ctx) Stats` | `Close()` | `Epoch()`.
`EncodeFrame / DecodeFrame(frame, Limits)` (versioned, bounded, fuzzed). `Sender func(ctx, frame) (Ack, error)`;
`Ack{Accepted, Duplicate, Rejected[{Seq,Code,Retryable}]}`; unlisted items count as delivered; non-retryable
rejections are dead-lettered (`OnDead`); a retryable one holds that row. Errors: `ErrTooLarge`, `ErrDiskFloor`
(map to HTTP 503), `ErrClosed`, `ErrNotWAL`. Loss marker: Signal `loss`, JSON `LossMarker`.

## What it guarantees (each has a test)
- Persist-before-return; seq is AUTOINCREMENT and never reused after a drain (receiver dedupes on (epoch, seq)).
- Bounds, oldest first, every shed counted: max age, max rows, max logical bytes, free-disk floor (stricter of
  percent and bytes, `Bavail` via injectable statfs). A floor breach sheds, then `incremental_vacuum` +
  `wal_checkpoint(TRUNCATE)` so the file really shrinks (test asserts size drops); when nothing is left to shed it
  returns `ErrDiskFloor` and counts the refusal. Unmeasurable statfs fails OPEN and is surfaced in `Stats.LastError`.
- One coalesced, protected loss marker row (rows, bytes, oldest/newest ts, seq range, per-reason counts, refusals),
  updated in the same transaction as the delete; an in-flight marker is never updated (a second is inserted).
- Rows the drainer holds in flight are never shed. `MaxRows` counts the marker row (transiently cap+1).
- `New` reads `journal_mode` / `synchronous` back: refuses non-WAL, reports `Durability` full|normal|weak.
  kill -9 survives WAL+NORMAL; power loss needs `synchronous=FULL` in the DSN (the caller owns the DB).

## Tests (targeted, `-race`)
32 tests (ack validation is a 10-case table) + 1 fuzz target + 4 benchmarks; all pass; statement coverage 86.8%. Crash tests re-exec the test binary and
SIGKILL it mid-enqueue (3 cycles, every acked seq present, integrity_check ok, seq monotonic) and mid-drain (nothing
lost, redelivery confined to one in-flight batch). Fuzz `FuzzDecodeFrame`: 1,442,547 execs in 30 s, no failure; any
frame that decodes re-encodes byte-identically. Mutation check `blobqueue/mutation_check.py`: 18/18 mutants killed
(caps, age cutoff, floor comparison, stricter-floor-wins, refuse-when-empty, protected rows, in-flight mark, shed
target, refusal fold-in, sent-marker rewrite, removed incremental_vacuum, auto_vacuum off, partial/inverted ack check,
uncounted statfs error). Concurrency: 8 producers + the drainer, every (epoch, seq) delivered exactly once.
Advisor round fixes (each with a test that fails on the old behaviour): (1) a loss marker already handed to the
Sender (`sent=1`) is never rewritten, because a deduping receiver would drop the update; later loss is a new marker
and the receiver SUMS markers (test uses a deduping receiver that stores then errors); (2) `Ack.Accepted/Duplicate`
are pointers and the batch is held unless the counts account for every item (an empty 2xx deletes nothing);
(3) the floor test now asserts the MAIN file shrinks and `freelist_count`==0 (a WAL truncation alone cannot pass),
and `auto_vacuum`==2 is asserted on a fresh and on a pre-existing file (pinned `sql.Conn`); (4) `Stats.StatfsErrs`
is a sticky counter so a silently disabled floor stays visible.
Opus round 1 (frozen 4cde1cc; report otel-w2-review-r1.md): 1 HIGH, 1 MED, 7 LOW, no silent-loss path found. Fixed:
HIGH `New` held a pinned conn while querying through the pool and hung forever under `SetMaxOpenConns(1)` (the fleet's
setting): now released in `prepareFile`, with a one-connection-pool life-cycle test (incl. floor shed); MED
`incremental_vacuum` yields one row per page so it is read via Query and drained, and the checkpoint's busy flag is
checked; LOW vacuum/checkpoint failures counted in sticky `Stats.ReclaimErrs`; second `Run` refused (`ErrRunning`); a
refused loss marker's body is handed to `OnDead` (`DeadLetter.Body`); test asserts the frame epoch. 37 tests, 21/21
mutants, coverage 87.4%, fuzz smoke 648k execs clean. DEFERRED with reason (design choices, LOW, need weeks of outage):
loss markers have no own bound (cap = a policy choice between durability and size; stage 2 decides with the receiver);
age shedding trusts the wall clock (a Pi clock step mis-sheds, counted as `age`, never silent; fixing needs a monotonic
or boot-id stamp, a schema change). Hot paths (Enqueue/drain) were not touched, so the Pi numbers above stand.
What resets on restart: the in-memory shed/refused/sent/dead counters and the pending refusal count. The durable
record of loss is the marker row (totals are re-derived at `New`).

## Benchmarks, Pi class (measured)
Pi: aarch64, 4 cores, MemAvailable 3.46 GiB at start, `/var/tmp` on **a USB-attached SSD (sda, ROTA 0)**, which is
the Pi-class storage (user rule 2026-11-26: the Pis run on SSD, never SD cards; fsync ~4.5 ms), GOMAXPROCS=2, `systemd-run` unit
MemoryMax=1G CPUQuota=200% AllowedCPUs=2-3 RuntimeMaxSec capped, 300 bodies x count 5 (drain: 1000 rows x 3), medians.
"Before" = `eventoutbox.BatchOutbox` carrying the same body base64-wrapped in a JSON object (the rejected option).
Bodies are incompressible random bytes (like gzip).

| enqueue, FULL (durable) | bodies/s | p50 ms | p99 ms | disk B/body | CPU ms/1k | RSS MiB |
|---|---|---|---|---|---|---|
| 1 KiB   blobqueue / before | 194 / 174 | 4.7 / 5.0 | 10.7 / 12.8 | 16411 / 16425 | 1163 / 1023 | 14 / 21 |
| 16 KiB  blobqueue / before | 173 / 142 | 5.1 / 6.1 | 28 / 57 | 51555 / 56866 | 1687 / 1833 | 18 / 22 |
| 64 KiB  blobqueue / before | 134 / 104 | 6.0 / 7.9 | 40 / 61 | 153614 / 189645 | 2663 / 3910 | 21 / 33 |

| enqueue, NORMAL | bodies/s | p50 ms | p99 ms | CPU ms/1k |
|---|---|---|---|---|
| 1 KiB  blobqueue / before | 2582 / 3033 | 0.37 / 0.29 | 0.9 / 0.8 | 387 / 327 |
| 16 KiB | 731 / 437 | 1.0 / 1.3 | 5.2 / 8.9 | 1153 / 1477 |
| 64 KiB | 351 / 132 | 1.5 / 2.8 | 58 / 167 | 2093 / 3427 |

Re-run on the Pi AFTER the advisor fixes (FULL, count 5, medians; blobqueue / before): 16 KiB **170 / 169** bodies/s
(parity; the baseline itself moved 142 -> 169 between runs), p99 28 / 33 ms, CPU 1773 / 1923 ms/1k; 64 KiB **142 / 117**
bodies/s, p99 42 / 47 ms, disk 153614 / 189645 B, CPU 2567 / 3880 ms/1k. No regression from the hardening. Honest
reading: FULL enqueue is fsync-bound, so throughput is parity at <=16 KiB; the real wins are bytes and CPU at 64 KiB.

Drain: NOT like for like. The blobqueue row calls `drainOnce` in-process with an ack-all sender; the baseline goes
through HTTP + JSON with a 1 ms sleep-poll. Do not quote the ratio as a headline; it only shows the binary path costs
no more than the JSON one. Post-fix Pi drain, blobqueue / before: 1 KiB 8807 / 3774, 16 KiB 2592 / 442, 64 KiB 1126 / 148 bodies/s.

| drain (ack-all sender, first Pi run) | bodies/s | CPU ms/1k | RSS MiB |
|---|---|---|---|
| 1 KiB  blobqueue / before | 5812 / 3271 | 93 / 269 | 20 / 46 |
| 16 KiB | 2350 / 396 | 340 / 2301 | 28 / 46 |
| 64 KiB | 1193 / 136 | 749 / 6727 | 46 / 76 |

- Op count: every enqueue chunk asserts rows written == bodies enqueued with no shed/marker row (passes). `strace -f
  -c -e trace=fsync` on the Pi, 16 KiB FULL, 301 enqueues: **329 fsync calls** = ~1 per Enqueue plus schema/purge setup.
- Write amplification at FULL: 1 KiB body writes ~16 KiB (4 pages: row, sqlite_sequence, WAL); large bodies ~2.4x (WAL
  + checkpoint copy). Equal to the baseline at 1 KiB, 19% less at 64 KiB (no base64).
- The durable cost floor is one fsync (~4.5 ms on this USB SSD) per body: 134-194 bodies/s per writer, i.e. above the
  brief's 10-100 bodies/s ceiling at every size here (64 KiB FULL: 1.4x). **Group commit was NOT built** on that
  measurement, on the Pi-class (SSD) storage the fleet uses. If a node class ever measures under 2x its real rate,
  build it (concurrent `Enqueue` callers share one transaction, no API change) and re-benchmark first.
- Variance caveat: Pi NORMAL runs spread up to 2.8x run to run (1 KiB NORMAL: 1018..2815 bodies/s), and a first run of
  a fresh process on the dev host measured 7.3 ms/enqueue vs 1.5 ms on rerun. The 1 KiB NORMAL "-15%" vs the baseline is
  inside that noise and is not claimed as a regression. No threshold was changed; no fix was needed from the numbers.

## Consumer impact
None. `eventoutbox` is untouched (no behaviour, schema, or golden-file change), so docker-agent, duplicacy-agent-api,
build-agent, gdrive-agent and the router batch handler are unaffected. No consumer repo was changed. The new package has
no consumer yet; otel-agent and otel-ingest import it in stage 2. No kit tag was cut (needs a relayed go).

## Left for stage 2
otel-agent HTTP accept on `/v1/{logs,metrics,traces}` mapping `ErrDiskFloor` to 503 and the body to an `Item`
(signal, content-type, `Content-Encoding: gzip` kept); config file; the forwarder `Sender` (bearer + local-Traefik
mTLS dial, classify refusals by body `code`, never status alone); loss-marker handling and the `RequestCount`-gap loss
detector at otel-ingest; Overview wiring and queue-depth/age metrics from `Stats`; pin the kit version in otel-agent
and re-run the closure test there. Open: `synchronous=FULL` must be in the agent's DSN (the queue only reports it).
