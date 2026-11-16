# agent-kit-go: batch drain + fingerprint-gated reconcile (hand-off)

Branch `feat/batch-gated-reporting` (worktree `/docker_container_volumes/agent-repos/agent-kit-go-batch`), commit `9a65eef`. Local only: no push, tag or merge. Stdlib only: `go.mod`/`go.sum` unchanged (frozen closure).

## What was added (additive, opt-in)
- `eventoutbox.NewBatch` / `BatchOutbox` (`eventoutbox/batch.go`): own `batch_events` + `batch_meta` tables; AUTOINCREMENT id = seq (never reused); epoch created with the schema in one transaction; ONE drainer, `Enqueue` pokes it. Body `{<envelope>,"events":[{"seq":N,...}]}`, <= 100 events and <= 256 KiB.
- `reconcile.NewGated` / `Gate` (`reconcile/gated.go`): caller-supplied fingerprint, full + heartbeat bodies; kit never inspects bodies. Heartbeat carries the fingerprint; fail-open = send full.
- Unchanged callers: `New`/`Enqueue`/`push` and `Run` are untouched; `TestGoldenWireEnqueuePush` and `TestGoldenWireRunSnapshot` pin the bytes.

## Semantics decided (wire contract: control-center-ecmp `docs/ECMP_STATE_CONTRACT.md` v1.1)
- Success = 2xx with `accepted + duplicate + len(rejected) == len(batch)`; delete exactly those ids in one transaction. Unparseable body, missing counts, shortfall, or a rejected seq outside the batch keeps every row.
- `rejected[]` = `{seq, code, retryable}`: terminal rows are dead-lettered (`OnDead`, counter, loud log), retryable rows stay.
- 413 and a non-retryable whole-request 4xx BISECT (the contract says dead-letter the batch; bisecting only ever dead-letters the offending event, so it is compatible and loses nothing good). A batch of one is dead-lettered. 408/429/5xx/network: capped exponential backoff with full jitter; a poke never cuts a backoff short.
- Bounds: age and rows drop-oldest on unprotected events; `protected` (recovery) events fall only to `HardRows`, loudly.
- Gate: `Interval` clamped to `MaxSilence`; waits jitter DOWN only (x[0.9,1.0]); first report at a random offset in [0, Interval); `NoteSent`/`OnSent` let event traffic count toward the bound; `want_snapshot:true` or an unconfirmed full forces the next full.

## Numbers
| gate | result |
|---|---|
| `go vet ./...`, `go test -race ./eventoutbox ./reconcile` | clean |
| coverage (package floor 95) | eventoutbox 95.2% (batch.go 95.6%, outbox.go 93.2%), reconcile 95.9% (gated.go 99.0%, reconcile.go 89.4%); `go test -coverprofile`, statement-weighted per file |
| single connection | `KIT_SINGLE_CONN=1 go test -race ./eventoutbox` (db.SetMaxOpenConns(1), the agent's one-connection mode): ok, no nested-query deadlock |
| mutation eventoutbox (`cmd/mutate`, -j 2) | 317 generated, 33 invalid, 284 valid, 257 killed, 27 listed equivalent (`eventoutbox/testdata/survivors.txt`), 0 problems |
| mutation reconcile (re-verified after removing a stale entry) | 104 generated, 3 invalid, 101 valid, 94 killed, 7 listed equivalent (`reconcile/testdata/survivors.txt`), 0 problems |
| mutation command | `scripts/capped.sh -m 2G -- go -C <ecmp-health-agent worktree> run ./cmd/mutate -dir <kit> -pkg eventoutbox\|reconcile -j 2 -timeout 90s -report <file>` (harness lives in the ecmp-health-agent repo; survivors files are keyed `file:line:col`, so any edit of a mutated .go file needs a re-run) |
| peak memory | race tests 95-193 MB; vet/build 816 MB; mutation per run under the 2 GB cap |

## Consumer repos against this kit (go.work, capped 3G, one at a time, `go test -count=1 ./...`)
| repo | result | peak |
|---|---|---|
| duplicacy-agent-api | ok | 1.35 GB |
| docker-agent | ok | 2.07 GB |
| gdrive-agent | ok | 0.88 GB |
| filemesh-agent | ok | 0.20 GB |
| build-agent | 7 tests fail IDENTICALLY with its own pinned kit v0.5.1 and `GOWORK=off` (`git tag v1` -> `fatal: no tag message`: the sandbox git needs a tag message/editor); no kit involvement | 0.12 GB |

## Frozen closure the ECMP agent's policy test must pin (kit go.mod at this commit)
golang.org/x/sys v0.42.0, modernc.org/sqlite v1.50.0, modernc.org/libc v1.72.0, modernc.org/mathutil v1.7.1, modernc.org/memory v1.11.0, github.com/dustin/go-humanize v1.0.1, github.com/google/uuid v1.6.0, github.com/mattn/go-isatty v0.0.20, github.com/ncruces/go-strftime v1.0.0, github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec. Unchanged by this commit.

## Pi-class numbers (taskset -c 3, GOMAXPROCS=1, -count 10, i5-1340P = smoke alarm, gate = allocs/op and POST count)
Min ns/op over 10 runs, WAL file DB on /var/tmp (synchronous=NORMAL, one connection), in-process RoundTripper (no socket):
| bench | ns/op | B/op | allocs/op |
|---|---|---|---|
| BatchEnqueue (1 INSERT + poke) | 16186 | 1001 | 22 |
| BatchBuild100 (stamp + encode 100 events) | 12526 | 33425 | 24 |
| BatchDrain100 (load 100, build, 1 POST, delete 100) | 429545 | 119025 | 1569 |
| LegacyEnqueuePush100 (before: 100 Enqueue + 100 POSTs) | 7746875 | 270631 | 4229 |
| GateDecide | 15.2 | 0 | 0 |
| GatedTickSkip (unchanged fingerprint, inside bound, no POST) | 52.3 | 4 | 2 |
Op counts asserted inside the benchmarks: BatchDrain100 = exactly 1 POST per 100 events (legacy = 100), GatedTickSkip = 0 POSTs after the first full. 100 events cost about 100 x 16 us + 430 us = 2.0 ms batched vs 7.75 ms legacy (3.9x less CPU, 100x fewer requests). The NAS-pool fsync decision (NORMAL vs FULL) is slice C's measurement, not made here.

## Uncovered branches (each is a defensive error path with no cheap trigger)
batch.go: crypto/rand.Read failure (217), tx Commit failure after a successful schema (227), scan errors on a driver row (415, 551, 588-599), delete-statement prepare/exec failure inside a transaction (613-627), prune query failure (686). outbox.go (legacy, untouched): CREATE failure after a successful open (73), delete-after-push warn (118), per-row scan failure (174-176), ctx.Done inside the push loop after a successful query (187). Each logs and backs off; none changes a delivery decision.

## Not measured / follow-ups
- The outbox DB must be opened with `busy_timeout` and WAL like `New` (documented on `NewBatch`).
- `BatchOutbox.Stats()` / `LastSeq()` read `sqlite_sequence`; a non-SQLite driver would need its own equivalent.
