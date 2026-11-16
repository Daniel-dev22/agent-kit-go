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
| coverage | eventoutbox 85.8% (new batch.go well above; legacy outbox.go lowers it), reconcile 97.2% |
| mutation eventoutbox (`cmd/mutate`, -j 2) | 317 generated, 33 invalid, 284 valid, 257 killed, 27 listed equivalent (`eventoutbox/testdata/survivors.txt`), 0 problems |
| mutation reconcile | 104 generated, 3 invalid, 101 valid, 94 killed, 7 listed equivalent (`reconcile/testdata/survivors.txt`) |
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

## Not measured / follow-ups
- Pi-class (CPU-pinned) numbers for the batch drain are NOT taken: it runs per event/pass, not per probe cycle, and was not benchmarked. The ECMP agent's slice C hand-off carries the end-to-end number.
- The outbox DB must be opened with `busy_timeout` and WAL like `New` (documented on `NewBatch`).
- `BatchOutbox.Stats()` / `LastSeq()` read `sqlite_sequence`; a non-SQLite driver would need its own equivalent.
