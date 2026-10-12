# agent-kit-go

Shared Go building blocks for **standalone agent services** — long-running daemons that sit on a
host, do something useful there (run backups, drive Docker, build images, sync files), and report
back to a central controller over HTTP.

```
go get github.com/Daniel-dev22/agent-kit-go
```

Every one of those agents needs the same unglamorous machinery: fire cron schedules locally so they
survive the controller being down, push job events without losing the terminal one, keep the
controller's view converged when a push *is* lost, fan a live snapshot out to dashboard
WebSockets, write files to disk in a way that survives a crash mid-write, and dial out through a
local reverse proxy. Writing that once per agent produced five subtly different versions of each,
and the differences were bugs. This module is where they live now.

**It is a toolbox, not a framework.** There is no `kit.Run()`, no lifecycle you must adopt, no
interface you must implement. Import the one package you need and ignore the rest — the packages
do not depend on each other, apart from `reconcile` using `jobstore`'s `JobRef` type.

## Design constraints

- **Library only.** No `main` package, no binary, no Dockerfile.
- **Standard library only, in the code that ships.** The single direct dependency,
  `modernc.org/sqlite`, is used *exclusively by tests* as a blank driver import. Nothing in the
  library imports it.
- **Driver-agnostic and schema-agnostic.** `jobstore` and `eventoutbox` take a caller-supplied
  `*sql.DB` and caller-supplied table names. They never open a database and never own one.
- **Transport-neutral where it can be.** `storage` has no HTTP, no OAuth, no Range/resume
  semantics — you drive the request and hand it an `io.Reader`.
- **Go 1.26**, because the test suite uses `testing/synctest` to run interval-loop tests against a
  virtual clock instead of real sleeps.

## Packages

| Package | What it gives you |
|---|---|
| [`scheduler`](scheduler/) | A per-agent cron scheduler. A 1-minute fire loop walks an in-memory schedule cache and calls your `FireFunc`; a 5-minute loop re-pulls the schedule list from the controller and replaces the cache, preserving `LastFiredAt`. An atomically-written JSON disk cache means a restart neither re-fires a schedule that already ran this minute nor floods with catch-up. Includes missed-run recovery policies and `Chain`, an after-wave primitive for ordering stages that must run once a burst of jobs drains. |
| [`storage`](storage/) | The crash-safe on-disk toolbox: `CopyToFileAtomic` (write to `.part` → fsync → rename → fsync the parent dir, hashing in-band so there is no second pass), `AtomicWriteJSON`, `MoveAtomic`, a resumable content-addressed `ChunkStage` with a bitmap sidecar that is fsynced *before* a chunk is marked received, a per-account `Manifest` index, `VersionLayout` rotation/prune, a `Graveyard` for soft deletes with retention, and path sanitisation. |
| [`jobstore`](jobstore/) | Two SQL helpers over your own schema: `MarkOrphanedFailed` (boot-time sweep for jobs a previous process left `running`) and `ListForReconcile` (the job snapshot the reconcile push sends). Table and column names are parameters. |
| [`eventoutbox`](eventoutbox/) | A durable, restart-surviving queue for outbound job events. `Enqueue` persists first and *then* pushes; a drain loop retries the backlog oldest-first. This exists because fire-and-forget POSTs lose the terminal event whenever the controller blinks, and a lost terminal event strands a central row in `running` forever with nothing to retry it. |
| [`reconcile`](reconcile/) | The level-triggered safety net that pairs with the outbox. On boot and every interval, POST the agent's full set of non-terminal and recently-finished jobs so the controller can converge its rows. Deltas give you latency; this gives you correctness. |
| [`fleet`](fleet/) | `Hub[T]` — a coalescing snapshot broadcaster. One builder goroutine per window, and each subscriber gets a **conflating** 1-slot mailbox: a slow consumer has its pending frame *replaced* by the newer one rather than the newer one being dropped. That matters when a snapshot is full replacing state — dropping the newest frame is exactly the wrong choice, and the drop-newest predecessor left dashboards stale for hours after a fast job's final frame was discarded. |
| [`transport`](transport/) | A pooled `*http.Transport` plus a constant-bearer round-tripper. Optionally rewrites the dial target to a local reverse-proxy container while keeping the URL authority as the `Host` header and TLS SNI — which is how an agent addresses the controller by its public name while the bytes go to a sidecar that attaches mutual TLS. |
| [`synckit`](synckit/) | The Go client half of a bidirectional last-write-wins sync protocol: one POST carries local dirty records up and everything newer than your cursor down, plus the conflicts your writes lost. `Wins()` is the same LWW rule the server applies, so a client can reason about conflicts identically. Deliberately does *not* own the apply-then-advance-cursor loop — that is yours, and it must only advance after a fully-applied pull. |
| [`logging`](logging/) | `Setup(level)` returns a JSON `slog` handler on stderr (text if `LOGUTIL_FORMAT=text`), plus a `Fatal` helper. |
| [`memlimit`](memlimit/) | `Apply(ctx, log)` resolves the process's own cgroup via `/proc/self/cgroup` (v2 `0::path`, or the v1 `memory` controller line), takes the tightest `memory.max` / `memory.limit_in_bytes` on the path up to the root, and sets `debug.SetMemoryLimit` to 85% of it, so the GC works harder before the kernel OOM-killer acts. Set `MEMLIMIT_RESERVE_BYTES` (or `Options.ReserveBytes`) to give child processes that share the cgroup room instead: the soft limit becomes limit minus reserve, never below 50%. The limit is re-read every 60s until `ctx` ends and re-applied (logged once) on change. An unlimited cgroup is a no-op with one warning; an explicit `GOMEMLIMIT` is respected. `ApplyWith` takes an injectable cgroup root, `/proc/self/cgroup` path, env lookup, setter and tick source for tests. `Apply` returns an error satisfying `errors.Is(err, memlimit.ErrInvalidConfig)` for a malformed `MEMLIMIT_RESERVE_BYTES` / `Options.ReserveBytes` or a cgroup limit too small to derive a limit from (needs a limit of 1 byte or less, in practice); treat that as fatal, since nothing is applied and no refresher runs. Any other error (an unreadable or unparsable cgroup file) is environmental and non-fatal: the refresher is already running and retries, so log it and continue. `StartPeakAnon(ctx, log, opts)` samples (default 5s) the anonymous memory of the process's own cgroup (v2 `anon`, v1 `total_rss`; page cache excluded) and exposes current and max-since-start via `Stats()` for health endpoints; it is the number to size a hard cap from, but `memory.max` also charges shared memory/tmpfs, page tables, kernel stacks, socket buffers and unreclaimable slab, so add margin on top of the observed peak. |

The environment variables the library reads are **`LOGUTIL_FORMAT`** (`logging`) and, in `memlimit`, **`GOMEMLIMIT`** (respected, never overwritten) and **`MEMLIMIT_RESERVE_BYTES`**. Everything else is passed as struct fields.

## Who uses it

| Agent | What it does | Uses |
|---|---|---|
| [duplicacy-agent-api](https://github.com/Daniel-dev22/duplicacy-agent-api) | Owns [duplicacy](https://github.com/gilbertchen/duplicacy) backups for one host — runs the CLI, parses its output into structured progress and stats, fires its own schedules | `scheduler`, `jobstore`, `eventoutbox`, `reconcile`, `fleet`, `transport`, `logging` |
| [docker-agent](https://github.com/Daniel-dev22/docker-agent) | Per-host Docker control plane: container lifecycle, Compose projects in-process via the Compose v5 SDK, image-update detection, health-gated stack updates with rollback | `jobstore`, `eventoutbox`, `reconcile`, `fleet`, `logging` |
| [gdrive-agent](https://github.com/Daniel-dev22/gdrive-agent) | Mirrors Google Drive accounts to local disk with resumable downloads, version history and a graveyard | `storage`, `scheduler`, `jobstore`, `eventoutbox`, `reconcile`, `fleet`, `transport`, `logging` |
| [filemesh-agent](https://github.com/Daniel-dev22/filemesh-agent) | Two-way file sync peer over an isolated vault — LWW metadata plus a resumable content-addressed chunk lane | `synckit`, `storage`, `transport`, `logging` |
| [build-agent](https://github.com/Daniel-dev22/build-agent) | Turns container image builds into trackable jobs driven by `docker buildx` | `jobstore`, `eventoutbox`, `reconcile`, `fleet`, `logging` |

Each agent pins its own version in `go.mod`; they are not required to move in lockstep.

## Versioning

Tagged releases, `v0.x.y`. Pre-1.0, so treat a minor bump as potentially breaking. A consumer
pins a specific version:

```
require github.com/Daniel-dev22/agent-kit-go v0.3.0
```

## Testing

```
go test ./...
```

Hermetic — no network, no build tags, no environment gates. 13 test files, 44 tests. `jobstore` and
`eventoutbox` run against a real in-memory SQLite via the pure-Go `modernc.org/sqlite` driver, so
the SQL is genuinely executed rather than mocked out.

`scheduler/synctest_example_test.go` is the reference for interval-loop testing: a 12-minute window
of a 5-minute reconcile loop runs in under a millisecond of virtual time. Note the pattern it
demonstrates — the HTTP response is served by an in-process `RoundTripper`, **not** an
`httptest.Server`. A synctest bubble only advances its clock once every goroutine in it is
*durably blocked*, and a goroutine parked on a real socket read never qualifies, so a real server
inside a bubble hangs the whole package until the test binary times out.

`logging` is the one package with no tests.

## License

MIT — see [LICENSE](LICENSE).
