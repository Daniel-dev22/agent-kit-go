# blobqueue: durable, binary-safe, acknowledged outbound queue (design)

Status: design, 2026-11-26 (otel-w2, Phase 1 stage 1). Plan: TRAEFIK_OTLP_PLAN.md s4.1, s5, s5.1, s11.

## 1. Placement decision (advisor-confirmed): new sibling package `blobqueue`

`eventoutbox.BatchOutbox` cannot carry an opaque body without change: JSON objects only (`ErrBadPayload`), a
`map[string]any` request envelope, and supersede / bisect-by-code / hold-on-refusal semantics built for the
job-event contract of four consumers (docker-agent, duplicacy-agent-api, build-agent, gdrive-agent) and the
router batch handler. Carrying binary through it means base64 (+33% bytes on SD writes and on the wire).

Therefore: `blobqueue` is a new package with its own wire frame, spoken only by otel-agent and otel-ingest.
`BatchOutbox` is not touched: its state machine, tests and golden files are byte-identical. Only a pure
backoff/jitter function may later be shared, as its own revertable commit with eventoutbox tests run before
and after. The kit owns BOTH encoder and decoder so otel-ingest imports the same implementation.

stdlib only (user directive 2026-11-26): `database/sql` with a caller-supplied `*sql.DB` (the agent opens its own
sqlite driver), `syscall.Statfs` for free space. No go.mod/go.sum change (the driver in go.mod is test-only here).

## 2. API

```go
q, err := blobqueue.New(db, blobqueue.Config{...})        // creates schema, reads/creates epoch
seq, err := q.Enqueue(ctx, blobqueue.Item{Signal, ContentType, ContentEncoding, Body})
err = q.Run(ctx, sender)   // drain loop; sender is func(ctx, frame []byte) (Ack, error)
st := q.Stats()            // depth, bytes, oldest age, shed counters, refused, last error
```

`Enqueue` returns only after the row is committed (persist-before-ack). Errors: `ErrTooLarge` (body over
`MaxBodyBytes`, default 4 MiB, configurable), `ErrDiskFloor` (stage 2 maps it to HTTP 503, which Traefik
retries), `ErrClosed`. Concurrent callers are safe; `Enqueue` blocking until commit lets callers later share
one transaction (group commit) with no API change. Group commit is built ONLY if the Pi per-row number says so.

## 3. Schema

- `blobq_items(seq INTEGER PRIMARY KEY AUTOINCREMENT, signal, ctype, cenc, body BLOB, size, created_at, protected, sent)`.
  AUTOINCREMENT is mandatory: a bare INTEGER PRIMARY KEY restarts at 1 when the table drains empty and the
  receiver would dedupe the new rows as duplicates (silent loss). Test: drain to empty, enqueue, seq > previous max.
  `protected=1` marks the loss marker (never shed); `sent=1` is set when a batch holding that marker is loaded for
  sending (see section 6).
- `blobq_meta(key,value)`: only `epoch` (random, new if the DB is recreated). Running totals (rows, logical bytes) are
  derived ONCE at `New` with a single SUM and then kept in memory under the write mutex, never SUM() per enqueue.
  Shed/refused/sent/dead counters and the pending refusal count are in-memory and restart from zero; the loss
  marker ROW is the durable record of what was shed, delivered to the receiver like any item.
- `PRAGMA auto_vacuum=INCREMENTAL` is set on one pinned `sql.Conn` together with the schema transaction (and a
  one-time `VACUUM` for an existing file created without it), because the pragma is per connection.

## 4. Wire frame (versioned, length-prefixed, no base64)

`magic "BQ" | version u8 | epoch (u8 len + bytes) | count u32 | per item: seq u64, signal u8-len string,
ctype, cenc (u16-len strings), body len u32 + bytes`. Little-endian. The decoder bounds every length against
the remaining buffer and a configured max before allocating (fuzz target). Unknown version is refused.
Ack is small JSON: `{"accepted":n,"duplicate":n,"rejected":[{"seq","code","retryable"}]}`; refusals are
classified by body `code`, never status alone (as BatchOutbox does). Receiver dedupes on (epoch, seq).

## 5. Drain

Oldest-first batches up to `MaxBatchItems` / `MaxBatchBytes`; delete after ack; exponential backoff with jitter
on failure (base 1 s, max 5 min, injectable rand/clock); non-retryable per-item refusals are dead-lettered
(counted, reported via `OnDead`), retryable ones held. Logging on state change only.

## 6. Bounds and shedding (plan s5.1), whichever bites first, OLDEST first, every shed counted

max age, max bytes (logical, running total), max rows, and a free-disk floor on the queue's filesystem
(percent AND absolute bytes; the stricter wins; `Bavail`; statfs injected for tests; path from config or
`PRAGMA database_list`). On a floor breach: shed oldest in batches, then `incremental_vacuum` + WAL checkpoint
so space really returns; STOP when the queue is empty (something else is filling the disk) and refuse new rows
(`ErrDiskFloor`, counted as loss; the refusal count reaches the marker with the next written row). The marker is
updated in the same transaction as the delete. An unmeasurable statfs fails OPEN (refusing all traffic on a transient
error is worse) and is surfaced by the sticky `Stats.StatfsErrs` counter: while it grows the floor is silently off.

Loss marker: a coalesced marker row with its own seq, protected from shedding, carrying {rows, bytes, oldest/newest
ts, seq range shed, refused, per-reason counts}. It is updated in place ONLY while `sent=0`. Once a batch holding it
has been handed to the Sender (even if that send failed after the receiver stored it) the row is never rewritten:
the receiver dedupes on (epoch, seq) and would discard changed counts. Later loss goes into a NEW marker carrying
only the delta, so **the receiver must SUM markers**. Test: a deduping receiver that stores then errors; the summed
marker rows must equal rows shed. `MaxRows` counts the marker row (transiently cap+1).

Ack contract: `Ack.Accepted` and `Ack.Duplicate` are pointers; the queue holds the whole batch unless both are
present, non-negative, accepted+duplicate+len(rejected) == items sent, and every rejection names a distinct item of
the batch (mirrors `eventoutbox.BatchOutbox`'s ack check). An empty or wrong-route 2xx therefore deletes nothing.

## 7. Durability statement

kill -9 survives with WAL + `synchronous=NORMAL`. Power loss on a Pi needs `synchronous=FULL`. PRAGMAs are
per-connection and the caller owns the DB, so `New` reads `journal_mode` and `synchronous` back and reports the
level in `Stats` (`Durability: "full"|"normal"|"weak"`) and refuses a non-WAL journal. DSN guidance in the package doc.
Expected fsyncs per Enqueue at FULL+WAL: one WAL fsync per commit; the op-count test asserts rows written ==
bodies enqueued and no extra transactions.

## 8. Failure modes and proving tests

| Failure | Test |
|---|---|
| kill -9 mid-enqueue / mid-drain | child-process crash test: no loss, no duplicate after ack |
| seq reuse after drain | drain-empty-enqueue test |
| floor breach frees nothing | fake statfs + file-size assertion after vacuum |
| shed whole queue forever | empty-queue stop + refuse test |
| marker lost / unbounded | marker durability + coalescing test |
| hostile frame | fuzz on the decoder; round-trip property test |
| bound comparison off by one | mutation check on shed/bounds |
| races | -race concurrency test |

## 9. Benchmarks

Baseline = BatchOutbox with the same gzip body base64-wrapped in a JSON object (puts a number on the rejected
option). Then blobqueue: enqueue ops/s, p50/p99, drain throughput, bytes written per body, RSS, CPU/1k bodies, at
1-64 KiB gzip, 10-100 bodies/s. Pi: cross-compiled arm64 test binary under a capped `systemd-run` unit on
ubuntu.pi.kdhomeapps.com, CPU-pinned replay, op-count assertion; fix causes, re-benchmark each fix.
