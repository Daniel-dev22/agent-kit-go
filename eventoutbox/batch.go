package eventoutbox

// Batch mode: an opt-in, additive sibling of Outbox. It drains N events per
// POST instead of one POST per job event, to a single endpoint, with an
// idempotent (epoch, seq) identity so a replayed batch is harmless.
//
// It shares nothing with Outbox's pending_events table, so existing callers and
// their on-disk databases are untouched. A single event is a batch of one: there
// is one send path.
//
// Contract (stdlib only, driver-agnostic like the rest of the package):
//   - Enqueue persists FIRST (AUTOINCREMENT row id = seq, never reused after the
//     queue empties), then pokes ONE drainer goroutine. No goroutine per event.
//   - The body is {<envelope fields>,"events":[{"seq":N,...},...]}. The seq is
//     stamped when the batch is built; payloads are caller JSON objects.
//   - A 2xx is a success only when the response acks every event:
//     accepted + duplicate + len(rejected) == len(batch). Anything else (an
//     unparseable body, a shortfall) keeps every row.
//   - Delete is exactly the acked ids in one transaction, never id <= max.
//   - 413 splits the batch (a batch of one is dead-lettered); 429, 5xx, timeouts
//     and network errors back off with capped exponential full jitter; any other
//     4xx is a refusal (bisect, then dead-letter) unless the body says
//     {"retryable":true} or BatchConfig.HoldOnRefusal holds the rows and backs off.
//   - Bounded by age and rows, drop-oldest, but "protected" events (recovery
//     events) are only dropped past the hard cap, loudly.

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTooLarge is returned by Enqueue for an event that cannot fit a request.
var ErrTooLarge = errors.New("eventoutbox: event larger than the batch byte limit")

// ErrBadPayload is returned by Enqueue for a payload that is not a JSON object
// or that already carries the reserved "seq" key.
var ErrBadPayload = errors.New("eventoutbox: payload must be a JSON object without a \"seq\" key")

// DeadLetter describes an event that was removed without being delivered.
type DeadLetter struct {
	Seq    int64
	Kind   string
	Code   string // controller refusal code, or one of the kit's: payload_too_large, refused, dropped_age, dropped_rows, dropped_hard_cap
	Reason string
}

// BatchConfig configures a BatchOutbox. URL is required.
type BatchConfig struct {
	URL       string
	Envelope  func(epoch string) map[string]any // fields sent beside the events; may be nil
	EventsKey string                            // default "events"

	MaxEvents int // events per request, default 100
	MaxBytes  int // whole request body, default 256 KiB

	MaxRows  int           // soft row bound, default 10000 (drops oldest unprotected)
	HardRows int           // absolute bound, default 2*MaxRows (drops oldest regardless, loudly)
	MaxAge   time.Duration // unprotected events older than this are dropped, default 24h

	DrainInterval time.Duration // idle re-check of the backlog, default 30s
	BackoffBase   time.Duration // default 1s
	BackoffMax    time.Duration // default 5m
	PushTimeout   time.Duration // default 15s

	// HoldOnRefusal changes how a non-retryable 4xx (other than 413) on the whole request is
	// handled. Default false: bisect to the offending event and dead-letter it. True: hold every
	// row and back off like a 5xx, for controllers whose contract says a request-wide refusal
	// means "not now" and per-event refusals arrive in a 2xx `rejected` list.
	HoldOnRefusal bool

	OnDead func(DeadLetter) // called for every event removed undelivered
	OnSent func()           // called after every 2xx (lets a heartbeat gate count events as traffic)

	Now    func() time.Time // default time.Now
	Rand   func() float64   // [0,1), default math/rand/v2
	Logger *slog.Logger
}

// BatchStats is a point-in-time view for health output.
type BatchStats struct {
	Epoch   string
	LastSeq int64
	Pending int
	Dead    int64
	Dropped int64
}

type rejection struct {
	Seq       int64  `json:"seq"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

type ack struct {
	Accepted  *int        `json:"accepted"`
	Duplicate *int        `json:"duplicate"`
	Rejected  []rejection `json:"rejected"`
}

type row struct {
	id      int64
	kind    string
	payload []byte
}

// BatchOutbox is the durable batched push queue.
type BatchOutbox struct {
	db     *sql.DB
	client *http.Client
	cfg    BatchConfig
	epoch  string
	log    *slog.Logger

	poke chan struct{}
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	limit      int // adaptive events-per-request, drainer-owned
	dead       atomic.Int64
	dropped    atomic.Int64
	failing    bool // drainer-owned: logging on state change only
	conflicted bool // drainer-owned: an epoch conflict is being held (HoldOnRefusal)
	failures   int  // drainer-owned: consecutive failed passes
}

// NewBatch creates the batch tables on db (idempotent) and returns a BatchOutbox.
// db needs the same busy_timeout/WAL settings as for New.
func NewBatch(db *sql.DB, client *http.Client, cfg BatchConfig) (*BatchOutbox, error) {
	if db == nil || client == nil || cfg.URL == "" {
		return nil, fmt.Errorf("eventoutbox: db, client and URL are required")
	}
	if cfg.EventsKey == "" {
		cfg.EventsKey = "events"
	}
	if cfg.MaxEvents <= 0 {
		cfg.MaxEvents = 100
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 256 << 10
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 10000
	}
	if cfg.HardRows < cfg.MaxRows {
		cfg.HardRows = 2 * cfg.MaxRows
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 24 * time.Hour
	}
	if cfg.DrainInterval <= 0 {
		cfg.DrainInterval = 30 * time.Second
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		cfg.BackoffMax = 5 * time.Minute
	}
	if cfg.PushTimeout <= 0 {
		cfg.PushTimeout = 15 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rand == nil {
		cfg.Rand = mrand.Float64
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	epoch, err := initBatchSchema(db)
	if err != nil {
		return nil, err
	}
	return &BatchOutbox{
		db: db, client: client, cfg: cfg, epoch: epoch, log: log,
		poke: make(chan struct{}, 1), stop: make(chan struct{}), limit: cfg.MaxEvents,
	}, nil
}

// initBatchSchema creates the tables and the epoch in ONE transaction: a DB that
// has events always has an epoch, and a recreated DB gets a new one.
func initBatchSchema(db *sql.DB) (string, error) {
	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("eventoutbox: begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS batch_events (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			kind       TEXT    NOT NULL,
			payload    BLOB    NOT NULL,
			protected  INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL,
			attempts   INTEGER NOT NULL DEFAULT 0,
			last_error TEXT
		);
		CREATE TABLE IF NOT EXISTS batch_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
	`); err != nil {
		return "", fmt.Errorf("eventoutbox: create batch schema: %w", err)
	}
	var epoch string
	err = tx.QueryRow(`SELECT value FROM batch_meta WHERE key='epoch'`).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", fmt.Errorf("eventoutbox: epoch: %w", err)
		}
		epoch = hex.EncodeToString(b[:])
		if _, err := tx.Exec(`INSERT INTO batch_meta (key, value) VALUES ('epoch', ?)`, epoch); err != nil {
			return "", fmt.Errorf("eventoutbox: store epoch: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("eventoutbox: read epoch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("eventoutbox: commit schema: %w", err)
	}
	return epoch, nil
}

// Epoch is the identity of this outbox database; it changes only if the DB is recreated.
func (o *BatchOutbox) Epoch() string { return o.epoch }

// Enqueue durably persists one event and wakes the drainer. payload must be a
// JSON object without a "seq" key. protected marks a recovery event that the
// soft bounds never drop. It returns the event's seq.
func (o *BatchOutbox) Enqueue(kind string, payload []byte, protected bool) (int64, error) {
	p := bytes.TrimSpace(payload)
	if len(p) < 2 || p[0] != '{' || p[len(p)-1] != '}' || !json.Valid(p) || bytes.Contains(p, []byte(`"seq"`)) {
		return 0, ErrBadPayload
	}
	if len(p)+o.envelopeLen()+len(o.cfg.EventsKey)+32 > o.cfg.MaxBytes {
		return 0, ErrTooLarge
	}
	prot := 0
	if protected {
		prot = 1
	}
	res, err := o.db.Exec(`INSERT INTO batch_events (kind, payload, protected, created_at) VALUES (?, ?, ?, ?)`,
		kind, p, prot, o.cfg.Now().Unix())
	if err != nil {
		return 0, fmt.Errorf("eventoutbox: enqueue: %w", err)
	}
	id, _ := res.LastInsertId()
	select {
	case o.poke <- struct{}{}:
	default:
	}
	return id, nil
}

func (o *BatchOutbox) envelopeLen() int {
	b, _ := o.envelope()
	return len(b)
}

// envelope returns the envelope JSON object (without the events key).
func (o *BatchOutbox) envelope() ([]byte, error) {
	m := map[string]any{}
	if o.cfg.Envelope != nil {
		m = o.cfg.Envelope(o.epoch)
	}
	return json.Marshal(m)
}

// LastSeq is the highest seq ever assigned (survives deletes: AUTOINCREMENT).
func (o *BatchOutbox) LastSeq() int64 {
	var s sql.NullInt64
	_ = o.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='batch_events'`).Scan(&s)
	return s.Int64
}

// Stats reports the queue for health output.
func (o *BatchOutbox) Stats() BatchStats {
	var n int
	_ = o.db.QueryRow(`SELECT COUNT(*) FROM batch_events`).Scan(&n)
	return BatchStats{Epoch: o.epoch, LastSeq: o.LastSeq(), Pending: n, Dead: o.dead.Load(), Dropped: o.dropped.Load()}
}

// Start runs the single drain goroutine until ctx is cancelled or Close is called.
func (o *BatchOutbox) Start(ctx context.Context) {
	o.wg.Go(func() { o.loop(ctx) })
}

// Close stops the drainer and waits for it. It does not close the *sql.DB.
func (o *BatchOutbox) Close() {
	if o == nil {
		return
	}
	o.once.Do(func() { close(o.stop) })
	o.wg.Wait()
}

func (o *BatchOutbox) loop(ctx context.Context) {
	timer := time.NewTimer(0) // first drain at once: flush what a previous run left
	defer timer.Stop()
	backingOff := false
	for {
		pk := o.poke
		if backingOff {
			pk = nil // a poke must not cut a backoff short: that is the herd
		}
		select {
		case <-ctx.Done():
			return
		case <-o.stop:
			return
		case <-pk:
		case <-timer.C:
			backingOff = false
		}
		var next time.Duration
		next, backingOff = o.afterPass(o.drain(ctx))
		timer.Reset(next) // go >= 1.23: Reset on a fired or stopped timer leaves no stale value
	}
}

// afterPass turns one drain pass's result into the wait before the next pass and
// whether pokes are ignored meanwhile (true only while backing off).
func (o *BatchOutbox) afterPass(ok bool) (time.Duration, bool) {
	if ok {
		o.failures = 0
		return o.cfg.DrainInterval, false
	}
	o.failures++
	return o.backoff(o.failures), true
}

// backoff is capped exponential with full jitter: uniform in [0, min(max, base*2^(n-1))].
func (o *BatchOutbox) backoff(n int) time.Duration {
	ceil := min(float64(o.cfg.BackoffBase)*math.Exp2(float64(min(n-1, 40))), float64(o.cfg.BackoffMax))
	return time.Duration(o.cfg.Rand() * ceil)
}

// drain sends batches until the queue is empty (true) or a transient failure
// says to back off (false).
func (o *BatchOutbox) drain(ctx context.Context) bool {
	o.prune()
	for {
		if ctx.Err() != nil {
			return true
		}
		rows, err := o.load()
		if err != nil {
			o.noteFailure("load", err)
			return false
		}
		if len(rows) == 0 {
			o.noteRecovered()
			return true
		}
		switch o.send(ctx, rows) {
		case sentOK:
			o.noteRecovered()
			o.limit = min(o.cfg.MaxEvents, o.limit*2)
		case sentSplit, sentDead:
			// no backoff; the limit already shrank (split) or must not grow (nothing was delivered)
		case sentRetry:
			return false
		case sentConflict:
			return true // rows kept; the next pass is one DrainInterval away, not a backoff
		}
	}
}

type outcome int

const (
	sentOK outcome = iota
	sentSplit
	sentDead
	sentRetry
	sentConflict // HoldOnRefusal: epoch_conflict; rows kept, no backoff
)

func (o *BatchOutbox) noteFailure(what string, err error) {
	if !o.failing {
		o.failing = true
		o.log.Warn("eventoutbox: batch push failing (retrying with backoff)", "stage", what, "error", err)
	}
}

func (o *BatchOutbox) noteRecovered() {
	if o.conflicted {
		o.conflicted = false
		o.log.Info("eventoutbox: epoch conflict cleared")
	}
	if o.failing {
		o.failing = false
		o.log.Info("eventoutbox: batch push recovered")
	}
}

// load reads up to o.limit rows oldest-first, then trims to the byte bound.
func (o *BatchOutbox) load() ([]row, error) {
	env, err := o.envelope()
	if err != nil {
		return nil, fmt.Errorf("envelope: %w", err)
	}
	budget := o.cfg.MaxBytes - len(env) - len(o.cfg.EventsKey) - 16
	rs, err := o.db.Query(`SELECT id, kind, payload FROM batch_events ORDER BY id ASC LIMIT ?`, o.limit)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []row
	used := 0
	for rs.Next() {
		var r row
		if err := rs.Scan(&r.id, &r.kind, &r.payload); err != nil {
			return nil, err
		}
		cost := len(r.payload) + 24 // seq stamp + comma
		if len(out) > 0 && used+cost > budget {
			break
		}
		used += cost
		out = append(out, r)
	}
	return out, rs.Err()
}

// build assembles the request body, stamping each event's seq.
func (o *BatchOutbox) build(rows []row) ([]byte, error) {
	env, err := o.envelope()
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.Write(env[:len(env)-1])
	if len(env) > 2 {
		b.WriteByte(',')
	}
	key, _ := json.Marshal(o.cfg.EventsKey)
	b.Write(key)
	b.WriteString(":[")
	for i, r := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"seq":`)
		b.WriteString(strconv.FormatInt(r.id, 10))
		if len(r.payload) > 2 {
			b.WriteByte(',')
			b.Write(r.payload[1:])
		} else {
			b.WriteByte('}')
		}
	}
	b.WriteString("]}")
	return b.Bytes(), nil
}

func (o *BatchOutbox) send(ctx context.Context, rows []row) outcome {
	body, err := o.build(rows)
	if err != nil {
		o.fail(rows, "build", err)
		return sentRetry
	}
	reqCtx, cancel := context.WithTimeout(ctx, o.cfg.PushTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, o.cfg.URL, bytes.NewReader(body))
	if err != nil {
		o.fail(rows, "request", err)
		return sentRetry
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.client.Do(req)
	if err != nil {
		o.fail(rows, "post", err)
		return sentRetry
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	st := resp.StatusCode
	switch {
	case st >= 200 && st < 300:
		return o.onAck(rows, rb)
	case st == http.StatusRequestEntityTooLarge:
		if len(rows) > 1 {
			o.limit = max(1, len(rows)/2)
			return sentSplit
		}
		o.deadLetter(rows, "payload_too_large", "413 for a batch of one")
		return sentDead
	case st == http.StatusRequestTimeout || st == http.StatusTooManyRequests || st >= 500:
		o.fail(rows, "status", fmt.Errorf("HTTP %d", st))
		return sentRetry
	case st >= 400 && st < 500:
		var rt struct {
			Retryable bool   `json:"retryable"`
			Code      string `json:"code"`
		}
		_ = json.Unmarshal(rb, &rt)
		if rt.Retryable {
			o.fail(rows, "status", fmt.Errorf("HTTP %d retryable", st))
			return sentRetry
		}
		if o.cfg.HoldOnRefusal && st == http.StatusConflict && rt.Code == "epoch_conflict" {
			// Another process holds this identity (contract v1.6): not an error to alarm on and not
			// worth an exponential climb. Keep every row, log once per state change at Info and
			// look again on the normal drain cadence, so a takeover is seen as soon as the other
			// process goes quiet.
			if !o.conflicted {
				o.conflicted = true
				o.log.Info("eventoutbox: controller reports an epoch conflict (events held, not an error)")
			}
			return sentConflict
		}
		if o.cfg.HoldOnRefusal {
			// The controller refused the REQUEST (route missing, state conflict, schema), not
			// one event: per-event refusals travel in a 2xx `rejected` list. Bisecting or
			// dead-lettering would destroy events the controller never judged, so keep every
			// row and back off until the controller can take them.
			o.fail(rows, "status", fmt.Errorf("HTTP %d refused the whole request (held)", st))
			return sentRetry
		}
		if len(rows) > 1 {
			// Bisect: isolate the refused event instead of dead-lettering good ones with it.
			o.limit = max(1, len(rows)/2)
			return sentSplit
		}
		code := rt.Code
		if code == "" {
			code = "refused"
		}
		o.deadLetter(rows, code, fmt.Sprintf("HTTP %d", st))
		return sentDead
	default:
		o.fail(rows, "status", fmt.Errorf("HTTP %d", st))
		return sentRetry
	}
}

// onAck applies a 2xx response. Only a fully acked batch deletes anything.
func (o *BatchOutbox) onAck(rows []row, rb []byte) outcome {
	var a ack
	if err := json.Unmarshal(rb, &a); err != nil || a.Accepted == nil || a.Duplicate == nil {
		o.fail(rows, "ack", errors.New("2xx without a parseable accepted/duplicate ack"))
		return sentRetry
	}
	if *a.Accepted+*a.Duplicate+len(a.Rejected) != len(rows) || *a.Accepted < 0 || *a.Duplicate < 0 {
		o.fail(rows, "ack", fmt.Errorf("ack shortfall: accepted=%d duplicate=%d rejected=%d batch=%d",
			*a.Accepted, *a.Duplicate, len(a.Rejected), len(rows)))
		return sentRetry
	}
	inBatch := make(map[int64]row, len(rows))
	for _, r := range rows {
		inBatch[r.id] = r
	}
	retry := map[int64]bool{}
	var terminal []row
	reasons := map[int64]string{}
	for _, rj := range a.Rejected {
		r, ok := inBatch[rj.Seq]
		if !ok || retry[rj.Seq] || reasons[rj.Seq] != "" {
			o.fail(rows, "ack", fmt.Errorf("ack rejects seq %d not in batch or twice", rj.Seq))
			return sentRetry
		}
		if rj.Retryable {
			retry[rj.Seq] = true
			continue
		}
		code := rj.Code
		if code == "" {
			code = "refused"
		}
		reasons[rj.Seq] = code
		terminal = append(terminal, r)
	}
	var del []int64
	for _, r := range rows {
		if !retry[r.id] {
			del = append(del, r.id)
		}
	}
	if err := o.deleteIDs(del); err != nil {
		// Delivered but not deleted: the replay arrives as a duplicate. Safe.
		o.fail(rows, "delete", err)
		return sentRetry
	}
	for _, r := range terminal {
		o.noteDead(DeadLetter{Seq: r.id, Kind: r.kind, Code: reasons[r.id], Reason: "refused by controller"})
	}
	if o.cfg.OnSent != nil {
		o.cfg.OnSent()
	}
	if len(retry) > 0 {
		var kept []row
		for _, r := range rows {
			if retry[r.id] {
				kept = append(kept, r)
			}
		}
		o.fail(kept, "ack", errors.New("per-row retryable refusal"))
		return sentRetry
	}
	return sentOK
}

func (o *BatchOutbox) deleteIDs(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := tx.Prepare(`DELETE FROM batch_events WHERE id = ?`)
	if err != nil {
		return err
	}
	defer st.Close()
	for _, id := range ids {
		if _, err := st.Exec(id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (o *BatchOutbox) fail(rows []row, stage string, err error) {
	o.noteFailure(stage, err)
	for _, r := range rows {
		if _, e := o.db.Exec(`UPDATE batch_events SET attempts = attempts + 1, last_error = ? WHERE id = ?`,
			err.Error(), r.id); e != nil {
			o.log.Warn("eventoutbox: record failure failed", "error", e, "seq", r.id)
		}
	}
}

func (o *BatchOutbox) deadLetter(rows []row, code, reason string) {
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.id
	}
	if err := o.deleteIDs(ids); err != nil {
		o.fail(rows, "delete", err)
		return
	}
	for _, r := range rows {
		o.noteDead(DeadLetter{Seq: r.id, Kind: r.kind, Code: code, Reason: reason})
	}
}

func (o *BatchOutbox) noteDead(d DeadLetter) {
	o.dead.Add(1)
	o.log.Error("eventoutbox: event dead-lettered (state is repaired by the next snapshot)",
		"seq", d.Seq, "kind", d.Kind, "code", d.Code, "reason", d.Reason)
	if o.cfg.OnDead != nil {
		o.cfg.OnDead(d)
	}
}

// prune enforces the age and row bounds, drop-oldest. Protected rows survive the
// soft bounds and fall only to the hard cap.
func (o *BatchOutbox) prune() {
	old := o.cfg.Now().Add(-o.cfg.MaxAge).Unix()
	o.dropWhere(`protected = 0 AND created_at < ?`, []any{old}, -1, "dropped_age")
	n := o.count()
	if n > o.cfg.MaxRows {
		o.dropWhere(`protected = 0`, nil, n-o.cfg.MaxRows, "dropped_rows")
	}
	if n = o.count(); n > o.cfg.HardRows {
		o.dropWhere(`1 = 1`, nil, n-o.cfg.HardRows, "dropped_hard_cap")
	}
}

func (o *BatchOutbox) count() int {
	var n int
	_ = o.db.QueryRow(`SELECT COUNT(*) FROM batch_events`).Scan(&n)
	return n
}

func (o *BatchOutbox) dropWhere(where string, args []any, limit int, code string) {
	q := `SELECT id, kind FROM batch_events WHERE ` + where + ` ORDER BY id ASC`
	if limit >= 0 {
		q += ` LIMIT ` + strconv.Itoa(limit)
	}
	rs, err := o.db.Query(q, args...)
	if err != nil {
		return
	}
	var drop []row
	for rs.Next() {
		var r row
		if rs.Scan(&r.id, &r.kind) == nil {
			drop = append(drop, r)
		}
	}
	rs.Close()
	if len(drop) == 0 {
		return
	}
	ids := make([]int64, len(drop))
	for i, r := range drop {
		ids[i] = r.id
	}
	if err := o.deleteIDs(ids); err != nil {
		return
	}
	o.dropped.Add(int64(len(drop)))
	for _, r := range drop {
		o.noteDead(DeadLetter{Seq: r.id, Kind: r.kind, Code: code, Reason: "queue bound"})
	}
}
