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
//   - Opt-in (zero value = the behaviour above): OnEpochChanged (a 2xx that says
//     epoch_changed:true is stored, not applied, so it is not an ack), SupersedeKey
//     (a newer event with the same key replaces its unacked predecessors) and
//     BisectOnRefusalCodes (with HoldOnRefusal, a refusal code on the allowlist
//     bisects to the offending event instead of holding the queue).

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

	// BisectOnRefusalCodes (used only with HoldOnRefusal; default empty = HoldOnRefusal exactly as
	// before) lists refusal codes, read from the `code` field of the refused reply's body, that
	// the controller documents as "this request contains an event I will never accept". A
	// request-wide non-retryable 4xx whose code is on the list is bisected down to the single
	// event and that event is dead-lettered (contract s4), so one bad event cannot block the head
	// of the queue. Leave it EMPTY when the controller reports every per-event refusal in a 2xx
	// `rejected` list (contract v1.7): then every 4xx is an envelope refusal and must never be
	// listed here. It is keyed on the CODE only: a refusal with any other code (or none) is still
	// held, because a single-event request that is refused can equally be an envelope refusal
	// (wrong site, bad schema) and dead-lettering on that would drain the whole queue.
	BisectOnRefusalCodes []string

	// SupersedeKey (optional) maps an event to a key; "" means no key. Enqueueing an event with a
	// key replaces every older, still-unacked row with the same key in the same transaction, so a
	// repeated recovery event (e.g. a boot marker) is bounded to one row. The replacement KEEPS THE
	// OLDEST SEQ of those rows (the oldest row is updated in place, the younger same-key rows are
	// deleted), so it never jumps behind events enqueued after the first one: boot1
	// agent_started(1), node_removed(2), boot2 agent_started still delivers 1 then 2. A row the
	// drainer holds in flight is never updated (its ack would delete the newer payload): a new row
	// is inserted behind it instead. The row keeps the strongest protection of the rows it
	// replaces. seq stays monotonic for every other row and FIFO order is untouched. A replaced
	// payload is NOT passed to OnDead (the newer event carries the same fact): each replacement is
	// counted in BatchStats.Superseded. Setting it adds a nullable supersede_key column to batch_events.
	SupersedeKey func(kind string, payload []byte) string

	OnDead func(DeadLetter) // called for every event removed undelivered, except those superseded
	OnSent func()           // called after every acking 2xx (lets a heartbeat gate count events as traffic)

	// OnEpochChanged (optional) is called INSTEAD of OnSent for a 2xx whose body says
	// epoch_changed:true: the controller stored the events but did not apply them (a straggler
	// from a non-current epoch), so the reply is not evidence that the controller has this
	// agent's state, and an agent that publishes a last-ack time must not advance it. The rows are
	// still deleted (they were stored; a replay would only be a duplicate). Nil keeps OnSent
	// firing on every 2xx. Counted in BatchStats.EpochChanged either way.
	OnEpochChanged func()

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

	Superseded   int64 // rows replaced under SupersedeKey
	EpochChanged int64 // acked 2xx replies that said epoch_changed:true
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

	EpochChanged bool `json:"epoch_changed"`
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
	superseded atomic.Int64
	inflightMu sync.Mutex         // orders load()'s marking against enqueueSuperseding's check
	inflight   map[int64]struct{} // ids of the batch the drainer holds; kept only with SupersedeKey
	epochChg   atomic.Int64
	bisect     map[string]struct{} // BisectOnRefusalCodes as a set; nil when empty
	failing    bool                // drainer-owned: logging on state change only
	conflicted bool                // drainer-owned: an epoch conflict is being held (HoldOnRefusal)
	failures   int                 // drainer-owned: consecutive failed passes
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
	if cfg.SupersedeKey != nil {
		if err := ensureSupersedeColumn(db); err != nil {
			return nil, err
		}
	}
	o := &BatchOutbox{
		db: db, client: client, cfg: cfg, epoch: epoch, log: log,
		poke: make(chan struct{}, 1), stop: make(chan struct{}), limit: cfg.MaxEvents,
	}
	if len(cfg.BisectOnRefusalCodes) > 0 {
		o.bisect = make(map[string]struct{}, len(cfg.BisectOnRefusalCodes))
		for _, c := range cfg.BisectOnRefusalCodes {
			o.bisect[c] = struct{}{}
		}
	}
	return o, nil
}

// ensureSupersedeColumn adds the supersede_key column and its partial index to an existing
// batch_events table. It runs only when SupersedeKey is set, so a database that never opts in
// keeps its schema byte for byte. Idempotent.
func ensureSupersedeColumn(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("eventoutbox: begin: %w", err)
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('batch_events') WHERE name='supersede_key'`).Scan(&n); err != nil {
		return fmt.Errorf("eventoutbox: inspect batch_events: %w", err)
	}
	if n == 0 {
		if _, err := tx.Exec(`ALTER TABLE batch_events ADD COLUMN supersede_key TEXT`); err != nil {
			return fmt.Errorf("eventoutbox: add supersede_key: %w", err)
		}
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS batch_events_supersede ON batch_events(supersede_key) WHERE supersede_key IS NOT NULL`); err != nil {
		return fmt.Errorf("eventoutbox: index supersede_key: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("eventoutbox: commit supersede schema: %w", err)
	}
	return nil
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
	var id int64
	var err error
	if key := o.supersedeKey(kind, p); key != "" {
		id, err = o.enqueueSuperseding(kind, p, prot, key)
	} else {
		var res sql.Result
		res, err = o.db.Exec(`INSERT INTO batch_events (kind, payload, protected, created_at) VALUES (?, ?, ?, ?)`,
			kind, p, prot, o.cfg.Now().Unix())
		if err == nil {
			id, _ = res.LastInsertId()
		}
	}
	if err != nil {
		return 0, fmt.Errorf("eventoutbox: enqueue: %w", err)
	}
	select {
	case o.poke <- struct{}{}:
	default:
	}
	return id, nil
}

func (o *BatchOutbox) supersedeKey(kind string, payload []byte) string {
	if o.cfg.SupersedeKey == nil {
		return ""
	}
	return o.cfg.SupersedeKey(kind, payload)
}

// enqueueSuperseding replaces the unacked rows of key with this event in ONE transaction (one
// commit), so a crash can never leave both or neither, and the replacement KEEPS THE OLDEST SEQ:
// the oldest row of the key that is not in flight is updated in place (payload, kind, created_at,
// protected = strongest of old and new) and its younger same-key rows are deleted. A fresh row is
// inserted only when none qualifies: no row exists, or every one is in flight, because an in-place
// update of a row the drainer already holds would be deleted by its ack and lose the newer payload.
// Keeping the oldest seq matters for order: a repeated boot marker must not jump behind the events
// enqueued after the first one (node_removed must never be delivered before agent_started).
// inflightMu orders this against load(), which marks the batch it holds under the same lock.
func (o *BatchOutbox) enqueueSuperseding(kind string, payload []byte, prot int, key string) (int64, error) {
	o.inflightMu.Lock()
	defer o.inflightMu.Unlock()
	tx, err := o.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// Take the write lock BEFORE reading. A deferred transaction that reads first and writes later
	// fails at once with SQLITE_BUSY (busy_timeout does not apply) when another writer commits in
	// between, i.e. the drainer's delete or fail record; a no-op write here waits on busy_timeout
	// instead, and the read below then sees the committed state.
	if _, err := tx.Exec(`UPDATE batch_events SET supersede_key = supersede_key WHERE id = 0`); err != nil {
		return 0, err
	}
	rs, err := tx.Query(`SELECT id, protected FROM batch_events WHERE supersede_key = ? ORDER BY id ASC`, key)
	if err != nil {
		return 0, err
	}
	var free []int64 // every same-key row not in flight, oldest first
	oldProt := 0
	for rs.Next() {
		var id int64
		var p int
		if err := rs.Scan(&id, &p); err != nil {
			rs.Close()
			return 0, err
		}
		oldProt = max(oldProt, p)
		if _, held := o.inflight[id]; !held {
			free = append(free, id)
		}
	}
	err = rs.Err()
	rs.Close()
	if err != nil {
		return 0, err
	}
	now := o.cfg.Now().Unix()
	var id, replaced int64
	if len(free) > 0 {
		res, err := tx.Exec(`UPDATE batch_events SET kind = ?, payload = ?, protected = ?, created_at = ?, attempts = 0, last_error = NULL WHERE id = ?`,
			kind, payload, max(prot, oldProt), now, free[0])
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			id, replaced = free[0], 1
		}
	}
	if id == 0 {
		res, err := tx.Exec(`INSERT INTO batch_events (kind, payload, protected, created_at, supersede_key) VALUES (?, ?, ?, ?, ?)`,
			kind, payload, max(prot, oldProt), now, key)
		if err != nil {
			return 0, err
		}
		id, _ = res.LastInsertId()
	}
	for _, d := range free {
		if d == id {
			continue
		}
		del, err := tx.Exec(`DELETE FROM batch_events WHERE id = ?`, d)
		if err != nil {
			return 0, err
		}
		n, _ := del.RowsAffected()
		replaced += n
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	if replaced > 0 {
		o.superseded.Add(replaced)
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
	return BatchStats{Epoch: o.epoch, LastSeq: o.LastSeq(), Pending: n, Dead: o.dead.Load(), Dropped: o.dropped.Load(),
		Superseded: o.superseded.Load(), EpochChanged: o.epochChg.Load()}
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
		if backingOff || o.conflicted {
			// A poke must not cut a backoff short (that is the herd), and while the controller
			// reports an epoch conflict another request would only be refused again: retry on the
			// normal DrainInterval cadence, not once per Enqueue.
			pk = nil
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
		res := o.send(ctx, rows)
		o.release()
		switch res {
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
	if o.cfg.SupersedeKey != nil {
		// Hold the lock across the read AND the marking: a superseding enqueue is then either fully
		// before the read (its payload is what we load) or fully after the mark (it sees the rows as
		// in flight and inserts instead of updating).
		o.inflightMu.Lock()
		defer o.inflightMu.Unlock()
	}
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
	if err := rs.Err(); err != nil {
		return nil, err
	}
	if o.cfg.SupersedeKey != nil {
		clear(o.inflight)
		if o.inflight == nil {
			o.inflight = make(map[int64]struct{}, len(out))
		}
		for _, r := range out {
			o.inflight[r.id] = struct{}{}
		}
	}
	return out, nil
}

// release forgets the in-flight batch once its send has resolved (acked, kept or dead-lettered).
func (o *BatchOutbox) release() {
	if o.cfg.SupersedeKey == nil {
		return
	}
	o.inflightMu.Lock()
	clear(o.inflight)
	o.inflightMu.Unlock()
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
		if _, ok := o.bisect[rt.Code]; ok && o.cfg.HoldOnRefusal && rt.Code != "" {
			// The controller says this code means "an event in here is never acceptable"
			// (BisectOnRefusalCodes): isolate it. Keyed on the code, never on a single-event
			// request still being refused (that can be an envelope refusal).
			if len(rows) > 1 {
				o.limit = max(1, len(rows)/2)
				return sentSplit
			}
			o.deadLetter(rows, rt.Code, fmt.Sprintf("HTTP %d", st))
			return sentDead
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
	switch {
	case a.EpochChanged:
		o.epochChg.Add(1)
		if o.cfg.OnEpochChanged != nil {
			o.cfg.OnEpochChanged() // stored, not applied: not an ack, OnSent must not fire
		} else if o.cfg.OnSent != nil {
			o.cfg.OnSent()
		}
	case o.cfg.OnSent != nil:
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
	return o.execEach(`DELETE FROM batch_events WHERE id = ?`, len(ids), func(i int) []any { return []any{ids[i]} })
}

// execEach runs query once per index in ONE transaction (one commit, one fsync), so a pass over
// up to MaxEvents rows costs a single write barrier rather than one per row.
func (o *BatchOutbox) execEach(query string, n int, args func(i int) []any) error {
	if n == 0 {
		return nil
	}
	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer st.Close()
	for i := range n {
		if _, err := st.Exec(args(i)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// fail records one failed pass: ONE transaction for all rows and, if even that fails, ONE log
// line carrying the row count (not one line and one autocommit fsync per row).
func (o *BatchOutbox) fail(rows []row, stage string, err error) {
	o.noteFailure(stage, err)
	msg := err.Error()
	if e := o.execEach(`UPDATE batch_events SET attempts = attempts + 1, last_error = ? WHERE id = ?`,
		len(rows), func(i int) []any { return []any{msg, rows[i].id} }); e != nil {
		o.log.Warn("eventoutbox: record failure failed", "error", e, "rows", len(rows))
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
