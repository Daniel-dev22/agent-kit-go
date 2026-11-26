package blobqueue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrTooLarge: the body (or a metadata field) exceeds the configured per-item cap.
	ErrTooLarge = errors.New("blobqueue: item larger than the per-body cap")
	// ErrDiskFloor: the queue's filesystem is below the free-space floor and nothing more can be shed.
	// The queue refuses the row rather than fill the disk; map it to HTTP 503 (Traefik retries 503).
	ErrDiskFloor = errors.New("blobqueue: free-disk floor reached, refusing new rows")
	// ErrClosed: Enqueue after Close.
	ErrClosed = errors.New("blobqueue: closed")
	// ErrNotWAL: the caller's database is not in WAL mode (set journal_mode=WAL in the DSN).
	ErrNotWAL = errors.New("blobqueue: database must use journal_mode=WAL")
	// ErrRunning: a second Run while one is active.
	ErrRunning = errors.New("blobqueue: Run is already active")
)

// Config configures a Queue. Zero values take the defaults in the comments; a negative bound disables it.
type Config struct {
	Signals []string // optional allow-list of Item.Signal values; empty accepts any

	MaxBodyBytes int           // per body, default 4 MiB (a gzip OTLP batch of <=512 records is far smaller)
	MaxRows      int           // default 100000
	MaxBytes     int64         // logical bytes (body + metadata) across the queue, default 1 GiB
	MaxAge       time.Duration // rows older than this are shed, default 24h

	// Free-disk floor on the queue's filesystem: the stricter of the percentage and the absolute bytes
	// wins. Defaults 5% and 256 MiB.
	FloorPercent float64
	FloorBytes   int64
	// Statfs reports free (Bavail) and total bytes for the filesystem holding path. Default: syscall.Statfs.
	Statfs func(path string) (free, total uint64, err error)
	// Path is the directory statfs'd for the floor. Default: the directory of the database file.
	Path string

	MaxBatchItems int // per request, default 100
	MaxBatchBytes int // per request frame, default 4 MiB (an over-size item still goes alone)

	DrainInterval time.Duration // idle re-check, default 30s
	BackoffBase   time.Duration // default 1s
	BackoffMax    time.Duration // default 5m

	AllowNonWAL bool // tests and in-memory databases only

	OnDead func(DeadLetter) // every row removed undelivered because the receiver refused it for good
	Now    func() time.Time // default time.Now
	Rand   func() float64   // [0,1) for backoff jitter; default math/rand/v2
	Logger *slog.Logger
}

// DeadLetter describes an item the receiver refused permanently.
type DeadLetter struct {
	Seq    uint64
	Signal string
	Code   string
	Body   []byte // set only for a refused loss marker, whose JSON is the sole durable loss record
}

// Queue is the durable queue. Safe for concurrent Enqueue; one Run at a time.
type Queue struct {
	db  *sql.DB
	cfg Config
	log *slog.Logger

	epoch      string
	durability string
	path       string // statfs path; "" disables the floor

	insert *sql.Stmt

	wmu          sync.Mutex // serialises every write so the running totals stay exact
	rows, bytes  int64      // running totals, guarded by wmu; derived once at New
	inflightMax  uint64     // highest seq of the batch the drainer holds (never shed or updated), guarded by wmu
	closed       bool
	lastMaintain time.Time
	pendRefused  int64 // refused-at-floor count not yet folded into a marker, guarded by wmu

	poke chan struct{}

	shedAge, shedRows, shedBytes, shedFloor atomic.Int64 // rows shed, by reason
	shedByteTotal                           atomic.Int64
	refused, dead, sent                     atomic.Int64
	statfsErrs                              atomic.Int64 // sticky: while it grows the floor is silently off
	reclaimErrs                             atomic.Int64 // sticky: vacuum/checkpoint failures; shedding may free no disk
	running                                 atomic.Bool  // one Run at a time
	lastErr                                 atomic.Value // string
}

// New creates the schema (idempotent) on db and returns a Queue. See docs/BLOBQUEUE_DESIGN.md.
func New(db *sql.DB, cfg Config) (*Queue, error) {
	if db == nil {
		return nil, errors.New("blobqueue: db is required")
	}
	cfg = cfg.withDefaults()
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	q := &Queue{db: db, cfg: cfg, log: log, poke: make(chan struct{}, 1)}
	q.lastErr.Store("")

	ctx := context.Background()
	var journal string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil {
		return nil, fmt.Errorf("blobqueue: read journal_mode: %w", err)
	}
	if journal != "wal" && !cfg.AllowNonWAL {
		return nil, ErrNotWAL
	}
	var syncLvl int
	if err := db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&syncLvl); err != nil {
		return nil, fmt.Errorf("blobqueue: read synchronous: %w", err)
	}
	switch {
	case journal == "wal" && syncLvl >= 2:
		q.durability = "full"
	case journal == "wal" && syncLvl == 1:
		q.durability = "normal"
	default:
		q.durability = "weak"
	}

	if err := q.prepareFile(ctx); err != nil {
		return nil, err
	}

	q.path = cfg.Path
	if q.path == "" {
		var seq int
		var name, file string
		if err := db.QueryRowContext(ctx, `SELECT seq, name, file FROM pragma_database_list WHERE name='main'`).Scan(&seq, &name, &file); err == nil && file != "" {
			q.path = filepath.Dir(file)
		}
	}
	if q.path == "" {
		log.Warn("blobqueue: database has no file; the free-disk floor is disabled")
	}

	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size),0) FROM blobq_items`).Scan(&q.rows, &q.bytes); err != nil {
		return nil, fmt.Errorf("blobqueue: totals: %w", err)
	}
	ins, err := db.PrepareContext(ctx, `INSERT INTO blobq_items (signal, ctype, cenc, body, size, created_at, protected) VALUES (?,?,?,?,?,?,0)`)
	if err != nil {
		return nil, fmt.Errorf("blobqueue: prepare insert: %w", err)
	}
	q.insert = ins
	return q, nil
}

// prepareFile runs the auto_vacuum pragma, the schema transaction and the one-time VACUUM on ONE
// pinned connection (auto_vacuum is per connection and only takes effect at table creation or VACUUM),
// and RELEASES it before returning: the caller's pool may be capped at one connection (the fleet's
// SetMaxOpenConns(1)), so no other query may run while it is held.
func (q *Queue) prepareFile(ctx context.Context) error {
	conn, err := q.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("blobqueue: conn: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA auto_vacuum=INCREMENTAL`); err != nil {
		return fmt.Errorf("blobqueue: auto_vacuum: %w", err)
	}
	if err := q.initSchema(ctx, conn); err != nil {
		return err
	}
	var av int
	if err := conn.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&av); err == nil && av != 2 {
		// An existing database created without it: rebuild once so shedding can return space.
		if _, err := conn.ExecContext(ctx, `VACUUM`); err != nil {
			q.reclaimErrs.Add(1)
			q.lastErr.Store("vacuum: " + err.Error())
			q.log.Warn("blobqueue: could not enable incremental vacuum; shedding will not shrink the file", "err", err)
		}
	}
	return nil
}

func (c Config) withDefaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 4 << 20
	}
	if c.MaxRows == 0 {
		c.MaxRows = 100000
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 1 << 30
	}
	if c.MaxAge == 0 {
		c.MaxAge = 24 * time.Hour
	}
	if c.FloorPercent == 0 {
		c.FloorPercent = 5
	}
	if c.FloorBytes == 0 {
		c.FloorBytes = 256 << 20
	}
	if c.Statfs == nil {
		c.Statfs = statfs
	}
	if c.MaxBatchItems <= 0 {
		c.MaxBatchItems = 100
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 4 << 20
	}
	if c.DrainInterval <= 0 {
		c.DrainInterval = 30 * time.Second
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax < c.BackoffBase {
		c.BackoffMax = 5 * time.Minute
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Rand == nil {
		c.Rand = randFloat
	}
	return c
}

func (q *Queue) initSchema(ctx context.Context, conn *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("blobqueue: begin: %w", err)
	}
	defer tx.Rollback()
	// AUTOINCREMENT is mandatory: a bare INTEGER PRIMARY KEY restarts at 1 once the table drains, and
	// the receiver would dedupe the new rows as duplicates of acked ones (silent loss).
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS blobq_items (
			seq        INTEGER PRIMARY KEY AUTOINCREMENT,
			signal     TEXT    NOT NULL,
			ctype      TEXT    NOT NULL,
			cenc       TEXT    NOT NULL,
			body       BLOB    NOT NULL,
			size       INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			protected  INTEGER NOT NULL DEFAULT 0,
			sent       INTEGER NOT NULL DEFAULT 0  -- 1 once a batch holding this row was handed to the Sender
		);
		CREATE TABLE IF NOT EXISTS blobq_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
	`); err != nil {
		return fmt.Errorf("blobqueue: create schema: %w", err)
	}
	var epoch string
	err = tx.QueryRowContext(ctx, `SELECT value FROM blobq_meta WHERE key='epoch'`).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return fmt.Errorf("blobqueue: epoch: %w", err)
		}
		epoch = hex.EncodeToString(b[:])
		if _, err := tx.ExecContext(ctx, `INSERT INTO blobq_meta (key,value) VALUES ('epoch',?)`, epoch); err != nil {
			return fmt.Errorf("blobqueue: store epoch: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("blobqueue: read epoch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("blobqueue: commit schema: %w", err)
	}
	q.epoch = epoch
	return nil
}

// Epoch identifies this database; the receiver dedupes on (Epoch, seq). It changes only if the file is recreated.
func (q *Queue) Epoch() string { return q.epoch }

func itemBytes(it *Item) int64 {
	return int64(len(it.Body) + len(it.Signal) + len(it.ContentType) + len(it.ContentEncoding))
}

func (q *Queue) validate(it *Item) error {
	if len(it.Body) > q.cfg.MaxBodyBytes || len(it.Signal) > 255 || len(it.ContentType) > 0xFFFF || len(it.ContentEncoding) > 0xFFFF {
		return ErrTooLarge
	}
	if it.Signal == "" || it.Signal == SignalLoss {
		return fmt.Errorf("blobqueue: signal %q is empty or reserved", it.Signal)
	}
	if len(q.cfg.Signals) > 0 {
		for _, s := range q.cfg.Signals {
			if s == it.Signal {
				return nil
			}
		}
		return fmt.Errorf("blobqueue: signal %q not allowed", it.Signal)
	}
	return nil
}

// Enqueue persists one item and returns its seq only after the row is durable (committed). Any
// rows shed to stay within bounds are shed in the same transaction and counted.
func (q *Queue) Enqueue(ctx context.Context, it Item) (uint64, error) {
	if err := q.validate(&it); err != nil {
		return 0, err
	}
	size := itemBytes(&it)
	q.wmu.Lock()
	defer q.wmu.Unlock()
	if q.closed {
		return 0, ErrClosed
	}
	now := q.cfg.Now()
	if err := q.ensureFloor(ctx, size); err != nil {
		q.refused.Add(1)
		q.pendRefused++
		q.lastErr.Store(err.Error())
		return 0, err
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("blobqueue: begin: %w", err)
	}
	defer tx.Rollback()
	res, err := tx.StmtContext(ctx, q.insert).ExecContext(ctx, it.Signal, it.ContentType, it.ContentEncoding, it.Body, size, now.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("blobqueue: insert: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	dRows, dBytes := int64(1), size

	rows, bytes := q.rows, q.bytes
	over := func(sr, sb int64) bool {
		return (q.cfg.MaxRows > 0 && rows+dRows-sr > int64(q.cfg.MaxRows)) ||
			(q.cfg.MaxBytes > 0 && bytes+dBytes-sb > q.cfg.MaxBytes)
	}
	var loss lossAcc
	if over(0, 0) {
		// Shed oldest rows older than the new one; the cap that bit decides the reason.
		reason := reasonRows
		if q.cfg.MaxBytes > 0 && bytes+dBytes > q.cfg.MaxBytes {
			reason = reasonBytes
		}
		t, err := q.shedUntil(ctx, tx, uint64(id), reason, &loss, func(_ shedRow, sr, sb int64) bool { return over(sr, sb) })
		if err != nil {
			return 0, err
		}
		dRows -= t.rows
		dBytes -= t.bytes
	}
	if q.pendRefused > 0 {
		loss.Refused += q.pendRefused
	}
	mDelta, err := q.flushLoss(ctx, tx, &loss)
	if err != nil {
		return 0, err
	}
	dRows += mDelta.rows
	dBytes += mDelta.bytes
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("blobqueue: commit: %w", err)
	}
	q.rows += dRows
	q.bytes += dBytes
	q.pendRefused = 0
	loss.count(q)
	select {
	case q.poke <- struct{}{}:
	default:
	}
	if now.Sub(q.lastMaintain) > time.Second {
		q.lastMaintain = now
		q.maintainLocked(ctx)
	}
	return uint64(id), nil
}

// Close makes further Enqueue calls fail with ErrClosed. Rows already persisted stay queued.
func (q *Queue) Close() {
	q.wmu.Lock()
	q.closed = true
	q.wmu.Unlock()
}
