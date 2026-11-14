// Package intentq is a durable SQLite intent queue: idempotency key, state machine,
// TTL, lease-based claim, per-attempt receipt rows and time+size retention.
//
// Restart contract (litmus test): pending intents resume once, expired ones are
// dropped (never replayed), and an intent left running by a crash becomes
// "uncertain" (a side effect may already have happened), never silently retried.
//
// The caller owns the *sql.DB (WAL + busy_timeout) and the table name; table
// names are interpolated and must be trusted identifiers, values are bound.
package intentq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// State of an intent.
type State string

const (
	Pending   State = "pending"
	Running   State = "running"
	Done      State = "done"
	Failed    State = "failed"
	Uncertain State = "uncertain"
	Expired   State = "expired"
	Cancelled State = "cancelled"
)

func (s State) terminal() bool {
	return s == Done || s == Failed || s == Uncertain || s == Expired || s == Cancelled
}

// ErrNotOwner is returned when an intent is not running under the caller's lease.
var ErrNotOwner = errors.New("intentq: intent is not running under this owner")

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Config for a Queue.
type Config struct {
	Table       string        // trusted identifier; attempts go to <Table>_stages
	DefaultTTL  time.Duration // used when Item.TTL is zero (required > 0)
	Retention   time.Duration // terminal rows older than this are pruned (0 = keep until MaxTerminal)
	MaxTerminal int           // keep at most this many terminal rows (0 = unbounded: only for tests)
	Now         func() time.Time
}

// Item is a submission.
type Item struct {
	Key     string // idempotency key; a resubmit returns the existing id
	Kind    string
	Payload []byte
	TTL     time.Duration
	// Subject groups intents by the thing they act on (e.g. a pane id) so an
	// event about that thing can Wake exactly its pending intents. Optional.
	Subject string
}

// Intent is a stored row.
type Intent struct {
	ID       int64
	Key      string
	Kind     string
	Payload  []byte
	State    State
	Attempt  int
	Reason   string
	Created  time.Time
	NextAt   time.Time
	Expires  time.Time
	Owner    string
	LeaseEnd time.Time
}

// Queue is the durable queue.
type Queue struct {
	db  *sql.DB
	cfg Config
	t   string
}

// New creates the tables if needed.
func New(ctx context.Context, db *sql.DB, cfg Config) (*Queue, error) {
	if !identRE.MatchString(cfg.Table) {
		return nil, fmt.Errorf("intentq: invalid table name %q", cfg.Table)
	}
	if cfg.DefaultTTL <= 0 {
		return nil, errors.New("intentq: DefaultTTL must be > 0")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	q := &Queue{db: db, cfg: cfg, t: cfg.Table}
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, payload BLOB,
			state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, reason TEXT NOT NULL DEFAULT '',
			created_ms INTEGER NOT NULL, next_ms INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
			lease_owner TEXT NOT NULL DEFAULT '', lease_ms INTEGER NOT NULL DEFAULT 0, updated_ms INTEGER NOT NULL,
				subject TEXT NOT NULL DEFAULT '')`, q.t),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_claim ON %s(state, next_ms)`, q.t, q.t),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_subject ON %s(state, subject)`, q.t, q.t),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_stages (
			id INTEGER PRIMARY KEY AUTOINCREMENT, intent_id INTEGER NOT NULL, attempt INTEGER NOT NULL,
			stage TEXT NOT NULL, state TEXT NOT NULL, facts BLOB, at_ms INTEGER NOT NULL)`, q.t),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_stages_intent ON %s_stages(intent_id)`, q.t, q.t),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func ms(t time.Time) int64 { return t.UnixMilli() }

// Enqueue stores the item as pending. created is false when the key already exists
// (the existing id and state are returned; a terminal row blocks reuse of the key
// until pruned, so callers wanting a fresh attempt include a nonce in the key).
func (q *Queue) Enqueue(ctx context.Context, it Item) (id int64, state State, created bool, err error) {
	if it.Key == "" || it.Kind == "" {
		return 0, "", false, errors.New("intentq: key and kind are required")
	}
	ttl := it.TTL
	if ttl <= 0 {
		ttl = q.cfg.DefaultTTL
	}
	now := q.cfg.Now()
	res, err := q.db.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s(key,kind,payload,state,created_ms,next_ms,expires_ms,updated_ms,subject) VALUES(?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(key) DO NOTHING`, q.t),
		it.Key, it.Kind, it.Payload, string(Pending), ms(now), ms(now), ms(now.Add(ttl)), ms(now), it.Subject)
	if err != nil {
		return 0, "", false, err
	}
	n, _ := res.RowsAffected()
	var st string
	err = q.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT id,state FROM %s WHERE key=?`, q.t), it.Key).Scan(&id, &st)
	return id, State(st), n == 1, err
}

const cols = `id,key,kind,payload,state,attempts,reason,created_ms,next_ms,expires_ms,lease_owner,lease_ms`

func scan(r interface{ Scan(...any) error }) (*Intent, error) {
	var in Intent
	var st string
	var c, n, e, l int64
	if err := r.Scan(&in.ID, &in.Key, &in.Kind, &in.Payload, &st, &in.Attempt, &in.Reason, &c, &n, &e, &in.Owner, &l); err != nil {
		return nil, err
	}
	in.State = State(st)
	in.Created, in.NextAt, in.Expires, in.LeaseEnd = time.UnixMilli(c), time.UnixMilli(n), time.UnixMilli(e), time.UnixMilli(l)
	return &in, nil
}

// Claim atomically takes the oldest due, unexpired pending intent under a lease, or returns (nil, nil).
// One statement, so concurrent claimers never get the same row.
func (q *Queue) Claim(ctx context.Context, owner string, lease time.Duration) (*Intent, error) {
	now := q.cfg.Now()
	row := q.db.QueryRowContext(ctx, fmt.Sprintf(
		`UPDATE %s SET state='running', attempts=attempts+1, lease_owner=?, lease_ms=?, updated_ms=?
		 WHERE id=(SELECT id FROM %s WHERE state='pending' AND next_ms<=? AND expires_ms>? ORDER BY next_ms,id LIMIT 1)
		 RETURNING %s`, q.t, q.t, cols), owner, ms(now.Add(lease)), ms(now), ms(now), ms(now))
	in, err := scan(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return in, err
}

// Record appends a stage receipt for an attempt.
func (q *Queue) Record(ctx context.Context, id int64, attempt int, stage, state string, facts []byte) error {
	_, err := q.db.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s_stages(intent_id,attempt,stage,state,facts,at_ms) VALUES(?,?,?,?,?,?)`, q.t),
		id, attempt, stage, state, facts, ms(q.cfg.Now()))
	return err
}

// finish moves a running intent owned by owner to next state, writing the attempt's outcome row in the same tx.
func (q *Queue) finish(ctx context.Context, id int64, owner string, to State, nextAt time.Time, reason string) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	now := q.cfg.Now()
	var attempt int
	err = tx.QueryRowContext(ctx, fmt.Sprintf(
		`UPDATE %s SET state=?, reason=?, next_ms=?, lease_owner='', lease_ms=0, updated_ms=?
		 WHERE id=? AND state='running' AND lease_owner=? RETURNING attempts`, q.t),
		string(to), reason, ms(nextAt), ms(now), id, owner).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotOwner
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s_stages(intent_id,attempt,stage,state,facts,at_ms) VALUES(?,?,?,?,?,?)`, q.t),
		id, attempt, "outcome", string(to), []byte(reason), ms(now)); err != nil {
		return err
	}
	return tx.Commit()
}

// Finish ends a running intent in a terminal state (Done, Failed, Uncertain, Cancelled).
func (q *Queue) Finish(ctx context.Context, id int64, owner string, to State, reason string) error {
	if !to.terminal() || to == Expired {
		return fmt.Errorf("intentq: %q is not a valid Finish state", to)
	}
	return q.finish(ctx, id, owner, to, q.cfg.Now(), reason)
}

// Defer puts a running intent back to pending, due at until (a transient refusal: wait, never drop).
func (q *Queue) Defer(ctx context.Context, id int64, owner string, until time.Time, reason string) error {
	return q.finish(ctx, id, owner, Pending, until, reason)
}

// Recovered lists intents changed by Recover.
type Recovered struct {
	ID    int64
	State State
}

// Recover is the boot/backstop sweep: running intents whose lease ended become uncertain
// (a keystroke may have gone out), pending intents past their TTL become expired. Neither is replayed.
func (q *Queue) Recover(ctx context.Context) ([]Recovered, error) {
	now := q.cfg.Now()
	var out []Recovered
	for _, step := range []struct {
		where string
		to    State
		why   string
	}{
		{`state='running' AND lease_ms<=?`, Uncertain, "lease ended while running; outcome unknown"},
		{`state='pending' AND expires_ms<=?`, Expired, "ttl elapsed before it could run"},
	} {
		rows, err := q.db.QueryContext(ctx, fmt.Sprintf(
			`UPDATE %s SET state=?, reason=?, lease_owner='', lease_ms=0, updated_ms=? WHERE %s RETURNING id`, q.t, step.where),
			string(step.to), step.why, ms(now), ms(now))
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return out, err
			}
			out = append(out, Recovered{ID: id, State: step.to})
		}
		if err := rows.Close(); err != nil {
			return out, err
		}
		if err := rows.Err(); err != nil {
			return out, err
		}
	}
	return out, nil
}

// Prune bounds the table by time AND size, whichever bites first, deleting only terminal rows
// and their stage rows. Returns the number of intents deleted.
func (q *Queue) Prune(ctx context.Context) (int64, error) {
	now := q.cfg.Now()
	term := `state IN ('done','failed','uncertain','expired','cancelled')`
	var total int64
	del := func(where string, args ...any) error {
		if _, err := q.db.ExecContext(ctx, fmt.Sprintf(
			`DELETE FROM %s_stages WHERE intent_id IN (SELECT id FROM %s WHERE %s)`, q.t, q.t, where), args...); err != nil {
			return err
		}
		res, err := q.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s`, q.t, where), args...)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		total += n
		return nil
	}
	if q.cfg.Retention > 0 {
		if err := del(term+` AND updated_ms<?`, ms(now.Add(-q.cfg.Retention))); err != nil {
			return total, err
		}
	}
	if q.cfg.MaxTerminal > 0 {
		if err := del(fmt.Sprintf(`%s AND id NOT IN (SELECT id FROM %s WHERE %s ORDER BY updated_ms DESC, id DESC LIMIT %d)`,
			term, q.t, term, q.cfg.MaxTerminal)); err != nil {
			return total, err
		}
	}
	return total, nil
}

// Wake makes every PENDING, unexpired intent with this subject due now, so an
// event about the subject (a pane going idle) retries its deferred intents at
// once instead of waiting out the backoff. Running and terminal rows are never
// touched. It returns how many rows were woken.
func (q *Queue) Wake(ctx context.Context, subject string) (int64, error) {
	if subject == "" {
		return 0, errors.New("intentq: Wake needs a subject")
	}
	now := q.cfg.Now()
	res, err := q.db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET next_ms=?, updated_ms=? WHERE state='pending' AND subject=? AND next_ms>? AND expires_ms>?`, q.t),
		ms(now), ms(now), subject, ms(now), ms(now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// WakeAll makes every PENDING, unexpired intent due now. It is the boot sweep:
// backoff timers were set by a process that no longer exists, and a pane may have
// gone quiet while no trigger was watching. Running and terminal rows are never touched.
func (q *Queue) WakeAll(ctx context.Context) (int64, error) {
	now := q.cfg.Now()
	res, err := q.db.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET next_ms=?, updated_ms=? WHERE state='pending' AND next_ms>? AND expires_ms>?`, q.t),
		ms(now), ms(now), ms(now), ms(now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// NextDue returns when the earliest unexpired pending intent becomes due, so a
// worker can sleep exactly that long. ok is false when nothing is pending.
func (q *Queue) NextDue(ctx context.Context) (t time.Time, ok bool, err error) {
	var n sql.NullInt64
	err = q.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT MIN(next_ms) FROM %s WHERE state='pending' AND expires_ms>?`, q.t), ms(q.cfg.Now())).Scan(&n)
	if err != nil || !n.Valid {
		return time.Time{}, false, err
	}
	return time.UnixMilli(n.Int64), true, nil
}

// Get returns one intent by id.
func (q *Queue) Get(ctx context.Context, id int64) (*Intent, error) {
	return scan(q.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE id=?`, cols, q.t), id))
}

// OldestPendingAge is the status metric: how long the oldest due pending intent has waited (0 when none).
func (q *Queue) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	var c sql.NullInt64
	err := q.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT MIN(created_ms) FROM %s WHERE state='pending'`, q.t)).Scan(&c)
	if err != nil || !c.Valid {
		return 0, err
	}
	return q.cfg.Now().Sub(time.UnixMilli(c.Int64)), nil
}
