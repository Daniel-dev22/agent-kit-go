package eventoutbox

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// commits counts real SQLite commits (autocommit statements included) on every connection opened
// by this test binary, so a test can assert "ONE transaction" rather than infer it from timing.
var commits atomic.Int64

func init() {
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, _ string) error {
		conn.(sqlite.HookRegisterer).RegisterCommitHook(func() int32 {
			commits.Add(1)
			return 0
		})
		return nil
	})
}

// logSink is a slog handler target that records the rendered lines.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.lines = append(l.lines, string(p))
	l.mu.Unlock()
	return len(p), nil
}

func (l *logSink) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			n++
		}
	}
	return n
}

func sinkLogger() (*slog.Logger, *logSink) {
	s := &logSink{}
	return slog.New(slog.NewTextHandler(s, nil)), s
}

func postsNow(c *ctl) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.posts
}

func waitPosts(t *testing.T, c *ctl, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if postsNow(c) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("posts = %d, want >= %d", postsNow(c), want)
}

// ---- L1: epoch_changed ----

func epochReply(changed bool) func(*ctl, http.ResponseWriter, []map[string]any) bool {
	return func(_ *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		fmt.Fprintf(w, `{"accepted":%d,"duplicate":0,"epoch_changed":%v}`, len(evs), changed)
		return true
	}
}

func TestEpochChangedIsNotAnAckWhenOptedIn(t *testing.T) {
	var sent, changed int
	c := &ctl{respond: epochReply(true)}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		cfg.OnSent = func() { sent++ }
		cfg.OnEpochChanged = func() { changed++ }
	})
	enq(t, o, 3, false)
	if !o.drain(context.Background()) {
		t.Fatal("a stored-not-applied reply must not back off")
	}
	if sent != 0 || changed != 1 {
		t.Fatalf("OnSent=%d OnEpochChanged=%d, want 0 and 1", sent, changed)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("rows were stored by the controller and must be deleted (a replay is only a duplicate)")
	}
	if st := o.Stats(); st.EpochChanged != 1 {
		t.Fatalf("Stats.EpochChanged = %d", st.EpochChanged)
	}
}

func TestEpochChangedFalseStillAcks(t *testing.T) {
	var sent, changed int
	c := &ctl{respond: epochReply(false)}
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) {
		cfg.OnSent = func() { sent++ }
		cfg.OnEpochChanged = func() { changed++ }
	})
	enq(t, o, 2, false)
	o.drain(context.Background())
	if sent != 1 || changed != 0 || o.Stats().EpochChanged != 0 {
		t.Fatalf("OnSent=%d OnEpochChanged=%d stats=%d", sent, changed, o.Stats().EpochChanged)
	}
}

// Default (no OnEpochChanged): OnSent fires on every 2xx exactly as in v0.9.0; the stat still counts.
func TestEpochChangedWithoutOptInKeepsOnSent(t *testing.T) {
	var sent int
	c := &ctl{respond: epochReply(true)}
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) { cfg.OnSent = func() { sent++ } })
	enq(t, o, 2, false)
	o.drain(context.Background())
	if sent != 1 || o.Stats().EpochChanged != 1 {
		t.Fatalf("OnSent=%d stats=%d", sent, o.Stats().EpochChanged)
	}
	// and neither hook set at all is fine
	o2, _ := newBatchT(t, &ctl{respond: epochReply(true)}, nil)
	enq(t, o2, 1, false)
	o2.drain(context.Background())
}

// ---- L2: pokes during a conflict ----

func TestEnqueueDuringEpochConflictSendsNothing(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(409)
		io.WriteString(w, `{"code":"epoch_conflict"}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.DrainInterval = time.Hour
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	defer o.Close()
	enq(t, o, 1, false)
	waitPosts(t, c, 1) // the first request discovers the conflict
	time.Sleep(50 * time.Millisecond)
	enq(t, o, 25, false) // N enqueues during the conflict
	time.Sleep(200 * time.Millisecond)
	if n := postsNow(c); n != 1 {
		t.Fatalf("posts = %d, want 1: an Enqueue during a conflict must not send (retry on the DrainInterval only)", n)
	}
	if rowsLeft(t, db) != 26 || len(dead) != 0 {
		t.Fatalf("rows=%d dead=%d", rowsLeft(t, db), len(dead))
	}
}

// The conflict does not latch: once the controller accepts again the queue drains.
func TestConflictCadenceStillRetriesAndRecovers(t *testing.T) {
	var mu sync.Mutex
	conflict := true
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		mu.Lock()
		defer mu.Unlock()
		if conflict {
			w.WriteHeader(409)
			io.WriteString(w, `{"code":"epoch_conflict"}`)
			return true
		}
		return false
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.DrainInterval = 60 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	defer o.Close()
	enq(t, o, 2, false)
	waitPosts(t, c, 3) // retried on the cadence
	mu.Lock()
	conflict = false
	mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && rowsLeft(t, db) != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("did not drain after the conflict cleared")
	}
}

// ---- L6: one transaction, one log line per failed pass ----

func TestFailRecordsAPassInOneTransaction(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(503)
		return true
	}}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.MaxEvents = 60 })
	enq(t, o, 60, false)
	before := commits.Load()
	if o.drain(context.Background()) {
		t.Fatal("a 503 must back off")
	}
	if n := commits.Load() - before; n != 1 {
		t.Fatalf("commits for one failed pass over 60 rows = %d, want 1", n)
	}
	var minAttempts, maxAttempts int
	if err := db.QueryRow(`SELECT MIN(attempts), MAX(attempts) FROM batch_events`).Scan(&minAttempts, &maxAttempts); err != nil {
		t.Fatal(err)
	}
	if minAttempts != 1 || maxAttempts != 1 {
		t.Fatalf("attempts min/max = %d/%d, want 1/1 on every row", minAttempts, maxAttempts)
	}
}

func TestFailLogsOnceWithACountWhenRecordingFails(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(503)
		return true
	}}
	lg, sink := sinkLogger()
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.Logger = lg })
	enq(t, o, 40, false)
	if _, err := db.Exec(`CREATE TRIGGER no_attempts BEFORE UPDATE ON batch_events BEGIN SELECT RAISE(ABORT, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	o.drain(context.Background())
	if n := sink.count("record failure failed"); n != 1 {
		t.Fatalf("'record failure failed' lines = %d, want 1 per pass", n)
	}
	if sink.count("rows=40") != 1 {
		t.Fatalf("the line must carry the row count: %v", sink.lines)
	}
	if rowsLeft(t, db) != 40 {
		t.Fatal("a failed bookkeeping write must not lose rows")
	}
}

// ---- L5: supersede-by-key ----

func supersedeCfg(dead *[]DeadLetter) func(*BatchConfig) {
	return func(cfg *BatchConfig) {
		cfg.SupersedeKey = func(kind string, _ []byte) string {
			if kind == "agent_started" {
				return "agent_started"
			}
			return ""
		}
		cfg.OnDead = func(d DeadLetter) { *dead = append(*dead, d) }
	}
}

func enqKind(t *testing.T, o *BatchOutbox, kind string, i int, protected bool) int64 {
	t.Helper()
	id, err := o.Enqueue(kind, []byte(fmt.Sprintf(`{"kind":%q,"i":%d}`, kind, i)), protected)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seqs(t *testing.T, db *sql.DB) []int64 {
	t.Helper()
	rs, err := db.Query(`SELECT id FROM batch_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []int64
	for rs.Next() {
		var id int64
		if err := rs.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func TestSupersedeBoundsRepeatedKeyedEventsToOne(t *testing.T) {
	var dead []DeadLetter
	c := &ctl{}
	o, db := newBatchT(t, c, supersedeCfg(&dead))
	a := enqKind(t, o, "agent_started", 0, true)
	other := enqKind(t, o, "node_removed", 0, true)
	var last int64
	for i := 1; i <= 50; i++ {
		last = enqKind(t, o, "agent_started", i, true)
	}
	if last != a {
		t.Fatalf("a replacement keeps the oldest seq: got %d, want %d", last, a)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{a, other}) {
		t.Fatalf("rows = %v, want [%d %d] (one agent_started, the unkeyed event untouched)", got, a, other)
	}
	if !(a < other) {
		t.Fatalf("seq not monotonic: %d %d", a, other)
	}
	if o.LastSeq() != other {
		t.Fatalf("LastSeq = %d, want %d", o.LastSeq(), other)
	}
	if st := o.Stats(); st.Superseded != 50 || st.Pending != 2 {
		t.Fatalf("stats = %+v", st)
	}
	if len(dead) != 0 || o.Stats().Dead != 0 {
		t.Fatal("a superseded row is not a dead letter")
	}
	// FIFO and Ack are unaffected: delivery is in id order, exactly the survivors, and the queue empties.
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("did not drain")
	}
	if fmt.Sprint(c.delivery) != fmt.Sprint([]int64{a, other}) {
		t.Fatalf("delivery = %v", c.delivery)
	}
	// a new keyed event after the queue emptied still gets a higher seq (AUTOINCREMENT, never reused)
	if n := enqKind(t, o, "agent_started", 99, true); n <= other {
		t.Fatalf("seq reused: %d <= %d", n, other)
	}
}

func TestSupersedeKeepsTheStrongestProtection(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	enqKind(t, o, "agent_started", 0, true)
	enqKind(t, o, "agent_started", 1, false)
	var p int
	if err := db.QueryRow(`SELECT protected FROM batch_events`).Scan(&p); err != nil || p != 1 {
		t.Fatalf("protected = %d err=%v, want 1 (a successor must not downgrade a recovery event)", p, err)
	}
}

func TestSupersedeOnlyWithinTheSameKey(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, func(cfg *BatchConfig) {
		cfg.SupersedeKey = func(kind string, _ []byte) string { return kind }
	})
	enqKind(t, o, "a", 0, false)
	enqKind(t, o, "b", 0, false)
	enqKind(t, o, "a", 1, false)
	enqKind(t, o, "b", 1, false)
	if n := rowsLeft(t, db); n != 2 {
		t.Fatalf("rows = %d, want 2 (one per key)", n)
	}
}

func TestSupersedeIsOneCommitAndUnkeyedEnqueueIsUnchanged(t *testing.T) {
	var dead []DeadLetter
	o, _ := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	enqKind(t, o, "agent_started", 0, true)
	before := commits.Load()
	enqKind(t, o, "agent_started", 1, true)
	if n := commits.Load() - before; n != 1 {
		t.Fatalf("a superseding enqueue = %d commits, want 1", n)
	}
	before = commits.Load()
	enqKind(t, o, "node_removed", 1, true)
	if n := commits.Load() - before; n != 1 {
		t.Fatalf("an unkeyed enqueue = %d commits, want 1", n)
	}
}

// A row that is in flight when its successor arrives: the stale ack/failure for the removed row is harmless.
func TestSupersedeWhileInFlightIsHarmless(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	enqKind(t, o, "agent_started", 0, true)
	rows, err := o.load()
	if err != nil || len(rows) != 1 {
		t.Fatalf("load: %v %d", err, len(rows))
	}
	last := enqKind(t, o, "agent_started", 1, true) // replaces the row the drainer holds
	o.fail(rows, "post", io.EOF)                    // the in-flight send failed: updates a row that is gone
	if err := o.deleteIDs([]int64{rows[0].id}); err != nil {
		t.Fatal(err)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{last}) {
		t.Fatalf("rows = %v", got)
	}
}

// Opting in migrates an existing database in place, idempotently; a database that never opts in
// keeps its schema.
func TestSupersedeColumnMigratesAnExistingDatabase(t *testing.T) {
	db := newDB(t)
	o0, err := NewBatch(db, http.DefaultClient, BatchConfig{URL: "http://127.0.0.1:1/x"})
	if err != nil {
		t.Fatal(err)
	}
	if n := o0.count(); n != 0 {
		t.Fatal(n)
	}
	hasCol := func() int {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('batch_events') WHERE name='supersede_key'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if hasCol() != 0 {
		t.Fatal("a non-opted-in database must keep its schema")
	}
	cfg := BatchConfig{URL: "http://127.0.0.1:1/x", SupersedeKey: func(string, []byte) string { return "k" }}
	for range 2 { // second open: the column already exists
		if _, err := NewBatch(db, http.DefaultClient, cfg); err != nil {
			t.Fatal(err)
		}
	}
	if hasCol() != 1 {
		t.Fatal("supersede_key column missing after opting in")
	}
}

func TestSupersedeSchemaErrorBranches(t *testing.T) {
	db := newDB(t)
	cfg := BatchConfig{URL: "http://127.0.0.1:1/x", SupersedeKey: func(string, []byte) string { return "k" }}
	if _, err := NewBatch(db, http.DefaultClient, BatchConfig{URL: cfg.URL}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if err := ensureSupersedeColumn(db); err == nil {
		t.Fatal("expected an error on a closed database")
	}
	db2 := newDB(t)
	if _, err := db2.Exec(`CREATE TABLE batch_events (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := ensureSupersedeColumn(db2); err != nil {
		t.Fatal(err) // adds the column to a bare table
	}
	if _, err := db2.Exec(`DROP TABLE batch_events`); err != nil {
		t.Fatal(err)
	}
	if err := ensureSupersedeColumn(db2); err == nil {
		t.Fatal("expected an error when the table is missing")
	}
}

func TestEnqueueSupersedingDatabaseFailure(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	db.Close()
	if _, err := o.Enqueue("agent_started", []byte(`{"kind":"agent_started"}`), true); err == nil {
		t.Fatal("expected an enqueue error on a closed database")
	}
}

// ---- L8: bisect on an allowlisted refusal code ----

// refuseWhenBad refuses any request containing an event with bad:true using code, else acks.
func refuseWhenBad(status int, code string) func(*ctl, http.ResponseWriter, []map[string]any) bool {
	return func(_ *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		for _, e := range evs {
			if b, _ := e["bad"].(bool); b {
				w.WriteHeader(status)
				fmt.Fprintf(w, `{"code":%q}`, code)
				return true
			}
		}
		return false
	}
}

func enqBad(t *testing.T, o *BatchOutbox, n, badAt int) {
	t.Helper()
	for i := 0; i < n; i++ {
		p := fmt.Sprintf(`{"kind":"k","i":%d}`, i)
		if i == badAt {
			p = fmt.Sprintf(`{"kind":"k","i":%d,"bad":true}`, i)
		}
		if _, err := o.Enqueue("k", []byte(p), true); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBisectOnAllowlistedCodeDeadLettersOnlyTheOffendingEvent(t *testing.T) {
	for _, badAt := range []int{0, 5, 9} {
		t.Run(fmt.Sprint(badAt), func(t *testing.T) {
			c := &ctl{respond: refuseWhenBad(422, "event_invalid")}
			var dead []DeadLetter
			o, db := newBatchT(t, c, func(cfg *BatchConfig) {
				holdCfg(&dead)(cfg)
				cfg.BisectOnRefusalCodes = []string{"other", "event_invalid"}
			})
			enqBad(t, o, 10, badAt)
			for i := 0; i < 20 && rowsLeft(t, db) > 0; i++ {
				if !o.drain(context.Background()) {
					t.Fatal("an allowlisted refusal must bisect, not back off")
				}
			}
			if rowsLeft(t, db) != 0 {
				t.Fatalf("rows left = %d", rowsLeft(t, db))
			}
			if len(dead) != 1 || dead[0].Seq != int64(badAt+1) || dead[0].Code != "event_invalid" {
				t.Fatalf("dead = %+v, want exactly seq %d with the refusal code", dead, badAt+1)
			}
			if len(c.delivery) != 9 {
				t.Fatalf("delivered %d good events, want 9: %v", len(c.delivery), c.delivery)
			}
		})
	}
}

func TestNonAllowlistedCodeStillHoldsTheRows(t *testing.T) {
	for _, code := range []string{"wrong_site", "other_code", ""} {
		t.Run("code="+code, func(t *testing.T) {
			c := &ctl{respond: refuseWhenBad(422, code)}
			var dead []DeadLetter
			o, db := newBatchT(t, c, func(cfg *BatchConfig) {
				holdCfg(&dead)(cfg)
				cfg.BisectOnRefusalCodes = []string{"event_invalid"}
			})
			enqBad(t, o, 5, 2)
			if o.drain(context.Background()) {
				t.Fatal("a non-allowlisted refusal must hold and back off")
			}
			if rowsLeft(t, db) != 5 || len(dead) != 0 || c.posts != 1 {
				t.Fatalf("rows=%d dead=%d posts=%d", rowsLeft(t, db), len(dead), c.posts)
			}
		})
	}
}

// An envelope refusal reaches a single-event request too. With a non-allowlisted code the whole
// queue must survive however many events there are (never key bisection on "a batch of one is refused").
func TestEnvelopeRefusalOfASingleEventIsNeverDeadLettered(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(409)
		io.WriteString(w, `{"code":"wrong_site"}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.BisectOnRefusalCodes = []string{"event_invalid"}
		cfg.MaxEvents = 1
	})
	enq(t, o, 3, true)
	for range 3 {
		o.drain(context.Background())
	}
	if rowsLeft(t, db) != 3 || len(dead) != 0 {
		t.Fatalf("rows=%d dead=%d", rowsLeft(t, db), len(dead))
	}
}

// Without HoldOnRefusal the allowlist changes nothing: the default already bisects every refusal.
func TestBisectAllowlistWithoutHoldIsInert(t *testing.T) {
	c := &ctl{respond: refuseWhenBad(422, "event_invalid")}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) }
		cfg.BisectOnRefusalCodes = []string{"event_invalid"}
	})
	enqBad(t, o, 4, 1)
	for i := 0; i < 10 && rowsLeft(t, db) > 0; i++ {
		o.drain(context.Background())
	}
	if len(dead) != 1 || rowsLeft(t, db) != 0 {
		t.Fatalf("dead=%d rows=%d", len(dead), rowsLeft(t, db))
	}
}

func TestEpochConflictOutranksTheBisectAllowlist(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(409)
		io.WriteString(w, `{"code":"epoch_conflict"}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.BisectOnRefusalCodes = []string{"epoch_conflict"}
	})
	enq(t, o, 2, false)
	if !o.drain(context.Background()) || rowsLeft(t, db) != 2 || len(dead) != 0 || !o.conflicted {
		t.Fatal("a conflict is held as a non-error even when someone lists it")
	}
}

// ---- error branches of the v0.9.1 write paths ----

func TestEnqueueSupersedingFailsAtEachStatement(t *testing.T) {
	for name, setup := range map[string][]string{
		"select": {`DROP INDEX batch_events_supersede`, `ALTER TABLE batch_events DROP COLUMN supersede_key`},
		"update": {`CREATE TRIGGER t BEFORE UPDATE ON batch_events BEGIN SELECT RAISE(ABORT, 'boom'); END`},
		// two unacked same-key rows (a crash-era leftover): the younger one is deleted
		"delete": {`INSERT INTO batch_events (kind, payload, created_at, supersede_key) VALUES ('agent_started', '{}', 0, 'agent_started')`,
			`CREATE TRIGGER t BEFORE DELETE ON batch_events BEGIN SELECT RAISE(ABORT, 'boom'); END`},
	} {
		t.Run(name, func(t *testing.T) {
			var dead []DeadLetter
			o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
			enqKind(t, o, "agent_started", 0, true) // so the update/delete statements have a target
			for _, q := range setup {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := o.Enqueue("agent_started", []byte(`{"kind":"agent_started"}`), true); err == nil {
				t.Fatal("expected an error")
			}
			if o.Stats().Superseded != 0 {
				t.Fatal("a failed transaction must not count a supersession")
			}
		})
	}
}

func TestExecEachPrepareAndExecErrors(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	enq(t, o, 1, false)
	if _, err := db.Exec(`CREATE TRIGGER t BEFORE DELETE ON batch_events BEGIN SELECT RAISE(ABORT, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	if err := o.deleteIDs([]int64{1}); err == nil {
		t.Fatal("expected an exec error")
	}
	if _, err := db.Exec(`DROP TABLE batch_events`); err != nil {
		t.Fatal(err)
	}
	if err := o.deleteIDs([]int64{1}); err == nil {
		t.Fatal("expected a prepare error")
	}
}

func TestEnsureSupersedeColumnAlterAndIndexErrors(t *testing.T) {
	db := newDB(t)
	if _, err := db.Exec(`CREATE VIEW batch_events AS SELECT 1 AS id`); err != nil {
		t.Fatal(err)
	}
	if err := ensureSupersedeColumn(db); err == nil {
		t.Fatal("expected an ALTER error on a view")
	}
	db2 := newDB(t)
	if _, err := db2.Exec(`CREATE VIEW batch_events AS SELECT 1 AS id, NULL AS supersede_key`); err != nil {
		t.Fatal(err)
	}
	if err := ensureSupersedeColumn(db2); err == nil {
		t.Fatal("expected an index error on a view")
	}
}

func TestNewBatchSupersedeSchemaFailureIsReturned(t *testing.T) {
	db := newDB(t)
	if _, err := NewBatch(db, http.DefaultClient, BatchConfig{URL: "http://127.0.0.1:1/x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA query_only = ON`); err != nil { // reads fine, the ALTER fails
		t.Fatal(err)
	}
	_, err := NewBatch(db, http.DefaultClient, BatchConfig{URL: "http://127.0.0.1:1/x", SupersedeKey: func(string, []byte) string { return "k" }})
	if err == nil {
		t.Fatal("expected the schema error from NewBatch")
	}
}

func TestZeroRowsAreNoTransaction(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, nil)
	before := commits.Load()
	o.fail(nil, "post", io.EOF)
	if err := o.deleteIDs(nil); err != nil || commits.Load() != before {
		t.Fatal("an empty pass must not open a transaction")
	}
}

func TestDeadLetterDeleteFailureKeepsTheRows(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	enq(t, o, 2, false)
	rows, err := o.load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER t BEFORE DELETE ON batch_events BEGIN SELECT RAISE(ABORT, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	o.deadLetter(rows, "refused", "HTTP 422")
	if rowsLeft(t, db) != 2 || o.Stats().Dead != 0 {
		t.Fatal("a failed delete must keep the rows and report no dead letter")
	}
}

func TestRejectedWithoutACodeIsDeadLetteredAsRefused(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		io.WriteString(w, `{"accepted":0,"duplicate":0,"rejected":[{"seq":1,"retryable":false}]}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) } })
	enq(t, o, 1, false)
	o.drain(context.Background())
	if len(dead) != 1 || dead[0].Code != "refused" || rowsLeft(t, db) != 0 {
		t.Fatalf("dead=%+v", dead)
	}
}

func TestBuildFailureAfterLoadRetries(t *testing.T) {
	calls := 0
	o, db := newBatchT(t, &ctl{}, func(cfg *BatchConfig) {
		cfg.Envelope = func(string) map[string]any {
			calls++
			if calls > 1 { // load() succeeds, build() fails
				return map[string]any{"bad": make(chan int)}
			}
			return map[string]any{}
		}
	})
	enq(t, o, 1, false)
	calls = 0 // drain: load() is call 1 (ok), build() is call 2 (fails)
	if o.drain(context.Background()) || rowsLeft(t, db) != 1 {
		t.Fatal("a build failure must keep the row and back off")
	}
}

// A one-code allowlist builds the set too, and the bisect halves the batch (not thirds, not floor 0).
func TestBisectSingleCodeAllowlistHalvesTheBatch(t *testing.T) {
	c := &ctl{respond: refuseWhenBad(422, "event_invalid")}
	var dead []DeadLetter
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.BisectOnRefusalCodes = []string{"event_invalid"}
	})
	enqBad(t, o, 10, 7)
	rows, err := o.load()
	if err != nil || len(rows) != 10 {
		t.Fatalf("load: %v %d", err, len(rows))
	}
	if got := o.send(context.Background(), rows); got != sentSplit || o.limit != 5 {
		t.Fatalf("outcome=%v limit=%d, want sentSplit and a limit of 5", got, o.limit)
	}
	// and an odd batch rounds down but never below one
	if got := o.send(context.Background(), rows[6:9]); got != sentSplit || o.limit != 1 {
		t.Fatalf("outcome=%v limit=%d, want sentSplit and a limit of 1", got, o.limit)
	}
}
