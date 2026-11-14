package intentq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func open(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newQ(t *testing.T, path string, c *clock, mod func(*Config)) *Queue {
	t.Helper()
	cfg := Config{Table: "intents", DefaultTTL: time.Hour, Now: c.now}
	if mod != nil {
		mod(&cfg)
	}
	q, err := New(context.Background(), open(t, path), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestIdempotentEnqueue(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	id1, _, created1, err := q.Enqueue(ctx, Item{Key: "k", Kind: "clear"})
	if err != nil || !created1 {
		t.Fatalf("first enqueue: %v created=%v", err, created1)
	}
	id2, st, created2, err := q.Enqueue(ctx, Item{Key: "k", Kind: "clear"})
	if err != nil || created2 || id2 != id1 || st != Pending {
		t.Fatalf("resubmit must return the existing id: id=%d/%d created=%v state=%s err=%v", id1, id2, created2, st, err)
	}
}

func TestClaimIsExclusiveAndDeferWaits(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	id, _, _, _ := q.Enqueue(ctx, Item{Key: "k", Kind: "clear"})
	in, err := q.Claim(ctx, "a", time.Minute)
	if err != nil || in == nil || in.ID != id || in.Attempt != 1 {
		t.Fatalf("claim: %+v %v", in, err)
	}
	if again, _ := q.Claim(ctx, "b", time.Minute); again != nil {
		t.Fatal("a running intent must not be claimed twice")
	}
	if err := q.Defer(ctx, id, "b", c.now().Add(time.Second), "busy"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("defer by non-owner must fail, got %v", err)
	}
	if err := q.Defer(ctx, id, "a", c.now().Add(10*time.Second), "busy: a turn is running"); err != nil {
		t.Fatal(err)
	}
	if early, _ := q.Claim(ctx, "a", time.Minute); early != nil {
		t.Fatal("a deferred intent must not run before it is due")
	}
	c.advance(11 * time.Second)
	in, _ = q.Claim(ctx, "a", time.Minute)
	if in == nil || in.Attempt != 2 {
		t.Fatalf("due intent must be claimed with attempt 2: %+v", in)
	}
	if err := q.Finish(ctx, id, "a", Done, "ok"); err != nil {
		t.Fatal(err)
	}
	var outcomes int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM intents_stages WHERE intent_id=? AND stage='outcome'`, id).Scan(&outcomes); err != nil || outcomes != 2 {
		t.Fatalf("every attempt must have an outcome row: %d %v", outcomes, err)
	}
}

// Restart: pending resumes once, expired is dropped (never replayed), running becomes uncertain.
func TestRestartRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "q.db")
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, path, c, nil)
	pend, _, _, _ := q.Enqueue(ctx, Item{Key: "pending", Kind: "clear"})
	short, _, _, _ := q.Enqueue(ctx, Item{Key: "short", Kind: "clear", TTL: time.Minute})
	run, _, _, _ := q.Enqueue(ctx, Item{Key: "run", Kind: "clear"})
	if in, _ := q.Claim(ctx, "d", time.Minute); in == nil || in.ID != pend {
		t.Fatalf("expected the oldest to be claimed first: %+v", in)
	}
	if err := q.Defer(ctx, pend, "d", c.now(), "busy"); err != nil {
		t.Fatal(err)
	}
	// claim `short`? make `run` running instead: advance so order is pend, short, run; claim until run is running.
	for {
		in, _ := q.Claim(ctx, "d", time.Minute)
		if in == nil {
			t.Fatal("run never claimed")
		}
		if in.ID == run {
			break
		}
		if err := q.Defer(ctx, in.ID, "d", c.now().Add(time.Hour), "park"); err != nil {
			t.Fatal(err)
		}
	}
	// daemon dies; time passes beyond lease and the short TTL; a new process opens the same file
	c.advance(2 * time.Minute)
	q2 := newQ(t, path, c, nil)
	rec, err := q2.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]State{}
	for _, r := range rec {
		got[r.ID] = r.State
	}
	if got[run] != Uncertain {
		t.Fatalf("an intent running at crash must be uncertain, never retried: %v", got)
	}
	if got[short] != Expired {
		t.Fatalf("an intent past its TTL must expire, never replay: %v", got)
	}
	if again, _ := q2.Recover(ctx); len(again) != 0 {
		t.Fatalf("recover must be idempotent: %v", again)
	}
	if st, _ := q2.Get(ctx, pend); st.State != Pending {
		t.Fatalf("an unexpired pending intent must resume: %+v", st)
	}
}

func TestPruneTimeAndSize(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, func(cfg *Config) { cfg.Retention = time.Hour; cfg.MaxTerminal = 2 })
	var ids []int64
	for i := 0; i < 5; i++ {
		id, _, _, _ := q.Enqueue(ctx, Item{Key: string(rune('a' + i)), Kind: "clear"})
		in, _ := q.Claim(ctx, "d", time.Minute)
		if in == nil || in.ID != id {
			t.Fatalf("claim order: %+v", in)
		}
		if err := q.Finish(ctx, id, "d", Done, "ok"); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		c.advance(time.Second)
	}
	live, _, _, _ := q.Enqueue(ctx, Item{Key: "live", Kind: "clear"})
	n, err := q.Prune(ctx)
	if err != nil || n != 3 {
		t.Fatalf("size cap must prune the 3 oldest terminal rows: n=%d err=%v", n, err)
	}
	if in, _ := q.Get(ctx, live); in == nil || in.State != Pending {
		t.Fatal("prune must never touch a pending intent")
	}
	c.advance(2 * time.Hour)
	if n, _ := q.Prune(ctx); n != 2 {
		t.Fatalf("time retention must prune the rest: %d", n)
	}
	var stages int
	_ = q.db.QueryRow(`SELECT COUNT(*) FROM intents_stages`).Scan(&stages)
	if stages != 0 {
		t.Fatalf("stage rows must go with their intents: %d (ids %v)", stages, ids)
	}
}

func TestInvalidTable(t *testing.T) {
	db := open(t, filepath.Join(t.TempDir(), "q.db"))
	if _, err := New(context.Background(), db, Config{Table: "x; DROP TABLE y", DefaultTTL: time.Hour}); err == nil {
		t.Fatal("a non-identifier table name must be refused")
	}
}

// Wake makes a subject's deferred intents due now; it matches the subject
// exactly (a LIKE on a pane id would wake %1 when %21 goes idle) and never
// touches running or terminal rows.
func TestWakeIsExactAndPendingOnly(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	ids := map[string]int64{}
	for _, s := range []string{"%1", "%21", "%11"} {
		ids[s], _, _, _ = q.Enqueue(ctx, Item{Key: "k" + s, Kind: "clear", Subject: s})
	}
	for range ids { // claim and defer each 1h out
		in, _ := q.Claim(ctx, "a", time.Minute)
		if err := q.Defer(ctx, in.ID, "a", c.now().Add(time.Hour), "busy"); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := q.NextDue(ctx); !ok {
		t.Fatal("NextDue must see the deferred rows")
	}
	n, err := q.Wake(ctx, "%1")
	if err != nil || n != 1 {
		t.Fatalf("Wake(%%1) must wake exactly one row, got %d %v", n, err)
	}
	in, _ := q.Claim(ctx, "a", time.Minute)
	if in == nil || in.ID != ids["%1"] {
		t.Fatalf("only %%1 may be due: %+v", in)
	}
	if n, _ := q.Wake(ctx, "%1"); n != 0 {
		t.Fatalf("a running row must not be woken, got %d", n)
	}
	if again, _ := q.Claim(ctx, "a", time.Minute); again != nil {
		t.Fatalf("%%21/%%11 must stay deferred, claimed %+v", again)
	}
	if _, err := q.Wake(ctx, ""); err == nil {
		t.Fatal("an empty subject must be refused")
	}
}

func TestNextDue(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	if _, ok, err := q.NextDue(ctx); ok || err != nil {
		t.Fatalf("empty queue: ok=%v err=%v", ok, err)
	}
	id, _, _, _ := q.Enqueue(ctx, Item{Key: "k", Kind: "clear"})
	in, _ := q.Claim(ctx, "a", time.Minute)
	due := c.now().Add(30 * time.Second)
	if err := q.Defer(ctx, id, "a", due, "busy"); err != nil || in == nil {
		t.Fatal(err)
	}
	got, ok, err := q.NextDue(ctx)
	if err != nil || !ok || !got.Equal(due) {
		t.Fatalf("NextDue=%v ok=%v err=%v, want %v", got, ok, err, due)
	}
}
