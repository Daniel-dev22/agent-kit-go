package intentq

import (
	"context"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// probeHook is called from inside SQLite (a trigger calls intentq_probe()), at the
// exact statement boundary a test wants to observe or interleave at. The function
// is process-global, so it is registered once and the per-test hook is swapped.
var (
	probeOnce sync.Once
	probeHook atomic.Pointer[func()]
)

func registerProbe() {
	probeOnce.Do(func() {
		sqlite.MustRegisterScalarFunction("intentq_probe", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			if h := probeHook.Load(); h != nil {
				(*h)()
			}
			return int64(0), nil
		})
	})
}

func setProbe(t *testing.T, h func()) {
	t.Helper()
	probeHook.Store(&h)
	t.Cleanup(func() { probeHook.Store(nil) })
}

func TestOldestPendingAgeIgnoresExpired(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	if _, _, _, err := q.Enqueue(ctx, Item{Key: "short", Kind: "clear", TTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	c.advance(30 * time.Second)
	if _, _, _, err := q.Enqueue(ctx, Item{Key: "long", Kind: "clear"}); err != nil {
		t.Fatal(err)
	}
	c.advance(10 * time.Second)
	if age, err := q.OldestPendingAge(ctx); err != nil || age != 40*time.Second {
		t.Fatalf("oldest unexpired pending is the first row, age 40s: %v %v", age, err)
	}
	c.advance(30 * time.Second) // "short" is now past its TTL (expires_ms <= now)
	age, err := q.OldestPendingAge(ctx)
	if err != nil || age != 40*time.Second {
		t.Fatalf("an expired pending row must be ignored; the oldest unexpired was created 40s ago: %v %v", age, err)
	}
}

// Recover must be safe to call periodically while workers run (second call, not only boot).
func TestRecoverPeriodicWhileWorkersRun(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	live, _, _, _ := q.Enqueue(ctx, Item{Key: "live", Kind: "clear"})
	dead, _, _, _ := q.Enqueue(ctx, Item{Key: "dead", Kind: "clear"})
	stale, _, _, _ := q.Enqueue(ctx, Item{Key: "stale", Kind: "clear", TTL: 90 * time.Second})
	if rec, err := q.Recover(ctx); err != nil || len(rec) != 0 {
		t.Fatalf("boot sweep over a fresh queue changes nothing: %v %v", rec, err)
	}
	// live: long lease; dead: short lease that will end. Claim order is next_ms,id.
	if in, _ := q.Claim(ctx, "w1", time.Hour); in == nil || in.ID != live {
		t.Fatalf("claim live: %+v", in)
	}
	if in, _ := q.Claim(ctx, "w2", time.Minute); in == nil || in.ID != dead {
		t.Fatalf("claim dead: %+v", in)
	}
	c.advance(2 * time.Minute)
	rec, err := q.Recover(ctx) // second call, workers still running
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]State{}
	for _, r := range rec {
		got[r.ID] = r.State
	}
	if len(got) != 2 || got[dead] != Uncertain || got[stale] != Expired {
		t.Fatalf("lease-expired running -> uncertain, TTL-expired pending -> expired: %v", got)
	}
	in, err := q.Get(ctx, live)
	if err != nil || in.State != Running || in.Owner != "w1" {
		t.Fatalf("a running row with a live lease must be untouched: %+v %v", in, err)
	}
	if err := q.Finish(ctx, live, "w1", Done, "ok"); err != nil {
		t.Fatalf("the live owner must still be able to finish: %v", err)
	}
}

func TestPruneLeavesNoOrphanStages(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, func(cfg *Config) { cfg.MaxTerminal = 2 })
	for i := 0; i < 5; i++ {
		id, _, _, _ := q.Enqueue(ctx, Item{Key: string(rune('a' + i)), Kind: "clear"})
		in, _ := q.Claim(ctx, "d", time.Minute)
		if in == nil || in.ID != id {
			t.Fatalf("claim order: %+v", in)
		}
		if err := q.Record(ctx, id, in.Attempt, "paste", "ok", nil); err != nil {
			t.Fatal(err)
		}
		if i < 4 { // the last one stays running: a Finish lands after the prune
			if err := q.Finish(ctx, id, "d", Done, "ok"); err != nil {
				t.Fatal(err)
			}
		}
		c.advance(time.Second)
	}
	if n, err := q.Prune(ctx); err != nil || n != 2 {
		t.Fatalf("prune: n=%d err=%v", n, err)
	}
	last, _ := q.Get(ctx, 5)
	if err := q.Finish(ctx, last.ID, "d", Done, "ok"); err != nil {
		t.Fatal(err)
	}
	var orphans, parents, stages int
	_ = q.db.QueryRow(`SELECT COUNT(*) FROM intents_stages WHERE intent_id NOT IN (SELECT id FROM intents)`).Scan(&orphans)
	_ = q.db.QueryRow(`SELECT COUNT(*) FROM intents`).Scan(&parents)
	_ = q.db.QueryRow(`SELECT COUNT(DISTINCT intent_id) FROM intents_stages`).Scan(&stages)
	if orphans != 0 || stages != parents {
		t.Fatalf("stage rows must match their parents: orphans=%d parents=%d stage-owners=%d", orphans, parents, stages)
	}
	// the running row's stages survived the prune and its Finish landed
	if got, _ := q.Get(ctx, 5); got.State != Done {
		t.Fatalf("finish after prune: %+v", got)
	}
}

func TestClaimCancelledContextLeavesNothingBehind(t *testing.T) {
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	id, _, _, _ := q.Enqueue(context.Background(), Item{Key: "k", Kind: "clear"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if in, err := q.Claim(ctx, "w", time.Minute); in != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled ctx must return ctx.Err and no row: %+v %v", in, err)
	}
	got, err := q.Get(context.Background(), id)
	if err != nil || got.State != Pending || got.Attempt != 0 {
		t.Fatalf("no claim may be committed behind a cancelled ctx: %+v %v", got, err)
	}
	if in, err := q.Claim(context.Background(), "w", time.Minute); err != nil || in == nil || in.Attempt != 1 {
		t.Fatalf("a live ctx still claims normally: %+v %v", in, err)
	}
}

func TestGetNotFoundIsTyped(t *testing.T) {
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	if _, err := q.Get(context.Background(), 999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing id must be ErrNotFound: %v", err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Get(cctx, 1); errors.Is(err, ErrNotFound) || err == nil {
		t.Fatalf("a DB error must stay distinct from ErrNotFound: %v", err)
	}
}

// L3 (negative control): the stage rows and their parent must disappear in ONE
// transaction. A trigger fires on the PARENT delete, i.e. after the stage DELETE has
// run; a second connection then looks at what is COMMITTED. With two autocommit
// statements the stage rows are already gone and their parent is still visible (an
// intent with no history, which a Finish landing there would also mis-attribute);
// in one transaction nothing of the prune is visible yet.
func TestPruneStageAndParentDeletesAreOneTransaction(t *testing.T) {
	registerProbe()
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	path := filepath.Join(t.TempDir(), "q.db")
	q := newQ(t, path, c, func(cfg *Config) { cfg.MaxTerminal = 1 })
	for i := 0; i < 3; i++ {
		id, _, _, _ := q.Enqueue(ctx, Item{Key: string(rune('a' + i)), Kind: "clear"})
		in, _ := q.Claim(ctx, "d", time.Minute)
		if in == nil || in.ID != id {
			t.Fatalf("claim order: %+v", in)
		}
		if err := q.Finish(ctx, id, "d", Done, "ok"); err != nil {
			t.Fatal(err)
		}
		c.advance(time.Second)
	}
	if _, err := q.db.ExecContext(ctx, `CREATE TRIGGER probe_del BEFORE DELETE ON intents BEGIN SELECT intentq_probe(); END`); err != nil {
		t.Fatal(err)
	}
	other := open(t, path)
	var probes, parentsWithoutStages atomic.Int64
	setProbe(t, func() {
		probes.Add(1)
		var n int64
		if err := other.QueryRow(`SELECT COUNT(*) FROM intents p WHERE NOT EXISTS (SELECT 1 FROM intents_stages s WHERE s.intent_id=p.id)`).Scan(&n); err != nil {
			t.Error(err)
		}
		if n > parentsWithoutStages.Load() {
			parentsWithoutStages.Store(n)
		}
	})
	if n, err := q.Prune(ctx); err != nil || n != 2 {
		t.Fatalf("prune: n=%d err=%v", n, err)
	}
	if probes.Load() == 0 {
		t.Fatal("the probe never ran: the test observed nothing")
	}
	if got := parentsWithoutStages.Load(); got != 0 {
		t.Fatalf("a concurrent reader saw %d intents whose stage rows were already deleted: the two deletes are not one transaction", got)
	}
}

// U1 (negative control): a ctx cancelled WHILE the claim statement runs (not before
// the call, which the early return answers) must leave nothing committed. The
// trigger fires inside the UPDATE, after the row changed and before it can commit.
func TestClaimCancelledDuringTheUpdateLeavesNothingBehind(t *testing.T) {
	registerProbe()
	c := &clock{t: time.Unix(1_000_000, 0)}
	q := newQ(t, filepath.Join(t.TempDir(), "q.db"), c, nil)
	id, _, _, _ := q.Enqueue(context.Background(), Item{Key: "k", Kind: "clear"})
	if _, err := q.db.Exec(`CREATE TRIGGER probe_upd AFTER UPDATE ON intents BEGIN SELECT intentq_probe(); END`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var fired atomic.Bool
	setProbe(t, func() { fired.Store(true); cancel() })
	if in, err := q.Claim(ctx, "w", time.Minute); in != nil || err == nil {
		t.Fatalf("a claim whose ctx ended mid-statement must fail, got %+v err=%v", in, err)
	}
	if !fired.Load() {
		t.Fatal("the cancel hook never ran: the test observed nothing")
	}
	probeHook.Store(nil)
	got, err := q.Get(context.Background(), id)
	if err != nil || got.State != Pending || got.Attempt != 0 || got.Owner != "" {
		t.Fatalf("no claim may be committed behind a ctx cancelled mid-claim: %+v %v", got, err)
	}
}
