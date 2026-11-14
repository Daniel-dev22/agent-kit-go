package intentq

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

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
