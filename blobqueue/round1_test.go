package blobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// The fleet opens its SQLite pool with SetMaxOpenConns(1). New used to hold a pinned connection while
// it ran more queries through the pool, hanging at startup with no error. The whole life cycle, floor
// shedding included, must work on a single connection.
func TestOneConnectionPool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+walDSN)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	var limit atomic.Int64
	limit.Store(1 << 40)
	st := func(string) (uint64, uint64, error) {
		return uint64(limit.Load() - fileSize(t, path)), 1 << 41, nil
	}
	type res struct {
		q   *Queue
		err error
	}
	ch := make(chan res, 1)
	go func() {
		q, err := New(db, Config{Statfs: st, FloorBytes: 1 << 20, FloorPercent: -1, MaxRows: -1, MaxBytes: -1})
		ch <- res{q, err}
	}()
	var q *Queue
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		q = r.q
	case <-time.After(10 * time.Second):
		t.Fatal("New hung on a pool capped at one connection")
	}

	enq(t, q, 40, 20_000)
	limit.Store(fileSize(t, path) + 1<<20 + 50) // force a floor shed + reclaim on the single connection
	if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: gz(20_000, 9)}); err != nil {
		t.Fatalf("enqueue under pressure on one connection: %v", err)
	}
	if s := q.Stats(context.Background()); s.ShedFloor == 0 || s.ReclaimErrs != 0 {
		t.Fatalf("stats = %+v", s)
	}
	rec := &recorder{}
	for {
		n, err := q.drainOnce(context.Background(), rec.send)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if len(rec.got) == 0 {
		t.Fatal("nothing drained on one connection")
	}
}

func TestSecondRunIsRefused(t *testing.T) {
	q, _ := newQ(t, Config{DrainInterval: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = q.Run(ctx, (&recorder{}).send); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for !q.running.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Bounded: a broken guard would let this second Run block forever.
	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()
	if err := q.Run(ctx3, (&recorder{}).send); !errors.Is(err, ErrRunning) {
		t.Fatalf("second Run = %v, want ErrRunning", err)
	}
	cancel()
	<-done
	// After the first Run ends a new one may start.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := q.Run(ctx2, (&recorder{}).send); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after the first ended = %v", err)
	}
}

// Every drained frame must carry this database's epoch (the receiver dedupes on (epoch, seq)).
func TestFrameCarriesQueueEpoch(t *testing.T) {
	q, _ := newQ(t, Config{})
	enq(t, q, 2, 5)
	rec := &recorder{}
	if _, err := q.drainOnce(context.Background(), rec.send); err != nil {
		t.Fatal(err)
	}
	if q.Epoch() == "" || rec.epoch != q.Epoch() {
		t.Fatalf("frame epoch %q != queue epoch %q", rec.epoch, q.Epoch())
	}
}

func TestReclaimFailuresAreCounted(t *testing.T) {
	q, _ := newQ(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q.reclaim(ctx)
	// A cancelled context fails BOTH the vacuum and the checkpoint; each must be counted.
	if s := q.Stats(context.Background()); s.ReclaimErrs != 2 || s.LastError == "" {
		t.Fatalf("a failed vacuum/checkpoint left no trace: %+v", s)
	}
}

// A refused loss marker is the only durable loss record: its body must reach OnDead.
func TestRefusedMarkerBodyReachesOnDead(t *testing.T) {
	q, _ := newQ(t, Config{MaxRows: 3})
	enq(t, q, 6, 5)
	var dead []DeadLetter
	q.cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) }
	rec := &recorder{ack: func(items []Item) (Ack, error) {
		for _, it := range items {
			if it.Signal == SignalLoss {
				return NewAck(len(items)-1, 0, Rejection{Seq: it.Seq, Code: "schema", Retryable: false}), nil
			}
		}
		return NewAck(len(items), 0), nil
	}}
	if _, err := q.drainOnce(context.Background(), rec.send); err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Signal != SignalLoss {
		t.Fatalf("dead = %+v", dead)
	}
	var m LossMarker
	if err := json.Unmarshal(dead[0].Body, &m); err != nil || m.Rows == 0 {
		t.Fatalf("marker body not delivered to OnDead: %q %v", dead[0].Body, err)
	}
}
