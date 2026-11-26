package blobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// dedupeReceiver is the real receiver's contract: it stores the first copy of each (epoch, seq) and
// discards later copies, even with a different body.
type dedupeReceiver struct {
	mu     sync.Mutex
	stored map[uint64]Item
	failAt int // fail (after storing) the first failAt sends
	sends  int
}

func (r *dedupeReceiver) send(_ context.Context, frame []byte) (Ack, error) {
	_, items, err := DecodeFrame(frame, Limits{})
	if err != nil {
		return Ack{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stored == nil {
		r.stored = map[uint64]Item{}
	}
	dup := 0
	for _, it := range items {
		if _, ok := r.stored[it.Seq]; ok {
			dup++
			continue
		}
		it.Body = append([]byte(nil), it.Body...)
		r.stored[it.Seq] = it
	}
	r.sends++
	if r.sends <= r.failAt {
		return Ack{}, errors.New("timeout after the receiver stored the batch")
	}
	return NewAck(len(items)-dup, dup), nil
}

// A loss marker the receiver may already hold must never be rewritten under the same (epoch, seq):
// the receiver would discard the update and the loss counts with it. Later loss goes in a new marker
// and the receiver sums markers; the sum must equal everything that was shed.
func TestMarkerSentIsNeverUpdatedInPlace(t *testing.T) {
	q, _ := newQ(t, Config{MaxRows: 3})
	enq(t, q, 6, 10) // sheds rows into marker M1
	rec := &dedupeReceiver{failAt: 1}
	if _, err := q.drainOnce(context.Background(), rec.send); err == nil {
		t.Fatal("first send was meant to fail after storing")
	}
	enq(t, q, 6, 10) // sheds more while M1 is stored at the receiver but still queued
	for {
		n, err := q.drainOnce(context.Background(), rec.send)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	var sum int64
	markers := 0
	for _, it := range rec.stored {
		if it.Signal != SignalLoss {
			continue
		}
		var m LossMarker
		if err := json.Unmarshal(it.Body, &m); err != nil {
			t.Fatal(err)
		}
		sum += m.Rows
		markers++
	}
	s := q.Stats(context.Background())
	if markers < 2 {
		t.Fatalf("expected a second marker after the first was sent, got %d", markers)
	}
	if sum != s.ShedRows {
		t.Fatalf("receiver-side marker sum %d != rows shed %d: loss was swallowed by (epoch,seq) dedupe", sum, s.ShedRows)
	}
}

func TestAckValidation(t *testing.T) {
	i := func(v int) *int { return &v }
	cases := []struct {
		name string
		ack  Ack
		ok   bool
	}{
		{"empty ack", Ack{}, false},
		{"accepted missing", Ack{Duplicate: i(3)}, false},
		{"duplicate missing", Ack{Accepted: i(3)}, false},
		{"shortfall", NewAck(2, 0), false},
		{"overcount", NewAck(3, 1), false},
		{"negative", NewAck(-1, 4), false},
		{"rejected not in batch", NewAck(2, 0, Rejection{Seq: 99}), false},
		{"rejected twice", NewAck(1, 0, Rejection{Seq: 1}, Rejection{Seq: 1}), false},
		{"all accepted", NewAck(3, 0), true},
		{"mixed", NewAck(1, 1, Rejection{Seq: 2, Retryable: false}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, _ := newQ(t, Config{})
			enq(t, q, 3, 5) // seqs 1..3
			n, err := q.drainOnce(context.Background(), func(context.Context, []byte) (Ack, error) { return c.ack, nil })
			if c.ok {
				if err != nil || n == 0 {
					t.Fatalf("valid ack refused: n=%d err=%v", n, err)
				}
				return
			}
			if err == nil || n != 0 {
				t.Fatalf("malformed ack accepted: n=%d err=%v", n, err)
			}
			if s := q.Stats(context.Background()); s.Rows != 3 {
				t.Fatalf("rows deleted on a malformed ack: %d left", s.Rows)
			}
		})
	}
}

func autoVacuum(t *testing.T, db *sql.DB) int {
	t.Helper()
	var av int
	if err := db.QueryRow(`PRAGMA auto_vacuum`).Scan(&av); err != nil {
		t.Fatal(err)
	}
	return av
}

// incremental_vacuum only works if auto_vacuum=2 is really in the file, on a fresh database AND on one
// created without it (rebuilt once with VACUUM on the same pinned connection).
func TestAutoVacuumEnabled(t *testing.T) {
	q, _ := newQ(t, Config{})
	if av := autoVacuum(t, q.db); av != 2 {
		t.Fatalf("fresh database: auto_vacuum = %d, want 2 (INCREMENTAL)", av)
	}
	path := filepath.Join(t.TempDir(), "old.sqlite")
	old := openDB(t, path)
	if _, err := old.Exec(`CREATE TABLE legacy (a INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if av := autoVacuum(t, old); av != 0 {
		t.Fatalf("precondition: legacy auto_vacuum = %d", av)
	}
	if _, err := New(old, Config{FloorBytes: -1, FloorPercent: -1}); err != nil {
		t.Fatal(err)
	}
	if av := autoVacuum(t, old); av != 2 {
		t.Fatalf("existing database: auto_vacuum = %d after New, want 2", av)
	}
}

func TestStatfsErrorsAreSticky(t *testing.T) {
	q, _ := newQ(t, Config{Statfs: func(string) (uint64, uint64, error) { return 0, 0, errors.New("boom") }, FloorBytes: 1, FloorPercent: -1})
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	q.lastErr.Store("") // a recovered drain clears LastError; the counter must survive it
	if s := q.Stats(context.Background()); s.StatfsErrs != 3 {
		t.Fatalf("StatfsErrs = %d, want 3", s.StatfsErrs)
	}
}
