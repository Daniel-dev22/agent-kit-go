package eventoutbox

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestInitBatchSchemaErrorBranches(t *testing.T) {
	cl := http.DefaultClient
	// closed database: Begin fails
	db := newDB(t)
	db.Close()
	if _, err := NewBatch(db, cl, BatchConfig{URL: "x"}); err == nil {
		t.Fatal("closed db accepted")
	}
	// read-only connection: creating the schema fails
	db = newDB(t)
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA query_only = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBatch(db, cl, BatchConfig{URL: "x"}); err == nil {
		t.Fatal("read-only db accepted")
	}
	// schema exists but storing a fresh epoch fails (read-only after create)
	db = newDB(t)
	db.SetMaxOpenConns(1)
	if _, err := NewBatch(db, cl, BatchConfig{URL: "x"}); err != nil {
		t.Fatal(err)
	}
	db.Exec(`DELETE FROM batch_meta`)
	db.Exec(`PRAGMA query_only = 1`)
	if _, err := NewBatch(db, cl, BatchConfig{URL: "x"}); err == nil || !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("epoch store failure not reported: %v", err)
	}
	// a batch_meta table of the wrong shape: reading the epoch fails
	db = newDB(t)
	db.Exec(`CREATE TABLE batch_meta (key TEXT PRIMARY KEY)`)
	if _, err := NewBatch(db, cl, BatchConfig{URL: "x"}); err == nil {
		t.Fatal("malformed meta table accepted")
	}
}

func TestBatchDatabaseFailuresBackOffNotCrash(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	enq(t, o, 2, false)
	db.Close()
	if o.drain(context.Background()) {
		t.Fatal("a dead database must report failure (back off)")
	}
	o.prune() // must not panic on a dead database
	if s := o.Stats(); s.Pending != 0 && s.Epoch == "" {
		t.Fatalf("stats on a dead db: %+v", s)
	}
}

func TestBatchStartDrainsAndCloseIsIdempotent(t *testing.T) {
	c := &ctl{}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.DrainInterval = 20 * time.Millisecond })
	enq(t, o, 3, false) // left from a "previous run"
	ctx, cancel := context.WithCancel(context.Background())
	o.Start(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && rowsLeft(t, db) != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("the first drain must flush the leftover backlog")
	}
	cancel()
	o.Close()
	o.Close()
}

func TestBatchRequestBuildFailureRetries(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.URL = "http://[::1" })
	enq(t, o, 1, false)
	if o.drain(context.Background()) || rowsLeft(t, db) != 1 {
		t.Fatal("an unbuildable request must keep the row and back off")
	}
}

func TestBatchEnvelopeMarshalFailureBacksOff(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	enq(t, o, 1, false)
	o.cfg.Envelope = func(string) map[string]any { return map[string]any{"bad": make(chan int)} }
	if o.drain(context.Background()) || rowsLeft(t, db) != 1 {
		t.Fatal("an envelope that cannot marshal must keep the row")
	}
}

func TestLegacyFailureBranches(t *testing.T) {
	// bad URL: NewRequest fails, failure recorded, push false
	o, count := legacyOn(t, 200, "", nil)
	o.urlFor = func(string) string { return "http://[::1" }
	o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	if o.push(context.Background(), 1, "j", []byte(`{}`)) || count() != 1 {
		t.Fatal("an unbuildable request must keep the row")
	}
	// transport error
	o2, count2 := legacyOn(t, 200, "", nil)
	o2.client = &http.Client{Transport: roundTripFn(func(*http.Request) (*http.Response, error) { return nil, errors.New("dial") })}
	o2.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	if o2.push(context.Background(), 1, "j", []byte(`{}`)) || count2() != 1 {
		t.Fatal("a transport error must keep the row")
	}
	// dead database: Enqueue loses the event loudly, recordFailure and drainOnce degrade without panic
	o3, _ := legacyOn(t, 200, "", nil)
	o3.db.Close()
	o3.Enqueue("j", "e", []byte(`{}`))
	o3.recordFailure(1, "x")
	o3.drainOnce(context.Background())
}

func TestLegacyDrainStopsOnFirstFailureAndOnCancel(t *testing.T) {
	o, count := legacyOn(t, 500, "", nil)
	for i := 0; i < 3; i++ {
		o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	}
	o.drainOnce(context.Background())
	var attempts int
	o.db.QueryRow(`SELECT SUM(attempts) FROM pending_events`).Scan(&attempts)
	if count() != 3 || attempts != 1 {
		t.Fatalf("a failing controller must stop the pass after the first row: rows=%d attempts=%d", count(), attempts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o.drainOnce(ctx) // cancelled: must return without pushing
	o.db.QueryRow(`SELECT SUM(attempts) FROM pending_events`).Scan(&attempts)
	if attempts != 1 {
		t.Fatalf("a cancelled pass pushed: attempts=%d", attempts)
	}
}

func TestLegacyStartDrainsAndCloses(t *testing.T) {
	o, count := legacyOn(t, 200, "", nil)
	o.interval = 20 * time.Millisecond
	o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	ctx, cancel := context.WithCancel(context.Background())
	o.Start(ctx)
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline) && count() != 0; {
		time.Sleep(10 * time.Millisecond)
	}
	if count() != 0 {
		t.Fatal("the drain loop did not flush the backlog")
	}
	cancel()
	o.Close()
	o.Close()
}

func TestLegacyNewOnClosedDBAndEmptyDrain(t *testing.T) {
	db := newDB(t)
	db.Close()
	if _, err := New(db, http.DefaultClient, func(string) string { return "x" }); err == nil {
		t.Fatal("New on a closed db must fail")
	}
	o, count := legacyOn(t, 200, "", nil)
	o.drainOnce(context.Background()) // empty queue: returns quietly
	if count() != 0 {
		t.Fatal("empty drain changed the queue")
	}
}

func TestLegacyLoopExitsOnContextAndOnClose(t *testing.T) {
	o, _ := legacyOn(t, 200, "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	o.Start(ctx)
	cancel()
	o.wg.Wait() // exits through ctx.Done
	o2, _ := legacyOn(t, 200, "", nil)
	o2.Start(context.Background())
	o2.Close() // exits through stop
}

func TestBatchLoopExitsOnContextAndOnClose(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	o.Start(ctx)
	cancel()
	o.wg.Wait()
	o2, _ := newBatchT(t, &ctl{}, nil)
	o2.Start(context.Background())
	o2.Close()
}

func TestBatchDrainOnCancelledContextReturnsCleanly(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	enq(t, o, 2, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !o.drain(ctx) || rowsLeft(t, db) != 2 {
		t.Fatal("a cancelled drain must stop without touching the queue")
	}
}
