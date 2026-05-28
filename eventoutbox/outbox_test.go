package eventoutbox

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestOutbox(t *testing.T, handler http.HandlerFunc) (*Outbox, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "events.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	o, err := New(db, srv.Client(), func(id string) string { return srv.URL + "/jobs/" + id + "/event" })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return o, db
}

func pendingCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEnqueuePushSuccessDeletesRow(t *testing.T) {
	var hits int32
	o, db := newTestOutbox(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	})
	o.Enqueue("job1", "completed", []byte(`{"x":1}`))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pendingCount(t, db) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c := pendingCount(t, db); c != 0 {
		t.Fatalf("row not deleted after successful push (count=%d)", c)
	}
	if atomic.LoadInt32(&hits) == 0 {
		t.Fatal("server never received the push")
	}
}

func TestDrainRetriesAfterFailure(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	o, db := newTestOutbox(t, func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Insert directly so we deterministically control the push path via drainOnce.
	if _, err := db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES (?,?,?)`,
		"job2", "failed", []byte(`{"y":2}`)); err != nil {
		t.Fatal(err)
	}

	o.drainOnce(context.Background())
	if pendingCount(t, db) != 1 {
		t.Fatal("row should remain after a failed drain")
	}
	var attempts int
	if err := db.QueryRow(`SELECT attempts FROM pending_events WHERE job_id='job2'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts < 1 {
		t.Fatalf("attempts not incremented on failure: %d", attempts)
	}

	fail.Store(false)
	o.drainOnce(context.Background())
	if pendingCount(t, db) != 0 {
		t.Fatal("row should be deleted after a successful drain")
	}
}
