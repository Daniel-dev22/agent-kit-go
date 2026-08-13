package reconcile

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

func TestRunPushesSnapshotImmediatelyAndConverges(t *testing.T) {
	var mu sync.Mutex
	var got []Snapshot
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var s Snapshot
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		got = append(got, s)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		Client:   srv.Client(),
		URL:      srv.URL,
		Site:     "site-a",
		Node:     "nas",
		Interval: 50 * time.Millisecond,
		Snapshot: func(ctx context.Context) ([]jobstore.JobRef, error) {
			return []jobstore.JobRef{
				{ID: "j1", State: "running"},
				{ID: "j2", State: "completed", ExitCode: 0},
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go Run(ctx, cfg)
	// Wait for at least 2 pushes (immediate + one tick).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	mu.Lock()
	defer mu.Unlock()
	if len(got) < 1 {
		t.Fatalf("expected at least one snapshot push, got 0")
	}
	first := got[0]
	if first.Site != "site-a" || first.Node != "nas" {
		t.Errorf("site/node = %q/%q, want kd/nas", first.Site, first.Node)
	}
	if first.SnapshotAt.IsZero() {
		t.Errorf("snapshot_at not set")
	}
	if len(first.Jobs) != 2 {
		t.Fatalf("jobs len = %d, want 2", len(first.Jobs))
	}
	if first.Jobs[0].ID != "j1" || first.Jobs[0].State != "running" {
		t.Errorf("job[0] = %+v", first.Jobs[0])
	}
}

func TestRunRetriesAfterFailure(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError) // first push fails
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		Client:   srv.Client(),
		URL:      srv.URL,
		Site:     "site-b",
		Node:     "nuc",
		Interval: 40 * time.Millisecond,
		Snapshot: func(ctx context.Context) ([]jobstore.JobRef, error) {
			return nil, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go Run(ctx, cfg)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&hits) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if atomic.LoadInt32(&hits) < 2 {
		t.Fatalf("expected retry after failure; hits=%d", atomic.LoadInt32(&hits))
	}
}

func TestRunNilSnapshotIsSafe(t *testing.T) {
	// Should log and return, not panic.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { Run(ctx, Config{Client: http.DefaultClient, URL: "http://x"}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run with nil Snapshot did not return")
	}
}
