package scheduler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestReconcileLoopUnderVirtualTime is the canonical example for using
// testing/synctest (Go 1.25 GA) in this codebase. Virtual time eliminates
// real-wall-clock Sleeps from the test suite — a 5-minute reconcile
// interval test that previously needed a 5-second mock now runs in <1ms
// with deterministic timing.
//
// The pattern:
//  1. Wrap the entire test body in synctest.Run.
//  2. Inside, time.Sleep / time.NewTicker / time.After all use the
//     synthetic clock — they don't block the real test process.
//  3. Call synctest.Wait() to wait until every goroutine in the bubble
//     is blocked on a channel/select/IO/sleep — the standard "tick
//     everything to quiescence" barrier.
//
// Apply this pattern to all retry/backoff/cron tests that currently
// rely on short real-time Sleeps. See plan: Wave 4e in
// /home/user/.claude/plans/review-all-of-our-bubbly-rain.md.
func TestReconcileLoopUnderVirtualTime(t *testing.T) {
	synctest.Run(func() {
		var pullCount atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pullCount.Add(1)
			_, _ = io.WriteString(w, `{"schedules":[]}`)
		}))
		defer srv.Close()

		tmpdir := t.TempDir()

		s, err := New(Config{
			NodeName:     "test-node",
			PullURL:      srv.URL,
			CachePath:    filepath.Join(tmpdir, "cache.json"),
			HTTPClient:   srv.Client(),
			FireInterval: 1 * time.Minute,
			PullInterval: 5 * time.Minute,
			Fire: func(ctx context.Context, sch LocalSchedule, trigger string, recovery bool) error {
				return nil
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.Start(ctx)
		defer s.Stop()

		// Start does one initial pull asynchronously. Wait for the
		// goroutines to quiesce so we can take a clean baseline.
		synctest.Wait()
		baseline := pullCount.Load()

		// Advance virtual time by 12 minutes. The reconcile loop ticks
		// every 5 min → 2 additional pulls (at virtual t+5 and t+10).
		time.Sleep(12 * time.Minute)
		synctest.Wait()

		got := pullCount.Load() - baseline
		if got != 2 {
			t.Errorf("expected 2 reconcile pulls in 12 virtual min, got %d", got)
		}
	})
}
