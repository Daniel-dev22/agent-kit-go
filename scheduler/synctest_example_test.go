package scheduler

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// roundTripperFunc lets a test serve HTTP entirely in-process.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestReconcileLoopUnderVirtualTime is the canonical example for using
// testing/synctest in this codebase. Virtual time eliminates real-wall-clock
// Sleeps from the test suite — a 5-minute reconcile interval test that would
// otherwise need a 5-second mock runs in <1ms with deterministic timing.
//
// The pattern:
//  1. Wrap the entire test body in synctest.Test(t, func(t *testing.T) { ... }).
//  2. Inside, time.Sleep / time.NewTicker / time.After all use the
//     synthetic clock — they don't block the real test process.
//  3. Call synctest.Wait() to wait until every goroutine in the bubble
//     is blocked on a channel/select/IO/sleep — the standard "tick
//     everything to quiescence" barrier.
//
// NOTE the transport. A synctest bubble only advances its clock once EVERY
// goroutine in it is durably blocked, and a goroutine parked on a real socket
// read never counts as durably blocked. So an httptest.NewServer inside a
// bubble hangs synctest.Wait() forever — the whole package times out rather
// than failing, which reads like an infrastructure problem instead of a test
// bug. Serve the request in-process with a RoundTripper and the bubble
// quiesces immediately.
//
// Apply this pattern to all retry/backoff/cron tests that currently
// rely on short real-time Sleeps.
func TestReconcileLoopUnderVirtualTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var pullCount atomic.Int64
		client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			pullCount.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"schedules":[]}`)),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		})}

		tmpdir := t.TempDir()

		s, err := New(Config{
			NodeName:     "test-node",
			PullURL:      "http://controller.example.com/api/schedules?node=test-node",
			CachePath:    filepath.Join(tmpdir, "cache.json"),
			HTTPClient:   client,
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
