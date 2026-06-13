package scheduler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// newTestScheduler builds a scheduler that never actually pulls (PullURL is a
// dummy) — for unit-testing dispatch/missedRunCheck directly without starting
// the loops.
func newTestScheduler(t *testing.T, fire FireFunc) *Scheduler {
	t.Helper()
	s, err := New(Config{
		NodeName:  "test-node",
		PullURL:   "http://unused.invalid",
		CachePath: filepath.Join(t.TempDir(), "cache.json"),
		Fire:      fire,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestDispatchDoesNotStampOnError is the regression test for the vm 2026-05-31
// miss: a fire whose FireFunc errors (e.g. secret-vend timeout) must NOT be
// recorded as fired, so missed-run recovery will retry it.
func TestDispatchDoesNotStampOnError(t *testing.T) {
	s := newTestScheduler(t, func(context.Context, LocalSchedule, string, bool) error {
		return errors.New("vend secrets failed: context deadline exceeded")
	})
	sch := LocalSchedule{ID: "vm-backup", Enabled: true, Action: "backup"}

	s.dispatch(context.Background(), sch, "schedule", false)

	if _, ok := s.cache.LastFiredAt[sch.ID]; ok {
		t.Fatalf("LastFiredAt must NOT be stamped when the fire errors (would mark the window permanently done)")
	}
}

// TestDispatchStampsOnSuccess confirms the happy path still records the window.
func TestDispatchStampsOnSuccess(t *testing.T) {
	s := newTestScheduler(t, func(context.Context, LocalSchedule, string, bool) error {
		return nil
	})
	sch := LocalSchedule{ID: "vm-backup", Enabled: true, Action: "backup"}

	s.dispatch(context.Background(), sch, "schedule", false)

	if _, ok := s.cache.LastFiredAt[sch.ID]; !ok {
		t.Fatalf("LastFiredAt must be stamped after a successful fire")
	}
}

// TestMissedRunCheckRetriesAfterFailure exercises the full retry contract: a
// window whose first fire errored stays un-stamped and is re-attempted by the
// next missedRunCheck; once a fire succeeds it is stamped and never re-fired.
func TestMissedRunCheckRetriesAfterFailure(t *testing.T) {
	var attempts atomic.Int64
	s := newTestScheduler(t, func(context.Context, LocalSchedule, string, bool) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient: controller unreachable")
		}
		return nil
	})
	// Interval=1 → every minute is a fire window, so findLastFireWindow always
	// returns the current minute regardless of wall clock.
	one := 1
	sch := LocalSchedule{
		ID:         "vm-backup",
		Enabled:    true,
		Action:     "backup",
		MissPolicy: MissRunOnceWithin24,
		Schedule:   Schedule{IntervalMinutes: &one},
	}
	s.cache.Schedules = []LocalSchedule{sch}

	// Attempt 1: errors → must not stamp.
	s.missedRunCheck(context.Background())
	if _, ok := s.cache.LastFiredAt[sch.ID]; ok {
		t.Fatalf("after a failed fire the window must remain un-stamped so it retries")
	}

	// Attempt 2: succeeds → must stamp.
	s.missedRunCheck(context.Background())
	if _, ok := s.cache.LastFiredAt[sch.ID]; !ok {
		t.Fatalf("the retry should have fired and stamped the window")
	}

	// Attempt 3: already stamped → must NOT fire again.
	s.missedRunCheck(context.Background())
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected exactly 2 fire attempts (fail, then success), got %d", got)
	}
}

// TestReconcileLoopRecoversFailedFire is the integration test for the periodic
// missed-run recovery (fix C): a window whose boot-time fire errored is
// recovered by the reconcile loop's missedRunCheck WITHOUT a process restart.
// Uses short real-time intervals (the live fire tick is set far in the future
// so it can't interfere — recovery can only come from the reconcile loop's
// missedRunCheck) and an interval=1 schedule so every minute is a fire window.
func TestReconcileLoopRecoversFailedFire(t *testing.T) {
	body := `{"schedules":[{"id":"vm-backup","name":"backup-vm","repo_id":"vm","action":"backup",` +
		`"miss_policy":"run-once-within-24h","enabled":true,` +
		`"schedule":{"interval_minutes":1}}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	var attempts atomic.Int64
	s, err := New(Config{
		NodeName:     "test-node",
		PullURL:      srv.URL,
		CachePath:    filepath.Join(t.TempDir(), "cache.json"),
		HTTPClient:   srv.Client(),
		FireInterval: 1 * time.Hour, // far future → the live fire tick can't interfere
		PullInterval: 20 * time.Millisecond,
		Fire: func(context.Context, LocalSchedule, string, bool) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient at boot")
			}
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

	// Boot fires once and errors (attempt 1) → un-stamped. Then the reconcile
	// loop's missedRunCheck retries (attempt 2) and succeeds → stamped. Poll
	// until recovered or time out.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.Cache().LastFiredAt["vm-backup"]; ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, ok := s.Cache().LastFiredAt["vm-backup"]; !ok {
		t.Fatalf("reconcile-loop missedRunCheck should have recovered the failed window without a restart")
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected at least 2 attempts (boot fail + reconcile retry), got %d", got)
	}
}
