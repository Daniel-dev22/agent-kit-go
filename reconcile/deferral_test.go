package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type levelLog struct {
	mu   sync.Mutex
	recs []string
}

func (h *levelLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *levelLog) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.recs = append(h.recs, r.Level.String()+" "+r.Message)
	h.mu.Unlock()
	return nil
}
func (h *levelLog) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *levelLog) WithGroup(string) slog.Handler      { return h }
func (h *levelLog) count(prefix string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.recs {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

// With NonErrorDeferrals a 409 and an applied:false reply are not failures: the next tick is on the
// normal cadence (no backoff), a full snapshot is forced, one Info line per state change and no
// Warn. Convergence clears the state.
func TestNonErrorDeferralsAreNotFailures(t *testing.T) {
	for name, deferral := range map[string]func() (*http.Response, error){
		"409 epoch_conflict":        func() (*http.Response, error) { return reply(409, `{"code":"epoch_conflict"}`), nil },
		"applied false + want_snap": func() (*http.Response, error) { return reply(200, `{"applied":false,"want_snapshot":true}`), nil },
		"applied false alone":       func() (*http.Response, error) { return reply(200, `{"applied":false}`), nil },
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, 100*time.Second, 300*time.Second)
			lg := &levelLog{}
			r.g.log = slog.New(lg)
			r.g.cfg.NonErrorDeferrals = true
			r.fp = "fp1"
			r.resp = func(int) (*http.Response, error) { return deferral() }
			for i := 0; i < 3; i++ {
				body, wait := r.tick()
				if wait != 100*time.Second {
					t.Fatalf("tick %d waited %v, want the normal interval (no backoff)", i, wait)
				}
				if body != "full" {
					t.Fatalf("tick %d sent %q: a deferral must force a full snapshot next", i, body)
				}
			}
			if r.g.failures != 0 || r.g.failing {
				t.Fatalf("failures=%d failing=%v", r.g.failures, r.g.failing)
			}
			if lg.count("WARN") != 0 || lg.count("INFO reconcile: controller deferred") != 1 {
				t.Fatalf("logs %v: want exactly one Info and no Warn across three deferrals", lg.recs)
			}
			r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
			r.tick()
			if r.g.deferredWhat != "" || lg.count("INFO reconcile: converged") != 1 {
				t.Fatalf("convergence not logged/cleared: %q %v", r.g.deferredWhat, lg.recs)
			}
		})
	}
}

// Default (flag off) behaviour is unchanged byte for byte: both replies are failures with backoff.
func TestDeferralsAreFailuresByDefault(t *testing.T) {
	for name, deferral := range map[string]func() (*http.Response, error){
		"409":           func() (*http.Response, error) { return reply(409, `{"code":"epoch_conflict"}`), nil },
		"applied false": func() (*http.Response, error) { return reply(200, `{"applied":false,"want_snapshot":true}`), nil },
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, 100*time.Second, 300*time.Second)
			lg := &levelLog{}
			r.g.log = slog.New(lg)
			r.fp = "fp1"
			r.resp = func(int) (*http.Response, error) { return deferral() }
			_, wait := r.tick()
			if wait != time.Second || !r.g.failing || lg.count("WARN reconcile: push failing") != 1 {
				t.Fatalf("wait %v failing %v logs %v", wait, r.g.failing, lg.recs)
			}
		})
	}
}

// After a healthy period a new deferral is a new state change and is logged again: recovery clears
// the log state even when the agent was never in the failing state.
func TestDeferralIsLoggedAgainAfterRecovery(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	lg := &levelLog{}
	r.g.log = slog.New(lg)
	r.g.cfg.NonErrorDeferrals = true
	r.fp = "fp1"
	seq := []string{"ok", "409", "ok", "409"}
	i := 0
	r.resp = func(int) (*http.Response, error) {
		s := seq[i]
		i++
		if s == "409" {
			return reply(409, ``), nil
		}
		return reply(200, `{"applied":true}`), nil
	}
	for n := range seq {
		r.fp = fmt.Sprint("fp", n) // a changed fingerprint: every tick posts instead of being gated away
		r.tick()
	}
	if lg.count("INFO reconcile: controller deferred") != 2 || lg.count("INFO reconcile: converged") != 2 {
		t.Fatalf("logs %v: want 2 deferred and 2 converged lines", lg.recs)
	}
}

// A 409 and an applied:false are distinct log states; switching between them logs the change once.
func TestDeferralLogsEachStateChangeOnce(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	lg := &levelLog{}
	r.g.log = slog.New(lg)
	r.g.cfg.NonErrorDeferrals = true
	r.fp = "fp1"
	seq := []func() (*http.Response, error){
		func() (*http.Response, error) { return reply(409, ``), nil },
		func() (*http.Response, error) { return reply(409, ``), nil },
		func() (*http.Response, error) { return reply(200, `{"applied":false}`), nil },
		func() (*http.Response, error) { return reply(200, `{"applied":false}`), nil },
	}
	i := 0
	r.resp = func(int) (*http.Response, error) { f := seq[i]; i++; return f() }
	for range seq {
		r.tick()
	}
	if lg.count("INFO reconcile: controller deferred") != 2 {
		t.Fatalf("logs %v", lg.recs)
	}
}
