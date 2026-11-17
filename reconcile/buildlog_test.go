package reconcile

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

// A Build that keeps failing is logged on the EDGE only: one Warn for the first failure, one Info
// for the recovery, nothing in between (a failing volume must not write a line per tick).
func TestBuildFailureIsLoggedOnTheEdgeOnly(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	lg := &levelLog{}
	r.g.log = slog.New(lg)
	r.fpErr = errors.New("disk read failed")
	for i := 0; i < 25; i++ {
		r.tick()
	}
	if n := lg.count("WARN reconcile: build failed"); n != 1 {
		t.Fatalf("build-failed Warn lines over 25 failing ticks = %d, want 1: %v", n, lg.recs)
	}
	if lg.count("INFO reconcile: build recovered") != 0 {
		t.Fatal("recovery logged while still failing")
	}
	r.fpErr = nil
	for i := 0; i < 3; i++ {
		r.tick()
	}
	if n := lg.count("INFO reconcile: build recovered"); n != 1 {
		t.Fatalf("recovery lines = %d, want exactly 1: %v", n, lg.recs)
	}
	// a second outage is a new edge
	r.fpErr = errors.New("again")
	r.tick()
	r.tick()
	if n := lg.count("WARN reconcile: build failed"); n != 2 {
		t.Fatalf("second outage Warn lines = %d, want 2 in total", n)
	}
}
