package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mustUTC(t *testing.T) *time.Location { t.Helper(); return time.UTC }

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return ts
}

// fakeStageState lets a test drive each stage's "is any job still active?"
// signal and record which stages fired.
type fakeStageState struct {
	active map[string]bool // stage name → has in-flight jobs
	fired  []string        // trigger keys in fire order
}

// buildTestChain wires a 2-stage chain (prune→check) over an in-memory
// scheduler whose cache holds one after-wave schedule per stage. dispatch's
// Fire callback records the trigger key instead of doing real work.
func buildTestChain(t *testing.T, st *fakeStageState) *Chain {
	t.Helper()
	dir := t.TempDir()

	s, err := New(Config{
		PullURL:   "http://unused",
		CachePath: filepath.Join(dir, "sched.json"),
		Fire: func(ctx context.Context, sch LocalSchedule, triggerKey string, missed bool) error {
			st.fired = append(st.fired, triggerKey)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("New scheduler: %v", err)
	}
	// Seed one after-wave schedule per maintenance action.
	s.cache.Schedules = []LocalSchedule{
		{ID: "p1", Action: "prune", Enabled: true, Schedule: Schedule{On: "after_wave"}},
		{ID: "c1", Action: "check", Enabled: true, Schedule: Schedule{On: "after_wave"}},
	}

	match := func(act string) func(LocalSchedule) bool {
		return func(sch LocalSchedule) bool { return sch.Schedule.On == "after_wave" && sch.Action == act }
	}
	active := func(name string) func() bool {
		return func() bool { return st.active[name] }
	}

	return NewChain(s, Spec{
		MarkerPath:   filepath.Join(dir, "markers.json"),
		IsWaveJob:    func(a string) bool { return a == "backup" || a == "copy" },
		WaveComplete: func() bool { return !st.active["wave"] },
		Stages: []Stage{
			{Name: "prune", TriggerKey: "chain-prune", Match: match("prune"), Active: active("prune")},
			{Name: "check", TriggerKey: "chain-check", Match: match("check"), Active: active("check")},
		},
	})
}

// Happy path: wave drains → prune fires; prune drains → check fires; check
// never re-fires prune.
func TestChainFiresPruneThenCheck(t *testing.T) {
	st := &fakeStageState{active: map[string]bool{}}
	c := buildTestChain(t, st)
	ctx := context.Background()

	// A copy completes but another wave job is still active → nothing fires.
	st.active["wave"] = true
	c.OnJobComplete(ctx, "copy")
	if len(st.fired) != 0 {
		t.Fatalf("fired %v before wave drained", st.fired)
	}

	// Last wave job completes → prune stage fires.
	st.active["wave"] = false
	st.active["prune"] = true // the fire registers prune jobs (still active)
	c.OnJobComplete(ctx, "copy")
	if got := st.fired; len(got) != 1 || got[0] != "chain-prune" {
		t.Fatalf("after wave drain, fired = %v, want [chain-prune]", got)
	}

	// A prune completes but more prune still active → check must NOT fire yet.
	c.OnJobComplete(ctx, "prune")
	if len(st.fired) != 1 {
		t.Fatalf("check fired before prune drained: %v", st.fired)
	}

	// Last prune completes → check stage fires.
	st.active["prune"] = false
	st.active["check"] = true
	c.OnJobComplete(ctx, "prune")
	if got := st.fired; len(got) != 2 || got[1] != "chain-check" {
		t.Fatalf("after prune drain, fired = %v, want [...,chain-check]", got)
	}

	// A check completes → nothing more to fire, prune must not re-fire.
	st.active["check"] = false
	c.OnJobComplete(ctx, "check")
	if len(st.fired) != 2 {
		t.Fatalf("extra fire after check drained: %v", st.fired)
	}
}

// Each stage fires at most once per day even if multiple wave/stage completions
// arrive (idempotency).
func TestChainStageFiresOncePerDay(t *testing.T) {
	st := &fakeStageState{active: map[string]bool{}}
	c := buildTestChain(t, st)
	ctx := context.Background()

	st.active["wave"] = false
	// Three wave completions in a row (e.g. several copies finishing together).
	for i := 0; i < 3; i++ {
		c.OnJobComplete(ctx, "copy")
	}
	pruneCount := 0
	for _, f := range st.fired {
		if f == "chain-prune" {
			pruneCount++
		}
	}
	if pruneCount != 1 {
		t.Fatalf("prune fired %d times, want 1 (fired=%v)", pruneCount, st.fired)
	}
}

// Restart resilience: a fresh Chain over the same marker file does NOT re-fire a
// stage that already fired today, and still advances to the next stage.
func TestChainRestartResumesFromMarker(t *testing.T) {
	st := &fakeStageState{active: map[string]bool{}}
	dir := t.TempDir()
	markerPath := filepath.Join(dir, "markers.json")

	newChain := func() *Chain {
		s, err := New(Config{
			PullURL:   "http://unused",
			CachePath: filepath.Join(dir, "sched.json"),
			Fire: func(ctx context.Context, sch LocalSchedule, triggerKey string, missed bool) error {
				st.fired = append(st.fired, triggerKey)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("New scheduler: %v", err)
		}
		s.cache.Schedules = []LocalSchedule{
			{ID: "p1", Action: "prune", Enabled: true, Schedule: Schedule{On: "after_wave"}},
			{ID: "c1", Action: "check", Enabled: true, Schedule: Schedule{On: "after_wave"}},
		}
		match := func(act string) func(LocalSchedule) bool {
			return func(sch LocalSchedule) bool { return sch.Schedule.On == "after_wave" && sch.Action == act }
		}
		active := func(name string) func() bool { return func() bool { return st.active[name] } }
		return NewChain(s, Spec{
			MarkerPath:   markerPath,
			IsWaveJob:    func(a string) bool { return a == "backup" || a == "copy" },
			WaveComplete: func() bool { return !st.active["wave"] },
			Stages: []Stage{
				{Name: "prune", TriggerKey: "chain-prune", Match: match("prune"), Active: active("prune")},
				{Name: "check", TriggerKey: "chain-check", Match: match("check"), Active: active("check")},
			},
		})
	}

	// First process: wave drains, prune fires (and is still running at "crash").
	c1 := newChain()
	st.active["wave"] = false
	st.active["prune"] = true
	c1.OnJobComplete(context.Background(), "copy")
	if len(st.fired) != 1 || st.fired[0] != "chain-prune" {
		t.Fatalf("pre-restart fired = %v, want [chain-prune]", st.fired)
	}

	// "Restart": new Chain, same marker file. Marker says prune already fired
	// today. Prune now drains → check fires; prune must NOT fire again.
	c2 := newChain()
	st.active["prune"] = false
	st.active["check"] = true
	c2.OnJobComplete(context.Background(), "prune")
	if got := st.fired; len(got) != 2 || got[1] != "chain-check" {
		t.Fatalf("post-restart fired = %v, want [chain-prune, chain-check]", got)
	}

	// Marker file should record both stages for today.
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("marker file missing: %v", err)
	}
}

// Matches() must never fire an after-wave schedule on the clock, and missed-run
// recovery (which uses findLastFireWindow→Matches) must skip it too.
func TestAfterWaveScheduleNeverCronMatches(t *testing.T) {
	s := Schedule{On: "after_wave", Days: []string{"*"}, Hours: []string{"*"}, Minute: "0"}
	// Even with wildcard day/hour and matching minute, On != "" short-circuits.
	if Matches(s, mustTime(t, "2026-05-30T04:00:00Z"), mustUTC(t)) {
		t.Fatal("after-wave schedule matched on the clock; must never cron-fire")
	}
	if _, ok := findLastFireWindow(s, mustTime(t, "2026-05-29T00:00:00Z"), mustTime(t, "2026-05-30T00:00:00Z"), mustUTC(t)); ok {
		t.Fatal("after-wave schedule produced a missed-run window; must be skipped")
	}
}
