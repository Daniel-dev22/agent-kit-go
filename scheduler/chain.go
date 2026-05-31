package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Chain turns an agent's job-completion stream into ordered, event-triggered
// schedule fires. It is the generic primitive behind "run the maintenance jobs
// after the nightly backup wave finishes": cron-fired backup/copy schedules
// land their data, and when the agent has no more of those in flight the Chain
// fires the next stage's On-schedules (e.g. prune), then the stage after that
// (e.g. check) once the prune jobs drain — strictly one stage per day.
//
// The Chain is deliberately agent-agnostic:
//   - It does not know about duplicacy actions. The agent supplies a Spec whose
//     callbacks classify a completed job (is it part of the wave? which stage's
//     work just finished?) and report whether any of a stage's jobs are still
//     active.
//   - Firing a stage means "FireMatching the schedules this stage owns". The
//     agent's Spec provides the predicate (typically: Schedule.On == sentinel
//     && Action == "<stage action>").
//
// Idempotency is per calendar day (agent-local timezone). Each stage records
// the date it last fired in an atomic on-disk marker file, so an agent restart
// mid-morning neither re-fires a stage that already ran today nor skips a stage
// whose turn had come. First boot with no marker does NOT retroactively fire a
// wave that completed before the process started — OnJobComplete only ever acts
// on transitions it personally witnesses.
type Chain struct {
	scheduler *Scheduler
	spec      Spec
	tz        *time.Location

	mu     sync.Mutex
	marker stageMarker // persisted: which stage last fired, and on what date
}

// Stage is one step in the chain, fired after the previous stage's work drains.
// Stage 0's "previous" is the backup+copy wave itself.
type Stage struct {
	// Name labels the stage in logs and is the key under which its
	// last-fired date is persisted. Stable across releases — changing it
	// resets that stage's idempotency marker.
	Name string

	// TriggerKey labels the jobs this stage starts (e.g. "chain-prune").
	// Surfaced on the job row so operators can see a job was chain-fired.
	TriggerKey string

	// Match selects the schedules this stage fires. Called for every cached
	// schedule; return true for the ones this stage owns (typically
	// sch.Schedule.On == "after_wave" && sch.Action == "<stage action>").
	Match func(LocalSchedule) bool

	// Active reports whether any job belonging to THIS stage is still
	// running or pending. The Chain advances to the next stage only when this
	// returns false after the stage has fired. For a stage that fires
	// "prune", Active is "any prune job running/pending?".
	Active func() bool
}

// Spec configures a Chain for a specific agent.
type Spec struct {
	// Stages run in order. Stage[i+1] fires once Stage[i] has fired today AND
	// Stage[i].Active() reports false (its jobs drained). Stage[0] fires once
	// the wave is complete (see WaveComplete).
	Stages []Stage

	// IsWaveJob reports whether a completed job is part of the nightly wave
	// (backup/copy) that gates Stage[0]. Completions of non-wave jobs (e.g.
	// the chain's own prune/check) still drive stage advancement but do not
	// re-arm the wave.
	IsWaveJob func(action string) bool

	// WaveComplete reports whether the wave has fully drained: at least one
	// wave job ran since arming and none are now running/pending. The agent
	// implements this against its job registry (e.g. "no backup/copy job is
	// running or pending AND we saw ≥1 finish"). The Chain calls it only on a
	// wave-job completion.
	WaveComplete func() bool

	// MarkerPath is the on-disk file where per-stage last-fired dates persist
	// (atomic write). Parent dir must exist. Required.
	MarkerPath string
}

type stageMarker struct {
	// FiredOn maps stage name → the YYYY-MM-DD (agent tz) it last fired.
	FiredOn map[string]string `json:"fired_on"`
}

// NewChain builds a Chain. tz defaults to the scheduler's configured timezone
// when nil. Loads the on-disk marker (missing file ⇒ empty, first-boot safe).
func NewChain(s *Scheduler, spec Spec) *Chain {
	c := &Chain{
		scheduler: s,
		spec:      spec,
		tz:        s.cfg.Timezone,
		marker:    stageMarker{FiredOn: map[string]string{}},
	}
	if m, err := loadStageMarker(spec.MarkerPath); err != nil {
		slog.Warn("chain: marker load failed (starting empty)", "error", err, "path", spec.MarkerPath)
	} else if m.FiredOn != nil {
		c.marker = m
	}
	return c
}

// OnJobComplete is the agent's hook into the Chain. Call it from the
// job-completion path with the action of the job that just reached a terminal
// state. Cheap and safe to call on every terminal event (it filters internally).
// The provided ctx is used for any schedule fires it triggers.
func (c *Chain) OnJobComplete(ctx context.Context, action string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	today := time.Now().In(c.tz).Format("2006-01-02")

	// Wave completion arms stage 0.
	if c.spec.IsWaveJob != nil && c.spec.IsWaveJob(action) {
		if c.spec.WaveComplete != nil && c.spec.WaveComplete() {
			c.maybeFireStage(ctx, 0, today)
		}
		return
	}

	// A non-wave completion may be a chain stage's own job finishing. Advance
	// from the FURTHEST stage that has fired today: once its jobs drain, fire
	// the next stage. Scanning for the furthest (not the first) fired stage is
	// what makes this correct for more than two stages — otherwise a 3rd stage
	// would never fire because the loop would keep re-examining stage 0.
	last := -1
	for i := range c.spec.Stages {
		if c.marker.FiredOn[c.spec.Stages[i].Name] == today {
			last = i
		}
	}
	if last < 0 {
		return // no stage has fired today yet (wave not complete) — nothing to advance
	}
	if a := c.spec.Stages[last].Active; a != nil && a() {
		return // furthest-fired stage still working; wait for the next completion
	}
	if last+1 < len(c.spec.Stages) {
		c.maybeFireStage(ctx, last+1, today)
	}
}

// maybeFireStage fires stage idx if it hasn't already fired today. Caller holds
// c.mu.
func (c *Chain) maybeFireStage(ctx context.Context, idx int, today string) {
	if idx >= len(c.spec.Stages) {
		return
	}
	st := c.spec.Stages[idx]
	if c.marker.FiredOn[st.Name] == today {
		return // already fired today — idempotent across restarts
	}
	n := c.scheduler.FireMatching(ctx, st.TriggerKey, st.Match)
	c.marker.FiredOn[st.Name] = today
	if err := saveStageMarker(c.spec.MarkerPath, c.marker); err != nil {
		slog.Warn("chain: marker save failed", "error", err, "path", c.spec.MarkerPath)
	}
	slog.Info("chain: stage fired",
		"stage", st.Name,
		"trigger", st.TriggerKey,
		"schedules_fired", n,
		"date", today,
	)
}

// --- marker persistence (atomic, mirrors cache.go) ---

func loadStageMarker(path string) (stageMarker, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return stageMarker{FiredOn: map[string]string{}}, nil
		}
		return stageMarker{}, err
	}
	var m stageMarker
	if err := json.Unmarshal(data, &m); err != nil {
		return stageMarker{}, err
	}
	if m.FiredOn == nil {
		m.FiredOn = map[string]string{}
	}
	return m, nil
}

func saveStageMarker(path string, m stageMarker) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
