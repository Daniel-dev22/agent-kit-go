package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// FireFunc is the agent-supplied callback invoked when a schedule
// matches the current minute. The kit only handles the bookkeeping —
// the agent is responsible for the actual work (CLI invocation,
// API call, etc).
//
// `triggerKey` is set by the kit:
//   - "schedule" for a normal fire from the 1-minute tick.
//   - "schedule-recovery" for a missed-run catch-up fire from Start.
//
// `missedRecovery` mirrors triggerKey for convenience and matches
// duplicacy-agent's existing telemetry shape.
//
// Returning an error does NOT stop the scheduler — it is logged and the
// next tick proceeds normally. The kit stamps LastFiredAt regardless of
// the FireFunc's return value (i.e. firing happened from the schedule's
// perspective even if the job itself failed to start; whether to retry
// is up to the agent's job runner). Callers that want to inhibit
// LastFiredAt on failure should panic — but don't, just handle it inside
// the FireFunc.
type FireFunc func(ctx context.Context, sch LocalSchedule, triggerKey string, missedRecovery bool) error

// Config configures the scheduler. PullURL, NodeName, CachePath, and
// Fire are required; the rest have safe defaults.
type Config struct {
	// PullURL is the full URL the scheduler GETs to refresh the local
	// schedule list, e.g.
	// "https://controller.example.com:1443/api/duplicacy/schedules?node=nas".
	// The kit does not format this — callers substitute their own node.
	PullURL string

	// NodeName identifies this agent in log lines. Not used in the
	// PullURL — that's the caller's responsibility.
	NodeName string

	// CachePath is the disk path for the JSON cache file. The kit writes
	// atomically (path + ".tmp" → rename) so a crash mid-write doesn't
	// corrupt the file. Parent directory must exist.
	CachePath string

	// HTTPClient does the pull. Defaults to http.DefaultClient. Pass a
	// pre-configured client when the call needs bearer auth, a custom
	// dial path (e.g. through Traefik), or non-default timeouts.
	HTTPClient *http.Client

	// Fire is the per-schedule callback. Required.
	Fire FireFunc

	// Timezone for cron evaluation. Defaults to America/New_York to
	// match the rest of the controller stack.
	Timezone *time.Location

	// FireInterval is the cron tick. Defaults to 1 * time.Minute. Tests
	// can lower this; production should not.
	FireInterval time.Duration

	// PullInterval is the reconcile tick. Defaults to 5 * time.Minute.
	PullInterval time.Duration
}

// Scheduler is the running scheduler. Create with New, drive with Start
// + Stop, accept early-reconcile pokes via Pull (intended for an HTTP
// /schedules/refresh handler).
type Scheduler struct {
	cfg Config

	mu    sync.RWMutex
	cache Cache

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New builds a Scheduler. Loads the on-disk cache (if any). Does NOT
// start any goroutines — call Start.
func New(cfg Config) (*Scheduler, error) {
	if cfg.PullURL == "" {
		return nil, errors.New("scheduler: PullURL required")
	}
	if cfg.CachePath == "" {
		return nil, errors.New("scheduler: CachePath required")
	}
	if cfg.Fire == nil {
		return nil, errors.New("scheduler: Fire callback required")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Timezone == nil {
		tz, err := time.LoadLocation("America/New_York")
		if err != nil {
			return nil, fmt.Errorf("load eastern tz: %w", err)
		}
		cfg.Timezone = tz
	}
	if cfg.FireInterval <= 0 {
		cfg.FireInterval = 1 * time.Minute
	}
	if cfg.PullInterval <= 0 {
		cfg.PullInterval = 5 * time.Minute
	}

	s := &Scheduler{
		cfg:  cfg,
		stop: make(chan struct{}),
	}
	s.cache = Cache{LastFiredAt: map[string]time.Time{}}

	c, err := loadCache(cfg.CachePath)
	if err != nil {
		// Don't fail New — the caller probably wants to start with an
		// empty cache instead of refusing to launch. Caller logs the err.
		log.Warn().Err(err).Str("path", cfg.CachePath).Msg("schedule cache load failed (starting empty)")
	} else if len(c.Schedules) > 0 || len(c.LastFiredAt) > 0 {
		if c.LastFiredAt == nil {
			c.LastFiredAt = map[string]time.Time{}
		}
		s.cache = c
		log.Info().Int("count", len(c.Schedules)).Msg("loaded schedule cache from disk")
	}

	return s, nil
}

// Start launches the 1-min fire loop, the 5-min reconcile loop, and a
// best-effort initial pull followed by a MissedRunCheck. Returns immediately.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(2)
	go s.fireLoop(ctx)
	go s.reconcileLoop(ctx)
	go func() {
		_ = s.Pull(ctx)
		s.missedRunCheck(ctx)
	}()
}

// Stop signals the loops to exit and waits for them to drain. Safe to
// call multiple times.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}

// Pull re-fetches the schedule list from controller and replaces
// the local cache. Exported so an HTTP /schedules/refresh handler can
// nudge an early reconcile after a CRUD write.
//
// Preserves LastFiredAt entries for schedule IDs that survive the diff
// so a freshly-pulled cache does not accidentally re-fire something
// that already fired this minute.
func (s *Scheduler) Pull(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.PullURL, nil)
	if err != nil {
		return err
	}
	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	var body struct {
		Schedules []LocalSchedule `json:"schedules"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	s.mu.Lock()
	preservedFired := map[string]time.Time{}
	for _, sch := range body.Schedules {
		if t, ok := s.cache.LastFiredAt[sch.ID]; ok {
			preservedFired[sch.ID] = t
		}
	}
	s.cache.Schedules = body.Schedules
	s.cache.LastFiredAt = preservedFired
	s.cache.LastFetchedAt = time.Now().UTC()
	cacheSnapshot := s.cache
	s.mu.Unlock()

	if err := saveCache(s.cfg.CachePath, cacheSnapshot); err != nil {
		log.Warn().Err(err).Str("path", s.cfg.CachePath).Msg("save cache after pull failed")
	}
	log.Info().Int("count", len(body.Schedules)).Str("node", s.cfg.NodeName).Msg("schedule pull complete")
	return nil
}

// Schedules returns a defensive copy of the current cache, sorted by Name.
// Intended for an agent's GET /schedules list handler.
func (s *Scheduler) Schedules() []LocalSchedule {
	s.mu.RLock()
	out := append([]LocalSchedule(nil), s.cache.Schedules...)
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Cache returns a defensive copy of the full cache including LastFiredAt.
// Intended for an agent's debug endpoint.
func (s *Scheduler) Cache() Cache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := Cache{
		Schedules:     append([]LocalSchedule(nil), s.cache.Schedules...),
		LastFetchedAt: s.cache.LastFetchedAt,
		LastFiredAt:   make(map[string]time.Time, len(s.cache.LastFiredAt)),
	}
	for k, v := range s.cache.LastFiredAt {
		c.LastFiredAt[k] = v
	}
	return c
}

// --- internals ---------------------------------------------------------

func (s *Scheduler) fireLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.FireInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case now := <-t.C:
			s.tick(ctx, now)
		}
	}
}

func (s *Scheduler) reconcileLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.PullInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-t.C:
			if err := s.Pull(ctx); err != nil {
				log.Warn().Err(err).Msg("schedule reconcile pull failed")
			}
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.RLock()
	schedules := append([]LocalSchedule(nil), s.cache.Schedules...)
	s.mu.RUnlock()

	for _, sch := range schedules {
		if !sch.Enabled {
			continue
		}
		if !Matches(sch.Schedule, now, s.cfg.Timezone) {
			continue
		}
		s.dispatch(ctx, sch, "schedule", false)
	}
}

// dispatch calls FireFunc and stamps LastFiredAt on the cache (regardless
// of FireFunc's return value — firing happened from the kit's perspective
// even if the job itself failed to start).
func (s *Scheduler) dispatch(ctx context.Context, sch LocalSchedule, triggerKey string, missedRecovery bool) {
	if err := s.cfg.Fire(ctx, sch, triggerKey, missedRecovery); err != nil {
		log.Error().Err(err).
			Str("schedule", sch.ID).
			Str("name", sch.Name).
			Str("action", sch.Action).
			Bool("missed_recovery", missedRecovery).
			Msg("schedule fire callback returned error")
		// Fall through — we still stamp LastFiredAt so the next tick
		// doesn't immediately re-fire the same minute.
	}

	s.mu.Lock()
	s.cache.LastFiredAt[sch.ID] = time.Now().UTC()
	cacheSnapshot := s.cache
	s.mu.Unlock()
	if err := saveCache(s.cfg.CachePath, cacheSnapshot); err != nil {
		log.Warn().Err(err).Msg("save cache after fire failed")
	}

	log.Info().
		Str("schedule", sch.ID).
		Str("name", sch.Name).
		Str("action", sch.Action).
		Str("repo", sch.RepoID).
		Str("trigger", triggerKey).
		Bool("missed_recovery", missedRecovery).
		Msg("schedule fired")
}

// missedRunCheck looks back 24 h per schedule and fires once for the
// most-recent missed window per MissPolicy. Runs after the initial pull
// in Start.
func (s *Scheduler) missedRunCheck(ctx context.Context) {
	s.mu.RLock()
	schedules := append([]LocalSchedule(nil), s.cache.Schedules...)
	lastFired := make(map[string]time.Time, len(s.cache.LastFiredAt))
	for k, v := range s.cache.LastFiredAt {
		lastFired[k] = v
	}
	s.mu.RUnlock()

	now := time.Now()

	for _, sch := range schedules {
		if !sch.Enabled || sch.MissPolicy == MissSkip {
			continue
		}
		if sch.MissPolicy == MissRunAll {
			// v1 truncation: run-all → run-once-within-24h to avoid a
			// missed-prune storm on first boot after a long outage.
			log.Warn().Str("schedule", sch.ID).Msg("run-all miss policy treated as run-once in v1")
		}

		windowEnd := now
		windowStart := now.Add(-24 * time.Hour)
		lastWindow, ok := findLastFireWindow(sch.Schedule, windowStart, windowEnd, s.cfg.Timezone)
		if !ok {
			continue
		}
		fired := lastFired[sch.ID]
		if fired.After(lastWindow) || fired.Equal(lastWindow) {
			continue
		}

		log.Info().
			Str("schedule", sch.ID).
			Time("missed_window", lastWindow).
			Time("last_fired", fired).
			Msg("missed-run recovery: firing catch-up")
		s.dispatch(ctx, sch, "schedule-recovery", true)
	}
}
