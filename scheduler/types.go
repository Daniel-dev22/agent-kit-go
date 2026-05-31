package scheduler

import "time"

// Schedule is the cron-like shape pulled from controller. Mirrors
// scheduler-api/main.go so the JSON wire format is identical whether the
// schedule fires from the central scheduler-api or from an agent.
//
// On is a non-cron trigger sentinel: when set (e.g. {"on":"after_wave"}), the
// schedule NEVER fires on the clock — Matches always returns false and
// missed-run recovery skips it. Such rows are fired on demand by an agent's
// chain controller via FireSchedule when an upstream condition is met (e.g.
// "the nightly backup+copy wave finished"). Days/Hours/Minute are ignored when
// On is set. Empty On means a normal cron schedule.
type Schedule struct {
	Days            []string `json:"days"`
	Hours           []string `json:"hours"`
	Minute          string   `json:"minute"`
	IntervalMinutes *int     `json:"interval_minutes,omitempty"`
	On              string   `json:"on,omitempty"`
}

// MissPolicy controls catch-up behaviour when an agent boots after
// missing a fire window.
type MissPolicy string

const (
	// MissSkip: never replay missed windows.
	MissSkip MissPolicy = "skip"
	// MissRunOnceWithin24 fires once on boot if the most recent window
	// within the last 24 h was missed (lastFired before that window).
	MissRunOnceWithin24 MissPolicy = "run-once-within-24h"
	// MissRunAll is currently treated identically to MissRunOnceWithin24
	// to avoid a missed-prune storm; the truncation is logged.
	MissRunAll MissPolicy = "run-all"
)

// LocalSchedule is one row pulled from controller plus the agent-side
// bookkeeping the kit maintains.
//
// Action is an opaque string — each agent defines its own action vocabulary
// ("backup" / "check" / "prune" for duplicacy; "backup" / "prune" for
// gdrive). The kit does not interpret it.
//
// Params is a free-form map that the agent's FireFunc decodes. Use the
// Param* helpers in this package to extract typed values without panicking
// on JSON-decoded interface{} variations.
type LocalSchedule struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	RepoID     string                 `json:"repo_id"`
	Action     string                 `json:"action"`
	Storage    string                 `json:"storage,omitempty"`
	Schedule   Schedule               `json:"schedule"`
	MissPolicy MissPolicy             `json:"miss_policy"`
	Enabled    bool                   `json:"enabled"`
	Params     map[string]interface{} `json:"params,omitempty"`
	Version    int64                  `json:"version"`
}

// Cache is the disk-persisted shape — exported so an agent's debug
// endpoint can render it directly.
type Cache struct {
	Schedules     []LocalSchedule      `json:"schedules"`
	LastFiredAt   map[string]time.Time `json:"last_fired_at"`
	LastFetchedAt time.Time            `json:"last_fetched_at"`
}
