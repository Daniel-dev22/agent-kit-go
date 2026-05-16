// Package scheduler implements a per-agent cron scheduler shared between
// the standalone "agent API" services that pair with the controller
// cluster (duplicacy-agent, gdrive-agent, future agents).
//
// # Operating model
//
// The agent owns its own cron scheduling — controller scheduler-api
// and EDA rulebooks are NOT in the firing path. Each agent runs:
//
//  1. A 1-minute fire loop that walks the in-memory schedule cache and
//     calls the agent-provided FireFunc for any schedule whose cron
//     expression matches the current minute.
//  2. A 5-minute reconcile loop that re-pulls the schedule list from
//     controller (GET <PullURL>) and replaces the local cache,
//     preserving LastFiredAt entries for surviving schedule IDs.
//  3. A persistent on-disk cache (atomic write to a JSON file) so a pod
//     restart does not (a) re-fire a schedule that already fired this
//     minute, or (b) flood with missed-run recovery for windows that
//     were already covered.
//
// The agent provides:
//
//   - The PullURL (e.g. "https://controller.../api/duplicacy/schedules?node=nas")
//   - A FireFunc callback that runs the actual job (duplicacy CLI invocation,
//     gdrive backup, etc.)
//   - A NodeName used only for logging
//   - A CachePath for disk persistence (typically under CONFIG_DIR)
//   - Optional: HTTP client (defaults to http.DefaultClient — usually you
//     want to pass a client that already has bearer-auth + traefik dial
//     plumbing installed)
//
// # Cron semantics
//
// Identical to controller scheduler-api/main.go's matcher so the
// JSON shape and matching behaviour are identical whether a schedule
// fires from the central scheduler-api or from the agent. Days use the
// 3-letter lowercase weekday names (mon, tue, ...). Hours and Minute are
// numeric strings. "*" wildcards both days and hours. IntervalMinutes
// overrides the days/hours/minute fields when set.
//
// All matching happens in America/New_York by default — override via
// Config.Timezone.
//
// # MissPolicy
//
//   - MissSkip: never replay missed windows.
//   - MissRunOnceWithin24h: on Start, after the first successful pull,
//     check if a fire window in the last 24h was missed (lastFired
//     before the last matching window) and fire ONCE as catch-up.
//   - MissRunAll: currently treated identically to MissRunOnceWithin24h
//     to avoid a missed-prune storm. Logged when truncated.
package scheduler
