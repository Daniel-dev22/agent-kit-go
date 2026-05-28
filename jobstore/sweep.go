// Package jobstore holds the small, genuinely-shared slice of agent job
// persistence. The agents' job *schemas* differ substantially (duplicacy tracks
// chunk/dedup progress; gdrive tracks file counts and bytes), so there is no
// generic job table — only the boot-time orphan sweep is identical, and that is
// shared here.
//
// MarkOrphanedFailed flips any job row still recorded as running/pending after a
// fresh boot to a terminal failed state. A JobHandle/goroutine is tied to a
// runtime instance, so a row still "running" after the process restarts is by
// definition an orphan from a prior crash/redeploy. Operating on the agent's
// LOCAL SQLite (not a remote query) means the sweep can't be defeated by the
// controller being unreachable at boot.
package jobstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SweepOptions configures MarkOrphanedFailed. Table/column names are
// caller-supplied trusted identifiers (interpolated into SQL); values are always
// bound parameters.
type SweepOptions struct {
	Table         string   // required
	IDColumn      string   // default "id"
	StateColumn   string   // default "state"
	RunningStates []string // default {"running","pending"}
	FailedState   string   // default "failed"

	// Optional terminal columns to set on swept rows.
	ErrorColumn     string // if set, set to ErrorMessage
	ErrorMessage    string
	ExitCodeColumn  string // if set, set to ExitCode
	ExitCode        int
	CompletedColumn string // if set, set to CURRENT_TIMESTAMP
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// MarkOrphanedFailed flips rows in RunningStates to FailedState and returns the
// affected job IDs (so the caller can enqueue synthetic terminal events for the
// controller). Returns (nil, nil) when there is nothing to sweep.
func MarkOrphanedFailed(ctx context.Context, db *sql.DB, opts SweepOptions) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("jobstore: db is required")
	}
	if opts.Table == "" {
		return nil, fmt.Errorf("jobstore: Table is required")
	}
	idCol := orDefault(opts.IDColumn, "id")
	stateCol := orDefault(opts.StateColumn, "state")
	failed := orDefault(opts.FailedState, "failed")
	running := opts.RunningStates
	if len(running) == 0 {
		running = []string{"running", "pending"}
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(running)), ",")
	stateArgs := make([]any, len(running))
	for i, s := range running {
		stateArgs[i] = s
	}

	// Collect affected IDs first so the caller can react (enqueue terminal events).
	selQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s)", idCol, opts.Table, stateCol, placeholders)
	rows, err := db.QueryContext(ctx, selQ, stateArgs...)
	if err != nil {
		return nil, fmt.Errorf("jobstore: select orphans: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("jobstore: scan orphan id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("jobstore: iterate orphans: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	sets := []string{fmt.Sprintf("%s = ?", stateCol)}
	setArgs := []any{failed}
	if opts.ErrorColumn != "" {
		sets = append(sets, fmt.Sprintf("%s = ?", opts.ErrorColumn))
		setArgs = append(setArgs, opts.ErrorMessage)
	}
	if opts.ExitCodeColumn != "" {
		sets = append(sets, fmt.Sprintf("%s = ?", opts.ExitCodeColumn))
		setArgs = append(setArgs, opts.ExitCode)
	}
	if opts.CompletedColumn != "" {
		sets = append(sets, fmt.Sprintf("%s = CURRENT_TIMESTAMP", opts.CompletedColumn))
	}

	updQ := fmt.Sprintf("UPDATE %s SET %s WHERE %s IN (%s)",
		opts.Table, strings.Join(sets, ", "), stateCol, placeholders)
	args := append(setArgs, stateArgs...)
	if _, err := db.ExecContext(ctx, updQ, args...); err != nil {
		return nil, fmt.Errorf("jobstore: update orphans: %w", err)
	}
	return ids, nil
}
