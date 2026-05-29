package jobstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// JobRef is the minimal job identity + state the reconcile snapshot carries to
// the controller. ExitCode is 0 when the column is NULL or absent. The
// controller derives any missing terminal detail (completed_at, default
// exit_code) itself — reconcile is a fallback for lost terminal events, not the
// primary data path, so a minimal, schema-agnostic shape is deliberate.
type JobRef struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	ExitCode int    `json:"exit_code"`
}

// ListOptions configures ListForReconcile. Table/column names are
// caller-supplied trusted identifiers (interpolated into SQL); values are bound
// parameters. The recent-terminal window predicate is caller-supplied because
// the agents' completion-time columns differ in type (duplicacy stores Unix
// nanoseconds as INTEGER, gdrive stores TIMESTAMP), so a single generic
// comparison expression can't be hardcoded here.
type ListOptions struct {
	Table          string   // required
	IDColumn       string   // default "id"
	StateColumn    string   // default "state"
	ExitCodeColumn string   // optional; "" → ExitCode always 0
	RunningStates  []string // default {"running","pending"}
	TerminalStates []string // default {"completed","failed","cancelled"}

	// RecentTerminalPredicate is an SQL boolean fragment selecting recently
	// finished jobs, e.g. "completed_at_ns >= ?" (duplicacy) or
	// "completed_at >= ?" (gdrive). RecentTerminalArg is bound to its single
	// placeholder. When the predicate is empty, ALL terminal rows are returned
	// (caller is responsible for keeping the table bounded).
	RecentTerminalPredicate string
	RecentTerminalArg       any
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// ListForReconcile returns every non-terminal job plus the recent-terminal jobs
// from the agent's LOCAL store — the agent's authoritative declaration of "what
// I am running and what I just finished". The controller converges its view to
// this set (stuck-running rows the agent no longer reports get resolved). Shared
// here so both agents issue an identical query shape rather than copy-pasting it.
func ListForReconcile(ctx context.Context, db *sql.DB, opts ListOptions) ([]JobRef, error) {
	if db == nil {
		return nil, fmt.Errorf("jobstore: db is required")
	}
	if opts.Table == "" {
		return nil, fmt.Errorf("jobstore: Table is required")
	}
	idCol := or(opts.IDColumn, "id")
	stateCol := or(opts.StateColumn, "state")
	running := opts.RunningStates
	if len(running) == 0 {
		running = []string{"running", "pending"}
	}
	terminal := opts.TerminalStates
	if len(terminal) == 0 {
		terminal = []string{"completed", "failed", "cancelled"}
	}

	exitSel := "0"
	if opts.ExitCodeColumn != "" {
		exitSel = fmt.Sprintf("COALESCE(%s, 0)", opts.ExitCodeColumn)
	}

	in := func(states []string) (string, []any) {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(states)), ",")
		args := make([]any, len(states))
		for i, s := range states {
			args[i] = s
		}
		return ph, args
	}
	runPH, runArgs := in(running)
	termPH, termArgs := in(terminal)

	termClause := fmt.Sprintf("%s IN (%s)", stateCol, termPH)
	args := append([]any{}, runArgs...)
	args = append(args, termArgs...)
	if opts.RecentTerminalPredicate != "" {
		termClause = fmt.Sprintf("(%s AND %s)", termClause, opts.RecentTerminalPredicate)
		args = append(args, opts.RecentTerminalArg)
	}

	q := fmt.Sprintf(
		"SELECT %s, %s, %s FROM %s WHERE %s IN (%s) OR %s",
		idCol, stateCol, exitSel, opts.Table, stateCol, runPH, termClause,
	)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("jobstore: list for reconcile: %w", err)
	}
	defer rows.Close()
	var out []JobRef
	for rows.Next() {
		var r JobRef
		if err := rows.Scan(&r.ID, &r.State, &r.ExitCode); err != nil {
			return nil, fmt.Errorf("jobstore: scan reconcile row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("jobstore: iterate reconcile rows: %w", err)
	}
	return out, nil
}
