package jobstore

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func setupListDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.Exec(`CREATE TABLE jobs (
		id TEXT PRIMARY KEY,
		state TEXT,
		exit_code INTEGER,
		completed_at_ns INTEGER
	)`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return db
}

func insert(t *testing.T, db *sql.DB, id, state string, exit int, completedNs int64) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO jobs (id,state,exit_code,completed_at_ns) VALUES (?,?,?,?)`,
		id, state, exit, completedNs); err != nil {
		t.Fatalf("insert %s: %v", id, err)
	}
}

func TestListForReconcile_NonTerminalAlwaysIncluded(t *testing.T) {
	db := setupListDB(t)
	insert(t, db, "run1", "running", 0, 0)
	insert(t, db, "pend1", "pending", 0, 0)
	// An old terminal that the recent-window predicate should exclude.
	insert(t, db, "oldDone", "completed", 0, 1)

	refs, err := ListForReconcile(context.Background(), db, ListOptions{
		Table:                   "jobs",
		ExitCodeColumn:          "exit_code",
		RecentTerminalPredicate: "completed_at_ns >= ?",
		RecentTerminalArg:       int64(1_000_000_000), // cutoff well after oldDone's ts=1
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := map[string]string{}
	for _, r := range refs {
		got[r.ID] = r.State
	}
	if _, ok := got["run1"]; !ok {
		t.Errorf("running job missing: %v", got)
	}
	if _, ok := got["pend1"]; !ok {
		t.Errorf("pending job missing: %v", got)
	}
	if _, ok := got["oldDone"]; ok {
		t.Errorf("old terminal should be excluded by recent window: %v", got)
	}
}

func TestListForReconcile_RecentTerminalIncludedWithExit(t *testing.T) {
	db := setupListDB(t)
	insert(t, db, "fresh", "failed", 137, 5_000_000_000)

	refs, err := ListForReconcile(context.Background(), db, ListOptions{
		Table:                   "jobs",
		ExitCodeColumn:          "exit_code",
		RecentTerminalPredicate: "completed_at_ns >= ?",
		RecentTerminalArg:       int64(1_000_000_000),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("want 1 ref, got %d (%+v)", len(refs), refs)
	}
	if refs[0].ID != "fresh" || refs[0].State != "failed" || refs[0].ExitCode != 137 {
		t.Errorf("ref = %+v", refs[0])
	}
}

func TestListForReconcile_NoPredicateReturnsAllTerminal(t *testing.T) {
	db := setupListDB(t)
	insert(t, db, "run1", "running", 0, 0)
	insert(t, db, "old", "completed", 0, 1)
	refs, err := ListForReconcile(context.Background(), db, ListOptions{
		Table:          "jobs",
		ExitCodeColumn: "exit_code",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("want 2 (running + all terminal), got %d", len(refs))
	}
}

func TestListForReconcile_RequiresTable(t *testing.T) {
	db := setupListDB(t)
	if _, err := ListForReconcile(context.Background(), db, ListOptions{}); err == nil {
		t.Fatal("expected error for missing Table")
	}
}
