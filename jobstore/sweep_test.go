package jobstore

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.sqlite"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMarkOrphanedFailed(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	if _, err := db.Exec(`CREATE TABLE jobs (
		id TEXT PRIMARY KEY, state TEXT, error TEXT, exit_code INTEGER, completed_at TIMESTAMP
	)`); err != nil {
		t.Fatal(err)
	}
	rows := [][2]string{
		{"a", "running"}, {"b", "pending"}, {"c", "completed"}, {"d", "failed"},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO jobs (id, state) VALUES (?, ?)`, r[0], r[1]); err != nil {
			t.Fatal(err)
		}
	}

	ids, err := MarkOrphanedFailed(ctx, db, SweepOptions{
		Table:           "jobs",
		ErrorColumn:     "error",
		ErrorMessage:    "agent restart orphan",
		ExitCodeColumn:  "exit_code",
		ExitCode:        137,
		CompletedColumn: "completed_at",
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("affected ids = %v, want [a b]", ids)
	}

	// a,b now failed with error+exit_code+completed_at set; c,d untouched.
	for _, id := range []string{"a", "b"} {
		var state, errMsg string
		var exit int
		var completed sql.NullString
		if err := db.QueryRow(`SELECT state, error, exit_code, completed_at FROM jobs WHERE id=?`, id).
			Scan(&state, &errMsg, &exit, &completed); err != nil {
			t.Fatal(err)
		}
		if state != "failed" || errMsg != "agent restart orphan" || exit != 137 || !completed.Valid {
			t.Fatalf("row %s = (%s,%s,%d,%v), want failed/orphan/137/set", id, state, errMsg, exit, completed.Valid)
		}
	}
	var cState string
	if err := db.QueryRow(`SELECT state FROM jobs WHERE id='c'`).Scan(&cState); err != nil {
		t.Fatal(err)
	}
	if cState != "completed" {
		t.Fatalf("completed row was modified: %s", cState)
	}
}

func TestMarkOrphanedFailedNoOp(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE jobs (id TEXT, state TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs VALUES ('x','completed')`); err != nil {
		t.Fatal(err)
	}
	ids, err := MarkOrphanedFailed(context.Background(), db, SweepOptions{Table: "jobs"})
	if err != nil {
		t.Fatal(err)
	}
	if ids != nil {
		t.Fatalf("expected no affected ids, got %v", ids)
	}
}
