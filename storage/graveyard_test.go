package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buryFile creates a live file under root and buries it via g.Bury at `at`,
// returning the resulting entry name.
func buryFile(t *testing.T, g Graveyard, root, fileID, display, body string, at time.Time) string {
	t.Helper()
	live := filepath.Join(root, "live-"+display)
	if err := os.WriteFile(live, []byte(body), 0o644); err != nil {
		t.Fatalf("write live: %v", err)
	}
	name, err := g.Bury(fileID, live, display, at)
	if err != nil {
		t.Fatalf("bury: %v", err)
	}
	if name == "" {
		t.Fatalf("bury returned empty name for existing file")
	}
	return name
}

// TestListGraveyardSortedNewestLast buries three copies of one file at
// increasing times and checks ListGraveyard returns them chronologically
// (newest last), with GraveyardPath resolving to readable bytes.
func TestListGraveyardSortedNewestLast(t *testing.T) {
	root := t.TempDir()
	g := NewGraveyard(root)
	const id = "11111111-1111-5111-8111-111111111111"

	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	n1 := buryFile(t, g, root, id, "note.md", "v1", base)
	n2 := buryFile(t, g, root, id, "note.md", "v2", base.Add(24*time.Hour))
	n3 := buryFile(t, g, root, id, "note.md", "v3", base.Add(48*time.Hour))

	got, err := g.ListGraveyard(id)
	if err != nil {
		t.Fatalf("ListGraveyard: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3 (%v)", len(got), got)
	}
	// Newest last.
	if got[0] != n1 || got[1] != n2 || got[2] != n3 {
		t.Errorf("order = %v, want [%s %s %s]", got, n1, n2, n3)
	}
	// GraveyardPath resolves to the buried bytes of the newest.
	b, err := os.ReadFile(g.GraveyardPath(id, got[2]))
	if err != nil {
		t.Fatalf("read newest: %v", err)
	}
	if string(b) != "v3" {
		t.Errorf("newest body = %q, want v3", b)
	}
}

// TestListGraveyardMissing returns an empty slice (no error) for a fileID that
// was never buried.
func TestListGraveyardMissing(t *testing.T) {
	g := NewGraveyard(t.TempDir())
	got, err := g.ListGraveyard("22222222-2222-5222-8222-222222222222")
	if err != nil {
		t.Fatalf("ListGraveyard: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

// TestListGraveyardIDsReflectsSweep confirms ListGraveyardIDs returns the set
// of recoverable fileIDs and that Sweep removing an id's last entry (and its
// now-empty subdir) drops it from the set — the property the app relies on to
// bound the deleted tree to the retention window.
func TestListGraveyardIDsReflectsSweep(t *testing.T) {
	root := t.TempDir()
	g := NewGraveyard(root)
	const oldID = "aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaaa"
	const newID = "bbbbbbbb-bbbb-5bbb-8bbb-bbbbbbbbbbbb"

	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	buryFile(t, g, root, oldID, "old.txt", "old", now.Add(-40*24*time.Hour)) // 40d ago
	buryFile(t, g, root, newID, "new.txt", "new", now.Add(-1*24*time.Hour))  // 1d ago

	ids, err := g.ListGraveyardIDs()
	if err != nil {
		t.Fatalf("ListGraveyardIDs: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("ids = %v, want both %s and %s", ids, oldID, newID)
	}

	// 30-day retention sweep: the 40-day-old entry expires, the 1-day-old stays.
	pruned, err := g.Sweep(30, now)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if pruned != 1 {
		t.Errorf("pruned = %d, want 1", pruned)
	}

	ids, err = g.ListGraveyardIDs()
	if err != nil {
		t.Fatalf("ListGraveyardIDs after sweep: %v", err)
	}
	if len(ids) != 1 || ids[0] != newID {
		t.Errorf("ids after sweep = %v, want [%s]", ids, newID)
	}
	// The old id's subdir must be gone (so the app never lists an unrecoverable file).
	if _, statErr := os.Stat(g.FileGraveyardDir(oldID)); !os.IsNotExist(statErr) {
		t.Errorf("old id subdir should be removed, stat err=%v", statErr)
	}
}

// TestParseStampRoundTrip confirms the exported ParseStamp inverts the stamp
// format embedded in entry names by Bury.
func TestParseStampRoundTrip(t *testing.T) {
	root := t.TempDir()
	g := NewGraveyard(root)
	const id = "cccccccc-cccc-5ccc-8ccc-cccccccccccc"
	at := time.Date(2026, 5, 14, 3, 0, 0, 0, time.UTC)
	name := buryFile(t, g, root, id, "report.md", "body", at)

	iso, display, ok := SplitVersionName(name)
	if !ok {
		t.Fatalf("SplitVersionName(%q) not ok", name)
	}
	if !strings.HasSuffix(display, "report.md") {
		t.Errorf("display = %q, want suffix report.md", display)
	}
	got, err := ParseStamp(iso)
	if err != nil {
		t.Fatalf("ParseStamp(%q): %v", iso, err)
	}
	if !got.Equal(at) {
		t.Errorf("ParseStamp = %v, want %v", got, at)
	}
}
