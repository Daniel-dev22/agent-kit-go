package dirsize

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The incomplete flag marks a directory total as a LOWER BOUND. Three properties
// matter and each has a negative control below:
//
//   - it is set only when content went uncounted (a vanished entry, an exclude, a
//     mount-guard skip and a symlink are all deliberate and must NOT set it);
//   - it propagates to every ancestor and survives the cached-reuse branch;
//   - a complete entry serialises byte-for-byte as before the field existed, and an
//     older cache file loads as complete, so no host re-walks from cold.

func TestCompleteEntryOmitsTheFlagAndOldFilesLoadComplete(t *testing.T) {
	b, err := json.Marshal(Entry{Bytes: 10, FileCount: 2, ComputedAtS: 1700000000, LastWalkMS: 5})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"i"`) {
		t.Fatalf("a complete entry must not carry the flag, got %s", b)
	}
	want := `{"b":10,"f":2,"t":1700000000,"w":5}`
	if string(b) != want {
		t.Fatalf("complete entry bytes changed: %s want %s", b, want)
	}

	inc, _ := json.Marshal(Entry{Bytes: 10, Incomplete: true})
	if !strings.Contains(string(inc), `"i":true`) {
		t.Fatalf("an incomplete entry must carry the flag, got %s", inc)
	}

	var old Entry
	if err := json.Unmarshal([]byte(`{"b":777,"f":5,"t":1700000000}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.Incomplete {
		t.Fatal("an entry written before the flag existed must load as complete")
	}
}

func TestOrdinaryTreeIsComplete(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(filepath.Join(root, "sub", "node_modules"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(root, "a"), 10)
	writeFileN(t, filepath.Join(root, "sub", "b"), 20)
	writeFileN(t, filepath.Join(root, "sub", "node_modules", "junk"), 999) // excluded by name
	if err := os.Symlink(filepath.Join(root, "a"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	cache := newTestCache(dir)
	g := NewGatherer(baselineConfig(dir), cache)
	b, c, inc, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if inc || b != 30 || c != 2 {
		t.Fatalf("walk = (%d, %d, incomplete=%v), want (30, 2, false): an exclude, a symlink and a clean tree are deliberate, not unread", b, c, inc)
	}
	for _, r := range cacheRows(t, cache, root) {
		if e, _ := cache.Get(filepath.Join(root, r.Rel)); e.Incomplete {
			t.Errorf("%q flagged incomplete in a fully readable tree", r.Rel)
		}
	}
}

func TestVanishedDirectoryIsNotIncomplete(t *testing.T) {
	dir := t.TempDir()
	cache := newTestCache(dir)
	g := NewGatherer(baselineConfig(dir), cache)
	gone := filepath.Join(dir, "gone")
	b, c, inc, err := g.walk(context.Background(), gone, 0)
	if err != nil {
		t.Fatal(err)
	}
	if inc || b != 0 || c != 0 {
		t.Fatalf("a directory that vanished is a true zero, got (%d, %d, incomplete=%v)", b, c, inc)
	}
	if e, ok := cache.Get(gone); !ok || e.Incomplete {
		t.Fatalf("the backoff entry must exist and be complete, got %+v ok=%v", e, ok)
	}
}

func TestUnreadableDirectoryIsIncompleteAndPropagatesToEveryAncestor(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can open a mode-000 directory, so the failure cannot be provoked")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	mid := filepath.Join(root, "mid")
	sealed := filepath.Join(mid, "sealed")
	if err := os.MkdirAll(sealed, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(root, "a"), 10)
	writeFileN(t, filepath.Join(mid, "b"), 20)
	writeFileN(t, filepath.Join(sealed, "hidden"), 999)
	if err := os.Chmod(sealed, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o700) })

	cache := newTestCache(dir)
	g := NewGatherer(baselineConfig(dir), cache)
	b, c, inc, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !inc {
		t.Fatal("a directory that could not be opened must make the total a lower bound")
	}
	if b != 30 || c != 2 {
		t.Fatalf("readable part = (%d, %d), want (30, 2)", b, c)
	}
	for _, p := range []string{sealed, mid, root} {
		if e, ok := cache.Get(p); !ok || !e.Incomplete {
			t.Errorf("%s: want a cached incomplete entry, got %+v ok=%v", p, e, ok)
		}
	}
	// The sibling that WAS fully read must not be dragged into the flag.
	if e, ok := cache.Get(filepath.Join(dir, "root")); ok && e.Bytes != 30 {
		t.Errorf("root bytes = %d, want 30", e.Bytes)
	}
}

func TestIncompleteSurvivesTheCachedReuseBranch(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	child := filepath.Join(root, "child")
	if err := os.MkdirAll(child, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(root, "a"), 10)

	cache := newTestCache(dir)
	// A fresh (not-due) incomplete child: the parent must fold in its total AND its flag
	// without descending, or an incomplete subtree silently becomes a confident one.
	cache.Put(child, Entry{Bytes: 500, FileCount: 7, ComputedAtS: time.Now().Unix(), Incomplete: true})
	g := NewGatherer(baselineConfig(dir), cache)
	b, c, inc, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !inc || b != 510 || c != 8 {
		t.Fatalf("walk = (%d, %d, incomplete=%v), want (510, 8, true)", b, c, inc)
	}
	if e, _ := cache.Get(root); !e.Incomplete {
		t.Fatal("the parent entry must carry the flag it inherited from the reused child")
	}
}
