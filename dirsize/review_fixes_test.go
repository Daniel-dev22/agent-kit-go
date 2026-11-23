package dirsize

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Regression tests for the Phase 1a review round: each one fails on the code it names.

// A prefix exclude, a SkipPath hit and a plain basename exclude are all deliberate and
// must never mark a total as a lower bound.
func TestDeliberateSkipsNeverSetIncomplete(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	for _, d := range []string{"keep", "prefixed/deep", "webcache/x", "node_modules/y"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeFileN(t, filepath.Join(root, "keep", "k"), 10)
	writeFileN(t, filepath.Join(root, "prefixed", "deep", "p"), 999)
	writeFileN(t, filepath.Join(root, "webcache", "x", "w"), 999)
	writeFileN(t, filepath.Join(root, "node_modules", "y", "n"), 999)

	opts := baselineConfig(dir)
	opts.ExcludePaths = func() []string { return []string{filepath.Join(root, "prefixed")} }
	opts.SkipPath = func(p string) bool { return strings.HasSuffix(p, "webcache") }
	cache := newTestCache(dir)
	g := NewGatherer(opts, cache)
	b, c, inc, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatal(err)
	}
	if inc || b != 10 || c != 1 {
		t.Fatalf("walk = (%d, %d, incomplete=%v), want (10, 1, false): a deliberate skip must not be reported as unread", b, c, inc)
	}
	cache.Range(func(p string, e Entry) {
		if e.Incomplete {
			t.Errorf("%s flagged incomplete in a tree whose only skips are deliberate", p)
		}
	})
}

// The flag must not leak sideways: a fully read sibling subtree stays complete while
// its unreadable neighbour and every ancestor are flagged.
func TestReadableSiblingStaysComplete(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can open a mode-000 directory, so the failure cannot be provoked")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	good := filepath.Join(root, "good")
	sealed := filepath.Join(root, "sealed")
	for _, d := range []string{filepath.Join(good, "sub"), sealed} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeFileN(t, filepath.Join(good, "sub", "f"), 7)
	writeFileN(t, filepath.Join(sealed, "hidden"), 999)
	if err := os.Chmod(sealed, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o700) })

	cache := newTestCache(dir)
	g := NewGatherer(baselineConfig(dir), cache)
	if _, _, inc, err := g.walk(context.Background(), root, 0); err != nil || !inc {
		t.Fatalf("walk inc=%v err=%v, want incomplete", inc, err)
	}
	for _, p := range []string{good, filepath.Join(good, "sub")} {
		if e, ok := cache.Get(p); !ok || e.Incomplete {
			t.Errorf("%s: want a cached COMPLETE entry, got %+v ok=%v", p, e, ok)
		}
	}
	for _, p := range []string{sealed, root} {
		if e, ok := cache.Get(p); !ok || !e.Incomplete {
			t.Errorf("%s: want a cached incomplete entry, got %+v ok=%v", p, e, ok)
		}
	}
}

// A cache file holding the JSON literal null must load as cold, not as a nil map that
// panics on the first Put.
func TestNullCacheFileLoadsCold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir_sizes.v2.json")
	if err := os.WriteFile(path, []byte("null"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewCache(path, "")
	c.load()
	c.Put("/p", Entry{Bytes: 1}) // panicked with "assignment to entry in nil map" before the guard
	if e, ok := c.Get("/p"); !ok || e.Bytes != 1 {
		t.Fatalf("Get after Put = %+v ok=%v", e, ok)
	}
}

// RunPass is exported, so a consumer may call it while Start's loop is mid-pass. The
// pass mutex serialises them; under -race an unguarded lastAttempt write fails here.
func TestRunPassIsSingleFlight(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(filepath.Join(root, "a"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(root, "a", "f"), 5)

	opts := baselineConfig(dir)
	opts.Roots = func() []string { return []string{root} }
	cache := newTestCache(dir)
	g := NewGatherer(opts, cache)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.RunPass(context.Background())
		}()
	}
	wg.Wait()
	if e, ok := cache.Get(root); !ok || e.Bytes != 5 || e.FileCount != 1 {
		t.Fatalf("root = %+v ok=%v, want 5 bytes in 1 file", e, ok)
	}
}
