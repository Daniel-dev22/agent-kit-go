package dirsize

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testExcludeBasenames is what the duplicacy agent skips by name; the walker itself has
// no built-in list, so tests that rely on a skipped name configure one.
var testExcludeBasenames = map[string]struct{}{"node_modules": {}}

func fixed(s []string) func() []string { return func() []string { return s } }

// The cache file names are the caller's choice; these are duplicacy's, kept so the
// moved v1/v2 versioning tests still exercise the real shape.
const (
	testCacheFile  = "dir_sizes.v2.json"
	testLegacyFile = "dir_sizes.json"
)

func newTestCache(dir string) *Cache {
	return NewCache(filepath.Join(dir, testCacheFile), filepath.Join(dir, testLegacyFile))
}

func testSizeConfig(dir string) Options {
	return Options{
		ExcludeBasenames:   testExcludeBasenames,
		Enabled:            true,
		LargeFileThreshold: 50000,
		SlowWalk:           30 * time.Second,
		LargeRefresh:       24 * time.Hour,
		SmallRefresh:       6 * time.Hour,
		WalkTimeout:        30 * time.Minute,
		StepSleep:          0,
	}
}

func writeFileN(t *testing.T, path string, n int) {
	t.Helper()
	if err := os.WriteFile(path, make([]byte, n), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestDirSizeWalkAccumulates verifies bottom-up accumulation, exclusion of
// excludeBasenames dirs, symlink skipping, and per-directory cache population.
func TestDirSizeWalkAccumulates(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	sub := filepath.Join(root, "sub")
	deeper := filepath.Join(sub, "deeper")
	excluded := filepath.Join(root, "node_modules") // in excludeBasenames
	for _, d := range []string{root, sub, deeper, excluded} {
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeFileN(t, filepath.Join(root, "a.txt"), 10)
	writeFileN(t, filepath.Join(sub, "b.txt"), 20)
	writeFileN(t, filepath.Join(sub, "c.txt"), 5)
	writeFileN(t, filepath.Join(deeper, "d.txt"), 100)
	writeFileN(t, filepath.Join(excluded, "junk.txt"), 999) // must NOT count
	// Symlink to a file — must be skipped, not followed.
	if err := os.Symlink(filepath.Join(root, "a.txt"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	cache := newTestCache(dir)
	g := NewGatherer(testSizeConfig(dir), cache)

	gotB, gotC, _, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if wantB := int64(135); gotB != wantB { // 10+20+5+100
		t.Errorf("root bytes = %d, want %d", gotB, wantB)
	}
	if wantC := int64(4); gotC != wantC {
		t.Errorf("root file count = %d, want %d", gotC, wantC)
	}

	cases := []struct {
		path  string
		bytes int64
		count int64
	}{
		{root, 135, 4},
		{sub, 125, 3},
		{deeper, 100, 1},
	}
	for _, tc := range cases {
		e, ok := cache.Get(tc.path)
		if !ok {
			t.Errorf("cache missing entry for %s", tc.path)
			continue
		}
		if e.Bytes != tc.bytes || e.FileCount != tc.count {
			t.Errorf("%s = (%d bytes, %d files), want (%d, %d)", tc.path, e.Bytes, e.FileCount, tc.bytes, tc.count)
		}
	}
	// Excluded directory must never be cached/descended.
	if _, ok := cache.Get(excluded); ok {
		t.Errorf("excluded dir %s should not be cached", excluded)
	}
}

// TestDirSizeWalkReusesNotDue verifies a not-due child is folded in from cache
// without re-descending (its on-disk content is changed but the cached value is
// kept because the entry is within its cadence window).
func TestDirSizeWalkReusesNotDue(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(sub, "b.txt"), 20)

	cache := newTestCache(dir)
	g := NewGatherer(testSizeConfig(dir), cache)

	// Seed `sub` with a fresh, deliberately-wrong cached total. Because it's
	// within the small cadence window, the parent walk must reuse it verbatim.
	cache.put(sub, Entry{Bytes: 999, FileCount: 7, ComputedAtS: time.Now().Unix()})

	gotB, gotC, _, err := g.walk(context.Background(), root, 0)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if gotB != 999 || gotC != 7 {
		t.Errorf("reuse failed: got (%d,%d), want (999,7) from cached child", gotB, gotC)
	}
}

func TestDirSizeCadence(t *testing.T) {
	g := NewGatherer(testSizeConfig(t.TempDir()), nil)
	cases := []struct {
		name string
		e    Entry
		want time.Duration
	}{
		{"small", Entry{FileCount: 100}, 6 * time.Hour},
		{"large by count", Entry{FileCount: 60000}, 24 * time.Hour},
		{"large by slow walk", Entry{FileCount: 100, LastWalkMS: 40000}, 24 * time.Hour},
	}
	for _, tc := range cases {
		if got := g.cadence(tc.e); got != tc.want {
			t.Errorf("%s: cadence = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDirSizeDue(t *testing.T) {
	dir := t.TempDir()
	cache := newTestCache(dir)
	g := NewGatherer(testSizeConfig(dir), cache)
	now := time.Now()

	if !g.due("/never/computed", now) {
		t.Error("uncached path should be due")
	}
	cache.put("/fresh", Entry{FileCount: 10, ComputedAtS: now.Unix()})
	if g.due("/fresh", now) {
		t.Error("freshly computed small dir should not be due")
	}
	cache.put("/stale", Entry{FileCount: 10, ComputedAtS: now.Add(-7 * time.Hour).Unix()})
	if !g.due("/stale", now) {
		t.Error("small dir older than 6h should be due")
	}
	cache.put("/bigrecent", Entry{FileCount: 60000, ComputedAtS: now.Add(-7 * time.Hour).Unix()})
	if g.due("/bigrecent", now) {
		t.Error("large dir within 24h should not be due")
	}
}

func TestDirSizeCachePersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := newTestCache(dir)
	c.put("/a", Entry{Bytes: 100, FileCount: 3, ComputedAtS: time.Now().Unix()})
	c.put("/b", Entry{Bytes: 200, FileCount: 9, ComputedAtS: time.Now().Unix()})
	if err := c.save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	c2 := newTestCache(dir)
	c2.load()
	for _, p := range []string{"/a", "/b"} {
		want, _ := c.Get(p)
		got, ok := c2.Get(p)
		if !ok {
			t.Errorf("reloaded cache missing %s", p)
			continue
		}
		if got.Bytes != want.Bytes || got.FileCount != want.FileCount || got.ComputedAtS != want.ComputedAtS {
			t.Errorf("%s round-trip mismatch: got %+v want %+v", p, got, want)
		}
	}
}

func TestSizeGathererExcludePaths(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep")
	excl := filepath.Join(dir, "excluded")
	if err := os.MkdirAll(filepath.Join(keep, "k"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(excl, "e"), 0700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(keep, "kf"), 10)
	writeFileN(t, filepath.Join(excl, "ef"), 999) // must be excluded from the sum

	cfg := testSizeConfig(dir)
	cfg.Roots = fixed([]string{keep, excl})
	cfg.ExcludePaths = fixed([]string{excl})
	cache := newTestCache(dir)
	g := NewGatherer(cfg, cache)

	// roots() drops the excluded backup root entirely.
	roots := g.roots()
	if len(roots) != 1 || roots[0] != keep {
		t.Fatalf("roots() = %v, want [%s]", roots, keep)
	}

	// Walking the parent must skip the excluded subtree (size + count).
	b, c, _, err := g.walk(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if b != 10 || c != 1 {
		t.Errorf("walk = (%d bytes, %d files), want (10,1) — excluded subtree must be skipped", b, c)
	}
	if _, ok := cache.Get(excl); ok {
		t.Error("excluded path should not be cached")
	}
}

func TestDirSizeCachePrune(t *testing.T) {
	c := newTestCache(t.TempDir())
	c.put("/keep/a", Entry{Bytes: 1})
	c.put("/var/lib/rancher/k3s", Entry{Bytes: 2})
	c.put("/var/lib/rancher/k3s/agent/x", Entry{Bytes: 3})

	if n := c.prune([]string{"/var/lib/rancher/k3s"}); n != 2 {
		t.Errorf("prune removed %d, want 2", n)
	}
	if _, ok := c.Get("/keep/a"); !ok {
		t.Error("/keep/a should remain")
	}
	if _, ok := c.Get("/var/lib/rancher/k3s"); ok {
		t.Error("k3s root should be pruned")
	}
	if _, ok := c.Get("/var/lib/rancher/k3s/agent/x"); ok {
		t.Error("k3s child should be pruned")
	}
	if n := c.prune(nil); n != 0 {
		t.Errorf("prune(nil) = %d, want 0", n)
	}
}
