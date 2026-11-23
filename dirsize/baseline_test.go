package dirsize

// Baseline harness for the directory-size gatherer, committed BEFORE the hoist into
// agent-kit-go so the same assertions and benchmark run on both sides of it. The tree
// builder, oracle and digest live in dirsize/dirsizetest, shared with every consumer's
// wiring test.
//
//   - TestDirSizeWalkMatchesIndependentOracle: every cached directory equals an
//     independent tally (differential, so a walk that visits less cannot pass), and the
//     digest of the sorted (relative path, bytes, files) tuples equals a pinned golden.
//   - BenchmarkDirSizeWalk: walk time, allocations (-benchmem) over a fixed synthetic
//     tree; BASELINE_TREE_* override the shape and location so the same binary can be
//     pinned on a Pi under taskset.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/dirsize/dirsizetest"
)

func baselineConfig(dir string) Options {
	c := testSizeConfig(dir)
	c.KeepDepth = 1 << 20 // keep every directory so the whole tally is observable
	return c
}

func cacheRows(tb testing.TB, cache *Cache, root string) []dirsizetest.Row {
	tb.Helper()
	return dirsizetest.RowsFrom(tb, root, func(add func(string, int64, int64)) {
		cache.Range(func(p string, e Entry) { add(p, e.Bytes, e.FileCount) })
	})
}

// baselineGolden pins the digest of the small shape below. It must not change in the
// hoist: the walker's arithmetic is the gate.
const baselineGolden = "31ca124a90e9c27cf5f530ade7ad8c00a2c10b21f33a9a871e97adf6a32727ba"

var smallShape = dirsizetest.Shape{Fanout: 4, Depth: 3, FilesPerDir: 7}

func TestDirSizeWalkMatchesIndependentOracle(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "tree")
	dirsizetest.BuildTree(t, root, smallShape)

	cache := newTestCache(dir)
	g := NewGatherer(baselineConfig(dir), cache)
	if _, _, _, err := g.walk(context.Background(), root, 0); err != nil {
		t.Fatal(err)
	}

	want := dirsizetest.OracleTally(t, root, testExcludeBasenames)
	got := cacheRows(t, cache, root)
	if len(got) != len(want) {
		t.Fatalf("cached %d dirs, oracle has %d", len(got), len(want))
	}
	for _, r := range got {
		w, ok := want[r.Rel]
		if !ok || w.Bytes != r.Bytes || w.Files != r.Files {
			t.Fatalf("%q = (%d B, %d files), oracle (%d, %d) present=%v", r.Rel, r.Bytes, r.Files, w.Bytes, w.Files, ok)
		}
	}
	// The pushed form reads Size() from the same cache; pin it for every directory.
	for _, r := range got {
		sz, ok := cache.Size(filepath.Join(root, r.Rel))
		if !ok || sz != r.Bytes {
			t.Fatalf("Size(%q) = %d,%v want %d", r.Rel, sz, ok, r.Bytes)
		}
	}
	if d := dirsizetest.DigestRows(got); d != baselineGolden {
		t.Fatalf("digest changed: %s (golden %s)", d, baselineGolden)
	}
}

func BenchmarkDirSizeWalk(b *testing.B) {
	benchRoot, benchWant := dirsizetest.EnsureBenchTree(b, testExcludeBasenames)
	cfgDir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	var dirs int
	for i := 0; i < b.N; i++ {
		cache := newTestCache(cfgDir)
		g := NewGatherer(baselineConfig(cfgDir), cache)
		if _, _, _, err := g.walk(context.Background(), benchRoot, 0); err != nil {
			b.Fatal(err)
		}
		dirs = cache.Len()
	}
	b.StopTimer()
	// Falsifiable control: a walk that visits less cannot report the oracle's digest.
	cache := newTestCache(cfgDir)
	g := NewGatherer(baselineConfig(cfgDir), cache)
	if _, _, _, err := g.walk(context.Background(), benchRoot, 0); err != nil {
		b.Fatal(err)
	}
	if d := dirsizetest.DigestRows(cacheRows(b, cache, benchRoot)); d != benchWant {
		b.Fatalf("walk digest %s != oracle digest %s", d, benchWant)
	}
	b.ReportMetric(float64(dirs), "dirs")
}
