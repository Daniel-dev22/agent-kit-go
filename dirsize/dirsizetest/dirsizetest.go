// Package dirsizetest is the shared test support for the directory-size gatherer: a
// deterministic tree builder, an independent filepath-style oracle, and the digest that
// pins both. It deliberately imports nothing from dirsize, so dirsize's own internal
// tests and every consumer's wiring test can use the same code without an import cycle.
//
// ComputedAtS and LastWalkMS are never part of a Row: they change on every run, so
// hashing them would make a golden digest meaningless.
package dirsizetest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Shape is a deterministic tree: Fanout^level directories per level down to Depth,
// FilesPerDir regular files each, plus one node_modules directory under the root and
// one symlink per directory that a walker must never count.
type Shape struct {
	Fanout, Depth, FilesPerDir int
}

// BuildTree writes the tree under root. Sizes depend only on position, never on the run.
func BuildTree(tb testing.TB, root string, s Shape) {
	tb.Helper()
	var build func(dir string, level int)
	build = func(dir string, level int) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			tb.Fatal(err)
		}
		for i := 0; i < s.FilesPerDir; i++ {
			n := 1 + (level*131+i*17)%997
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d", i)), make([]byte, n), 0o600); err != nil {
				tb.Fatal(err)
			}
		}
		if err := os.Symlink("f000", filepath.Join(dir, "link")); err != nil {
			tb.Fatal(err)
		}
		if level == 0 {
			nm := filepath.Join(dir, "node_modules")
			if err := os.MkdirAll(nm, 0o700); err != nil {
				tb.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(nm, "junk"), make([]byte, 4096), 0o600); err != nil {
				tb.Fatal(err)
			}
		}
		if level == s.Depth {
			return
		}
		for c := 0; c < s.Fanout; c++ {
			build(filepath.Join(dir, fmt.Sprintf("d%02d", c)), level+1)
		}
	}
	build(root, 0)
}

// Row is one directory's recursive totals, keyed by path relative to the tree root.
type Row struct {
	Rel          string
	Bytes, Files int64
}

// OracleTally computes per-directory recursive totals with plain os.ReadDir recursion
// and the given exclusion rules (names to skip, symlinks never counted), independent
// of the walker under test.
func OracleTally(tb testing.TB, root string, excludeBasenames map[string]struct{}) map[string]Row {
	tb.Helper()
	rows := map[string]Row{}
	var rec func(dir string) (int64, int64)
	rec = func(dir string) (int64, int64) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			tb.Fatal(err)
		}
		var b, f int64
		for _, e := range ents {
			if _, skip := excludeBasenames[e.Name()]; skip {
				continue
			}
			if e.Type()&fs.ModeSymlink != 0 {
				continue
			}
			p := filepath.Join(dir, e.Name())
			if e.IsDir() {
				cb, cf := rec(p)
				b += cb
				f += cf
				continue
			}
			info, err := e.Info()
			if err != nil {
				tb.Fatal(err)
			}
			if info.Mode().IsRegular() {
				b += info.Size()
				f++
			}
		}
		rel, _ := filepath.Rel(root, dir)
		rows[rel] = Row{Rel: rel, Bytes: b, Files: f}
		return b, f
	}
	rec(root)
	return rows
}

// RowsFromOracle flattens an OracleTally result.
func RowsFromOracle(m map[string]Row) []Row {
	out := make([]Row, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	return out
}

// DigestRows hashes sorted (relative path, bytes, files) tuples. No timestamps.
func DigestRows(rows []Row) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Rel < rows[j].Rel })
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%s\x00%d\x00%d\n", r.Rel, r.Bytes, r.Files)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RowsFrom turns a cache walk into Rows relative to root. visit is handed an add
// function and calls it once per cached directory; a key outside root fails the test.
func RowsFrom(tb testing.TB, root string, visit func(add func(path string, bytes, files int64))) []Row {
	tb.Helper()
	var rows []Row
	visit(func(p string, bytes, files int64) {
		rel, err := filepath.Rel(root, p)
		if err != nil || strings.HasPrefix(rel, "..") {
			tb.Fatalf("cache key %q outside root %q", p, root)
		}
		rows = append(rows, Row{Rel: rel, Bytes: bytes, Files: files})
	})
	return rows
}

// BenchShape is the benchmark tree. BASELINE_TREE_FANOUT/DEPTH/FILES override the shape
// so one binary can be pinned on a Pi under taskset.
func BenchShape() Shape {
	atoi := func(k string, d int) int {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
			return v
		}
		return d
	}
	return Shape{
		Fanout:      atoi("BASELINE_TREE_FANOUT", 8),
		Depth:       atoi("BASELINE_TREE_DEPTH", 4),
		FilesPerDir: atoi("BASELINE_TREE_FILES", 12),
	}
}

// EnsureBenchTree returns the benchmark tree's root with the oracle digest every walker
// must reproduce. It builds under BASELINE_TREE_DIR when set (so a Pi run can reuse a
// tree across runs), otherwise under b.TempDir().
//
// It deliberately keeps NO process-wide state: the testing package runs a benchmark
// function several times (N=1, then the timed N, then once per -count) and removes
// b.TempDir() after each, so a cached path would point at a deleted tree on the second
// call and the timed loop would measure a no-op. Call it before b.ResetTimer.
func EnsureBenchTree(b *testing.B, excludeBasenames map[string]struct{}) (root, wantDigest string) {
	b.Helper()
	base := os.Getenv("BASELINE_TREE_DIR")
	if base == "" {
		base = b.TempDir()
	}
	root = filepath.Join(base, "tree")
	if _, err := os.Stat(root); err != nil {
		BuildTree(b, root, BenchShape())
	}
	return root, DigestRows(RowsFromOracle(OracleTally(b, root, excludeBasenames)))
}
