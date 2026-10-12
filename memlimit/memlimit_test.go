package memlimit

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

type env map[string]string

func (e env) get(k string) string { return e[k] }

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	// Atomic replace: the refresher reads concurrently and a truncate-then-write
	// would show it a torn (empty) file, which the kernel's files never do.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

const (
	mib = int64(1 << 20)
	gib = int64(1 << 30)
)

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// rig is one isolated cgroup tree plus recorders.
type rig struct {
	t    *testing.T
	root string
	self string
	env  env
	logs *syncBuf
	mu   sync.Mutex
	set  []int64
	sets chan int64
}

func newRig(t *testing.T) *rig {
	t.Helper()
	d := t.TempDir()
	return &rig{t: t, root: filepath.Join(d, "cg"), self: filepath.Join(d, "self-cgroup"),
		env: env{}, logs: &syncBuf{}, sets: make(chan int64, 16)}
}

func (r *rig) opts() Options {
	return Options{
		Root: r.root, SelfCgroup: r.self, Getenv: r.env.get, Interval: -1,
		SetLimit: func(n int64) int64 {
			r.mu.Lock()
			r.set = append(r.set, n)
			r.mu.Unlock()
			select {
			case r.sets <- n:
			default:
			}
			return 0
		},
	}
}

func (r *rig) apply(o Options) (Result, error) {
	return ApplyWith(context.Background(), slog.New(slog.NewTextHandler(r.logs, nil)), o)
}

func (r *rig) calls() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.set...)
}

func (r *rig) cg(rel, file, content string) { write(r.t, filepath.Join(r.root, rel, file), content) }

func TestV2OwnCgroupNumeric(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1073741824\n")
	res, err := r.apply(r.opts())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.Source != SourceCgroupV2 || res.LimitBytes != 912680550 || res.CgroupBytes != 1073741824 {
		t.Fatalf("result %+v", res)
	}
	if c := r.calls(); len(c) != 1 || c[0] != 912680550 {
		t.Fatalf("set %v", c)
	}
	if strings.Count(r.logs.String(), "\n") != 1 {
		t.Fatalf("want exactly one log line, got %q", r.logs.String())
	}
}

func TestV2NestedCgroupAndTightestAncestorWins(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/system.slice/unit.service\n")
	r.cg("system.slice/unit.service", "memory.max", "max\n") // own: unlimited
	r.cg("system.slice", "memory.max", "2147483648\n")       // parent: tight
	r.cg("", "memory.max", "8589934592\n")                   // looser, must not win
	res, err := r.apply(r.opts())
	if err != nil || res.CgroupBytes != 2147483648 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestV2OwnLimitTighterThanAncestor(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/a/b\n")
	r.cg("a/b", "memory.max", "1000000000")
	r.cg("a", "memory.max", "3000000000")
	res, _ := r.apply(r.opts())
	if res.CgroupBytes != 1000000000 {
		t.Fatalf("res %+v", res)
	}
}

func TestV2NoMemoryMaxAtRootStillFindsAncestorLimit(t *testing.T) {
	// The cgroupns=host case: root has no memory.max, the limit is on the unit.
	r := newRig(t)
	write(t, r.self, "0::/docker/abc\n")
	r.cg("docker/abc", "memory.max", "536870912")
	res, err := r.apply(r.opts())
	if err != nil || !res.Applied || res.CgroupBytes != 536870912 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestV2MaxEverywhere(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/a\n")
	r.cg("a", "memory.max", "max\n")
	r.cg("", "memory.max", "max\n")
	res, err := r.apply(r.opts())
	if err != nil || res.Applied || len(r.calls()) != 0 {
		t.Fatalf("res %+v err %v set %v", res, err, r.calls())
	}
	if l := r.logs.String(); strings.Count(l, "level=WARN") != 1 || strings.Count(l, "\n") != 1 {
		t.Fatalf("want one warning, got %q", l)
	}
}

func TestV1MemoryControllerLine(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "12:cpu,cpuacct:/x\n4:memory:/docker/abc\n1:name=systemd:/x\n")
	write(t, filepath.Join(r.root, "memory", "docker", "abc", "memory.limit_in_bytes"), "536870912\n")
	write(t, filepath.Join(r.root, "memory", "memory.limit_in_bytes"), "9223372036854771712\n")
	res, err := r.apply(r.opts())
	if err != nil || !res.Applied || res.Source != SourceCgroupV1 || res.LimitBytes != 456340275 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestV1AncestorTighter(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "4:cpu,memory:/p/c\n")
	write(t, filepath.Join(r.root, "memory", "p", "c", "memory.limit_in_bytes"), "9223372036854771712")
	write(t, filepath.Join(r.root, "memory", "p", "memory.limit_in_bytes"), "268435456")
	res, _ := r.apply(r.opts())
	if res.CgroupBytes != 268435456 || res.Source != SourceCgroupV1 {
		t.Fatalf("res %+v", res)
	}
}

func TestV1Huge(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "4:memory:/\n")
	write(t, filepath.Join(r.root, "memory", "memory.limit_in_bytes"), "9223372036854771712\n")
	res, err := r.apply(r.opts())
	if err != nil || res.Applied || len(r.calls()) != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	if strings.Count(r.logs.String(), "level=WARN") != 1 {
		t.Fatalf("want one warning, got %q", r.logs.String())
	}
}

func TestHybridPrefersWhicheverHasALimit(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "4:memory:/c\n0::/c\n")
	write(t, filepath.Join(r.root, "memory", "c", "memory.limit_in_bytes"), "134217728")
	res, err := r.apply(r.opts())
	if err != nil || res.Source != SourceCgroupV1 || res.CgroupBytes != 134217728 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestSelfCgroupUnreadableFallsBackToRoot(t *testing.T) {
	r := newRig(t) // r.self never written
	r.cg("", "memory.max", "1073741824")
	res, err := r.apply(r.opts())
	if err != nil || !res.Applied || res.Source != SourceCgroupV2 {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func TestSelfCgroupUnreadableRootV1AndV2MaxAuthoritative(t *testing.T) {
	r := newRig(t)
	r.cg("", "memory.max", "max")
	write(t, filepath.Join(r.root, "memory", "memory.limit_in_bytes"), "536870912")
	res, _ := r.apply(r.opts())
	if res.Applied {
		t.Fatalf("v2 'max' must win over v1: %+v", res)
	}
	r2 := newRig(t)
	write(t, filepath.Join(r2.root, "memory", "memory.limit_in_bytes"), "536870912")
	res, _ = r2.apply(r2.opts())
	if !res.Applied || res.Source != SourceCgroupV1 {
		t.Fatalf("res %+v", res)
	}
}

func TestMissingFiles(t *testing.T) {
	r := newRig(t)
	res, err := r.apply(r.opts())
	if err != nil || res.Applied || len(r.calls()) != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	if strings.Count(r.logs.String(), "level=WARN") != 1 {
		t.Fatalf("want one warning, got %q", r.logs.String())
	}
}

func TestGOMEMLIMITRespectedAndNoRefresher(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1073741824")
	r.env["GOMEMLIMIT"] = "512MiB"
	tick := make(chan time.Time)
	o := r.opts()
	o.Interval, o.Tick = 0, tick
	res, err := r.apply(o)
	if err != nil || res.Applied || res.Source != SourceEnv || len(r.calls()) != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	select {
	case tick <- time.Now():
		t.Fatal("refresher must not run when GOMEMLIMIT is set")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestInvalidContents(t *testing.T) {
	for name, tc := range map[string]struct{ self, file, content string }{
		"v2 garbage":  {"0::/\n", "memory.max", "lots"},
		"v2 negative": {"0::/\n", "memory.max", "-5"},
		"v2 zero":     {"0::/\n", "memory.max", "0"},
		"v2 empty":    {"0::/\n", "memory.max", ""},
		"v1 garbage":  {"4:memory:/\n", filepath.Join("memory", "memory.limit_in_bytes"), "x12"},
		"v1 negative": {"4:memory:/\n", filepath.Join("memory", "memory.limit_in_bytes"), "-1"},
		"fallback v2": {"", "memory.max", "lots"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			if tc.self != "" {
				write(t, r.self, tc.self)
			}
			r.cg("", tc.file, tc.content)
			res, err := r.apply(r.opts())
			if err == nil || res.Applied || len(r.calls()) != 0 {
				t.Fatalf("want error and no set: res %+v err %v set %v", res, err, r.calls())
			}
		})
	}
}

func TestUnreadablePathIsError(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	if err := os.MkdirAll(filepath.Join(r.root, "memory.max"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := r.apply(r.opts()); err == nil || len(r.calls()) != 0 {
		t.Fatalf("err %v set %v", err, r.calls())
	}
}

func TestPathTraversalInSelfCgroupStaysUnderRoot(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/../../escape\n")
	write(t, filepath.Join(filepath.Dir(r.root), "memory.max"), "1024") // outside root
	res, err := r.apply(r.opts())
	if err != nil || res.Applied {
		t.Fatalf("must not read outside root: res %+v err %v", res, err)
	}
}

func TestReserve(t *testing.T) {
	const max = int64(4 << 30)
	cases := []struct {
		name    string
		env     string
		opt     int64
		want    int64
		wantErr bool
	}{
		{"env reserve", "1610612736", 0, max - 1610612736, false},
		{"option reserve", "", 1 << 30, max - 1<<30, false},
		{"option beats env", "1610612736", 1 << 30, max - 1<<30, false},
		{"reserve too big floors at 50%", "", 3 << 30, max / 2, false},
		{"reserve equals max floors at 50%", "", max, max / 2, false},
		{"no reserve is 85%", "", 0, 3650722201, false},
		{"env garbage", "lots", 0, 0, true},
		{"env negative", "-1", 0, 0, true},
		{"env zero", "0", 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			write(t, r.self, "0::/\n")
			r.cg("", "memory.max", "4294967296")
			if tc.env != "" {
				r.env[ReserveEnv] = tc.env
			}
			o := r.opts()
			o.ReserveBytes = tc.opt
			res, err := r.apply(o)
			if tc.wantErr {
				if err == nil || len(r.calls()) != 0 {
					t.Fatalf("want error, got res %+v err %v", res, err)
				}
				return
			}
			if err != nil || res.LimitBytes != tc.want {
				t.Fatalf("res %+v err %v want %d", res, err, tc.want)
			}
			if c := r.calls(); len(c) != 1 || c[0] != tc.want {
				t.Fatalf("set %v", c)
			}
		})
	}
}

func TestRefresherReappliesOnChangeOnlyAndLogsOncePerChange(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1073741824")
	tick := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := r.opts()
	o.Interval, o.Tick = 0, tick
	if _, err := ApplyWith(ctx, slog.New(slog.NewTextHandler(r.logs, nil)), o); err != nil {
		t.Fatal(err)
	}
	<-r.sets // initial

	tick <- time.Now() // unchanged
	tick <- time.Now() // guarantees the previous tick was fully processed
	if c := r.calls(); len(c) != 1 {
		t.Fatalf("unchanged limit must not re-apply: %v", c)
	}

	r.cg("", "memory.max", "536870912")
	tick <- time.Now()
	if got := <-r.sets; got != 456340275 {
		t.Fatalf("got %d", got)
	}
	tick <- time.Now()
	tick <- time.Now()
	if c := r.calls(); len(c) != 2 {
		t.Fatalf("change must apply exactly once: %v", c)
	}
	if n := strings.Count(r.logs.String(), "limit changed"); n != 1 {
		t.Fatalf("want one change log, got %d: %q", n, r.logs.String())
	}

	r.cg("", "memory.max", "max")
	tick <- time.Now()
	if got := <-r.sets; got != math.MaxInt64 {
		t.Fatalf("limit removal must clear, got %d", got)
	}

	r.cg("", "memory.max", "garbage")
	tick <- time.Now()
	tick <- time.Now()
	tick <- time.Now()
	if n := strings.Count(r.logs.String(), "re-read failed"); n != 1 {
		t.Fatalf("a persistent read error must log once, got %d: %s", n, r.logs.String())
	}
	if c := r.calls(); c[len(c)-1] != math.MaxInt64 {
		t.Fatalf("a failed re-read must keep the previous limit: %v", c)
	}
}

func TestRefresherStopsWithContext(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1073741824")
	tick := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	o := r.opts()
	o.Interval, o.Tick = 0, tick
	if _, err := ApplyWith(ctx, slog.New(slog.NewTextHandler(r.logs, nil)), o); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case tick <- time.Now():
		// A tick racing the cancel may still be taken once; the next must not be.
		select {
		case tick <- time.Now():
			t.Fatal("refresher still running after ctx cancel")
		case <-time.After(100 * time.Millisecond):
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestApplyDefaultsDoNotPanic(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	res, err := Apply(context.Background(), nil)
	if err != nil || res.Source != SourceEnv {
		t.Fatalf("res %+v err %v", res, err)
	}
}

// TestAST_RefresherHonoursContext pins the structural properties tests cannot
// observe from outside: the refresher selects on ctx.Done(), and Apply's
// SetLimit is only reachable after the GOMEMLIMIT check.
func TestAST_RefresherHonoursContext(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "memlimit.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sawDone bool
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "refresh" {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Done" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "ctx" {
					sawDone = true
				}
			}
			return true
		})
	}
	if !sawDone {
		t.Fatal("refresh must select on ctx.Done()")
	}
}

func TestRatioIsExactly85Percent(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1048576000\n") // 1000 MiB
	res, err := r.apply(r.opts())
	if err != nil || res.LimitBytes != 850*mib {
		t.Fatalf("res %+v err %v, want exactly %d (850 MiB)", res, err, 850*mib)
	}
	if c := r.calls(); len(c) != 1 || c[0] != 891289600 {
		t.Fatalf("set %v, want [891289600]", c)
	}
}

func TestSoftLimitLiterals(t *testing.T) {
	for _, tc := range []struct{ max, reserve, want int64 }{
		{1000 * mib, 0, 850 * mib},
		{4 * gib, 1536 * mib, 4*gib - 1536*mib},
		{4 * gib, 3 * gib, 2 * gib}, // floored at exactly 50%
		{4 * gib, 4 * gib, 2 * gib},
		{4 * gib, 2 * gib, 2 * gib}, // reserve exactly at the floor
		{4 * gib, 2*gib - 1, 2*gib + 1},
	} {
		if got := SoftLimit(tc.max, tc.reserve); got != tc.want {
			t.Errorf("SoftLimit(%d, %d) = %d, want %d", tc.max, tc.reserve, got, tc.want)
		}
	}
}

// TestRealSetMemoryLimitEffect uses the real debug.SetMemoryLimit (no seam)
// and reads the runtime's limit back.
func TestRealSetMemoryLimitEffect(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1048576000")
	o := r.opts()
	o.SetLimit = nil // use the real default
	if _, err := r.apply(o); err != nil {
		t.Fatal(err)
	}
	if got := debug.SetMemoryLimit(-1); got != 850*mib {
		t.Fatalf("runtime limit = %d, want %d", got, 850*mib)
	}
}

// TestRealRefresherClearsRuntimeLimit: the refresher's removal path also goes
// through the real setter.
func TestRealRefresherUpdatesRuntimeLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	r := newRig(t)
	write(t, r.self, "0::/\n")
	r.cg("", "memory.max", "1048576000")
	tick := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o := r.opts()
	o.SetLimit, o.Interval, o.Tick = nil, 0, tick
	if _, err := ApplyWith(ctx, slog.New(slog.NewTextHandler(r.logs, nil)), o); err != nil {
		t.Fatal(err)
	}
	r.cg("", "memory.max", "2097152000") // 2000 MiB
	tick <- time.Now()
	tick <- time.Now() // previous tick fully processed
	if got := debug.SetMemoryLimit(-1); got != 1700*mib {
		t.Fatalf("runtime limit = %d, want %d", got, 1700*mib)
	}
}

func TestV1ZeroIsNoLimit(t *testing.T) {
	r := newRig(t)
	write(t, r.self, "4:memory:/\n")
	write(t, filepath.Join(r.root, "memory", "memory.limit_in_bytes"), "0\n")
	res, err := r.apply(r.opts())
	if err != nil || res.Applied || len(r.calls()) != 0 {
		t.Fatalf("res %+v err %v", res, err)
	}
	if strings.Count(r.logs.String(), "level=WARN") != 1 {
		t.Fatalf("want one no-limit warning, got %q", r.logs.String())
	}
}
