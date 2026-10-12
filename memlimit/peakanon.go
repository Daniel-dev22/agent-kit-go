package memlimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultAnonInterval is how often PeakAnon samples.
const DefaultAnonInterval = 5 * time.Second

// AnonStats is a point-in-time view of a PeakAnon sampler.
type AnonStats struct {
	// CurrentBytes is the most recent anonymous-memory reading of the
	// process's own cgroup (the whole cgroup: the process plus its children).
	CurrentBytes int64
	// MaxBytes is the largest CurrentBytes seen since the sampler started.
	MaxBytes int64
	// Samples is the number of successful readings.
	Samples int64
}

// PeakOptions is the injection seam for StartPeakAnon.
type PeakOptions struct {
	// Root is the cgroup filesystem root. Defaults to DefaultRoot.
	Root string
	// SelfCgroup is the /proc/self/cgroup-format file. Defaults to DefaultSelfCgroup.
	SelfCgroup string
	// Interval is the sampling period; 0 means DefaultAnonInterval.
	Interval time.Duration
	// Tick, if non-nil, replaces the interval timer (tests).
	Tick <-chan time.Time
}

// PeakAnon samples the anonymous memory of the process's own cgroup and
// remembers the maximum seen since start. Unlike memory.peak it excludes page
// cache and reclaimable slab, so it is the number to size a hard memory cap
// from. The cgroup is resolved exactly as Apply resolves it.
type PeakAnon struct {
	cur, max, samples atomic.Int64
}

// StartPeakAnon takes one sample immediately, then one per interval until ctx
// is done. It never returns nil. A failing read keeps the previous values and
// is logged once per distinct error. A nil logger uses slog.Default().
func StartPeakAnon(ctx context.Context, log *slog.Logger, opts PeakOptions) *PeakAnon {
	if log == nil {
		log = slog.Default()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Root == "" {
		opts.Root = DefaultRoot
	}
	if opts.SelfCgroup == "" {
		opts.SelfCgroup = DefaultSelfCgroup
	}
	p := &PeakAnon{}
	lastErr := p.sample(log, opts, "")
	go p.run(ctx, log, opts, lastErr)
	return p
}

// Stats returns the current and maximum readings. Safe for concurrent use,
// e.g. from a health endpoint handler.
func (p *PeakAnon) Stats() AnonStats {
	return AnonStats{CurrentBytes: p.cur.Load(), MaxBytes: p.max.Load(), Samples: p.samples.Load()}
}

// Current is the latest reading in bytes (0 before the first successful one).
func (p *PeakAnon) Current() int64 { return p.cur.Load() }

// Max is the largest reading since start in bytes.
func (p *PeakAnon) Max() int64 { return p.max.Load() }

func (p *PeakAnon) run(ctx context.Context, log *slog.Logger, opts PeakOptions, lastErr string) {
	tick := opts.Tick
	if tick == nil {
		iv := opts.Interval
		if iv <= 0 {
			iv = DefaultAnonInterval
		}
		t := time.NewTicker(iv)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}
		if ctx.Err() != nil {
			return
		}
		lastErr = p.sample(log, opts, lastErr)
	}
}

// sample reads once and returns the error text to remember ("" on success).
func (p *PeakAnon) sample(log *slog.Logger, opts PeakOptions, lastErr string) string {
	n, err := ReadSelfAnon(opts.Root, opts.SelfCgroup)
	if err != nil {
		if err.Error() != lastErr {
			log.Warn("memlimit: anon sample failed, keeping previous values", "error", err)
		}
		return err.Error()
	}
	if lastErr != "" {
		log.Info("memlimit: anon sampling recovered")
	}
	p.cur.Store(n)
	p.samples.Add(1)
	for {
		m := p.max.Load()
		if n <= m || p.max.CompareAndSwap(m, n) {
			break
		}
	}
	return ""
}

// ReadSelfAnon returns the anonymous memory in bytes of the process's own
// cgroup: the "anon" field of cgroup v2 memory.stat, or "total_rss" of cgroup
// v1 memory.stat. If selfCgroup cannot be read it uses the files directly under
// root.
func ReadSelfAnon(root, selfCgroup string) (int64, error) {
	v2Path, haveV2, v1Path, haveV1, ok := parseSelfCgroup(selfCgroup)
	type cand struct {
		file, key string
	}
	var cands []cand
	switch {
	case !ok:
		cands = []cand{{filepath.Join(root, "memory.stat"), "anon"}, {filepath.Join(root, "memory", "memory.stat"), "total_rss"}}
	default:
		if haveV2 {
			cands = append(cands, cand{filepath.Join(root, filepath.Clean("/"+v2Path), "memory.stat"), "anon"})
		}
		if haveV1 {
			cands = append(cands, cand{filepath.Join(root, "memory", filepath.Clean("/"+v1Path), "memory.stat"), "total_rss"})
		}
	}
	for _, c := range cands {
		n, err := readStatKey(c.file, c.key)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		return n, err
	}
	return 0, fmt.Errorf("memlimit: no memory.stat found for the process's cgroup under %s", root)
}

func readStatKey(path, key string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, found := strings.Cut(sc.Text(), " ")
		if !found || k != key {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("memlimit: parse %s %s: %q", path, key, v)
		}
		return n, nil
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("memlimit: read %s: %w", path, err)
	}
	return 0, fmt.Errorf("memlimit: %s has no %q field", path, key)
}
