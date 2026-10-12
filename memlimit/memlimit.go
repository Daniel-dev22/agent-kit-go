// Package memlimit derives the Go runtime soft memory limit from the
// process's own cgroup memory limit, so the garbage collector works harder
// before the kernel OOM-killer steps in.
//
// A Go process that is not told about its cgroup limit sizes its heap as if
// the whole host were available; a hard cgroup limit then kills it instead of
// making the GC collect more aggressively. Calling Apply at startup, before
// any server or worker starts, sets debug.SetMemoryLimit below the cgroup
// limit, leaving headroom for memory the Go runtime does not account for.
//
// Usage in an agent's main:
//
//	logger := logging.Setup(os.Getenv("LOG_LEVEL"))
//	if _, err := memlimit.Apply(ctx, logger); err != nil {
//	    logger.Warn("memlimit: could not apply", "error", err)
//	}
//
// Which cgroup: the process's own, resolved through /proc/self/cgroup (cgroup
// v2 "0::<path>", or the v1 "memory" controller line). The tightest limit on
// the path from that cgroup up to the root wins, so a limit on an ancestor
// (a systemd unit's MemoryMax, a parent slice) is honoured. Only if
// /proc/self/cgroup cannot be read does it fall back to the cgroup root
// itself, which is the container's own cgroup under a private cgroup
// namespace.
//
// Soft limit: by default 85% of the cgroup limit. Child processes started by
// the agent live in the same cgroup but are invisible to the Go runtime's
// accounting, so set a reserve for them: MEMLIMIT_RESERVE_BYTES (or
// Options.ReserveBytes) makes the soft limit limit-reserve, never below 50%
// of the limit.
//
// Other rules:
//   - "max" (v2) or a "huge" value (v1) at every level means no limit: nothing
//     is set and one warning is logged.
//   - If GOMEMLIMIT is already set in the environment it wins and nothing is
//     changed; Apply starts no refresher either.
//   - The limit is re-read every Interval (default 60s) until ctx is done; a
//     change (e.g. an operator raising or lowering the container limit)
//     re-applies SetMemoryLimit and logs once per change.
package memlimit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	// Ratio is the default fraction of the cgroup limit given to the Go runtime.
	Ratio = 0.85

	// MinFraction is the floor for the soft limit when a reserve is set.
	MinFraction = 0.5

	// ReserveEnv names the environment variable holding the reserve in bytes.
	ReserveEnv = "MEMLIMIT_RESERVE_BYTES"

	// DefaultRoot is where the cgroup filesystem is mounted in a container.
	DefaultRoot = "/sys/fs/cgroup"

	// DefaultSelfCgroup is the file naming the process's own cgroup.
	DefaultSelfCgroup = "/proc/self/cgroup"

	// DefaultInterval is how often the limit is re-read.
	DefaultInterval = 60 * time.Second

	// v1Unlimited is the threshold at or above which a cgroup v1
	// memory.limit_in_bytes value means "no limit". The kernel reports
	// 9223372036854771712 (MaxInt64 rounded down to a page) when unset.
	v1Unlimited = uint64(1) << 62
)

// ErrInvalidConfig marks a configuration the caller got wrong: a malformed
// MEMLIMIT_RESERVE_BYTES or Options.ReserveBytes, or a cgroup limit too small
// to derive a Go limit from. Test with errors.Is. Callers must treat it as
// fatal at startup: the process would otherwise run with no soft limit under a
// hard cgroup cap. Other errors (an unreadable or unparsable cgroup file) are
// environmental; Apply still starts the refresher for those, which keeps
// retrying, so they are safe to log and continue.
var ErrInvalidConfig = errors.New("memlimit: invalid configuration")

// Source says where the limit came from.
type Source string

const (
	SourceNone     Source = ""
	SourceCgroupV2 Source = "cgroup-v2"
	SourceCgroupV1 Source = "cgroup-v1"
	SourceEnv      Source = "GOMEMLIMIT"
)

// Result describes what Apply did at startup.
type Result struct {
	// Applied is true only if debug.SetMemoryLimit was called.
	Applied bool
	// Source is the origin of CgroupBytes, or SourceEnv when GOMEMLIMIT was
	// already set, or SourceNone when no limit was found.
	Source Source
	// CgroupBytes is the detected (tightest) cgroup limit; 0 if none.
	CgroupBytes int64
	// ReserveBytes is the reserve in effect; 0 when the default ratio was used.
	ReserveBytes int64
	// LimitBytes is the Go soft limit that was set; 0 if Applied is false.
	LimitBytes int64
}

// Options is the injection seam. The zero value uses the real cgroup mount,
// /proc/self/cgroup, the real environment and debug.SetMemoryLimit.
type Options struct {
	// Root is the cgroup filesystem root. Defaults to DefaultRoot.
	Root string
	// SelfCgroup is the path of the file naming the process's cgroup.
	// Defaults to DefaultSelfCgroup.
	SelfCgroup string
	// Getenv looks up an environment variable. Defaults to os.Getenv.
	Getenv func(string) string
	// SetLimit applies the soft limit. Defaults to debug.SetMemoryLimit.
	SetLimit func(int64) int64
	// ReserveBytes overrides MEMLIMIT_RESERVE_BYTES when > 0.
	ReserveBytes int64
	// Interval is the re-read period; 0 means DefaultInterval, negative
	// disables the refresher.
	Interval time.Duration
	// Tick, if non-nil, replaces the interval timer (tests).
	Tick <-chan time.Time
}

// Apply reads the cgroup limit, sets the Go soft memory limit and, until ctx
// is done, keeps it in step with the cgroup. A nil logger uses slog.Default().
//
// An error satisfying errors.Is(err, ErrInvalidConfig) must be treated as
// fatal by the caller: nothing was applied and no refresher runs. Any other
// error is environmental; the refresher is running and will retry.
func Apply(ctx context.Context, log *slog.Logger) (Result, error) {
	return ApplyWith(ctx, log, Options{})
}

// ApplyWith is Apply with an injectable cgroup root, environment and setter.
func ApplyWith(ctx context.Context, log *slog.Logger, opts Options) (Result, error) {
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
	if opts.Getenv == nil {
		opts.Getenv = os.Getenv
	}
	if opts.SetLimit == nil {
		opts.SetLimit = debug.SetMemoryLimit
	}

	if env := strings.TrimSpace(opts.Getenv("GOMEMLIMIT")); env != "" {
		log.Info("memlimit: GOMEMLIMIT set in environment, leaving it unchanged", "GOMEMLIMIT", env)
		return Result{Source: SourceEnv}, nil
	}

	reserve, err := reserveBytes(opts)
	if err != nil {
		return Result{}, err
	}

	res, rerr := resolve(opts, reserve)
	if rerr != nil && errors.Is(rerr, ErrInvalidConfig) {
		return Result{}, rerr
	}
	switch {
	case rerr != nil:
		// Environmental (unreadable/unparsable cgroup file): no limit yet; the
		// refresher below keeps retrying and logs the failure once.
		res = Result{}
		log.Warn("memlimit: could not read the cgroup limit, Go memory limit not set yet", "error", rerr)
	case res.Source == SourceNone:
		log.Warn("memlimit: no cgroup memory limit found, Go memory limit not set")
	default:
		opts.SetLimit(res.LimitBytes)
		res.Applied = true
		logApplied(log, "memlimit: Go soft memory limit set from cgroup", res)
	}

	if opts.Interval >= 0 {
		go refresh(ctx, log, opts, reserve, res, rerr)
	}
	return res, rerr
}

func logApplied(log *slog.Logger, msg string, r Result) {
	log.Info(msg, "source", string(r.Source), "cgroup_bytes", r.CgroupBytes,
		"reserve_bytes", r.ReserveBytes, "go_limit_bytes", r.LimitBytes)
}

// resolve reads the cgroup limit and derives the soft limit.
func resolve(opts Options, reserve int64) (Result, error) {
	max, src, err := ReadSelfLimit(opts.Root, opts.SelfCgroup)
	if err != nil {
		return Result{}, err
	}
	if src == SourceNone {
		return Result{}, nil
	}
	limit := SoftLimit(max, reserve)
	if limit < 1 {
		return Result{}, fmt.Errorf("%w: cgroup limit %d too small to derive a Go limit", ErrInvalidConfig, max)
	}
	r := Result{Source: src, CgroupBytes: max, LimitBytes: limit}
	if reserve > 0 {
		r.ReserveBytes = reserve
	}
	return r, nil
}

// SoftLimit returns the Go soft limit for a cgroup limit: max-reserve floored
// at MinFraction of max when reserve > 0, otherwise Ratio of max.
func SoftLimit(max, reserve int64) int64 {
	if reserve <= 0 {
		return int64(float64(max) * Ratio)
	}
	floor := int64(float64(max) * MinFraction)
	if limit := max - reserve; limit > floor {
		return limit
	}
	return floor
}

func reserveBytes(opts Options) (int64, error) {
	if opts.ReserveBytes > 0 {
		return opts.ReserveBytes, nil
	}
	s := strings.TrimSpace(opts.Getenv(ReserveEnv))
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %s %q: want a positive integer byte count", ErrInvalidConfig, ReserveEnv, s)
	}
	return n, nil
}

// refresh re-reads the limit until ctx is done and re-applies on change.
// Failures and "no limit" are logged once per change of state, not per tick.
func refresh(ctx context.Context, log *slog.Logger, opts Options, reserve int64, last Result, startErr error) {
	tick := opts.Tick
	if tick == nil {
		iv := opts.Interval
		if iv == 0 {
			iv = DefaultInterval
		}
		t := time.NewTicker(iv)
		defer t.Stop()
		tick = t.C
	}
	lastErr := ""
	if startErr != nil {
		lastErr = startErr.Error() // already logged by Apply
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}
		if ctx.Err() != nil { // a ready tick must not outrank cancellation
			return
		}
		cur, err := resolve(opts, reserve)
		if err != nil {
			if err.Error() != lastErr {
				lastErr = err.Error()
				log.Warn("memlimit: re-read failed, keeping previous limit", "error", err)
			}
			continue
		}
		lastErr = ""
		if cur.LimitBytes == last.LimitBytes && cur.Source == last.Source {
			continue
		}
		if cur.Source == SourceNone {
			opts.SetLimit(math.MaxInt64)
			log.Warn("memlimit: cgroup memory limit removed, Go memory limit cleared")
		} else {
			opts.SetLimit(cur.LimitBytes)
			logApplied(log, "memlimit: cgroup memory limit changed, Go soft memory limit updated", cur)
		}
		last = cur
	}
}

// ReadSelfLimit returns the tightest memory limit in bytes that applies to the
// process's own cgroup. selfCgroup is the path of a /proc/self/cgroup-format
// file; root is the cgroup filesystem mount. SourceNone with a nil error means
// no limit anywhere on the path. If selfCgroup cannot be read it falls back
// to the limit files directly under root.
func ReadSelfLimit(root, selfCgroup string) (int64, Source, error) {
	v2Path, haveV2, v1Path, haveV1, ok := parseSelfCgroup(selfCgroup)
	if !ok {
		return ReadCgroupLimit(root)
	}

	if haveV2 {
		min, found, err := walkMin(root, v2Path, "memory.max", parseV2)
		if err != nil {
			return 0, SourceNone, err
		}
		if found {
			return min, SourceCgroupV2, nil
		}
	}
	if haveV1 {
		min, found, err := walkMin(filepath.Join(root, "memory"), v1Path, "memory.limit_in_bytes", parseV1)
		if err != nil {
			return 0, SourceNone, err
		}
		if found {
			return min, SourceCgroupV1, nil
		}
	}
	if !haveV2 && !haveV1 {
		return ReadCgroupLimit(root)
	}
	return 0, SourceNone, nil
}

// parseSelfCgroup reads a /proc/self/cgroup-format file. ok is false when the
// file cannot be read.
func parseSelfCgroup(selfCgroup string) (v2Path string, haveV2 bool, v1Path string, haveV1 bool, ok bool) {
	f, err := os.Open(selfCgroup)
	if err != nil {
		return "", false, "", false, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.SplitN(sc.Text(), ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			v2Path, haveV2 = parts[2], true
		case hasController(parts[1], "memory"):
			v1Path, haveV1 = parts[2], true
		}
	}
	if sc.Err() != nil {
		return "", false, "", false, false
	}
	return v2Path, haveV2, v1Path, haveV1, true
}

func hasController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name || c == "name="+name {
			return true
		}
	}
	return false
}

// limitParser returns (limit, bounded, error); bounded=false means "no limit here".
type limitParser func(string) (int64, bool, error)

// walkMin reads file in the cgroup dir root+cgPath and in every ancestor up to
// root, returning the smallest bounded value.
func walkMin(root, cgPath, file string, parse limitParser) (int64, bool, error) {
	dir := filepath.Join(root, filepath.Clean("/"+cgPath))
	var min int64
	found := false
	for {
		p := filepath.Join(dir, file)
		raw, err := os.ReadFile(p)
		switch {
		case err == nil:
			n, bounded, perr := parse(strings.TrimSpace(string(raw)))
			if perr != nil {
				return 0, false, fmt.Errorf("memlimit: parse %s: %w", p, perr)
			}
			if bounded && (!found || n < min) {
				min, found = n, true
			}
		case !errors.Is(err, fs.ErrNotExist):
			return 0, false, fmt.Errorf("memlimit: read %s: %w", p, err)
		}
		if dir == root || len(dir) <= len(root) {
			return min, found, nil
		}
		dir = filepath.Dir(dir)
	}
}

func parseV2(s string) (int64, bool, error) {
	if s == "max" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, err
	}
	if n <= 0 || n == math.MaxInt64 {
		return 0, false, fmt.Errorf("invalid limit %d", n)
	}
	return n, true, nil
}

func parseV1(s string) (int64, bool, error) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, err
	}
	if u >= v1Unlimited {
		return 0, false, nil
	}
	if u == 0 {
		// Not a real limit: some runtimes report 0 for "unset".
		return 0, false, nil
	}
	return int64(u), true, nil
}

// ReadCgroupLimit reads the limit files directly under root: v2 memory.max,
// then v1 memory/memory.limit_in_bytes. It is the fallback when
// /proc/self/cgroup is unreadable. SourceNone with a nil error means the
// files are absent or report no limit.
func ReadCgroupLimit(root string) (int64, Source, error) {
	if n, found, err := walkMin(root, "/", "memory.max", parseV2); err != nil {
		return 0, SourceNone, err
	} else if found {
		return n, SourceCgroupV2, nil
	}
	// A present v2 file reporting "max" is authoritative: do not fall through.
	if _, err := os.Stat(filepath.Join(root, "memory.max")); err == nil {
		return 0, SourceNone, nil
	}
	if n, found, err := walkMin(filepath.Join(root, "memory"), "/", "memory.limit_in_bytes", parseV1); err != nil {
		return 0, SourceNone, err
	} else if found {
		return n, SourceCgroupV1, nil
	}
	return 0, SourceNone, nil
}
