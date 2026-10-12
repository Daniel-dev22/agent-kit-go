// Package memlimit derives the Go runtime soft memory limit from the
// container's cgroup memory limit, so the garbage collector works harder
// before the kernel OOM-killer steps in.
//
// A Go process that is not told about its cgroup limit sizes its heap as if
// the whole host were available; a hard cgroup limit then kills it instead of
// making the GC collect more aggressively. Calling Apply at startup, before
// any server or worker starts, sets debug.SetMemoryLimit to a fraction
// (85%) of the cgroup limit, leaving headroom for non-heap memory.
//
// Usage in an agent's main:
//
//	logger := logging.Setup(os.Getenv("LOG_LEVEL"))
//	if _, err := memlimit.Apply(logger); err != nil {
//	    logger.Warn("memlimit: could not apply", "error", err)
//	}
//
// Rules:
//   - cgroup v2 memory.max is read first, then cgroup v1 memory.limit_in_bytes.
//   - "max" (v2) or a "huge" value (v1) means no limit: nothing is set and one
//     warning is logged.
//   - If GOMEMLIMIT is already set in the environment it wins and nothing is
//     changed; the Go runtime has already applied it.
package memlimit

import (
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
)

const (
	// Ratio is the fraction of the cgroup limit given to the Go runtime.
	Ratio = 0.85

	// DefaultRoot is where the cgroup filesystem is mounted in a container.
	DefaultRoot = "/sys/fs/cgroup"

	// v1Unlimited is the threshold at or above which a cgroup v1
	// memory.limit_in_bytes value means "no limit". The kernel reports
	// 9223372036854771712 (MaxInt64 rounded down to a page) when unset.
	v1Unlimited = uint64(1) << 62
)

// Source says where the limit came from.
type Source string

const (
	SourceNone     Source = ""
	SourceCgroupV2 Source = "cgroup-v2"
	SourceCgroupV1 Source = "cgroup-v1"
	SourceEnv      Source = "GOMEMLIMIT"
)

// Result describes what Apply did.
type Result struct {
	// Applied is true only if debug.SetMemoryLimit was called.
	Applied bool
	// Source is the origin of CgroupBytes, or SourceEnv when GOMEMLIMIT was
	// already set, or SourceNone when no limit was found.
	Source Source
	// CgroupBytes is the detected cgroup limit; 0 if none.
	CgroupBytes int64
	// LimitBytes is the Go soft limit that was set; 0 if Applied is false.
	LimitBytes int64
}

// Options is the injection seam. The zero value uses the real cgroup mount,
// the real environment and debug.SetMemoryLimit.
type Options struct {
	// Root is the cgroup filesystem root. Defaults to DefaultRoot.
	Root string
	// Getenv looks up an environment variable. Defaults to os.Getenv.
	Getenv func(string) string
	// SetLimit applies the soft limit. Defaults to debug.SetMemoryLimit.
	SetLimit func(int64) int64
}

// Apply reads the cgroup limit and sets the Go soft memory limit.
// A nil logger uses slog.Default().
func Apply(log *slog.Logger) (Result, error) {
	return ApplyWith(log, Options{})
}

// ApplyWith is Apply with an injectable cgroup root, environment and setter.
func ApplyWith(log *slog.Logger, opts Options) (Result, error) {
	if log == nil {
		log = slog.Default()
	}
	if opts.Root == "" {
		opts.Root = DefaultRoot
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

	max, src, err := ReadCgroupLimit(opts.Root)
	if err != nil {
		return Result{}, err
	}
	if src == SourceNone {
		log.Warn("memlimit: no cgroup memory limit found, Go memory limit not set")
		return Result{}, nil
	}

	limit := int64(float64(max) * Ratio)
	if limit < 1 {
		return Result{Source: src, CgroupBytes: max}, fmt.Errorf("memlimit: cgroup limit %d too small to derive a Go limit", max)
	}
	opts.SetLimit(limit)
	log.Info("memlimit: Go soft memory limit set from cgroup",
		"source", string(src), "cgroup_bytes", max, "go_limit_bytes", limit, "ratio", Ratio)
	return Result{Applied: true, Source: src, CgroupBytes: max, LimitBytes: limit}, nil
}

// ReadCgroupLimit returns the cgroup memory limit in bytes under root, trying
// v2 (memory.max) and then v1 (memory/memory.limit_in_bytes). SourceNone with
// a nil error means the files are absent or report no limit. An error means a
// file was present but could not be read or parsed.
func ReadCgroupLimit(root string) (int64, Source, error) {
	v2 := filepath.Join(root, "memory.max")
	switch raw, err := os.ReadFile(v2); {
	case err == nil:
		s := strings.TrimSpace(string(raw))
		if s == "max" {
			return 0, SourceNone, nil
		}
		n, err := parseBytes(s)
		if err != nil {
			return 0, SourceNone, fmt.Errorf("memlimit: parse %s: %w", v2, err)
		}
		return n, SourceCgroupV2, nil
	case !errors.Is(err, fs.ErrNotExist):
		return 0, SourceNone, fmt.Errorf("memlimit: read %s: %w", v2, err)
	}

	v1 := filepath.Join(root, "memory", "memory.limit_in_bytes")
	raw, err := os.ReadFile(v1)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, SourceNone, nil
	}
	if err != nil {
		return 0, SourceNone, fmt.Errorf("memlimit: read %s: %w", v1, err)
	}
	s := strings.TrimSpace(string(raw))
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, SourceNone, fmt.Errorf("memlimit: parse %s: %w", v1, err)
	}
	if u >= v1Unlimited || u == 0 {
		return 0, SourceNone, nil
	}
	return int64(u), SourceCgroupV1, nil
}

func parseBytes(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if n <= 0 || n == math.MaxInt64 {
		return 0, fmt.Errorf("invalid limit %d", n)
	}
	return n, nil
}
