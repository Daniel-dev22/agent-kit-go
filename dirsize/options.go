package dirsize

import "time"

// Options configures a Gatherer. Everything that was hard-coded in the duplicacy
// agent's size gatherer is either a plain field here or a hook the caller supplies;
// the kit owns no file names, environment variables or repository concepts.
//
// The hooks are called from the gatherer's single goroutine (and, for ExcludePaths,
// Roots and Anchors, once per pass or per decision), so they must be cheap and safe
// to call repeatedly. A nil hook means "none".
type Options struct {
	// Enabled is the master switch; Start is a no-op (with one log line) when false.
	Enabled bool

	// Cadence. A directory refreshes every SmallRefresh; one whose subtree holds more
	// than LargeFileThreshold files, or whose last walk took longer than SlowWalk,
	// drops to LargeRefresh.
	SmallRefresh       time.Duration
	LargeRefresh       time.Duration
	LargeFileThreshold int64
	SlowWalk           time.Duration

	// WalkTimeout bounds one root's walk; progress made before it is kept and the
	// next cadence resumes by reusing cached children.
	WalkTimeout time.Duration
	// StepSleep is a gentle pause per StepEvery directories, amortised so a large
	// tree is not stretched past WalkTimeout before a byte is read.
	StepSleep time.Duration
	StepEvery int

	// KeepDepth / KeepMinFiles bound the cache: an entry is kept only when it is
	// shallow enough to be displayed or big enough that reusing it skips real work.
	// MaxEntries is the hard backstop (0 = unbounded).
	KeepDepth    int
	KeepMinFiles int64
	MaxEntries   int

	// Roots returns the paths to size. The gatherer cleans, de-duplicates and drops
	// any root under an excluded prefix.
	Roots func() []string
	// ExcludePaths returns the absolute path prefixes the walk must never enter,
	// merged by the caller from every source it honours (env, pushed filter rules).
	ExcludePaths func() []string
	// ExcludeBasenames are directory or file names skipped wherever they appear.
	ExcludeBasenames map[string]struct{}
	// SkipPath, when set, reports whether a child path is skipped. It is the hook for
	// rules that are not a prefix or a basename (duplicacy's web-cache layout).
	SkipPath func(path string) bool
	// Anchors returns extra paths a consumer can root a displayed tree at (for
	// duplicacy, registered repositories). Roots are always anchors; entering an
	// anchor restarts the KeepDepth budget so every displayed node stays cached.
	Anchors func() []string
	// Stop, when non-nil, is a second shutdown signal alongside the Start context.
	Stop <-chan struct{}
}

func (o Options) roots() []string {
	if o.Roots == nil {
		return nil
	}
	return o.Roots()
}

func (o Options) excludePaths() []string {
	if o.ExcludePaths == nil {
		return nil
	}
	return o.ExcludePaths()
}

func (o Options) anchors() []string {
	if o.Anchors == nil {
		return nil
	}
	return o.Anchors()
}
