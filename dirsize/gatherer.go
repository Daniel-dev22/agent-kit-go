package dirsize

// Per-directory recursive size gathering for the filter file picker.
//
// Design (fully decoupled from the 5-min tree push in trees.go):
//
//   - Gatherer is its own goroutine — a SCHEDULER THAT IDLES, not a
//     perpetual walker. Each cycle it walks the due backup roots, sleeps until
//     the next root is due, and repeats. If a pass finishes in 20 min and the
//     cadence is 6h, it idles ~5h40m before the next pass.
//   - A consumer's tree push only ever READS this cache (Cache.Size): it attaches a
//     size to directory nodes that already have a value and omits it otherwise. The
//     push never computes, enqueues, or blocks on the gatherer.
//
// Cadence (the "4×/day, large 1×/day" rule):
//   - A directory refreshes every SmallRefresh (~6h → 4×/day).
//   - A directory whose subtree file count exceeds LargeFileThreshold,
//     or whose last walk took longer than SlowWalk, drops to
//     LargeRefresh (~24h → 1×/day).
//
// Efficiency (cost of a pass scales with what's actually due, not the tree):
//   - Bottom-up single pass per root: one traversal accumulates every dir's
//     recursive Bytes/FileCount as the recursion unwinds.
//   - Reuse cached totals for subtrees that aren't due — a 6h parent pass folds
//     in a giant 24h-cadence child's cached total instead of re-descending it.
//   - Allocation-free per-file stat: os.Open + ReadDir + unix.Fstatat into a
//     stack Stat_t; file paths are never materialised, only directory paths
//     (the cache keys). See sizeOfRegularFile.
//
// Persistence: the cache is written atomically to the path the consumer passes to
// NewCache (the kit owns no file names) so a restart resumes the cadence instead of
// re-walking every root from scratch.
// First run: an empty cache means the push simply reports no sizes until the
// gatherer fills them — never a synchronous walk storm at startup.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// -----------------------------------------------------------------------------
// Gatherer — the idle-between-cycles scheduler
// -----------------------------------------------------------------------------

type Gatherer struct {
	opts  Options
	cache *Cache

	// lastAttempt records when each root was last walked (success OR timeout),
	// so a root that can't finish within WalkTimeout still backs off to
	// its cadence between passes instead of hot-looping. In-memory only — on
	// restart we simply re-attempt, which is correct.
	lastAttempt map[string]time.Time
	firstPass   bool

	// passMu makes RunPass single-flight: Start's loop and a consumer's own call
	// (a wiring test, an on-demand refresh) serialise instead of racing on the
	// per-pass state below (lastAttempt, guard, anchors, stepN).
	passMu sync.Mutex

	// guard classifies foreign mounts (pseudo filesystems, mounted disk images,
	// orphaned dm targets) so the walk never descends into one. Rebuilt at the
	// start of every pass because the mount table changes between passes — that is
	// the whole point, an image build can leak a mount at any time. Nil is safe and
	// means "no mount is skipped"; see RunPass for why that degradation is correct.
	guard *MountGuard
	// lastGuardSig is the previous pass's skip set, so the summary logs on change
	// rather than every pass.
	lastGuardSig string
	// stepN counts directories since the last throttle pause. Single-goroutine —
	// the gatherer's loop is the only writer.
	stepN int
	// anchors is the set of paths a tree push can be rooted at; see rebuildAnchors.
	// Refreshed each pass because repos are registered and removed at runtime.
	anchors map[string]struct{}
}

func NewGatherer(opts Options, cache *Cache) *Gatherer {
	return &Gatherer{
		opts:        opts,
		cache:       cache,
		lastAttempt: map[string]time.Time{},
		firstPass:   true,
	}
}

// roots returns the deduplicated set of paths to size. Roots cover both
// the node-tree picker and (via the mirrored mount layout) every repo whose
// source lives under a backup root. Sizes are keyed by absolute path, so a
// repo's sub-directories populated during a backup-root walk are readable by
// the repo-tree push regardless of their depth.
func (g *Gatherer) roots() []string {
	seen := map[string]struct{}{}
	raw := g.opts.roots()
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		r = filepath.Clean(r)
		if _, ok := seen[r]; ok {
			continue
		}
		if PathUnderAny(r, g.excludePaths()) {
			continue // size-excluded (env var, or a controller-pushed filter rule)
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// excludePaths is the single exclusion list the gatherer honours: the two env-var
// lists plus the absolute exclude rules from the controller-pushed filter sets.
//
// Merging them here rather than at each call site is the point. There were three
// separate exclusion checks in walk() and two more in roots()/Start(), all reading
// only the env vars — which is how a rule the operator set in the UI ended up
// applying to backups but not to this walker.
func (g *Gatherer) excludePaths() []string {
	return g.opts.excludePaths()
}

// isDeadMountErr reports whether err means the backing store is gone rather than
// the file being absent — a detached loop device, a dead NFS/CIFS server. These are
// leaks worth naming: ENOENT is routine, EIO is not.
func isDeadMountErr(err error) bool {
	return errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.ESTALE) ||
		errors.Is(err, unix.ENOTCONN) ||
		errors.Is(err, unix.ENODEV)
}

// cadence returns the refresh interval for a cached entry: the daily interval
// when the subtree is large (by file count) or was slow to walk, else 4×/day.
func (g *Gatherer) cadence(e Entry) time.Duration {
	if e.FileCount > g.opts.LargeFileThreshold ||
		time.Duration(e.LastWalkMS)*time.Millisecond > g.opts.SlowWalk {
		return g.opts.LargeRefresh
	}
	return g.opts.SmallRefresh
}

// due reports whether a cached directory should be recomputed now.
func (g *Gatherer) due(path string, now time.Time) bool {
	e, ok := g.cache.Get(path)
	if !ok {
		return true // never computed
	}
	return now.Sub(e.computedAt()) >= g.cadence(e)
}

func (g *Gatherer) Start(ctx context.Context) {
	if !g.opts.Enabled {
		slog.Info("dir size gatherer disabled")
		return
	}
	g.cache.load()
	// Drop any cached entries for now-excluded paths (e.g. k3s sized before the
	// exclude was added) so the picker stops showing their stale sizes.
	//
	// This now also covers the controller-pushed rules, which makes it self-healing:
	// the 142,066 phantom keys under the leaked image chroot on ng-pi were already
	// covered by an org exclude, so a boot after this change drops them without an
	// operator deleting dir_sizes.json by hand.
	excludes := g.excludePaths()
	if pruned := g.cache.prune(excludes); pruned > 0 {
		slog.Info("dir size cache: pruned size-excluded entries", "count", pruned, "excludes", excludes)
		if err := g.cache.save(); err != nil {
			slog.Warn("dir size cache save after prune failed", "error", err)
		}
	}
	go g.loop(ctx)
}

func (g *Gatherer) loop(ctx context.Context) {
	slog.Info("dir size gatherer started",
		"roots", g.roots(),
		"exclude_paths", g.excludePaths(),
		"small_refresh", g.opts.SmallRefresh,
		"large_refresh", g.opts.LargeRefresh,
		"large_file_threshold", g.opts.LargeFileThreshold)

	for {
		g.RunPass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-g.opts.Stop:
			return
		case <-time.After(g.untilNextDue(time.Now())):
		}
	}
}

// untilNextDue returns how long to sleep before the earliest root is due again.
// Clamped so we never hot-loop (min 1m) nor oversleep past the small cadence.
func (g *Gatherer) untilNextDue(now time.Time) time.Duration {
	const minSleep = time.Minute
	next := now.Add(g.opts.SmallRefresh)
	for _, root := range g.roots() {
		at, attempted := g.lastAttempt[root]
		if !attempted {
			return minSleep // due now
		}
		cad := g.opts.SmallRefresh
		if e, ok := g.cache.Get(root); ok {
			cad = g.cadence(e)
		}
		if d := at.Add(cad); d.Before(next) {
			next = d
		}
	}
	d := time.Until(next)
	if d < minSleep {
		return minSleep
	}
	return d
}

// persist enforces the hard entry cap and writes the cache, logging any eviction.
func (g *Gatherer) persist() {
	if dropped := g.cache.enforceCap(g.opts.MaxEntries); dropped > 0 {
		// Never silent: state what was discarded and what remains.
		slog.Warn("dir size cache hit its entry cap, evicted smallest subtrees",
			"dropped", dropped, "kept", g.cache.Len(), "cap", g.opts.MaxEntries)
	}
	if err := g.cache.save(); err != nil {
		slog.Warn("dir size cache save failed", "error", err)
	}
}

// RunPass walks each due root once, bottom-up, refreshing due directories and
// reusing cached totals for not-due subtrees, then persists the cache once. Start's
// loop calls it on the cadence; it is exported so a consumer can drive exactly one
// synchronous pass (a wiring test, a benchmark) without timing or polling.
//
// RunPass alone never loads the persisted file or prunes now-excluded keys: that
// boot work belongs to Start. Concurrent calls are serialised.
func (g *Gatherer) RunPass(ctx context.Context) {
	g.passMu.Lock()
	defer g.passMu.Unlock()
	now := time.Now()
	var dirty bool

	// Rebuild the mount view for this pass. A failure here must NOT stop the pass:
	// sizes are advisory (the tree push omits what it lacks), so degrading to
	// "skip nothing" keeps the picker populated. It is logged at WARN because the
	// walk is then unguarded, which is the state that produced the EXT4 errors.
	guard, err := NewMountGuard()
	if err != nil {
		slog.Warn("dir size gatherer: mount guard unavailable, walking unguarded", "error", err)
	}
	g.guard = guard
	// Repos come and go at runtime, so the anchor set is rebuilt per pass rather
	// than captured once at construction.
	g.rebuildAnchors()
	// Log on the first pass and on every CHANGE thereafter, never every pass: a new
	// leaked image mount must be visible in the agent log the night it appears, but
	// a steady state must not reprint hourly.
	if sig := guard.signature(); g.firstPass || sig != g.lastGuardSig {
		guard.logSummary()
		g.lastGuardSig = sig
	}

	var walked int
	for _, root := range g.roots() {
		select {
		case <-ctx.Done():
			return
		case <-g.opts.Stop:
			return
		default:
		}
		if !g.due(root, now) {
			continue
		}
		walked++
		wctx, cancel := context.WithTimeout(ctx, g.opts.WalkTimeout)
		_, _, _, err := g.walk(wctx, root, 0)
		cancel()
		g.lastAttempt[root] = time.Now()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("dir size walk failed", "root", root, "error", err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("dir size walk hit timeout (will resume next cadence)", "root", root, "timeout", g.opts.WalkTimeout)
		}
		dirty = true
		// Honour shutdown promptly if the app-level ctx was cancelled. Save first —
		// a timed-out or cancelled walk still made real progress, and discarding it
		// would make a large filesystem restart from zero every pass.
		if ctx.Err() != nil {
			g.persist()
			return
		}
	}
	// Save ONCE per pass, not once per root. Each save rewrites the whole file;
	// doing it per root multiplied a multi-hundred-megabyte write by the root count
	// for no benefit, since a pass is not durable until it finishes anyway.
	if dirty {
		g.persist()
	}
	if g.firstPass {
		root, files := g.summary()
		slog.Info("dir size gatherer first pass complete", "roots_walked", walked, "dirs_cached", root, "files_counted", files)
		g.firstPass = false
	}
}

// summary returns (dirs cached, total files counted) for the first-pass log.
func (g *Gatherer) summary() (int, int64) {
	g.cache.mu.RLock()
	defer g.cache.mu.RUnlock()
	var files int64
	for _, e := range g.cache.m {
		files += e.FileCount
	}
	return len(g.cache.m), files
}

// worthCaching decides whether a computed directory total earns a cache slot.
//
// Measured on kd-nas01 (1,248,152 entries, 309,170,812 bytes): 81.1% of entries
// described a subtree of FEWER THAN TEN FILES, and 95.3% fewer than a hundred.
// Those cost ~236 B on disk plus a map slot each, and save almost nothing when
// reused — recomputing one is a single openat+getdents plus a handful of fstatat.
//
// An entry earns its place two ways, and only these two:
//
//   - It is shallow enough that the UI can READ it. annotateSizes only ever looks
//     up nodes inside the pushed tree, which walkDir caps at treeMaxDepth (5). A
//     size cached below that depth is written, reloaded and re-serialised forever
//     without a single consumer.
//   - Its subtree is big enough that reusing it skips real work — either by file
//     count, or because it was slow enough to have earned the daily cadence, which
//     is precisely the signal that re-descending it is expensive.
//
// anchors are the paths a tree push can be ROOTED AT: every backup root and every
// registered repo. Depth for the display test is measured from the nearest
// enclosing anchor, not from the walk root.
//
// This is not a refinement, it is a correctness requirement. trees.go caps a
// pushed tree at treeMaxDepth (5) BELOW WHATEVER IT IS ROOTED AT, and repos are
// not at the top: on kd-nas01 the backup root is /mnt while repos sit at
// /mnt/raid_array/kdhome_backup/duplicacy-relay (depth 4) and deeper. A repo tree
// there spans absolute depths 5..9, so measuring from the backup root and keeping
// six levels would silently drop the sizes for the bottom half of that picker —
// the UI would render directories with no size at all.
func (g *Gatherer) rebuildAnchors() {
	a := make(map[string]struct{}, 8)
	for _, r := range g.roots() {
		a[filepath.Clean(r)] = struct{}{}
	}
	for _, p := range g.opts.anchors() {
		if p != "" {
			a[filepath.Clean(p)] = struct{}{}
		}
	}
	g.anchors = a
}

func (g *Gatherer) isAnchor(dir string) bool {
	_, ok := g.anchors[dir]
	return ok
}

func (g *Gatherer) worthCaching(depth int, e Entry) bool {
	if depth <= g.opts.KeepDepth {
		return true
	}
	if e.FileCount >= g.opts.KeepMinFiles {
		return true
	}
	// Slow-walk demotion must survive, or the dir loses its 24h cadence and gets
	// re-descended four times a day instead of once.
	return time.Duration(e.LastWalkMS)*time.Millisecond > g.opts.SlowWalk
}

// walk returns the recursive (bytes, fileCount, incomplete) for dir, refreshing the cache
// for every directory it recomputes. A not-due cached directory is reused
// without descending. ctx cancellation/timeout propagates up so in-progress
// parents are NOT cached partial — already-cached children remain valid and the
// next pass resumes by reusing them.
//
// depth is measured from the walk root, so the "shallow enough to be displayed"
// test matches what the tree push can actually reach.
func (g *Gatherer) walk(ctx context.Context, dir string, depth int) (int64, int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	// Entering a path a tree can be rooted at restarts the display-depth budget, so
	// every node the picker can show from that root stays cached.
	if g.isAnchor(dir) {
		depth = 0
	}
	now := time.Now()
	if e, ok := g.cache.Get(dir); ok && now.Sub(e.computedAt()) < g.cadence(e) {
		return e.Bytes, e.FileCount, e.Incomplete, nil // reuse, no descent
	}

	t0 := time.Now()
	f, err := os.Open(dir)
	if err != nil {
		// Unreadable (permissions, vanished). Record an empty entry so the
		// scheduler backs off to cadence rather than retrying every minute.
		//
		// EIO/ESTALE/ENOTCONN mean the BACKING STORE is gone, not that a file was
		// removed — a detached loop device, a dead NFS server. That is a leak worth
		// naming: it is how the orphaned kpartx maps announced themselves, and it
		// previously surfaced only as a kernel EXT4 error with no agent-side trace.
		if isDeadMountErr(err) {
			slog.Warn("dir size: unreadable mount, skipping subtree",
				"dir", dir, "error", err)
		}
		// Always cached regardless of worthiness: this entry exists to make the
		// scheduler back off to cadence instead of retrying an unreadable
		// directory every minute, which is a job no recomputation can do.
		//
		// A directory that VANISHED (ENOENT) between the parent listing and this open is
		// routine and its contribution really is zero. Any other failure (permissions, a
		// dead mount) means real content went uncounted, so the zero is a lower bound.
		unreadable := !errors.Is(err, os.ErrNotExist)
		g.cache.Put(dir, Entry{ComputedAtS: now.Unix(), LastWalkMS: time.Since(t0).Milliseconds(), Incomplete: unreadable})
		return 0, 0, unreadable, nil
	}
	entries, readErr := f.ReadDir(-1)
	fd := int(f.Fd())

	var bytes, count int64
	var incomplete bool
	for _, e := range entries {
		name := e.Name()
		if _, skip := g.opts.ExcludeBasenames[name]; skip {
			continue
		}
		if e.Type()&os.ModeSymlink != 0 {
			continue // never follow symlinks (cycles / double-walks), like the tree walk
		}
		child := filepath.Join(dir, name)
		if (g.opts.SkipPath != nil && g.opts.SkipPath(child)) || PathUnderAny(child, g.excludePaths()) {
			continue
		}
		if e.IsDir() {
			// A foreign mount: pseudo filesystem, mounted disk image, or an
			// orphaned dm target whose backing device is gone. Never descend.
			if reason, skip := g.guard.SkipReason(child); skip {
				slog.Debug("dir size: skipping mount", "dir", child, "reason", reason)
				continue
			}
			cb, cc, cinc, cerr := g.walk(ctx, child, depth+1)
			if cerr != nil {
				f.Close()
				return 0, 0, false, cerr // ctx cancelled — don't cache this parent
			}
			bytes += cb
			count += cc
			incomplete = incomplete || cinc
		} else if sz, ok, unreadable := sizeOfRegularFile(fd, name); ok {
			bytes += sz
			count++
		} else if unreadable {
			incomplete = true
		}
	}
	f.Close()

	// Throttle, but amortised over StepEvery directories rather than paused
	// after each one — see the config comment for the 42-minute floor this removes.
	// The ctx check stays unconditional so cancellation is still honoured promptly
	// on the directories that do not sleep.
	if err := ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	if g.opts.StepSleep > 0 {
		g.stepN++
		every := g.opts.StepEvery
		if every < 1 {
			every = 1
		}
		if g.stepN >= every {
			g.stepN = 0
			select {
			case <-ctx.Done():
				return 0, 0, false, ctx.Err()
			case <-time.After(g.opts.StepSleep):
			}
		}
	}

	if readErr != nil {
		// Partial listing — cache what we summed but don't treat as authoritative
		// for long; small cadence will recompute. Still record to enable backoff.
		slog.Debug("dir size: partial readdir", "dir", dir, "error", readErr)
		incomplete = true
	}
	entry := Entry{
		Bytes:       bytes,
		FileCount:   count,
		ComputedAtS: now.Unix(),
		LastWalkMS:  time.Since(t0).Milliseconds(),
		Incomplete:  incomplete,
	}
	// The total is returned to the parent either way; only STORAGE is conditional.
	// Correctness of every reported size is therefore independent of this filter —
	// it trades a cheap recomputation for a cache slot, nothing more.
	if g.worthCaching(depth, entry) {
		g.cache.Put(dir, entry)
	} else {
		g.cache.drop(dir)
	}
	return bytes, count, incomplete, nil
}

// sizeOfRegularFile stats a directory entry relative to its parent fd without
// allocating a fileInfo or materialising the file's path. Returns the apparent
// size and whether the entry is a regular file (symlinks/devices/etc. → false).
// unreadable is true when the stat itself failed for a reason other than the entry
// having vanished (ENOENT is routine); a dead-mount EIO there means a real file
// went uncounted, which marks the directory total as a lower bound.
// unix.Fstatat with AT_SYMLINK_NOFOLLOW stats the entry itself, never a target.
func sizeOfRegularFile(dirFd int, name string) (size int64, regular, unreadable bool) {
	var st unix.Stat_t
	if err := unix.Fstatat(dirFd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0, false, !errors.Is(err, unix.ENOENT)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return 0, false, false
	}
	return st.Size, true, false
}
