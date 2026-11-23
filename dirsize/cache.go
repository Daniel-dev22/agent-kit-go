package dirsize

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Entry is one cached directory total. Keyed by absolute path. Bytes is the sum
// of regular-file apparent sizes in the subtree under the walker's exclusions,
// so it is apparent size, not raw on-disk (block-rounded) usage.
//
// FIELD NAMES ARE ONE BYTE ON PURPOSE and the timestamp is unix seconds, not
// RFC3339. Measured on kd-nas01: 1,248,152 entries occupied 309,170,812 bytes —
// 236 B/entry, of which the value was ~110 B and `"computed_at":"2026-08-13T21:
// 26:11.006582794-04:00"` alone was ~48 B. Second precision is ample for a cache
// whose shortest cadence is six hours.
//
// The format is versioned VIA THE FILENAME the caller chooses (duplicacy uses
// dir_sizes.v2.json), not by trusting the decoder to reject the old shape. It does
// not reject it: encoding/json ignores unknown fields and leaves missing ones
// zero, so every legacy entry would load as {Bytes:0, ComputedAtS:0} — and Size()
// would then answer (0, true), painting a confident "0 B" on every directory
// instead of omitting the size. Unknown must not render as zero.
//
// Incomplete is the one field added after v2 shipped, and it is additive on
// purpose: omitempty means an entry that is complete serialises byte-for-byte as
// before, and an older file loads with Incomplete=false, which is exactly what it
// meant then. No filename bump, so no host re-walks from cold.
type Entry struct {
	Bytes       int64 `json:"b"`
	FileCount   int64 `json:"f"`
	ComputedAtS int64 `json:"t"`           // unix seconds
	LastWalkMS  int64 `json:"w,omitempty"` // wall-clock of the last recompute → slow-walk demotion
	// Incomplete is true when Bytes/FileCount are a LOWER BOUND: some part of the
	// subtree could not be read (open or readdir failed, a file's stat failed with a
	// dead-mount errno, or a child subtree was itself incomplete). A confident
	// undercount is still wrong, so a consumer shows "at least".
	Incomplete bool `json:"i,omitempty"`
}

func (d Entry) computedAt() time.Time { return time.Unix(d.ComputedAtS, 0) }

// -----------------------------------------------------------------------------
// Cache — concurrent map + atomic disk persistence
// -----------------------------------------------------------------------------

type Cache struct {
	path       string
	legacyPath string

	mu sync.RWMutex
	m  map[string]Entry

	persistMu sync.Mutex // serialises the tmp+rename pair, like repoMappingStore
}

// NewCache returns an empty cache persisted at path. legacyPath, when non-empty,
// names a superseded file that Load deletes on first load (duplicacy's v1
// dir_sizes.json reached 309,170,812 bytes on kd-nas01 and nothing reads it again).
// Both are supplied by the caller: the kit owns no file names.
func NewCache(path, legacyPath string) *Cache {
	return &Cache{
		path:       path,
		legacyPath: legacyPath,
		m:          map[string]Entry{},
	}
}

// Size returns the cached subtree bytes for a path. The bool distinguishes
// "not computed yet" from a genuine zero so the push can omit the field.
func (c *Cache) Size(path string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[path]
	return e.Bytes, ok
}

// Get returns the full cached entry for a path (bytes, file count, incomplete flag).
func (c *Cache) Get(path string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[path]
	return e, ok
}

// Put stores an entry. The gatherer fills the cache itself; Put exists so a consumer
// test can seed one without running a walk.
func (c *Cache) Put(path string, e Entry) {
	c.mu.Lock()
	c.m[path] = e
	c.mu.Unlock()
}

// drop removes an entry. Needed because an unworthy directory may already be in
// the cache from a file written before the worthiness filter existed, or from
// before it shrank below the threshold. Without this the old bloat would be
// reloaded and re-serialised forever, and the cache would never actually shrink.
func (c *Cache) drop(path string) {
	c.mu.Lock()
	delete(c.m, path)
	c.mu.Unlock()
}

func (c *Cache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// enforceCap is the hard backstop required by the bounded-growth rule: the
// worthiness filter in walk() bounds the cache in practice, but nothing stops a
// filesystem from having a million directories that each clear the threshold.
//
// Eviction keeps the entries with the highest FileCount, because that is exactly
// the value an entry carries: reusing a cached total skips descending that
// subtree, and the work skipped is proportional to what is under it. Evicting the
// smallest first therefore discards the cheapest-to-recompute entries.
//
// Returns the number dropped. NEVER silent — the caller logs it, because a cap
// that hides work reads as "that is all of them".
func (c *Cache) enforceCap(max int) int {
	if max <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) <= max {
		return 0
	}
	type kv struct {
		k string
		n int64
	}
	all := make([]kv, 0, len(c.m))
	for k, e := range c.m {
		all = append(all, kv{k, e.FileCount})
	}
	// Partial order is enough: we only need the boundary between keep and drop.
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })
	dropped := 0
	for _, e := range all[max:] {
		delete(c.m, e.k)
		dropped++
	}
	return dropped
}

// prune removes cache entries at or under any of the given path prefixes.
// Returns the count removed. Used to drop size-excluded mounts left over in the
// cache from before they were excluded (e.g. /var/lib/rancher/k3s).
func (c *Cache) prune(excludes []string) int {
	if len(excludes) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for p := range c.m {
		if PathUnderAny(p, excludes) {
			delete(c.m, p)
			n++
		}
	}
	return n
}

// load reads the on-disk cache. Missing/empty/corrupt file is non-fatal — we
// just start cold and the gatherer refills it (a corrupt cache is never worse
// than no cache here, unlike repos.json which is authoritative).
func (c *Cache) load() {
	// Reclaim the v1 file. It is never read again, and on kd-nas01 it was 295 MB
	// sitting in the agent's state dir (and therefore in the backup).
	if c.legacyPath != "" {
		if st, err := os.Stat(c.legacyPath); err == nil {
			if err := os.Remove(c.legacyPath); err != nil {
				slog.Warn("could not remove legacy dir size cache", "path", c.legacyPath, "error", err)
			} else {
				slog.Info("removed legacy dir size cache (v1 format, superseded)",
					"path", c.legacyPath, "bytes", st.Size())
			}
		}
	}

	data, err := os.ReadFile(c.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("dir size cache unreadable, starting cold", "error", err, "path", c.path)
		}
		return
	}
	if len(data) == 0 {
		return
	}
	var m map[string]Entry
	if err := json.Unmarshal(data, &m); err != nil {
		slog.Warn("dir size cache corrupt, starting cold", "error", err, "path", c.path)
		return
	}
	c.mu.Lock()
	c.m = m
	c.mu.Unlock()
}

// save writes the cache atomically (.tmp + rename), STREAMING the encode.
//
// The previous implementation did json.Marshal(c.m) into one []byte and then
// os.WriteFile. On kd-nas01 that allocated a 295 MB contiguous buffer on top of
// the map itself, every save, on a host where the agent is capped well below
// that. Encoding straight into a buffered file writer keeps peak allocation at
// the buffer size regardless of cache size.
//
// The map is snapshotted under RLock into a slice of key pointers first: holding
// the read lock across a multi-hundred-megabyte write would block every walk()
// put for the duration.
func (c *Cache) save() error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()

	c.mu.RLock()
	keys := make([]string, 0, len(c.m))
	for k := range c.m {
		keys = append(keys, k)
	}
	snapshot := make([]Entry, len(keys))
	for i, k := range keys {
		snapshot[i] = c.m[k]
	}
	c.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(c.path), 0700); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(c.path), err)
	}
	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("open %s: %w", tmp, err)
	}
	w := bufio.NewWriterSize(f, 256*1024)
	enc := json.NewEncoder(w)

	writeErr := func() error {
		if _, err := w.WriteString("{"); err != nil {
			return err
		}
		for i, k := range keys {
			if i > 0 {
				if _, err := w.WriteString(","); err != nil {
					return err
				}
			}
			// Encode the key as a JSON string so paths with quotes/backslashes
			// round-trip; Encoder appends a newline, which json tolerates but we
			// do not want mid-object, so marshal the key separately.
			kb, err := json.Marshal(k)
			if err != nil {
				return err
			}
			if _, err := w.Write(kb); err != nil {
				return err
			}
			if _, err := w.WriteString(":"); err != nil {
				return err
			}
			if err := enc.Encode(snapshot[i]); err != nil {
				return err
			}
		}
		_, err := w.WriteString("}")
		return err
	}()
	if writeErr == nil {
		writeErr = w.Flush()
	}
	if cerr := f.Close(); writeErr == nil {
		writeErr = cerr
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, writeErr)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", c.path, err)
	}
	return nil
}
