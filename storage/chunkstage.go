package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ChunkStage stages a single content-addressed file as a set of fixed-size
// chunks written out of order and resumably. It is the disk half of a
// mesh/peer transfer: a peer pulls the chunks it is missing and pwrites each at
// its byte offset; a bitmap sidecar records which chunks have landed so a
// restart mid-transfer resumes instead of re-pulling everything.
//
// Layout under dir (the sha256hex is the content address):
//
//	<sha256hex>.part          the assembled file, pre-sized to `size`
//	<sha256hex>.part.bitmap   JSON sidecar: {sha256,size,chunk_size,received}
//
// Durability contract: WriteChunk fsyncs the data BEFORE marking the bitmap, so
// a crash can only ever leave a landed-but-unmarked chunk (re-pulled on resume,
// harmless) — never a marked-but-absent chunk. Complete re-hashes the whole
// assembled file and only renames it into place if the full sha256 matches, so
// a torn or reordered write can never be published.
//
// A ChunkStage is safe for concurrent WriteChunk/Bitmap/Missing calls.
type ChunkStage struct {
	partPath   string
	bitmapPath string
	sha256hex  string
	size       int64
	chunkSize  int64
	nchunks    int

	mu       sync.Mutex
	f        *os.File
	received []bool
}

// stageSidecar is the on-disk bitmap record. The metadata lets OpenStage
// detect a chunk-geometry change and start fresh rather than trust stale marks.
type stageSidecar struct {
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	ChunkSize int64  `json:"chunk_size"`
	Received  []bool `json:"received"`
}

// OpenStage creates or reopens the stage for content sha256hex of the given
// size, chunked at chunkSize bytes. The .part file is created (or reopened) and
// pre-sized to size; the bitmap sidecar is loaded when its geometry matches,
// else initialized empty. Reopening a partially-filled stage returns it with
// the prior Bitmap()/Missing() intact so a transfer resumes.
func OpenStage(dir, sha256hex string, size, chunkSize int64) (*ChunkStage, error) {
	if sha256hex == "" {
		return nil, errors.New("storage: OpenStage requires sha256hex")
	}
	if size < 0 {
		return nil, fmt.Errorf("storage: OpenStage invalid size %d", size)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("storage: OpenStage invalid chunkSize %d", chunkSize)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir stage dir: %w", err)
	}

	nchunks := 0
	if size > 0 {
		nchunks = int((size + chunkSize - 1) / chunkSize)
	}

	s := &ChunkStage{
		partPath:   filepath.Join(dir, sha256hex+".part"),
		bitmapPath: filepath.Join(dir, sha256hex+".part.bitmap"),
		sha256hex:  sha256hex,
		size:       size,
		chunkSize:  chunkSize,
		nchunks:    nchunks,
		received:   make([]bool, nchunks),
	}

	f, err := os.OpenFile(s.partPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open .part: %w", err)
	}
	// Pre-size so pwrite offsets are always valid and the trailing (unwritten)
	// region is well-defined. Truncate to exactly size preserves any bytes
	// already landed within [0,size) on a resume.
	if err := f.Truncate(size); err != nil {
		f.Close()
		return nil, fmt.Errorf("size .part: %w", err)
	}
	// fsync the parent dir ONCE so the .part directory entry is durable from the
	// start (a crash can't lose the .part while a later bitmap rename survives).
	// Done here, not per-chunk, to avoid a per-chunk parent-fsync perf cost.
	syncParentDir(s.partPath)
	s.f = f

	// Load the bitmap sidecar; only trust it when its geometry matches, so a
	// changed chunkSize (or a corrupt sidecar) safely restarts from empty —
	// Complete's full-file hash is the ultimate correctness gate regardless.
	if data, rErr := os.ReadFile(s.bitmapPath); rErr == nil {
		var sc stageSidecar
		if json.Unmarshal(data, &sc) == nil &&
			sc.SHA256 == sha256hex && sc.Size == size && sc.ChunkSize == chunkSize &&
			len(sc.Received) == nchunks {
			s.received = sc.Received
		}
	}
	return s, nil
}

// WriteChunk pwrites the bytes read from r at index*chunkSize and marks the
// chunk received. r is bounded to the chunk's true length (chunkSize, or the
// short final chunk), so an over-long reader cannot bleed into the next chunk.
// Writing an already-received chunk is allowed and idempotent (it re-lands the
// bytes and re-marks). The data is fsynced before the bitmap is persisted.
//
// ctx cancels the (potentially slow, network-backed) read+write promptly. The
// stage mutex is held only to validate/snapshot geometry and to persist the mark
// — NOT across the blocking copy+fsync — so a stalled peer on one chunk never
// wedges Bitmap()/Missing()/other WriteChunk calls. pwrite at distinct offsets
// is safe without the lock; the durability contract (fsync-before-mark) is
// preserved because the fsync completes before the re-lock that marks.
func (s *ChunkStage) WriteChunk(ctx context.Context, index int, r io.Reader) error {
	if r == nil {
		return errors.New("storage: WriteChunk nil reader")
	}
	s.mu.Lock()
	if s.f == nil {
		s.mu.Unlock()
		return errors.New("storage: WriteChunk on closed stage")
	}
	if index < 0 || index >= s.nchunks {
		s.mu.Unlock()
		return fmt.Errorf("storage: chunk index %d out of range [0,%d)", index, s.nchunks)
	}
	f := s.f
	offset := int64(index) * s.chunkSize
	limit := s.chunkSize
	if rem := s.size - offset; rem < limit {
		limit = rem
	}
	s.mu.Unlock() // release across the blocking network read + fsync

	ow := io.NewOffsetWriter(f, offset)
	bufPtr := bufPool.Get().(*[]byte)
	n, err := io.CopyBuffer(ow, io.LimitReader(&ctxReader{ctx: ctx, r: r}, limit), *bufPtr)
	bufPool.Put(bufPtr)
	if err != nil {
		return fmt.Errorf("write chunk %d: %w", index, err)
	}
	if n != limit {
		return fmt.Errorf("chunk %d: wrote %d bytes, want %d", index, n, limit)
	}
	// fsync data BEFORE marking so a crash never marks an absent chunk.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync chunk %d: %w", index, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		// Stage was Closed/Completed concurrently; don't mark a torn transfer.
		return errors.New("storage: stage closed during WriteChunk")
	}
	s.received[index] = true
	if err := s.persistBitmapLocked(); err != nil {
		// The mark is durable-in-memory but not on disk; roll it back so
		// Missing() stays truthful and a resume re-pulls this chunk.
		s.received[index] = false
		return fmt.Errorf("persist bitmap: %w", err)
	}
	return nil
}

func (s *ChunkStage) persistBitmapLocked() error {
	payload, err := json.Marshal(stageSidecar{
		SHA256:    s.sha256hex,
		Size:      s.size,
		ChunkSize: s.chunkSize,
		Received:  s.received,
	})
	if err != nil {
		return err
	}
	return AtomicWriteJSON(s.bitmapPath, payload)
}

// Bitmap returns a copy of the received-chunk flags (index → landed).
func (s *ChunkStage) Bitmap() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]bool, len(s.received))
	copy(out, s.received)
	return out
}

// Missing returns the indices of chunks not yet received, ascending.
func (s *ChunkStage) Missing() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int
	for i, ok := range s.received {
		if !ok {
			out = append(out, i)
		}
	}
	return out
}

// Complete verifies the assembled .part's full sha256 equals the stage's
// content address and, on match, atomically renames it to finalDst (fsyncing
// the parent dir) and drops the bitmap sidecar. On a hash mismatch it returns
// an error wrapping ErrHashMismatch and LEAVES the .part in place so the caller
// can re-pull the corrupt chunks and retry. Complete may be retried.
func (s *ChunkStage) Complete(finalDst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return errors.New("storage: Complete on closed stage")
	}

	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync stage: %w", err)
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek stage: %w", err)
	}
	h := sha256.New()
	bufPtr := bufPool.Get().(*[]byte)
	_, err := io.CopyBuffer(h, io.LimitReader(s.f, s.size), *bufPtr)
	bufPool.Put(bufPtr)
	if err != nil {
		return fmt.Errorf("hash stage: %w", err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, s.sha256hex) {
		return fmt.Errorf("%w: expected sha256 %s got %s", ErrHashMismatch, s.sha256hex, got)
	}

	if err := os.MkdirAll(filepath.Dir(finalDst), 0o755); err != nil {
		return fmt.Errorf("mkdir final parent: %w", err)
	}
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("close stage: %w", err)
	}
	s.f = nil
	if err := os.Rename(s.partPath, finalDst); err != nil {
		return fmt.Errorf("rename stage → final: %w", err)
	}
	syncParentDir(finalDst)
	_ = os.Remove(s.bitmapPath)
	return nil
}

// Close releases the underlying file descriptor without completing the
// transfer, leaving the .part and bitmap on disk for a later OpenStage to
// resume. Safe to call more than once. Not required after a successful
// Complete (which already closed the file).
func (s *ChunkStage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}
