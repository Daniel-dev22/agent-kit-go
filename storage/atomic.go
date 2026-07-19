// Package storage is the shared on-disk toolbox for controller agents that
// stage large files to local disk: crash-safe atomic writes, an in-band-hashed
// streaming copy primitive, a resumable content-addressed chunk stager, plus
// the per-account manifest / version-rotation / graveyard / path-sanitize
// helpers promoted from gdrive-agent's internal/storage.
//
// Everything here is transport-neutral: no HTTP, oauth2, Drive, or Range/resume
// semantics. Agents that need those drive the request themselves and hand the
// response body to CopyToFileAtomic (or the ChunkStage) as an io.Reader.
package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// bufferSize is the per-copy block size. 64KB is large enough that read/write
// syscalls aren't the bottleneck on any reasonable hardware, small enough that
// goroutine memory stays flat even with hundreds of concurrent transfers.
const bufferSize = 64 * 1024

// bufPool reuses 64KB byte slices across every copy operation. sync.Pool is
// goroutine-safe and the Go runtime drains it under memory pressure, so the
// agent never holds idle buffers when nothing's transferring.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufferSize)
		return &b
	},
}

// ErrHashMismatch is returned (wrapped) when a computed digest disagrees with
// the expected one. Callers use errors.Is(err, ErrHashMismatch). Kept generic
// (not md5/sha-specific) so every consumer shares one sentinel.
var ErrHashMismatch = errors.New("storage: hash mismatch")

// writerOnly hides an io.ReaderFrom (e.g. *os.File) so io.CopyBuffer is forced
// to stream through the pooled buffer instead of taking a zero-copy fast path.
type writerOnly struct{ io.Writer }

// ctxReader makes an io.Reader honor context cancellation between reads: each
// 64KB block checks ctx.Err() first, so a cancelled transfer stops promptly.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// CopyToFileAtomic streams r into dst atomically and durably, computing a hash
// in-band when h != nil. It writes to dst+".part", fsyncs it, renames
// .part -> dst, then fsyncs the parent directory so the rename survives a
// crash. It returns the number of bytes written and, if h != nil, the
// lowercase-hex digest of exactly those bytes (no second pass over the data).
//
// This is the transport-neutral core of a streamed download: it has no HTTP,
// Range, or resume semantics. Callers needing those (e.g. gdrive-agent's
// StreamDownload) build the request themselves and pass the response body as r.
// On any error the .part file is removed best-effort so a retry starts clean.
// It does NOT compare the digest to an expected value — the caller does that
// against whatever authority it has (Drive md5, catalog sha, …) and may use
// ErrHashMismatch for a consistent error.
func CopyToFileAtomic(ctx context.Context, r io.Reader, dst string, h hash.Hash) (written int64, hexsum string, err error) {
	if r == nil || dst == "" {
		return 0, "", errors.New("storage: CopyToFileAtomic requires r and dst")
	}

	partPath := dst + ".part"
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		return 0, "", fmt.Errorf("mkdir parent: %w", err)
	}

	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, "", fmt.Errorf("open .part: %w", err)
	}
	// Remove a partial file on any error path. On success we set done=true so
	// the rename result is kept.
	done := false
	defer func() {
		if !done {
			_ = f.Close()
			_ = os.Remove(partPath)
		}
	}()

	var dstw io.Writer = writerOnly{f}
	if h != nil {
		h.Reset()
		dstw = io.MultiWriter(f, h)
	}

	bufPtr := bufPool.Get().(*[]byte)
	written, err = io.CopyBuffer(dstw, &ctxReader{ctx: ctx, r: r}, *bufPtr)
	bufPool.Put(bufPtr)
	if err != nil {
		return written, "", fmt.Errorf("copy body: %w", err)
	}

	if err := f.Sync(); err != nil {
		return written, "", fmt.Errorf("fsync .part: %w", err)
	}
	if err := f.Close(); err != nil {
		return written, "", fmt.Errorf("close .part: %w", err)
	}
	if err := os.Rename(partPath, dst); err != nil {
		return written, "", fmt.Errorf("rename .part → dst: %w", err)
	}
	done = true // .part is gone (renamed); nothing left to clean up.
	syncParentDir(dst)

	if h != nil {
		hexsum = hex.EncodeToString(h.Sum(nil))
	}
	return written, hexsum, nil
}

// AtomicWriteJSON writes payload to dst via tmp+rename, with fsync. Used by the
// manifest writer and the chunk-stage bitmap sidecar.
func AtomicWriteJSON(dst string, payload []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp.*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		// Best-effort cleanup if we bail before Rename.
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, dst)
}

// MoveAtomic renames src → dst, creating dst's parent dir if needed. Used by
// versions.go to rotate live files into _versions/ and by graveyard.go to move
// soft-deleted files out of the live tree.
func MoveAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.Rename(src, dst)
}

// syncParentDir fsyncs the directory containing path so a preceding rename is
// durable after a crash. Best-effort — a failure here doesn't fail the write.
func syncParentDir(path string) {
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
