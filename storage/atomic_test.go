package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestCopyToFileAtomicHashed streams a reader to disk, computing the hash
// in-band, and checks the written count, digest, file contents, and that no
// .part is left behind.
func TestCopyToFileAtomicHashed(t *testing.T) {
	dir := t.TempDir()
	content, sum := makeContent(200*1024 + 123) // spans several 64KB buffers

	dst := filepath.Join(dir, "sub", "out.bin")
	n, hexsum, err := CopyToFileAtomic(context.Background(), bytes.NewReader(content), dst, sha256.New())
	if err != nil {
		t.Fatalf("CopyToFileAtomic: %v", err)
	}
	if n != int64(len(content)) {
		t.Errorf("written = %d, want %d", n, len(content))
	}
	if hexsum != sum {
		t.Errorf("hexsum = %s, want %s", hexsum, sum)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("dst content mismatch")
	}
	if _, err := os.Stat(dst + ".part"); !os.IsNotExist(err) {
		t.Errorf(".part should be gone, stat err=%v", err)
	}
}

// TestCopyToFileAtomicNilHash confirms a nil hasher yields an empty digest but
// still writes the file.
func TestCopyToFileAtomicNilHash(t *testing.T) {
	dir := t.TempDir()
	content := []byte("hello mesh")
	dst := filepath.Join(dir, "plain.txt")
	n, hexsum, err := CopyToFileAtomic(context.Background(), bytes.NewReader(content), dst, nil)
	if err != nil {
		t.Fatalf("CopyToFileAtomic: %v", err)
	}
	if n != int64(len(content)) || hexsum != "" {
		t.Errorf("n=%d hexsum=%q, want n=%d hexsum=\"\"", n, hexsum, len(content))
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, content) {
		t.Error("dst content mismatch")
	}
	// Sanity: the digest a caller would compute matches the bytes on disk.
	want := sha256.Sum256(content)
	h := sha256.New()
	h.Write(got)
	if hex.EncodeToString(h.Sum(nil)) != hex.EncodeToString(want[:]) {
		t.Error("on-disk digest mismatch")
	}
}

// errReader yields some bytes then a hard error, to exercise the failure path.
type errReader struct {
	data []byte
	off  int
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.off >= len(e.data) {
		return 0, errors.New("synthetic read failure")
	}
	n := copy(p, e.data[e.off:])
	e.off += n
	return n, nil
}

// TestCopyToFileAtomicErrorCleansUp verifies a mid-stream read error removes the
// .part and never creates dst.
func TestCopyToFileAtomicErrorCleansUp(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "partial.bin")
	_, _, err := CopyToFileAtomic(context.Background(), &errReader{data: []byte("some bytes")}, dst, sha256.New())
	if err == nil {
		t.Fatal("expected an error from the failing reader")
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("dst must not exist after error, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(dst + ".part"); !os.IsNotExist(statErr) {
		t.Errorf(".part must be cleaned up after error, stat err=%v", statErr)
	}
}

// TestCopyToFileAtomicCtxCancel confirms a cancelled context stops the copy.
func TestCopyToFileAtomicCtxCancel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dst := filepath.Join(dir, "cancelled.bin")
	_, _, err := CopyToFileAtomic(ctx, bytes.NewReader(make([]byte, 1<<20)), dst, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(dst); !os.IsNotExist(statErr) {
		t.Errorf("dst must not exist after cancel, stat err=%v", statErr)
	}
}

var _ io.Reader = (*errReader)(nil)
