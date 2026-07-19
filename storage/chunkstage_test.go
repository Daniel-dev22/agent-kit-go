package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// makeContent returns deterministic pseudo-random bytes of length n and their
// sha256 hex digest.
func makeContent(n int) ([]byte, string) {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + 7) & 0xff)
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func chunkOf(content []byte, index int, chunkSize int64) []byte {
	off := int64(index) * chunkSize
	end := off + chunkSize
	if end > int64(len(content)) {
		end = int64(len(content))
	}
	return content[off:end]
}

// TestChunkStageFullWriteComplete writes every chunk (including a short final
// one) then completes: the final file must match byte-for-byte and the .part +
// bitmap sidecar must be gone.
func TestChunkStageFullWriteComplete(t *testing.T) {
	dir := t.TempDir()
	const chunkSize = 1024
	// 3.5 chunks so the last one is short.
	content, sum := makeContent(chunkSize*3 + 500)

	st, err := OpenStage(dir, sum, int64(len(content)), chunkSize)
	if err != nil {
		t.Fatalf("OpenStage: %v", err)
	}
	nchunks := len(st.Missing())
	if nchunks != 4 {
		t.Fatalf("expected 4 chunks missing, got %d", nchunks)
	}
	for i := 0; i < nchunks; i++ {
		if err := st.WriteChunk(i, bytes.NewReader(chunkOf(content, i, chunkSize))); err != nil {
			t.Fatalf("WriteChunk %d: %v", i, err)
		}
	}
	if m := st.Missing(); len(m) != 0 {
		t.Fatalf("expected no missing chunks, got %v", m)
	}

	finalDst := filepath.Join(dir, "assembled.bin")
	if err := st.Complete(finalDst); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got, err := os.ReadFile(finalDst)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("final content mismatch: got %d bytes, want %d", len(got), len(content))
	}
	if _, err := os.Stat(filepath.Join(dir, sum+".part")); !os.IsNotExist(err) {
		t.Errorf(".part should be gone after Complete, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, sum+".part.bitmap")); !os.IsNotExist(err) {
		t.Errorf("bitmap sidecar should be gone after Complete, stat err=%v", err)
	}
}

// TestChunkStageResume writes a subset of chunks, reopens the stage (simulating
// a restart), and verifies the received marks survived so only the missing
// chunks need writing to complete.
func TestChunkStageResume(t *testing.T) {
	dir := t.TempDir()
	const chunkSize = 512
	content, sum := makeContent(chunkSize * 5)
	total := int64(len(content))

	// First session: write chunks 0, 2, 4 then abandon (Close).
	st1, err := OpenStage(dir, sum, total, chunkSize)
	if err != nil {
		t.Fatalf("OpenStage 1: %v", err)
	}
	for _, i := range []int{0, 2, 4} {
		if err := st1.WriteChunk(i, bytes.NewReader(chunkOf(content, i, chunkSize))); err != nil {
			t.Fatalf("WriteChunk %d: %v", i, err)
		}
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Second session: bitmap must show 1 and 3 as the only missing chunks.
	st2, err := OpenStage(dir, sum, total, chunkSize)
	if err != nil {
		t.Fatalf("OpenStage 2 (resume): %v", err)
	}
	missing := st2.Missing()
	if !equalInts(missing, []int{1, 3}) {
		t.Fatalf("resume missing = %v, want [1 3]", missing)
	}
	bm := st2.Bitmap()
	for _, i := range []int{0, 2, 4} {
		if !bm[i] {
			t.Errorf("resume: chunk %d should be marked received", i)
		}
	}

	// Write ONLY the missing chunks; the previously-written ones must not need
	// rewriting for Complete to succeed.
	for _, i := range missing {
		if err := st2.WriteChunk(i, bytes.NewReader(chunkOf(content, i, chunkSize))); err != nil {
			t.Fatalf("WriteChunk resume %d: %v", i, err)
		}
	}
	finalDst := filepath.Join(dir, "resumed.bin")
	if err := st2.Complete(finalDst); err != nil {
		t.Fatalf("Complete after resume: %v", err)
	}
	got, err := os.ReadFile(finalDst)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("resumed content mismatch")
	}
}

// TestChunkStageSha256Mismatch completes a stage whose declared content address
// does not match the bytes written: Complete must return ErrHashMismatch and
// leave the .part in place (finalDst never created).
func TestChunkStageSha256Mismatch(t *testing.T) {
	dir := t.TempDir()
	const chunkSize = 256
	content, realSum := makeContent(chunkSize * 3)

	// Declare a wrong address: flip the last hex nibble of the real digest.
	wrongSum := flipLastNibble(realSum)
	if wrongSum == realSum {
		t.Fatal("failed to construct a wrong digest")
	}

	st, err := OpenStage(dir, wrongSum, int64(len(content)), chunkSize)
	if err != nil {
		t.Fatalf("OpenStage: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := st.WriteChunk(i, bytes.NewReader(chunkOf(content, i, chunkSize))); err != nil {
			t.Fatalf("WriteChunk %d: %v", i, err)
		}
	}

	finalDst := filepath.Join(dir, "should-not-exist.bin")
	err = st.Complete(finalDst)
	if err == nil {
		t.Fatal("Complete succeeded despite sha256 mismatch")
	}
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("Complete error = %v, want ErrHashMismatch", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, wrongSum+".part")); statErr != nil {
		t.Errorf(".part should be retained after mismatch, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(finalDst); !os.IsNotExist(statErr) {
		t.Errorf("finalDst must not exist after mismatch, stat err=%v", statErr)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func flipLastNibble(hexsum string) string {
	if hexsum == "" {
		return hexsum
	}
	last := hexsum[len(hexsum)-1]
	var repl byte
	if last == '0' {
		repl = '1'
	} else {
		repl = '0'
	}
	return hexsum[:len(hexsum)-1] + string(repl)
}
