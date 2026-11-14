package unixhttp

import (
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func serve(t *testing.T, path string, uid int) net.Listener {
	t.Helper()
	l, err := Listen(path, uid)
	if err != nil {
		t.Fatal(err)
	}
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") }))
	t.Cleanup(func() { l.Close() })
	return l
}

func TestRoundTripAndMode(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.sock")
	serve(t, p, os.Getuid())
	fi, err := os.Stat(p)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v err %v", fi.Mode().Perm(), err)
	}
	resp, err := Client(p, time.Second).Get("http://x/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if b, _ := io.ReadAll(resp.Body); string(b) != "ok" {
		t.Fatalf("body %q", b)
	}
}

// The Client verifies the SERVER's SO_PEERCRED uid on every dial. Same-uid passes
// (TestRoundTripAndMode). A server running as a different uid is refused with
// ErrWrongPeer, but that cannot be unit-tested without a second uid; here we only
// assert the verification reads the real server uid (equal to ours for a local listener).
func TestClientChecksServerUID(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.sock")
	serve(t, p, os.Getuid())
	c, err := net.Dial("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := peerUID(c); got != os.Getuid() {
		t.Fatalf("server uid as seen by the client: %d, want %d", got, os.Getuid())
	}
	if resp, err := Client(p, time.Second).Get("http://x/ping"); err != nil {
		t.Fatalf("same-uid server must be accepted: %v", err)
	} else {
		resp.Body.Close()
	}
}

func TestWrongUIDRejected(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.sock")
	serve(t, p, os.Getuid()+1)
	if resp, err := Client(p, time.Second).Get("http://x/ping"); err == nil {
		resp.Body.Close()
		t.Fatal("a peer with the wrong uid must be refused")
	}
}

func TestStaleRemovedLiveRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.sock")
	l, err := net.Listen("unix", p)
	if err != nil {
		t.Fatal(err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	l.Close() // leaves a stale socket file behind
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("stale file expected: %v", err)
	}
	l2, err := Listen(p, os.Getuid())
	if err != nil {
		t.Fatalf("stale must be cleaned: %v", err)
	}
	defer l2.Close()
	if _, err := Listen(p, os.Getuid()); !errors.Is(err, ErrInUse) {
		t.Fatalf("live listener must refuse: %v", err)
	}
}

func TestNonSocketRefused(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	os.WriteFile(p, nil, 0o600)
	if _, err := Listen(p, os.Getuid()); err == nil {
		t.Fatal("must not delete a regular file")
	}
}
