package sdnotify

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestNotifyUnset(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	if sent, err := Ready(); sent || err != nil {
		t.Fatalf("unset: sent=%v err=%v", sent, err)
	}
}

func TestNotifyDelivers(t *testing.T) {
	p := filepath.Join(t.TempDir(), "n.sock")
	l, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: p, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	t.Setenv("NOTIFY_SOCKET", p)
	for _, tc := range []struct {
		fn   func() (bool, error)
		want string
	}{{Ready, "READY=1"}, {Watchdog, "WATCHDOG=1"}} {
		if sent, err := tc.fn(); !sent || err != nil {
			t.Fatalf("%s: sent=%v err=%v", tc.want, sent, err)
		}
		buf := make([]byte, 64)
		l.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := l.ReadFromUnix(buf)
		if err != nil || string(buf[:n]) != tc.want {
			t.Fatalf("got %q err=%v want %q", buf[:n], err, tc.want)
		}
	}
}

func TestWatchdogInterval(t *testing.T) {
	t.Setenv("WATCHDOG_PID", "")
	t.Setenv("WATCHDOG_USEC", "")
	if d, err := WatchdogInterval(); d != 0 || err != nil {
		t.Fatalf("unset: %v %v", d, err)
	}
	t.Setenv("WATCHDOG_USEC", "30000000")
	if d, _ := WatchdogInterval(); d != 15*time.Second {
		t.Fatalf("got %v want 15s", d)
	}
	t.Setenv("WATCHDOG_PID", "1")
	if d, _ := WatchdogInterval(); d != 0 {
		t.Fatalf("other pid must disable: %v", d)
	}
	t.Setenv("WATCHDOG_PID", "")
	t.Setenv("WATCHDOG_USEC", "x")
	if _, err := WatchdogInterval(); err == nil {
		t.Fatal("bad value must error")
	}
}
