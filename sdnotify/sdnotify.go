// Package sdnotify speaks the systemd notify protocol (READY, WATCHDOG, STATUS)
// over NOTIFY_SOCKET with no dependencies.
package sdnotify

import (
	"errors"
	"net"
	"os"
	"strconv"
	"time"
)

// Notify sends state to systemd. sent is false (and err nil) when the process
// is not running under a notify-aware unit (NOTIFY_SOCKET unset).
func Notify(state string) (sent bool, err error) {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return false, nil
	}
	if addr[0] == '@' { // abstract socket
		addr = "\x00" + addr[1:]
	}
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		return false, err
	}
	defer c.Close()
	if _, err := c.Write([]byte(state)); err != nil {
		return false, err
	}
	return true, nil
}

// Ready reports READY=1.
func Ready() (bool, error) { return Notify("READY=1") }

// Stopping reports STOPPING=1.
func Stopping() (bool, error) { return Notify("STOPPING=1") }

// Status sets the free-form STATUS= line.
func Status(s string) (bool, error) { return Notify("STATUS=" + s) }

// Watchdog pings the service watchdog (WATCHDOG=1).
func Watchdog() (bool, error) { return Notify("WATCHDOG=1") }

// WatchdogInterval returns half of WATCHDOG_USEC, the recommended ping period,
// or 0 when no watchdog is configured for this process.
func WatchdogInterval() (time.Duration, error) {
	us := os.Getenv("WATCHDOG_USEC")
	if us == "" {
		return 0, nil
	}
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return 0, nil
	}
	n, err := strconv.ParseInt(us, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("sdnotify: invalid WATCHDOG_USEC")
	}
	return time.Duration(n) * time.Microsecond / 2, nil
}
