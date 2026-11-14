// Package unixhttp serves and calls HTTP over a unix socket with owner-only
// permissions, SO_PEERCRED uid enforcement and stale-socket cleanup.
package unixhttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// ErrInUse is returned when another process is already listening on the path.
var ErrInUse = errors.New("unixhttp: socket is in use by a live listener")

// Listen creates the socket at path with mode 0600. A leftover socket file with
// no listener is removed; a live listener yields ErrInUse. Only connections from
// uid are accepted (others are closed before any byte is read).
func Listen(path string, uid int) (net.Listener, error) {
	if err := clearStale(path); err != nil {
		return nil, err
	}
	old := unix.Umask(0o177)
	l, err := net.Listen("unix", path)
	unix.Umask(old)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	return &peerListener{Listener: l, uid: uid}, nil
}

func clearStale(path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("unixhttp: %s exists and is not a socket", path)
	}
	c, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		c.Close()
		return ErrInUse
	}
	return os.Remove(path)
}

type peerListener struct {
	net.Listener
	uid int
}

func (p *peerListener) Accept() (net.Conn, error) {
	for {
		c, err := p.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if peerUID(c) == p.uid {
			return c, nil
		}
		c.Close()
	}
}

func peerUID(c net.Conn) int {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return -1
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return -1
	}
	uid := -1
	if err := raw.Control(func(fd uintptr) {
		if cred, e := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED); e == nil {
			uid = int(cred.Uid)
		}
	}); err != nil {
		return -1
	}
	return uid
}

// ErrWrongPeer is returned by a Client dial when the socket's server runs as a different uid.
var ErrWrongPeer = errors.New("unixhttp: socket server uid differs from the caller's")

// Client returns an http.Client that dials the socket at path whatever the URL host is.
// Every dial verifies the server's SO_PEERCRED uid equals the caller's uid and refuses
// (closing the connection) otherwise, so a socket planted by another user is never trusted.
func Client(path string, timeout time.Duration) *http.Client {
	uid := os.Getuid()
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				c, err := d.DialContext(ctx, "unix", path)
				if err != nil {
					return nil, err
				}
				if got := peerUID(c); got != uid {
					c.Close()
					return nil, fmt.Errorf("%w (server uid %d, caller uid %d)", ErrWrongPeer, got, uid)
				}
				return c, nil
			},
			MaxIdleConns:    2,
			IdleConnTimeout: 30 * time.Second,
		},
	}
}
