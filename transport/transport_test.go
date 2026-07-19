package transport

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRouterTransportDialRewrite verifies dial-target selection: in rewrite
// mode every dial lands on <dns>:<port> regardless of the requested address; in
// direct mode the requested address is dialed unchanged (no redirection).
func TestRouterTransportDialRewrite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	ctx := context.Background()

	// Rewrite mode: the dial target is ignored — a bogus authority still lands
	// on the listener. (example.invalid is never resolved because the closure
	// discards the addr argument.)
	rw := NewRouterTransport(host, port)
	conn, err := rw.DialContext(ctx, "tcp", "example.invalid:443")
	if err != nil {
		t.Fatalf("rewrite dial: %v", err)
	}
	if got := conn.RemoteAddr().String(); got != ln.Addr().String() {
		t.Errorf("rewrite dialed %s, want %s", got, ln.Addr().String())
	}
	conn.Close()

	// Direct mode: dial the address exactly as given.
	direct := NewRouterTransport("", "")
	conn2, err := direct.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("direct dial: %v", err)
	}
	if got := conn2.RemoteAddr().String(); got != ln.Addr().String() {
		t.Errorf("direct dialed %s, want %s", got, ln.Addr().String())
	}
	conn2.Close()

	// Direct mode must NOT redirect: an unreachable address has to fail,
	// proving the listener target didn't leak in from rewrite mode.
	if c, err := direct.DialContext(ctx, "tcp", "127.0.0.1:1"); err == nil {
		c.Close()
		t.Error("direct dial to 127.0.0.1:1 unexpectedly succeeded (rewrite leaked?)")
	}
}

// TestBearerRoundTripper verifies the bearer header is attached when absent,
// never overwrites a caller-set header, and is omitted for an empty token.
func TestBearerRoundTripper(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	do := func(c *http.Client, req *http.Request) {
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp.Body.Close()
	}

	// nil base defaults to http.DefaultTransport; bearer attached when absent.
	c := &http.Client{Transport: NewBearerRoundTripper(nil, "secret")}
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	do(c, req)
	if gotAuth != "Bearer secret" {
		t.Errorf("attach: got %q, want %q", gotAuth, "Bearer secret")
	}

	// A caller-set Authorization header is preserved.
	req2, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req2.Header.Set("Authorization", "Bearer preset")
	do(c, req2)
	if gotAuth != "Bearer preset" {
		t.Errorf("override: got %q, want %q", gotAuth, "Bearer preset")
	}

	// Empty token adds nothing.
	c2 := &http.Client{Transport: NewBearerRoundTripper(nil, "")}
	req3, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	do(c2, req3)
	if gotAuth != "" {
		t.Errorf("empty token: got %q, want no header", gotAuth)
	}
}

// TestNewRouterClient sanity-checks composition: bearer set -> requests carry
// the token; bearer empty -> the transport is the bare pooled transport.
func TestNewRouterClient(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	c := NewRouterClient(RouterClientConfig{BearerToken: "tok"})
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if gotAuth != "Bearer tok" {
		t.Errorf("router client bearer: got %q, want %q", gotAuth, "Bearer tok")
	}
	if c.Timeout != clientTimeout {
		t.Errorf("client timeout: got %v, want %v", c.Timeout, clientTimeout)
	}

	c2 := NewRouterClient(RouterClientConfig{})
	if _, ok := c2.Transport.(*http.Transport); !ok {
		t.Errorf("no-bearer client transport = %T, want *http.Transport", c2.Transport)
	}
}
