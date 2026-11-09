package transport

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// recorder is a server that records every request it receives.
type recorder struct {
	*httptest.Server
	mu   sync.Mutex
	auth []string
	body []string
}

func newRecorder(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *recorder {
	t.Helper()
	rec := &recorder{}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.auth = append(rec.auth, r.Header.Get("Authorization"))
		rec.body = append(rec.body, string(b))
		rec.mu.Unlock()
		if h != nil {
			h(w, r)
		}
	}))
	t.Cleanup(rec.Close)
	return rec
}

func (s *recorder) hits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.auth)
}

func redirectTo(loc string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, loc, http.StatusTemporaryRedirect)
	}
}

// localhostURL rewrites a 127.0.0.1:port URL to localhost:port: a genuinely
// different host name (not just a different port) on the same machine.
func localhostURL(u string) string { return strings.Replace(u, "127.0.0.1", "localhost", 1) }

func post(t *testing.T, rawurl string) error {
	t.Helper()
	c := NewRouterClient(RouterClientConfig{BearerToken: "s3cret"})
	resp, err := c.Post(rawurl, "text/plain", strings.NewReader("SECRET-BODY"))
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func assertRefused(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrCrossOriginRedirect) {
		t.Fatalf("want ErrCrossOriginRedirect, got %v", err)
	}
}

func assertUntouched(t *testing.T, name string, s *recorder) {
	t.Helper()
	if n := s.hits(); n != 0 {
		t.Fatalf("%s received %d request(s) (auth=%q body=%q); the hop must be refused", name, n, s.auth, s.body)
	}
}

func TestRedirectToDifferentHostRefused(t *testing.T) {
	target := newRecorder(t, nil)
	origin := newRecorder(t, redirectTo(localhostURL(target.URL)))
	assertRefused(t, post(t, origin.URL))
	assertUntouched(t, "target", target)
}

func TestRedirectToDifferentPortRefused(t *testing.T) {
	target := newRecorder(t, nil)
	origin := newRecorder(t, redirectTo(target.URL))
	assertRefused(t, post(t, origin.URL))
	assertUntouched(t, "target", target)
}

// A -> B -> B: the second hop is B vs B, which a previous-hop comparison calls
// same-origin and re-attaches the token. The first hop is refused so B never
// sees anything, body included.
func TestRedirectChainAtoBtoBRefused(t *testing.T) {
	b := newRecorder(t, redirectTo("/y"))
	origin := newRecorder(t, redirectTo(localhostURL(b.URL)+"/x"))
	assertRefused(t, post(t, origin.URL))
	assertUntouched(t, "B", b)
}

func TestRedirectChainAtoBtoARefused(t *testing.T) {
	var a *recorder
	b := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, a.URL+"/back", http.StatusTemporaryRedirect)
	})
	a = newRecorder(t, redirectTo(localhostURL(b.URL)))
	assertRefused(t, post(t, a.URL))
	assertUntouched(t, "B", b)
	if n := a.hits(); n != 1 {
		t.Fatalf("A hit %d times, want exactly the original request", n)
	}
}

func TestSameOriginChainKeepsBearerAndBody(t *testing.T) {
	srv := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/a":
			http.Redirect(w, r, "/b", http.StatusTemporaryRedirect)
		case "/b":
			http.Redirect(w, r, "/c", http.StatusPermanentRedirect)
		}
	})
	if err := post(t, srv.URL+"/a"); err != nil {
		t.Fatal(err)
	}
	if srv.hits() != 3 {
		t.Fatalf("hits=%d want 3", srv.hits())
	}
	for i := range srv.auth {
		if srv.auth[i] != "Bearer s3cret" || srv.body[i] != "SECRET-BODY" {
			t.Fatalf("hop %d auth=%q body=%q", i, srv.auth[i], srv.body[i])
		}
	}
}

// chain builds synthetic redirect hops (root first) the way net/http links
// them: each hop's Response.Request is the previous hop.
func chain(t *testing.T, urls ...string) *http.Request {
	t.Helper()
	var prev *http.Request
	for _, u := range urls {
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			t.Fatal(err)
		}
		if prev != nil {
			req.Response = &http.Response{Request: prev}
		}
		prev = req
	}
	return prev
}

type captureRT struct{ got *http.Request }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.got = r
	return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
}

func TestRedirectChainOriginRules(t *testing.T) {
	for _, tc := range []struct {
		name string
		urls []string
		ok   bool
	}{
		{"single request", []string{"https://h.example/x"}, true},
		{"same origin", []string{"https://h.example/x", "https://h.example/y"}, true},
		{"https->https->http same host", []string{"https://h.example/x", "https://h.example/y", "http://h.example/z"}, false},
		{"https->http same host", []string{"https://h.example/x", "http://h.example/x"}, false},
		{"http->https is a different origin", []string{"http://h.example/x", "https://h.example/x"}, false},
		{"A->B->B", []string{"https://a.example/x", "https://b.example/x", "https://b.example/y"}, false},
		{"A->B->A", []string{"https://a.example/x", "https://b.example/x", "https://a.example/y"}, false},
		{"other port", []string{"https://h.example/x", "https://h.example:8443/x"}, false},
		{"explicit default port https", []string{"https://h.example/x", "https://h.example:443/y"}, true},
		{"explicit default port http", []string{"http://h.example:80/x", "http://h.example/y"}, true},
		{"host case", []string{"https://H.Example/x", "https://h.example/y"}, true},
		{"trailing dot", []string{"https://h.example./x", "https://h.example/y"}, true},
		{"ipv6", []string{"https://[::1]:8443/x", "https://[::1]:8443/y"}, true},
	} {
		cap := &captureRT{}
		_, err := NewBearerRoundTripper(cap, "s3cret").RoundTrip(chain(t, tc.urls...))
		if tc.ok {
			if err != nil || cap.got == nil || cap.got.Header.Get("Authorization") != "Bearer s3cret" {
				t.Errorf("%s: err=%v, want allowed with bearer", tc.name, err)
			}
			continue
		}
		if !errors.Is(err, ErrCrossOriginRedirect) || cap.got != nil {
			t.Errorf("%s: err=%v forwarded=%v, want refused and not forwarded", tc.name, err, cap.got != nil)
		}
	}
}

type closeBody struct {
	io.Reader
	closed bool
}

func (c *closeBody) Close() error { c.closed = true; return nil }

func TestRefusedHopClosesBody(t *testing.T) {
	req := chain(t, "https://a.example/x", "https://b.example/x")
	cb := &closeBody{Reader: strings.NewReader("x")}
	req.Body = cb
	_, err := NewBearerRoundTripper(&captureRT{}, "t").RoundTrip(req)
	assertRefused(t, err)
	if !cb.closed {
		t.Fatal("request body not closed on refusal")
	}
}
