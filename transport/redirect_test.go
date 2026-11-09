package transport

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// A redirect hop to another authority must never carry the bearer: net/http
// strips a caller-set Authorization on cross-host hops, but a RoundTripper
// that adds the header itself re-attaches it after that stripping.
func TestBearerNotSentToRedirectTargetHost(t *testing.T) {
	var got string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	c := NewRouterClient(RouterClientConfig{BearerToken: "s3cret"})
	resp, err := c.Post(origin.URL, "text/plain", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != "" {
		t.Fatalf("bearer leaked to redirect target: %q", got)
	}
}

// Same-authority redirects (trailing-slash, path moves) keep working.
func TestBearerKeptOnSameHostRedirect(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a" {
			http.Redirect(w, r, "/b", http.StatusTemporaryRedirect)
			return
		}
		got = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	c := NewRouterClient(RouterClientConfig{BearerToken: "s3cret"})
	resp, err := c.Get(srv.URL + "/a")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != "Bearer s3cret" {
		t.Fatalf("same-host redirect lost bearer: %q", got)
	}
}

type captureRT struct{ got *http.Request }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.got = r
	return &http.Response{StatusCode: 200, Body: http.NoBody, Request: r}, nil
}

func prev(rawurl string) *http.Response {
	u, _ := url.Parse(rawurl)
	return &http.Response{Request: &http.Request{URL: u}}
}

// A scheme downgrade (https -> http) on the same host is a different origin.
func TestBearerNotSentOnSchemeDowngrade(t *testing.T) {
	cap := &captureRT{}
	rt := NewBearerRoundTripper(cap, "s3cret")
	for _, tc := range []struct {
		name, from, to string
		want           bool
	}{
		{"downgrade", "https://h.example/x", "http://h.example/x", false},
		{"other-host", "https://h.example/x", "https://o.example/x", false},
		{"other-port", "https://h.example/x", "https://h.example:8443/x", false},
		{"same", "https://h.example/x", "https://h.example/y", true},
		{"upgrade", "http://h.example/x", "https://h.example/x", true},
	} {
		req, _ := http.NewRequest(http.MethodGet, tc.to, nil)
		req.Response = prev(tc.from)
		if _, err := rt.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
		if has := cap.got.Header.Get("Authorization") != ""; has != tc.want {
			t.Errorf("%s: bearer attached=%v want %v", tc.name, has, tc.want)
		}
	}
}
