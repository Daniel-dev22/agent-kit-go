package reconcile

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// OnReply sees the raw body of a confirmed 2xx reply and nothing else: a consumer reads the
// capability list the controller advertises, and an unconfirmed reply must never change it.
func TestOnReplyOnlyForConfirmedReplies(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	var got []string
	r.g.cfg.OnReply = func(b []byte) { got = append(got, string(b)) }
	cases := []struct {
		name string
		resp func() (*http.Response, error)
		want bool
	}{
		{"confirmed with capability list", func() (*http.Response, error) {
			return reply(200, `{"applied":true,"supported_kinds":["a","b"]}`), nil
		}, true},
		{"confirmed without a list (older controller)", func() (*http.Response, error) { return reply(200, `{"applied":true}`), nil }, true},
		{"applied false is still a parsed reply", func() (*http.Response, error) { return reply(200, `{"applied":false}`), nil }, true},
		{"no applied field", func() (*http.Response, error) { return reply(200, `{"supported_kinds":["a"]}`), nil }, false},
		{"not json", func() (*http.Response, error) { return reply(200, `oops`), nil }, false},
		{"500", func() (*http.Response, error) { return reply(500, `{"applied":true}`), nil }, false},
		{"transport error", func() (*http.Response, error) { return nil, errors.New("down") }, false},
	}
	for _, c := range cases {
		got = nil
		c := c
		r.resp = func(int) (*http.Response, error) { return c.resp() }
		r.g.mu.Lock()
		r.g.gate.WantSnapshot()
		r.g.mu.Unlock()
		r.g.Tick(context.Background())
		if len(got) > 1 || (len(got) == 1) != c.want {
			t.Errorf("%s: OnReply calls = %v, want called=%v", c.name, got, c.want)
		}
	}
	if len(got) == 1 && got[0] == "" {
		t.Error("empty body passed to OnReply")
	}
}

func TestOnReplyReceivesTheBody(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	var body string
	r.g.cfg.OnReply = func(b []byte) { body = string(b) }
	r.resp = func(int) (*http.Response, error) {
		return reply(200, `{"applied":true,"supported_kinds":["names_correlated_on"]}`), nil
	}
	r.g.Tick(context.Background())
	if body != `{"applied":true,"supported_kinds":["names_correlated_on"]}` {
		t.Fatalf("body = %q", body)
	}
}
