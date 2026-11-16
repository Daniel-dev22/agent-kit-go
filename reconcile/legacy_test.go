package reconcile

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

func legacyCfg(resp func() (*http.Response, error)) Config {
	return Config{
		Client:   &http.Client{Transport: rt(func(*http.Request) (*http.Response, error) { return resp() })},
		URL:      "http://ctl/reconcile",
		Snapshot: func(context.Context) ([]jobstore.JobRef, error) { return nil, nil },
	}
}

func TestPushOnceStatusRangeEdges(t *testing.T) {
	for status, ok := range map[int]bool{199: false, 200: true, 201: true, 299: true, 300: false, 301: false, 500: false} {
		cfg := legacyCfg(func() (*http.Response, error) { return reply(status, ``), nil })
		err := pushOnce(context.Background(), cfg, slog.Default())
		if (err == nil) != ok {
			t.Fatalf("status %d: err=%v, want ok=%v", status, err, ok)
		}
	}
}

func TestPushOnceClosesBodyAndTruncatesErrorText(t *testing.T) {
	closed := 0
	cfg := legacyCfg(func() (*http.Response, error) {
		return &http.Response{StatusCode: 500, Header: http.Header{},
			Body: closeCounter{strings.NewReader(strings.Repeat("x", 600)), &closed}}, nil
	})
	err := pushOnce(context.Background(), cfg, slog.Default())
	if closed != 1 {
		t.Fatalf("body closed %d times", closed)
	}
	if err == nil || strings.Count(err.Error(), "x") != 512 {
		t.Fatalf("error text carries %d body bytes, want 512: %v", strings.Count(err.Error(), "x"), err)
	}
	var _ io.Closer = closeCounter{}
}

func TestRunValidatesEachRequiredField(t *testing.T) {
	snap := func(context.Context) ([]jobstore.JobRef, error) { return nil, nil }
	for name, cfg := range map[string]Config{
		"no client": {URL: "x", Snapshot: snap},
		"no url":    {Client: http.DefaultClient, Snapshot: snap},
	} {
		done := make(chan struct{})
		go func() { Run(context.Background(), cfg); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s: Run did not return", name)
		}
	}
}

func TestRunZeroIntervalDefaultsInsteadOfPanicking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := legacyCfg(func() (*http.Response, error) { return reply(200, ``), nil })
	done := make(chan struct{})
	go func() { Run(ctx, cfg); close(done) }() // Interval 0: NewTicker(0) would panic if not defaulted
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
