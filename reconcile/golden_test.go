package reconcile

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

// TestGoldenWireRunSnapshot pins the exact POST an existing Run caller sends:
// method, Content-Type and the field set/order of the body (snapshot_at is the
// only moving value). The gated extension is additive; this must not change.
func TestGoldenWireRunSnapshot(t *testing.T) {
	type seen struct{ method, ctype, body string }
	ch := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		select {
		case ch <- seen{r.Method, r.Header.Get("Content-Type"), string(b)}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, Config{Client: srv.Client(), URL: srv.URL, Site: "kd", Node: "nas", Interval: time.Hour,
		Snapshot: func(context.Context) ([]jobstore.JobRef, error) {
			return []jobstore.JobRef{{ID: "j1", State: "running"}}, nil
		}})
	select {
	case s := <-ch:
		if s.method != "POST" || s.ctype != "application/json" {
			t.Fatalf("method/ctype = %q/%q", s.method, s.ctype)
		}
		norm := regexp.MustCompile(`"snapshot_at":"[^"]+"`).ReplaceAllString(s.body, `"snapshot_at":"T"`)
		const prefix = `{"site":"kd","node":"nas","snapshot_at":"T","jobs":[{"id":"j1","state":"running"`
		if len(norm) < len(prefix) || norm[:len(prefix)] != prefix {
			t.Fatalf("body = %s", norm)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no POST")
	}
}
