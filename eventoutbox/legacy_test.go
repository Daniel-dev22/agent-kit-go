package eventoutbox

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func legacyOn(t *testing.T, status int, body string, closed *int) (*Outbox, func() int) {
	t.Helper()
	db := newDB(t)
	client := &http.Client{Transport: roundTripFn(func(*http.Request) (*http.Response, error) {
		var b io.ReadCloser = io.NopCloser(strings.NewReader(body))
		if closed != nil {
			b = &countClose{Reader: strings.NewReader(body), n: closed}
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: b}, nil
	})}
	o, err := New(db, client, func(id string) string { return "http://x/" + id })
	if err != nil {
		t.Fatal(err)
	}
	return o, func() int { return pendingCount(t, db) }
}

func TestLegacyNewRequiresEachArgument(t *testing.T) {
	db := newDB(t)
	cl := http.DefaultClient
	u := func(string) string { return "x" }
	for name, f := range map[string]func() error{
		"no db":     func() error { _, e := New(nil, cl, u); return e },
		"no client": func() error { _, e := New(db, nil, u); return e },
		"no urlFor": func() error { _, e := New(db, cl, nil); return e },
	} {
		if f() == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestLegacyPushStatusEdgesBodyCloseAndErrorText(t *testing.T) {
	for status, ok := range map[int]bool{199: false, 200: true, 299: true, 300: false, 301: false, 500: false} {
		closed := 0
		o, count := legacyOn(t, status, "", &closed)
		o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
		if got := o.push(context.Background(), 1, "j", []byte(`{}`)); got != ok {
			t.Fatalf("status %d: push = %v", status, got)
		}
		if closed != 1 {
			t.Fatalf("status %d: body closed %d times", status, closed)
		}
		if (count() == 0) != ok {
			t.Fatalf("status %d: row presence wrong", status)
		}
	}
	o, _ := legacyOn(t, 500, strings.Repeat("z", 5000), nil)
	o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	o.push(context.Background(), 1, "j", []byte(`{}`))
	var le string
	o.db.QueryRow(`SELECT last_error FROM pending_events WHERE id = 1`).Scan(&le)
	if want := "HTTP 500: " + strings.Repeat("z", 200); le != want {
		t.Fatalf("last_error = %q (%d bytes), want the body cut to 200 bytes", le, len(le))
	}
}

func TestLegacyDrainBatchLimitIntervalAndNilClose(t *testing.T) {
	o, count := legacyOn(t, 200, "", nil)
	if o.interval != 30*time.Second {
		t.Fatalf("drain interval = %v", o.interval)
	}
	for i := 0; i < 150; i++ {
		o.db.Exec(`INSERT INTO pending_events (job_id, event, payload) VALUES ('j','e','{}')`)
	}
	o.drainOnce(context.Background())
	if count() != 50 {
		t.Fatalf("after one drain %d rows remain, want 50 (batch limit 100)", count())
	}
	var nilOutbox *Outbox
	nilOutbox.Close()
}

func TestBatchOversizedFirstEventIsStillSent(t *testing.T) {
	c := &ctl{}
	o, _ := newBatchT(t, c, nil)
	p := []byte(`{"a":"` + strings.Repeat("x", 100) + `"}`)
	o.cfg.MaxBytes = len(p) + o.envelopeLen() + len(o.cfg.EventsKey) + 32 // passes Enqueue, but exceeds the load budget
	if _, err := o.Enqueue("k", p, false); err != nil {
		t.Fatal(err)
	}
	rows, err := o.load()
	if err != nil || len(rows) != 1 {
		t.Fatalf("an event that passed Enqueue must always load alone: %d rows, %v", len(rows), err)
	}
}
