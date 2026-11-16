package eventoutbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewBatchRequiresEachArgument(t *testing.T) {
	db := newDB(t)
	cl := http.DefaultClient
	for name, f := range map[string]func() error{
		"no db":     func() error { _, e := NewBatch(nil, cl, BatchConfig{URL: "x"}); return e },
		"no client": func() error { _, e := NewBatch(db, nil, BatchConfig{URL: "x"}); return e },
		"no url":    func() error { _, e := NewBatch(db, cl, BatchConfig{}); return e },
	} {
		if f() == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func cfgOf(t *testing.T, c BatchConfig) BatchConfig {
	t.Helper()
	c.URL = "http://x"
	o, err := NewBatch(newDB(t), http.DefaultClient, c)
	if err != nil {
		t.Fatal(err)
	}
	return o.cfg
}

func TestConfigDefaultsAndBoundaries(t *testing.T) {
	for _, v := range []int{0, -1} {
		c := cfgOf(t, BatchConfig{MaxEvents: v, MaxBytes: v, MaxRows: v})
		if c.MaxEvents != 100 || c.MaxBytes != 256<<10 || c.MaxRows != 10000 || c.HardRows != 20000 {
			t.Fatalf("int defaults for %d: %+v", v, c)
		}
	}
	for _, v := range []time.Duration{0, -1} {
		c := cfgOf(t, BatchConfig{MaxAge: v, DrainInterval: v, BackoffBase: v, BackoffMax: v, PushTimeout: v})
		if c.MaxAge != 24*time.Hour || c.DrainInterval != 30*time.Second || c.BackoffBase != time.Second ||
			c.BackoffMax != 5*time.Minute || c.PushTimeout != 15*time.Second {
			t.Fatalf("duration defaults for %v: %+v", v, c)
		}
	}
	// a configured 1 is a value, not "unset"
	c := cfgOf(t, BatchConfig{MaxEvents: 1, MaxBytes: 1, MaxRows: 1, HardRows: 1, MaxAge: 1, DrainInterval: 1,
		BackoffBase: 1, BackoffMax: 1, PushTimeout: 1})
	if c.MaxEvents != 1 || c.MaxBytes != 1 || c.MaxRows != 1 || c.HardRows != 1 || c.MaxAge != 1 || c.DrainInterval != 1 ||
		c.BackoffBase != 1 || c.BackoffMax != 1 || c.PushTimeout != 1 {
		t.Fatalf("a configured 1 was replaced: %+v", c)
	}
	if c := cfgOf(t, BatchConfig{MaxRows: 7}); c.HardRows != 14 {
		t.Fatalf("HardRows default = %d, want 2*MaxRows", c.HardRows)
	}
	if c := cfgOf(t, BatchConfig{MaxRows: 7, HardRows: 3}); c.HardRows != 14 {
		t.Fatalf("HardRows below MaxRows must be raised, got %d", c.HardRows)
	}
	if c := cfgOf(t, BatchConfig{BackoffBase: 2 * time.Second, BackoffMax: time.Second}); c.BackoffMax != 5*time.Minute {
		t.Fatalf("BackoffMax below base must default, got %v", c.BackoffMax)
	}
	if c := cfgOf(t, BatchConfig{BackoffBase: 2 * time.Second, BackoffMax: 2 * time.Second}); c.BackoffMax != 2*time.Second {
		t.Fatalf("BackoffMax == base must be kept, got %v", c.BackoffMax)
	}
}

func TestRandDefaultAndOverride(t *testing.T) {
	if c := cfgOf(t, BatchConfig{}); c.Rand == nil || c.Rand() < 0 || c.Rand() >= 1 {
		t.Fatal("default Rand must be a [0,1) source")
	}
	if c := cfgOf(t, BatchConfig{Rand: func() float64 { return 0.25 }}); c.Rand() != 0.25 {
		t.Fatal("a configured Rand was replaced")
	}
}

func TestStructuralConstants(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, nil)
	if cap(o.poke) != 1 {
		t.Fatalf("poke capacity %d: one drainer, one pending wake-up", cap(o.poke))
	}
	if len(o.Epoch()) != 16 {
		t.Fatalf("epoch %q: want 8 random bytes as 16 hex", o.Epoch())
	}
	var nilOutbox *BatchOutbox
	nilOutbox.Close() // must not panic
}

func TestEnqueueErrorsReturnZeroAndBoundaryIsExact(t *testing.T) {
	o, db := newBatchT(t, &ctl{}, nil)
	if id, err := o.Enqueue("k", []byte(`nope`), false); id != 0 || !errors.Is(err, ErrBadPayload) {
		t.Fatalf("bad payload -> %d %v", id, err)
	}
	p := []byte(`{"a":"` + strings.Repeat("x", 40) + `"}`)
	fixed := o.envelopeLen() + len(o.cfg.EventsKey) + 32
	o.cfg.MaxBytes = len(p) + fixed
	if _, err := o.Enqueue("k", p, true); err != nil {
		t.Fatalf("an event that exactly fits was refused: %v", err)
	}
	o.cfg.MaxBytes--
	if id, err := o.Enqueue("k", p, false); id != 0 || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("one byte over -> %d %v", id, err)
	}
	var prot int
	if err := db.QueryRow(`SELECT protected FROM batch_events WHERE id = 1`).Scan(&prot); err != nil || prot != 1 {
		t.Fatalf("protected column = %d (%v), want 1", prot, err)
	}
	db.Close()
	if id, err := o.Enqueue("k", []byte(`{}`), false); id != 0 || err == nil {
		t.Fatalf("a closed db must fail the enqueue, got %d %v", id, err)
	}
}

// bytesForTwo returns the MaxBytes at which exactly two events of payload size P fit one request.
func TestByteBudgetBoundaryIsExactAndRowsAreReleased(t *testing.T) {
	payload := []byte(`{"p":"` + strings.Repeat("y", 30) + `"}`)
	for _, tc := range []struct {
		delta     int
		wantFirst int
	}{{0, 2}, {-1, 1}} {
		c := &ctl{}
		o, db := newBatchT(t, c, nil)
		env := o.envelopeLen()
		o.cfg.MaxBytes = 2*(len(payload)+24) + env + len(o.cfg.EventsKey) + 16 + tc.delta
		for i := 0; i < 3; i++ {
			if _, err := o.Enqueue("k", payload, false); err != nil {
				t.Fatal(err)
			}
		}
		rows, err := o.load()
		if err != nil || len(rows) != tc.wantFirst {
			t.Fatalf("delta %d: loaded %d rows (%v), want %d", tc.delta, len(rows), err, tc.wantFirst)
		}
		if st := db.Stats(); st.InUse != 0 {
			t.Fatalf("delta %d: %d connections still in use after load: rows leaked", tc.delta, st.InUse)
		}
		body, _ := o.build(rows)
		if len(body) > o.cfg.MaxBytes {
			t.Fatalf("request is %d bytes, over MaxBytes %d", len(body), o.cfg.MaxBytes)
		}
	}
}

func TestBodyCloseAndReadLimit(t *testing.T) {
	ok := `{"accepted":1,"duplicate":0}`
	run := func(body string) (closed int, rows int) {
		db := newDB(t)
		client := &http.Client{Transport: roundTripFn(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{},
				Body: &countClose{Reader: strings.NewReader(body), n: &closed}}, nil
		})}
		o, err := NewBatch(db, client, BatchConfig{URL: "http://x", Rand: func() float64 { return 1 }})
		if err != nil {
			t.Fatal(err)
		}
		o.Enqueue("k", []byte(`{}`), false)
		o.drain(context.Background())
		return closed, rowsLeft(t, db)
	}
	closed, left := run(strings.Repeat(" ", 64<<10-len(ok)) + ok) // exactly 64 KiB
	if closed != 1 || left != 0 {
		t.Fatalf("exact 64 KiB: closed=%d left=%d, want body closed once and acked", closed, left)
	}
	if _, left := run(strings.Repeat(" ", 64<<10+1) + ok); left != 1 {
		t.Fatal("an ack beyond the 64 KiB read limit must be unseen (keep the row)")
	}
}

type countClose struct {
	io.Reader
	n *int
}

func (c *countClose) Close() error { *c.n++; return nil }

func TestStatusEdges(t *testing.T) {
	ackOK := `{"accepted":1,"duplicate":0}`
	for _, tc := range []struct {
		status   int
		wantRows int // after one send of a single event
		wantSent bool
	}{
		{199, 1, false}, {200, 0, true}, {299, 0, true}, {300, 1, false}, {301, 1, false},
		{399, 1, false}, {400, 0, false}, {499, 0, false}, {500, 1, false}, {501, 1, false},
	} {
		db := newDB(t)
		client := &http.Client{Transport: roundTripFn(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(ackOK))}, nil
		})}
		o, err := NewBatch(db, client, BatchConfig{URL: "http://x", Rand: func() float64 { return 1 }})
		if err != nil {
			t.Fatal(err)
		}
		enq(t, o, 1, false)
		rows, _ := o.load()
		got := o.send(context.Background(), rows)
		if rowsLeft(t, db) != tc.wantRows {
			t.Fatalf("status %d: %d rows left, want %d", tc.status, rowsLeft(t, db), tc.wantRows)
		}
		if (got == sentOK) != tc.wantSent {
			t.Fatalf("status %d: outcome %v", tc.status, got)
		}
	}
}

func TestSplitSizesAndSmallBatchesOn413And4xx(t *testing.T) {
	for _, status := range []int{413, 400} {
		for n, wantLimit := range map[int]int{2: 1, 3: 1, 5: 2, 7: 3} {
			c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
				w.WriteHeader(status)
				return true
			}}
			o, _ := newBatchT(t, c, nil)
			enq(t, o, n, false)
			rows, _ := o.load()
			if got := o.send(context.Background(), rows); got != sentSplit || o.limit != wantLimit {
				t.Fatalf("status %d n=%d: outcome %v limit %d, want split to %d", status, n, got, o.limit, wantLimit)
			}
		}
	}
}

func TestLimitDoublesAfterEachSuccess(t *testing.T) {
	c := &ctl{}
	o, _ := newBatchT(t, c, nil)
	enq(t, o, 3, false)
	o.limit = 3
	o.drain(context.Background())
	if o.limit != 6 {
		t.Fatalf("limit after a success = %d, want doubled to 6", o.limit)
	}
}

func TestAckParsingNeedsBothCounts(t *testing.T) {
	for name, body := range map[string]string{
		"accepted only":  `{"accepted":1}`,
		"duplicate only": `{"duplicate":1}`,
		"neither":        `{"rejected":[]}`,
		"garbage":        `}{`,
	} {
		c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool { io.WriteString(w, body); return true }}
		o, db := newBatchT(t, c, nil)
		enq(t, o, 1, false)
		if o.drain(context.Background()) || rowsLeft(t, db) != 1 {
			t.Fatalf("%s: a partial ack must keep the row and back off", name)
		}
	}
}

func TestRetryableRowReportsRetryAndOnlyItsAttemptsRise(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		io.WriteString(w, `{"accepted":1,"duplicate":0,"rejected":[{"seq":2,"code":"busy","retryable":true}]}`)
		return true
	}}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 2, false)
	rows, _ := o.load()
	if got := o.send(context.Background(), rows); got != sentRetry {
		t.Fatalf("a single retryable rejection must report retry, got %v", got)
	}
	var attempts int
	if err := db.QueryRow(`SELECT attempts FROM batch_events WHERE id = 2`).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("retryable row attempts = %d (%v), want 1", attempts, err)
	}
	if rowsLeft(t, db) != 1 {
		t.Fatalf("rows left = %d, want only the retryable one", rowsLeft(t, db))
	}
}

func TestPruneBoundariesAreExactAndSingleDropWorks(t *testing.T) {
	c := &ctl{}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.MaxRows, cfg.HardRows = 3, 5 })
	enq(t, o, 3, false)
	o.prune()
	if rowsLeft(t, db) != 3 {
		t.Fatal("exactly MaxRows must be kept")
	}
	enq(t, o, 1, false) // 4 rows: exactly ONE over
	o.prune()
	if rowsLeft(t, db) != 3 {
		t.Fatalf("a single row over MaxRows must be dropped, left %d", rowsLeft(t, db))
	}
	// hard cap: protected rows only; exactly HardRows is kept, one over drops one
	o2, db2 := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.MaxRows, cfg.HardRows = 3, 5 })
	enq(t, o2, 5, true)
	o2.prune()
	if rowsLeft(t, db2) != 5 {
		t.Fatalf("exactly HardRows must be kept, left %d", rowsLeft(t, db2))
	}
	enq(t, o2, 1, true)
	o2.prune()
	if rowsLeft(t, db2) != 5 {
		t.Fatalf("one over HardRows must drop one, left %d", rowsLeft(t, db2))
	}
}

func TestAgeDropOfExactlyOneRow(t *testing.T) {
	now := time.Unix(5_000_000, 0)
	o, db := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.MaxAge = time.Hour; cfg.Now = func() time.Time { return now } })
	enq(t, o, 1, false)
	now = now.Add(2 * time.Hour)
	o.prune()
	if rowsLeft(t, db) != 0 {
		t.Fatal("a single expired row must be dropped")
	}
	now = now.Add(time.Hour)
	enq(t, o, 1, false)
	now = now.Add(time.Hour) // exactly MaxAge old: kept (strictly older is dropped)
	o.prune()
	if rowsLeft(t, db) != 1 {
		t.Fatal("a row exactly MaxAge old must be kept")
	}
}

func TestAfterPassBookkeeping(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) {
		cfg.BackoffBase, cfg.BackoffMax, cfg.DrainInterval = time.Second, time.Minute, 7*time.Second
	})
	if w, bo := o.afterPass(true); w != 7*time.Second || bo {
		t.Fatalf("success -> %v %v", w, bo)
	}
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		if w, bo := o.afterPass(false); w != want || !bo {
			t.Fatalf("failure %d -> %v %v, want %v backing off", i+1, w, bo, want)
		}
	}
	o.afterPass(true)
	if w, _ := o.afterPass(false); w != time.Second {
		t.Fatalf("backoff must restart after a success, got %v", w)
	}
}

func TestBackoffExponentCap(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.BackoffBase, cfg.BackoffMax = 1, time.Duration(1)<<62 })
	if w := o.backoff(500); w != time.Duration(1)<<40 {
		t.Fatalf("backoff(500) = %v, want 2^40 ns (exponent capped at 40)", w)
	}
	if w := o.backoff(41); w != time.Duration(1)<<40 {
		t.Fatalf("backoff(41) = %v", w)
	}
	if w := o.backoff(40); w != time.Duration(1)<<39 {
		t.Fatalf("backoff(40) = %v", w)
	}
}

type recHandler struct {
	mu   *sync.Mutex
	logs *[]string
}

func (h recHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	*h.logs = append(*h.logs, r.Level.String()+" "+r.Message)
	h.mu.Unlock()
	return nil
}
func (h recHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recHandler) WithGroup(string) slog.Handler      { return h }

func TestFailureLoggedOncePerStretchAndRecoveryOnce(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	failing := true
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		if failing {
			w.WriteHeader(503)
			return true
		}
		return false
	}}
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) { cfg.Logger = slog.New(recHandler{&mu, &logs}) })
	enq(t, o, 1, false)
	for i := 0; i < 3; i++ {
		o.drain(context.Background())
	}
	count := func(prefix string) int {
		n := 0
		for _, l := range logs {
			if strings.HasPrefix(l, prefix) {
				n++
			}
		}
		return n
	}
	if count("WARN") != 1 {
		t.Fatalf("a failing stretch must log once, got %v", logs)
	}
	failing = false
	o.drain(context.Background())
	o.drain(context.Background())
	if count("INFO") != 1 {
		t.Fatalf("recovery must log once, got %v", logs)
	}
	failing = true
	o.drain(context.Background())
	enq(t, o, 1, false)
	o.drain(context.Background())
	if count("WARN") != 2 {
		t.Fatalf("a new failing stretch must log again, got %v", logs)
	}
}

func TestEmptyQueueDoesNotLogRecovery(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.Logger = slog.New(recHandler{&mu, &logs}) })
	o.drain(context.Background())
	if len(logs) != 0 {
		t.Fatalf("an idle drain logged %v", logs)
	}
}

func TestSingleEventPayloadsAreStampedExactly(t *testing.T) {
	for payload, want := range map[string]string{`{}`: `{"seq":1}`, `{"a":1}`: `{"seq":1,"a":1}`} {
		o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.Envelope = nil })
		o.Enqueue("k", []byte(payload), false)
		rows, _ := o.load()
		body, _ := o.build(rows)
		if !bytes.Contains(body, []byte(want)) {
			t.Fatalf("payload %s -> %s, want to contain %s", payload, body, want)
		}
	}
}

var _ = fmt.Sprint
var _ = httptest.NewServer
var _ sql.DB
