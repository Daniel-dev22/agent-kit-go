package eventoutbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// ctl is a fake controller that dedupes on (epoch, seq) like the real one.
type ctl struct {
	mu       sync.Mutex
	posts    int
	bodies   []string
	seen     map[string]int // epoch/seq -> times accepted fresh
	delivery []int64        // seqs in first-accept order
	respond  func(c *ctl, w http.ResponseWriter, evs []map[string]any) bool
}

func (c *ctl) handler(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var env struct {
		Epoch  string           `json:"epoch"`
		Events []map[string]any `json:"events"`
	}
	_ = json.Unmarshal(b, &env)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.posts++
	c.bodies = append(c.bodies, string(b))
	if c.respond != nil && c.respond(c, w, env.Events) {
		return
	}
	acc, dup := 0, 0
	for _, e := range env.Events {
		k := fmt.Sprintf("%s/%v", env.Epoch, e["seq"])
		if c.seen[k] > 0 {
			dup++
			continue
		}
		c.seen[k]++
		acc++
		c.delivery = append(c.delivery, int64(e["seq"].(float64)))
	}
	fmt.Fprintf(w, `{"accepted":%d,"duplicate":%d,"last_seq":0,"epoch_changed":false}`, acc, dup)
}

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "b.sqlite")+
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if os.Getenv("KIT_SINGLE_CONN") == "1" {
		// The ECMP agent runs ONE connection and one writer: any nested query (rows held open
		// while another statement runs) deadlocks here and passes under the default pool.
		db.SetMaxOpenConns(1)
	}
	return db
}

func newBatchT(t *testing.T, c *ctl, mut func(*BatchConfig)) (*BatchOutbox, *sql.DB) {
	t.Helper()
	if c.seen == nil {
		c.seen = map[string]int{}
	}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	db := newDB(t)
	return batchOn(t, db, srv, mut), db
}

func batchOn(t *testing.T, db *sql.DB, srv *httptest.Server, mut func(*BatchConfig)) *BatchOutbox {
	t.Helper()
	cfg := BatchConfig{
		URL:      srv.URL + "/api/ecmp/events",
		Envelope: func(e string) map[string]any { return map[string]any{"agent_id": "a1", "epoch": e} },
		Rand:     func() float64 { return 1 },
	}
	if mut != nil {
		mut(&cfg)
	}
	o, err := NewBatch(db, srv.Client(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func enq(t *testing.T, o *BatchOutbox, n int, protected bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := o.Enqueue("k", []byte(fmt.Sprintf(`{"kind":"k","i":%d}`, i)), protected); err != nil {
			t.Fatal(err)
		}
	}
}

func rowsLeft(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM batch_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBatchWireIsOneEnvelopeWithStampedSeq(t *testing.T) {
	c := &ctl{}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 3, false)
	if !o.drain(context.Background()) {
		t.Fatal("drain reported failure")
	}
	if c.posts != 1 {
		t.Fatalf("posts = %d, want 1 (one POST for N events)", c.posts)
	}
	want := fmt.Sprintf(`{"agent_id":"a1","epoch":"%s","events":[{"seq":1,"kind":"k","i":0},{"seq":2,"kind":"k","i":1},{"seq":3,"kind":"k","i":2}]}`, o.Epoch())
	if c.bodies[0] != want {
		t.Fatalf("body\n got %s\nwant %s", c.bodies[0], want)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("acked rows not deleted")
	}
}

func TestBatchOfOneAndEmptyObjectAndNilEnvelope(t *testing.T) {
	c := &ctl{}
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) { cfg.Envelope = nil })
	if _, err := o.Enqueue("k", []byte(`{}`), false); err != nil {
		t.Fatal(err)
	}
	o.drain(context.Background())
	if c.bodies[0] != `{"events":[{"seq":1}]}` {
		t.Fatalf("body = %s", c.bodies[0])
	}
}

func TestSeqNeverReusedAfterQueueEmpties(t *testing.T) {
	c := &ctl{}
	o, _ := newBatchT(t, c, nil)
	enq(t, o, 2, false)
	o.drain(context.Background())
	enq(t, o, 1, false)
	o.drain(context.Background())
	if got := c.delivery; len(got) != 3 || got[2] != 3 {
		t.Fatalf("delivered seqs %v, want 1,2,3 (a reused id would dedupe as a duplicate and be lost)", got)
	}
	if o.LastSeq() != 3 {
		t.Fatalf("LastSeq = %d", o.LastSeq())
	}
}

func TestEpochStableOnSameDBAndFreshOnNewDB(t *testing.T) {
	c := &ctl{seen: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	db1, db2 := newDB(t), newDB(t)
	a, b, a2 := batchOn(t, db1, srv, nil), batchOn(t, db2, srv, nil), batchOn(t, db1, srv, nil)
	if a.Epoch() == "" || a.Epoch() != a2.Epoch() {
		t.Fatalf("epoch changed across reopen: %q vs %q", a.Epoch(), a2.Epoch())
	}
	if a.Epoch() == b.Epoch() {
		t.Fatal("a recreated DB must get a new epoch")
	}
}

func TestBatchLimitsEventsAndBytesOldestFirst(t *testing.T) {
	c := &ctl{}
	o, _ := newBatchT(t, c, nil)
	enq(t, o, 250, false)
	o.drain(context.Background())
	if c.posts != 3 {
		t.Fatalf("posts = %d, want 3 (100+100+50)", c.posts)
	}
	for i, s := range c.delivery {
		if s != int64(i+1) {
			t.Fatalf("not oldest-first at %d: %d", i, s)
		}
	}
	// byte bound: tiny MaxBytes forces one event per request
	c2 := &ctl{}
	o2, _ := newBatchT(t, c2, func(cfg *BatchConfig) { cfg.MaxBytes = 120 })
	enq(t, o2, 5, false)
	o2.drain(context.Background())
	if c2.posts != 5 {
		t.Fatalf("byte-bounded posts = %d, want 5", c2.posts)
	}
	for _, b := range c2.bodies {
		if len(b) > 120 {
			t.Fatalf("request %d bytes exceeds MaxBytes 120", len(b))
		}
	}
}

func TestAckMustCoverTheWholeBatch(t *testing.T) {
	for name, resp := range map[string]string{
		"shortfall":   `{"accepted":2,"duplicate":0}`,
		"unparseable": `not json`,
		"empty":       ``,
		"no-counts":   `{"ok":true}`,
		"overcount":   `{"accepted":3,"duplicate":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
				io.WriteString(w, resp)
				return true
			}}
			o, db := newBatchT(t, c, nil)
			enq(t, o, 3, false)
			if o.drain(context.Background()) {
				t.Fatal("a shortfall must report failure (back off)")
			}
			if rowsLeft(t, db) != 3 {
				t.Fatal("rows deleted on an ack that did not cover the batch")
			}
			var attempts int
			db.QueryRow(`SELECT MIN(attempts) FROM batch_events`).Scan(&attempts)
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
		})
	}
}

func TestDuplicatesCountAsAcked(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		io.WriteString(w, `{"accepted":1,"duplicate":2}`)
		return true
	}}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 3, false)
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("accepted+duplicate covering the batch must delete it")
	}
}

func TestRejectedRowsTerminalVersusRetryable(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		io.WriteString(w, `{"accepted":1,"duplicate":0,"rejected":[{"seq":2,"code":"bad_event","retryable":false},{"seq":3,"code":"store_busy","retryable":true}]}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) } })
	enq(t, o, 3, false)
	if o.drain(context.Background()) {
		t.Fatal("a retryable per-row refusal must back off")
	}
	var left []int64
	rs, _ := db.Query(`SELECT id FROM batch_events ORDER BY id`)
	for rs.Next() {
		var id int64
		rs.Scan(&id)
		left = append(left, id)
	}
	rs.Close()
	if len(left) != 1 || left[0] != 3 {
		t.Fatalf("left = %v, want only the retryable seq 3", left)
	}
	if len(dead) != 1 || dead[0].Seq != 2 || dead[0].Code != "bad_event" {
		t.Fatalf("dead = %+v", dead)
	}
	if o.Stats().Dead != 1 {
		t.Fatalf("dead counter = %d", o.Stats().Dead)
	}
}

func TestRejectedSeqOutsideBatchIsAShortfall(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		io.WriteString(w, `{"accepted":1,"duplicate":0,"rejected":[{"seq":99,"code":"x","retryable":false}]}`)
		return true
	}}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 2, false)
	if o.drain(context.Background()) || rowsLeft(t, db) != 2 {
		t.Fatal("a rejection naming a seq outside the batch must not delete anything")
	}
}

func TestStatusClassification(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantOK     bool
		wantRows   int
		wantDeadN  int
		wantPosts  int
		wantCode   string
		batchSize  int
		bigPayload bool
	}{
		{name: "500 retries", status: 500, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
		{name: "503 retries", status: 503, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
		{name: "429 retries", status: 429, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
		{name: "408 retries", status: 408, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
		{name: "4xx retryable true keeps", status: 422, body: `{"code":"x","retryable":true}`, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
		{name: "4xx refusal bisects then dead-letters each", status: 400, body: `{"code":"batch_invalid"}`, wantOK: true, wantRows: 0, wantDeadN: 3, wantPosts: 4, wantCode: "batch_invalid", batchSize: 3},
		{name: "4xx no code", status: 404, wantOK: true, wantRows: 0, wantDeadN: 3, wantPosts: 4, wantCode: "refused", batchSize: 3},
		{name: "413 of one dead-letters", status: 413, body: `{"code":"batch_too_large"}`, wantOK: true, wantRows: 0, wantDeadN: 1, wantPosts: 1, wantCode: "payload_too_large", batchSize: 1},
		{name: "302 retries", status: 302, wantOK: false, wantRows: 3, wantPosts: 1, batchSize: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
				return true
			}}
			var dead []DeadLetter
			o, db := newBatchT(t, c, func(cfg *BatchConfig) { cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) } })
			enq(t, o, tc.batchSize, false)
			if got := o.drain(context.Background()); got != tc.wantOK {
				t.Fatalf("drain ok = %v, want %v", got, tc.wantOK)
			}
			if rowsLeft(t, db) != tc.wantRows || len(dead) != tc.wantDeadN || c.posts != tc.wantPosts {
				t.Fatalf("rows=%d dead=%d posts=%d", rowsLeft(t, db), len(dead), c.posts)
			}
			if tc.wantCode != "" && dead[0].Code != tc.wantCode {
				t.Fatalf("code = %q", dead[0].Code)
			}
		})
	}
}

func TestSplitOn413ThenRecoversBatchSize(t *testing.T) {
	c := &ctl{respond: func(c *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		if len(evs) > 10 {
			w.WriteHeader(413)
			return true
		}
		return false
	}}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 40, false)
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("did not converge by splitting")
	}
	for i, s := range c.delivery {
		if s != int64(i+1) {
			t.Fatalf("order broken by split at %d: %d", i, s)
		}
	}
	if o.limit < 10 {
		t.Fatalf("limit stuck at %d after successes", o.limit)
	}
}

func TestPoisonEventDoesNotWedgeTheQueue(t *testing.T) {
	c := &ctl{respond: func(c *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		for _, e := range evs {
			if e["poison"] == true {
				w.WriteHeader(400)
				io.WriteString(w, `{"code":"bad"}`)
				return true
			}
		}
		return false
	}}
	o, db := newBatchT(t, c, nil)
	o.Enqueue("k", []byte(`{"poison":true}`), false)
	enq(t, o, 2, false)
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("queue wedged behind a refused batch")
	}
	if fmt.Sprint(c.delivery) != "[2 3]" || o.Stats().Dead != 1 {
		t.Fatalf("delivered %v dead %d: good events must survive the poison one", c.delivery, o.Stats().Dead)
	}
}

func TestCrashBetweenPersistAndPushDeliversAfterRestart(t *testing.T) {
	c := &ctl{seen: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	db := newDB(t)
	o1 := batchOn(t, db, srv, nil)
	enq(t, o1, 3, true) // persisted, process "dies" before any push
	o2 := batchOn(t, db, srv, nil)
	if o2.Epoch() != o1.Epoch() {
		t.Fatal("epoch must survive restart")
	}
	o2.drain(context.Background())
	if len(c.delivery) != 3 || rowsLeft(t, db) != 0 {
		t.Fatalf("delivered %v, left %d", c.delivery, rowsLeft(t, db))
	}
}

func TestCrashBetweenPushAndAckReplaysAsDuplicate(t *testing.T) {
	c := &ctl{seen: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	db := newDB(t)
	// First process: the controller stores the batch, the agent never sees the ack.
	cut := &http.Client{Transport: roundTripFn(func(r *http.Request) (*http.Response, error) {
		resp, err := srv.Client().Transport.RoundTrip(r)
		if err == nil {
			resp.Body.Close()
		}
		return nil, errors.New("connection reset after the controller processed it")
	})}
	o1, err := NewBatch(db, cut, BatchConfig{URL: srv.URL, Rand: func() float64 { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	enq(t, o1, 3, true)
	if o1.drain(context.Background()) {
		t.Fatal("a cut connection must report failure")
	}
	if rowsLeft(t, db) != 3 || len(c.delivery) != 3 {
		t.Fatalf("rows=%d delivered=%d", rowsLeft(t, db), len(c.delivery))
	}
	// Restart: replay arrives as duplicates, rows go, nothing is double-applied or lost.
	o2, err := NewBatch(db, srv.Client(), BatchConfig{URL: srv.URL, Rand: func() float64 { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	if !o2.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("replay did not drain")
	}
	if len(c.delivery) != 3 {
		t.Fatalf("applied %d times, want exactly 3", len(c.delivery))
	}
}

type roundTripFn func(*http.Request) (*http.Response, error)

func (f roundTripFn) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCrashMidTransactionLeavesNoPartialDelete(t *testing.T) {
	c := &ctl{}
	o, db := newBatchT(t, c, nil)
	enq(t, o, 3, false)
	// A failing delete mid-transaction must roll back: drop the table's trigger target by
	// making the second delete fail via a trigger.
	if _, err := db.Exec(`CREATE TRIGGER boom BEFORE DELETE ON batch_events WHEN old.id = 2 BEGIN SELECT RAISE(ABORT,'boom'); END`); err != nil {
		t.Fatal(err)
	}
	if o.drain(context.Background()) {
		t.Fatal("a failed delete must report failure")
	}
	if rowsLeft(t, db) != 3 {
		t.Fatalf("partial delete: %d rows left, want all 3 (rolled back)", rowsLeft(t, db))
	}
}

func TestPruneAgeRowsAndHardCapProtectRecoveryEvents(t *testing.T) {
	var dead []DeadLetter
	now := time.Unix(1_000_000, 0)
	c := &ctl{}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		cfg.MaxRows, cfg.HardRows, cfg.MaxAge = 4, 6, time.Hour
		cfg.Now = func() time.Time { return now }
		cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) }
	})
	// Two old unprotected + one old protected, then 5 fresh unprotected.
	o.Enqueue("k", []byte(`{"a":1}`), false)
	o.Enqueue("k", []byte(`{"a":2}`), false)
	o.Enqueue("rec", []byte(`{"a":3}`), true)
	now = now.Add(2 * time.Hour)
	enq(t, o, 5, false)
	o.prune()
	var ids []int64
	rs, _ := db.Query(`SELECT id FROM batch_events ORDER BY id`)
	for rs.Next() {
		var id int64
		rs.Scan(&id)
		ids = append(ids, id)
	}
	rs.Close()
	// age drops 1,2 (not protected 3); rows bound 4: 6 left -> drop oldest unprotected 4,5 -> 3,6,7,8
	if fmt.Sprint(ids) != "[3 6 7 8]" {
		t.Fatalf("after prune ids = %v, want [3 6 7 8]", ids)
	}
	codes := ""
	for _, d := range dead {
		codes += d.Code + ","
	}
	if codes != "dropped_age,dropped_age,dropped_rows,dropped_rows," {
		t.Fatalf("codes = %s", codes)
	}
	// Hard cap: protected events may only fall here.
	for i := 0; i < 8; i++ {
		o.Enqueue("rec", []byte(`{"p":1}`), true)
	}
	dead = nil
	o.prune()
	if rowsLeft(t, db) != 6 {
		t.Fatalf("rows = %d, want hard cap 6", rowsLeft(t, db))
	}
	for _, d := range dead {
		if d.Kind == "rec" && d.Code != "dropped_hard_cap" {
			t.Fatalf("protected event dropped by %q, only the hard cap may", d.Code)
		}
	}
}

func TestEnqueueValidation(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) { cfg.MaxBytes = 300 })
	for _, p := range []string{``, `[]`, `"x"`, `{bad`, `{"seq":1}`, `{"a":{"seq":1}}`} {
		if _, err := o.Enqueue("k", []byte(p), false); !errors.Is(err, ErrBadPayload) {
			t.Fatalf("payload %q: err = %v, want ErrBadPayload", p, err)
		}
	}
	if _, err := o.Enqueue("k", []byte(`{"a":"`+strings.Repeat("x", 400)+`"}`), false); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
}

func TestBackoffIsCappedExponentialFullJitter(t *testing.T) {
	o, _ := newBatchT(t, &ctl{}, func(cfg *BatchConfig) {
		cfg.BackoffBase, cfg.BackoffMax = time.Second, 8*time.Second
	})
	o.cfg.Rand = func() float64 { return 1 }
	for n, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 5: 8 * time.Second, 500: 8 * time.Second} {
		if got := o.backoff(n); got != want {
			t.Fatalf("backoff(%d) = %v, want %v", n, got, want)
		}
	}
	o.cfg.Rand = func() float64 { return 0 }
	if o.backoff(3) != 0 {
		t.Fatal("full jitter must reach 0")
	}
	o.cfg.Rand = func() float64 { return 0.5 }
	if o.backoff(3) != 2*time.Second {
		t.Fatalf("backoff(3)@0.5 = %v", o.backoff(3))
	}
}

func TestLoopBurstIsFewPostsAndPokeDuringBackoffIsIgnored(t *testing.T) {
	var fail = true
	var mu sync.Mutex
	c := &ctl{respond: func(c *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			w.WriteHeader(503)
			return true
		}
		return false
	}}
	o, db := newBatchT(t, c, func(cfg *BatchConfig) {
		// 1.5 s, not 400 ms: 20 fsynced enqueues on a loaded host can outlast a short backoff
		// and turn "a poke cut the backoff short" into a false report (seen at baseline v0.9.0).
		cfg.BackoffBase, cfg.BackoffMax = 1500*time.Millisecond, 1500*time.Millisecond
		cfg.DrainInterval = time.Hour
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	defer o.Close()
	enq(t, o, 1, false)
	time.Sleep(100 * time.Millisecond)
	enq(t, o, 20, false) // pokes during backoff must not send
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	during := c.posts
	c.mu.Unlock()
	if during != 1 {
		t.Fatalf("posts during backoff = %d, want 1 (a poke must not cut the backoff short)", during)
	}
	mu.Lock()
	fail = false
	mu.Unlock()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && rowsLeft(t, db) != 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("did not drain after the controller recovered")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.delivery) != 21 {
		t.Fatalf("delivered %d events, want 21 exactly once", len(c.delivery))
	}
	if c.posts > 4 {
		t.Fatalf("posts = %d for 21 events: the burst must coalesce", c.posts)
	}
}

func TestOnSentFiresOnlyOnAckedTraffic(t *testing.T) {
	n := 0
	c := &ctl{}
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) { cfg.OnSent = func() { n++ } })
	enq(t, o, 2, false)
	o.drain(context.Background())
	if n != 1 {
		t.Fatalf("OnSent calls = %d", n)
	}
	c.respond = func(*ctl, http.ResponseWriter, []map[string]any) bool { return false }
	o.drain(context.Background()) // empty queue: no traffic, no call
	if n != 1 {
		t.Fatal("OnSent fired without traffic")
	}
}
