package blobqueue

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const walDSN = "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"

func openDB(t testing.TB, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+walDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func newQ(t testing.TB, cfg Config) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "q.sqlite")
	if cfg.FloorBytes == 0 && cfg.FloorPercent == 0 {
		cfg.FloorBytes, cfg.FloorPercent = -1, -1 // floor off unless the test is about it
	}
	q, err := New(openDB(t, path), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return q, path
}

func gz(n int, fill byte) []byte { return bytes.Repeat([]byte{fill, 0x00, 0xff, 0x1f, 0x8b}, n) }

func enq(t testing.TB, q *Queue, n int, size int) []uint64 {
	t.Helper()
	var seqs []uint64
	for i := 0; i < n; i++ {
		s, err := q.Enqueue(context.Background(), Item{Signal: "logs", ContentType: "application/x-protobuf", ContentEncoding: "gzip", Body: gz(size, byte(i))})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		seqs = append(seqs, s)
	}
	return seqs
}

// recorder is a Sender that records every delivered (epoch, seq) and acks per policy.
type recorder struct {
	mu     sync.Mutex
	got    []Item
	epoch  string
	frames int
	ack    func(items []Item) (Ack, error)
}

func (r *recorder) send(_ context.Context, frame []byte) (Ack, error) {
	ep, items, err := DecodeFrame(frame, Limits{})
	if err != nil {
		return Ack{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frames++
	r.epoch = ep
	cp := make([]Item, len(items))
	for i, it := range items {
		it.Body = append([]byte(nil), it.Body...)
		cp[i] = it
	}
	if r.ack != nil {
		a, err := r.ack(cp)
		if err != nil {
			return a, err
		}
		r.got = append(r.got, cp...)
		return a, nil
	}
	r.got = append(r.got, cp...)
	return NewAck(len(items), 0), nil
}

func TestEnqueueDrainRoundTripBinary(t *testing.T) {
	q, _ := newQ(t, Config{})
	want := enq(t, q, 5, 100)
	rec := &recorder{}
	n, err := q.drainOnce(context.Background(), rec.send)
	if err != nil || n != 5 {
		t.Fatalf("drain n=%d err=%v", n, err)
	}
	for i, it := range rec.got {
		if it.Seq != want[i] || it.Signal != "logs" || it.ContentEncoding != "gzip" || !bytes.Equal(it.Body, gz(100, byte(i))) {
			t.Fatalf("item %d mismatch: %+v", i, it)
		}
	}
	if rec.epoch != q.Epoch() {
		t.Fatalf("epoch %q != %q", rec.epoch, q.Epoch())
	}
	if s := q.Stats(context.Background()); s.Rows != 0 || s.Bytes != 0 || s.Sent != 5 {
		t.Fatalf("stats after ack: %+v", s)
	}
}

// Seq must never be reused after the table drains empty: the receiver dedupes on (epoch, seq).
func TestSeqNeverReusedAfterDrain(t *testing.T) {
	q, _ := newQ(t, Config{})
	first := enq(t, q, 3, 10)
	if _, err := q.drainOnce(context.Background(), (&recorder{}).send); err != nil {
		t.Fatal(err)
	}
	next := enq(t, q, 1, 10)
	if next[0] <= first[2] {
		t.Fatalf("seq reused: %d after %d", next[0], first[2])
	}
}

func TestReopenKeepsRowsAndEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+walDSN)
	if err != nil {
		t.Fatal(err)
	}
	q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1})
	if err != nil {
		t.Fatal(err)
	}
	enq(t, q, 4, 50)
	epoch := q.Epoch()
	db.Close()
	q2, err := New(openDB(t, path), Config{FloorBytes: -1, FloorPercent: -1})
	if err != nil {
		t.Fatal(err)
	}
	if q2.Epoch() != epoch {
		t.Fatalf("epoch changed %q -> %q", epoch, q2.Epoch())
	}
	if s := q2.Stats(context.Background()); s.Rows != 4 {
		t.Fatalf("rows after reopen = %d", s.Rows)
	}
	next := enq(t, q2, 1, 5)
	if next[0] != 5 {
		t.Fatalf("seq after reopen = %d, want 5", next[0])
	}
}

func TestDurabilityReadBack(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name, dsn, want string
		wantErr         error
	}{
		{"full", "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)", "full", nil},
		{"normal", "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", "normal", nil},
		{"not wal", "?_pragma=journal_mode(DELETE)", "", ErrNotWAL},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, c.name+".sqlite")+c.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1})
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			if err == nil && q.Stats(context.Background()).Durability != c.want {
				t.Fatalf("durability = %q, want %q", q.Stats(context.Background()).Durability, c.want)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	q, _ := newQ(t, Config{MaxBodyBytes: 10, Signals: []string{"logs"}})
	cases := []struct {
		name string
		it   Item
		err  error
	}{
		{"too large", Item{Signal: "logs", Body: make([]byte, 11)}, ErrTooLarge},
		{"at cap ok", Item{Signal: "logs", Body: make([]byte, 10)}, nil},
		{"reserved", Item{Signal: SignalLoss}, errors.New("x")},
		{"empty signal", Item{}, errors.New("x")},
		{"not allowed", Item{Signal: "metrics"}, errors.New("x")},
	}
	for _, c := range cases {
		_, err := q.Enqueue(context.Background(), c.it)
		if (err == nil) != (c.err == nil) || (c.err == ErrTooLarge && !errors.Is(err, ErrTooLarge)) {
			t.Fatalf("%s: err = %v", c.name, err)
		}
	}
	q.Close()
	if _, err := q.Enqueue(context.Background(), Item{Signal: "logs"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}

func marker(t *testing.T, q *Queue) (LossMarker, int) {
	t.Helper()
	rs, err := q.db.Query(`SELECT body FROM blobq_items WHERE signal=? AND protected=1`, SignalLoss)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var m LossMarker
	n := 0
	for rs.Next() {
		var b []byte
		if err := rs.Scan(&b); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		n++
	}
	return m, n
}

func TestRowCapShedsOldestAndCounts(t *testing.T) {
	q, _ := newQ(t, Config{MaxRows: 5})
	enq(t, q, 8, 10)
	s := q.Stats(context.Background())
	m, n := marker(t, q)
	// MaxRows bounds ALL rows including the protected marker (transiently cap+1 on the enqueue that
	// first creates it): 8 enqueued, 4 shed, 4 live + the marker = 5.
	if n != 1 || m.Rows != 4 || m.RowCap != 4 || m.FirstSeq != 1 || m.LastSeq != 4 {
		t.Fatalf("marker = %+v (n=%d)", m, n)
	}
	if s.ShedRows != 4 || s.Rows != 5 {
		t.Fatalf("stats = %+v", s)
	}
	rec := &recorder{}
	if _, err := q.drainOnce(context.Background(), rec.send); err != nil {
		t.Fatal(err)
	}
	live := 0
	for _, it := range rec.got {
		if it.Signal == "logs" {
			live++
			if it.Seq <= 4 {
				t.Fatalf("shed row %d was delivered", it.Seq)
			}
		}
	}
	if live != 4 || len(rec.got) != 5 {
		t.Fatalf("delivered %d live of %d", live, len(rec.got))
	}
}

func TestByteCapSheds(t *testing.T) {
	q, _ := newQ(t, Config{MaxBytes: 1000})
	enq(t, q, 10, 40) // each ~200 B body + metadata
	s := q.Stats(context.Background())
	if s.ShedBytes == 0 || s.Bytes > 1000+600 { // cap + room for the marker
		t.Fatalf("byte cap not enforced: %+v", s)
	}
	m, _ := marker(t, q)
	if m.ByteCap != s.ShedBytes || m.Bytes != s.ShedByte {
		t.Fatalf("marker %+v disagrees with stats %+v", m, s)
	}
}

func TestAgeShed(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	q, _ := newQ(t, Config{MaxAge: time.Hour, Now: func() time.Time { return now }})
	enq(t, q, 3, 10)
	now = now.Add(30 * time.Minute)
	enq(t, q, 2, 10)
	now = now.Add(45 * time.Minute) // the first three are now 75 min old, the last two 45
	q.Maintain(context.Background())
	s := q.Stats(context.Background())
	if s.ShedAge != 3 || s.Rows != 3 { // 2 live + marker
		t.Fatalf("stats = %+v", s)
	}
	m, _ := marker(t, q)
	if m.Age != 3 || m.OldestNs == 0 || m.NewestNs < m.OldestNs {
		t.Fatalf("marker = %+v", m)
	}
}

// One marker row, coalesced across many sheds and updated in place.
func TestMarkerCoalesced(t *testing.T) {
	q, _ := newQ(t, Config{MaxRows: 2})
	enq(t, q, 12, 5)
	m, n := marker(t, q)
	if n != 1 || m.Rows != 11 { // cap 2 incl. the marker: 12 enqueued, 1 live + marker survive
		t.Fatalf("marker n=%d %+v", n, m)
	}
}

// Rows the drainer holds in flight are never shed.
func TestInflightRowsNeverShed(t *testing.T) {
	q, _ := newQ(t, Config{MaxRows: 3})
	enq(t, q, 3, 5)
	rows, err := q.loadBatch(context.Background()) // rows 1..3 in flight
	if err != nil || len(rows) != 3 {
		t.Fatalf("load: %v %d", err, len(rows))
	}
	enq(t, q, 3, 5) // over the cap, but the only sheddable rows (1..3) are in flight
	var have int
	if err := q.db.QueryRow(`SELECT COUNT(*) FROM blobq_items WHERE seq<=3`).Scan(&have); err != nil || have != 3 {
		t.Fatalf("in-flight rows were shed: %d %v", have, err)
	}
	q.release()
}

func fileSize(t testing.TB, path string) int64 {
	t.Helper()
	var total int64
	for _, p := range []string{path, path + "-wal"} {
		if fi, err := os.Stat(p); err == nil {
			total += fi.Size()
		}
	}
	return total
}

func TestFloorShedsOldestAndReclaimsFile(t *testing.T) {
	// A fake disk of `limit` bytes whose only occupant is the queue file: free space moves only if
	// shedding really shrinks the file (rows deleted but never vacuumed would leave it flat).
	var limit atomic.Int64
	limit.Store(1 << 40)
	var path string
	st := func(string) (uint64, uint64, error) {
		return uint64(limit.Load() - fileSize(t, path)), 1 << 41, nil
	}
	var q *Queue
	q, path = newQ(t, Config{Statfs: st, FloorBytes: 1 << 20, FloorPercent: -1, MaxRows: -1, MaxBytes: -1})
	enq(t, q, 60, 20_000) // ~100 KB bodies, ~6 MB
	// Fold the WAL into the main file so `before` is the main file alone: a WAL truncation by itself
	// must not be able to pass this test, only a real incremental_vacuum shrinks the MAIN file.
	if _, err := q.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	before := fileSize(t, path)
	limit.Store(before + 1<<20 + 50) // 50 B above the floor: the next ~100 KB row needs a deficit
	if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: gz(20_000, 9)}); err != nil {
		t.Fatalf("enqueue under pressure: %v", err)
	}
	st2 := q.Stats(context.Background())
	if st2.ShedFloor == 0 {
		t.Fatalf("nothing shed at the floor: %+v", st2)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() >= before {
		t.Fatalf("main db file did not shrink: before %d after %v (%v): shed rows freed nothing", before, fi, err)
	}
	var free int
	if err := q.db.QueryRow(`PRAGMA freelist_count`).Scan(&free); err != nil || free != 0 {
		t.Fatalf("freelist_count = %d (%v): freed pages were never returned to the filesystem", free, err)
	}
	m, _ := marker(t, q)
	if m.Floor != st2.ShedFloor || m.FirstSeq != 1 {
		t.Fatalf("marker %+v vs stats %+v", m, st2)
	}
}

// When shedding empties the queue and the disk is still below the floor, something else fills it:
// refuse, count it, and fold the refusals into the marker once a row can be written again.
func TestFloorRefusesWhenNothingLeftToShed(t *testing.T) {
	var free atomic.Uint64
	free.Store(100)
	st := func(string) (uint64, uint64, error) { return free.Load(), 1000, nil }
	q, _ := newQ(t, Config{Statfs: st, FloorBytes: 500, FloorPercent: -1})
	for i := 0; i < 3; i++ {
		if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: []byte("x")}); !errors.Is(err, ErrDiskFloor) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if s := q.Stats(context.Background()); s.Refused != 3 || s.Rows != 0 {
		t.Fatalf("stats = %+v", s)
	}
	free.Store(10_000)
	if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if m, n := marker(t, q); n != 1 || m.Refused != 3 {
		t.Fatalf("refusals not folded into the marker: n=%d %+v", n, m)
	}
}

func TestFloorStricterOfPercentAndBytes(t *testing.T) {
	q, _ := newQ(t, Config{FloorBytes: 100, FloorPercent: 10})
	if got := q.floorBytes(10_000); got != 1000 {
		t.Fatalf("percent should win: %d", got)
	}
	if got := q.floorBytes(500); got != 100 {
		t.Fatalf("bytes should win: %d", got)
	}
	q2, _ := newQ(t, Config{FloorBytes: 100, FloorPercent: -1})
	if got := q2.floorBytes(1 << 40); got != 100 {
		t.Fatalf("percent disabled: %d", got)
	}
}

func TestStatfsErrorFailsOpenAndIsReported(t *testing.T) {
	q, _ := newQ(t, Config{Statfs: func(string) (uint64, uint64, error) { return 0, 0, errors.New("boom") }, FloorBytes: 1, FloorPercent: -1})
	if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: []byte("x")}); err != nil {
		t.Fatalf("unmeasurable disk must not stop intake: %v", err)
	}
	if s := q.Stats(context.Background()); s.LastError == "" {
		t.Fatal("statfs failure not surfaced")
	}
}

func TestAckClassification(t *testing.T) {
	q, _ := newQ(t, Config{})
	enq(t, q, 4, 5) // seqs 1..4
	var dead []DeadLetter
	q.cfg.OnDead = func(d DeadLetter) { dead = append(dead, d) }
	rec := &recorder{ack: func([]Item) (Ack, error) {
		return NewAck(1, 1, Rejection{Seq: 2, Code: "bad", Retryable: false}, Rejection{Seq: 4, Code: "busy", Retryable: true}), nil
	}}
	n, err := q.drainOnce(context.Background(), rec.send)
	if err == nil || n != 3 {
		t.Fatalf("want 3 deleted and a retryable error, got n=%d err=%v", n, err)
	}
	if len(dead) != 1 || dead[0].Seq != 2 || dead[0].Code != "bad" {
		t.Fatalf("dead = %+v", dead)
	}
	s := q.Stats(context.Background())
	if s.Rows != 1 || s.Dead != 1 || s.Sent != 2 {
		t.Fatalf("stats = %+v", s)
	}
	var left uint64
	if err := q.db.QueryRow(`SELECT seq FROM blobq_items`).Scan(&left); err != nil || left != 4 {
		t.Fatalf("held row = %d %v", left, err)
	}
}

func TestSenderErrorHoldsEverything(t *testing.T) {
	q, _ := newQ(t, Config{})
	enq(t, q, 3, 5)
	n, err := q.drainOnce(context.Background(), func(context.Context, []byte) (Ack, error) { return Ack{}, errors.New("503") })
	if err == nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if s := q.Stats(context.Background()); s.Rows != 3 {
		t.Fatalf("rows = %d", s.Rows)
	}
}

func TestBatchLimits(t *testing.T) {
	q, _ := newQ(t, Config{MaxBatchItems: 3, MaxBatchBytes: 1000})
	enq(t, q, 7, 10) // 50 B bodies
	rec := &recorder{}
	for {
		n, err := q.drainOnce(context.Background(), rec.send)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if rec.frames != 3 || len(rec.got) != 7 { // 3+3+1
		t.Fatalf("frames=%d items=%d", rec.frames, len(rec.got))
	}
	q2, _ := newQ(t, Config{MaxBatchItems: 10, MaxBatchBytes: 100})
	enq(t, q2, 3, 100) // 500 B bodies each over the byte limit: one per frame, never zero
	rec2 := &recorder{}
	for {
		n, _ := q2.drainOnce(context.Background(), rec2.send)
		if n == 0 {
			break
		}
	}
	if rec2.frames != 3 {
		t.Fatalf("oversize items: frames=%d", rec2.frames)
	}
}

func TestBackoff(t *testing.T) {
	cases := []struct {
		n    int
		r    float64
		want time.Duration
	}{
		{1, 1, time.Second}, {1, 0, 500 * time.Millisecond}, {2, 1, 2 * time.Second},
		{4, 1, 8 * time.Second}, {20, 1, time.Minute}, {20, 0, 30 * time.Second},
	}
	for _, c := range cases {
		if got := backoff(c.n, time.Second, time.Minute, c.r); got != c.want {
			t.Fatalf("backoff(%d,%v) = %v, want %v", c.n, c.r, got, c.want)
		}
	}
}

// Concurrent producers and the drainer: every body is delivered exactly once per (epoch, seq).
func TestConcurrentEnqueueAndRun(t *testing.T) {
	q, _ := newQ(t, Config{DrainInterval: 20 * time.Millisecond, MaxRows: -1, MaxBytes: -1})
	const producers, each = 8, 60
	ctx, cancel := context.WithCancel(context.Background())
	rec := &recorder{}
	done := make(chan struct{})
	go func() { _ = q.Run(ctx, rec.send); close(done) }()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: []byte(fmt.Sprintf("p%d-%d", p, i))}); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if s := q.Stats(context.Background()); s.Rows == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	rec.mu.Lock()
	defer rec.mu.Unlock()
	seen := map[uint64]int{}
	for _, it := range rec.got {
		seen[it.Seq]++
	}
	if len(seen) != producers*each {
		t.Fatalf("delivered %d distinct, want %d", len(seen), producers*each)
	}
	for s, c := range seen {
		if c != 1 {
			t.Fatalf("seq %d delivered %d times", s, c)
		}
	}
}

func TestRunBacksOffAndRecovers(t *testing.T) {
	var calls atomic.Int32
	q, _ := newQ(t, Config{DrainInterval: time.Hour, BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond, Rand: func() float64 { return 0 }})
	enq(t, q, 2, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	send := func(_ context.Context, frame []byte) (Ack, error) {
		if calls.Add(1) < 4 {
			return Ack{}, errors.New("down")
		}
		_, items, _ := DecodeFrame(frame, Limits{})
		return NewAck(len(items), 0), nil
	}
	go func() {
		for q.Stats(context.Background()).Rows != 0 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()
	if err := q.Run(ctx, send); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if calls.Load() != 4 {
		t.Fatalf("calls = %d, want 4 (3 failures then success)", calls.Load())
	}
}
