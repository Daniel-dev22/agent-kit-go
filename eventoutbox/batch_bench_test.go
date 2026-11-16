package eventoutbox

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Pi-class numbers: run with taskset -c 3, GOMAXPROCS=1, -count 10 -benchmem, TMPDIR on the real disk.

func benchDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(b.TempDir(), "b.sqlite")+
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	return db
}

func ackAll(calls *atomic.Int64, n int) http.RoundTripper {
	body := `{"accepted":` + strconv.Itoa(n) + `,"duplicate":0}`
	return roundTripFn(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
}

var benchPayload = []byte(`{"kind":"node_removed","vip":"192.168.30.51","node_ip":"192.168.30.40","node_name":"kd-nuc02","reason":"probe_failed","consecutive_fails":3}`)

func benchBatch(b *testing.B, calls *atomic.Int64, n int) *BatchOutbox {
	b.Helper()
	o, err := NewBatch(benchDB(b), &http.Client{Transport: ackAll(calls, n)}, BatchConfig{
		URL: "http://ctl/api/ecmp/events", Rand: func() float64 { return 1 },
		Envelope: func(e string) map[string]any { return map[string]any{"agent_id": "kd-nas01", "epoch": e, "site": "kd"} },
	})
	if err != nil {
		b.Fatal(err)
	}
	return o
}

func BenchmarkBatchEnqueue(b *testing.B) {
	var calls atomic.Int64
	o := benchBatch(b, &calls, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.Enqueue("node_removed", benchPayload, false); err != nil {
			b.Fatal(err)
		}
		if i%1000 == 999 { // keep the table bounded without timing the delete
			b.StopTimer()
			o.db.Exec(`DELETE FROM batch_events`)
			b.StartTimer()
		}
	}
}

func BenchmarkBatchBuild100(b *testing.B) {
	var calls atomic.Int64
	o := benchBatch(b, &calls, 100)
	for i := 0; i < 100; i++ {
		o.Enqueue("k", benchPayload, false)
	}
	rows, _ := o.load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.build(rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBatchDrain100 is one full pass: load 100, build, POST (in-process transport), delete 100 rows.
func BenchmarkBatchDrain100(b *testing.B) {
	var calls atomic.Int64
	o := benchBatch(b, &calls, 100)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 100; j++ {
			o.Enqueue("k", benchPayload, false)
		}
		b.StartTimer()
		if !o.drain(context.Background()) {
			b.Fatal("drain failed")
		}
	}
	if calls.Load() != int64(b.N) {
		b.Fatalf("%d POSTs for %d passes of 100 events: want exactly one per pass", calls.Load(), b.N)
	}
}

// BenchmarkLegacyEnqueuePush100 is the "before": 100 events through the per-event Outbox = 100 POSTs.
func BenchmarkLegacyEnqueuePush100(b *testing.B) {
	var calls atomic.Int64
	db := benchDB(b)
	o, err := New(db, &http.Client{Transport: ackAll(&calls, 1)}, func(id string) string { return "http://ctl/jobs/" + id })
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			o.Enqueue("j", "e", benchPayload)
		}
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
			var n int
			db.QueryRow(`SELECT COUNT(*) FROM pending_events`).Scan(&n)
			if n == 0 {
				break
			}
			time.Sleep(200 * time.Microsecond)
		}
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "posts/100ev")
}
