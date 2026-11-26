package blobqueue

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/eventoutbox"
)

// Weakest-node benchmarks (skill benchmark-on-the-weakest-node). Pi-class runs: a capped transient
// systemd unit, taskset -c 2,3 with GOMAXPROCS=2, TMPDIR on the SD card, -benchtime=Nx -count=5.
// Each enqueue benchmark reports ops/s (via ns/op), p50/p99 enqueue latency, bytes WRITTEN to disk
// per body (write amplification, from /proc/self/io), CPU ms per 1k bodies (rusage) and peak RSS.
// The BatchOutbox variants are the "before": the same gzip-sized body base64-wrapped in a JSON object,
// which is what carrying binary through eventoutbox would cost.

var benchSizes = []int{1 << 10, 16 << 10, 64 << 10}

func benchBodies(size int) [][]byte {
	r := rand.New(rand.NewPCG(1, uint64(size))) // incompressible, like gzip output
	out := make([][]byte, 64)
	for i := range out {
		b := make([]byte, size)
		for j := 0; j+8 <= len(b); j += 8 {
			v := r.Uint64()
			for k := 0; k < 8; k++ {
				b[j+k] = byte(v >> (8 * k))
			}
		}
		out[i] = b
	}
	return out
}

type meter struct {
	wall      time.Duration
	cpu       time.Duration
	wbytes    int64
	lat       []time.Duration
	n         int
	startWall time.Time
	startCPU  time.Duration
	startW    int64
}

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func procWriteBytes() int64 {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "write_bytes: "); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return 0
}

func peakRSSMiB() float64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, ln := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(ln, "VmHWM:"); ok {
			kb, _ := strconv.ParseFloat(strings.Fields(v)[0], 64)
			return kb / 1024
		}
	}
	return 0
}

func (m *meter) begin() {
	m.startWall, m.startCPU, m.startW = time.Now(), cpuTime(), procWriteBytes()
}

func (m *meter) end() {
	m.wall += time.Since(m.startWall)
	m.cpu += cpuTime() - m.startCPU
	m.wbytes += procWriteBytes() - m.startW
}

func (m *meter) report(b *testing.B) {
	if m.n == 0 {
		return
	}
	b.ReportMetric(float64(m.n)/m.wall.Seconds(), "bodies/s")
	if len(m.lat) > 0 {
		slices.Sort(m.lat)
		b.ReportMetric(float64(m.lat[len(m.lat)/2].Microseconds()), "p50_us")
		b.ReportMetric(float64(m.lat[len(m.lat)*99/100].Microseconds()), "p99_us")
	}
	b.ReportMetric(float64(m.wbytes)/float64(m.n), "disk_B/body")
	b.ReportMetric(float64(m.cpu.Milliseconds())*1000/float64(m.n), "cpu_ms/1k")
	b.ReportMetric(peakRSSMiB(), "rss_MiB")
}

func benchDB(b *testing.B, sync string) (*sql.DB, string) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "b.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous("+sync+")")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db, path
}

const chunk = 500

// enqueueLoop times each call; every `chunk` bodies it verifies the op count (rows written == bodies
// enqueued, no shed, no marker) and purges untimed so the table stays small.
func enqueueLoop(b *testing.B, one func(i int) error, rowsNow func() int64, purge func()) {
	var m meter
	m.lat = make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for done := 0; done < b.N; {
		n := min(chunk, b.N-done)
		m.begin()
		for i := 0; i < n; i++ {
			t0 := time.Now()
			if err := one(done + i); err != nil {
				b.Fatal(err)
			}
			m.lat = append(m.lat, time.Since(t0))
		}
		m.end()
		if got := rowsNow(); got != int64(n) {
			b.Fatalf("op-count: %d rows after %d enqueues (extra or shed rows)", got, n)
		}
		m.n += n
		done += n
		b.StopTimer()
		purge()
		b.StartTimer()
	}
	m.report(b)
}

func BenchmarkBlobEnqueue(b *testing.B) {
	for _, size := range benchSizes {
		for _, sync := range []string{"FULL", "NORMAL"} {
			b.Run(fmt.Sprintf("%dKiB/%s", size>>10, sync), func(b *testing.B) {
				db, _ := benchDB(b, sync)
				q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1, MaxRows: -1, MaxBytes: -1})
				if err != nil {
					b.Fatal(err)
				}
				bodies := benchBodies(size)
				b.SetBytes(int64(size))
				enqueueLoop(b,
					func(i int) error {
						_, err := q.Enqueue(context.Background(), Item{Signal: "logs", ContentType: "application/x-protobuf", ContentEncoding: "gzip", Body: bodies[i%len(bodies)]})
						return err
					},
					func() int64 { q.wmu.Lock(); defer q.wmu.Unlock(); return q.rows },
					func() {
						q.wmu.Lock()
						defer q.wmu.Unlock()
						_, _ = db.Exec(`DELETE FROM blobq_items`)
						q.rows, q.bytes = 0, 0
					})
			})
		}
	}
}

type rtFn func(*http.Request) (*http.Response, error)

func (f rtFn) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// BenchmarkBatchB64Enqueue is the baseline: eventoutbox.BatchOutbox carrying the same body as
// {"ct","enc":"b64","body"} (+33% bytes, JSON-object payload).
func BenchmarkBatchB64Enqueue(b *testing.B) {
	for _, size := range benchSizes {
		for _, sync := range []string{"FULL", "NORMAL"} {
			b.Run(fmt.Sprintf("%dKiB/%s", size>>10, sync), func(b *testing.B) {
				db, _ := benchDB(b, sync)
				o, err := eventoutbox.NewBatch(db, &http.Client{Transport: rtFn(func(*http.Request) (*http.Response, error) {
					return nil, fmt.Errorf("never drained in the enqueue benchmark")
				})}, eventoutbox.BatchConfig{URL: "http://ctl/x", MaxRows: 1 << 30, MaxAge: 1000 * time.Hour,
					Envelope: func(e string) map[string]any { return map[string]any{"epoch": e} }})
				if err != nil {
					b.Fatal(err)
				}
				bodies := benchBodies(size)
				payloads := make([][]byte, len(bodies))
				for i, body := range bodies {
					payloads[i], _ = json.Marshal(map[string]string{"ct": "application/x-protobuf", "enc": "b64", "body": base64.StdEncoding.EncodeToString(body)})
				}
				b.SetBytes(int64(size))
				enqueueLoop(b,
					func(i int) error { _, err := o.Enqueue("logs", payloads[i%len(payloads)], false); return err },
					func() int64 { var n int64; _ = db.QueryRow(`SELECT COUNT(*) FROM batch_events`).Scan(&n); return n },
					func() { _, _ = db.Exec(`DELETE FROM batch_events`) })
			})
		}
	}
}

const drainRows = 1000

func BenchmarkBlobDrain(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			db, _ := benchDB(b, "FULL")
			q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1, MaxRows: -1, MaxBytes: -1})
			if err != nil {
				b.Fatal(err)
			}
			bodies := benchBodies(size)
			ack := func(_ context.Context, frame []byte) (Ack, error) {
				_, items, err := DecodeFrame(frame, Limits{})
				return NewAck(len(items), 0), err
			}
			var m meter
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < drainRows; j++ {
					if _, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: bodies[j%len(bodies)]}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				m.begin()
				for {
					n, err := q.drainOnce(context.Background(), ack)
					if err != nil {
						b.Fatal(err)
					}
					if n == 0 {
						break
					}
					m.n += n
				}
				m.end()
			}
			m.report(b)
		})
	}
}

func BenchmarkBatchB64Drain(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			db, _ := benchDB(b, "FULL")
			ackAll := rtFn(func(r *http.Request) (*http.Response, error) {
				var req struct {
					Events []json.RawMessage `json:"events"`
				}
				raw, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(raw, &req)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"accepted":` + strconv.Itoa(len(req.Events)) + `,"duplicate":0}`))}, nil
			})
			bodies := benchBodies(size)
			var m meter
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				o, err := eventoutbox.NewBatch(db, &http.Client{Transport: ackAll}, eventoutbox.BatchConfig{
					URL: "http://ctl/x", MaxRows: 1 << 30, MaxBytes: 4 << 20, DrainInterval: time.Millisecond, MaxAge: 1000 * time.Hour,
					Envelope: func(e string) map[string]any { return map[string]any{"epoch": e} }})
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < drainRows; j++ {
					p, _ := json.Marshal(map[string]string{"ct": "application/x-protobuf", "enc": "b64", "body": base64.StdEncoding.EncodeToString(bodies[j%len(bodies)])})
					if _, err := o.Enqueue("logs", p, false); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				m.begin()
				ctx, cancel := context.WithCancel(context.Background())
				o.Start(ctx)
				for o.Stats().Pending > 0 {
					time.Sleep(time.Millisecond)
				}
				m.end()
				cancel()
				o.Close()
				m.n += drainRows
			}
			m.report(b)
		})
	}
}
