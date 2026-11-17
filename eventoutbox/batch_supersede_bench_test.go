package eventoutbox

import (
	"net/http"
	"sync/atomic"
	"testing"
)

// BenchmarkBatchEnqueueSuperseding is the keyed enqueue that replaces its predecessor (the
// steady state of a boot-marker event): insert + delete in one transaction. Compare with
// BenchmarkBatchEnqueue (no SupersedeKey: one autocommit INSERT, unchanged from v0.9.0).
func BenchmarkBatchEnqueueSuperseding(b *testing.B) {
	var calls atomic.Int64
	o, err := NewBatch(benchDB(b), &http.Client{Transport: ackAll(&calls, 1)}, BatchConfig{
		URL:          "http://ctl/api/ecmp/events",
		SupersedeKey: func(string, []byte) string { return "agent_started" },
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.Enqueue("agent_started", benchPayload, true); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if n := o.count(); n != 1 {
		b.Fatalf("%d rows after %d keyed enqueues, want exactly 1", n, b.N)
	}
}
