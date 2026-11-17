package eventoutbox

import (
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"

	"database/sql"
)

func benchDBSync(b *testing.B, mode string) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(b.TempDir(), "b.sqlite")+
		"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous("+mode+")")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	return db
}

// BenchmarkBatchFailHeld100 is one held/failed pass over 100 rows (a controller refusing the
// whole request, or an unreachable controller): the bookkeeping write that records the failure.
// Under synchronous=FULL (the NAS pool) every commit is an fsync, so commits per pass is the cost.
func BenchmarkBatchFailHeld100(b *testing.B) {
	for _, mode := range []string{"NORMAL", "FULL"} {
		b.Run("sync="+mode, func(b *testing.B) {
			var calls atomic.Int64
			o, err := NewBatch(benchDBSync(b, mode), &http.Client{Transport: ackAll(&calls, 100)}, BatchConfig{URL: "http://ctl/x"})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 100; i++ {
				o.Enqueue("k", benchPayload, false)
			}
			rows, err := o.load()
			if err != nil || len(rows) != 100 {
				b.Fatalf("load: %v %d", err, len(rows))
			}
			failure := errors.New("HTTP 503")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o.fail(rows, "status", failure)
			}
		})
	}
}
