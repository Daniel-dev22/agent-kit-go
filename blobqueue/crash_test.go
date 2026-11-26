package blobqueue

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The crash tests re-exec the test binary as a child that is SIGKILLed mid-write / mid-drain, then
// reopen the database in the parent. kill -9 survival is a WAL+synchronous guarantee, so the child
// opens the same DSN the package documents (synchronous=FULL).

const (
	envMode = "BLOBQ_CRASH_MODE"
	envDB   = "BLOBQ_CRASH_DB"
	envLog  = "BLOBQ_CRASH_LOG"
)

func TestCrashChild(t *testing.T) {
	mode := os.Getenv(envMode)
	if mode == "" {
		t.Skip("helper process")
	}
	db, err := sql.Open("sqlite", "file:"+os.Getenv(envDB)+walDSN)
	if err != nil {
		t.Fatal(err)
	}
	q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1, MaxRows: -1, MaxBytes: -1, MaxBatchItems: 20})
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "enqueue":
		for i := 0; ; i++ {
			seq, err := q.Enqueue(context.Background(), Item{Signal: "logs", Body: gz(2000, byte(i))})
			if err != nil {
				t.Fatal(err)
			}
			// Printed only after Enqueue returned: the parent treats it as acknowledged-durable.
			fmt.Fprintf(os.Stdout, "%d\n", seq)
		}
	case "drain":
		f, err := os.OpenFile(os.Getenv(envLog), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		send := func(_ context.Context, frame []byte) (Ack, error) {
			_, items, err := DecodeFrame(frame, Limits{})
			if err != nil {
				return Ack{}, err
			}
			for _, it := range items {
				fmt.Fprintf(f, "%d\n", it.Seq) // the "receiver" records it before the ack returns
			}
			time.Sleep(2 * time.Millisecond) // widen the window between receipt and delete
			return Ack{Accepted: len(items)}, nil
		}
		_ = q.Run(context.Background(), send)
	}
}

func startChild(t *testing.T, mode, db, log string) (*exec.Cmd, *bufio.Scanner) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.v=false")
	cmd.Env = append(os.Environ(), envMode+"="+mode, envDB+"="+db, envLog+"="+log)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, bufio.NewScanner(out)
}

func kill(cmd *exec.Cmd) {
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func integrity(t *testing.T, db *sql.DB) {
	t.Helper()
	var res string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&res); err != nil || res != "ok" {
		t.Fatalf("integrity_check = %q %v", res, err)
	}
}

// kill -9 mid-enqueue, three times over the same file: every sequence the child reported as
// returned must be in the database after reopen, seq stays monotonic, the file passes integrity_check.
func TestCrashMidEnqueue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.sqlite")
	acked := map[uint64]bool{}
	var maxSeen uint64
	for cycle := 0; cycle < 3; cycle++ {
		cmd, sc := startChild(t, "enqueue", path, "")
		got := 0
		for got < 150 && sc.Scan() {
			s, err := strconv.ParseUint(strings.TrimSpace(sc.Text()), 10, 64)
			if err != nil {
				continue // a line cut by the kill, or test chatter
			}
			if s <= maxSeen {
				t.Fatalf("cycle %d: seq %d not greater than previous %d", cycle, s, maxSeen)
			}
			maxSeen = s
			acked[s] = true
			got++
		}
		kill(cmd)
		if got < 150 {
			t.Fatalf("child produced only %d acks", got)
		}
		db := openDB(t, path)
		integrity(t, db)
		q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1})
		if err != nil {
			t.Fatal(err)
		}
		present := map[uint64]bool{}
		rs, err := db.Query(`SELECT seq FROM blobq_items`)
		if err != nil {
			t.Fatal(err)
		}
		for rs.Next() {
			var s uint64
			_ = rs.Scan(&s)
			present[s] = true
		}
		rs.Close()
		for s := range acked {
			if !present[s] {
				t.Fatalf("cycle %d: acknowledged seq %d lost after kill -9", cycle, s)
			}
		}
		if st := q.Stats(context.Background()); st.Rows != int64(len(present)) {
			t.Fatalf("running total %d != rows %d after recovery", st.Rows, len(present))
		}
	}
}

// kill -9 mid-drain: nothing is lost, and anything delivered twice is confined to the one batch that
// was in flight (the receiver dedupes on (epoch, seq)).
func TestCrashMidDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "q.sqlite")
	logPath := filepath.Join(dir, "recv.log")
	db := openDB(t, path)
	q, err := New(db, Config{FloorBytes: -1, FloorPercent: -1, MaxRows: -1, MaxBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	const total = 600
	enq(t, q, total, 100)
	db.Close()

	cmd, _ := startChild(t, "drain", path, logPath)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if b, _ := os.ReadFile(logPath); strings.Count(string(b), "\n") >= 100 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	kill(cmd)

	first := map[uint64]bool{}
	b, _ := os.ReadFile(logPath)
	for _, ln := range strings.Fields(string(b)) {
		if s, err := strconv.ParseUint(ln, 10, 64); err == nil {
			first[s] = true
		}
	}
	if len(first) < 100 || len(first) >= total {
		t.Fatalf("child delivered %d of %d: the kill did not land mid-drain", len(first), total)
	}

	db2 := openDB(t, path)
	integrity(t, db2)
	q2, err := New(db2, Config{FloorBytes: -1, FloorPercent: -1, MaxRows: -1, MaxBytes: -1, MaxBatchItems: 20})
	if err != nil {
		t.Fatal(err)
	}
	if q2.Epoch() != q.Epoch() {
		t.Fatalf("epoch changed across the crash")
	}
	rec := &recorder{}
	for {
		n, err := q2.drainOnce(context.Background(), rec.send)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	second := map[uint64]int{}
	for _, it := range rec.got {
		second[it.Seq]++
	}
	redelivered := 0
	for s := uint64(1); s <= total; s++ {
		if !first[s] && second[s] == 0 {
			t.Fatalf("seq %d lost across the crash", s)
		}
		if second[s] > 1 {
			t.Fatalf("seq %d delivered %d times in one run", s, second[s])
		}
		if first[s] && second[s] > 0 {
			redelivered++
		}
	}
	if redelivered > 20 {
		t.Fatalf("%d rows re-delivered after the crash, want <= one batch (20)", redelivered)
	}
	if st := q2.Stats(context.Background()); st.Rows != 0 {
		t.Fatalf("queue not empty after full ack: %+v", st)
	}
}
