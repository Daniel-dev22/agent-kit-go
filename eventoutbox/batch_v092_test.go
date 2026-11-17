package eventoutbox

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"testing"
)

// A superseding enqueue keeps the OLDEST seq of its key, so a repeated boot marker never jumps
// behind the events enqueued after the first one.

// order records "kind:i" per delivered event, in the order the controller saw them.
func recordOrder(order *[]string) func(*ctl, http.ResponseWriter, []map[string]any) bool {
	return func(_ *ctl, _ http.ResponseWriter, evs []map[string]any) bool {
		for _, e := range evs {
			*order = append(*order, fmt.Sprintf("%v:%v", e["kind"], e["i"]))
		}
		return false // fall through to the normal dedupe-and-ack handler
	}
}

func payloadOf(t *testing.T, db *sql.DB, id int64) (string, int) {
	t.Helper()
	var p string
	var prot int
	if err := db.QueryRow(`SELECT payload, protected FROM batch_events WHERE id = ?`, id).Scan(&p, &prot); err != nil {
		t.Fatal(err)
	}
	return p, prot
}

func TestSupersedeKeepsAgentStartedBeforeNodeRemovedAcrossBoots(t *testing.T) {
	var dead []DeadLetter
	var order []string
	c := &ctl{respond: recordOrder(&order)}
	o, db := newBatchT(t, c, supersedeCfg(&dead))
	first := enqKind(t, o, "agent_started", 1, true) // boot 1
	removed := enqKind(t, o, "node_removed", 1, true)
	second := enqKind(t, o, "agent_started", 2, true) // boot 2, controller still down
	if second != first {
		t.Fatalf("replacement seq = %d, want the oldest seq %d", second, first)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{first, removed}) {
		t.Fatalf("rows = %v, want [%d %d]", got, first, removed)
	}
	if p, _ := payloadOf(t, db, first); p != `{"kind":"agent_started","i":2}` {
		t.Fatalf("payload = %s, want the newest", p)
	}
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("did not drain")
	}
	if fmt.Sprint(order) != "[agent_started:2 node_removed:1]" {
		t.Fatalf("delivered %v: agent_started must precede node_removed", order)
	}
}

func TestSupersedeReplacementDuringAnInFlightSendIsNotLost(t *testing.T) {
	var dead []DeadLetter
	var order []string
	var o *BatchOutbox
	var replacedSeq int64
	rec := recordOrder(&order)
	c := &ctl{}
	c.respond = func(c *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		if len(order) == 0 { // first request: the row is in flight right now
			replacedSeq = enqKind(t, o, "agent_started", 2, true)
		}
		return rec(c, w, evs)
	}
	var db *sql.DB
	o, db = newBatchT(t, c, supersedeCfg(&dead))
	first := enqKind(t, o, "agent_started", 1, true)
	if !o.drain(context.Background()) {
		t.Fatal("first pass backed off")
	}
	if replacedSeq <= first {
		t.Fatalf("a replacement of an in-flight row must get a NEW seq, got %d (first %d)", replacedSeq, first)
	}
	// The ack of the in-flight row did not take the newer payload with it: drain loops until
	// empty, so the replacement was sent in the same call.
	if fmt.Sprint(order) != "[agent_started:1 agent_started:2]" {
		t.Fatalf("delivered %v, want the old then the newest payload exactly once each", order)
	}
	if rowsLeft(t, db) != 0 {
		t.Fatal("queue should be empty")
	}
}

func TestSupersedeWhileInFlightThenFurtherReplacementsUpdateTheNewRowInPlace(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	held := enqKind(t, o, "agent_started", 1, true)
	rows, err := o.load() // the drainer now holds `held`
	if err != nil || len(rows) != 1 {
		t.Fatalf("load: %v %d", err, len(rows))
	}
	n1 := enqKind(t, o, "agent_started", 2, true) // inserts behind the in-flight row
	n2 := enqKind(t, o, "agent_started", 3, true) // updates n1 in place
	n3 := enqKind(t, o, "agent_started", 4, true)
	if !(n1 > held) || n2 != n1 || n3 != n1 {
		t.Fatalf("seqs held=%d n1=%d n2=%d n3=%d: want one new row updated in place", held, n1, n2, n3)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{held, n1}) {
		t.Fatalf("rows = %v", got)
	}
	if p, _ := payloadOf(t, db, n1); p != `{"kind":"agent_started","i":4}` {
		t.Fatalf("payload = %s", p)
	}
	if p, _ := payloadOf(t, db, held); p != `{"kind":"agent_started","i":1}` {
		t.Fatalf("the in-flight row was modified: %s", p)
	}
	o.release()
	// After the send resolves the row is replaceable in place again (oldest seq kept).
	if n4 := enqKind(t, o, "agent_started", 5, true); n4 != held {
		t.Fatalf("after release the oldest row %d is replaced in place, got %d", held, n4)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{held}) {
		t.Fatalf("rows = %v, want the younger same-key row removed", got)
	}
}

func TestSupersedeSeqStaysMonotonicForNonSupersededRows(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	a := enqKind(t, o, "agent_started", 1, true)
	b := enqKind(t, o, "node_removed", 1, true)
	enqKind(t, o, "agent_started", 2, true)
	c := enqKind(t, o, "node_added", 1, false)
	d := enqKind(t, o, "node_added", 2, false)
	if !(a < b && b < c && c < d) {
		t.Fatalf("not monotonic: %d %d %d %d", a, b, c, d)
	}
	if got := seqs(t, db); fmt.Sprint(got) != fmt.Sprint([]int64{a, b, c, d}) {
		t.Fatalf("rows = %v", got)
	}
	if o.LastSeq() != d {
		t.Fatalf("LastSeq = %d, want %d", o.LastSeq(), d)
	}
	if !o.drain(context.Background()) {
		t.Fatal("drain")
	}
	if n := enqKind(t, o, "agent_started", 3, true); n <= d {
		t.Fatalf("seq reused after the queue emptied: %d <= %d", n, d)
	}
}

func TestUnkeyedConsumersAreFIFOAndUnchanged(t *testing.T) {
	var order []string
	c := &ctl{respond: recordOrder(&order)}
	o, db := newBatchT(t, c, nil) // no SupersedeKey
	for i := 1; i <= 5; i++ {
		enqKind(t, o, "agent_started", i, true)
	}
	if n := rowsLeft(t, db); n != 5 {
		t.Fatalf("rows = %d, want 5 (no supersede without the option)", n)
	}
	if o.inflight != nil {
		t.Fatal("a consumer without SupersedeKey must not track in-flight rows")
	}
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 {
		t.Fatal("did not drain")
	}
	if fmt.Sprint(c.delivery) != "[1 2 3 4 5]" || fmt.Sprint(order) != "[agent_started:1 agent_started:2 agent_started:3 agent_started:4 agent_started:5]" {
		t.Fatalf("delivery %v %v", c.delivery, order)
	}
	if o.Stats().Superseded != 0 {
		t.Fatal("Superseded must stay 0")
	}
}

func TestSupersedeProtectionIsTheStrongestOfOldAndNewBothWays(t *testing.T) {
	var dead []DeadLetter
	o, db := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	id := enqKind(t, o, "agent_started", 1, false)
	enqKind(t, o, "agent_started", 2, true) // unprotected old, protected new
	if _, p := payloadOf(t, db, id); p != 1 {
		t.Fatalf("protected = %d, want 1 (new is stronger)", p)
	}
	enqKind(t, o, "agent_started", 3, false) // protected old, unprotected new
	if _, p := payloadOf(t, db, id); p != 1 {
		t.Fatalf("protected = %d, want 1 (old is stronger)", p)
	}
}

func TestSupersededStatCountsInPlaceReplacementsAndSkipsInFlightInserts(t *testing.T) {
	var dead []DeadLetter
	o, _ := newBatchT(t, &ctl{}, supersedeCfg(&dead))
	enqKind(t, o, "agent_started", 1, true)
	for i := 2; i <= 4; i++ {
		enqKind(t, o, "agent_started", i, true)
	}
	if got := o.Stats().Superseded; got != 3 {
		t.Fatalf("Superseded = %d, want 3 in-place replacements", got)
	}
	if _, err := o.load(); err != nil { // hold the row in flight
		t.Fatal(err)
	}
	enqKind(t, o, "agent_started", 5, true) // inserted behind it: nothing replaced
	if got := o.Stats().Superseded; got != 3 {
		t.Fatalf("Superseded = %d after an in-flight insert, want 3", got)
	}
	enqKind(t, o, "agent_started", 6, true) // updates the inserted row
	if got := o.Stats().Superseded; got != 4 {
		t.Fatalf("Superseded = %d, want 4", got)
	}
}
