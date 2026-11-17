package eventoutbox

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// Contract v1.7 reply shapes as the router answers them (ecmpRefuse): per-event refusals are HTTP
// 200 with rejected:[{seq,code,retryable}]; every 4xx is an envelope refusal; 413 batch_too_large
// splits; 409 epoch_conflict holds; only 503 store_unavailable is retryable. These run with the
// agent's real configuration (HoldOnRefusal, no bisect allowlist).

func w2Cfg(dead *[]DeadLetter) func(*BatchConfig) {
	return func(cfg *BatchConfig) {
		holdCfg(dead)(cfg)
	}
}

// (a) a 200 with a retryable:false rejected row dead-letters that row and acks the rest.
func TestW2PerEventTerminalRejectionIsDeadLetteredAndRestAcked(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		fmt.Fprintf(w, `{"accepted":%d,"duplicate":0,"rejected":[{"seq":3,"code":"invalid_event","retryable":false}],"last_seq":%d,"epoch_changed":false}`,
			len(evs)-1, len(evs))
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, w2Cfg(&dead))
	enq(t, o, 5, true)
	if !o.drain(context.Background()) {
		t.Fatal("a per-event rejection is not a failure")
	}
	if rowsLeft(t, db) != 0 || len(dead) != 1 || dead[0].Seq != 3 || dead[0].Code != "invalid_event" || c.posts != 1 {
		t.Fatalf("rows=%d dead=%+v posts=%d", rowsLeft(t, db), dead, c.posts)
	}
}

// (b) retryable:true is kept and retried; only that row stays, the rest are acked.
func TestW2PerEventRetryableRejectionIsRetried(t *testing.T) {
	retry := true
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		if retry {
			fmt.Fprintf(w, `{"accepted":%d,"duplicate":0,"rejected":[{"seq":2,"code":"store_error","retryable":true}],"last_seq":0,"epoch_changed":false}`, len(evs)-1)
			return true
		}
		return false
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, w2Cfg(&dead))
	enq(t, o, 3, true)
	if o.drain(context.Background()) {
		t.Fatal("a retryable row must make the pass back off")
	}
	if rowsLeft(t, db) != 1 || len(dead) != 0 {
		t.Fatalf("rows=%d dead=%d, want only the retryable row kept", rowsLeft(t, db), len(dead))
	}
	retry = false
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 || len(dead) != 0 {
		t.Fatal("the retried row was not delivered")
	}
}

// (c) 413 batch_too_large splits and everything is eventually delivered (HoldOnRefusal on).
func TestW2BatchTooLargeSplitsUnderHoldOnRefusal(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		if len(evs) > 2 {
			w.WriteHeader(413)
			io.WriteString(w, `{"code":"batch_too_large","message":"x"}`)
			return true
		}
		return false
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, w2Cfg(&dead))
	enq(t, o, 8, true)
	for i := 0; i < 20 && rowsLeft(t, db) > 0; i++ {
		if !o.drain(context.Background()) {
			t.Fatal("a 413 must split, not back off")
		}
	}
	if rowsLeft(t, db) != 0 || len(dead) != 0 || len(c.delivery) != 8 {
		t.Fatalf("rows=%d dead=%d delivered=%d", rowsLeft(t, db), len(dead), len(c.delivery))
	}
}

// (d) every other envelope 4xx holds the rows (no bisect, no dead letter), including invalid_json.
func TestW2EnvelopeRefusalsHoldTheRows(t *testing.T) {
	for _, tc := range []struct {
		st   int
		code string
	}{
		{400, "invalid_json"}, {400, "invalid_body"}, {400, "invalid_agent_id"}, {400, "invalid_epoch"},
		{400, "invalid_version"}, {400, "empty_batch"}, {422, "invalid_agent"}, {422, "wrong_site"},
		{422, "wrong_home"}, {409, "some_other_conflict"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
				w.WriteHeader(tc.st)
				fmt.Fprintf(w, `{"code":%q,"message":"m"}`, tc.code)
				return true
			}}
			var dead []DeadLetter
			o, db := newBatchT(t, c, w2Cfg(&dead))
			enq(t, o, 4, true)
			if o.drain(context.Background()) {
				t.Fatal("must hold and back off")
			}
			if rowsLeft(t, db) != 4 || len(dead) != 0 || c.posts != 1 {
				t.Fatalf("rows=%d dead=%d posts=%d", rowsLeft(t, db), len(dead), c.posts)
			}
		})
	}
	// 503 store_unavailable retryable:true also holds and backs off
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(503)
		io.WriteString(w, `{"code":"store_unavailable","retryable":true}`)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, w2Cfg(&dead))
	enq(t, o, 2, true)
	if o.drain(context.Background()) || rowsLeft(t, db) != 2 || len(dead) != 0 {
		t.Fatal("503 must hold")
	}
}
