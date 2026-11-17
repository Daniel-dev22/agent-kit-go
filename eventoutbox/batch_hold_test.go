package eventoutbox

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func holdCfg(dead *[]DeadLetter) func(*BatchConfig) {
	return func(cfg *BatchConfig) {
		cfg.HoldOnRefusal = true
		cfg.OnDead = func(d DeadLetter) { *dead = append(*dead, d) }
	}
}

// A request-wide refusal holds every row at the FIRST level: one POST, no bisect, no
// dead letter, drain reports a backoff. Per contract s4 the controller refused the request, not
// an event.
func TestHoldOnRefusalKeepsRowsAndBacksOffOnWholeRequestRefusal(t *testing.T) {
	for _, st := range []int{400, 404, 409, 422} {
		t.Run(fmt.Sprint(st), func(t *testing.T) {
			c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
				w.WriteHeader(st)
				io.WriteString(w, `{"code":"batch_invalid"}`)
				return true
			}}
			var dead []DeadLetter
			o, db := newBatchT(t, c, holdCfg(&dead))
			enq(t, o, 3, false)
			if o.drain(context.Background()) {
				t.Fatal("drain reported success: it must back off")
			}
			if rowsLeft(t, db) != 3 || len(dead) != 0 || c.posts != 1 || o.Stats().Dead != 0 {
				t.Fatalf("rows=%d dead=%d posts=%d", rowsLeft(t, db), len(dead), c.posts)
			}
			var attempts int
			var lastErr string
			if err := db.QueryRow(`SELECT MIN(attempts), MIN(last_error) FROM batch_events`).Scan(&attempts, &lastErr); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 || lastErr == "" {
				t.Fatalf("held rows must record the failure: attempts=%d err=%q", attempts, lastErr)
			}
		})
	}
}

// A batch of ONE is held too (the default would dead-letter it), and 'retryable' bodies behave as before.
func TestHoldOnRefusalAlsoHoldsABatchOfOne(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(409)
		return true
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, holdCfg(&dead))
	enq(t, o, 1, true)
	if o.drain(context.Background()) || rowsLeft(t, db) != 1 || len(dead) != 0 {
		t.Fatal("a protected event must survive a request-wide refusal")
	}
}

// 413 is about size, not state: it still splits under hold, and only 4xx refusals are held.
func TestHoldOnRefusalDoesNotChangeSizeSplitOrSuccess(t *testing.T) {
	c := &ctl{respond: func(c *ctl, w http.ResponseWriter, evs []map[string]any) bool {
		if len(evs) > 10 {
			w.WriteHeader(413)
			return true
		}
		return false
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, holdCfg(&dead))
	enq(t, o, 40, false)
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 || len(dead) != 0 {
		t.Fatal("413 must still split to convergence under hold")
	}
}

// Held rows are delivered, in order, once the controller accepts: nothing was lost while it refused.
func TestHoldOnRefusalDeliversEverythingOnceTheControllerRecovers(t *testing.T) {
	refuse := true
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		if refuse {
			w.WriteHeader(404)
			return true
		}
		return false
	}}
	var dead []DeadLetter
	o, db := newBatchT(t, c, holdCfg(&dead))
	enq(t, o, 5, false)
	for i := 0; i < 3; i++ {
		if o.drain(context.Background()) {
			t.Fatal("must keep backing off while refused")
		}
	}
	refuse = false
	if !o.drain(context.Background()) || rowsLeft(t, db) != 0 || len(dead) != 0 {
		t.Fatal("held rows were not delivered after recovery")
	}
	if fmt.Sprint(c.delivery) != "[1 2 3 4 5]" {
		t.Fatalf("order/loss: %v", c.delivery)
	}
}

// The loop obeys the backoff for a held refusal (a poke must not cut it short: that is the herd).
func TestHoldOnRefusalBacksOffInTheLoop(t *testing.T) {
	c := &ctl{respond: func(_ *ctl, w http.ResponseWriter, _ []map[string]any) bool {
		w.WriteHeader(422)
		return true
	}}
	var dead []DeadLetter
	o, _ := newBatchT(t, c, func(cfg *BatchConfig) {
		holdCfg(&dead)(cfg)
		cfg.BackoffBase = time.Hour
		cfg.BackoffMax = time.Hour
	})
	enq(t, o, 2, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.Start(ctx)
	defer o.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := c.posts
		c.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		enq(t, o, 1, false)
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	n := c.posts
	c.mu.Unlock()
	if n != 1 {
		t.Fatalf("posts = %d: pokes during a held-refusal backoff must not re-post", n)
	}
}
