package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type rt func(*http.Request) (*http.Response, error)

func (f rt) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func reply(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

type rig struct {
	g      *Gated
	clk    *fakeClock
	bodies []string
	resp   func(n int) (*http.Response, error)
	fp     string
	fpErr  error
}

func newRig(t *testing.T, interval, maxSilence time.Duration) *rig {
	t.Helper()
	r := &rig{clk: &fakeClock{t: time.Unix(1_000_000, 0)}, fp: "sha256:a"}
	r.resp = func(int) (*http.Response, error) {
		return reply(200, `{"applied":true,"repairs":0,"want_snapshot":false}`), nil
	}
	g, err := NewGated(GatedConfig{
		Client: &http.Client{Transport: rt(func(req *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(req.Body)
			r.bodies = append(r.bodies, string(b))
			return r.resp(len(r.bodies))
		})},
		URL:      "http://ctl/api/ecmp/reconcile",
		Interval: interval, MaxSilence: maxSilence,
		Now:  r.clk.Now,
		Rand: func() float64 { return 1 },
		Build: func(context.Context) (Frame, error) {
			if r.fpErr != nil {
				return Frame{}, r.fpErr
			}
			return Frame{Fingerprint: r.fp, Full: []byte(`{"full":"` + r.fp + `"}`), Heartbeat: []byte(`{"heartbeat":true,"fingerprint":"` + r.fp + `"}`)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.g = g
	return r
}

func (r *rig) tick() (string, time.Duration) {
	before := len(r.bodies)
	w := r.g.Tick(context.Background())
	r.clk.Add(w)
	if len(r.bodies) == before {
		return "skip", w
	}
	switch b := r.bodies[len(r.bodies)-1]; {
	case strings.Contains(b, `"heartbeat":true`):
		return "heartbeat", w
	default:
		return "full", w
	}
}

func TestGateFirstTickIsFullThenUnchangedIsHeartbeatOrSkip(t *testing.T) {
	r := newRig(t, 100*time.Second, 300*time.Second)
	got := []string{}
	for i := 0; i < 8; i++ {
		a, _ := r.tick()
		got = append(got, a)
	}
	if got[0] != "full" {
		t.Fatalf("first tick = %s, want full (boot is ungated)", got[0])
	}
	for _, a := range got[1:] {
		if a == "full" {
			t.Fatalf("an unchanged fingerprint re-sent the full snapshot: %v", got)
		}
	}
	hb := 0
	for _, a := range got {
		if a == "heartbeat" {
			hb++
		}
	}
	if hb == 0 {
		t.Fatalf("no heartbeat in %v: silence would exceed the bound", got)
	}
}

func TestGateChangedFingerprintSendsFullAndHeartbeatCarriesFingerprint(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	r.tick()
	r.tick() // interval == max silence: heartbeat each tick
	if !strings.Contains(r.bodies[1], `"fingerprint":"sha256:a"`) {
		t.Fatalf("heartbeat does not carry the fingerprint: %s", r.bodies[1])
	}
	r.fp = "sha256:b"
	if a, _ := r.tick(); a != "full" {
		t.Fatalf("changed fingerprint sent %s", a)
	}
	if a, _ := r.tick(); a != "heartbeat" {
		t.Fatalf("after the change was acked sent %s", a)
	}
}

func TestSilenceBoundHoldsAcrossJitterDownward(t *testing.T) {
	for _, ms := range []time.Duration{100 * time.Second, 250 * time.Second, 301 * time.Second} {
		const iv = 100 * time.Second
		for _, rnd := range []float64{0, 0.5, 1} {
			clk := &fakeClock{t: time.Unix(1_000_000, 0)}
			var sentAt []time.Time
			g, _ := NewGated(GatedConfig{
				Client: &http.Client{Transport: rt(func(*http.Request) (*http.Response, error) {
					sentAt = append(sentAt, clk.Now())
					return reply(200, `{"applied":true}`), nil
				})},
				URL: "http://x", Interval: iv, MaxSilence: ms, Now: clk.Now, Rand: func() float64 { return rnd },
				Build: func(context.Context) (Frame, error) {
					return Frame{Fingerprint: "f", Full: []byte(`{}`), Heartbeat: []byte(`{"heartbeat":true}`)}, nil
				},
			})
			for i := 0; i < 400; i++ {
				clk.Add(g.Tick(context.Background()))
			}
			for i := 1; i < len(sentAt); i++ {
				if gap := sentAt[i].Sub(sentAt[i-1]); gap > ms {
					t.Fatalf("max=%v rnd=%v: silence %v between sends exceeds the bound", ms, rnd, gap)
				}
			}
			if len(sentAt) < 5 {
				t.Fatalf("only %d sends", len(sentAt))
			}
		}
	}
}

func TestEventTrafficCountsTowardSilence(t *testing.T) {
	r := newRig(t, time.Minute, 5*time.Minute)
	r.tick() // full
	for i := 0; i < 3; i++ {
		r.clk.Add(time.Minute)
		r.g.NoteSent()
		if a, _ := r.tick(); a != "skip" {
			t.Fatalf("heartbeat sent while event traffic keeps the channel alive: %s", a)
		}
	}
}

func TestFailOpenSendsFull(t *testing.T) {
	t.Run("empty fingerprint", func(t *testing.T) {
		r := newRig(t, time.Minute, 10*time.Minute)
		r.tick()
		r.fp = ""
		if a, _ := r.tick(); a != "full" {
			t.Fatalf("empty fingerprint -> %s, want full", a)
		}
	})
	t.Run("build error sends nothing but backs off (nothing to send)", func(t *testing.T) {
		r := newRig(t, time.Minute, 10*time.Minute)
		r.tick()
		r.fpErr = errors.New("boom")
		a, w := r.tick()
		if a != "skip" || w > time.Minute {
			t.Fatalf("build error -> %s wait %v", a, w)
		}
	})
	t.Run("unparseable 2xx is not a confirmation", func(t *testing.T) {
		r := newRig(t, time.Minute, 10*time.Minute)
		r.resp = func(int) (*http.Response, error) { return reply(200, `ok`), nil }
		r.tick()
		r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
		if a, _ := r.tick(); a != "full" {
			t.Fatalf("after an unconfirmed full, next = %s, want full resend", a)
		}
		if a, _ := r.tick(); a == "full" {
			t.Fatal("confirmed full must stop the resend")
		}
	})
	t.Run("applied false resends full", func(t *testing.T) {
		r := newRig(t, time.Minute, 10*time.Minute)
		r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":false}`), nil }
		r.tick()
		r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
		if a, _ := r.tick(); a != "full" {
			t.Fatalf("got %s", a)
		}
	})
}

func TestFailedFullIsResentAndBackoffIsBoundedAndFloored(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	r.resp = func(int) (*http.Response, error) { return reply(503, `busy`), nil }
	var waits []time.Duration
	for i := 0; i < 8; i++ {
		a, w := r.tick()
		if a != "full" {
			t.Fatalf("attempt %d sent %s, want full resend while unacked", i, a)
		}
		waits = append(waits, w)
	}
	for i := 1; i < len(waits); i++ {
		if waits[i] < waits[i-1] && waits[i] != time.Minute {
			t.Fatalf("backoff not monotonic with Rand=1: %v", waits)
		}
	}
	if waits[7] != time.Minute {
		t.Fatalf("backoff not capped at BackoffMax (Interval): %v", waits)
	}
	r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
	if a, _ := r.tick(); a != "full" {
		t.Fatal("recovery must send the full snapshot")
	}
	if a, _ := r.tick(); a == "full" {
		t.Fatal("after the ack the gate must close")
	}
}

func TestWantSnapshotForcesNextFullUngated(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	r.tick()
	r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true,"want_snapshot":true}`), nil }
	if a, _ := r.tick(); a != "heartbeat" {
		t.Fatalf("got %s", a)
	}
	r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
	if a, _ := r.tick(); a != "full" {
		t.Fatalf("want_snapshot did not force a full: %s", a)
	}
	if a, _ := r.tick(); a != "heartbeat" {
		t.Fatalf("gate did not close again: %s", a)
	}
}

func TestIntervalClampedToMaxSilenceAndJitterOnlyDownward(t *testing.T) {
	g, err := NewGated(GatedConfig{Client: http.DefaultClient, URL: "x", Interval: time.Hour, MaxSilence: time.Minute,
		Build: func(context.Context) (Frame, error) { return Frame{}, nil }})
	if err != nil || g.cfg.Interval != time.Minute {
		t.Fatalf("interval = %v err=%v", g.cfg.Interval, err)
	}
	for _, rnd := range []float64{0, 0.25, 0.999999, 1} {
		g.cfg.Rand = func() float64 { return rnd }
		if w := g.nextInterval(); w > time.Minute || w < 54*time.Second {
			t.Fatalf("rnd %v -> wait %v outside [0.9, 1.0] x interval", rnd, w)
		}
	}
	if _, err := NewGated(GatedConfig{}); err == nil {
		t.Fatal("missing config accepted")
	}
}

func TestBootOffsetIsWithinInterval(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1, 0)}
	started := make(chan time.Time, 1)
	g, _ := NewGated(GatedConfig{
		Client: &http.Client{Transport: rt(func(*http.Request) (*http.Response, error) {
			select {
			case started <- time.Now():
			default:
			}
			return reply(200, `{"applied":true}`), nil
		})},
		URL: "http://x", Interval: 200 * time.Millisecond, Now: clk.Now, Rand: func() float64 { return 0.5 },
		Build: func(context.Context) (Frame, error) { return Frame{Fingerprint: "f", Full: []byte(`{}`)}, nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t0 := time.Now()
	go g.Run(ctx)
	select {
	case s := <-started:
		if d := s.Sub(t0); d < 80*time.Millisecond || d > 190*time.Millisecond {
			t.Fatalf("first report after %v, want about 100ms (Rand 0.5 x 200ms)", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no report")
	}
}

func TestPostRejectsNon2xxAndNetworkError(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	for _, mk := range []func(int) (*http.Response, error){
		func(int) (*http.Response, error) { return reply(400, ``), nil },
		func(int) (*http.Response, error) { return nil, errors.New("dial") },
		func(int) (*http.Response, error) { return reply(302, ``), nil },
	} {
		r.resp = mk
		if a, _ := r.tick(); a != "full" {
			t.Fatal(fmt.Sprint("sent ", a))
		}
	}
	if r.g.gate.haveFP {
		t.Fatal("a failed full must not advance the gate")
	}
}

func TestDecideEachDisjunctAloneForcesFull(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	acked := func() *Gate {
		g := NewGate(10*time.Second, 30*time.Second)
		g.FullAcked("f", t0)
		return g
	}
	if a := acked().Decide(t0.Add(time.Second), "f", true); a != ActionSkip {
		t.Fatalf("baseline = %v", a)
	}
	cases := map[string]func() (*Gate, string, bool){
		"fingerprint unavailable": func() (*Gate, string, bool) { return acked(), "f", false },
		"empty fingerprint":       func() (*Gate, string, bool) { return acked(), "", true },
		"changed fingerprint":     func() (*Gate, string, bool) { return acked(), "g", true },
		"no baseline":             func() (*Gate, string, bool) { return NewGate(10*time.Second, 30*time.Second), "f", true },
		"want_snapshot": func() (*Gate, string, bool) {
			g := acked()
			g.WantSnapshot()
			return g, "f", true
		},
	}
	for name, mk := range cases {
		g, fp, ok := mk()
		if a := g.Decide(t0.Add(time.Second), fp, ok); a != ActionFull {
			t.Fatalf("%s -> %v, want full", name, a)
		}
	}
}

func TestDecideSilenceBoundaryIsExact(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	g := NewGate(10*time.Second, 30*time.Second)
	g.FullAcked("f", t0)
	if a := g.Decide(t0.Add(20*time.Second), "f", true); a != ActionSkip {
		t.Fatalf("elapsed+interval == max must still skip, got %v", a)
	}
	if a := g.Decide(t0.Add(20*time.Second+1), "f", true); a != ActionHeartbeat {
		t.Fatalf("one ns past the bound must heartbeat, got %v", a)
	}
	g.Sent(t0.Add(25 * time.Second))
	if a := g.Decide(t0.Add(30*time.Second), "f", true); a != ActionSkip {
		t.Fatalf("Sent must reset the silence clock, got %v", a)
	}
}

func TestNewGatedDefaultsAndNonPositiveValues(t *testing.T) {
	build := func(context.Context) (Frame, error) { return Frame{}, nil }
	for _, v := range []time.Duration{0, -1} {
		g, err := NewGated(GatedConfig{Client: http.DefaultClient, URL: "x", Build: build,
			Interval: v, MaxSilence: v, BackoffBase: v, BackoffMax: v, PostTimeout: v})
		if err != nil {
			t.Fatal(err)
		}
		c := g.cfg
		if c.Interval != 5*time.Minute || c.MaxSilence != 5*time.Minute || c.BackoffBase != time.Second ||
			c.BackoffMax != 5*time.Minute || c.PostTimeout != 15*time.Second {
			t.Fatalf("defaults for %v = %+v", v, c)
		}
	}
	g, _ := NewGated(GatedConfig{Client: http.DefaultClient, URL: "x", Build: build,
		Interval: 1, MaxSilence: 1, BackoffBase: 1, BackoffMax: 1, PostTimeout: 1})
	c := g.cfg
	if c.Interval != 1 || c.MaxSilence != 1 || c.BackoffBase != 1 || c.BackoffMax != 1 || c.PostTimeout != 1 {
		t.Fatalf("a positive value was replaced by a default: %+v", c)
	}
}

func TestStatusRangeAndAppliedParsing(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		ok     bool
	}{
		{199, `{"applied":true}`, false}, {200, `{"applied":true}`, true}, {299, `{"applied":true}`, true},
		{300, `{"applied":true}`, false}, {301, `{"applied":true}`, false},
		{200, `{}`, false}, {200, `not json`, false}, {200, `{"applied":false}`, false},
		{200, `{"applied":true,"want_snapshot":"x"}`, false}, // type error: not a confirmation even though applied is set
	} {
		r := newRig(t, time.Minute, time.Minute)
		r.resp = func(int) (*http.Response, error) { return reply(tc.status, tc.body), nil }
		r.tick()
		confirmed := r.g.gate.haveFP
		if confirmed != tc.ok {
			t.Fatalf("status %d body %q: confirmed=%v, want %v", tc.status, tc.body, confirmed, tc.ok)
		}
	}
}

func TestPostReadsAtMostTheBodyLimit(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	big := `{"applied":true}` + strings.Repeat(" ", 64<<10)
	r.resp = func(int) (*http.Response, error) { return reply(200, big), nil }
	r.tick()
	if !r.g.gate.haveFP {
		t.Fatal("trailing whitespace past the limit must not matter")
	}
	huge := strings.Repeat(" ", 64<<10) + `{"applied":true}`
	r.g.gate.haveFP = false
	r.resp = func(int) (*http.Response, error) { return reply(200, huge), nil }
	r.tick()
	if r.g.gate.haveFP {
		t.Fatal("a body past the 64 KiB limit must be cut, so its ack is unseen")
	}
}

func TestWaitsAfterSuccessFailureAndReset(t *testing.T) {
	r := newRig(t, time.Minute, time.Minute)
	r.g.cfg.Rand = func() float64 { return 1 }
	r.g.cfg.BackoffBase, r.g.cfg.BackoffMax = time.Second, time.Minute
	if _, w := r.tick(); w != time.Minute {
		t.Fatalf("success wait = %v, want the interval (Rand 1)", w)
	}
	r.resp = func(int) (*http.Response, error) { return reply(503, ``), nil }
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		if _, w := r.tick(); w != want {
			t.Fatalf("failure %d wait = %v, want %v", i+1, w, want)
		}
	}
	r.resp = func(int) (*http.Response, error) { return reply(200, `{"applied":true}`), nil }
	r.tick()
	r.resp = func(int) (*http.Response, error) { return reply(503, ``), nil }
	if _, w := r.tick(); w != time.Second {
		t.Fatalf("after a success the backoff must restart at the base, got %v", w)
	}
}

func TestFailWaitExponentCapAndFloor(t *testing.T) {
	g, _ := NewGated(GatedConfig{Client: http.DefaultClient, URL: "x", Interval: time.Hour,
		BackoffBase: 1, BackoffMax: time.Duration(1) << 62, Rand: func() float64 { return 1 },
		Build: func(context.Context) (Frame, error) { return Frame{}, nil }})
	var w time.Duration
	for i := 0; i < 50; i++ {
		w = g.failWait()
	}
	if w != time.Duration(1)<<40 {
		t.Fatalf("exponent cap: wait = %v, want 2^40 ns", w)
	}
	g.failures = 0
	if g.cfg.Rand = func() float64 { return 0 }; g.failWait() != 100*time.Millisecond {
		t.Fatal("full jitter must be floored at 100ms so a failing controller never spins")
	}
}

func TestNewGatedRequiresEachField(t *testing.T) {
	build := func(context.Context) (Frame, error) { return Frame{}, nil }
	for name, c := range map[string]GatedConfig{
		"no client": {URL: "x", Build: build},
		"no url":    {Client: http.DefaultClient, Build: build},
		"no build":  {Client: http.DefaultClient, URL: "x"},
	} {
		if _, err := NewGated(c); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestDefaultBoundariesEachValueAlone(t *testing.T) {
	build := func(context.Context) (Frame, error) { return Frame{}, nil }
	mk := func(c GatedConfig) GatedConfig { c.Client, c.URL, c.Build = http.DefaultClient, "x", build; return c }
	g, _ := NewGated(mk(GatedConfig{Interval: 1}))
	if g.cfg.Interval != 1 || g.cfg.MaxSilence != 1 {
		t.Fatalf("Interval=1 alone: %+v", g.cfg)
	}
	g, _ = NewGated(mk(GatedConfig{Interval: 10, MaxSilence: 1}))
	if g.cfg.MaxSilence != 1 || g.cfg.Interval != 1 {
		t.Fatalf("MaxSilence=1 must clamp the interval: %+v", g.cfg)
	}
	g, _ = NewGated(mk(GatedConfig{Interval: 10, MaxSilence: 10, BackoffMax: 1}))
	if g.cfg.BackoffMax != 1 {
		t.Fatalf("BackoffMax=1 replaced: %+v", g.cfg)
	}
}

type closeCounter struct {
	io.Reader
	closed *int
}

func (c closeCounter) Close() error { *c.closed++; return nil }

func TestResponseBodyIsClosedAndReadLimitIsExact(t *testing.T) {
	closed := 0
	r := newRig(t, time.Minute, time.Minute)
	body := `{"applied":true}`
	exact := strings.Repeat(" ", 64<<10-len(body)) + body // exactly 64 KiB: must parse
	r.resp = func(int) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: closeCounter{strings.NewReader(exact), &closed}}, nil
	}
	r.tick()
	if closed != 1 {
		t.Fatalf("body closed %d times, want 1", closed)
	}
	if !r.g.gate.haveFP {
		t.Fatal("a body of exactly 64 KiB must be read in full")
	}
}
