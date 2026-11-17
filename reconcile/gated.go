package reconcile

// Gated reconcile: an opt-in sibling of Run. Run pushes the full snapshot every
// interval unconditionally; Gated pushes it only when the caller's content
// fingerprint changed, and otherwise sends a small heartbeat that CARRIES the
// fingerprint so the controller can tell "unchanged" from "lost".
//
// The kit never hashes or inspects the body: the caller owns what the
// fingerprint covers (it must exclude self-moving fields such as timestamps and
// RTTs) and builds both bodies. Fail-open: when the gate cannot decide (no
// fingerprint, a Build error, an unparseable or negative controller answer) it
// sends the full snapshot.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// Frame is what the caller's Build returns each tick.
type Frame struct {
	Fingerprint string // opaque; empty means "cannot decide" and forces a full send
	Full        []byte // complete snapshot JSON body
	Heartbeat   []byte // heartbeat JSON body (carries the fingerprint)
}

// GatedConfig configures Gated. Client, URL and Build are required.
type GatedConfig struct {
	Client *http.Client
	URL    string
	Build  func(ctx context.Context) (Frame, error)

	// Interval is the tick period. Each wait is Interval x [0.9, 1.0]: jitter goes
	// DOWN only, so the MaxSilence bound below holds.
	Interval time.Duration // default 5m
	// MaxSilence bounds the time between any two messages from this agent
	// (snapshot, heartbeat, or traffic reported through NoteSent). Interval is
	// clamped to it. Default = Interval.
	MaxSilence  time.Duration
	BackoffBase time.Duration // after a failed send, default 1s
	BackoffMax  time.Duration // default Interval
	PostTimeout time.Duration // default 15s

	// OnReply, when set, receives the raw body of every 2xx reply that parses as a confirmation
	// (applied present), BEFORE the gate acts on it, so a consumer can read fields the kit does not
	// know (e.g. a capability list the controller advertises). It is called on the tick goroutine
	// and must not block; body is only valid for the duration of the call (copy to keep it, at most
	// 64 KiB). Replies that fail to parse, non-2xx replies and transport errors never call it.
	// Optional: nil keeps the existing behaviour byte for byte.
	OnReply func(body []byte)

	// NonErrorDeferrals makes two controller replies a deferral instead of a failure (contract
	// v1.6): HTTP 409 (another process holds this identity) and a 2xx with applied:false. A deferral
	// forces a full snapshot on the next tick, logs once per state change at Info, does not back
	// off and does not count as a failure, so the heartbeat cadence continues. Default false keeps
	// today's behaviour byte for byte (both are retried with backoff and logged at Warn).
	NonErrorDeferrals bool

	Now    func() time.Time
	Rand   func() float64
	Logger *slog.Logger
}

// Action is the gate's decision for one tick.
type Action int

const (
	ActionSkip Action = iota
	ActionHeartbeat
	ActionFull
)

// Gate is the pure decision state, separate from I/O so it can be property-tested.
type Gate struct {
	interval   time.Duration
	maxSilence time.Duration

	haveFP    bool
	lastFP    string
	forceFull bool
	lastSent  time.Time
}

// NewGate returns a Gate; the first Decide always answers ActionFull.
func NewGate(interval, maxSilence time.Duration) *Gate {
	return &Gate{interval: interval, maxSilence: maxSilence}
}

// Decide chooses what to send at now. fpOK is false when no fingerprint is available.
func (g *Gate) Decide(now time.Time, fp string, fpOK bool) Action {
	if !fpOK || fp == "" || !g.haveFP || g.forceFull || fp != g.lastFP {
		return ActionFull
	}
	// Skipping is safe only if the next tick (at most interval away) still lands inside the bound.
	if now.Sub(g.lastSent)+g.interval > g.maxSilence {
		return ActionHeartbeat
	}
	return ActionSkip
}

// Sent records that a message reached the controller (any kind).
func (g *Gate) Sent(now time.Time) { g.lastSent = now }

// FullAcked records that fp was applied by the controller.
func (g *Gate) FullAcked(fp string, now time.Time) {
	g.haveFP, g.lastFP, g.forceFull = fp != "", fp, false
	g.lastSent = now
}

// WantSnapshot makes the next Decide answer ActionFull regardless of the fingerprint.
func (g *Gate) WantSnapshot() { g.forceFull = true }

// Gated is the running reconcile loop.
type Gated struct {
	cfg  GatedConfig
	log  *slog.Logger
	mu   sync.Mutex
	gate *Gate

	failures     int
	failing      bool
	buildFailing bool   // Build is failing: logged on the edge only, not every tick
	healthy      bool   // first-success confirmation logged
	deferredWhat string // the deferral being logged (NonErrorDeferrals), "" when none
}

// NewGated validates and defaults cfg.
func NewGated(cfg GatedConfig) (*Gated, error) {
	if cfg.Client == nil || cfg.URL == "" || cfg.Build == nil {
		return nil, fmt.Errorf("reconcile: Client, URL and Build are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.MaxSilence <= 0 {
		cfg.MaxSilence = cfg.Interval
	}
	if cfg.Interval > cfg.MaxSilence {
		cfg.Interval = cfg.MaxSilence
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = cfg.Interval
	}
	if cfg.PostTimeout <= 0 {
		cfg.PostTimeout = postTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rand == nil {
		cfg.Rand = mrand.Float64
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Gated{cfg: cfg, log: log, gate: NewGate(cfg.Interval, cfg.MaxSilence)}, nil
}

// NoteSent tells the gate that other traffic (an event batch) reached the
// controller, so a heartbeat is not due on its account.
func (g *Gated) NoteSent() {
	g.mu.Lock()
	g.gate.Sent(g.cfg.Now())
	g.mu.Unlock()
}

// Run waits a random boot offset in [0, Interval) (no herd after a mass restart),
// then ticks until ctx ends. The first tick is always a full snapshot.
func (g *Gated) Run(ctx context.Context) {
	wait := time.Duration(g.cfg.Rand() * float64(g.cfg.Interval))
	for {
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		wait = g.Tick(ctx)
	}
}

// noteBuild logs a Build failure on the edge only (first failure at Warn, recovery at Info): a
// builder that fails for hours (a failing volume) must not write one line per tick.
func (g *Gated) noteBuild(err error) {
	switch {
	case err != nil && !g.buildFailing:
		g.buildFailing = true
		g.log.Warn("reconcile: build failed; sending full snapshot if one can be built", "error", err)
	case err == nil && g.buildFailing:
		g.buildFailing = false
		g.log.Info("reconcile: build recovered")
	}
}

// Tick runs one gated cycle and returns how long to wait before the next.
func (g *Gated) Tick(ctx context.Context) time.Duration {
	fr, err := g.cfg.Build(ctx)
	fpOK := err == nil
	g.noteBuild(err)
	g.mu.Lock()
	act := g.gate.Decide(g.cfg.Now(), fr.Fingerprint, fpOK)
	g.mu.Unlock()

	var body []byte
	switch act {
	case ActionSkip:
		return g.nextInterval()
	case ActionHeartbeat:
		body = fr.Heartbeat
	default:
		body = fr.Full
	}
	if len(body) == 0 { // Build failed or produced nothing: nothing to send
		return g.failWait()
	}
	applied, want, err := g.post(ctx, body)
	if errors.Is(err, errConflict) || errors.Is(err, errNotApplied) {
		return g.deferred(err)
	}
	now := g.cfg.Now()
	g.mu.Lock()
	switch {
	case err != nil:
		// leave the gate untouched: the next tick decides again, fail-open
	case !applied:
		// 2xx but unparseable or applied:false: not a confirmation. Resend full.
		g.gate.WantSnapshot()
		err = fmt.Errorf("controller did not confirm the %s", map[Action]string{ActionHeartbeat: "heartbeat", ActionFull: "snapshot"}[act])
	case act == ActionFull:
		g.gate.FullAcked(fr.Fingerprint, now)
	default:
		g.gate.Sent(now)
	}
	if err == nil && want {
		g.gate.WantSnapshot()
	}
	g.mu.Unlock()
	if err != nil {
		if !g.failing {
			g.failing = true
			g.log.Warn("reconcile: push failing (retrying with backoff)", "error", err)
		}
		return g.failWait()
	}
	if g.failing || !g.healthy || g.deferredWhat != "" {
		g.log.Info("reconcile: converged with controller", "action", act)
		g.failing, g.healthy, g.deferredWhat = false, true, ""
	}
	g.failures = 0
	return g.nextInterval()
}

func (g *Gated) nextInterval() time.Duration {
	return time.Duration(float64(g.cfg.Interval) * (0.9 + 0.1*g.cfg.Rand()))
}

// failWait is capped exponential backoff with full jitter, floored so it never spins.
func (g *Gated) failWait() time.Duration {
	g.failures++
	ceil := min(float64(g.cfg.BackoffBase)*math.Exp2(float64(min(g.failures-1, 40))), float64(g.cfg.BackoffMax))
	return max(100*time.Millisecond, time.Duration(g.cfg.Rand()*ceil))
}

// post sends body; applied is true only for a 2xx that parses and does not say applied:false.
func (g *Gated) post(ctx context.Context, body []byte) (applied, want bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, g.cfg.PostTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.cfg.Client.Do(req)
	if err != nil {
		return false, false, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode == http.StatusConflict && g.cfg.NonErrorDeferrals {
		return false, false, errConflict
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, false, fmt.Errorf("controller returned %d: %.200s", resp.StatusCode, rb)
	}
	var r struct {
		Applied *bool `json:"applied"`
		Want    bool  `json:"want_snapshot"`
	}
	if json.Unmarshal(rb, &r) != nil || r.Applied == nil {
		return false, false, nil
	}
	if g.cfg.OnReply != nil {
		g.cfg.OnReply(rb)
	}
	if !*r.Applied && g.cfg.NonErrorDeferrals {
		return false, r.Want, errNotApplied
	}
	return *r.Applied, r.Want, nil
}

// errConflict and errNotApplied are post's two deferral outcomes (NonErrorDeferrals only).
var (
	errConflict   = errors.New("reconcile: 409 conflict")
	errNotApplied = errors.New("reconcile: applied:false")
)

// deferred handles a controller deferral: the controller is reachable and answering, it just asks
// for a snapshot (applied:false) or has another process holding the identity (409 conflict). That
// is not a failure: no backoff, no failure count, one Info line per state change, the next tick on
// the normal cadence with a full snapshot forced, so a heartbeat keeps flowing and a takeover is
// seen as soon as the other process goes quiet.
func (g *Gated) deferred(err error) time.Duration {
	what := "applied:false"
	if errors.Is(err, errConflict) {
		what = "409 conflict"
	}
	g.mu.Lock()
	g.gate.WantSnapshot()
	g.mu.Unlock()
	if g.deferredWhat != what {
		g.deferredWhat = what
		g.log.Info("reconcile: controller deferred the message (not an error)", "reply", what)
	}
	g.failures = 0
	return g.nextInterval()
}
