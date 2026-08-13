// Package reconcile gives a standalone agent a level-triggered (declarative)
// way to keep the controller's job view converged with the agent's own
// authoritative local state.
//
// The fast path stays the per-event delta push (eventoutbox): started ->
// progress -> completed/failed. But deltas can be lost or arrive out of order,
// which strands a controller row in a non-terminal state forever (witnessed:
// a late 'running' delta overwriting a delivered 'completed'). This package is
// the safety net: on startup and every Interval the agent POSTs its FULL set of
// non-terminal + recently-finished jobs, and the controller makes its rows for
// this (site,node) match. It is intentionally low-frequency — correctness, not
// latency, is its job; the deltas already provide instant UI updates.
//
// Transport reuses the agent's existing pooled, bearer-authed, Traefik-dialing
// HTTP client, so this is a plain stateless POST: no persistent socket, no
// router-pod pinning, survives router rollouts.
package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

// Snapshot is the agent's declared job state for one (site,node) at a point in
// time. SnapshotAt lets the controller skip rows that changed after the agent
// built the snapshot (avoids racing an in-flight delta for a just-started job).
type Snapshot struct {
	Site       string            `json:"site"`
	Node       string            `json:"node"`
	SnapshotAt time.Time         `json:"snapshot_at"`
	Jobs       []jobstore.JobRef `json:"jobs"`
}

// SnapshotFunc returns the agent's current authoritative job set from its local
// store (typically via jobstore.ListForReconcile). Called once per tick.
type SnapshotFunc func(ctx context.Context) ([]jobstore.JobRef, error)

// Config wires the reconcile loop. All fields except Interval are required.
type Config struct {
	Client   *http.Client  // pooled, bearer + Traefik-dial transport (reuse the outbox client)
	URL      string        // full controller endpoint, e.g. https://…/api/duplicacy/reconcile
	Site     string        // site identifier, e.g. "site-a"
	Node     string        // short host name
	Interval time.Duration // default 5m
	Snapshot SnapshotFunc  // builds the declared job set from the local DB
	Logger   *slog.Logger  // optional; defaults to slog.Default()
}

const (
	defaultInterval = 5 * time.Minute
	postTimeout     = 15 * time.Second
)

// Run fires one reconcile immediately (so a just-booted agent converges the
// controller right after the orphan sweep), then every Interval until ctx is
// cancelled. Failures are logged and retried on the next tick — a missed
// reconcile is self-healing by construction (the next full snapshot corrects
// whatever drifted).
func Run(ctx context.Context, cfg Config) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Snapshot == nil {
		log.Error("reconcile: Snapshot func is nil; loop not started")
		return
	}
	if cfg.Client == nil || cfg.URL == "" {
		log.Error("reconcile: Client and URL are required; loop not started")
		return
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}

	first := true
	if err := pushOnce(ctx, cfg, log); err != nil {
		log.Warn("reconcile: initial push failed (will retry)", "error", err)
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := pushOnce(ctx, cfg, log); err != nil {
				log.Warn("reconcile: push failed (will retry)", "error", err)
			} else if first {
				// Confirm the channel works once, then stay quiet on the
				// happy path (logging-in-loops discipline).
				log.Info("reconcile: converged with controller", "node", cfg.Node)
				first = false
			}
		}
	}
}

func pushOnce(ctx context.Context, cfg Config, log *slog.Logger) error {
	snapCtx, cancel := context.WithTimeout(ctx, postTimeout)
	defer cancel()

	jobs, err := cfg.Snapshot(snapCtx)
	if err != nil {
		return fmt.Errorf("build snapshot: %w", err)
	}
	snap := Snapshot{
		Site:       cfg.Site,
		Node:       cfg.Node,
		SnapshotAt: time.Now().UTC(),
		Jobs:       jobs,
	}
	body, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	req, err := http.NewRequestWithContext(snapCtx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := cfg.Client.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("controller returned %d: %s", resp.StatusCode, string(b))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}
