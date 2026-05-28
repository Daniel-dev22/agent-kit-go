// Package fleet provides a generic, coalescing snapshot-broadcast hub shared by
// the standalone agents' dashboard WebSocket endpoints (duplicacy-agent-api and
// gdrive-agent).
//
// A single broadcaster goroutine builds at most one snapshot per coalescing
// window (driven by Trigger() calls and/or an optional periodic tick) and fans
// it out to all subscribers. Slow subscribers drop frames rather than stalling
// the broadcaster, and the broadcaster NEVER unregisters a subscriber itself —
// cleanup happens exactly once, via the closure returned by Subscribe(), guarded
// against double-close. This is the correct shape; duplicacy-agent-api's
// hand-rolled hub previously had two unregister paths plus an unconditional
// close, which double-closed the send channel and panicked the whole process
// ("close of closed channel") under bursty job churn.
//
// Generic over the agent-specific snapshot payload T so each agent broadcasts
// its own shape while sharing the hub machinery.
package fleet

import (
	"context"
	"sync"
	"time"
)

// Hub fans coalesced snapshots out to all current subscribers.
type Hub[T any] struct {
	build func(context.Context) (T, error)
	tick  time.Duration

	mu      sync.RWMutex
	subs    map[chan T]struct{}
	trigger chan struct{} // buffered cap 1 so coalescing producers never block
}

// NewHub returns a hub that builds snapshots via build. If tick > 0 the hub also
// rebuilds on that interval (liveness); tick == 0 means trigger-only.
func NewHub[T any](build func(context.Context) (T, error), tick time.Duration) *Hub[T] {
	return &Hub[T]{
		build:   build,
		tick:    tick,
		subs:    map[chan T]struct{}{},
		trigger: make(chan struct{}, 1),
	}
}

// Trigger asks the hub to build + broadcast a fresh snapshot. Non-blocking;
// calls within the same build window coalesce (the cap-1 buffer drops extras).
func (h *Hub[T]) Trigger() {
	if h == nil {
		return
	}
	select {
	case h.trigger <- struct{}{}:
	default:
	}
}

// Run is the broadcaster loop. Cancel ctx to terminate. Build errors are
// skipped (the next trigger/tick retries) so a transient read failure never
// kills the loop.
func (h *Hub[T]) Run(ctx context.Context) {
	var tickC <-chan time.Time
	if h.tick > 0 {
		t := time.NewTicker(h.tick)
		defer t.Stop()
		tickC = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tickC:
		case <-h.trigger:
		}
		snap, err := h.build(ctx)
		if err != nil {
			continue
		}
		h.broadcast(snap)
	}
}

func (h *Hub[T]) broadcast(snap T) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.subs {
		select {
		case c <- snap:
		default:
			// Slow consumer — drop this frame. The WS read pump detects the
			// disconnect and calls the unsubscribe closure; we never unregister
			// from here, so there is no second close path.
		}
	}
}

// Subscribe registers a new subscriber and returns its channel plus an
// idempotent unsubscribe closure. Callers MUST call unsubscribe (typically via
// defer) once they stop reading. Calling it more than once is safe.
func (h *Hub[T]) Subscribe() (<-chan T, func()) {
	c := make(chan T, 4)
	h.mu.Lock()
	h.subs[c] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	return c, func() {
		once.Do(func() {
			h.mu.Lock()
			if _, ok := h.subs[c]; ok {
				delete(h.subs, c)
				close(c)
			}
			h.mu.Unlock()
		})
	}
}

// SnapshotNow builds one snapshot synchronously without broadcasting — used to
// greet a freshly-connected subscriber so it doesn't wait for the next tick.
func (h *Hub[T]) SnapshotNow(ctx context.Context) (T, error) {
	return h.build(ctx)
}
