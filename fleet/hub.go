// Package fleet provides a generic, coalescing snapshot-broadcast hub shared by
// the standalone agents' dashboard WebSocket endpoints (duplicacy-agent-api and
// gdrive-agent).
//
// A single broadcaster goroutine builds at most one snapshot per coalescing
// window (driven by Trigger() calls and/or an optional periodic tick) and fans
// it out to all subscribers. Each subscriber has a conflating 1-slot mailbox:
// when a consumer is slow, the broadcaster REPLACES the pending frame with the
// newer one rather than dropping the newer one or stalling. A snapshot is a
// full replacing state, so a consumer only ever needs the latest — never a
// backlog of stale snapshots. This matters because the agent broadcasts on job
// state-change only: if a fast backup's final "completed" frame were dropped
// (the old cap-4 drop-newest behaviour), the controller's fleet cache would
// stay stale for ~24h until the next job. The broadcaster NEVER unregisters a
// subscriber itself —
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

// DefaultLivenessTick is the standard periodic rebuild cadence agents pass to
// NewHub. With conflating delivery the event-driven Trigger() path already
// guarantees the latest snapshot is never dropped, so this tick is pure
// belt-and-suspenders (it only covers the rare connect/subscribe race) and is
// deliberately slow to stay cheap on low-power hosts (e.g. a Pi querying a large
// events.sqlite). Both agents build their hub with this constant so they cannot
// drift apart again.
const DefaultLivenessTick = 30 * time.Second

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
		// Conflating send into the subscriber's 1-slot mailbox: if a frame is
		// still pending (slow consumer), replace it with this newer snapshot so
		// the consumer always observes the latest state and never a stale one.
		// Single-broadcaster/single-consumer makes the drain-then-put safe: only
		// this goroutine ever sends, so the slot can only be filled here.
		select {
		case c <- snap:
		default:
			select { // discard the stale pending frame (consumer may have just taken it)
			case <-c:
			default:
			}
			select { // install the newest frame
			case c <- snap:
			default:
			}
		}
	}
}

// Subscribe registers a new subscriber and returns its channel plus an
// idempotent unsubscribe closure. Callers MUST call unsubscribe (typically via
// defer) once they stop reading. Calling it more than once is safe.
func (h *Hub[T]) Subscribe() (<-chan T, func()) {
	// Cap-1 conflating mailbox: broadcast() keeps it holding the latest snapshot
	// (see broadcast's drain-then-put), so a slow consumer reads current state
	// rather than a backlog of stale frames.
	c := make(chan T, 1)
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
