package fleet

import (
	"context"
	"testing"
	"time"
)

func TestHubDoubleUnsubscribeNoPanic(t *testing.T) {
	h := NewHub(func(context.Context) (int, error) { return 1, nil }, 0)
	ch, unsub := h.Subscribe()
	unsub()
	unsub() // must be a safe no-op, not a double close
	if _, ok := <-ch; ok {
		t.Fatal("expected channel closed after unsubscribe")
	}
}

func TestHubTriggerBroadcasts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := NewHub(func(context.Context) (int, error) { return 42, nil }, 0)
	go h.Run(ctx)

	ch, unsub := h.Subscribe()
	defer unsub()
	h.Trigger()

	select {
	case v := <-ch:
		if v != 42 {
			t.Fatalf("got %d, want 42", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast")
	}
}

func TestHubSnapshotNow(t *testing.T) {
	h := NewHub(func(context.Context) (string, error) { return "snap", nil }, 0)
	v, err := h.SnapshotNow(context.Background())
	if err != nil || v != "snap" {
		t.Fatalf("got (%q,%v), want (snap,nil)", v, err)
	}
}

func TestHubBroadcastDoesNotBlockOnSlowConsumer(t *testing.T) {
	h := NewHub(func(context.Context) (int, error) { return 1, nil }, 0)
	// Subscriber that never drains. Buffer is 4; broadcasting more than 4 times
	// must not block (frames are dropped).
	_, unsub := h.Subscribe()
	defer unsub()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			h.broadcast(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on a slow consumer")
	}
}
