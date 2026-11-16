package reconcile

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func BenchmarkGateDecide(b *testing.B) {
	t0 := time.Unix(1_000_000, 0)
	g := NewGate(10*time.Second, 30*time.Second)
	g.FullAcked("f", t0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if g.Decide(t0.Add(time.Second), "f", true) != ActionSkip {
			b.Fatal("not skip")
		}
	}
}

// BenchmarkGatedTickSkip is the steady-state cycle: unchanged fingerprint, inside the silence bound, no POST.
func BenchmarkGatedTickSkip(b *testing.B) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	posts := 0
	g, _ := NewGated(GatedConfig{
		Client: &http.Client{Transport: rt(func(*http.Request) (*http.Response, error) {
			posts++
			return reply(200, `{"applied":true}`), nil
		})},
		URL: "http://x", Interval: time.Minute, MaxSilence: time.Hour, Now: clk.Now, Rand: func() float64 { return 1 },
		Build: func(context.Context) (Frame, error) {
			return Frame{Fingerprint: "sha256:abc", Full: []byte(`{}`), Heartbeat: []byte(`{}`)}, nil
		},
	})
	g.Tick(context.Background()) // first full
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Tick(context.Background())
	}
	b.StopTimer()
	if posts != 1 {
		b.Fatalf("%d POSTs: an unchanged fingerprint inside the bound must send nothing", posts)
	}
}
