package eventoutbox

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestGoldenWireEnqueuePush pins the exact bytes an existing caller puts on the
// wire: method, URL path, Content-Type and body, one POST per event. The batch
// extension is additive and opt-in; this test fails if it ever changes them.
func TestGoldenWireEnqueuePush(t *testing.T) {
	type seen struct{ method, path, ctype, body string }
	var mu sync.Mutex
	var got []seen
	o, db := newTestOutbox(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, seen{r.Method, r.URL.Path, r.Header.Get("Content-Type"), string(b)})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	o.Enqueue("job7", "completed", []byte(`{"state":"completed","exit":0}`))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && pendingCount(t, db) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("posts = %d, want exactly 1", len(got))
	}
	want := seen{"POST", "/jobs/job7/event", "application/json", `{"state":"completed","exit":0}`}
	if got[0] != want {
		t.Fatalf("wire = %+v, want %+v", got[0], want)
	}
}

var _ = httptest.NewRecorder
