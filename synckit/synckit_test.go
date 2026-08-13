package synckit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var (
	tA = time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	tB = time.Date(2026, 7, 5, 11, 0, 0, 0, time.UTC)
)

// TestWins pins the LWW rule against the server engine: strictly-newer wins, ties
// keep existing except a tombstone beats a same-instant non-delete.
func TestWins(t *testing.T) {
	cases := []struct {
		name string
		inc  Record
		cur  Meta
		want bool
	}{
		{"newer wins", Record{UpdatedAt: tB}, Meta{UpdatedAt: tA}, true},
		{"older loses", Record{UpdatedAt: tA}, Meta{UpdatedAt: tB}, false},
		{"tie keeps existing", Record{UpdatedAt: tB}, Meta{UpdatedAt: tB}, false},
		{"tie delete beats non-delete", Record{UpdatedAt: tB, Deleted: true}, Meta{UpdatedAt: tB}, true},
		{"tie delete vs delete keeps", Record{UpdatedAt: tB, Deleted: true}, Meta{UpdatedAt: tB, Deleted: true}, false},
		{"newer delete wins", Record{UpdatedAt: tB, Deleted: true}, Meta{UpdatedAt: tA}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Wins(c.inc, c.cur); got != c.want {
				t.Errorf("Wins = %v, want %v", got, c.want)
			}
		})
	}
}

// --- wire-contract golden tests: pin the exact JSON the server expects ---

// TestContractRecordWithFields pins a data record's serialization: snake_case
// keys, JSON-number scalars, JSON-array values, sorted Fields keys, and the
// non-omitempty rev/deleted always present.
func TestContractRecordWithFields(t *testing.T) {
	rec := Record{
		Kind:      "host",
		ID:        "h1",
		UpdatedAt: time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC),
		Fields: map[string]any{
			"name": "pi",
			"port": 2222,
			"tags": []string{"home", "pi"},
		},
	}
	got, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Go sorts map keys, so the shape is deterministic and can be pinned exactly.
	const want = `{"kind":"host","id":"h1","rev":0,"updated_at":"2026-07-05T12:00:00Z","deleted":false,"fields":{"name":"pi","port":2222,"tags":["home","pi"]}}`
	if string(got) != want {
		t.Errorf("record JSON drift:\n got=%s\nwant=%s", got, want)
	}
}

// TestContractTombstone pins a delete: deleted:true, and both fields and
// updated_by omitted.
func TestContractTombstone(t *testing.T) {
	tomb := Record{
		Kind:      "host",
		ID:        "33333333",
		UpdatedAt: time.Date(2026, 7, 5, 13, 0, 0, 0, time.UTC),
		Deleted:   true,
	}
	got, err := json.Marshal(tomb)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"kind":"host","id":"33333333","rev":0,"updated_at":"2026-07-05T13:00:00Z","deleted":true}`
	if string(got) != want {
		t.Errorf("tombstone JSON drift:\n got=%s\nwant=%s", got, want)
	}
}

// TestContractRequest pins the push+pull request envelope.
func TestContractRequest(t *testing.T) {
	req := Request{
		Since: 5,
		Changes: []Record{{
			Kind:      "host",
			ID:        "33333333",
			UpdatedAt: time.Date(2026, 7, 5, 13, 0, 0, 0, time.UTC),
			Deleted:   true,
		}},
	}
	got, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"since":5,"changes":[{"kind":"host","id":"33333333","rev":0,"updated_at":"2026-07-05T13:00:00Z","deleted":true}]}`
	if string(got) != want {
		t.Errorf("request JSON drift:\n got=%s\nwant=%s", got, want)
	}
}

// TestContractResponseDecode decodes a golden server response literal and
// checks the field mapping (snake_case server_rev, decoded Fields, conflicts,
// numeric fields as float64) plus forward-compat: unknown fields ignored.
func TestContractResponseDecode(t *testing.T) {
	const body = `{
	  "server_rev": 7,
	  "records": [
	    {"kind":"key","id":"k1","rev":7,"updated_at":"2026-07-05T12:00:00Z","deleted":false,
	     "fields":{"name":"ed","public_key":"ssh-ed25519 AAAA"}},
	    {"kind":"host","id":"h1","rev":6,"updated_at":"2026-07-05T11:00:00Z","deleted":false,
	     "fields":{"name":"pi","port":22,"tags":["a"]}}
	  ],
	  "conflicts": [
	    {"kind":"host","id":"h1","device_updated_at":"2026-07-05T10:00:00Z","server_updated_at":"2026-07-05T11:00:00Z"}
	  ],
	  "future_field": true
	}`

	var resp Response
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ServerRev != 7 {
		t.Errorf("server_rev = %d, want 7", resp.ServerRev)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(resp.Records))
	}
	key := resp.Records[0]
	if key.Kind != "key" || key.Rev != 7 || key.Fields["public_key"] != "ssh-ed25519 AAAA" {
		t.Errorf("key record decoded wrong: %+v", key)
	}
	host := resp.Records[1]
	// JSON numbers decode into any as float64.
	if port, ok := host.Fields["port"].(float64); !ok || port != 22 {
		t.Errorf("host port = %v (%T), want float64(22)", host.Fields["port"], host.Fields["port"])
	}
	if tags, ok := host.Fields["tags"].([]any); !ok || len(tags) != 1 {
		t.Errorf("host tags = %v, want 1-element array", host.Fields["tags"])
	}
	if len(resp.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(resp.Conflicts))
	}
	c := resp.Conflicts[0]
	if !c.DeviceUpdatedAt.Equal(tA) || !c.ServerUpdatedAt.Equal(tB) {
		t.Errorf("conflict times decoded wrong: %+v", c)
	}
}

// TestPullSuccess drives a real HTTP round-trip: the request body must be the
// exact wire shape and the 200 JSON response must decode into a Response.
func TestPullSuccess(t *testing.T) {
	var gotBody []byte
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotCT = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"server_rev":9,"records":[{"kind":"host","id":"h1","rev":9,"updated_at":"2026-07-05T12:00:00Z","deleted":false}],"conflicts":[]}`)
	}))
	defer srv.Close()

	req := Request{Since: 3, Changes: []Record{{Kind: "host", ID: "h1", UpdatedAt: tB}}}
	resp, err := Pull(context.Background(), srv.Client(), srv.URL, req)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotCT)
	}
	const wantBody = `{"since":3,"changes":[{"kind":"host","id":"h1","rev":0,"updated_at":"2026-07-05T11:00:00Z","deleted":false}]}`
	if string(gotBody) != wantBody {
		t.Errorf("request body drift:\n got=%s\nwant=%s", gotBody, wantBody)
	}
	if resp.ServerRev != 9 || len(resp.Records) != 1 || resp.Records[0].Rev != 9 {
		t.Errorf("response decoded wrong: %+v", resp)
	}
}

// TestPullNon2xx confirms a non-2xx status is a hard error (caller must not
// advance its cursor) and no Response is produced.
func TestPullNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	resp, err := Pull(context.Background(), srv.Client(), srv.URL, Request{Since: 1})
	if err == nil {
		t.Fatal("expected an error on 503")
	}
	if resp.ServerRev != 0 || resp.Records != nil {
		t.Errorf("expected zero Response on error, got %+v", resp)
	}
}
