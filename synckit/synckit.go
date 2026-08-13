// Package synckit is the Go client half of a bidirectional, last-write-wins
// sync protocol. It shares an exact wire contract with the server engine and
// with the protocol's other client implementations: a single POST carries both
// directions —
//
//	request : { since: <lastServerRev>, changes: [<local dirty Records>] }
//	response: { server_rev: <new cursor>, records: [<everything rev > since>],
//	            conflicts: [<device changes that lost LWW>] }
//
// The Record/Conflict/Meta/Request/Response types here are byte-compatible with
// the server's, and Wins() is a copy of the server's wins() LWW rule so a
// client can reason about conflicts identically. Deliberately minimal: this
// package does NOT own the apply-then-advance-cursor / dirty-tracking loop —
// that belongs to the consuming agent, which must only advance its `since`
// cursor after a successful, fully-applied Pull.
package synckit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Meta is the sync-metadata of a locally-stored row, used for the LWW decision.
type Meta struct {
	Rev       int64
	UpdatedAt time.Time
	Deleted   bool
}

// Record is one syncable row on the wire, in either direction.
//
//   - Kind identifies which logical table the row belongs to (e.g. "host",
//     "key") so one stream can carry several kinds under a single `since`.
//   - Rev is the server-assigned monotonic version; clients send 0 on input
//     (the server assigns it) and echo it back from pulled records.
//   - Fields holds the app data columns keyed by column name. On the wire it is
//     whatever JSON produced (numbers as float64, bytea as base64 strings, …).
type Record struct {
	Kind      string         `json:"kind"`
	ID        string         `json:"id"`
	Rev       int64          `json:"rev"`
	UpdatedAt time.Time      `json:"updated_at"`
	Deleted   bool           `json:"deleted"`
	UpdatedBy string         `json:"updated_by,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Conflict records a local change that lost the last-write-wins comparison (the
// server already had a strictly-newer version). Surfaced so the caller can log
// it; the client is expected to reconcile from the returned records.
type Conflict struct {
	Kind            string    `json:"kind"`
	ID              string    `json:"id"`
	DeviceUpdatedAt time.Time `json:"device_updated_at"`
	ServerUpdatedAt time.Time `json:"server_updated_at"`
}

// Request is the sync POST body: the client's last server cursor plus its
// locally-dirty records to push.
type Request struct {
	Since   int64    `json:"since"`
	Changes []Record `json:"changes"`
}

// Response is the sync POST result: the new server cursor, every row with
// rev > since, and any pushed changes that lost LWW.
type Response struct {
	ServerRev int64      `json:"server_rev"`
	Records   []Record   `json:"records"`
	Conflicts []Conflict `json:"conflicts"`
}

// Wins reports whether an incoming record should overwrite the current stored
// row under last-write-wins. IDENTICAL to the server engine's wins():
// strictly-newer updated_at wins; on an exact updated_at tie a delete beats a
// non-delete (a tombstone is never silently lost to a same-instant edit); any
// other tie keeps the existing row (deterministic, no flapping).
func Wins(inc Record, cur Meta) bool {
	if inc.UpdatedAt.After(cur.UpdatedAt) {
		return true
	}
	if inc.UpdatedAt.Equal(cur.UpdatedAt) {
		return inc.Deleted && !cur.Deleted
	}
	return false
}

// Pull performs one push+pull round-trip: it POSTs req as JSON to url and
// decodes a 2xx JSON body into a Response. A non-2xx status returns an error
// and an empty Response WITHOUT decoding a body, so the caller knows NOT to
// advance its `since` cursor. Applying the returned records/conflicts and
// advancing the cursor only after a fully-successful apply is the caller's job.
func Pull(ctx context.Context, httpClient *http.Client, url string, req Request) (Response, error) {
	if httpClient == nil {
		return Response{}, fmt.Errorf("synckit: nil http client")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Response{}, fmt.Errorf("marshal sync request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, fmt.Errorf("build sync request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("sync POST: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return Response{}, fmt.Errorf("sync HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}

	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Response{}, fmt.Errorf("decode sync response: %w", err)
	}
	return out, nil
}
