package synckit

import (
	"context"
	"errors"
	"fmt"
)

// PullFunc performs one push+pull round-trip (production: a closure over Pull).
type PullFunc func(ctx context.Context, req Request) (Response, error)

// ErrResync is returned by PullPaged when the server reports ResyncRequired.
// The caller must discard its mirror, reset its cursor to 0 and call PullPaged
// again; PullPaged never wipes anything itself.
var ErrResync = errors.New("synckit: resync required")

// maxPages bounds a runaway server that keeps answering More.
const maxPages = 100000

// PullPaged drives the paged protocol: the first request carries the dirty
// changes, follow-ups carry none and resume from the previous page's ServerRev.
// apply is called once per page and must durably apply it AND persist the
// cursor (resp.ServerRev) in the same step; the next page is requested only
// after apply returns nil, so a crash never advances the cursor past unapplied
// records. Returns the final cursor.
func PullPaged(ctx context.Context, pull PullFunc, since int64, changes []Record, limit int,
	apply func(Response) error) (int64, error) {
	if pull == nil || apply == nil {
		return since, fmt.Errorf("synckit: pull and apply are required")
	}
	for page := 0; page < maxPages; page++ {
		resp, err := pull(ctx, Request{Since: since, Changes: changes, Limit: limit})
		if err != nil {
			return since, err
		}
		if resp.ResyncRequired {
			return since, ErrResync
		}
		if err := apply(resp); err != nil {
			return since, err
		}
		changes = nil // pushed in the first page's transaction only
		if !resp.More {
			return resp.ServerRev, nil
		}
		if resp.ServerRev <= since {
			return since, fmt.Errorf("synckit: server paging did not advance (since=%d server_rev=%d)", since, resp.ServerRev)
		}
		since = resp.ServerRev
	}
	return since, fmt.Errorf("synckit: exceeded %d pages", maxPages)
}
