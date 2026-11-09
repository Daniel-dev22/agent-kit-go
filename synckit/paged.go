package synckit

import (
	"context"
	"errors"
	"fmt"
)

// PullFunc performs one push+pull round-trip (production: a closure over Pull).
type PullFunc func(ctx context.Context, req Request) (Response, error)

// ErrResync is returned by PullPaged when the server reports ResyncRequired.
//
// The server REFUSES the whole call on resync: the first page's changes were
// NOT applied and any Deduped it carried is meaningless. The caller must
// discard its CLEAN mirror rows only (never its queued, unpushed writes), reset
// its cursor to 0, and call PullPaged again re-sending the same changes.
// PullPaged never wipes anything itself.
var ErrResync = errors.New("synckit: resync required")

// ErrProtocol marks a server answer that violates the paging contract. The page
// is NOT passed to apply, so nothing about it is persisted.
var ErrProtocol = errors.New("synckit: protocol violation")

// maxPages bounds a runaway server that keeps answering More.
const maxPages = 100000

// PullPaged drives the paged protocol: the first request carries the dirty
// changes, follow-ups carry an empty (non-null) changes list and resume from
// the previous page's ServerRev.
//
// Every page is validated BEFORE apply, so a bad page never reaches storage.
// apply is called once per page and must durably apply it AND persist the
// cursor (resp.ServerRev) in the same step; the next page is requested only
// after apply returns nil, so a crash never advances the cursor past unapplied
// records. Returns the final cursor.
func PullPaged(ctx context.Context, pull PullFunc, since int64, changes []Record, limit int,
	apply func(Response) error) (int64, error) {
	if pull == nil || apply == nil {
		return since, fmt.Errorf("synckit: pull and apply are required")
	}
	if changes == nil {
		changes = []Record{}
	}
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return since, err
		}
		resp, err := pull(ctx, Request{Since: since, Changes: changes, Limit: limit})
		if err != nil {
			return since, err
		}
		if resp.ResyncRequired {
			if since == 0 {
				// since=0 is always a legitimate full pull and never resyncs; a
				// caller that reset to 0 would otherwise loop forever.
				return since, fmt.Errorf("%w: resync_required at since=0", ErrProtocol)
			}
			return since, ErrResync
		}
		if err := validatePage(since, resp); err != nil {
			return since, err
		}
		if err := apply(resp); err != nil {
			return since, err
		}
		changes = []Record{} // pushed in the first page's transaction only
		if !resp.More {
			return resp.ServerRev, nil
		}
		since = resp.ServerRev
	}
	return since, fmt.Errorf("synckit: exceeded %d pages", maxPages)
}

func validatePage(since int64, resp Response) error {
	if resp.More {
		if resp.ServerRev <= since {
			return fmt.Errorf("%w: more=true but server_rev %d did not advance past since %d", ErrProtocol, resp.ServerRev, since)
		}
		if n := len(resp.Records); n > 0 && resp.Records[n-1].Rev != resp.ServerRev {
			return fmt.Errorf("%w: more=true but server_rev %d is not the last returned rev %d", ErrProtocol, resp.ServerRev, resp.Records[n-1].Rev)
		}
		return nil
	}
	if resp.ServerRev < since {
		return fmt.Errorf("%w: server_rev %d regressed below since %d", ErrProtocol, resp.ServerRev, since)
	}
	return nil
}
