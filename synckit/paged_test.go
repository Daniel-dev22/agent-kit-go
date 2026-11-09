package synckit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func pager(total, per int) PullFunc {
	return func(_ context.Context, r Request) (Response, error) {
		end := r.Since + int64(per)
		more := true
		if end >= int64(total) {
			end, more = int64(total), false
		}
		var recs []Record
		for i := r.Since + 1; i <= end; i++ {
			recs = append(recs, Record{ID: string(rune('a' + i)), Rev: i})
		}
		return Response{ServerRev: end, Records: recs, More: more}, nil
	}
}

func TestPullPagedAppliesEachPageThenAdvances(t *testing.T) {
	var got, calls int
	rev, err := PullPaged(context.Background(), pager(10, 3), 0, nil, 3, func(r Response) error {
		calls++
		got += len(r.Records)
		return nil
	})
	if err != nil || rev != 10 || got != 10 || calls != 4 {
		t.Fatalf("rev=%d got=%d calls=%d err=%v", rev, got, calls, err)
	}
}

func TestPullPagedPushesChangesOnFirstPageOnly(t *testing.T) {
	var sizes []int
	inner := pager(6, 3)
	pull := func(ctx context.Context, r Request) (Response, error) {
		sizes = append(sizes, len(r.Changes))
		return inner(ctx, r)
	}
	if _, err := PullPaged(context.Background(), pull, 0, []Record{{ID: "x"}}, 3, func(Response) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] != 1 || sizes[1] != 0 {
		t.Fatalf("sizes=%v", sizes)
	}
}

func TestPullPagedApplyFailureStopsAndKeepsCursor(t *testing.T) {
	calls := 0
	rev, err := PullPaged(context.Background(), pager(10, 3), 0, nil, 3, func(Response) error {
		calls++
		if calls == 2 {
			return errors.New("disk full")
		}
		return nil
	})
	// page 1 (rev 3) applied; page 2's apply failed, so the cursor stays at 3
	if err == nil || rev != 3 || calls != 2 {
		t.Fatalf("rev=%d calls=%d err=%v", rev, calls, err)
	}
}

func TestPullPagedResync(t *testing.T) {
	pull := func(context.Context, Request) (Response, error) {
		return Response{ResyncRequired: true, MinRetainedRev: 9}, nil
	}
	_, err := PullPaged(context.Background(), pull, 4, nil, 3, func(Response) error {
		t.Fatal("apply must not run on resync")
		return nil
	})
	if !errors.Is(err, ErrResync) {
		t.Fatalf("err=%v", err)
	}
}

func TestPullPagedStuckServerErrors(t *testing.T) {
	pull := func(context.Context, Request) (Response, error) { return Response{ServerRev: 5, More: true}, nil }
	if _, err := PullPaged(context.Background(), pull, 5, nil, 3, func(Response) error { return nil }); err == nil {
		t.Fatal("expected non-advancing error")
	}
}

func TestLegacyWireOmitsPagingFields(t *testing.T) {
	b, _ := json.Marshal(Request{Since: 1})
	if string(b) != `{"since":1,"changes":null}` {
		t.Fatalf("legacy request changed: %s", b)
	}
	b, _ = json.Marshal(Response{ServerRev: 2})
	if string(b) != `{"server_rev":2,"records":null}` {
		t.Fatalf("legacy response changed: %s", b)
	}
}
