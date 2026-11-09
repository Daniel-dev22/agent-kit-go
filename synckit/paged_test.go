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

func noApply(t *testing.T) func(Response) error {
	return func(Response) error { t.Fatal("apply must not run for this page"); return nil }
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

// L4: the exact requests PullPaged sends, byte for byte. Follow-up pages carry
// `changes:[]` (the contract/fixture shape), never null.
func TestPullPagedRequestBytes(t *testing.T) {
	var sent []string
	inner := pager(4, 2)
	pull := func(ctx context.Context, r Request) (Response, error) {
		b, _ := json.Marshal(r)
		sent = append(sent, string(b))
		return inner(ctx, r)
	}
	if _, err := PullPaged(context.Background(), pull, 0, nil, 2, func(Response) error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`{"since":0,"changes":[],"limit":2}`,
		`{"since":2,"changes":[],"limit":2}`,
	}
	if len(sent) != 2 || sent[0] != want[0] || sent[1] != want[1] {
		t.Fatalf("sent=%v", sent)
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

func TestPullPagedResyncRefusesAndNeverApplies(t *testing.T) {
	pull := func(context.Context, Request) (Response, error) {
		return Response{ResyncRequired: true, MinRetainedRev: 9, Deduped: []Deduped{{ID: "x", Of: "y"}}}, nil
	}
	_, err := PullPaged(context.Background(), pull, 4, nil, 3, noApply(t))
	if !errors.Is(err, ErrResync) {
		t.Fatalf("err=%v", err)
	}
}

// L3: since=0 never resyncs; answering so is a protocol error, not ErrResync,
// so a caller's reset-and-retry loop cannot spin forever.
func TestPullPagedResyncAtZeroIsProtocolError(t *testing.T) {
	pull := func(context.Context, Request) (Response, error) { return Response{ResyncRequired: true}, nil }
	_, err := PullPaged(context.Background(), pull, 0, nil, 3, noApply(t))
	if !errors.Is(err, ErrProtocol) || errors.Is(err, ErrResync) {
		t.Fatalf("err=%v", err)
	}
}

// M1: a bad page is rejected BEFORE apply, so a cursor-saving apply can never
// persist a regression. A recording apply would see the page; none must run.
func TestPullPagedRejectsBadPagesBeforeApply(t *testing.T) {
	cases := map[string]Response{
		"more but stuck":       {ServerRev: 500, More: true},
		"more but regressed":   {ServerRev: 200, More: true, Records: []Record{{ID: "a", Rev: 200}}},
		"more, rev != last":    {ServerRev: 900, More: true, Records: []Record{{ID: "a", Rev: 600}}},
		"final but regressed":  {ServerRev: 200},
		"final regressed+recs": {ServerRev: 499, Records: []Record{{ID: "a", Rev: 499}}},
		"record <= since":      {ServerRev: 600, Records: []Record{{ID: "a", Rev: 500}}},
		"record > server_rev":  {ServerRev: 600, Records: []Record{{ID: "a", Rev: 601}}},
		"paged out of order":   {ServerRev: 700, More: true, Records: []Record{{ID: "a", Rev: 650}, {ID: "b", Rev: 600}, {ID: "c", Rev: 700}}},
	}
	for name, resp := range cases {
		t.Run(name, func(t *testing.T) {
			pull := func(context.Context, Request) (Response, error) { return resp, nil }
			rev, err := PullPaged(context.Background(), pull, 500, nil, 3, noApply(t))
			if !errors.Is(err, ErrProtocol) || rev != 500 {
				t.Fatalf("rev=%d err=%v", rev, err)
			}
		})
	}
}

// M1: with an apply that saves ServerRev, the stored cursor never moves backwards.
func TestPullPagedStoredCursorNeverRegresses(t *testing.T) {
	stored := int64(500)
	pull := func(context.Context, Request) (Response, error) { return Response{ServerRev: 200, More: true}, nil }
	_, err := PullPaged(context.Background(), pull, 500, nil, 3, func(r Response) error {
		stored = r.ServerRev
		return nil
	})
	if err == nil || stored != 500 {
		t.Fatalf("stored cursor regressed to %d (err=%v)", stored, err)
	}
}

// L5: a cancelled context stops the loop even though pull would succeed.
func TestPullPagedStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pages := 0
	_, err := PullPaged(ctx, pager(100, 1), 0, nil, 1, func(Response) error {
		pages++
		if pages == 2 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || pages != 2 {
		t.Fatalf("pages=%d err=%v", pages, err)
	}
}

// L6: the new fields are absent at their zero values. Conflicts becoming
// omitempty is a deliberate change (the server omits it); compat with the
// old wire is proven separately in compat_test.go.
func TestPagingFieldsOmittedAtZeroValues(t *testing.T) {
	b, _ := json.Marshal(Request{Since: 1})
	if string(b) != `{"since":1,"changes":null}` {
		t.Fatalf("request: %s", b)
	}
	b, _ = json.Marshal(Response{ServerRev: 2})
	if string(b) != `{"server_rev":2,"records":null}` {
		t.Fatalf("response: %s", b)
	}
}
