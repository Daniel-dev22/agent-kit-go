package synckit

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Old-wire regression: oldRequest/oldResponse are the Request/Response structs
// verbatim from kit main 5643bfd (before paging). Bytes produced by them are
// exactly what main emits; the new kit must decode them with identical
// behaviour, emit identical request bytes when paging is unused, and the new
// Response (every new field set) must still decode into the old shape.
type oldRequest struct {
	Since   int64    `json:"since"`
	Changes []Record `json:"changes"`
}

type oldResponse struct {
	ServerRev int64      `json:"server_rev"`
	Records   []Record   `json:"records"`
	Conflicts []Conflict `json:"conflicts"`
}

var compatAt = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

func compatRecord() Record {
	return Record{Kind: "file", ID: "f1", Rev: 4, UpdatedAt: compatAt, UpdatedBy: "peer",
		Fields: map[string]any{"path": "a/b.txt", "size": float64(3)}}
}

func TestOldWireRequestBytesUnchangedWhenPagingUnused(t *testing.T) {
	for name, changes := range map[string][]Record{"nil": nil, "empty": {}, "one": {compatRecord()}} {
		oldB, _ := json.Marshal(oldRequest{Since: 7, Changes: changes})
		newB, _ := json.Marshal(Request{Since: 7, Changes: changes})
		if string(oldB) != string(newB) {
			t.Fatalf("%s: request bytes changed\n old %s\n new %s", name, oldB, newB)
		}
	}
}

func TestNewKitDecodesOldResponsesWithIdenticalBehaviour(t *testing.T) {
	conflict := []Conflict{{Kind: "file", ID: "f1", DeviceUpdatedAt: compatAt, ServerUpdatedAt: compatAt.Add(time.Hour)}}
	for name, old := range map[string]oldResponse{
		"conflicts null":  {ServerRev: 9, Records: []Record{compatRecord()}},
		"conflicts empty": {ServerRev: 9, Records: []Record{compatRecord()}, Conflicts: []Conflict{}},
		"conflicts set":   {ServerRev: 9, Records: []Record{compatRecord()}, Conflicts: conflict},
		"no records":      {ServerRev: 3, Records: []Record{}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(old)
			var got Response
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.ServerRev != old.ServerRev || len(got.Records) != len(old.Records) ||
				len(got.Conflicts) != len(old.Conflicts) || got.More || got.ResyncRequired ||
				got.MinRetainedRev != 0 || len(got.Deduped) != 0 {
				t.Fatalf("decoded differently: %+v vs %+v", got, old)
			}
			if len(old.Records) > 0 && !reflect.DeepEqual(got.Records, old.Records) {
				t.Fatalf("records differ: %+v vs %+v", got.Records, old.Records)
			}
			// Behaviour through the paged path: an old server's answer is one final page.
			calls := 0
			pull := func(context.Context, Request) (Response, error) { return got, nil }
			rev, err := PullPaged(context.Background(), pull, 2, nil, 0, func(Response) error { calls++; return nil })
			if err != nil || rev != old.ServerRev || calls != 1 {
				t.Fatalf("rev=%d calls=%d err=%v", rev, calls, err)
			}
		})
	}
}

func TestNewResponseDecodesIntoOldShape(t *testing.T) {
	full := Response{ServerRev: 8, More: true, ResyncRequired: false, MinRetainedRev: 5,
		Records:   []Record{compatRecord()},
		Conflicts: []Conflict{{Kind: "file", ID: "f1", DeviceUpdatedAt: compatAt, ServerUpdatedAt: compatAt}},
		Deduped:   []Deduped{{ID: "x", Status: "duplicate", Of: "y", Score: 1}}}
	raw, _ := json.Marshal(full)
	var old oldResponse
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("an old client cannot decode the new response: %v", err)
	}
	if old.ServerRev != 8 || !reflect.DeepEqual(old.Records, full.Records) || !reflect.DeepEqual(old.Conflicts, full.Conflicts) {
		t.Fatalf("old fields changed meaning: %+v", old)
	}
}
