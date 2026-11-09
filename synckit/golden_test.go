package synckit

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// testdata_sync_golden.json is a verbatim copy of agent-skills
// docs/memory_api_sync_golden.json (540258f): the wire fixture memory-api and
// this client must both round-trip JSON-equal, omitted keys included.
type goldenFile struct {
	Cases []struct {
		Name     string          `json:"name"`
		Request  json.RawMessage `json:"request"`
		Response json.RawMessage `json:"response"`
	} `json:"cases"`
}

func jsonEqual(t *testing.T, what string, typed any, raw json.RawMessage) {
	t.Helper()
	if err := json.Unmarshal(raw, typed); err != nil {
		t.Fatalf("%s: decode: %v", what, err)
	}
	re, err := json.Marshal(typed)
	if err != nil {
		t.Fatalf("%s: encode: %v", what, err)
	}
	var a, b any
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(re, &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s: not JSON-equal after round trip\n want %s\n  got %s", what, raw, re)
	}
}

func TestGoldenWireRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata_sync_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g goldenFile
	if err := json.Unmarshal(data, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Cases) < 4 {
		t.Fatalf("fixture has %d cases, want >= 4", len(g.Cases))
	}
	for _, c := range g.Cases {
		t.Run(c.Name, func(t *testing.T) {
			jsonEqual(t, "request", &Request{}, c.Request)
			jsonEqual(t, "response", &Response{}, c.Response)
		})
	}
}
