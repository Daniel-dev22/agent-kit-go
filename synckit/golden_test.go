package synckit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// testdata_sync_golden.json is a verbatim copy of agent-skills
// docs/memory_api_sync_golden.json at ec5e84c (6 cases): the wire fixture
// memory-api and this client must both round-trip JSON-equal, omitted keys
// included.
//
// To update it: copy the canonical file over testdata_sync_golden.json, set
// goldenSHA256 to `sha256sum` of it, extend wantCases, in ONE change.
// goldenSHA256 is the gate: it is the same value pinned in the contract doc
// (agent-skills docs/memory_api_CONTRACT.md) and in memory-api's twin test, so
// all three move together or a suite fails. CI has no agent-skills checkout;
// TestGoldenMatchesCanonical is a local-only extra: set KIT_GOLDEN_CANONICAL to
// /docker_container_volumes/agent-skills/docs/memory_api_sync_golden.json to
// byte-compare the copy with the live file.
const goldenSHA256 = "03f65739db2a10176e2008d9d1bfd675a46e6e118ca409a4f684c63180d0bbde"

var wantCases = []string{"push_and_first_page", "last_page_no_changes", "dedup_on_push", "conflict_on_push", "more_with_deduped", "resync_required"}

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

func TestGoldenPinnedHash(t *testing.T) {
	data, err := os.ReadFile("testdata_sync_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != goldenSHA256 {
		t.Fatalf("fixture copy changed (sha256 %s); re-copy the canonical file and update goldenSHA256 + wantCases", got)
	}
}

func TestGoldenMatchesCanonical(t *testing.T) {
	path := os.Getenv("KIT_GOLDEN_CANONICAL")
	if path == "" {
		t.Skip("local-only: set KIT_GOLDEN_CANONICAL to byte-compare against the canonical fixture")
	}
	canon, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	local, _ := os.ReadFile("testdata_sync_golden.json")
	if !bytes.Equal(canon, local) {
		t.Fatalf("kit copy has drifted from %s", path)
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
	var names []string
	for _, c := range g.Cases {
		names = append(names, c.Name)
	}
	if !reflect.DeepEqual(names, wantCases) {
		t.Fatalf("cases = %v, want %v", names, wantCases)
	}
	for _, c := range g.Cases {
		t.Run(c.Name, func(t *testing.T) {
			jsonEqual(t, "request", &Request{}, c.Request)
			jsonEqual(t, "response", &Response{}, c.Response)
		})
	}
}
