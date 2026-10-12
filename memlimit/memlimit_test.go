package memlimit

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type env map[string]string

func (e env) get(k string) string { return e[k] }

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func run(t *testing.T, root string, e env) (Result, error, string, []int64) {
	t.Helper()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	var set []int64
	res, err := ApplyWith(log, Options{
		Root:     root,
		Getenv:   e.get,
		SetLimit: func(n int64) int64 { set = append(set, n); return 0 },
	})
	return res, err, buf.String(), set
}

func TestV2Numeric(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory.max"), "1073741824\n")
	res, err, logs, set := run(t, root, env{})
	if err != nil {
		t.Fatal(err)
	}
	want := derive(1073741824)
	if !res.Applied || res.Source != SourceCgroupV2 || res.LimitBytes != want || res.CgroupBytes != 1073741824 {
		t.Fatalf("result %+v", res)
	}
	if len(set) != 1 || set[0] != want {
		t.Fatalf("set %v", set)
	}
	if strings.Count(logs, "\n") != 1 {
		t.Fatalf("want exactly one log line, got %q", logs)
	}
}

func TestV2Max(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory.max"), "max\n")
	res, err, logs, set := run(t, root, env{})
	if err != nil || res.Applied || len(set) != 0 {
		t.Fatalf("res %+v err %v set %v", res, err, set)
	}
	if strings.Count(logs, "level=WARN") != 1 || strings.Count(logs, "\n") != 1 {
		t.Fatalf("want one warning, got %q", logs)
	}
}

func TestV2MaxDoesNotFallThroughToV1(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory.max"), "max")
	write(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "536870912")
	res, _, _, set := run(t, root, env{})
	if res.Applied || len(set) != 0 {
		t.Fatalf("v2 'max' must win over v1: %+v", res)
	}
}

func TestV1Numeric(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "536870912\n")
	res, err, _, set := run(t, root, env{})
	if err != nil {
		t.Fatal(err)
	}
	want := derive(536870912)
	if !res.Applied || res.Source != SourceCgroupV1 || len(set) != 1 || set[0] != want {
		t.Fatalf("res %+v set %v", res, set)
	}
}

func TestV1Huge(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory", "memory.limit_in_bytes"), "9223372036854771712\n")
	res, err, logs, set := run(t, root, env{})
	if err != nil || res.Applied || len(set) != 0 {
		t.Fatalf("res %+v err %v set %v", res, err, set)
	}
	if strings.Count(logs, "level=WARN") != 1 {
		t.Fatalf("want one warning, got %q", logs)
	}
}

func TestMissingFiles(t *testing.T) {
	res, err, logs, set := run(t, t.TempDir(), env{})
	if err != nil || res.Applied || len(set) != 0 {
		t.Fatalf("res %+v err %v set %v", res, err, set)
	}
	if strings.Count(logs, "level=WARN") != 1 {
		t.Fatalf("want one warning, got %q", logs)
	}
}

func TestGOMEMLIMITRespected(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "memory.max"), "1073741824")
	res, err, _, set := run(t, root, env{"GOMEMLIMIT": "512MiB"})
	if err != nil || res.Applied || res.Source != SourceEnv || len(set) != 0 {
		t.Fatalf("res %+v err %v set %v", res, err, set)
	}
}

func TestInvalidContents(t *testing.T) {
	for name, tc := range map[string]struct{ file, content string }{
		"v2 garbage":  {"memory.max", "lots"},
		"v2 negative": {"memory.max", "-5"},
		"v2 zero":     {"memory.max", "0"},
		"v2 empty":    {"memory.max", ""},
		"v1 garbage":  {filepath.Join("memory", "memory.limit_in_bytes"), "x12"},
		"v1 negative": {filepath.Join("memory", "memory.limit_in_bytes"), "-1"},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write(t, filepath.Join(root, tc.file), tc.content)
			res, err, _, set := run(t, root, env{})
			if err == nil || res.Applied || len(set) != 0 {
				t.Fatalf("want error and no set: res %+v err %v set %v", res, err, set)
			}
		})
	}
}

func TestUnreadablePathIsError(t *testing.T) {
	root := t.TempDir()
	// memory.max is a directory: present but unreadable as a file.
	if err := os.MkdirAll(filepath.Join(root, "memory.max"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err, _, set := run(t, root, env{})
	if err == nil || len(set) != 0 {
		t.Fatalf("err %v set %v", err, set)
	}
}

func TestApplyDefaultsDoNotPanic(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB") // keeps the real runtime limit untouched
	res, err := Apply(nil)
	if err != nil || res.Source != SourceEnv {
		t.Fatalf("res %+v err %v", res, err)
	}
}

func derive(n int64) int64 { return int64(float64(n) * Ratio) }
