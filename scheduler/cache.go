package scheduler

import (
	"encoding/json"
	"fmt"
	"os"
)

// loadCache reads the on-disk cache. Missing file is not an error — the
// scheduler starts with an empty cache. Corrupt file IS an error so the
// caller can log and decide whether to delete + restart fresh.
func loadCache(path string) (Cache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Cache{}, nil
		}
		return Cache{}, err
	}
	var c Cache
	if err := json.Unmarshal(data, &c); err != nil {
		return Cache{}, fmt.Errorf("parse cache: %w", err)
	}
	return c, nil
}

// saveCache atomically writes `c` to `path`: write to .tmp + rename, so a
// crash mid-write can't leave a half-written file that fails to parse on
// the next boot.
func saveCache(path string, c Cache) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
