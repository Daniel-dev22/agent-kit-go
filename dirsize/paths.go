package dirsize

import (
	"path/filepath"
	"strings"
)

// PathUnderAny reports whether path equals or is nested under any of prefixes.
// Prefixes are cleaned; path is compared as given (callers pass cleaned paths).
func PathUnderAny(path string, prefixes []string) bool {
	for _, p := range prefixes {
		pc := filepath.Clean(p)
		if path == pc || strings.HasPrefix(path, pc+string(filepath.Separator)) {
			return true
		}
	}
	return false
}
