// Package logging centralizes slog setup + a Fatal helper for standalone
// Go agents that depend on agent-kit-go. Mirrors the
// controller/shared/logutil package one-for-one — keep them in sync if
// you change the signature so both ecosystems behave identically.
//
// Usage in an agent's main:
//
//	func main() {
//	    logger := logging.Setup(os.Getenv("LOG_LEVEL"))
//	    logger.Info("agent starting", "node", cfg.NodeName)
//	    ...
//	    if err := boot(); err != nil {
//	        logging.Fatal("boot failed", "error", err)
//	    }
//	}
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup parses level (debug|info|warn|error; defaults to info), wires it
// into the default slog logger using a JSON handler against stderr (or
// text if LOGUTIL_FORMAT=text), and returns the same logger.
func Setup(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOGUTIL_FORMAT")) == "text" {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	} else {
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	}

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// Fatal logs an error-level record then calls os.Exit(1).
func Fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}
