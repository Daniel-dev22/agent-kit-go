package scheduler

import (
	"strconv"
	"strings"
	"time"
)

// Matches returns true if `now` (interpreted in `tz`) falls inside one of
// `s`'s configured fire windows.
//
// Mirrors scheduler-api/main.go's matcher verbatim so the JSON Schedule
// shape behaves identically whether the central scheduler-api or an agent
// is evaluating it.
//
// Exported so tests (and the kit's own MissedRunCheck) can re-use the same
// predicate — callers normally do not need to invoke it directly.
func Matches(s Schedule, now time.Time, tz *time.Location) bool {
	now = now.In(tz)
	if s.IntervalMinutes != nil && *s.IntervalMinutes > 0 {
		minutesSinceMidnight := now.Hour()*60 + now.Minute()
		return minutesSinceMidnight%*s.IntervalMinutes == 0
	}
	dayName := strings.ToLower(now.Weekday().String()[:3])
	if !contains(s.Days, "*") && !contains(s.Days, dayName) {
		return false
	}
	hourStr := strconv.Itoa(now.Hour())
	if !contains(s.Hours, "*") && !contains(s.Hours, hourStr) {
		return false
	}
	minuteInt, _ := strconv.Atoi(s.Minute)
	return now.Minute() == minuteInt
}

// findLastFireWindow scans backward minute-by-minute looking for the most
// recent minute in [start, end] where Matches returns true.
func findLastFireWindow(s Schedule, start, end time.Time, tz *time.Location) (time.Time, bool) {
	t := end.Truncate(time.Minute)
	startBound := start.Truncate(time.Minute)
	for t.After(startBound) || t.Equal(startBound) {
		if Matches(s, t, tz) {
			return t, true
		}
		t = t.Add(-1 * time.Minute)
	}
	return time.Time{}, false
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
