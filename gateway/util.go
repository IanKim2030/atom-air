package main

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// isoNow renders the timestamp format the cloud and the licence cache use.
func isoNow() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

// parseISO is deliberately tolerant: it accepts RFC 3339 with Z or an offset,
// and also sqlite's "YYYY-MM-DD HH:MM:SS", which an operator editing the
// database by hand will produce.
func parseISO(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
func round1(v float64) float64 { return math.Round(v*10) / 10 }

func itoa(v int) string { return strconv.Itoa(v) }

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// redactToken strips the auth token from a URL before it reaches the log.
func redactToken(url string) string {
	if i := strings.Index(url, "?token="); i >= 0 {
		return url[:i] + "?token=***"
	}
	return url
}
