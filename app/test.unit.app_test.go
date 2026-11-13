package app

import (
	"testing"
	"time"

	domain "github.com/slidebolt/sb-domain"
)

func TestNextTimeSkipsPastOneShot(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	if got := nextTime(domain.TimerTrigger{Kind: "at", At: &past}, now); got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestNextTimeCronTimezone(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := nextTime(domain.TimerTrigger{Kind: "cron", Cron: "0 19 * * *", TimeZone: "America/Chicago"}, now)
	want := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if got == nil || !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestValidateTimerRejectsReversedOneShot(t *testing.T) {
	on := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	off := on.Add(-time.Hour)
	err := validateTimer(domain.Timer{Target: "plugin.device.switch", On: domain.TimerTrigger{Kind: "at", At: &on}, Off: domain.TimerTrigger{Kind: "at", At: &off}})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
