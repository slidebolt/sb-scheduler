package app

import (
	"testing"
	"time"

	domain "github.com/slidebolt/sb-domain"
)

func TestNextTimeSkipsPastOneShot(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	if got := nextTime(domain.Trigger{Kind: "at", At: &past}, now); got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestNextTimeCronTimezone(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got := nextTime(domain.Trigger{Kind: "cron", Cron: "0 19 * * *", TimeZone: "America/Chicago"}, now)
	want := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if got == nil || !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestValidateScheduleRejectsDuplicateStepIDs(t *testing.T) {
	at := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	step := domain.ScheduleStep{ID: "run", Trigger: domain.Trigger{Kind: "at", At: &at}, Actions: []domain.Action{{Target: "plugin.device.switch", Command: "switch_turn_on"}}}
	err := validateSchedule(domain.Schedule{Enabled: true, Steps: []domain.ScheduleStep{step, step}})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
