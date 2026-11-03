package logging

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLineFormat(t *testing.T) {
	var buf bytes.Buffer
	h := Component(New(&buf, slog.LevelInfo), "billing.jobs").Handler()

	// A time in another zone is written in UTC.
	at := time.Date(2026, 11, 3, 15, 0, 0, 123_000_000, time.FixedZone("UZT", 5*3600))
	rec := slog.NewRecord(at, slog.LevelInfo, "charged order 812 (49.90 USD)", 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	want := "2026-11-03T10:00:00.123Z INFO billing.jobs: charged order 812 (49.90 USD)\n"
	if got := buf.String(); got != want {
		t.Errorf("line = %q, want %q", got, want)
	}
}

func TestErrorWithAttributes(t *testing.T) {
	var buf bytes.Buffer
	log := Component(New(&buf, slog.LevelInfo), "billing.queue")

	log.Error("job failed", "kind", "charge_order", "id", 7, "error", errors.New("card declined"))

	re := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z ERROR billing\.queue: job failed kind=charge_order id=7 error="card declined"\n$`)
	if got := buf.String(); !re.MatchString(got) {
		t.Errorf("line = %q, want match of %s", got, re)
	}
}

func TestComponentOnRecord(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, slog.LevelInfo).Info("hello", "component", "billing.main")

	if got := buf.String(); !strings.Contains(got, " INFO billing.main: hello\n") {
		t.Errorf("line = %q, want component billing.main", got)
	}
}

func TestLevelsBelowThresholdAreDropped(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, slog.LevelWarn)

	log.Debug("debug line")
	log.Info("info line")
	log.Warn("warn line")
	log.Error("error line")

	got := buf.String()
	for _, dropped := range []string{"debug line", "info line"} {
		if strings.Contains(got, dropped) {
			t.Errorf("output contains %q, want it dropped:\n%s", dropped, got)
		}
	}
	for _, kept := range []string{" WARN warn line", " ERROR error line"} {
		if !strings.Contains(got, kept) {
			t.Errorf("output lacks %q:\n%s", kept, got)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"", slog.LevelInfo, false},
		{"info", slog.LevelInfo, false},
		{"DEBUG", slog.LevelDebug, false},
		{"warn", slog.LevelWarn, false},
		{"Error", slog.LevelError, false},
		{"loud", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseLevel(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseLevel(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
		}
		if err == nil && got != tt.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
