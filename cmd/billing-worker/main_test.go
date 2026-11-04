package main

import (
	"errors"
	"flag"
	"testing"
	"time"
)

func TestParsePurgeArgsDefaultsToThirtyDays(t *testing.T) {
	olderThan, err := parsePurgeArgs(nil)
	if err != nil {
		t.Fatalf("parsePurgeArgs(nil): %v", err)
	}
	if olderThan != defaultPurgeOlderThan {
		t.Errorf("older-than = %v, want the default %v", olderThan, defaultPurgeOlderThan)
	}
}

func TestParsePurgeArgsAcceptsOlderThan(t *testing.T) {
	olderThan, err := parsePurgeArgs([]string{"--older-than", "48h"})
	if err != nil {
		t.Fatalf("parsePurgeArgs: %v", err)
	}
	if olderThan != 48*time.Hour {
		t.Errorf("older-than = %v, want 48h", olderThan)
	}
}

func TestParsePurgeArgsRejectsBadInput(t *testing.T) {
	for _, args := range [][]string{
		{"--older-than", "soon"},
		{"--older-than", "-1h"},
		{"--bogus"},
		{"extra"},
	} {
		if _, err := parsePurgeArgs(args); err == nil {
			t.Errorf("parsePurgeArgs(%v) = nil, want an error", args)
		}
	}
}

func TestParsePurgeArgsHelpIsNotAnError(t *testing.T) {
	if _, err := parsePurgeArgs([]string{"-h"}); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("parsePurgeArgs(-h) = %v, want flag.ErrHelp", err)
	}
}
