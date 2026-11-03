package cardnet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestChargeApproved(t *testing.T) {
	for _, card := range []string{"4242424242424242", "4242 4242 4242 4242", "4242-4242-4242-4242"} {
		ref, err := Charge(context.Background(), card, 4990)
		if err != nil {
			t.Fatalf("Charge(%q): %v", card, err)
		}
		if !strings.HasPrefix(ref, "net_") {
			t.Errorf("Charge(%q) ref = %q, want a net_ reference", card, ref)
		}
	}
}

func TestChargeDeclined(t *testing.T) {
	_, err := Charge(context.Background(), "4000 0000 0000 0002", 4990)
	if !errors.Is(err, ErrDeclined) {
		t.Fatalf("err = %v, want ErrDeclined", err)
	}
}

func TestChargeUnknownCard(t *testing.T) {
	_, err := Charge(context.Background(), "1234 5678 9012 3456", 4990)
	if !errors.Is(err, ErrDeclined) {
		t.Fatalf("err = %v, want ErrDeclined", err)
	}
	if !strings.Contains(err.Error(), "invalid card") {
		t.Errorf("err = %q, want it to say invalid card", err)
	}
}

func TestChargeTimeout(t *testing.T) {
	old := timeoutDelay
	timeoutDelay = 10 * time.Millisecond
	t.Cleanup(func() { timeoutDelay = old })

	_, err := Charge(context.Background(), "4000 0000 0000 0119", 4990)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}

func TestChargeTimeoutReturnsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := Charge(ctx, "4000000000000119", 4990)
	if !errors.Is(err, ErrTimeout) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrTimeout wrapping context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed >= timeoutDelay {
		t.Errorf("Charge took %v, want it to return before the fake delay %v", elapsed, timeoutDelay)
	}
}

func TestLast4(t *testing.T) {
	tests := map[string]string{
		"4242 4242 4242 4242": "4242",
		"4000-0000-0000-0002": "0002",
		"12":                  "12",
	}
	for card, want := range tests {
		if got := Last4(card); got != want {
			t.Errorf("Last4(%q) = %q, want %q", card, got, want)
		}
	}
}
