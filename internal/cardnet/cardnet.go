// Package cardnet is a fake card network. The outcome depends only on the
// card number, so tests and demos can pick it.
package cardnet

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

const (
	cardApproved = "4242424242424242"
	cardDeclined = "4000000000000002"
	cardTimeout  = "4000000000000119"
)

var (
	// ErrDeclined is returned when the network declines the card.
	ErrDeclined = errors.New("card declined")
	// ErrTimeout is returned when the network does not answer in time.
	ErrTimeout = errors.New("card network timeout")
)

// timeoutDelay is how long the fake network keeps a timing-out charge
// waiting before it gives up on its own.
var timeoutDelay = 2 * time.Second

// Normalize removes spaces and dashes from a card number.
func Normalize(card string) string {
	return strings.NewReplacer(" ", "", "-", "").Replace(card)
}

// Last4 returns the last four digits of a card number.
func Last4(card string) string {
	n := Normalize(card)
	if len(n) < 4 {
		return n
	}
	return n[len(n)-4:]
}

// Charge asks the network to charge amountCents to the card and returns the
// network reference of an approved charge. A decline wraps ErrDeclined; a
// timeout, or a cancelled ctx while waiting, wraps ErrTimeout.
func Charge(ctx context.Context, card string, amountCents int64) (string, error) {
	switch Normalize(card) {
	case cardApproved:
		return fmt.Sprintf("net_%016x", rand.Uint64()), nil
	case cardDeclined:
		return "", ErrDeclined
	case cardTimeout:
		t := time.NewTimer(timeoutDelay)
		defer t.Stop()
		select {
		case <-t.C:
			return "", ErrTimeout
		case <-ctx.Done():
			return "", fmt.Errorf("%w: %w", ErrTimeout, ctx.Err())
		}
	default:
		return "", fmt.Errorf("%w: invalid card", ErrDeclined)
	}
}
