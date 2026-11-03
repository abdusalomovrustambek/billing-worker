// Package fx looks up exchange rates and converts amounts. Money is int64
// cents and rates are int64 parts per million of one USD; there are no
// floats.
package fx

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
)

// Querier is the part of a pool, connection or transaction that Rate needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Rate returns the rate of currency in ppm per 1 USD.
func Rate(ctx context.Context, q Querier, currency string) (int64, error) {
	var ppm int64
	err := q.QueryRow(ctx, `SELECT rate_ppm FROM billing.fx_rates WHERE currency = $1`, currency).Scan(&ppm)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load rate %s: %w", currency, err)
	}
	return ppm, nil
}

// Convert converts cents from the currency with rate fromPPM to the one with
// rate toPPM, rounding half up. The cents × ppm product is computed with
// big.Int, so it cannot overflow; an error is returned if the result does not
// fit in int64.
func Convert(cents, fromPPM, toPPM int64) (int64, error) {
	if cents < 0 {
		return 0, fmt.Errorf("negative amount %d", cents)
	}
	if fromPPM <= 0 {
		return 0, fmt.Errorf("invalid source rate %d", fromPPM)
	}
	// floor((2*cents*to + from) / (2*from)) is cents*to/from rounded half up.
	num := new(big.Int).Mul(big.NewInt(cents), big.NewInt(toPPM))
	num.Lsh(num, 1)
	num.Add(num, big.NewInt(fromPPM))
	den := new(big.Int).Lsh(big.NewInt(fromPPM), 1)
	num.Quo(num, den)
	if !num.IsInt64() {
		return 0, fmt.Errorf("converted amount of %d cents overflows", cents)
	}
	return num.Int64(), nil
}
