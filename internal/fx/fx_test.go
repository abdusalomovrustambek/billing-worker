package fx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

const (
	usd = 1_000_000
	eur = 920_000
	gbp = 790_000
)

func TestConvert(t *testing.T) {
	tests := []struct {
		name            string
		cents, from, to int64
		want            int64
	}{
		{"usd to eur rounds up", 4990, usd, eur, 4591},
		{"usd to eur rounds down", 7, usd, eur, 6},
		{"half rounds up", 5, usd, 500_000, 3},
		{"below half rounds down", 3, usd, 400_000, 1},
		{"eur to gbp", 10_000, eur, gbp, 8587},
		{"usd to usd", 4990, usd, usd, 4990},
		{"zero stays zero", 0, usd, eur, 0},
		{"large amount", 1_000_000_000_000, usd, eur, 920_000_000_000},
		{"large amount between currencies", 1_000_000_000_000, eur, gbp, 858_695_652_174},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert(tt.cents, tt.from, tt.to)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if got != tt.want {
				t.Errorf("Convert(%d, %d, %d) = %d, want %d", tt.cents, tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestConvertDoesNotOverflow(t *testing.T) {
	// 4e18 cents × 500000 ppm is far above MaxInt64 before the division.
	got, err := Convert(4_000_000_000_000_000_000, usd, 500_000)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if want := int64(2_000_000_000_000_000_000); got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

// stubQuerier returns a fixed Scan result for the rate lookup, so Rate can
// be tested without a database.
type stubQuerier struct {
	ppm int64
	err error
}

// stubRateRow scans the stubbed rate answer of the fx_rates lookup.
type stubRateRow struct {
	ppm int64
	err error
}

func (q stubQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return stubRateRow(q)
}

func (r stubRateRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	ppm, ok := dest[0].(*int64)
	if !ok || len(dest) != 1 {
		return errors.New("stub row scans one *int64")
	}
	*ppm = r.ppm
	return nil
}

func TestRateKnownCurrency(t *testing.T) {
	got, err := Rate(context.Background(), stubQuerier{ppm: eur}, "EUR")
	if err != nil {
		t.Fatalf("Rate(EUR): %v", err)
	}
	if got != eur {
		t.Errorf("Rate(EUR) = %d, want %d", got, eur)
	}
}

func TestRateUnknownCurrency(t *testing.T) {
	// An unseeded currency must come back as an error, not as a zero rate:
	// a zero rate converts every amount to 0 with no error.
	rate, err := Rate(context.Background(), stubQuerier{err: pgx.ErrNoRows}, "UZS")
	if err == nil {
		t.Fatalf("Rate(UZS) = %d, want an unknown-currency error", rate)
	}
	if !strings.Contains(err.Error(), "UZS") {
		t.Errorf("Rate(UZS) error = %q, want it to name the currency", err)
	}
}

func TestConvertErrors(t *testing.T) {
	tests := []struct {
		name            string
		cents, from, to int64
	}{
		{"negative amount", -1, usd, eur},
		{"zero source rate", 100, 0, eur},
		{"result does not fit", 9_000_000_000_000_000_000, 1, 1_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Convert(tt.cents, tt.from, tt.to); err == nil {
				t.Error("Convert returned no error")
			}
		})
	}
}
