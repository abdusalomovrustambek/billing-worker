package invoice

import (
	"reflect"
	"testing"
	"time"
)

func TestNumber(t *testing.T) {
	tests := []struct {
		year int
		seq  int64
		want string
	}{
		{2026, 123, "INV-2026-000123"},
		{2026, 1, "INV-2026-000001"},
		{2027, 1234567, "INV-2027-1234567"},
	}
	for _, tt := range tests {
		if got := Number(tt.year, tt.seq); got != tt.want {
			t.Errorf("Number(%d, %d) = %q, want %q", tt.year, tt.seq, got, tt.want)
		}
	}
}

func TestLines(t *testing.T) {
	items := []Item{
		{SKU: "mug", Name: "Mug", UnitPriceCents: 1500, Quantity: 2},
		{SKU: "tee", Name: "T-shirt", UnitPriceCents: 1990, Quantity: 1},
	}

	got := Lines(items)

	want := []Line{
		{SKU: "mug", Name: "Mug", UnitPriceCents: 1500, Quantity: 2, TotalCents: 3000},
		{SKU: "tee", Name: "T-shirt", UnitPriceCents: 1990, Quantity: 1, TotalCents: 1990},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines = %+v, want %+v", got, want)
	}
}

func TestMoney(t *testing.T) {
	tests := []struct {
		cents int64
		want  string
	}{
		{4990, "49.90 USD"},
		{5, "0.05 USD"},
		{0, "0.00 USD"},
		{100, "1.00 USD"},
		{-250, "-2.50 USD"},
	}
	for _, tt := range tests {
		if got := Money(tt.cents, "USD"); got != tt.want {
			t.Errorf("Money(%d) = %q, want %q", tt.cents, got, tt.want)
		}
	}
}

func TestRender(t *testing.T) {
	inv := Invoice{
		Number:   "INV-2026-000123",
		OrderID:  812,
		Currency: "EUR",
		Lines: []Line{
			{SKU: "mug", Name: "Mug", UnitPriceCents: 1500, Quantity: 2, TotalCents: 3000},
		},
		SubtotalCents: 3000,
		TotalCents:    2700,
		CreatedAt:     time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC),
	}

	got := Render(inv)

	want := "Invoice INV-2026-000123\n" +
		"Order: 812\n" +
		"Date: 2026-11-03\n" +
		"\n" +
		"Mug (mug) 2 x 15.00 EUR = 30.00 EUR\n" +
		"\n" +
		"Subtotal: 30.00 EUR\n" +
		"Total: 27.00 EUR\n"
	if got != want {
		t.Errorf("Render =\n%s\nwant\n%s", got, want)
	}
}
