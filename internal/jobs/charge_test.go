package jobs

import "testing"

// A zero-quantity line must fail the totals check with an error, never with
// an integer divide by zero: validation runs before checkTotals, but the
// guard is what keeps a crafted payload from panicking the worker.
func TestCheckTotalsRejectsZeroQuantity(t *testing.T) {
	p := ChargeOrder{
		OrderID:  1,
		UserID:   7,
		Currency: "EUR",
		Items: []Item{
			{SKU: "mug", Name: "Mug", UnitPriceCents: 1500, Quantity: 0},
		},
		SubtotalCents: 0,
		TotalCents:    0,
		Card:          Card{Number: "4242 4242 4242 4242", Exp: "12/29", CVC: "123"},
	}
	if err := checkTotals(p); err == nil {
		t.Fatal("checkTotals accepted a zero-quantity line")
	}
}
