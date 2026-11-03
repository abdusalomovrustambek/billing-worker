// Package invoice builds invoice numbers and lines and renders an invoice as
// plain text. It is pure: no database, no clock.
package invoice

import (
	"fmt"
	"strings"
	"time"
)

// Item is one ordered item, priced per unit.
type Item struct {
	SKU            string
	Name           string
	UnitPriceCents int64
	Quantity       int64
}

// Line is one invoice line; it is stored as JSON in billing.invoices.lines.
type Line struct {
	SKU            string `json:"sku"`
	Name           string `json:"name"`
	UnitPriceCents int64  `json:"unit_price_cents"`
	Quantity       int64  `json:"quantity"`
	TotalCents     int64  `json:"total_cents"`
}

// Invoice is everything Render needs.
type Invoice struct {
	Number        string
	OrderID       int64
	Currency      string
	Lines         []Line
	SubtotalCents int64
	TotalCents    int64
	CreatedAt     time.Time
}

// Number formats an invoice number such as INV-2026-000123.
func Number(year int, seq int64) string {
	return fmt.Sprintf("INV-%d-%06d", year, seq)
}

// Lines builds one line per item, with total = unit price × quantity.
func Lines(items []Item) []Line {
	lines := make([]Line, 0, len(items))
	for _, it := range items {
		lines = append(lines, Line{
			SKU:            it.SKU,
			Name:           it.Name,
			UnitPriceCents: it.UnitPriceCents,
			Quantity:       it.Quantity,
			TotalCents:     it.UnitPriceCents * it.Quantity,
		})
	}
	return lines
}

// Money formats cents as "49.90 USD" using integer maths only.
func Money(cents int64, currency string) string {
	sign := ""
	if cents < 0 {
		sign = "-"
		cents = -cents
	}
	return fmt.Sprintf("%s%d.%02d %s", sign, cents/100, cents%100, currency)
}

// Render returns the invoice as plain text.
func Render(inv Invoice) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Invoice %s\n", inv.Number)
	fmt.Fprintf(&b, "Order: %d\n", inv.OrderID)
	fmt.Fprintf(&b, "Date: %s\n\n", inv.CreatedAt.UTC().Format("2006-01-02"))
	for _, l := range inv.Lines {
		fmt.Fprintf(&b, "%s (%s) %d x %s = %s\n", l.Name, l.SKU, l.Quantity,
			Money(l.UnitPriceCents, inv.Currency), Money(l.TotalCents, inv.Currency))
	}
	fmt.Fprintf(&b, "\nSubtotal: %s\n", Money(inv.SubtotalCents, inv.Currency))
	fmt.Fprintf(&b, "Total: %s\n", Money(inv.TotalCents, inv.Currency))
	return b.String()
}
