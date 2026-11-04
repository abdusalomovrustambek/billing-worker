package jobs

import (
	"errors"
	"fmt"

	"github.com/shop/billing-worker/internal/invoice"
)

// Item is one ordered item of a charge_order payload.
type Item struct {
	SKU            string `json:"sku"`
	Name           string `json:"name"`
	UnitPriceCents int64  `json:"unit_price_cents"`
	Quantity       int64  `json:"quantity"`
}

// Card is the card data of a charge_order payload.
type Card struct {
	Number string `json:"number"`
	Exp    string `json:"exp"`
	CVC    string `json:"cvc"`
}

// ChargeOrder is the payload shop-api inserts for a charge_order job. Its
// field names are the contract with shop-api. Amounts are USD cents.
type ChargeOrder struct {
	OrderID       int64  `json:"order_id"`
	UserID        int64  `json:"user_id"`
	Currency      string `json:"currency"`
	Items         []Item `json:"items"`
	SubtotalCents int64  `json:"subtotal_cents"`
	DiscountCents int64  `json:"discount_cents"`
	TotalCents    int64  `json:"total_cents"`
	Card          Card   `json:"card"`
}

// ConvertInvoice is the payload of a convert_invoice job: the order data
// without the card. Amounts are USD cents; Currency is the one to convert to.
type ConvertInvoice struct {
	OrderID       int64  `json:"order_id"`
	Currency      string `json:"currency"`
	Items         []Item `json:"items"`
	SubtotalCents int64  `json:"subtotal_cents"`
	DiscountCents int64  `json:"discount_cents"`
	TotalCents    int64  `json:"total_cents"`
}

// Validate checks that the required fields are present.
func (p ChargeOrder) Validate() error {
	if p.OrderID <= 0 {
		return errors.New("order_id is required")
	}
	if p.UserID <= 0 {
		return errors.New("user_id is required")
	}
	if p.Currency == "" {
		return errors.New("currency is required")
	}
	if err := validateItems(p.Items); err != nil {
		return err
	}
	if p.Card.Number == "" || p.Card.Exp == "" || p.Card.CVC == "" {
		return errors.New("card number, exp and cvc are required")
	}
	return nil
}

// Validate checks that the required fields are present.
func (p ConvertInvoice) Validate() error {
	if p.OrderID <= 0 {
		return errors.New("order_id is required")
	}
	if p.Currency == "" {
		return errors.New("currency is required")
	}
	return validateItems(p.Items)
}

func validateItems(items []Item) error {
	if len(items) == 0 {
		return errors.New("items are required")
	}
	for i, it := range items {
		if it.SKU == "" {
			return fmt.Errorf("items[%d]: sku is required", i)
		}
	}
	return nil
}

func invoiceItems(items []Item) []invoice.Item {
	out := make([]invoice.Item, 0, len(items))
	for _, it := range items {
		out = append(out, invoice.Item{
			SKU:            it.SKU,
			Name:           it.Name,
			UnitPriceCents: it.UnitPriceCents,
			Quantity:       it.Quantity,
		})
	}
	return out
}
