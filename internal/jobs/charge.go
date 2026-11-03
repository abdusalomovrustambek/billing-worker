package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/shop/billing-worker/internal/cardnet"
	"github.com/shop/billing-worker/internal/invoice"
	"github.com/shop/billing-worker/internal/queue"
)

// ChargeOrder charges the card of an order. All its writes use the job's
// transaction, so a decline or timeout leaves nothing behind and the job is
// retried.
func (h *Handlers) ChargeOrder(ctx context.Context, tx pgx.Tx, raw []byte) error {
	var p ChargeOrder
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("decode charge_order payload: %w", err)
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("invalid charge_order payload: %w", err)
	}
	if err := checkTotals(p); err != nil {
		return fmt.Errorf("order %d: %w", p.OrderID, err)
	}

	var charged bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM billing.charges WHERE order_id = $1 AND result = 'approved')`,
		p.OrderID).Scan(&charged)
	if err != nil {
		return fmt.Errorf("look up charge of order %d: %w", p.OrderID, err)
	}
	if charged {
		h.Log.Info(fmt.Sprintf("order %d is already charged", p.OrderID))
		return nil
	}

	ref, err := cardnet.Charge(ctx, p.Card.Number, p.TotalCents)
	if err != nil {
		if errors.Is(err, cardnet.ErrDeclined) {
			h.Log.Error(fmt.Sprintf("charge declined for order %d", p.OrderID), "payload", string(raw))
		}
		return fmt.Errorf("charge order %d: %w", p.OrderID, err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO billing.charges (order_id, amount_cents, currency, card_last4, result, network_ref)
		VALUES ($1, $2, 'USD', $3, 'approved', $4)`,
		p.OrderID, p.TotalCents, cardnet.Last4(p.Card.Number), ref)
	if err != nil {
		return fmt.Errorf("record charge of order %d: %w", p.OrderID, err)
	}

	tag, err := tx.Exec(ctx, `UPDATE shop.orders SET status = 'paid' WHERE id = $1`, p.OrderID)
	if err != nil {
		return fmt.Errorf("mark order %d paid: %w", p.OrderID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark order %d paid: order not found", p.OrderID)
	}

	next := ConvertInvoice{
		OrderID:       p.OrderID,
		Currency:      p.Currency,
		Items:         p.Items,
		SubtotalCents: p.SubtotalCents,
		DiscountCents: p.DiscountCents,
		TotalCents:    p.TotalCents,
	}
	if err := queue.Enqueue(ctx, tx, KindConvertInvoice, next); err != nil {
		return err
	}

	h.Log.Info(fmt.Sprintf("charged order %d (%s)", p.OrderID, invoice.Money(p.TotalCents, "USD")))
	return nil
}

// ChargeOrderDead marks the order failed when its charge_order job is dead,
// so a handled failure cannot leave the order pending for ever.
func (h *Handlers) ChargeOrderDead(ctx context.Context, tx pgx.Tx, raw []byte) error {
	var p struct {
		OrderID int64 `json:"order_id"`
	}
	// A payload that cannot be read names no order to mark.
	if json.Unmarshal(raw, &p) != nil || p.OrderID <= 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE shop.orders SET status = 'failed' WHERE id = $1`, p.OrderID); err != nil {
		return fmt.Errorf("mark order %d failed: %w", p.OrderID, err)
	}
	return nil
}

// checkTotals checks the lines against the order totals: each line's total
// must match its unit price, the lines must add up to the subtotal, and the
// subtotal minus the discount must be the total.
func checkTotals(p ChargeOrder) error {
	var sum int64
	for _, l := range invoice.Lines(invoiceItems(p.Items)) {
		if l.TotalCents/l.Quantity != l.UnitPriceCents {
			return fmt.Errorf("sku %s: line total %d does not match unit price %d", l.SKU, l.TotalCents, l.UnitPriceCents)
		}
		sum += l.TotalCents
	}
	if sum != p.SubtotalCents {
		return fmt.Errorf("lines add up to %d, subtotal is %d", sum, p.SubtotalCents)
	}
	if p.SubtotalCents-p.DiscountCents != p.TotalCents {
		return fmt.Errorf("subtotal %d minus discount %d is not total %d", p.SubtotalCents, p.DiscountCents, p.TotalCents)
	}
	return nil
}
