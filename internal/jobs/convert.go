package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/shop/billing-worker/internal/fx"
	"github.com/shop/billing-worker/internal/invoice"
)

// ConvertInvoice converts the order totals from USD to the order currency
// and writes the invoice.
func (h *Handlers) ConvertInvoice(ctx context.Context, tx pgx.Tx, raw []byte) error {
	var p ConvertInvoice
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("decode convert_invoice payload: %w", err)
	}
	if err := p.Validate(); err != nil {
		return fmt.Errorf("invalid convert_invoice payload: %w", err)
	}

	usd, err := fx.Rate(ctx, tx, "USD")
	if err != nil {
		return err
	}
	target, err := fx.Rate(ctx, tx, p.Currency)
	if err != nil {
		return err
	}

	items := make([]invoice.Item, 0, len(p.Items))
	for _, it := range invoiceItems(p.Items) {
		it.UnitPriceCents, err = fx.Convert(it.UnitPriceCents, usd, target)
		if err != nil {
			return fmt.Errorf("convert price of %s: %w", it.SKU, err)
		}
		items = append(items, it)
	}
	subtotal, err := fx.Convert(p.SubtotalCents, usd, target)
	if err != nil {
		return fmt.Errorf("convert subtotal: %w", err)
	}
	total, err := fx.Convert(p.TotalCents, usd, target)
	if err != nil {
		return fmt.Errorf("convert total: %w", err)
	}

	var seq int64
	if err := tx.QueryRow(ctx, `SELECT nextval('billing.invoice_seq')`).Scan(&seq); err != nil {
		return fmt.Errorf("next invoice number: %w", err)
	}
	number := invoice.Number(h.Now().UTC().Year(), seq)
	lines, err := json.Marshal(invoice.Lines(items))
	if err != nil {
		return fmt.Errorf("marshal invoice lines: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO billing.invoices (number, order_id, currency, subtotal_cents, total_cents, lines)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		number, p.OrderID, p.Currency, subtotal, total, lines)
	if err != nil {
		return fmt.Errorf("write invoice %s: %w", number, err)
	}

	h.Log.Info(fmt.Sprintf("invoice %s written", number), "order_id", p.OrderID, "total", invoice.Money(total, p.Currency))
	if total == 0 {
		h.Log.Error(fmt.Sprintf("invoice %s total is 0", number), "order_id", p.OrderID, "currency", p.Currency)
	}
	return nil
}
