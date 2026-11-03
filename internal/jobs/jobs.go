// Package jobs holds the handlers for the job kinds that shop-api and the
// worker itself put on the queue.
package jobs

import (
	"log/slog"
	"time"

	"github.com/shop/billing-worker/internal/queue"
)

// Job kinds.
const (
	KindChargeOrder    = "charge_order"
	KindConvertInvoice = "convert_invoice"
)

// Handlers implements the job kinds. Log should carry the billing.jobs
// component.
type Handlers struct {
	Log *slog.Logger
	Now func() time.Time
}

// New returns Handlers that log to log and use the real clock.
func New(log *slog.Logger) *Handlers {
	return &Handlers{Log: log, Now: time.Now}
}

// Registry maps each job kind to its handler.
func (h *Handlers) Registry() map[string]queue.Handler {
	return map[string]queue.Handler{
		KindChargeOrder:    {Run: h.ChargeOrder, OnDead: h.ChargeOrderDead},
		KindConvertInvoice: {Run: h.ConvertInvoice},
	}
}
