package jobs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shop/billing-worker/internal/dbtest"
	"github.com/shop/billing-worker/internal/jobs"
	"github.com/shop/billing-worker/internal/logging"
	"github.com/shop/billing-worker/internal/queue"
)

const (
	cardApproved = "4242 4242 4242 4242"
	cardDeclined = "4000 0000 0000 0002"
	orderID      = 812
)

type env struct {
	pool *pgxpool.Pool
	h    *jobs.Handlers
	logs *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := dbtest.New(t)
	logs := &bytes.Buffer{}
	h := jobs.New(logging.Component(logging.New(logs, slog.LevelDebug), "billing.jobs"))
	h.Now = func() time.Time { return time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC) }
	return &env{pool: pool, h: h, logs: logs}
}

func chargePayload(card string) jobs.ChargeOrder {
	return jobs.ChargeOrder{
		OrderID:  orderID,
		UserID:   7,
		Currency: "EUR",
		Items: []jobs.Item{
			{SKU: "mug", Name: "Mug", UnitPriceCents: 1500, Quantity: 2},
			{SKU: "tee", Name: "T-shirt", UnitPriceCents: 1990, Quantity: 1},
		},
		SubtotalCents: 4990,
		TotalCents:    4990,
		Card:          jobs.Card{Number: card, Exp: "12/29", CVC: "123"},
	}
}

func (e *env) addJob(t *testing.T, kind string, payload any) int64 {
	t.Helper()
	b, ok := payload.([]byte)
	if !ok {
		var err error
		if b, err = json.Marshal(payload); err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
	}
	var id int64
	err := e.pool.QueryRow(context.Background(),
		`INSERT INTO billing.jobs (kind, payload) VALUES ($1, $2) RETURNING id`, kind, b).Scan(&id)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return id
}

// runNext does what a worker does for one job: claim it, run its handler in
// the job's transaction, then complete it or record the failure. It returns
// the handler's error.
func (e *env) runNext(t *testing.T) error {
	t.Helper()
	ctx := context.Background()
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()

	tx, job, err := queue.Claim(ctx, conn)
	if err != nil || job == nil {
		t.Fatalf("Claim = %v, %v, want a job", job, err)
	}
	h := e.h.Registry()[job.Kind]
	if err := h.Run(ctx, tx, job.Payload); err != nil {
		if err := tx.Rollback(ctx); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if _, ferr := queue.Fail(ctx, conn, job, err, h.OnDead); ferr != nil {
			t.Fatalf("Fail: %v", ferr)
		}
		return err
	}
	if err := queue.Complete(ctx, tx, job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return nil
}

func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (e *env) jobState(t *testing.T, id int64) (status string, attempts int) {
	t.Helper()
	err := e.pool.QueryRow(context.Background(),
		`SELECT status, attempts FROM billing.jobs WHERE id = $1`, id).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("job %d: %v", id, err)
	}
	return status, attempts
}

func TestChargeOrderApproved(t *testing.T) {
	e := newEnv(t)
	dbtest.InsertOrder(t, e.pool, orderID)
	id := e.addJob(t, jobs.KindChargeOrder, chargePayload(cardApproved))

	if err := e.runNext(t); err != nil {
		t.Fatalf("charge_order: %v", err)
	}

	if status, _ := e.jobState(t, id); status != "done" {
		t.Errorf("job status = %q, want done", status)
	}
	var amount int64
	var last4, result, ref string
	err := e.pool.QueryRow(context.Background(),
		`SELECT amount_cents, card_last4, result, network_ref FROM billing.charges WHERE order_id = $1`, orderID).
		Scan(&amount, &last4, &result, &ref)
	if err != nil {
		t.Fatalf("select charge: %v", err)
	}
	if amount != 4990 || last4 != "4242" || result != "approved" || ref == "" {
		t.Errorf("charge = %d %q %q %q, want 4990 4242 approved and a reference", amount, last4, result, ref)
	}
	if got := dbtest.OrderStatus(t, e.pool, orderID); got != "paid" {
		t.Errorf("order status = %q, want paid", got)
	}
	if want := "INFO billing.jobs: charged order 812 (49.90 USD)"; !strings.Contains(e.logs.String(), want) {
		t.Errorf("log lacks %q:\n%s", want, e.logs)
	}

	var raw []byte
	err = e.pool.QueryRow(context.Background(),
		`SELECT payload FROM billing.jobs WHERE kind = 'convert_invoice' AND status = 'queued'`).Scan(&raw)
	if err != nil {
		t.Fatalf("select convert_invoice job: %v", err)
	}
	var next map[string]any
	if err := json.Unmarshal(raw, &next); err != nil {
		t.Fatal(err)
	}
	if _, ok := next["card"]; ok || strings.Contains(string(raw), "4242") {
		t.Errorf("convert_invoice payload carries card data: %s", raw)
	}
	if next["order_id"] != float64(orderID) || next["currency"] != "EUR" {
		t.Errorf("convert_invoice payload = %s", raw)
	}
}

func TestChargeOrderThenConvertInvoice(t *testing.T) {
	e := newEnv(t)
	dbtest.InsertOrder(t, e.pool, orderID)
	e.addJob(t, jobs.KindChargeOrder, chargePayload(cardApproved))

	if err := e.runNext(t); err != nil {
		t.Fatalf("charge_order: %v", err)
	}
	if err := e.runNext(t); err != nil {
		t.Fatalf("convert_invoice: %v", err)
	}

	if n := e.count(t, `SELECT count(*) FROM billing.jobs WHERE status = 'done'`); n != 2 {
		t.Errorf("%d jobs done, want 2", n)
	}
	if want := "INFO billing.jobs: invoice INV-2026-000001 written"; !strings.Contains(e.logs.String(), want) {
		t.Errorf("log lacks %q:\n%s", want, e.logs)
	}
}

func TestConvertInvoiceEUR(t *testing.T) {
	e := newEnv(t)
	p := chargePayload(cardApproved)
	e.addJob(t, jobs.KindConvertInvoice, jobs.ConvertInvoice{
		OrderID:       p.OrderID,
		Currency:      "EUR",
		Items:         p.Items,
		SubtotalCents: p.SubtotalCents,
		TotalCents:    p.TotalCents,
	})

	if err := e.runNext(t); err != nil {
		t.Fatalf("convert_invoice: %v", err)
	}

	var number, currency string
	var subtotal, total int64
	var lines []struct {
		SKU            string `json:"sku"`
		UnitPriceCents int64  `json:"unit_price_cents"`
		Quantity       int64  `json:"quantity"`
		TotalCents     int64  `json:"total_cents"`
	}
	err := e.pool.QueryRow(context.Background(),
		`SELECT number, currency, subtotal_cents, total_cents, lines FROM billing.invoices WHERE order_id = $1`, orderID).
		Scan(&number, &currency, &subtotal, &total, &lines)
	if err != nil {
		t.Fatalf("select invoice: %v", err)
	}
	// 4990 USD cents at 0.92 EUR per USD is 4590.8, rounded half up.
	if number != "INV-2026-000001" || currency != "EUR" || subtotal != 4591 || total != 4591 {
		t.Errorf("invoice = %s %s %d %d, want INV-2026-000001 EUR 4591 4591", number, currency, subtotal, total)
	}
	if len(lines) != 2 || lines[0].SKU != "mug" || lines[0].UnitPriceCents != 1380 || lines[0].Quantity != 2 || lines[0].TotalCents != 2760 {
		t.Errorf("lines = %+v", lines)
	}
	if want := "INFO billing.jobs: invoice INV-2026-000001 written"; !strings.Contains(e.logs.String(), want) {
		t.Errorf("log lacks %q:\n%s", want, e.logs)
	}
}

func TestDeclinedChargeRetriesAndOrderFailsOnlyWhenDead(t *testing.T) {
	e := newEnv(t)
	dbtest.InsertOrder(t, e.pool, orderID)
	id := e.addJob(t, jobs.KindChargeOrder, chargePayload(cardDeclined))
	ctx := context.Background()

	for attempt := 1; attempt <= queue.MaxAttempts; attempt++ {
		if _, err := e.pool.Exec(ctx, `UPDATE billing.jobs SET run_after = now() WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		if err := e.runNext(t); err == nil {
			t.Fatalf("attempt %d: charge_order succeeded with a declined card", attempt)
		}

		status, attempts := e.jobState(t, id)
		if attempts != attempt {
			t.Errorf("attempt %d: attempts = %d", attempt, attempts)
		}
		wantStatus, wantOrder := "queued", "pending"
		if attempt == queue.MaxAttempts {
			wantStatus, wantOrder = "dead", "failed"
		}
		if status != wantStatus {
			t.Errorf("attempt %d: job status = %q, want %q", attempt, status, wantStatus)
		}
		if got := dbtest.OrderStatus(t, e.pool, orderID); got != wantOrder {
			t.Errorf("attempt %d: order status = %q, want %q", attempt, got, wantOrder)
		}
		if n := e.count(t, `SELECT count(*) FROM billing.charges`); n != 0 {
			t.Errorf("attempt %d: %d charge rows left", attempt, n)
		}
	}

	var lastError string
	if err := e.pool.QueryRow(ctx, `SELECT last_error FROM billing.jobs WHERE id = $1`, id).Scan(&lastError); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lastError, "card declined") {
		t.Errorf("last_error = %q, want it to say the card was declined", lastError)
	}
	if n := e.count(t, `SELECT count(*) FROM billing.jobs WHERE kind = 'convert_invoice'`); n != 0 {
		t.Errorf("%d convert_invoice jobs enqueued for a failed order", n)
	}
}

// stubTx is a pgx.Tx that reports no earlier approved charge, so ChargeOrder
// reaches the card network without a database. Anything beyond the EXISTS
// lookup panics: a declined charge never gets there.
type stubTx struct {
	pgx.Tx
}

// stubExistsRow scans the not-charged-before answer of the EXISTS lookup.
type stubExistsRow struct{}

func (stubTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row { return stubExistsRow{} }

func (stubExistsRow) Scan(dest ...any) error {
	charged, ok := dest[0].(*bool)
	if !ok || len(dest) != 1 {
		return errors.New("stub row scans one *bool")
	}
	*charged = false
	return nil
}

func TestDeclinedChargeLogsOnlyCardLast4(t *testing.T) {
	logs := &bytes.Buffer{}
	h := jobs.New(logging.Component(logging.New(logs, slog.LevelDebug), "billing.jobs"))

	raw, err := json.Marshal(chargePayload(cardDeclined))
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	err = h.ChargeOrder(context.Background(), stubTx{}, raw)
	if err == nil {
		t.Fatal("ChargeOrder succeeded with a declined card")
	}
	if strings.Contains(err.Error(), "4000") {
		t.Errorf("returned error leaks card data: %v", err)
	}

	got := logs.String()
	for _, want := range []string{"charge declined for order 812", "card_last4=0002"} {
		if !strings.Contains(got, want) {
			t.Errorf("log lacks %q:\n%s", want, got)
		}
	}
	for _, leak := range []string{cardDeclined, "12/29", "cvc", "payload="} {
		if strings.Contains(got, leak) {
			t.Errorf("decline log leaks card data %q:\n%s", leak, got)
		}
	}
}

func TestChargeOrderRollsBackWhenOrderIsMissing(t *testing.T) {
	e := newEnv(t)
	id := e.addJob(t, jobs.KindChargeOrder, chargePayload(cardApproved))

	if err := e.runNext(t); err == nil {
		t.Fatal("charge_order succeeded without an order")
	}

	if n := e.count(t, `SELECT count(*) FROM billing.charges`); n != 0 {
		t.Errorf("%d charge rows left after the rollback", n)
	}
	if status, attempts := e.jobState(t, id); status != "queued" || attempts != 1 {
		t.Errorf("job = %q after %d attempts, want queued after 1", status, attempts)
	}
}

func TestChargeOrderIsIdempotent(t *testing.T) {
	e := newEnv(t)
	dbtest.InsertOrder(t, e.pool, orderID)
	e.addJob(t, jobs.KindChargeOrder, chargePayload(cardApproved))
	for _, kind := range []string{jobs.KindChargeOrder, jobs.KindConvertInvoice} {
		if err := e.runNext(t); err != nil {
			t.Fatalf("first %s: %v", kind, err)
		}
	}

	// The same order arrives again, for example after a retry that found
	// the first run's charge already committed.
	again := e.addJob(t, jobs.KindChargeOrder, chargePayload(cardApproved))
	if err := e.runNext(t); err != nil {
		t.Fatalf("repeated charge_order: %v", err)
	}

	if status, _ := e.jobState(t, again); status != "done" {
		t.Errorf("repeated job status = %q, want done", status)
	}
	if n := e.count(t, `SELECT count(*) FROM billing.charges WHERE order_id = $1`, orderID); n != 1 {
		t.Errorf("%d charges for the order, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM billing.jobs WHERE kind = 'convert_invoice'`); n != 1 {
		t.Errorf("%d convert_invoice jobs, want 1", n)
	}
}

func TestInvalidPayloadFailsTheAttempt(t *testing.T) {
	tests := []struct {
		name    string
		payload func() any
	}{
		{"wrong field type", func() any { return []byte(`{"order_id": "abc"}`) }},
		{"incomplete payload", func() any { return []byte(`{}`) }},
		{"subtotal does not match lines", func() any {
			p := chargePayload(cardApproved)
			p.SubtotalCents, p.TotalCents = 5000, 5000
			return p
		}},
		{"total does not match subtotal minus discount", func() any {
			p := chargePayload(cardApproved)
			p.DiscountCents = 500
			return p
		}},
		{"missing currency", func() any {
			p := chargePayload(cardApproved)
			p.Currency = ""
			return p
		}},
		{"missing items", func() any {
			p := chargePayload(cardApproved)
			p.Items = nil
			return p
		}},
		{"missing card", func() any {
			return chargePayload("")
		}},
		{"zero quantity", func() any {
			p := chargePayload(cardApproved)
			p.Items[0].Quantity = 0
			p.SubtotalCents, p.TotalCents = 1990, 1990
			return p
		}},
		{"negative quantity", func() any {
			p := chargePayload(cardApproved)
			p.Items[1].Quantity = -2
			return p
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			dbtest.InsertOrder(t, e.pool, orderID)
			id := e.addJob(t, jobs.KindChargeOrder, tt.payload())

			if err := e.runNext(t); err == nil {
				t.Fatal("charge_order accepted the payload")
			}

			if n := e.count(t, `SELECT count(*) FROM billing.charges`); n != 0 {
				t.Errorf("%d charge rows, want 0", n)
			}
			if status, attempts := e.jobState(t, id); status != "queued" || attempts != 1 {
				t.Errorf("job = %q after %d attempts, want queued after 1", status, attempts)
			}
			if got := dbtest.OrderStatus(t, e.pool, orderID); got != "pending" {
				t.Errorf("order status = %q, want pending", got)
			}
		})
	}
}

func TestChargeOrderRejectsNonPositiveQuantity(t *testing.T) {
	for _, qty := range []int64{0, -1} {
		p := chargePayload(cardApproved)
		p.Items[0].Quantity = qty
		if err := p.Validate(); err == nil {
			t.Errorf("quantity %d: Validate accepted the payload", qty)
		}
		next := jobs.ConvertInvoice{
			OrderID:       p.OrderID,
			Currency:      p.Currency,
			Items:         p.Items,
			SubtotalCents: p.SubtotalCents,
			TotalCents:    p.TotalCents,
		}
		if err := next.Validate(); err == nil {
			t.Errorf("quantity %d: ConvertInvoice Validate accepted the payload", qty)
		}
	}
}
