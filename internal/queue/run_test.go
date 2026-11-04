package queue

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// A panicking handler must come back as an error, not kill the caller:
// runOne turns any error from run into a rollback plus a recorded attempt.
func TestRunConvertsPanicToError(t *testing.T) {
	w := &Worker{Handlers: map[string]Handler{
		"panics": {Run: func(context.Context, pgx.Tx, []byte) error {
			panic("boom")
		}},
		"fails": {Run: func(context.Context, pgx.Tx, []byte) error {
			return errors.New("boom")
		}},
	}}

	err := w.run(context.Background(), nil, &Job{Kind: "panics", Payload: []byte(`{}`)})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("run with panicking handler = %v, want an error mentioning the panic", err)
	}

	// The worker is still usable after the panic: other outcomes are unchanged.
	if err := w.run(context.Background(), nil, &Job{Kind: "fails", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("run with failing handler = nil, want the handler error")
	}
	if err := w.run(context.Background(), nil, &Job{Kind: "mystery", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("run with unknown kind = nil, want an error")
	}
}
