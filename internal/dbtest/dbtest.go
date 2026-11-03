// Package dbtest gives tests a migrated database. Tests that use it are
// skipped unless BILLING_TEST_DATABASE_URL is set.
package dbtest

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shop/billing-worker/migrations"
)

// lockKey serialises the tests of different packages: `go test ./...` runs
// packages in parallel, and they share one database.
const lockKey = 727001

// URL returns BILLING_TEST_DATABASE_URL, or skips the test if it is unset.
func URL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("BILLING_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("BILLING_TEST_DATABASE_URL is not set")
	}
	return url
}

// New migrates the database, creates a stub shop.orders (shop-api owns the
// real table), empties the billing tables and returns a pool. The database
// is locked for the test, so tests never overlap.
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := URL(t)
	ctx := context.Background()

	lock, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = lock.Close(ctx) }) // closing the session frees the lock
	if _, err := lock.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		t.Fatalf("lock database: %v", err)
	}

	if err := migrations.Up(ctx, url); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS shop;
		CREATE TABLE IF NOT EXISTS shop.orders (id bigint PRIMARY KEY, status text NOT NULL DEFAULT 'pending');
		TRUNCATE shop.orders, billing.jobs, billing.charges, billing.invoices RESTART IDENTITY;
		ALTER SEQUENCE billing.invoice_seq RESTART`)
	if err != nil {
		t.Fatalf("reset database: %v", err)
	}
	return pool
}

// InsertOrder adds a pending order to the stub shop.orders.
func InsertOrder(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO shop.orders (id) VALUES ($1)`, id); err != nil {
		t.Fatalf("insert order %d: %v", id, err)
	}
}

// OrderStatus returns the status of an order in the stub shop.orders.
func OrderStatus(t *testing.T, pool *pgxpool.Pool, id int64) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM shop.orders WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("order %d status: %v", id, err)
	}
	return status
}
