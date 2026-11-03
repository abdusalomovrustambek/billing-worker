// Package migrations holds the goose SQL migrations for schema billing and
// the function that applies them.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/pressly/goose/v3"
)

// FS holds the embedded *.sql migration files.
//
//go:embed *.sql
var FS embed.FS

// versionTable keeps goose's bookkeeping inside schema billing: the billing
// role owns only that schema and cannot create tables in public.
const versionTable = "billing.goose_db_version"

// Up applies all pending migrations over its own short-lived connection.
// Goose creates its version table before the first migration runs, so the
// schema has to exist already.
func Up(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)

	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS billing"); err != nil {
		return fmt.Errorf("create schema billing: %w", err)
	}

	goose.SetBaseFS(FS)
	goose.SetTableName(versionTable)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "."); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
