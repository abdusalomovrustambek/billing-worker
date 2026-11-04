# billing-worker

A Go worker that takes jobs from a PostgreSQL queue (schema `billing`), charges orders against a fake card network, converts currencies and writes invoices. It has no public API. See `Implementation plan.md` for the design.

## Commands

- `make build` builds `bin/billing-worker`.
- `make test` runs `go test -race ./...`.
- `make lint` runs `go vet` and `golangci-lint`.
- DB tests need `BILLING_TEST_DATABASE_URL`, for example `postgres://billing:billing@localhost:5432/billing_test?sslmode=disable`. Without it they are skipped.
- Config comes from the environment: `DATABASE_URL` (required), `WORKERS` (default 2), `LOG_LEVEL` (debug, info, warn, error; default info).
- `billing-worker healthcheck` pings the database (3 s timeout) and sets the exit code. It does not run migrations.
- `billing-worker purge-cards [--older-than DURATION]` strips card numbers from finished `charge_order` payloads (default `720h`, i.e. 30 days) and reports how many rows it stripped. Run it once at rollout; it takes `DATABASE_URL` like the other commands and does not run migrations.

## Layout

- `cmd/billing-worker`: config, logger, migrations, worker pool. No business logic.
- `internal/queue`: claim, complete, fail and enqueue SQL, and the worker pool. It knows nothing about job kinds.
- `internal/jobs`: the job kinds (`charge_order`, `convert_invoice`), their payload types and the registry.
- `internal/cardnet`: the fake card network. Pure, no database.
- `internal/fx`: rate lookup and integer conversion.
- `internal/invoice`: invoice numbers, lines, plain-text rendering, money formatting. Pure.
- `internal/logging`: the log line format.
- `internal/dbtest`: test helper that migrates the test database and creates a stub `shop.orders` (shop-api owns the real table).
- `migrations`: goose SQL migrations, embedded and run on start. The goose version table is `billing.goose_db_version`.

## Conventions

- Money is `int64` cents. Exchange rates are `int64` parts per million per 1 USD. No floats anywhere near money.
- Amounts in job payloads are USD cents. The card is charged in USD; invoices are converted to the order's currency.
- Log lines look like `2006-01-02T15:04:05.000Z INFO billing.jobs: charged order 812 (49.90 USD)`: UTC time, level, component, message, then `key=value` pairs. The component (`billing.main`, `billing.queue`, `billing.jobs`) is set once on the logger with `logging.Component`. Log through the logger you are given, not through `fmt` or `log`.
- Errors are returned with context (`fmt.Errorf("charge order %d: %w", id, err)`). Do not put payloads or card data in error text: it ends up in `billing.jobs.last_error`.
- Tests sit next to their package. Tests that need Postgres use `dbtest.New(t)`, which skips them when `BILLING_TEST_DATABASE_URL` is unset and locks the database so packages do not overlap.
- Migrations are goose files in `migrations/`. An applied migration is never edited; a change is a new file with a matching down.

## Job handlers

A handler has the signature `func(ctx, tx pgx.Tx, payload []byte) error` and runs inside the job's transaction, which the queue opened when it claimed the row.

- Do all writes through `tx`. Returning `nil` commits them together with the job being marked `done`; returning an error rolls them all back and the queue records the failed attempt.
- Do not commit, roll back or begin a transaction in a handler, and do not use a pool connection for writes that belong to the job.
- Use the `ctx` you get; it carries the job's time limit. Block only on calls that honour it.
- A failed job is retried with a backoff of 2^attempts seconds and is `dead` after 5 failures. An optional `OnDead` hook of the kind runs in the transaction that marks the job dead.
- Handlers must be safe to run again: check for an earlier result (for example an approved charge for the order) before doing the work.
- Enqueue follow-up jobs with `queue.Enqueue(ctx, tx, kind, payload)`, so they exist only if the job commits.

## Contract with shop-api

shop-api inserts `charge_order` jobs in the same transaction as the order:

```json
{
  "order_id": 812,
  "user_id": 7,
  "currency": "EUR",
  "items": [{"sku": "mug", "name": "Mug", "unit_price_cents": 1500, "quantity": 2}],
  "subtotal_cents": 3000,
  "discount_cents": 0,
  "total_cents": 3000,
  "card": {"number": "4242 4242 4242 4242", "exp": "12/29", "cvc": "123"}
}
```

- `kind` is `charge_order`; `status`, `attempts` and `run_after` take their column defaults.
- The lines must add up to `subtotal_cents`, and `subtotal_cents - discount_cents` must equal `total_cents`; otherwise the job fails validation.
- The worker sets `shop.orders.status` to `paid` when the charge succeeds and to `failed` when the job is dead. The card is never stored: `billing.charges` keeps only `card_last4`.
- Test cards: `4242 4242 4242 4242` approves, `4000 0000 0000 0002` declines, `4000 0000 0000 0119` times out, anything else declines as an invalid card.
- A change to this contract is a change in both repositories.

A `charge_order` that succeeds enqueues `convert_invoice` with the same order data and no card.
