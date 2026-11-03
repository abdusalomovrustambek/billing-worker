# billing-worker — Implementation Plan

Nov 3, 2026 · draft · @Rustam

## Overview

billing-worker is the back office of **Shop**, the demo store for agnts. It is a Go worker that takes jobs from a PostgreSQL queue, charges orders against a fake card network, converts currencies and writes invoices. It has no public API.

It exists to show agnts on the kind of code people are afraid to touch: background jobs that crash, restart and leak data into logs. Its planted bugs are a panic that kills the process over and over, a card number and a database password in the log, and an unknown currency.

**The Shop system**

| Repo | What it is | Stack |
|---|---|---|
| shop-api | Catalog, cart, coupons, orders, checkout | Go, PostgreSQL |
| shop-web | Storefront | Vue 3, Vite, TypeScript |
| **billing-worker** (this) | Charges orders, writes invoices | Go, PostgreSQL |
| auth-gateway | Sign-in, JWTs, rate limits, access log, static files, proxy | Go (Gin) |

**In scope:** the queue, two job kinds, the fake card network, invoices, tests, CI.
**Out of scope:** real payment providers, receipts by email, PDF invoices, refunds, metrics.

**Principles**

1. A job is a row; the worker is stateless. Any number of workers can run.
2. Money is integer cents; exchange rates are integers in parts per million.
3. Handled failures retry with backoff up to 5 attempts, then the job is `dead` and stays for inspection.
4. One unrecovered goroutine panic kills the process, as in real Go workers; compose restarts it. That is the repeated crash the demo needs.

## Layout

```
billing-worker/
  cmd/billing-worker/main.go   config, DB, migrations, worker pool; `billing-worker healthcheck` pings the DB
  internal/queue/              claim, complete, fail (SELECT … FOR UPDATE SKIP LOCKED)
  internal/jobs/               charge_order, convert_invoice
  internal/cardnet/            fake card network: approve, decline, timeout by test card
  internal/fx/                 rates table and conversion
  internal/invoice/            invoice numbers, lines, plain-text rendering
  migrations/                  goose SQL migrations (schema `billing`)
  AGENTS.md                    conventions, the job contract with shop-api
  Dockerfile, Makefile, .golangci.yml, .github/workflows/ci.yml
```

## Data

Schema `billing`:

| Table | Columns |
|---|---|
| `jobs` | id, kind, payload (jsonb), status (`queued`/`done`/`dead`), attempts, run_after, last_error, created_at, updated_at |
| `charges` | id, order_id, amount_cents, currency, card_last4, result, network_ref, created_at |
| `invoices` | id, number (`INV-2026-000123`), order_id, currency, subtotal_cents, total_cents, lines (jsonb), created_at |
| `fx_rates` | currency, rate_ppm (per 1 USD), updated_at; seeded with USD, EUR, GBP (not UZS) |

**The contract with shop-api.** shop-api inserts `charge_order` jobs with payload `{order_id, user_id, currency, items: [{sku, name, unit_price_cents, quantity}], subtotal_cents, discount_cents, total_cents, card: {number, exp, cvc}}` in the same transaction as the order. The worker updates `shop.orders.status` to `paid` or `failed` when the charge settles. Both `AGENTS.md` files describe this; a change to it is a multi-repo change.

## Queue

A worker claims a job and runs it **inside one transaction that stays open for the whole job**:

```sql
BEGIN;
SELECT … FROM billing.jobs
 WHERE status = 'queued' AND run_after <= now()
 ORDER BY run_after FOR UPDATE SKIP LOCKED LIMIT 1;
-- run the handler; its writes use the same transaction
UPDATE billing.jobs SET status = 'done' | attempts = attempts + 1, run_after = …, status = CASE … 'dead' … END …;
COMMIT;
```

- The row lock is the claim: no other worker can take the job while it runs, and there is no `running` status that a crash could leave behind.
- If the process dies mid-job, Postgres rolls the transaction back and releases the lock; the job is `queued` again and the next worker takes it at once. This is what turns BW-1 into a repeated crash.
- A handled error (decline, timeout, unknown currency) rolls back the handler's writes, then a short second transaction records `attempts + 1`, `last_error` and the backoff (`run_after = now() + 2^attempts s`), or `dead` after 5.
- `WORKERS` goroutines (default 2), each with its own connection; idle workers poll every 500 ms. `SIGTERM` lets running jobs finish (30 s).

## Jobs

| Kind | Does | Then |
|---|---|---|
| `charge_order` | validates the payload, checks each item's unit price against the totals, charges the card, records the charge, sets the order status | enqueues `convert_invoice` |
| `convert_invoice` | converts the totals to the order currency with `fx_rates`, writes the invoice, logs `invoice INV-… written` | – |

Fake card network: `4242 4242 4242 4242` approves, `4000 0000 0000 0002` declines, `4000 0000 0000 0119` times out, anything else declines with `invalid card`.

## Logging

Text lines like a typical Go service (`slog` text handler): `2026-11-03T10:00:00.000Z INFO billing.jobs: charged order 812 (49.90 USD)`. Errors at `ERROR` with the job kind and id. The four Shop services log in three formats (JSON, text, access log) to show that agnts reads them all.

## Planted bugs

| ID | Where | The bug | What shows | Seen by |
|---|---|---|---|---|
| BW-1 | `jobs/charge.go` | the unit price check does `line_total / quantity` with `int64` and no zero check, and trusts shop-api to never send quantity 0 (it does: API-6); no `recover` around the handler | `panic: runtime error: integer divide by zero`, the process exits, compose restarts it, the job is taken again: the same panic every few seconds | Log watcher (error: one finding, count rising) |
| BW-2 | `jobs/charge.go` on decline | logs the whole payload at `ERROR`, card number included | `card number written to the log` (Luhn-valid test card) | Log watcher (personal data) |
| BW-3 | `cmd/billing-worker/main.go` | logs the database URL at start (`connecting to postgres://billing:…@postgres:5432/shop`) | `password in a URL written to the log` | Log watcher (secret) |
| BW-4 | `fx/fx.go` | an unknown currency returns rate 0 instead of an error; `convert_invoice` writes a zero-total invoice for `UZS` | `ERROR billing.jobs: invoice INV-… total is 0` | Log watcher (error), review |
| BW-5 | `queue/queue.go` | a job that killed the process is never counted as an attempt (the count is written only after the handler returns), so BW-1 repeats forever and never goes `dead` | the crash never stops on its own | Review of the BW-1 fix |

BW-1 and BW-5 together are the strongest log-watcher demo: a real process crashing every few seconds, which agnts groups into one finding with a count, maps to this repo and turns into a ticket. A good fix touches both: a per-job `recover` that marks the job failed, rejecting zero quantities, and counting the attempt in a separate transaction before the handler runs. agnts's review should ask for BW-5 if Muse only fixes BW-1, and point at API-6 as the source of the bad data.

Note: with Loki as the source, agnts sees the repeated panic, not the container restarts; restarts are visible only with `agnts watch` on the host. The demo talks about "the same crash every few seconds", which is what the finding shows.

shop-api's shopper triggers BW-1 through the real path (its `zero-qty` incident: add an item with quantity 0, then check out) and BW-4 with a checkout in `UZS`. A crash also delays every job behind the poison job, which the dry run should show once (orders stay `pending`).

## Scenarios this repo plays in

| Scenario | What the judges see |
|---|---|
| Crash → fix | one finding "integer divide by zero in charge_order" with its count rising; ticket on billing-worker; Muse's fix with a test for zero quantity; the review asks about the retry count and the API's validation |
| Data in logs | two security findings (card number, password in a URL), masked, with "rotate" advice; one ticket fixes both call sites |
| Multi-repo (recorded) | the **Shop** group ticket "…and on invoices" adds a discount line to invoices here, with `discount_cents` from shop-api |

## Tests and CI

- Unit: `fx` conversion and rounding, invoice numbering and rendering, card network outcomes.
- DB tests (`BILLING_TEST_DATABASE_URL`, a fresh database): two workers never take the same job; a handled error backs off and goes `dead` after 5; a worker killed mid-job leaves the job `queued` (closing the connection mid-transaction). BW-5's attempt count is the case not asserted.
- CI: `go vet`, `golangci-lint`, `go test -race ./...` with a Postgres service, `docker build`.

## Deploy

Built from `/opt/shop/src/billing-worker` and run as the compose service **`billing-worker`** in shop-api's `deploy/docker-compose.yml`, on the internal `shop` network only, `restart: unless-stopped`, `WORKERS=2`, health check `billing-worker healthcheck`. It connects as the `billing` role: owner of schema `billing` (its migrations run on start), update on `shop.orders`, nothing else.

## Phases

1. Schema, queue, `charge_order` with the card network, tests, CI.
2. `convert_invoice`, graceful shutdown, health check.
3. Bugs BW-1 to BW-5, deploy, log watcher mapping, a dry run of the crash scenario.

## Decisions

- **Postgres queue** over Redis or NATS: one less service, transactional enqueue from shop-api, and the job contract is visible in SQL.
- **The claim is the row lock of an open transaction**, not a `running` status: a crash can never strand a job, which is both the correct design and what makes BW-1 repeat.
- **No recover around jobs (on purpose, BW-1)**: the common real mistake, and the repeated crash it causes is what makes the log watcher demo convincing.
- **Cut for the demo:** receipts, metrics, LISTEN/NOTIFY wake-ups. None is on a demo path; polling every 500 ms is enough.
