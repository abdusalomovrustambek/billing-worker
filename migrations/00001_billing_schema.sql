-- +goose Up
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE billing.jobs (
    id         bigserial PRIMARY KEY,
    kind       text        NOT NULL,
    payload    jsonb       NOT NULL,
    status     text        NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'done', 'dead')),
    attempts   int         NOT NULL DEFAULT 0,
    run_after  timestamptz NOT NULL DEFAULT now(),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX jobs_status_run_after_idx ON billing.jobs (status, run_after);

CREATE TABLE billing.charges (
    id           bigserial PRIMARY KEY,
    order_id     bigint      NOT NULL,
    amount_cents bigint      NOT NULL,
    currency     text        NOT NULL,
    card_last4   text        NOT NULL,
    result       text        NOT NULL CHECK (result IN ('approved', 'declined')),
    network_ref  text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- A retried job must not be able to charge an order twice.
CREATE UNIQUE INDEX charges_order_approved_idx ON billing.charges (order_id) WHERE result = 'approved';

CREATE TABLE billing.invoices (
    id             bigserial PRIMARY KEY,
    number         text        NOT NULL UNIQUE,
    order_id       bigint      NOT NULL UNIQUE,
    currency       text        NOT NULL,
    subtotal_cents bigint      NOT NULL,
    total_cents    bigint      NOT NULL,
    lines          jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE billing.fx_rates (
    currency   text PRIMARY KEY,
    rate_ppm   bigint      NOT NULL CHECK (rate_ppm > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE billing.invoice_seq;

-- +goose Down
-- The schema stays: goose keeps its version table in it.
DROP SEQUENCE billing.invoice_seq;
DROP TABLE billing.fx_rates;
DROP TABLE billing.invoices;
DROP TABLE billing.charges;
DROP TABLE billing.jobs;
