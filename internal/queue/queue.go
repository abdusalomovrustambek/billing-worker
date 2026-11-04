// Package queue is a job queue on top of the billing.jobs table. A worker
// claims a job by locking its row inside a transaction that stays open while
// the job runs: if the process dies, Postgres rolls the transaction back and
// the job is queued again.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MaxAttempts is the number of failures after which a job is dead.
const MaxAttempts = 5

// Beginner is satisfied by *pgx.Conn and *pgxpool.Conn.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Execer is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Job is a claimed row of billing.jobs.
type Job struct {
	ID       int64
	Kind     string
	Payload  []byte
	Attempts int
}

// DeadFunc runs inside the transaction that marks a job dead.
type DeadFunc func(ctx context.Context, tx pgx.Tx, payload []byte) error

// Claim starts a transaction on conn and locks the next runnable job in it.
// It returns a nil job (and no transaction) when nothing is runnable. The
// caller must end a returned transaction with Complete or Rollback.
func Claim(ctx context.Context, conn Beginner) (pgx.Tx, *Job, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin claim: %w", err)
	}
	var job Job
	err = tx.QueryRow(ctx, `
		SELECT id, kind, payload, attempts
		  FROM billing.jobs
		 WHERE status = 'queued' AND run_after <= now()
		 ORDER BY run_after, id
		   FOR UPDATE SKIP LOCKED
		 LIMIT 1`).Scan(&job.ID, &job.Kind, &job.Payload, &job.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Rollback(ctx); err != nil {
			return nil, nil, fmt.Errorf("end empty claim: %w", err)
		}
		return nil, nil, nil
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, fmt.Errorf("claim job: %w", err)
	}
	return tx, &job, nil
}

// Complete marks the job done and commits the job's transaction, together
// with everything the handler wrote in it. For charge_order jobs it also
// strips the card from the stored payload: the card is never stored, only
// card_last4 in billing.charges. The kind stays a literal because jobs
// imports queue, and the shop-api contract names it in AGENTS.md.
func Complete(ctx context.Context, tx pgx.Tx, id int64) error {
	if _, err := tx.Exec(ctx, `UPDATE billing.jobs SET status = 'done', payload = CASE WHEN kind = 'charge_order' THEN payload - 'card' ELSE payload END, updated_at = now() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("mark job %d done: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job %d: %w", id, err)
	}
	return nil
}

// Enqueue inserts a job in tx; payload is marshalled to JSON.
func Enqueue(ctx context.Context, tx pgx.Tx, kind string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s payload: %w", kind, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO billing.jobs (kind, payload) VALUES ($1, $2)`, kind, b); err != nil {
		return fmt.Errorf("enqueue %s: %w", kind, err)
	}
	return nil
}

// Fail records a failed run in a second short transaction on conn, after the
// job's own transaction was rolled back: attempts + 1, the error, and the
// next run in 2^attempts seconds. On the MaxAttempts-th failure the job
// becomes dead and onDead, if set, runs in the same transaction. A dead
// charge_order loses its card with the dead flip, the same guarantee as
// Complete; retryable failures keep the payload so the next attempt can run.
// It reports whether the job is dead now.
func Fail(ctx context.Context, conn Beginner, job *Job, jobErr error, onDead DeadFunc) (bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin fail: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after commit

	var status string
	err = tx.QueryRow(ctx, `
		UPDATE billing.jobs
		   SET attempts   = attempts + 1,
		       last_error = $2,
		       run_after  = now() + interval '1 second' * power(2, attempts + 1),
		       status     = CASE WHEN attempts + 1 >= $3 THEN 'dead' ELSE status END,
		       payload    = CASE WHEN attempts + 1 >= $3 AND kind = 'charge_order' THEN payload - 'card' ELSE payload END,
		       updated_at = now()
		 WHERE id = $1
		RETURNING status`, job.ID, jobErr.Error(), MaxAttempts).Scan(&status)
	if err != nil {
		return false, fmt.Errorf("record failure of job %d: %w", job.ID, err)
	}
	dead := status == "dead"
	if dead && onDead != nil {
		if err := onDead(ctx, tx, job.Payload); err != nil {
			return false, fmt.Errorf("on-dead hook of job %d: %w", job.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit failure of job %d: %w", job.ID, err)
	}
	return dead, nil
}

// PurgeFinishedPayloads strips the card from finished charge_order jobs last
// updated before before. It backfills rows finished before Complete and Fail
// learned to redact, and bounds how long a card survives in any row the
// redaction missed. It reports how many rows it stripped.
func PurgeFinishedPayloads(ctx context.Context, db Execer, before time.Time) (int64, error) {
	tag, err := db.Exec(ctx, `
		UPDATE billing.jobs
		   SET payload = payload - 'card', updated_at = now()
		 WHERE kind = 'charge_order'
		   AND status IN ('done', 'dead')
		   AND updated_at < $1
		   AND payload ? 'card'`, before)
	if err != nil {
		return 0, fmt.Errorf("purge finished payloads: %w", err)
	}
	return tag.RowsAffected(), nil
}
