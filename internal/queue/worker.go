package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// PollInterval is how long an idle worker waits before it looks again.
	PollInterval = 500 * time.Millisecond
	// JobTimeout is the hard limit for one job, also after a shutdown signal.
	JobTimeout = 30 * time.Second
	// cleanupTimeout bounds the rollback and failure bookkeeping of a job.
	cleanupTimeout = 10 * time.Second
)

// Handler runs jobs of one kind. Run works inside the job's transaction:
// returning nil commits its writes, returning an error rolls them back and
// counts a failed attempt. OnDead is optional.
type Handler struct {
	Run    func(ctx context.Context, tx pgx.Tx, payload []byte) error
	OnDead DeadFunc
}

// Worker runs Count goroutines that claim and run jobs. Each job uses its own
// pool connection, so the pool needs more connections than Count.
type Worker struct {
	Pool     *pgxpool.Pool
	Handlers map[string]Handler
	Count    int
	Log      *slog.Logger
}

// Run starts the workers and blocks until ctx is cancelled and every job that
// was already claimed has finished. Cancelling ctx stops claiming only; a
// claimed job keeps running, for at most JobTimeout.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range w.Count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		worked, err := w.runOne(ctx)
		if err != nil && ctx.Err() == nil {
			w.Log.Error("worker error", "error", err)
		}
		if worked {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(PollInterval):
		}
	}
}

// runOne claims and runs a single job. It reports whether there was one.
func (w *Worker) runOne(ctx context.Context) (bool, error) {
	conn, err := w.Pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	tx, job, err := Claim(ctx, conn)
	if err != nil || job == nil {
		return false, err
	}

	// A signal must not cut a claimed job short.
	jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), JobTimeout)
	defer cancel()

	err = w.run(jobCtx, tx, job)
	if err == nil {
		return true, nil
	}

	cleanCtx, cleanCancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cleanCancel()
	_ = tx.Rollback(cleanCtx)
	w.Log.Error("job failed", "kind", job.Kind, "id", job.ID, "attempt", job.Attempts+1, "error", err)

	dead, err := Fail(cleanCtx, conn, job, err, w.Handlers[job.Kind].OnDead)
	if err != nil {
		return true, fmt.Errorf("fail job %d: %w", job.ID, err)
	}
	if dead {
		w.Log.Error("job is dead", "kind", job.Kind, "id", job.ID)
	}
	return true, nil
}

func (w *Worker) run(ctx context.Context, tx pgx.Tx, job *Job) (err error) {
	h, ok := w.Handlers[job.Kind]
	if !ok {
		return fmt.Errorf("unknown job kind %q", job.Kind)
	}
	// A panicking handler must fail this one attempt, not the whole worker:
	// runOne rolls the job back and records the failure for any error.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	if err := h.Run(ctx, tx, job.Payload); err != nil {
		return err
	}
	return Complete(ctx, tx, job.ID)
}
