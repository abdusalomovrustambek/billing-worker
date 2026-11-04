package queue_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/shop/billing-worker/internal/dbtest"
	"github.com/shop/billing-worker/internal/logging"
	"github.com/shop/billing-worker/internal/queue"
)

// startWorker runs w until the returned stop function is called, which waits
// for Run to return.
func startWorker(w *queue.Worker) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		w.Run(ctx)
	}()
	return func() {
		cancel()
		<-stopped
	}
}

func discardLog() *slog.Logger {
	return logging.New(io.Discard, slog.LevelError)
}

func TestWorkerRunsEachJobOnceAndStopsOnCancel(t *testing.T) {
	pool := dbtest.New(t)
	const jobs = 10
	for range jobs {
		addJob(t, pool, "count")
	}

	var calls atomic.Int32
	var handled sync.WaitGroup
	handled.Add(jobs)
	handler := queue.Handler{Run: func(context.Context, pgx.Tx, []byte) error {
		if calls.Add(1) <= jobs {
			handled.Done()
		}
		return nil
	}}
	w := &queue.Worker{Pool: pool, Handlers: map[string]queue.Handler{"count": handler}, Count: 3, Log: discardLog()}
	stop := startWorker(w)
	defer stop()

	allHandled := make(chan struct{})
	go func() {
		handled.Wait()
		close(allHandled)
	}()
	select {
	case <-allHandled:
	case <-time.After(10 * time.Second):
		t.Fatal("the workers did not run all jobs in 10 s")
	}
	stop() // returns only after every claimed job is committed

	if n := calls.Load(); n != jobs {
		t.Errorf("handler ran %d times for %d jobs", n, jobs)
	}
	var done int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM billing.jobs WHERE status = 'done'`).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if done != jobs {
		t.Errorf("%d jobs done, want %d", done, jobs)
	}
}

func TestWorkerRecordsFailedAttempt(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "broken")

	failed := make(chan struct{}, 1)
	handler := queue.Handler{Run: func(context.Context, pgx.Tx, []byte) error {
		select {
		case failed <- struct{}{}:
		default:
		}
		return errors.New("boom")
	}}
	w := &queue.Worker{Pool: pool, Handlers: map[string]queue.Handler{"broken": handler}, Count: 2, Log: discardLog()}
	stop := startWorker(w)
	defer stop()

	select {
	case <-failed:
	case <-time.After(10 * time.Second):
		t.Fatal("the job was not run in 10 s")
	}
	stop() // returns only after the failure is recorded

	j := getJob(t, pool, id)
	if j.Status != "queued" || j.Attempts != 1 || j.LastError == nil || *j.LastError != "boom" {
		t.Errorf("job = %+v, want queued after 1 attempt with last_error boom", j)
	}
}

func TestWorkerFailsJobsOfUnknownKind(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "mystery")

	w := &queue.Worker{Pool: pool, Handlers: map[string]queue.Handler{}, Count: 1, Log: discardLog()}
	stop := startWorker(w)
	defer stop()

	deadline := time.After(10 * time.Second)
	for {
		if j := getJob(t, pool, id); j.Attempts == 1 {
			if j.LastError == nil || *j.LastError != `unknown job kind "mystery"` {
				t.Errorf("last_error = %v", j.LastError)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("the job was not failed in 10 s")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
