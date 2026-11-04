package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shop/billing-worker/internal/dbtest"
	"github.com/shop/billing-worker/internal/queue"
)

func addJob(t *testing.T, pool *pgxpool.Pool, kind string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO billing.jobs (kind, payload) VALUES ($1, '{}') RETURNING id`, kind).Scan(&id)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return id
}

type jobRow struct {
	Status    string
	Attempts  int
	LastError *string
}

func getJob(t *testing.T, pool *pgxpool.Pool, id int64) jobRow {
	t.Helper()
	var j jobRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts, last_error FROM billing.jobs WHERE id = $1`, id).
		Scan(&j.Status, &j.Attempts, &j.LastError)
	if err != nil {
		t.Fatalf("select job %d: %v", id, err)
	}
	return j
}

func acquire(t *testing.T, pool *pgxpool.Pool) *pgxpool.Conn {
	t.Helper()
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(conn.Release)
	return conn
}

func TestClaimEmptyQueue(t *testing.T) {
	pool := dbtest.New(t)

	tx, job, err := queue.Claim(context.Background(), acquire(t, pool))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if tx != nil || job != nil {
		t.Errorf("Claim = %v, %v, want no job", tx, job)
	}
}

func TestClaimSkipsJobsRunningLater(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "charge_order")
	if _, err := pool.Exec(context.Background(), `UPDATE billing.jobs SET run_after = now() + interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	_, job, err := queue.Claim(context.Background(), acquire(t, pool))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if job != nil {
		t.Errorf("claimed job %d that is not due yet", job.ID)
	}
}

func TestClaimGivesAJobToOneWorkerOnly(t *testing.T) {
	pool := dbtest.New(t)
	addJob(t, pool, "charge_order")
	ctx := context.Background()

	txA, jobA, err := queue.Claim(ctx, acquire(t, pool))
	if err != nil || jobA == nil {
		t.Fatalf("first Claim = %v, %v, want a job", jobA, err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	_, jobB, err := queue.Claim(ctx, acquire(t, pool))
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if jobB != nil {
		t.Errorf("second worker claimed job %d while the first holds it", jobB.ID)
	}
}

func TestConcurrentClaimsNeverShareAJob(t *testing.T) {
	pool := dbtest.New(t)
	const jobs, workers = 20, 4
	for range jobs {
		addJob(t, pool, "charge_order")
	}
	ctx := context.Background()

	var (
		mu      sync.Mutex
		claimed = map[int64]int{}
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)
	for range workers {
		conn := acquire(t, pool)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				tx, job, err := queue.Claim(ctx, conn)
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if job == nil {
					return
				}
				mu.Lock()
				claimed[job.ID]++
				mu.Unlock()
				if err := queue.Complete(ctx, tx, job.ID); err != nil {
					t.Errorf("Complete: %v", err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(claimed) != jobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("job %d was claimed %d times", id, n)
		}
	}
}

func TestCompleteCommitsHandlerWrites(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "charge_order")
	ctx := context.Background()

	tx, job, err := queue.Claim(ctx, acquire(t, pool))
	if err != nil || job == nil {
		t.Fatalf("Claim = %v, %v", job, err)
	}
	if err := queue.Enqueue(ctx, tx, "convert_invoice", map[string]int{"order_id": 1}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := queue.Complete(ctx, tx, job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if got := getJob(t, pool, id).Status; got != "done" {
		t.Errorf("status = %q, want done", got)
	}
	var queued int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.jobs WHERE kind = 'convert_invoice' AND status = 'queued'`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Errorf("%d convert_invoice jobs queued, want 1", queued)
	}
}

func TestFailBacksOffAndGoesDeadAfterFiveAttempts(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "charge_order")
	ctx := context.Background()
	conn := acquire(t, pool)
	failure := errors.New("card declined")

	var deadCalls int
	onDead := func(ctx context.Context, tx pgx.Tx, payload []byte) error {
		deadCalls++
		_, err := tx.Exec(ctx, `INSERT INTO billing.jobs (kind, payload) VALUES ('dead_marker', '{}')`)
		return err
	}

	for attempt := 1; attempt <= queue.MaxAttempts; attempt++ {
		// Make the job due again, as the backoff would.
		if _, err := pool.Exec(ctx, `UPDATE billing.jobs SET run_after = now() WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		tx, job, err := queue.Claim(ctx, conn)
		if err != nil || job == nil {
			t.Fatalf("attempt %d: Claim = %v, %v", attempt, job, err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}

		dead, err := queue.Fail(ctx, conn, job, failure, onDead)
		if err != nil {
			t.Fatalf("attempt %d: Fail: %v", attempt, err)
		}

		j := getJob(t, pool, id)
		if j.Attempts != attempt {
			t.Errorf("attempt %d: attempts = %d", attempt, j.Attempts)
		}
		if j.LastError == nil || *j.LastError != failure.Error() {
			t.Errorf("attempt %d: last_error = %v", attempt, j.LastError)
		}
		wantDead := attempt == queue.MaxAttempts
		if dead != wantDead || (j.Status == "dead") != wantDead {
			t.Errorf("attempt %d: dead = %v, status = %q, want dead %v", attempt, dead, j.Status, wantDead)
		}
		if !wantDead {
			wantDelay := time.Duration(1<<attempt) * time.Second
			var delaySecs float64
			err := pool.QueryRow(ctx, `SELECT extract(epoch FROM run_after - now())::float8 FROM billing.jobs WHERE id = $1`, id).Scan(&delaySecs)
			if err != nil {
				t.Fatal(err)
			}
			if delay := time.Duration(delaySecs * float64(time.Second)); delay > wantDelay || delay < wantDelay-5*time.Second {
				t.Errorf("attempt %d: run_after in %v, want about %v", attempt, delay, wantDelay)
			}
		}
	}

	if deadCalls != 1 {
		t.Errorf("OnDead ran %d times, want 1", deadCalls)
	}
	var markers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.jobs WHERE kind = 'dead_marker'`).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Errorf("OnDead writes: %d markers committed, want 1", markers)
	}
}

func TestFailRollsBackWhenOnDeadFails(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "charge_order")
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE billing.jobs SET attempts = $2 WHERE id = $1`, id, queue.MaxAttempts-1); err != nil {
		t.Fatal(err)
	}

	job := &queue.Job{ID: id, Kind: "charge_order", Payload: []byte("{}")}
	onDead := func(context.Context, pgx.Tx, []byte) error { return errors.New("hook broke") }
	if _, err := queue.Fail(ctx, acquire(t, pool), job, errors.New("boom"), onDead); err == nil {
		t.Fatal("Fail returned no error")
	}

	j := getJob(t, pool, id)
	if j.Status != "queued" || j.Attempts != queue.MaxAttempts-1 {
		t.Errorf("job = %+v, want the failure rolled back", j)
	}
}

func TestClosedConnectionLeavesJobQueued(t *testing.T) {
	pool := dbtest.New(t)
	id := addJob(t, pool, "charge_order")
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dbtest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	tx, job, err := queue.Claim(ctx, conn)
	if err != nil || job == nil {
		t.Fatalf("Claim = %v, %v", job, err)
	}
	if err := queue.Enqueue(ctx, tx, "convert_invoice", map[string]int{"order_id": 1}); err != nil {
		t.Fatal(err)
	}
	// The process dies mid-job: its connection goes away without a commit.
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}

	var again *queue.Job
	deadline := time.Now().Add(5 * time.Second)
	for again == nil && time.Now().Before(deadline) {
		c, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
		tx2, j, err := queue.Claim(ctx, c)
		if err != nil {
			c.Release()
			t.Fatalf("Claim after close: %v", err)
		}
		if j != nil {
			_ = tx2.Rollback(ctx)
		}
		c.Release()
		again = j
		if again == nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if again == nil || again.ID != id {
		t.Fatalf("job %d was not claimable after its connection closed", id)
	}
	if j := getJob(t, pool, id); j.Status != "queued" || j.Attempts != 0 {
		t.Errorf("job = %+v, want queued", j)
	}
	var enqueued int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM billing.jobs WHERE kind = 'convert_invoice'`).Scan(&enqueued); err != nil {
		t.Fatal(err)
	}
	if enqueued != 0 {
		t.Errorf("%d convert_invoice jobs survived the rollback, want 0", enqueued)
	}
}

// chargePayloadWithCard is a charge_order payload as shop-api inserts it: the
// full card travels in billing.jobs.payload while the job runs.
const chargePayloadWithCard = `{"order_id":812,"user_id":7,"currency":"EUR","subtotal_cents":3000,"discount_cents":0,"total_cents":3000,"card":{"number":"4242424242424242","exp":"12/29","cvc":"123"}}`

func addJobWithPayload(t *testing.T, pool *pgxpool.Pool, kind, payload string) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(),
		`INSERT INTO billing.jobs (kind, payload) VALUES ($1, $2) RETURNING id`, kind, payload).Scan(&id)
	if err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return id
}

func getPayload(t *testing.T, pool *pgxpool.Pool, id int64) []byte {
	t.Helper()
	var payload []byte
	err := pool.QueryRow(context.Background(),
		`SELECT payload FROM billing.jobs WHERE id = $1`, id).Scan(&payload)
	if err != nil {
		t.Fatalf("select payload of job %d: %v", id, err)
	}
	return payload
}

// A finished charge_order must not keep the card: the card is never stored,
// only card_last4 in billing.charges.
func TestCompleteStripsCardFromChargeOrderPayload(t *testing.T) {
	pool := dbtest.New(t)
	id := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	ctx := context.Background()

	tx, job, err := queue.Claim(ctx, acquire(t, pool))
	if err != nil || job == nil {
		t.Fatalf("Claim = %v, %v", job, err)
	}
	// While the job runs the handler still needs the card.
	if !strings.Contains(string(job.Payload), "4242424242424242") {
		t.Errorf("claimed payload = %s, want it to still carry the card", job.Payload)
	}
	if err := queue.Complete(ctx, tx, job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if got := getJob(t, pool, id).Status; got != "done" {
		t.Errorf("status = %q, want done", got)
	}
	payload := getPayload(t, pool, id)
	if strings.Contains(string(payload), "4242424242424242") {
		t.Errorf("completed payload = %s, want no full card number", payload)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode completed payload: %v", err)
	}
	if _, ok := decoded["card"]; ok {
		t.Errorf("completed payload = %s, want the card object removed", payload)
	}
	if decoded["order_id"] != float64(812) {
		t.Errorf("completed payload = %s, want the order data kept", payload)
	}
}

// Other kinds carry no card, so completing them must leave the payload alone.
func TestCompleteKeepsOtherKindPayloads(t *testing.T) {
	pool := dbtest.New(t)
	id := addJobWithPayload(t, pool, "convert_invoice", `{"order_id":1,"currency":"EUR","total_cents":3000}`)
	ctx := context.Background()

	tx, job, err := queue.Claim(ctx, acquire(t, pool))
	if err != nil || job == nil {
		t.Fatalf("Claim = %v, %v", job, err)
	}
	if err := queue.Complete(ctx, tx, job.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(getPayload(t, pool, id), &decoded); err != nil {
		t.Fatalf("decode completed payload: %v", err)
	}
	if decoded["order_id"] != float64(1) || decoded["total_cents"] != float64(3000) {
		t.Errorf("completed payload = %v, want it unchanged", decoded)
	}
}

// A charge_order that runs out of attempts goes dead with its card stripped,
// while retryable failures and other kinds keep their payloads.
func TestFailStripsCardWhenChargeOrderGoesDead(t *testing.T) {
	pool := dbtest.New(t)
	ctx := context.Background()
	fail := func(id int64, kind string) (bool, error) {
		return queue.Fail(ctx, acquire(t, pool), &queue.Job{ID: id, Kind: kind}, errors.New("card declined"), nil)
	}
	failOnLastAttempt := func(t *testing.T, id int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE billing.jobs SET attempts = $2 WHERE id = $1`, id, queue.MaxAttempts-1); err != nil {
			t.Fatal(err)
		}
	}

	dying := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	failOnLastAttempt(t, dying)
	dead, err := fail(dying, "charge_order")
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if !dead {
		t.Fatal("Fail did not report the job dead")
	}
	if got := getJob(t, pool, dying).Status; got != "dead" {
		t.Fatalf("status = %q, want dead", got)
	}
	if stripped := getPayload(t, pool, dying); strings.Contains(string(stripped), "4242424242424242") {
		t.Errorf("dead payload = %s, want no full card number", stripped)
	}

	// A retryable failure keeps the card: the next attempt still needs it.
	retry := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	if dead, err := fail(retry, "charge_order"); err != nil {
		t.Fatalf("Fail: %v", err)
	} else if dead {
		t.Fatal("first Fail reported the job dead")
	}
	if kept := getPayload(t, pool, retry); !strings.Contains(string(kept), "4242424242424242") {
		t.Errorf("retried payload = %s, want the card kept for the next attempt", kept)
	}

	// Other kinds keep their payloads even when they go dead.
	other := addJobWithPayload(t, pool, "convert_invoice", `{"order_id":1,"currency":"EUR","total_cents":3000}`)
	failOnLastAttempt(t, other)
	if dead, err := fail(other, "convert_invoice"); err != nil {
		t.Fatalf("Fail: %v", err)
	} else if !dead {
		t.Fatal("Fail did not report the job dead")
	}
	var decoded map[string]any
	if err := json.Unmarshal(getPayload(t, pool, other), &decoded); err != nil {
		t.Fatalf("decode dead convert_invoice payload: %v", err)
	}
	if decoded["order_id"] != float64(1) || decoded["total_cents"] != float64(3000) {
		t.Errorf("dead convert_invoice payload = %v, want it unchanged", decoded)
	}
}

// The retention purge strips the card from finished charge_order jobs older
// than the cutoff and leaves everything else alone.
func TestPurgeFinishedPayloadsStripsOnlyOldFinishedChargeOrders(t *testing.T) {
	pool := dbtest.New(t)
	ctx := context.Background()

	oldDone := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	oldDead := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	newDone := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	queued := addJobWithPayload(t, pool, "charge_order", chargePayloadWithCard)
	other := addJobWithPayload(t, pool, "convert_invoice", `{"order_id":1,"currency":"EUR","total_cents":3000}`)
	backdate := func(id int64, status string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE billing.jobs SET status = $2, attempts = $3, updated_at = now() - interval '60 days' WHERE id = $1`,
			id, status, queue.MaxAttempts); err != nil {
			t.Fatal(err)
		}
	}
	backdate(oldDone, "done")
	backdate(oldDead, "dead")
	if _, err := pool.Exec(ctx, `UPDATE billing.jobs SET status = 'done' WHERE id = $1`, newDone); err != nil {
		t.Fatal(err)
	}

	purged, err := queue.PurgeFinishedPayloads(ctx, pool, time.Now().Add(-30*24*time.Hour))
	if err != nil {
		t.Fatalf("PurgeFinishedPayloads: %v", err)
	}
	if purged != 2 {
		t.Errorf("purged %d rows, want 2", purged)
	}
	for _, id := range []int64{oldDone, oldDead} {
		if payload := getPayload(t, pool, id); strings.Contains(string(payload), "4242424242424242") {
			t.Errorf("purged payload of job %d = %s, want no full card number", id, payload)
		}
	}
	for _, id := range []int64{newDone, queued} {
		if payload := getPayload(t, pool, id); !strings.Contains(string(payload), "4242424242424242") {
			t.Errorf("payload of job %d = %s, want the card kept", id, payload)
		}
	}
	var decoded map[string]any
	if err := json.Unmarshal(getPayload(t, pool, other), &decoded); err != nil {
		t.Fatalf("decode convert_invoice payload: %v", err)
	}
	if decoded["order_id"] != float64(1) || decoded["total_cents"] != float64(3000) {
		t.Errorf("convert_invoice payload = %v, want it unchanged", decoded)
	}
}
