package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var queueCounter atomic.Int64

func setup(t *testing.T) (*Client, string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	c := New(pool)
	if err := c.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// every test has its own queue, so tests do not see each other's jobs
	queue := fmt.Sprintf("%s-%d-%d-%d", t.Name(), os.Getpid(), time.Now().UnixNano(), queueCounter.Add(1))
	return c, queue
}

func newTestWorker(t *testing.T, c *Client, queue string, concurrency int, handlers map[string]HandlerFunc) *Worker {
	t.Helper()
	w, err := NewWorker(c, WorkerConfig{
		Queue:        queue,
		Concurrency:  concurrency,
		Handlers:     handlers,
		PollInterval: 10 * time.Millisecond,
		Backoff:      func(int) time.Duration { return 0 },
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// runUntil runs the worker until the queue has `done` + `dead` finished jobs.
func runUntil(t *testing.T, c *Client, w *Worker, queue string, finished int64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		stats, err := c.Stats(context.Background(), queue)
		if err != nil {
			t.Fatal(err)
		}
		if stats["done"]+stats["dead"] >= finished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout, stats: %v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEveryJobRunsExactlyOnce(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()

	const total = 300
	for i := 0; i < total; i++ {
		if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "count"}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	runs := map[int64]int{}
	handlers := map[string]HandlerFunc{
		"count": func(ctx context.Context, job *Job) error {
			mu.Lock()
			defer mu.Unlock()
			runs[job.ID]++
			return nil
		},
	}

	// two separate workers with many goroutines compete for the same rows
	w1 := newTestWorker(t, c, queue, 10, handlers)
	w2 := newTestWorker(t, c, queue, 10, handlers)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, w := range []*Worker{w1, w2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(runCtx); err != nil {
				t.Error(err)
			}
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		stats, err := c.Stats(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}
		if stats["done"] == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout, stats: %v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if len(runs) != total {
		t.Errorf("%d different jobs ran, want %d", len(runs), total)
	}
	for id, n := range runs {
		if n != 1 {
			t.Errorf("job %d ran %d times", id, n)
		}
	}
}

func TestRetryThenSuccess(t *testing.T) {
	c, queue := setup(t)
	if _, err := c.Enqueue(context.Background(), EnqueueParams{Queue: queue, Kind: "flaky"}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	w := newTestWorker(t, c, queue, 1, map[string]HandlerFunc{
		"flaky": func(ctx context.Context, job *Job) error {
			if calls.Add(1) < 3 {
				return errors.New("temporary problem")
			}
			return nil
		},
	})
	runUntil(t, c, w, queue, 1)

	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	stats, _ := c.Stats(context.Background(), queue)
	if stats["done"] != 1 || stats["dead"] != 0 {
		t.Errorf("stats = %v", stats)
	}
}

func TestDeadAfterMaxAttemptsAndRetryDead(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "broken", MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	w := newTestWorker(t, c, queue, 1, map[string]HandlerFunc{
		"broken": func(ctx context.Context, job *Job) error {
			calls.Add(1)
			return errors.New("always fails")
		},
	})
	runUntil(t, c, w, queue, 1)

	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	var status, lastError string
	err = c.pool.QueryRow(ctx, `SELECT status, last_error FROM jobs WHERE id = $1`, id).Scan(&status, &lastError)
	if err != nil {
		t.Fatal(err)
	}
	if status != "dead" || lastError != "always fails" {
		t.Errorf("status = %q, last_error = %q", status, lastError)
	}

	if err := c.RetryDead(ctx, id); err != nil {
		t.Fatal(err)
	}
	stats, _ := c.Stats(ctx, queue)
	if stats["queued"] != 1 {
		t.Errorf("stats after RetryDead = %v", stats)
	}
}

func TestPanicAndUnknownKindDoNotKillWorker(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	for _, kind := range []string{"panics", "no-such-handler", "ok"} {
		if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: kind, MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
	}

	w := newTestWorker(t, c, queue, 1, map[string]HandlerFunc{
		"panics": func(ctx context.Context, job *Job) error { panic("oops") },
		"ok":     func(ctx context.Context, job *Job) error { return nil },
	})
	runUntil(t, c, w, queue, 3)

	stats, _ := c.Stats(ctx, queue)
	if stats["done"] != 1 || stats["dead"] != 2 {
		t.Errorf("stats = %v", stats)
	}
}

func TestDelayedJobIsNotClaimedEarly(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "later", Delay: time.Hour}); err != nil {
		t.Fatal(err)
	}
	job, err := c.claim(ctx, queue, "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if job != nil {
		t.Errorf("claimed a job that should wait: %+v", job)
	}
}

func TestUniqueKey(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	p := EnqueueParams{Queue: queue, Kind: "report", UniqueKey: "user-1"}

	if _, err := c.Enqueue(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, p); !errors.Is(err, ErrDuplicate) {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}

	job, err := c.claim(ctx, queue, "w1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, p); !errors.Is(err, ErrDuplicate) {
		t.Errorf("running job: err = %v, want ErrDuplicate", err)
	}

	if err := c.complete(ctx, job, "w1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(ctx, p); err != nil {
		t.Errorf("key should be free after the job is done: %v", err)
	}
}

func TestEnqueueTxRollback(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnqueueTx(ctx, tx, EnqueueParams{Queue: queue, Kind: "email"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	stats, _ := c.Stats(ctx, queue)
	if stats["queued"] != 0 {
		t.Errorf("job survived a rollback: %v", stats)
	}
}

func TestReaperReturnsJobOfCrashedWorker(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "x"}); err != nil {
		t.Fatal(err)
	}

	crashed, err := c.claim(ctx, queue, "crashed", time.Millisecond)
	if err != nil || crashed == nil {
		t.Fatalf("claim: %v, %v", crashed, err)
	}
	time.Sleep(20 * time.Millisecond)

	n, err := c.reapExpired(ctx, queue)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reaped %d, want 1", n)
	}

	next, err := c.claim(ctx, queue, "alive", time.Minute)
	if err != nil || next == nil {
		t.Fatalf("claim after reap: %v, %v", next, err)
	}
	if next.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", next.Attempts)
	}

	// the old worker wakes up and tries to report: it must not change anything
	if err := c.complete(ctx, crashed, "crashed"); err != nil {
		t.Fatal(err)
	}
	stats, _ := c.Stats(ctx, queue)
	if stats["running"] != 1 || stats["done"] != 0 {
		t.Errorf("late result of old worker was saved: %v", stats)
	}
}

func TestHeartbeatKeepsLongJobAlive(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "slow"}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	w, err := NewWorker(c, WorkerConfig{
		Queue:        queue,
		Concurrency:  3,
		PollInterval: 10 * time.Millisecond,
		Lease:        300 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handlers: map[string]HandlerFunc{
			"slow": func(ctx context.Context, job *Job) error {
				calls.Add(1)
				time.Sleep(time.Second) // much longer than the lease
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runUntil(t, c, w, queue, 1)

	if calls.Load() != 1 {
		t.Errorf("job ran %d times, the lease was not extended", calls.Load())
	}
}

func TestGracefulShutdownWaitsForRunningJob(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "slow"}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	w := newTestWorker(t, c, queue, 1, map[string]HandlerFunc{
		"slow": func(ctx context.Context, job *Job) error {
			close(started)
			time.Sleep(200 * time.Millisecond)
			return ctx.Err() // shutdown must not cancel a job that has time to finish
		},
	})

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	stats, _ := c.Stats(ctx, queue)
	if stats["done"] != 1 {
		t.Errorf("stats = %v, job should be finished before Run returns", stats)
	}
}

func TestShutdownTimeoutCancelsJob(t *testing.T) {
	c, queue := setup(t)
	ctx := context.Background()
	if _, err := c.Enqueue(ctx, EnqueueParams{Queue: queue, Kind: "stuck"}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	w, err := NewWorker(c, WorkerConfig{
		Queue:           queue,
		PollInterval:    10 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handlers: map[string]HandlerFunc{
			"stuck": func(ctx context.Context, job *Job) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	<-started
	cancel()

	if err := <-done; err == nil {
		t.Error("expected a shutdown timeout error")
	}
	stats, _ := c.Stats(ctx, queue)
	if stats["queued"] != 1 {
		t.Errorf("stats = %v, cancelled job should go back to the queue", stats)
	}
}

func TestDefaultBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		minWait time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{5, 32 * time.Second},
		{20, time.Hour},
		{1000, time.Hour}, // must not overflow
	}
	for _, tt := range tests {
		got := DefaultBackoff(tt.attempt)
		maxWait := tt.minWait + tt.minWait/10
		if got < tt.minWait || got > maxWait {
			t.Errorf("attempt %d: got %v, want between %v and %v", tt.attempt, got, tt.minWait, maxWait)
		}
	}
}
