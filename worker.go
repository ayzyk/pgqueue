package pgqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"sync"
	"time"
)

// HandlerFunc does the work. The context is cancelled when the worker loses the
// lease or the shutdown timeout runs out, long handlers should respect it.
//
// A job can run more than once (crash after the work but before saving the result),
// so handlers have to be idempotent.
type HandlerFunc func(ctx context.Context, job *Job) error

type WorkerConfig struct {
	Queue       string
	Concurrency int
	Handlers    map[string]HandlerFunc

	// All fields below are optional.
	PollInterval    time.Duration
	Lease           time.Duration
	ShutdownTimeout time.Duration
	Backoff         func(attempt int) time.Duration
	Logger          *slog.Logger
}

type Worker struct {
	client *Client
	cfg    WorkerConfig
	id     string
}

func NewWorker(client *Client, cfg WorkerConfig) (*Worker, error) {
	if cfg.Queue == "" {
		return nil, errors.New("queue is required")
	}
	if len(cfg.Handlers) == 0 {
		return nil, errors.New("at least one handler is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{client: client, cfg: cfg, id: newWorkerID()}, nil
}

func newWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}

// DefaultBackoff waits 2, 4, 8... seconds up to one hour, plus up to 10% jitter
// so that jobs which failed together do not all come back at the same moment.
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	wait := time.Hour
	if attempt < 12 {
		wait = min(time.Duration(1<<attempt)*time.Second, time.Hour)
	}
	return wait + time.Duration(mrand.Int64N(int64(wait)/10+1))
}

// Run blocks until ctx is cancelled and all running jobs are finished.
func (w *Worker) Run(ctx context.Context) error {
	// Handlers do not use ctx directly: on shutdown they get ShutdownTimeout to finish.
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx, jobsCtx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.reapLoop(ctx)
	}()

	<-ctx.Done()
	w.cfg.Logger.Info("stopping worker, waiting for running jobs", "queue", w.cfg.Queue)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancelJobs()
		<-done
		return errors.New("shutdown timeout, some jobs were cancelled")
	}
}

func (w *Worker) loop(ctx, jobsCtx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := w.client.claim(ctx, w.cfg.Queue, w.id, w.cfg.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.cfg.Logger.Error("claim failed", "err", err)
		}
		if job != nil {
			w.process(jobsCtx, job)
			continue // there may be more work, do not sleep
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(w.cfg.PollInterval)):
		}
	}
}

func (w *Worker) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Lease / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.client.reapExpired(ctx, w.cfg.Queue)
			if err != nil && ctx.Err() == nil {
				w.cfg.Logger.Error("reap failed", "err", err)
			}
			if n > 0 {
				w.cfg.Logger.Warn("returned expired jobs to the queue", "count", n)
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, job *Job) {
	log := w.cfg.Logger.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempts)

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stopHeartbeat := w.startHeartbeat(jobCtx, cancel, job)
	jobErr := w.runHandler(jobCtx, job)
	stopHeartbeat()

	// The result has to be saved even if the worker is shutting down.
	saveCtx, cancelSave := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSave()

	if jobErr == nil {
		if err := w.client.complete(saveCtx, job, w.id); err != nil {
			log.Error("could not save result", "err", err)
		}
		return
	}

	log.Warn("job failed", "err", jobErr)
	if err := w.client.fail(saveCtx, job, w.id, jobErr, w.cfg.Backoff(job.Attempts)); err != nil {
		log.Error("could not save failure", "err", err)
	}
}

// startHeartbeat keeps extending the lease while the handler runs.
func (w *Worker) startHeartbeat(ctx context.Context, cancelJob context.CancelFunc, job *Job) (stop func()) {
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(w.cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				owned, err := w.client.extendLease(hbCtx, job, w.id, w.cfg.Lease)
				if err != nil {
					if hbCtx.Err() == nil {
						w.cfg.Logger.Error("heartbeat failed", "job_id", job.ID, "err", err)
					}
					continue
				}
				if !owned {
					w.cfg.Logger.Warn("lost the lease, cancelling job", "job_id", job.ID)
					cancelJob()
					return
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func (w *Worker) runHandler(ctx context.Context, job *Job) (err error) {
	// one bad job must not kill the whole worker process
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	handler, ok := w.cfg.Handlers[job.Kind]
	if !ok {
		return fmt.Errorf("no handler for kind %q", job.Kind)
	}
	return handler(ctx, job)
}

func jitter(d time.Duration) time.Duration {
	return d + time.Duration(mrand.Int64N(int64(d)/5+1))
}
