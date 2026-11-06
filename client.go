// Package pgqueue is a background job queue that only needs PostgreSQL.
package pgqueue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// ErrDuplicate means a job with this unique key is already queued or running.
var ErrDuplicate = errors.New("job with this unique key already exists")

type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
}

type Client struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Client {
	return &Client{pool: pool}
}

// Migrate creates the tables. It is safe to call on every start.
func (c *Client) Migrate(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

type EnqueueParams struct {
	Queue   string
	Kind    string
	Payload any
	// Delay postpones the first run.
	Delay time.Duration
	// MaxAttempts defaults to 5.
	MaxAttempts int
	// UniqueKey is optional, see ErrDuplicate.
	UniqueKey string
}

func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (int64, error) {
	return enqueue(ctx, c.pool, p)
}

// EnqueueTx adds a job inside the caller's transaction. The job exists only if
// the transaction commits, so there is no "saved the order but lost the email" case.
func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, p EnqueueParams) (int64, error) {
	return enqueue(ctx, tx, p)
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func enqueue(ctx context.Context, q querier, p EnqueueParams) (int64, error) {
	if p.Queue == "" || p.Kind == "" {
		return 0, errors.New("queue and kind are required")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.Payload == nil {
		p.Payload = struct{}{}
	}
	payload, err := json.Marshal(p.Payload)
	if err != nil {
		return 0, fmt.Errorf("marshal payload: %w", err)
	}

	var uniqueKey *string
	if p.UniqueKey != "" {
		uniqueKey = &p.UniqueKey
	}

	var id int64
	err = q.QueryRow(ctx, `
		INSERT INTO jobs (queue, kind, payload, max_attempts, unique_key, run_at)
		VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
		ON CONFLICT (queue, unique_key)
			WHERE unique_key IS NOT NULL AND status IN ('queued', 'running')
			DO NOTHING
		RETURNING id`,
		p.Queue, p.Kind, payload, p.MaxAttempts, uniqueKey, p.Delay.Seconds(),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrDuplicate
	}
	if err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	return id, nil
}

// Stats returns the number of jobs per status in a queue.
func (c *Client) Stats(ctx context.Context, queue string) (map[string]int64, error) {
	rows, err := c.pool.Query(ctx, `SELECT status, count(*) FROM jobs WHERE queue = $1 GROUP BY status`, queue)
	if err != nil {
		return nil, fmt.Errorf("select stats: %w", err)
	}
	defer rows.Close()

	stats := map[string]int64{"queued": 0, "running": 0, "done": 0, "dead": 0}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
	}
	return stats, rows.Err()
}

// RetryDead puts a dead job back in the queue after the bug is fixed.
func (c *Client) RetryDead(ctx context.Context, id int64) error {
	tag, err := c.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL
		WHERE id = $1 AND status = 'dead'`, id)
	if err != nil {
		return fmt.Errorf("retry job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("job %d is not dead", id)
	}
	return nil
}

// claim takes the next ready job or returns nil if there is nothing to do.
//
// FOR UPDATE SKIP LOCKED is the whole trick: workers skip rows another worker
// is claiming right now instead of waiting for them.
func (c *Client) claim(ctx context.Context, queue, workerID string, lease time.Duration) (*Job, error) {
	var j Job
	err := c.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id
			FROM jobs
			WHERE queue = $1 AND status = 'queued' AND run_at <= now()
			ORDER BY run_at, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET status = 'running',
		    attempts = attempts + 1,
		    locked_by = $2,
		    locked_until = now() + make_interval(secs => $3)
		FROM next
		WHERE jobs.id = next.id
		RETURNING jobs.id, jobs.queue, jobs.kind, jobs.payload, jobs.attempts, jobs.max_attempts`,
		queue, workerID, lease.Seconds(),
	).Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Attempts, &j.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return &j, nil
}

// All updates below check locked_by and attempts. If this worker lost the lease and
// someone else took the job, the old worker must not overwrite the result.

func (c *Client) complete(ctx context.Context, j *Job, workerID string) error {
	_, err := c.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'done', finished_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3`,
		j.ID, workerID, j.Attempts)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", j.ID, err)
	}
	return nil
}

func (c *Client) fail(ctx context.Context, j *Job, workerID string, jobErr error, retryIn time.Duration) error {
	_, err := c.pool.Exec(ctx, `
		UPDATE jobs
		SET status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
		    run_at      = now() + make_interval(secs => $4),
		    last_error  = $5,
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3`,
		j.ID, workerID, j.Attempts, retryIn.Seconds(), jobErr.Error())
	if err != nil {
		return fmt.Errorf("fail job %d: %w", j.ID, err)
	}
	return nil
}

// extendLease returns false if the job is no longer owned by this worker.
func (c *Client) extendLease(ctx context.Context, j *Job, workerID string, lease time.Duration) (bool, error) {
	tag, err := c.pool.Exec(ctx, `
		UPDATE jobs
		SET locked_until = now() + make_interval(secs => $4)
		WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3`,
		j.ID, workerID, j.Attempts, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("extend lease of job %d: %w", j.ID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// reapExpired returns jobs of crashed workers to the queue.
func (c *Client) reapExpired(ctx context.Context, queue string) (int64, error) {
	tag, err := c.pool.Exec(ctx, `
		UPDATE jobs
		SET status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
		    run_at      = now(),
		    last_error  = 'lease expired, worker ' || locked_by || ' is gone',
		    locked_by = NULL,
		    locked_until = NULL
		WHERE queue = $1 AND status = 'running' AND locked_until < now()`, queue)
	if err != nil {
		return 0, fmt.Errorf("reap expired jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}
