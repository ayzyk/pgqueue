# pgqueue

A background job queue for Go that needs nothing except PostgreSQL. No Redis, no Kafka.

If a project already has Postgres and only a modest amount of background work, a separate broker is one more thing to run.
I wanted to see how far a single table can go, and what a queue actually has to handle when workers crash.

## Features

- Many workers on many machines, each job goes to one of them (`FOR UPDATE SKIP LOCKED`)
- Retries with exponential backoff and jitter, then a dead state you can inspect and retry by hand
- Crash recovery: a job has a lease, a heartbeat extends it, an expired lease returns the job to the queue
- Graceful shutdown: running jobs get time to finish, then their context is cancelled
- Delayed jobs and unique keys
- `EnqueueTx`: add a job in the same transaction as your business data
- A panic in a handler fails that job and does not kill the worker

## Usage

```go
client := pgqueue.New(pool)
if err := client.Migrate(ctx); err != nil {
    log.Fatal(err)
}

client.Enqueue(ctx, pgqueue.EnqueueParams{
    Queue:   "emails",
    Kind:    "send_email",
    Payload: map[string]string{"to": "user@example.com"},
})

worker, err := pgqueue.NewWorker(client, pgqueue.WorkerConfig{
    Queue:       "emails",
    Concurrency: 8,
    Handlers: map[string]pgqueue.HandlerFunc{
        "send_email": func(ctx context.Context, job *pgqueue.Job) error {
            // returning an error schedules a retry
            return nil
        },
    },
})
if err != nil {
    log.Fatal(err)
}
worker.Run(ctx) // blocks until ctx is cancelled and running jobs are finished
```

## Demo

```bash
make db
make demo
```

200 fake emails, 30% of the sends fail and get retried:

```
queued=88    running=8   done=104   dead=0
queued=8     running=5   done=187   dead=0
queued=1     running=1   done=198   dead=0
queued=0     running=0   done=200   dead=0
```

## How it works

```
             enqueue                claim
 (delay) ------------> queued ----------------> running ---- ok ----> done
                         ^                         |
                         |   error, attempts left  |
                         +-------------------------+
                         |   lease expired         |
                         +-------------------------+
                                                   |  no attempts left
                                                   +----------------> dead
```

The claim is one statement:

```sql
WITH next AS (
    SELECT id FROM jobs
    WHERE queue = $1 AND status = 'queued' AND run_at <= now()
    ORDER BY run_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs SET status = 'running', attempts = attempts + 1, ...
FROM next WHERE jobs.id = next.id
RETURNING ...
```

Without `SKIP LOCKED` all workers would wait on the same first row and run one at a time.

The job is not held in an open transaction while the handler runs. A job that takes an hour would keep a
connection busy and hold back vacuum for that whole hour. That is why there are leases instead.

### Things that surprised me

**Delivery is at least once, not exactly once.** A worker can finish the work and die before saving the result.
Then the job runs again. The queue can not solve this, so handlers have to be idempotent.

**A slow worker can come back from the dead.** Worker A stalls, its lease expires, worker B takes the job, and then
A wakes up and reports a result. So every update checks `locked_by` and `attempts`, and A's late update changes nothing.
There is a test for this: `TestReaperReturnsJobOfCrashedWorker`.

**The context on shutdown.** My first idea was to pass the `Run` context to handlers, but then SIGTERM instantly
cancels all running jobs. So there are two contexts: one stops taking new jobs, the other one is cancelled only after `ShutdownTimeout`.

**A normal index is not enough.** Finished jobs pile up, so `jobs_ready_idx` is a partial index with only `status = 'queued'` rows.

## Tests

```bash
make test
```

The tests run against real Postgres with the race detector. The main one puts 300 jobs in front of two workers with 10 goroutines each and
checks that every job ran exactly once.

## Not done yet

- `LISTEN/NOTIFY` so an idle worker does not have to wait for the next poll
- Claiming jobs in batches, one claim per job is one round trip
- A cleanup job for old `done` rows, partitioning by month would make this cheap
- Priorities
- Metrics

## When not to use this

Polling and updating rows has a limit, every claim is a write and creates a dead tuple.
For thousands of jobs per second, or when you need replay of events, a real broker is the better choice.
