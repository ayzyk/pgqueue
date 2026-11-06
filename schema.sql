SELECT pg_advisory_xact_lock(918273);

CREATE TABLE IF NOT EXISTS jobs (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue        TEXT        NOT NULL,
    kind         TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}',
    status       TEXT        NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'done', 'dead')),
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    unique_key   TEXT,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by    TEXT,
    locked_until TIMESTAMPTZ,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

-- Partial index, it only has rows that are waiting. The table can grow to millions
-- of finished jobs and the claim query still looks at a small index.
CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (queue, run_at, id) WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS jobs_expired_idx ON jobs (queue, locked_until) WHERE status = 'running';

-- The same key can be enqueued again after the previous job has finished.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_key_idx ON jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND status IN ('queued', 'running');
