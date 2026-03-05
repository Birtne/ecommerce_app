ALTER TABLE replay_jobs
  ADD COLUMN IF NOT EXISTS trace_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS command_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_replay_jobs_trace_command ON replay_jobs(trace_id, command_id, created_at DESC);

ALTER TABLE outbox_events
  ADD COLUMN IF NOT EXISTS trace_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS command_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS correlation_source TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS replay_job_id BIGINT,
  ADD COLUMN IF NOT EXISTS dead_letter_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_outbox_trace_command ON outbox_events(trace_id, command_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_outbox_replay_job ON outbox_events(replay_job_id, id DESC);
