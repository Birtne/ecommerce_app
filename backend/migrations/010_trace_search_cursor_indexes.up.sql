CREATE INDEX IF NOT EXISTS idx_replay_jobs_trace_command_id_desc
ON replay_jobs(trace_id, command_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_trace_id_desc
ON replay_jobs(trace_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_command_id_desc
ON replay_jobs(command_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_trace_id_desc
ON outbox_events(trace_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_command_id_desc
ON outbox_events(command_id, id DESC);
