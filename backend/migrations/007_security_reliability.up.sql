ALTER TABLE admin_sessions
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_admin_sessions_active ON admin_sessions(token, expires_at, revoked_at);

CREATE TABLE IF NOT EXISTS role_permissions (
  role TEXT NOT NULL,
  action TEXT NOT NULL,
  PRIMARY KEY(role, action)
);

INSERT INTO role_permissions(role, action) VALUES
  ('admin','replay_job:create'),
  ('admin','replay_job:read'),
  ('admin','replay_job:retry_failed'),
  ('admin','audit:read')
ON CONFLICT(role, action) DO NOTHING;

ALTER TABLE replay_jobs
  ADD COLUMN IF NOT EXISTS lease_owner TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5;

ALTER TABLE replay_job_items
  ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5,
  ADD COLUMN IF NOT EXISTS terminal BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_replay_job_items_next_attempt ON replay_job_items(job_id, status, next_attempt_at);
