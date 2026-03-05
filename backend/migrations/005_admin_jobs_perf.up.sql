ALTER TABLE users
  ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'customer';

CREATE INDEX IF NOT EXISTS idx_users_email_role ON users(email, role);

CREATE TABLE IF NOT EXISTS admin_sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_token_expire ON admin_sessions(token, expires_at);

CREATE TABLE IF NOT EXISTS audit_logs (
  id BIGSERIAL PRIMARY KEY,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action, created_at DESC);

CREATE TABLE IF NOT EXISTS replay_jobs (
  id BIGSERIAL PRIMARY KEY,
  created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
  status TEXT NOT NULL,
  topic_filter TEXT NOT NULL DEFAULT '',
  total_items INT NOT NULL DEFAULT 0,
  processed_items INT NOT NULL DEFAULT 0,
  success_items INT NOT NULL DEFAULT 0,
  failed_items INT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS replay_job_items (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
  dead_letter_id BIGINT NOT NULL REFERENCES dead_letter_events(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  error_group TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(job_id, dead_letter_id)
);

CREATE INDEX IF NOT EXISTS idx_replay_jobs_status_updated ON replay_jobs(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_replay_job_items_job_status ON replay_job_items(job_id, status, updated_at);

CREATE INDEX IF NOT EXISTS idx_orders_user_status_created_id ON orders(user_id, status, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_orders_user_created_id ON orders(user_id, created_at DESC, id DESC);
