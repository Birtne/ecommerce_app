CREATE TABLE IF NOT EXISTS admin_session_blacklist (
  token TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_session_blacklist_expire ON admin_session_blacklist(expires_at);

CREATE TABLE IF NOT EXISTS admin_command_idempotency (
  actor_user_id BIGINT NOT NULL,
  action TEXT NOT NULL,
  command_id TEXT NOT NULL,
  response_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(actor_user_id, action, command_id)
);

CREATE INDEX IF NOT EXISTS idx_admin_command_idempotency_created ON admin_command_idempotency(created_at DESC);

CREATE TABLE IF NOT EXISTS user_order_totals (
  user_id BIGINT NOT NULL,
  status TEXT NOT NULL,
  total BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(user_id, status)
);

CREATE TABLE IF NOT EXISTS order_stats_refresh_queue (
  user_id BIGINT PRIMARY KEY,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_stats_refresh_queue_requested ON order_stats_refresh_queue(requested_at);

CREATE INDEX IF NOT EXISTS idx_audit_logs_target_created ON audit_logs(target_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_payload_gin ON audit_logs USING GIN (payload jsonb_path_ops);
CREATE INDEX IF NOT EXISTS idx_audit_logs_search_tsv ON audit_logs USING GIN (to_tsvector('simple', coalesce(action,'') || ' ' || coalesce(target_type,'') || ' ' || coalesce(target_id,'')));
