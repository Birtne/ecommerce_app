CREATE TABLE IF NOT EXISTS dead_letter_events (
  id BIGSERIAL PRIMARY KEY,
  source_outbox_id BIGINT NOT NULL,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  last_error TEXT,
  retry_count INT NOT NULL,
  moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_moved_at ON dead_letter_events (moved_at DESC);
