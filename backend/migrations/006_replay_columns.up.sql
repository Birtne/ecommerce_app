ALTER TABLE idempotency_keys
  ADD COLUMN IF NOT EXISTS last_replay_at TIMESTAMPTZ;

ALTER TABLE dead_letter_events
  ADD COLUMN IF NOT EXISTS last_replay_at TIMESTAMPTZ;
