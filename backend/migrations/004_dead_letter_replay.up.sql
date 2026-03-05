ALTER TABLE dead_letter_events
  ADD COLUMN IF NOT EXISTS replay_status TEXT NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS replay_count INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_replay_error TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_replay_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_dead_letter_replay_status ON dead_letter_events(replay_status, moved_at DESC);
