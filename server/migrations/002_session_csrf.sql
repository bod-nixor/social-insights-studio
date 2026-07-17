ALTER TABLE user_sessions
  ADD COLUMN csrf_token_hash CHAR(64) NOT NULL AFTER token_hash,
  ADD INDEX user_sessions_csrf_idx (csrf_token_hash);
