ALTER TABLE user_sessions
  ADD COLUMN device_label VARCHAR(160) NULL AFTER user_agent_hash;

ALTER TABLE workspace_invitations
  ADD COLUMN last_sent_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) AFTER created_at,
  ADD COLUMN send_count SMALLINT UNSIGNED NOT NULL DEFAULT 1 AFTER last_sent_at,
  ADD COLUMN accepted_by_user_id CHAR(36) NULL AFTER accepted_at,
  ADD CONSTRAINT workspace_invitations_accepted_by_fk
    FOREIGN KEY (accepted_by_user_id) REFERENCES users(id) ON DELETE SET NULL,
  ADD INDEX workspace_invitations_token_status_idx (token_hash, accepted_at, revoked_at, expires_at);
