CREATE TABLE users (
  id CHAR(36) PRIMARY KEY,
  email VARCHAR(320) NOT NULL,
  display_name VARCHAR(255) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_login_at DATETIME(3) NULL,
  deleted_at DATETIME(3) NULL,
  CONSTRAINT users_email_unique UNIQUE (email),
  CONSTRAINT users_status_check CHECK (status IN ('active', 'disabled', 'deleted'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE user_identities (
  id CHAR(36) PRIMARY KEY,
  user_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  provider_subject VARCHAR(255) NOT NULL,
  email VARCHAR(320) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT user_identities_provider_subject_unique UNIQUE (provider, provider_subject),
  CONSTRAINT user_identities_user_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE user_sessions (
  id CHAR(36) PRIMARY KEY,
  user_id CHAR(36) NOT NULL,
  token_hash CHAR(64) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_seen_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  idle_expires_at DATETIME(3) NOT NULL,
  revoked_at DATETIME(3) NULL,
  user_agent_hash CHAR(64) NULL,
  CONSTRAINT user_sessions_token_hash_unique UNIQUE (token_hash),
  CONSTRAINT user_sessions_user_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  INDEX user_sessions_user_idx (user_id, revoked_at, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE magic_link_tokens (
  id CHAR(36) PRIMARY KEY,
  email VARCHAR(320) NOT NULL,
  user_id CHAR(36) NULL,
  token_hash CHAR(64) NOT NULL,
  requested_ip_hash CHAR(64) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  consumed_at DATETIME(3) NULL,
  CONSTRAINT magic_link_tokens_hash_unique UNIQUE (token_hash),
  CONSTRAINT magic_link_tokens_user_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
  INDEX magic_link_tokens_email_idx (email, created_at),
  INDEX magic_link_tokens_expiry_idx (expires_at, consumed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workspaces (
  id CHAR(36) PRIMARY KEY,
  name VARCHAR(160) NOT NULL,
  slug VARCHAR(120) NOT NULL,
  created_by CHAR(36) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  CONSTRAINT workspaces_slug_unique UNIQUE (slug),
  CONSTRAINT workspaces_created_by_fk FOREIGN KEY (created_by) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workspace_memberships (
  workspace_id CHAR(36) NOT NULL,
  user_id CHAR(36) NOT NULL,
  role VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  invited_by CHAR(36) NULL,
  joined_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (workspace_id, user_id),
  CONSTRAINT workspace_memberships_role_check CHECK (role IN ('owner', 'admin', 'analyst', 'viewer')),
  CONSTRAINT workspace_memberships_status_check CHECK (status IN ('active', 'invited', 'removed')),
  CONSTRAINT workspace_memberships_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT workspace_memberships_user_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT workspace_memberships_invited_by_fk FOREIGN KEY (invited_by) REFERENCES users(id) ON DELETE SET NULL,
  INDEX workspace_memberships_user_idx (user_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workspace_invitations (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  email VARCHAR(320) NOT NULL,
  role VARCHAR(32) NOT NULL,
  token_hash CHAR(64) NOT NULL,
  invited_by CHAR(36) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  accepted_at DATETIME(3) NULL,
  revoked_at DATETIME(3) NULL,
  CONSTRAINT workspace_invitations_role_check CHECK (role IN ('admin', 'analyst', 'viewer')),
  CONSTRAINT workspace_invitations_token_unique UNIQUE (token_hash),
  CONSTRAINT workspace_invitations_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT workspace_invitations_invited_by_fk FOREIGN KEY (invited_by) REFERENCES users(id),
  INDEX workspace_invitations_workspace_email_idx (workspace_id, email, accepted_at, revoked_at),
  INDEX workspace_invitations_expiry_idx (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE audit_logs (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NULL,
  actor_user_id CHAR(36) NULL,
  action VARCHAR(120) NOT NULL,
  target_type VARCHAR(80) NULL,
  target_id CHAR(36) NULL,
  metadata JSON NULL,
  correlation_id VARCHAR(128) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT audit_logs_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL,
  CONSTRAINT audit_logs_actor_fk FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL,
  INDEX audit_logs_workspace_created_idx (workspace_id, created_at),
  INDEX audit_logs_actor_created_idx (actor_user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE data_sources (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'disconnected',
  reconnect_reason VARCHAR(255) NULL,
  last_sync_at DATETIME(3) NULL,
  last_successful_sync_at DATETIME(3) NULL,
  next_sync_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  CONSTRAINT data_sources_status_check CHECK (status IN ('disconnected', 'connecting', 'active', 'reconnect_required', 'revoked', 'disabled')),
  CONSTRAINT data_sources_provider_check CHECK (provider IN ('tiktok')),
  CONSTRAINT data_sources_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  INDEX data_sources_workspace_provider_idx (workspace_id, provider, status),
  INDEX data_sources_due_sync_idx (status, next_sync_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_accounts (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  provider_account_id VARCHAR(191) NOT NULL,
  union_id VARCHAR(191) NULL,
  username VARCHAR(255) NULL,
  display_name VARCHAR(255) NULL,
  metadata JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_accounts_data_source_unique UNIQUE (data_source_id),
  CONSTRAINT provider_accounts_workspace_provider_unique UNIQUE (workspace_id, provider, provider_account_id),
  CONSTRAINT provider_accounts_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT provider_accounts_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE oauth_credentials (
  id CHAR(36) PRIMARY KEY,
  data_source_id CHAR(36) NOT NULL,
  access_token_ciphertext TEXT NOT NULL,
  access_token_iv VARCHAR(64) NOT NULL,
  access_token_tag VARCHAR(64) NOT NULL,
  refresh_token_ciphertext TEXT NOT NULL,
  refresh_token_iv VARCHAR(64) NOT NULL,
  refresh_token_tag VARCHAR(64) NOT NULL,
  key_version VARCHAR(64) NOT NULL,
  token_type VARCHAR(32) NOT NULL DEFAULT 'Bearer',
  access_expires_at DATETIME(3) NULL,
  refresh_expires_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  revoked_at DATETIME(3) NULL,
  CONSTRAINT oauth_credentials_data_source_unique UNIQUE (data_source_id),
  CONSTRAINT oauth_credentials_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  INDEX oauth_credentials_expiry_idx (access_expires_at, refresh_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE oauth_transactions (
  id CHAR(36) PRIMARY KEY,
  state_hash CHAR(64) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  workspace_id CHAR(36) NOT NULL,
  initiated_by CHAR(36) NOT NULL,
  return_path VARCHAR(512) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  consumed_at DATETIME(3) NULL,
  CONSTRAINT oauth_transactions_state_hash_unique UNIQUE (state_hash),
  CONSTRAINT oauth_transactions_provider_check CHECK (provider IN ('tiktok')),
  CONSTRAINT oauth_transactions_status_check CHECK (status IN ('pending', 'consumed', 'expired', 'failed')),
  CONSTRAINT oauth_transactions_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT oauth_transactions_initiated_by_fk FOREIGN KEY (initiated_by) REFERENCES users(id) ON DELETE CASCADE,
  INDEX oauth_transactions_workspace_status_idx (workspace_id, status, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_scopes (
  data_source_id CHAR(36) NOT NULL,
  scope VARCHAR(120) NOT NULL,
  status VARCHAR(32) NOT NULL,
  granted_at DATETIME(3) NULL,
  last_confirmed_at DATETIME(3) NULL,
  PRIMARY KEY (data_source_id, scope),
  CONSTRAINT provider_scopes_status_check CHECK (status IN ('granted', 'missing', 'revoked')),
  CONSTRAINT provider_scopes_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE sync_runs (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  trigger_type VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  started_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  finished_at DATETIME(3) NULL,
  duration_ms INT UNSIGNED NULL,
  attempt INT UNSIGNED NOT NULL DEFAULT 1,
  profile_count INT UNSIGNED NOT NULL DEFAULT 0,
  content_seen_count INT UNSIGNED NOT NULL DEFAULT 0,
  content_snapshot_count INT UNSIGNED NOT NULL DEFAULT 0,
  correlation_id VARCHAR(128) NULL,
  CONSTRAINT sync_runs_trigger_check CHECK (trigger_type IN ('scheduled', 'manual', 'reconnect', 'backfill')),
  CONSTRAINT sync_runs_status_check CHECK (status IN ('running', 'success', 'partial', 'failed', 'cancelled')),
  CONSTRAINT sync_runs_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT sync_runs_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  INDEX sync_runs_workspace_started_idx (workspace_id, started_at),
  INDEX sync_runs_data_source_started_idx (data_source_id, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE sync_errors (
  id CHAR(36) PRIMARY KEY,
  sync_run_id CHAR(36) NOT NULL,
  category VARCHAR(64) NOT NULL,
  provider_code VARCHAR(120) NULL,
  message VARCHAR(512) NULL,
  retryable BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT sync_errors_category_check CHECK (category IN ('authentication', 'scope', 'rate_limit', 'provider', 'timeout', 'network', 'malformed_response', 'internal')),
  CONSTRAINT sync_errors_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX sync_errors_run_idx (sync_run_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE content_items (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  provider_content_id VARCHAR(191) NOT NULL,
  published_at DATETIME(3) NULL,
  title VARCHAR(512) NULL,
  description TEXT NULL,
  share_url TEXT NULL,
  duration_seconds INT UNSIGNED NULL,
  height INT UNSIGNED NULL,
  width INT UNSIGNED NULL,
  provider_metadata JSON NULL,
  first_seen_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_seen_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  CONSTRAINT content_items_data_source_provider_unique UNIQUE (data_source_id, provider_content_id),
  CONSTRAINT content_items_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT content_items_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  INDEX content_items_workspace_published_idx (workspace_id, published_at),
  INDEX content_items_data_source_seen_idx (data_source_id, last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE profile_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  follower_count BIGINT UNSIGNED NULL,
  following_count BIGINT UNSIGNED NULL,
  likes_count BIGINT UNSIGNED NULL,
  video_count BIGINT UNSIGNED NULL,
  provider_metrics JSON NULL,
  CONSTRAINT profile_snapshots_data_source_run_unique UNIQUE (data_source_id, sync_run_id),
  CONSTRAINT profile_snapshots_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT profile_snapshots_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  CONSTRAINT profile_snapshots_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX profile_snapshots_workspace_observed_idx (workspace_id, observed_at),
  INDEX profile_snapshots_data_source_observed_idx (data_source_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE content_metric_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  content_item_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  view_count BIGINT UNSIGNED NULL,
  like_count BIGINT UNSIGNED NULL,
  comment_count BIGINT UNSIGNED NULL,
  share_count BIGINT UNSIGNED NULL,
  provider_metrics JSON NULL,
  CONSTRAINT content_metric_snapshots_item_run_unique UNIQUE (content_item_id, sync_run_id),
  CONSTRAINT content_metric_snapshots_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT content_metric_snapshots_item_fk FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
  CONSTRAINT content_metric_snapshots_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX content_metric_snapshots_workspace_observed_idx (workspace_id, observed_at),
  INDEX content_metric_snapshots_item_observed_idx (content_item_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE sync_jobs (
  id CHAR(36) PRIMARY KEY,
  data_source_id CHAR(36) NOT NULL,
  run_after DATETIME(3) NOT NULL,
  lease_owner VARCHAR(128) NULL,
  lease_expires_at DATETIME(3) NULL,
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'due',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT sync_jobs_data_source_unique UNIQUE (data_source_id),
  CONSTRAINT sync_jobs_status_check CHECK (status IN ('due', 'leased', 'paused', 'disabled')),
  CONSTRAINT sync_jobs_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  INDEX sync_jobs_due_idx (status, run_after, lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE exports (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  type VARCHAR(32) NOT NULL,
  configuration JSON NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_by CHAR(36) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT exports_type_check CHECK (type IN ('csv')),
  CONSTRAINT exports_status_check CHECK (status IN ('active', 'disabled', 'deleted')),
  CONSTRAINT exports_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT exports_created_by_fk FOREIGN KEY (created_by) REFERENCES users(id),
  INDEX exports_workspace_idx (workspace_id, type, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE export_runs (
  id CHAR(36) PRIMARY KEY,
  export_id CHAR(36) NOT NULL,
  status VARCHAR(32) NOT NULL,
  started_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  finished_at DATETIME(3) NULL,
  row_count INT UNSIGNED NULL,
  error_message VARCHAR(512) NULL,
  CONSTRAINT export_runs_status_check CHECK (status IN ('running', 'success', 'failed')),
  CONSTRAINT export_runs_export_fk FOREIGN KEY (export_id) REFERENCES exports(id) ON DELETE CASCADE,
  INDEX export_runs_export_started_idx (export_id, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE data_deletion_requests (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NULL,
  requester_user_id CHAR(36) NULL,
  requester_email VARCHAR(320) NULL,
  scope VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'requested',
  verification_hash CHAR(64) NULL,
  requested_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  completed_at DATETIME(3) NULL,
  sanitized_result JSON NULL,
  CONSTRAINT data_deletion_requests_scope_check CHECK (scope IN ('user', 'workspace', 'provider_account')),
  CONSTRAINT data_deletion_requests_status_check CHECK (status IN ('requested', 'verified', 'processing', 'completed', 'rejected')),
  CONSTRAINT data_deletion_requests_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE SET NULL,
  CONSTRAINT data_deletion_requests_user_fk FOREIGN KEY (requester_user_id) REFERENCES users(id) ON DELETE SET NULL,
  INDEX data_deletion_requests_status_idx (status, requested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE retention_jobs (
  id CHAR(36) PRIMARY KEY,
  policy_name VARCHAR(120) NOT NULL,
  policy_version VARCHAR(64) NOT NULL,
  cutoff_at DATETIME(3) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  started_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  affected_rows INT UNSIGNED NOT NULL DEFAULT 0,
  error_message VARCHAR(512) NULL,
  CONSTRAINT retention_jobs_status_check CHECK (status IN ('pending', 'running', 'success', 'failed')),
  INDEX retention_jobs_status_idx (status, cutoff_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
