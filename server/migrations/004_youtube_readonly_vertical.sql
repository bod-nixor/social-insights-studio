ALTER TABLE data_sources
  DROP CONSTRAINT data_sources_provider_check;

ALTER TABLE data_sources
  ADD CONSTRAINT data_sources_provider_check CHECK (provider IN ('tiktok', 'youtube'));

ALTER TABLE oauth_transactions
  DROP CONSTRAINT oauth_transactions_provider_check;

ALTER TABLE oauth_transactions
  ADD CONSTRAINT oauth_transactions_provider_check CHECK (provider IN ('tiktok', 'youtube'));

ALTER TABLE oauth_transactions
  ADD COLUMN session_id CHAR(36) NULL AFTER initiated_by,
  ADD COLUMN provider_authorization_id CHAR(36) NULL AFTER session_id,
  ADD COLUMN target_connection_id CHAR(36) NULL AFTER provider_authorization_id,
  ADD COLUMN requested_scopes JSON NULL AFTER return_path,
  ADD COLUMN redirect_uri VARCHAR(2048) NULL AFTER requested_scopes,
  ADD COLUMN pkce_verifier_ciphertext TEXT NULL AFTER redirect_uri,
  ADD COLUMN pkce_verifier_iv VARCHAR(64) NULL AFTER pkce_verifier_ciphertext,
  ADD COLUMN pkce_verifier_tag VARCHAR(64) NULL AFTER pkce_verifier_iv,
  ADD COLUMN pkce_key_version VARCHAR(64) NULL AFTER pkce_verifier_tag,
  ADD CONSTRAINT oauth_transactions_session_fk FOREIGN KEY (session_id) REFERENCES user_sessions(id) ON DELETE CASCADE,
  ADD CONSTRAINT oauth_transactions_authorization_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE SET NULL,
  ADD CONSTRAINT oauth_transactions_target_connection_fk FOREIGN KEY (target_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE SET NULL,
  ADD INDEX oauth_transactions_session_provider_idx (session_id, provider, status, expires_at);

ALTER TABLE provider_authorizations
  ADD COLUMN last_validated_at DATETIME(3) NULL AFTER granted_at,
  ADD COLUMN deletion_due_at DATETIME(3) NULL AFTER revoked_at,
  ADD INDEX provider_authorizations_youtube_validation_due_idx (provider, status, deletion_due_at);

ALTER TABLE provider_resources
  ADD CONSTRAINT provider_resources_workspace_resource_unique
    UNIQUE (workspace_id, provider, resource_type, provider_resource_id);

ALTER TABLE sync_jobs
  ADD COLUMN requested_trigger_type VARCHAR(16) NOT NULL DEFAULT 'scheduled' AFTER status,
  ADD CONSTRAINT sync_jobs_requested_trigger_check CHECK (requested_trigger_type IN ('scheduled', 'manual'));

ALTER TABLE sync_errors
  DROP CONSTRAINT sync_errors_category_check;

ALTER TABLE sync_errors
  ADD CONSTRAINT sync_errors_category_check CHECK (
    category IN ('authentication', 'scope', 'rate_limit', 'quota', 'data_delay', 'provider', 'timeout', 'network', 'malformed_response', 'internal')
  );

CREATE TABLE youtube_channel_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  subscriber_count BIGINT UNSIGNED NULL,
  subscriber_count_hidden BOOLEAN NOT NULL DEFAULT FALSE,
  lifetime_view_count BIGINT UNSIGNED NULL,
  public_video_count BIGINT UNSIGNED NULL,
  uploads_playlist_id VARCHAR(191) NULL,
  thumbnail_url TEXT NULL,
  availability JSON NOT NULL,
  CONSTRAINT youtube_channel_snapshots_source_run_unique UNIQUE (data_source_id, sync_run_id),
  CONSTRAINT youtube_channel_snapshots_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT youtube_channel_snapshots_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  CONSTRAINT youtube_channel_snapshots_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT youtube_channel_snapshots_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX youtube_channel_snapshots_source_observed_idx (data_source_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE youtube_analytics_daily_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  report_date DATE NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  data_through_date DATE NOT NULL,
  views BIGINT UNSIGNED NULL,
  estimated_minutes_watched DECIMAL(20,4) NULL,
  average_view_duration DECIMAL(20,4) NULL,
  average_view_percentage DECIMAL(12,6) NULL,
  subscribers_gained BIGINT UNSIGNED NULL,
  subscribers_lost BIGINT UNSIGNED NULL,
  likes BIGINT UNSIGNED NULL,
  comments BIGINT UNSIGNED NULL,
  shares BIGINT UNSIGNED NULL,
  availability JSON NOT NULL,
  CONSTRAINT youtube_analytics_daily_source_run_date_unique UNIQUE (data_source_id, sync_run_id, report_date),
  CONSTRAINT youtube_analytics_daily_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT youtube_analytics_daily_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  CONSTRAINT youtube_analytics_daily_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT youtube_analytics_daily_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX youtube_analytics_daily_source_date_idx (data_source_id, report_date),
  INDEX youtube_analytics_daily_workspace_date_idx (workspace_id, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE youtube_video_analytics_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  content_item_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  period_key VARCHAR(16) NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  data_through_date DATE NULL,
  observed_at DATETIME(3) NOT NULL,
  views BIGINT UNSIGNED NULL,
  estimated_minutes_watched DECIMAL(20,4) NULL,
  average_view_duration DECIMAL(20,4) NULL,
  average_view_percentage DECIMAL(12,6) NULL,
  subscribers_gained BIGINT UNSIGNED NULL,
  subscribers_lost BIGINT UNSIGNED NULL,
  likes BIGINT UNSIGNED NULL,
  comments BIGINT UNSIGNED NULL,
  shares BIGINT UNSIGNED NULL,
  availability JSON NOT NULL,
  CONSTRAINT youtube_video_analytics_period_check CHECK (period_key IN ('7d', '30d', '90d')),
  CONSTRAINT youtube_video_analytics_item_run_period_unique UNIQUE (content_item_id, sync_run_id, period_key),
  CONSTRAINT youtube_video_analytics_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT youtube_video_analytics_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  CONSTRAINT youtube_video_analytics_item_fk FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
  CONSTRAINT youtube_video_analytics_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX youtube_video_analytics_source_period_idx (data_source_id, period_key, period_end),
  INDEX youtube_video_analytics_workspace_period_idx (workspace_id, period_key, period_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_request_events (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  provider_authorization_id CHAR(36) NULL,
  workspace_provider_connection_id CHAR(36) NULL,
  sync_run_id CHAR(36) NULL,
  provider VARCHAR(32) NOT NULL,
  request_category VARCHAR(32) NOT NULL,
  method_name VARCHAR(120) NOT NULL,
  quota_cost_estimate DECIMAL(10,2) NOT NULL DEFAULT 0,
  page_number INT UNSIGNED NULL,
  item_count INT UNSIGNED NULL,
  attempts INT UNSIGNED NOT NULL DEFAULT 1,
  status VARCHAR(32) NOT NULL,
  failure_category VARCHAR(64) NULL,
  retry_after_seconds INT UNSIGNED NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_request_events_provider_check CHECK (provider IN ('tiktok', 'instagram', 'facebook_pages', 'youtube', 'google_analytics_4')),
  CONSTRAINT provider_request_events_category_check CHECK (request_category IN ('oauth', 'data_api', 'analytics_api', 'revoke')),
  CONSTRAINT provider_request_events_status_check CHECK (status IN ('success', 'empty', 'partial', 'failed', 'delayed')),
  CONSTRAINT provider_request_events_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT provider_request_events_authorization_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE SET NULL,
  CONSTRAINT provider_request_events_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE SET NULL,
  CONSTRAINT provider_request_events_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX provider_request_events_run_created_idx (sync_run_id, created_at),
  INDEX provider_request_events_workspace_provider_idx (workspace_id, provider, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
