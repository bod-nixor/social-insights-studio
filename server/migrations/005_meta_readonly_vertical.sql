ALTER TABLE data_sources
  DROP CONSTRAINT data_sources_provider_check;

ALTER TABLE data_sources
  ADD CONSTRAINT data_sources_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram')
  );

ALTER TABLE oauth_transactions
  DROP CONSTRAINT oauth_transactions_provider_check;

ALTER TABLE oauth_transactions
  ADD CONSTRAINT oauth_transactions_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram')
  );

CREATE TABLE provider_resource_credentials (
  id CHAR(36) PRIMARY KEY,
  provider_resource_id CHAR(36) NOT NULL,
  access_token_ciphertext TEXT NOT NULL,
  access_token_iv VARCHAR(64) NOT NULL,
  access_token_tag VARCHAR(64) NOT NULL,
  key_version VARCHAR(64) NOT NULL,
  token_type VARCHAR(32) NOT NULL DEFAULT 'Bearer',
  access_expires_at DATETIME(3) NULL,
  revoked_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_resource_credentials_resource_unique UNIQUE (provider_resource_id),
  CONSTRAINT provider_resource_credentials_resource_fk
    FOREIGN KEY (provider_resource_id) REFERENCES provider_resources(id) ON DELETE CASCADE,
  INDEX provider_resource_credentials_expiry_idx (access_expires_at, revoked_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE meta_account_insight_snapshots (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  snapshot_kind VARCHAR(16) NOT NULL,
  report_date DATE NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  metric_values JSON NOT NULL,
  availability JSON NOT NULL,
  CONSTRAINT meta_account_insights_provider_check CHECK (provider IN ('facebook_pages', 'instagram')),
  CONSTRAINT meta_account_insights_kind_check CHECK (snapshot_kind IN ('profile', 'daily')),
  CONSTRAINT meta_account_insights_source_run_kind_date_unique
    UNIQUE (data_source_id, sync_run_id, snapshot_kind, report_date),
  CONSTRAINT meta_account_insights_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT meta_account_insights_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
  CONSTRAINT meta_account_insights_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT meta_account_insights_run_fk FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX meta_account_insights_source_date_idx (data_source_id, snapshot_kind, report_date),
  INDEX meta_account_insights_workspace_provider_date_idx (workspace_id, provider, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE meta_callback_events (
  id CHAR(36) PRIMARY KEY,
  callback_type VARCHAR(24) NOT NULL,
  signed_request_hash CHAR(64) NOT NULL,
  provider_subject_hash CHAR(64) NOT NULL,
  confirmation_code VARCHAR(96) NULL,
  status VARCHAR(24) NOT NULL,
  authorization_count INT UNSIGNED NOT NULL DEFAULT 0,
  completed_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT meta_callback_events_type_check CHECK (callback_type IN ('data_deletion', 'deauthorization')),
  CONSTRAINT meta_callback_events_status_check CHECK (status IN ('processing', 'completed', 'failed')),
  CONSTRAINT meta_callback_events_signed_request_unique UNIQUE (signed_request_hash),
  CONSTRAINT meta_callback_events_confirmation_unique UNIQUE (confirmation_code),
  INDEX meta_callback_events_subject_created_idx (provider_subject_hash, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
