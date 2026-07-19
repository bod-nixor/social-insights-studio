ALTER TABLE data_sources
  DROP CONSTRAINT data_sources_provider_check;

ALTER TABLE data_sources
  ADD CONSTRAINT data_sources_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  );

ALTER TABLE oauth_transactions
  DROP CONSTRAINT oauth_transactions_provider_check;

ALTER TABLE oauth_transactions
  ADD CONSTRAINT oauth_transactions_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  );

ALTER TABLE sync_runs
  ADD COLUMN workspace_provider_connection_id CHAR(36) NULL AFTER data_source_id,
  ADD COLUMN provider_api_version VARCHAR(64) NULL AFTER correlation_id,
  ADD CONSTRAINT sync_runs_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE SET NULL,
  ADD INDEX sync_runs_connection_started_idx (workspace_provider_connection_id, started_at);

CREATE TABLE provider_resource_observations (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  data_through_at DATETIME(3) NULL,
  source_timezone VARCHAR(64) NULL,
  observed_values JSON NOT NULL,
  availability JSON NOT NULL,
  CONSTRAINT resource_observations_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  ),
  CONSTRAINT resource_observations_run_unique
    UNIQUE (workspace_provider_connection_id, sync_run_id),
  CONSTRAINT resource_observations_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT resource_observations_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT resource_observations_run_fk
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX resource_observations_workspace_provider_idx (workspace_id, provider, observed_at),
  INDEX resource_observations_connection_idx (workspace_provider_connection_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_metric_observations (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  metric_key VARCHAR(160) NOT NULL,
  grain VARCHAR(24) NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  data_through_at DATETIME(3) NULL,
  numeric_value DECIMAL(30,8) NULL,
  unit VARCHAR(32) NOT NULL,
  availability_status VARCHAR(32) NOT NULL,
  availability_reason VARCHAR(255) NULL,
  definition_version VARCHAR(64) NOT NULL,
  CONSTRAINT metric_observations_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  ),
  CONSTRAINT metric_observations_grain_check CHECK (
    grain IN ('snapshot', 'daily', 'range', 'lifetime')
  ),
  CONSTRAINT metric_observations_availability_check CHECK (
    availability_status IN (
      'available', 'not_granted', 'not_supported', 'not_reported',
      'delayed', 'thresholded', 'provider_error'
    )
  ),
  CONSTRAINT metric_observations_period_check CHECK (period_start <= period_end),
  CONSTRAINT metric_observations_value_check CHECK (
    (availability_status = 'available' AND numeric_value IS NOT NULL)
    OR (availability_status <> 'available' AND numeric_value IS NULL)
  ),
  CONSTRAINT metric_observations_run_metric_unique
    UNIQUE (workspace_provider_connection_id, sync_run_id, metric_key, grain, period_start, period_end),
  CONSTRAINT metric_observations_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT metric_observations_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT metric_observations_run_fk
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX metric_observations_workspace_metric_idx (workspace_id, provider, metric_key, period_end),
  INDEX metric_observations_connection_period_idx (workspace_provider_connection_id, period_start, period_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_dimension_observations (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_run_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  breakdown_key VARCHAR(160) NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  data_through_at DATETIME(3) NULL,
  dimension_hash CHAR(64) NOT NULL,
  dimension_values JSON NOT NULL,
  metric_values JSON NOT NULL,
  availability JSON NOT NULL,
  thresholded BOOLEAN NOT NULL DEFAULT FALSE,
  row_position INT UNSIGNED NOT NULL DEFAULT 0,
  CONSTRAINT dimension_observations_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  ),
  CONSTRAINT dimension_observations_period_check CHECK (period_start <= period_end),
  CONSTRAINT dimension_observations_run_row_unique
    UNIQUE (
      workspace_provider_connection_id, sync_run_id, breakdown_key,
      period_start, period_end, dimension_hash
    ),
  CONSTRAINT dimension_observations_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT dimension_observations_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT dimension_observations_run_fk
    FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
  INDEX dimension_observations_workspace_idx (workspace_id, provider, breakdown_key, period_end),
  INDEX dimension_observations_connection_idx (workspace_provider_connection_id, period_start, period_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_definitions (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  created_by_user_id CHAR(36) NOT NULL,
  title VARCHAR(180) NOT NULL,
  subtitle VARCHAR(300) NULL,
  timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
  range_start DATE NOT NULL,
  range_end DATE NOT NULL,
  comparison_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  configuration JSON NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'active',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  CONSTRAINT report_definitions_range_check CHECK (range_start <= range_end),
  CONSTRAINT report_definitions_status_check CHECK (status IN ('active', 'deleted')),
  CONSTRAINT report_definitions_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT report_definitions_creator_fk
    FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE RESTRICT,
  INDEX report_definitions_workspace_created_idx (workspace_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_definition_resources (
  report_definition_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  position SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (report_definition_id, workspace_provider_connection_id),
  CONSTRAINT report_definition_resources_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  ),
  CONSTRAINT report_definition_resources_definition_fk
    FOREIGN KEY (report_definition_id) REFERENCES report_definitions(id) ON DELETE CASCADE,
  CONSTRAINT report_definition_resources_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  INDEX report_definition_resources_connection_idx (workspace_provider_connection_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_runs (
  id CHAR(36) PRIMARY KEY,
  report_definition_id CHAR(36) NOT NULL,
  workspace_id CHAR(36) NOT NULL,
  requested_by_user_id CHAR(36) NULL,
  idempotency_key CHAR(64) NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'queued',
  configuration_snapshot JSON NOT NULL,
  metric_definitions_snapshot JSON NOT NULL,
  data_through_at DATETIME(3) NULL,
  queued_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  run_after DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  started_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  lease_owner VARCHAR(128) NULL,
  lease_expires_at DATETIME(3) NULL,
  attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  max_attempts SMALLINT UNSIGNED NOT NULL DEFAULT 3,
  progress_percent SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  failure_category VARCHAR(64) NULL,
  failure_code VARCHAR(120) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT report_runs_status_check CHECK (
    status IN ('queued', 'running', 'complete', 'failed', 'expired')
  ),
  CONSTRAINT report_runs_progress_check CHECK (progress_percent <= 100),
  CONSTRAINT report_runs_definition_fk
    FOREIGN KEY (report_definition_id) REFERENCES report_definitions(id) ON DELETE RESTRICT,
  CONSTRAINT report_runs_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT report_runs_requester_fk
    FOREIGN KEY (requested_by_user_id) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT report_runs_workspace_idempotency_unique UNIQUE (workspace_id, idempotency_key),
  INDEX report_runs_workspace_created_idx (workspace_id, created_at),
  INDEX report_runs_worker_due_idx (status, run_after, lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_run_resources (
  report_run_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NULL,
  provider VARCHAR(32) NOT NULL,
  provider_resource_id VARCHAR(191) NOT NULL,
  resource_name VARCHAR(255) NOT NULL,
  data_through_at DATETIME(3) NULL,
  position SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (report_run_id, provider, provider_resource_id),
  CONSTRAINT report_run_resources_provider_check CHECK (
    provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4')
  ),
  CONSTRAINT report_run_resources_run_fk
    FOREIGN KEY (report_run_id) REFERENCES report_runs(id) ON DELETE CASCADE,
  CONSTRAINT report_run_resources_connection_fk
    FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE SET NULL,
  INDEX report_run_resources_connection_idx (workspace_provider_connection_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_artifacts (
  id CHAR(36) PRIMARY KEY,
  report_run_id CHAR(36) NOT NULL,
  workspace_id CHAR(36) NOT NULL,
  storage_key VARCHAR(255) NOT NULL,
  download_filename VARCHAR(255) NOT NULL,
  mime_type VARCHAR(80) NOT NULL DEFAULT 'application/pdf',
  byte_size BIGINT UNSIGNED NOT NULL,
  sha256 CHAR(64) NOT NULL,
  page_count SMALLINT UNSIGNED NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'active',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  expires_at DATETIME(3) NOT NULL,
  deleted_at DATETIME(3) NULL,
  CONSTRAINT report_artifacts_status_check CHECK (status IN ('active', 'deleted', 'expired')),
  CONSTRAINT report_artifacts_run_unique UNIQUE (report_run_id),
  CONSTRAINT report_artifacts_storage_key_unique UNIQUE (storage_key),
  CONSTRAINT report_artifacts_run_fk
    FOREIGN KEY (report_run_id) REFERENCES report_runs(id) ON DELETE CASCADE,
  CONSTRAINT report_artifacts_workspace_fk
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  INDEX report_artifacts_expiry_idx (status, expires_at),
  INDEX report_artifacts_workspace_created_idx (workspace_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE report_download_grants (
  id CHAR(36) PRIMARY KEY,
  report_artifact_id CHAR(36) NOT NULL,
  requested_by_user_id CHAR(36) NOT NULL,
  token_hash CHAR(64) NOT NULL,
  expires_at DATETIME(3) NOT NULL,
  consumed_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT report_download_grants_token_unique UNIQUE (token_hash),
  CONSTRAINT report_download_grants_artifact_fk
    FOREIGN KEY (report_artifact_id) REFERENCES report_artifacts(id) ON DELETE CASCADE,
  CONSTRAINT report_download_grants_user_fk
    FOREIGN KEY (requested_by_user_id) REFERENCES users(id) ON DELETE CASCADE,
  INDEX report_download_grants_expiry_idx (expires_at, consumed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
