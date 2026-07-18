CREATE TABLE provider_authorizations (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  actor_user_id CHAR(36) NULL,
  source_data_source_id CHAR(36) NULL,
  provider_subject VARCHAR(191) NULL,
  display_name VARCHAR(255) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'authorizing',
  auth_product VARCHAR(32) NOT NULL DEFAULT 'analytics',
  api_version VARCHAR(64) NULL,
  granted_at DATETIME(3) NULL,
  revoked_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_authorizations_provider_check CHECK (provider IN ('tiktok', 'instagram', 'facebook_pages', 'youtube', 'google_analytics_4')),
  CONSTRAINT provider_authorizations_status_check CHECK (status IN ('authorizing', 'connecting', 'active', 'reconnect_required', 'disconnected', 'revoked', 'disabled')),
  CONSTRAINT provider_authorizations_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT provider_authorizations_actor_fk FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT provider_authorizations_source_fk FOREIGN KEY (source_data_source_id) REFERENCES data_sources(id) ON DELETE SET NULL,
  CONSTRAINT provider_authorizations_source_unique UNIQUE (source_data_source_id),
  INDEX provider_authorizations_workspace_provider_idx (workspace_id, provider, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_authorization_credentials (
  id CHAR(36) PRIMARY KEY,
  provider_authorization_id CHAR(36) NOT NULL,
  access_token_ciphertext TEXT NOT NULL,
  access_token_iv VARCHAR(64) NOT NULL,
  access_token_tag VARCHAR(64) NOT NULL,
  refresh_token_ciphertext TEXT NULL,
  refresh_token_iv VARCHAR(64) NULL,
  refresh_token_tag VARCHAR(64) NULL,
  key_version VARCHAR(64) NOT NULL,
  token_type VARCHAR(32) NOT NULL DEFAULT 'Bearer',
  access_expires_at DATETIME(3) NULL,
  refresh_expires_at DATETIME(3) NULL,
  revoked_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_authorization_credentials_auth_unique UNIQUE (provider_authorization_id),
  CONSTRAINT provider_authorization_credentials_auth_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE CASCADE,
  INDEX provider_authorization_credentials_expiry_idx (access_expires_at, refresh_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_authorization_scopes (
  provider_authorization_id CHAR(36) NOT NULL,
  scope VARCHAR(255) NOT NULL,
  status VARCHAR(32) NOT NULL,
  granted_at DATETIME(3) NULL,
  last_confirmed_at DATETIME(3) NULL,
  PRIMARY KEY (provider_authorization_id, scope),
  CONSTRAINT provider_authorization_scopes_status_check CHECK (status IN ('granted', 'missing', 'denied', 'revoked')),
  CONSTRAINT provider_authorization_scopes_auth_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_resources (
  id CHAR(36) PRIMARY KEY,
  provider_authorization_id CHAR(36) NOT NULL,
  workspace_id CHAR(36) NOT NULL,
  provider VARCHAR(32) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  provider_resource_id VARCHAR(191) NOT NULL,
  display_name VARCHAR(255) NOT NULL,
  metadata JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_resources_provider_check CHECK (provider IN ('tiktok', 'instagram', 'facebook_pages', 'youtube', 'google_analytics_4')),
  CONSTRAINT provider_resources_type_check CHECK (resource_type IN ('tiktok_account', 'instagram_account', 'facebook_page', 'youtube_channel', 'ga4_property')),
  CONSTRAINT provider_resources_auth_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE CASCADE,
  CONSTRAINT provider_resources_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT provider_resources_auth_resource_unique UNIQUE (provider_authorization_id, resource_type, provider_resource_id),
  INDEX provider_resources_workspace_provider_idx (workspace_id, provider, resource_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE workspace_provider_connections (
  id CHAR(36) PRIMARY KEY,
  workspace_id CHAR(36) NOT NULL,
  provider_resource_id CHAR(36) NOT NULL,
  data_source_id CHAR(36) NULL,
  provider VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'disconnected',
  last_sync_at DATETIME(3) NULL,
  last_successful_sync_at DATETIME(3) NULL,
  next_sync_at DATETIME(3) NULL,
  data_through_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT workspace_provider_connections_status_check CHECK (status IN ('connecting', 'active', 'reconnect_required', 'disconnected', 'revoked', 'disabled')),
  CONSTRAINT workspace_provider_connections_workspace_fk FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
  CONSTRAINT workspace_provider_connections_resource_fk FOREIGN KEY (provider_resource_id) REFERENCES provider_resources(id) ON DELETE RESTRICT,
  CONSTRAINT workspace_provider_connections_data_source_fk FOREIGN KEY (data_source_id) REFERENCES data_sources(id) ON DELETE SET NULL,
  CONSTRAINT workspace_provider_connections_resource_unique UNIQUE (workspace_id, provider_resource_id),
  CONSTRAINT workspace_provider_connections_source_unique UNIQUE (data_source_id),
  INDEX workspace_provider_connections_workspace_provider_idx (workspace_id, provider, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_capabilities (
  id CHAR(36) PRIMARY KEY,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  capability_key VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL,
  reason VARCHAR(255) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_capabilities_status_check CHECK (status IN ('available', 'not_granted', 'not_supported', 'delayed', 'provider_error', 'disabled', 'pending')),
  CONSTRAINT provider_capabilities_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT provider_capabilities_connection_key_unique UNIQUE (workspace_provider_connection_id, capability_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_sync_states (
  id CHAR(36) PRIMARY KEY,
  workspace_provider_connection_id CHAR(36) NOT NULL,
  sync_key VARCHAR(128) NOT NULL,
  cursor_state JSON NULL,
  api_version VARCHAR(64) NULL,
  last_attempt_at DATETIME(3) NULL,
  last_success_at DATETIME(3) NULL,
  data_through_at DATETIME(3) NULL,
  retry_after_at DATETIME(3) NULL,
  failure_category VARCHAR(64) NULL,
  failure_count INT UNSIGNED NOT NULL DEFAULT 0,
  CONSTRAINT provider_sync_states_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE CASCADE,
  CONSTRAINT provider_sync_states_connection_key_unique UNIQUE (workspace_provider_connection_id, sync_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE provider_revocation_events (
  id CHAR(36) PRIMARY KEY,
  provider_authorization_id CHAR(36) NOT NULL,
  workspace_provider_connection_id CHAR(36) NULL,
  actor_user_id CHAR(36) NULL,
  provider VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  provider_status_code INT NULL,
  failure_category VARCHAR(64) NULL,
  metadata JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  CONSTRAINT provider_revocation_events_provider_check CHECK (provider IN ('tiktok', 'instagram', 'facebook_pages', 'youtube', 'google_analytics_4')),
  CONSTRAINT provider_revocation_events_status_check CHECK (status IN ('requested', 'provider_revoked', 'local_revoked', 'failed')),
  CONSTRAINT provider_revocation_events_auth_fk FOREIGN KEY (provider_authorization_id) REFERENCES provider_authorizations(id) ON DELETE CASCADE,
  CONSTRAINT provider_revocation_events_connection_fk FOREIGN KEY (workspace_provider_connection_id) REFERENCES workspace_provider_connections(id) ON DELETE SET NULL,
  CONSTRAINT provider_revocation_events_actor_fk FOREIGN KEY (actor_user_id) REFERENCES users(id) ON DELETE SET NULL,
  INDEX provider_revocation_events_auth_created_idx (provider_authorization_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO provider_authorizations
  (id, workspace_id, provider, actor_user_id, source_data_source_id, provider_subject, display_name, status, granted_at, revoked_at, created_at, updated_at)
SELECT
  UUID(),
  ds.workspace_id,
  ds.provider,
  w.created_by,
  ds.id,
  pa.provider_account_id,
  COALESCE(pa.display_name, pa.username, 'TikTok account'),
  ds.status,
  CASE WHEN ds.status IN ('active', 'reconnect_required') THEN COALESCE(pa.created_at, ds.created_at) ELSE NULL END,
  CASE WHEN ds.status IN ('revoked', 'disabled', 'disconnected') THEN oc.revoked_at ELSE NULL END,
  ds.created_at,
  ds.updated_at
FROM data_sources ds
JOIN workspaces w ON w.id = ds.workspace_id
LEFT JOIN provider_accounts pa ON pa.data_source_id = ds.id
LEFT JOIN oauth_credentials oc ON oc.data_source_id = ds.id
WHERE ds.provider = 'tiktok'
  AND ds.deleted_at IS NULL
ON DUPLICATE KEY UPDATE
  provider_subject = VALUES(provider_subject),
  display_name = VALUES(display_name),
  status = VALUES(status),
  revoked_at = VALUES(revoked_at),
  updated_at = UTC_TIMESTAMP(3);

INSERT INTO provider_authorization_credentials
  (id, provider_authorization_id, access_token_ciphertext, access_token_iv, access_token_tag,
   refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version, token_type,
   access_expires_at, refresh_expires_at, revoked_at, created_at, updated_at)
SELECT
  UUID(),
  pauth.id,
  oc.access_token_ciphertext,
  oc.access_token_iv,
  oc.access_token_tag,
  oc.refresh_token_ciphertext,
  oc.refresh_token_iv,
  oc.refresh_token_tag,
  oc.key_version,
  oc.token_type,
  oc.access_expires_at,
  oc.refresh_expires_at,
  oc.revoked_at,
  oc.created_at,
  oc.updated_at
FROM oauth_credentials oc
JOIN provider_authorizations pauth ON pauth.source_data_source_id = oc.data_source_id
ON DUPLICATE KEY UPDATE
  access_token_ciphertext = VALUES(access_token_ciphertext),
  access_token_iv = VALUES(access_token_iv),
  access_token_tag = VALUES(access_token_tag),
  refresh_token_ciphertext = VALUES(refresh_token_ciphertext),
  refresh_token_iv = VALUES(refresh_token_iv),
  refresh_token_tag = VALUES(refresh_token_tag),
  key_version = VALUES(key_version),
  token_type = VALUES(token_type),
  access_expires_at = VALUES(access_expires_at),
  refresh_expires_at = VALUES(refresh_expires_at),
  revoked_at = VALUES(revoked_at),
  updated_at = UTC_TIMESTAMP(3);

INSERT INTO provider_authorization_scopes
  (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
SELECT
  pauth.id,
  ps.scope,
  ps.status,
  ps.granted_at,
  ps.last_confirmed_at
FROM provider_scopes ps
JOIN provider_authorizations pauth ON pauth.source_data_source_id = ps.data_source_id
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  granted_at = VALUES(granted_at),
  last_confirmed_at = VALUES(last_confirmed_at);

INSERT INTO provider_resources
  (id, provider_authorization_id, workspace_id, provider, resource_type, provider_resource_id, display_name, metadata, created_at, updated_at)
SELECT
  UUID(),
  pauth.id,
  ds.workspace_id,
  'tiktok',
  'tiktok_account',
  COALESCE(pa.provider_account_id, ds.id),
  COALESCE(pa.display_name, pa.username, 'TikTok account'),
  pa.metadata,
  COALESCE(pa.created_at, ds.created_at),
  COALESCE(pa.updated_at, ds.updated_at)
FROM provider_authorizations pauth
JOIN data_sources ds ON ds.id = pauth.source_data_source_id
LEFT JOIN provider_accounts pa ON pa.data_source_id = ds.id
WHERE pauth.provider = 'tiktok'
ON DUPLICATE KEY UPDATE
  display_name = VALUES(display_name),
  metadata = VALUES(metadata),
  updated_at = VALUES(updated_at);

INSERT INTO workspace_provider_connections
  (id, workspace_id, provider_resource_id, data_source_id, provider, status, last_sync_at, last_successful_sync_at, next_sync_at, created_at, updated_at)
SELECT
  UUID(),
  ds.workspace_id,
  pr.id,
  ds.id,
  ds.provider,
  ds.status,
  ds.last_sync_at,
  ds.last_successful_sync_at,
  ds.next_sync_at,
  ds.created_at,
  ds.updated_at
FROM data_sources ds
JOIN provider_authorizations pauth ON pauth.source_data_source_id = ds.id
JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id AND pr.resource_type = 'tiktok_account'
WHERE ds.provider = 'tiktok'
  AND ds.deleted_at IS NULL
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  last_sync_at = VALUES(last_sync_at),
  last_successful_sync_at = VALUES(last_successful_sync_at),
  next_sync_at = VALUES(next_sync_at),
  updated_at = UTC_TIMESTAMP(3);

INSERT INTO provider_capabilities
  (id, workspace_provider_connection_id, capability_key, status, reason)
SELECT
  UUID(),
  wpc.id,
  capability.capability_key,
  CASE
    WHEN wpc.status = 'active' THEN 'available'
    WHEN wpc.status = 'reconnect_required' THEN 'not_granted'
    WHEN wpc.status IN ('disconnected', 'revoked', 'disabled') THEN 'disabled'
    ELSE 'pending'
  END,
  CASE
    WHEN wpc.status = 'reconnect_required' THEN 'missing_or_expired_scope'
    WHEN wpc.status IN ('disconnected', 'revoked', 'disabled') THEN 'connection_not_active'
    ELSE NULL
  END
FROM workspace_provider_connections wpc
JOIN (
  SELECT 'profile_identity' AS capability_key
  UNION ALL SELECT 'profile_snapshot_metrics'
  UNION ALL SELECT 'content_listing'
  UNION ALL SELECT 'content_snapshot_metrics'
  UNION ALL SELECT 'disconnect'
) capability
WHERE wpc.provider = 'tiktok'
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  reason = VALUES(reason),
  updated_at = UTC_TIMESTAMP(3);

INSERT INTO provider_sync_states
  (id, workspace_provider_connection_id, sync_key, cursor_state, last_attempt_at, last_success_at, data_through_at, retry_after_at, failure_category)
SELECT
  UUID(),
  wpc.id,
  'tiktok.sync',
  JSON_OBJECT(),
  ds.last_sync_at,
  ds.last_successful_sync_at,
  ds.last_successful_sync_at,
  sj.run_after,
  NULL
FROM workspace_provider_connections wpc
JOIN data_sources ds ON ds.id = wpc.data_source_id
LEFT JOIN sync_jobs sj ON sj.data_source_id = ds.id
WHERE wpc.provider = 'tiktok'
ON DUPLICATE KEY UPDATE
  last_attempt_at = VALUES(last_attempt_at),
  last_success_at = VALUES(last_success_at),
  data_through_at = VALUES(data_through_at),
  retry_after_at = VALUES(retry_after_at);

INSERT INTO provider_revocation_events
  (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id, provider, status, metadata, created_at)
SELECT
  UUID(),
  pauth.id,
  wpc.id,
  pauth.actor_user_id,
  pauth.provider,
  'local_revoked',
  JSON_OBJECT('source_status', wpc.status),
  COALESCE(pauth.revoked_at, wpc.updated_at)
FROM provider_authorizations pauth
JOIN workspace_provider_connections wpc ON wpc.data_source_id = pauth.source_data_source_id
WHERE pauth.provider = 'tiktok'
  AND wpc.status IN ('disconnected', 'revoked', 'disabled');
