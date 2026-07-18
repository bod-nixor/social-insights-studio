const { createId } = require('./security');

async function providerFoundationTablesExist(connection) {
  const rows = await connection.query(
    `SELECT TABLE_NAME AS table_name
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'provider_authorizations'
     LIMIT 1`
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function findProviderAuthorization(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT id FROM provider_authorizations
     WHERE source_data_source_id = ?
     LIMIT 1`,
    [dataSourceId]
  );
  return rows[0] || null;
}

async function upsertTikTokProviderFoundation(connection, { workspaceId, actorUserId, dataSourceId, profile, status }) {
  if (!(await providerFoundationTablesExist(connection))) {
    return;
  }

  await connection.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, source_data_source_id, provider_subject, display_name, status, granted_at)
     VALUES (?, ?, 'tiktok', ?, ?, ?, ?, ?, UTC_TIMESTAMP(3))
     ON DUPLICATE KEY UPDATE
      actor_user_id = VALUES(actor_user_id),
      provider_subject = VALUES(provider_subject),
      display_name = VALUES(display_name),
      status = VALUES(status),
      revoked_at = NULL,
      updated_at = UTC_TIMESTAMP(3)`,
    [
      createId(),
      workspaceId,
      actorUserId,
      dataSourceId,
      profile.open_id,
      profile.display_name || profile.username || 'TikTok account',
      status
    ]
  );

  const authorization = await findProviderAuthorization(connection, dataSourceId);
  if (!authorization) return;

  await connection.query(
    `INSERT INTO provider_authorization_credentials
      (id, provider_authorization_id, access_token_ciphertext, access_token_iv, access_token_tag,
       refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version, token_type,
       access_expires_at, refresh_expires_at, revoked_at, created_at, updated_at)
     SELECT ?, ?, access_token_ciphertext, access_token_iv, access_token_tag,
            refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version, token_type,
            access_expires_at, refresh_expires_at, revoked_at, created_at, updated_at
     FROM oauth_credentials
     WHERE data_source_id = ?
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
      updated_at = UTC_TIMESTAMP(3)`,
    [createId(), authorization.id, dataSourceId]
  );

  await connection.query(
    `INSERT INTO provider_authorization_scopes
      (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
     SELECT ?, scope, status, granted_at, last_confirmed_at
     FROM provider_scopes
     WHERE data_source_id = ?
     ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      granted_at = VALUES(granted_at),
      last_confirmed_at = VALUES(last_confirmed_at)`,
    [authorization.id, dataSourceId]
  );

  await connection.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type, provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'tiktok', 'tiktok_account', ?, ?, JSON_OBJECT('profile_deep_link', ?))
     ON DUPLICATE KEY UPDATE
      display_name = VALUES(display_name),
      metadata = VALUES(metadata),
      updated_at = UTC_TIMESTAMP(3)`,
    [
      createId(),
      authorization.id,
      workspaceId,
      profile.open_id,
      profile.display_name || profile.username || 'TikTok account',
      profile.profile_deep_link || null
    ]
  );

  const resourceRows = await connection.query(
    `SELECT id FROM provider_resources
     WHERE provider_authorization_id = ? AND resource_type = 'tiktok_account' AND provider_resource_id = ?
     LIMIT 1`,
    [authorization.id, profile.open_id]
  );
  const resource = resourceRows[0] || null;
  if (!resource) return;

  await connection.query(
    `INSERT INTO workspace_provider_connections
      (id, workspace_id, provider_resource_id, data_source_id, provider, status, last_sync_at, last_successful_sync_at, next_sync_at)
     SELECT ?, ds.workspace_id, ?, ds.id, ds.provider, ds.status,
            ds.last_sync_at, ds.last_successful_sync_at, ds.next_sync_at
     FROM data_sources ds
     WHERE ds.id = ?
     ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      last_sync_at = VALUES(last_sync_at),
      last_successful_sync_at = VALUES(last_successful_sync_at),
      next_sync_at = VALUES(next_sync_at),
      updated_at = UTC_TIMESTAMP(3)`,
    [createId(), resource.id, dataSourceId]
  );

  await upsertTikTokCapabilityState(connection, dataSourceId);
  await upsertTikTokSyncState(connection, dataSourceId);
}

async function upsertTikTokCapabilityState(connection, dataSourceId) {
  await connection.query(
    `INSERT INTO provider_capabilities
      (id, workspace_provider_connection_id, capability_key, status, reason)
     SELECT UUID(), wpc.id, capability.capability_key,
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
     WHERE wpc.data_source_id = ?
     ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      reason = VALUES(reason),
      updated_at = UTC_TIMESTAMP(3)`,
    [dataSourceId]
  );
}

async function upsertTikTokSyncState(connection, dataSourceId) {
  await connection.query(
    `INSERT INTO provider_sync_states
      (id, workspace_provider_connection_id, sync_key, cursor_state, last_attempt_at, last_success_at, data_through_at, retry_after_at)
     SELECT UUID(), wpc.id, 'tiktok.sync', JSON_OBJECT(),
            ds.last_sync_at, ds.last_successful_sync_at, ds.last_successful_sync_at, sj.run_after
     FROM workspace_provider_connections wpc
     JOIN data_sources ds ON ds.id = wpc.data_source_id
     LEFT JOIN sync_jobs sj ON sj.data_source_id = ds.id
     WHERE wpc.data_source_id = ?
     ON DUPLICATE KEY UPDATE
      last_attempt_at = VALUES(last_attempt_at),
      last_success_at = VALUES(last_success_at),
      data_through_at = VALUES(data_through_at),
      retry_after_at = VALUES(retry_after_at)`,
    [dataSourceId]
  );
}

async function markTikTokProviderFoundationDisconnected(connection, { dataSourceId, actorUserId, providerRevoke }) {
  if (!(await providerFoundationTablesExist(connection))) {
    return;
  }
  await connection.query(
    `UPDATE provider_authorizations
     SET status = 'disconnected', revoked_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3)
     WHERE source_data_source_id = ?`,
    [dataSourceId]
  );
  await connection.query(
    `UPDATE workspace_provider_connections
     SET status = 'disconnected', next_sync_at = NULL, updated_at = UTC_TIMESTAMP(3)
     WHERE data_source_id = ?`,
    [dataSourceId]
  );
  await upsertTikTokCapabilityState(connection, dataSourceId);
  const authorization = await findProviderAuthorization(connection, dataSourceId);
  if (!authorization) return;
  const connectionRows = await connection.query(
    `SELECT id FROM workspace_provider_connections
     WHERE data_source_id = ?
     LIMIT 1`,
    [dataSourceId]
  );
  await connection.query(
    `INSERT INTO provider_revocation_events
      (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id, provider, status,
       provider_status_code, failure_category, metadata)
     VALUES (?, ?, ?, ?, 'tiktok', ?, ?, ?, ?)`,
    [
      createId(),
      authorization.id,
      connectionRows[0] ? connectionRows[0].id : null,
      actorUserId,
      providerRevoke && providerRevoke.success ? 'provider_revoked' : 'local_revoked',
      providerRevoke ? providerRevoke.status || null : null,
      providerRevoke && providerRevoke.error ? providerRevoke.error.category : null,
      JSON.stringify({
        provider_revoke: providerRevoke && {
          attempted: providerRevoke.attempted,
          success: providerRevoke.success,
          status: providerRevoke.status,
          category: providerRevoke.error && providerRevoke.error.category
        }
      })
    ]
  );
}

module.exports = {
  markTikTokProviderFoundationDisconnected,
  providerFoundationTablesExist,
  upsertTikTokProviderFoundation
};
