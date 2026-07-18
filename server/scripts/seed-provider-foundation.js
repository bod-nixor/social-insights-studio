function fixedId(group, index) {
  return `${String(group).padStart(8, '0')}-0000-4000-8000-${String(index).padStart(12, '0')}`;
}

function utcSql(date) {
  return date.toISOString().slice(0, 23).replace('T', ' ');
}

async function upsertProviderFoundation(connection, source, ownerId) {
  const fixtureIndex = Number(source.id.slice(-3));
  const providerSubject = `local-demo-tiktok-${fixtureIndex}`;
  const displayName = source.workspaceName.replace(/ Workspace$/, ' Account');
  await connection.query(
    `INSERT INTO provider_accounts
       (id, workspace_id, data_source_id, provider, provider_account_id, username, display_name, metadata)
     VALUES (?, ?, ?, 'tiktok', ?, ?, ?, JSON_OBJECT('fixture', TRUE))
     ON DUPLICATE KEY UPDATE
       provider_account_id = VALUES(provider_account_id),
       username = VALUES(username),
       display_name = VALUES(display_name),
       metadata = VALUES(metadata),
       updated_at = UTC_TIMESTAMP(3)`,
    [
      fixedId(71000000, fixtureIndex),
      source.workspaceId,
      source.id,
      providerSubject,
      `local-demo-${fixtureIndex}`,
      displayName
    ]
  );
  await connection.query(
    `INSERT INTO provider_authorizations
       (id, workspace_id, provider, actor_user_id, source_data_source_id,
        provider_subject, display_name, status, api_version, granted_at)
     VALUES (?, ?, 'tiktok', ?, ?, ?, ?, ?, 'v2', ?)
     ON DUPLICATE KEY UPDATE
       actor_user_id = VALUES(actor_user_id),
       provider_subject = VALUES(provider_subject),
       display_name = VALUES(display_name),
       status = VALUES(status),
       api_version = VALUES(api_version),
       granted_at = VALUES(granted_at),
       revoked_at = NULL,
       updated_at = UTC_TIMESTAMP(3)`,
    [
      fixedId(72000000, fixtureIndex),
      source.workspaceId,
      ownerId,
      source.id,
      providerSubject,
      displayName,
      source.status,
      source.status === 'active' ? utcSql(source.lastSuccessfulSyncAt || new Date()) : null
    ]
  );
  const authorizationRows = await connection.query(
    'SELECT id FROM provider_authorizations WHERE source_data_source_id = ? LIMIT 1',
    [source.id]
  );
  const authorizationId = authorizationRows[0].id;
  await connection.query(
    `INSERT INTO provider_resources
       (id, provider_authorization_id, workspace_id, provider, resource_type,
        provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'tiktok', 'tiktok_account', ?, ?, JSON_OBJECT('fixture', TRUE))
     ON DUPLICATE KEY UPDATE
       display_name = VALUES(display_name),
       metadata = VALUES(metadata),
       updated_at = UTC_TIMESTAMP(3)`,
    [fixedId(73000000, fixtureIndex), authorizationId, source.workspaceId, providerSubject, displayName]
  );
  const resourceRows = await connection.query(
    `SELECT id FROM provider_resources
     WHERE provider_authorization_id = ? AND resource_type = 'tiktok_account' AND provider_resource_id = ?
     LIMIT 1`,
    [authorizationId, providerSubject]
  );
  await connection.query(
    `INSERT INTO workspace_provider_connections
       (id, workspace_id, provider_resource_id, data_source_id, provider, status,
        last_sync_at, last_successful_sync_at, next_sync_at, data_through_at)
     VALUES (?, ?, ?, ?, 'tiktok', ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       provider_resource_id = VALUES(provider_resource_id),
       status = VALUES(status),
       last_sync_at = VALUES(last_sync_at),
       last_successful_sync_at = VALUES(last_successful_sync_at),
       next_sync_at = VALUES(next_sync_at),
       data_through_at = VALUES(data_through_at),
       updated_at = UTC_TIMESTAMP(3)`,
    [
      fixedId(74000000, fixtureIndex),
      source.workspaceId,
      resourceRows[0].id,
      source.id,
      source.status,
      source.lastSyncAt ? utcSql(source.lastSyncAt) : null,
      source.lastSuccessfulSyncAt ? utcSql(source.lastSuccessfulSyncAt) : null,
      source.nextSyncAt ? utcSql(source.nextSyncAt) : null,
      source.lastSuccessfulSyncAt ? utcSql(source.lastSuccessfulSyncAt) : null
    ]
  );
}

module.exports = {
  upsertProviderFoundation
};
