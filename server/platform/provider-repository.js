const { getConnection } = require('../database');

function databaseUnavailable() {
  const error = new Error('database_not_configured');
  error.status = 503;
  error.code = 'database_not_configured';
  return error;
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) {
    throw databaseUnavailable();
  }
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
}

async function listProviderAuthorizations(workspaceId) {
  return withConnection(connection => connection.query(
    `SELECT id, workspace_id, provider, actor_user_id, source_data_source_id,
            provider_subject, display_name, status, auth_product, granted_at, revoked_at,
            created_at, updated_at
     FROM provider_authorizations
     WHERE workspace_id = ?
     ORDER BY provider, created_at`,
    [workspaceId]
  ));
}

async function listWorkspaceProviderConnections(workspaceId) {
  return withConnection(connection => connection.query(
    `SELECT wpc.id,
            wpc.workspace_id,
            wpc.provider,
            wpc.status,
            wpc.data_source_id,
            pr.resource_type,
            pr.provider_resource_id,
            pr.display_name,
            pauth.id AS provider_authorization_id
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     WHERE wpc.workspace_id = ?
     ORDER BY wpc.provider, pr.display_name`,
    [workspaceId]
  ));
}

async function findProviderAuthorizationByDataSource(dataSourceId) {
  return withConnection(async connection => {
    const rows = await connection.query(
      `SELECT pauth.*, pac.key_version, pac.access_expires_at, pac.refresh_expires_at, pac.revoked_at AS credential_revoked_at
       FROM provider_authorizations pauth
       LEFT JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
       WHERE pauth.source_data_source_id = ?
       LIMIT 1`,
      [dataSourceId]
    );
    return rows[0] || null;
  });
}

module.exports = {
  findProviderAuthorizationByDataSource,
  listProviderAuthorizations,
  listWorkspaceProviderConnections
};
