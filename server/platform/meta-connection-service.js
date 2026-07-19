const { getConnection } = require('../database');
const meta = require('../integrations/meta');
const { normalizeReturnPath } = require('./connection-service');
const { assertCapability } = require('./rbac');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId, hashSecret, randomToken } = require('./security');
const {
  META_GRAPH_API_VERSION,
  META_LOGIN_CONFIG_ENV,
  META_REQUIRED_SCOPES,
  getMetaConfiguration
} = require('./meta-config');

const META_PROVIDERS = Object.freeze(['facebook_pages', 'instagram']);
const META_CAPABILITIES = Object.freeze({
  facebook_pages: Object.freeze([
    'resource_discovery',
    'page_insights',
    'post_listing',
    'post_insights',
    'disconnect'
  ]),
  instagram: Object.freeze([
    'resource_discovery',
    'profile_insights',
    'media_listing',
    'media_insights',
    'disconnect'
  ])
});

function createHttpError(status, code, details = null) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  error.details = details;
  return error;
}

function assertMetaProvider(provider) {
  if (!META_PROVIDERS.includes(provider)) throw createHttpError(400, 'meta_provider_invalid');
  return provider;
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) throw createHttpError(503, 'database_not_configured');
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
}

async function requireWorkspaceRole(connection, workspaceId, userId, capability) {
  const rows = await connection.query(
    `SELECT role FROM workspace_memberships
     WHERE workspace_id = ? AND user_id = ? AND status = 'active'
     LIMIT 1`,
    [workspaceId, userId]
  );
  const membership = rows[0] || null;
  if (!membership) throw createHttpError(404, 'workspace_not_found');
  assertCapability(membership.role, capability);
  return membership.role;
}

async function metaFoundationReady(connection) {
  const tableRows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN (
         'provider_authorizations',
         'provider_authorization_credentials',
         'provider_resources',
         'provider_resource_credentials',
         'workspace_provider_connections',
         'meta_account_insight_snapshots',
         'meta_callback_events',
         'provider_request_events'
       )`
  );
  if (Number(tableRows[0] && tableRows[0].count) !== 8) return false;
  const columnRows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE()
       AND (
         (TABLE_NAME = 'meta_account_insight_snapshots'
          AND COLUMN_NAME IN ('range_days', 'range_start_date', 'range_end_date'))
         OR (TABLE_NAME = 'oauth_transactions' AND COLUMN_NAME = 'provider_config_id')
       )`
  );
  return Number(columnRows[0] && columnRows[0].count) === 4;
}

async function requireMetaReady(connection, provider, env = process.env) {
  assertMetaProvider(provider);
  const configuration = getMetaConfiguration(provider, env, {
    databaseReady: true,
    foundationReady: await metaFoundationReady(connection),
    workerReady: true
  });
  if (!configuration.connectable) {
    throw createHttpError(503, `${provider}_not_configured`, configuration.warnings);
  }
  return configuration;
}

async function writeAuditLog(connection, {
  workspaceId,
  actorUserId = null,
  action,
  targetType = null,
  targetId = null,
  metadata = null
}) {
  await connection.query(
    `INSERT INTO audit_logs
      (id, workspace_id, actor_user_id, action, target_type, target_id, metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      workspaceId,
      actorUserId,
      action,
      targetType,
      targetId,
      metadata ? JSON.stringify(metadata) : null
    ]
  );
}

function parseJson(value, fallback = {}) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function pictureUrl(picture) {
  return picture && picture.data && picture.data.url ? String(picture.data.url) : null;
}

function pageTasks(page) {
  return Array.isArray(page && page.tasks) ? page.tasks.map(task => String(task).toUpperCase()) : [];
}

function normalizeDiscoveredResources(provider, pages) {
  const resources = [];
  for (const page of pages) {
    if (!page || !page.id || !page.access_token) continue;
    const tasks = pageTasks(page);
    if (!tasks.includes('ANALYZE')) continue;
    if (provider === 'facebook_pages') {
      resources.push({
        providerResourceId: String(page.id),
        resourceType: 'facebook_page',
        displayName: String(page.name || 'Facebook Page').slice(0, 255),
        accessToken: String(page.access_token),
        metadata: {
          thumbnailUrl: pictureUrl(page.picture),
          analyzeAccess: true,
          selectable: true,
          discoveryStatus: 'available'
        }
      });
      continue;
    }
    const account = page.instagram_business_account;
    if (!account || !account.id) continue;
    resources.push({
      providerResourceId: String(account.id),
      resourceType: 'instagram_account',
      displayName: String(account.name || account.username || 'Instagram professional account').slice(0, 255),
      accessToken: String(page.access_token),
      metadata: {
        username: account.username ? String(account.username) : null,
        thumbnailUrl: account.profile_picture_url ? String(account.profile_picture_url) : null,
        followerCount: Number.isFinite(Number(account.followers_count)) ? Number(account.followers_count) : null,
        mediaCount: Number.isFinite(Number(account.media_count)) ? Number(account.media_count) : null,
        sourcePageId: String(page.id),
        sourcePageName: page.name ? String(page.name) : null,
        analyzeAccess: true,
        selectable: true,
        discoveryStatus: 'available',
        storyHistory: 'not_collected_without_webhooks'
      }
    });
  }
  return resources;
}

async function startMetaConnection({
  provider,
  userId,
  sessionId,
  workspaceId,
  returnPath = '/',
  targetConnectionId = null
}) {
  assertMetaProvider(provider);
  return withConnection(async connection => {
    const safeReturnPath = normalizeReturnPath(returnPath);
    const readiness = await requireMetaReady(connection, provider);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      let authorizationId = null;
      if (targetConnectionId) {
        const targetRows = await connection.query(
          `SELECT pauth.id AS authorization_id
           FROM workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
           WHERE wpc.id = ? AND wpc.workspace_id = ? AND wpc.provider = ?
           LIMIT 1 FOR UPDATE`,
          [targetConnectionId, workspaceId, provider]
        );
        if (!targetRows[0]) throw createHttpError(404, `${provider}_connection_not_found`);
        authorizationId = targetRows[0].authorization_id;
      } else {
        const existingRows = await connection.query(
          `SELECT id FROM provider_authorizations
           WHERE workspace_id = ? AND provider = ?
             AND status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
           ORDER BY FIELD(status, 'active', 'reconnect_required', 'authorizing', 'disabled'), updated_at DESC
           LIMIT 1 FOR UPDATE`,
          [workspaceId, provider]
        );
        authorizationId = existingRows[0] ? existingRows[0].id : null;
      }
      if (!authorizationId) {
        authorizationId = createId();
        await connection.query(
          `INSERT INTO provider_authorizations
            (id, workspace_id, provider, actor_user_id, status, auth_product, api_version)
           VALUES (?, ?, ?, ?, 'authorizing', 'analytics', ?)`,
          [authorizationId, workspaceId, provider, userId, META_GRAPH_API_VERSION]
        );
      } else {
        await connection.query(
          `UPDATE provider_authorizations
           SET actor_user_id = ?, status = 'authorizing', revoked_at = NULL,
               api_version = ?, updated_at = UTC_TIMESTAMP(3)
           WHERE id = ?`,
          [userId, META_GRAPH_API_VERSION, authorizationId]
        );
        await connection.query(
          `UPDATE workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           JOIN data_sources ds ON ds.id = wpc.data_source_id
           SET wpc.status = 'connecting', ds.status = 'connecting',
               ds.reconnect_reason = NULL, wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
          [authorizationId, provider]
        );
        await connection.query(
          `UPDATE sync_jobs sj
           JOIN workspace_provider_connections wpc ON wpc.data_source_id = sj.data_source_id
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           SET sj.status = 'paused', sj.lease_owner = NULL, sj.lease_expires_at = NULL,
               sj.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
          [authorizationId, provider]
        );
      }

      await connection.query(
        `UPDATE oauth_transactions
         SET status = 'failed', consumed_at = COALESCE(consumed_at, UTC_TIMESTAMP(3))
         WHERE provider_authorization_id = ? AND provider = ? AND status = 'pending'`,
        [authorizationId, provider]
      );
      const state = randomToken(32);
      await connection.query(
        `INSERT INTO oauth_transactions
          (id, state_hash, provider, workspace_id, initiated_by, session_id,
           provider_authorization_id, target_connection_id, return_path, requested_scopes,
           redirect_uri, provider_config_id, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
        [
          createId(),
          hashSecret(state),
          provider,
          workspaceId,
          userId,
          sessionId,
          authorizationId,
          targetConnectionId,
          safeReturnPath,
          JSON.stringify(META_REQUIRED_SCOPES[provider]),
          readiness.redirectUri,
          process.env[META_LOGIN_CONFIG_ENV[provider]],
          readiness.limits.oauthStateTtlSeconds
        ]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: `connection.${provider}.authorization_started`,
        targetType: 'provider_authorization',
        targetId: authorizationId,
        metadata: { requested_scopes: META_REQUIRED_SCOPES[provider] }
      });
      await connection.commit();
      return {
        authorization_url: meta.buildAuthorizationUrl(provider, { state }),
        expires_in_seconds: readiness.limits.oauthStateTtlSeconds
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function consumeOAuthTransaction(connection, { provider, state, sessionId, userId }) {
  if (!state) throw createHttpError(400, `${provider}_oauth_state_missing`);
  const rows = await connection.query(
    `SELECT oauth_transactions.*, expires_at <= UTC_TIMESTAMP(3) AS is_expired
     FROM oauth_transactions WHERE state_hash = ? LIMIT 1 FOR UPDATE`,
    [hashSecret(state)]
  );
  const transaction = rows[0] || null;
  if (!transaction) throw createHttpError(400, `${provider}_oauth_state_invalid`);
  if (transaction.provider !== provider) throw createHttpError(400, `${provider}_oauth_provider_mismatch`);
  if (transaction.consumed_at || transaction.status !== 'pending') {
    throw createHttpError(400, `${provider}_oauth_state_replayed`);
  }
  if (Number(transaction.is_expired) === 1) {
    await connection.query(
      `UPDATE oauth_transactions SET status = 'expired', consumed_at = UTC_TIMESTAMP(3) WHERE id = ?`,
      [transaction.id]
    );
    throw createHttpError(400, `${provider}_oauth_state_expired`);
  }
  if (transaction.session_id !== sessionId) throw createHttpError(403, `${provider}_oauth_session_mismatch`);
  if (transaction.initiated_by !== userId) throw createHttpError(403, `${provider}_oauth_user_mismatch`);
  await requireWorkspaceRole(connection, transaction.workspace_id, userId, 'manageConnection');
  const authRows = await connection.query(
    `SELECT workspace_id, provider, provider_subject
     FROM provider_authorizations WHERE id = ? LIMIT 1`,
    [transaction.provider_authorization_id]
  );
  const authorization = authRows[0] || null;
  if (!authorization || authorization.provider !== provider || authorization.workspace_id !== transaction.workspace_id) {
    throw createHttpError(400, `${provider}_oauth_authorization_mismatch`);
  }
  transaction.bound_provider_subject = authorization.provider_subject || null;
  if (transaction.target_connection_id) {
    const targetRows = await connection.query(
      `SELECT workspace_id, provider FROM workspace_provider_connections WHERE id = ? LIMIT 1`,
      [transaction.target_connection_id]
    );
    const target = targetRows[0] || null;
    if (!target || target.workspace_id !== transaction.workspace_id || target.provider !== provider) {
      throw createHttpError(400, `${provider}_oauth_workspace_mismatch`);
    }
  }
  const requested = parseJson(transaction.requested_scopes, []);
  const required = META_REQUIRED_SCOPES[provider];
  if (requested.length !== required.length || !required.every(scope => requested.includes(scope))) {
    throw createHttpError(400, `${provider}_oauth_scope_binding_mismatch`);
  }
  const configuration = getMetaConfiguration(provider);
  if (!configuration.connectable) {
    throw createHttpError(503, `${provider}_not_configured`, configuration.warnings);
  }
  if (transaction.redirect_uri !== configuration.redirectUri) {
    throw createHttpError(400, `${provider}_oauth_redirect_mismatch`);
  }
  if (transaction.provider_config_id !== process.env[META_LOGIN_CONFIG_ENV[provider]]) {
    throw createHttpError(400, `${provider}_oauth_config_mismatch`);
  }
  await connection.query(
    `UPDATE oauth_transactions SET status = 'consumed', consumed_at = UTC_TIMESTAMP(3) WHERE id = ?`,
    [transaction.id]
  );
  return transaction;
}

async function markTransactionFailed(transaction, outcomeCategory, grantedPermissions = null) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await connection.query(`UPDATE oauth_transactions SET status = 'failed' WHERE id = ?`, [transaction.id]);
      if (Array.isArray(grantedPermissions)) {
        await connection.query(
          `DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?`,
          [transaction.provider_authorization_id]
        );
        for (const permission of grantedPermissions) {
          await connection.query(
            `INSERT INTO provider_authorization_scopes
              (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
             VALUES (?, ?, 'granted', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
            [transaction.provider_authorization_id, String(permission)]
          );
        }
      }
      const reconnect = Boolean(transaction.target_connection_id);
      await connection.query(
        `UPDATE provider_authorizations SET status = ?, updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'disabled', transaction.provider_authorization_id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections wpc
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         JOIN data_sources ds ON ds.id = wpc.data_source_id
         SET wpc.status = 'reconnect_required', ds.status = 'reconnect_required',
             ds.reconnect_reason = ?, wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
        [
          `${transaction.provider}_authorization_${outcomeCategory}`.slice(0, 255),
          transaction.provider_authorization_id,
          transaction.provider
        ]
      );
      await connection.query(
        `UPDATE provider_capabilities pc
         JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         SET pc.status = 'not_granted', pc.reason = ?, pc.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
        [
          `${transaction.provider}_authorization_${outcomeCategory}`.slice(0, 255),
          transaction.provider_authorization_id,
          transaction.provider
        ]
      );
      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        action: `connection.${transaction.provider}.authorization_failed`,
        targetType: 'provider_authorization',
        targetId: transaction.provider_authorization_id,
        metadata: { outcome_category: outcomeCategory }
      });
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function recordAuthorizationRequest(transaction, details) {
  return withConnection(connection => connection.query(
    `INSERT INTO provider_request_events
      (id, workspace_id, provider_authorization_id, provider, request_category,
       method_name, quota_cost_estimate, item_count, attempts, status,
       failure_category, retry_after_seconds)
     VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)`,
    [
      createId(),
      transaction.workspace_id,
      transaction.provider_authorization_id,
      transaction.provider,
      details.category,
      details.method,
      details.itemCount === undefined ? null : details.itemCount,
      details.result && Number.isInteger(details.result.attempts) ? details.result.attempts : 1,
      details.status,
      details.result && details.result.error ? details.result.error.category : null,
      details.result ? details.result.retryAfterSeconds : null
    ]
  ));
}

async function discoverResources(transaction, accessToken) {
  const pages = [];
  const seenCursors = new Set();
  let after = null;
  for (let pageNumber = 1; pageNumber <= 10; pageNumber += 1) {
    const result = await meta.listManagedPages(accessToken, after, { maxRetries: 0 });
    const rows = result.body && Array.isArray(result.body.data) ? result.body.data : null;
    await recordAuthorizationRequest(transaction, {
      category: 'data_api',
      method: 'me.accounts.discovery',
      itemCount: rows ? rows.length : null,
      result,
      status: !result.ok || !rows ? 'failed' : rows.length > 0 ? 'success' : 'empty'
    });
    if (!result.ok || !rows) throw createHttpError(502, `${transaction.provider}_resource_discovery_failed`);
    pages.push(...rows);
    const cursor = result.body && result.body.paging && result.body.paging.cursors
      ? result.body.paging.cursors.after
      : null;
    if (!cursor || seenCursors.has(cursor)) break;
    seenCursors.add(cursor);
    after = cursor;
  }
  return normalizeDiscoveredResources(transaction.provider, pages);
}

async function saveAuthorizationResult(transaction, {
  accessToken,
  expiresAt,
  providerSubject,
  displayName,
  permissionRows,
  resources
}) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const access = encryptSecret(accessToken);
      await connection.query(
        `INSERT INTO provider_authorization_credentials
          (id, provider_authorization_id, access_token_ciphertext, access_token_iv,
           access_token_tag, key_version, token_type, access_expires_at)
         VALUES (?, ?, ?, ?, ?, ?, 'Bearer', ?)
         ON DUPLICATE KEY UPDATE
           access_token_ciphertext = VALUES(access_token_ciphertext),
           access_token_iv = VALUES(access_token_iv),
           access_token_tag = VALUES(access_token_tag),
           refresh_token_ciphertext = NULL, refresh_token_iv = NULL, refresh_token_tag = NULL,
           key_version = VALUES(key_version), token_type = 'Bearer',
           access_expires_at = VALUES(access_expires_at), refresh_expires_at = NULL,
           revoked_at = NULL, updated_at = UTC_TIMESTAMP(3)`,
        [
          createId(),
          transaction.provider_authorization_id,
          access.ciphertext,
          access.iv,
          access.tag,
          access.keyVersion,
          expiresAt
        ]
      );
      await connection.query(
        `DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?`,
        [transaction.provider_authorization_id]
      );
      for (const row of permissionRows) {
        const status = row.status === 'granted' ? 'granted' : row.status === 'declined' ? 'denied' : 'missing';
        await connection.query(
          `INSERT INTO provider_authorization_scopes
            (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
           VALUES (?, ?, ?, IF(? = 'granted', UTC_TIMESTAMP(3), NULL), UTC_TIMESTAMP(3))`,
          [transaction.provider_authorization_id, String(row.permission), status, status]
        );
      }

      const discoveredIds = new Set();
      for (const resource of resources) {
        const conflictingRows = await connection.query(
          `SELECT id, provider_authorization_id FROM provider_resources
           WHERE workspace_id = ? AND provider = ? AND resource_type = ? AND provider_resource_id = ?
           LIMIT 1`,
          [
            transaction.workspace_id,
            transaction.provider,
            resource.resourceType,
            resource.providerResourceId
          ]
        );
        const conflicting = conflictingRows[0] || null;
        if (conflicting && conflicting.provider_authorization_id !== transaction.provider_authorization_id) {
          throw createHttpError(409, `${transaction.provider}_resource_already_discovered`);
        }
        const resourceId = conflicting ? conflicting.id : createId();
        discoveredIds.add(resourceId);
        await connection.query(
          `INSERT INTO provider_resources
            (id, provider_authorization_id, workspace_id, provider, resource_type,
             provider_resource_id, display_name, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             display_name = VALUES(display_name), metadata = VALUES(metadata), updated_at = UTC_TIMESTAMP(3)`,
          [
            resourceId,
            transaction.provider_authorization_id,
            transaction.workspace_id,
            transaction.provider,
            resource.resourceType,
            resource.providerResourceId,
            resource.displayName,
            JSON.stringify(resource.metadata)
          ]
        );
        const resourceAccess = encryptSecret(resource.accessToken);
        await connection.query(
          `INSERT INTO provider_resource_credentials
            (id, provider_resource_id, access_token_ciphertext, access_token_iv,
             access_token_tag, key_version, token_type, access_expires_at)
           VALUES (?, ?, ?, ?, ?, ?, 'Bearer', ?)
           ON DUPLICATE KEY UPDATE
             access_token_ciphertext = VALUES(access_token_ciphertext),
             access_token_iv = VALUES(access_token_iv), access_token_tag = VALUES(access_token_tag),
             key_version = VALUES(key_version), token_type = 'Bearer',
             access_expires_at = VALUES(access_expires_at), revoked_at = NULL,
             updated_at = UTC_TIMESTAMP(3)`,
          [
            createId(),
            resourceId,
            resourceAccess.ciphertext,
            resourceAccess.iv,
            resourceAccess.tag,
            resourceAccess.keyVersion,
            expiresAt
          ]
        );
      }

      const existingRows = await connection.query(
        `SELECT pr.id, wpc.id AS connection_id, wpc.data_source_id
         FROM provider_resources pr
         LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
         WHERE pr.provider_authorization_id = ? AND pr.provider = ?`,
        [transaction.provider_authorization_id, transaction.provider]
      );
      let targetConnectionRestored = !transaction.target_connection_id;
      for (const existing of existingRows) {
        if (discoveredIds.has(existing.id)) {
          if (existing.connection_id === transaction.target_connection_id) targetConnectionRestored = true;
          continue;
        }
        if (!existing.connection_id) {
          await connection.query(`DELETE FROM provider_resources WHERE id = ?`, [existing.id]);
          continue;
        }
        await connection.query(
          `UPDATE provider_resources
           SET metadata = JSON_SET(COALESCE(metadata, JSON_OBJECT()),
             '$.selectable', FALSE, '$.discoveryStatus', 'not_returned'),
             updated_at = UTC_TIMESTAMP(3)
           WHERE id = ?`,
          [existing.id]
        );
        await connection.query(
          `UPDATE workspace_provider_connections wpc
           JOIN data_sources ds ON ds.id = wpc.data_source_id
           SET wpc.status = 'reconnect_required', ds.status = 'reconnect_required',
               ds.reconnect_reason = ?, wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
           WHERE wpc.id = ?`,
          [`${transaction.provider}_resource_no_longer_returned`, existing.connection_id]
        );
        await connection.query(
          `UPDATE sync_jobs SET status = 'paused', lease_owner = NULL, lease_expires_at = NULL,
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
          [existing.data_source_id]
        );
      }

      await connection.query(
        `UPDATE provider_authorizations
         SET provider_subject = ?, display_name = ?, status = 'active', granted_at = UTC_TIMESTAMP(3),
             last_validated_at = UTC_TIMESTAMP(3), revoked_at = NULL,
             deletion_due_at = DATE_ADD(?, INTERVAL 30 DAY), api_version = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [
          providerSubject,
          displayName,
          expiresAt,
          META_GRAPH_API_VERSION,
          transaction.provider_authorization_id
        ]
      );
      await connection.query(
        `UPDATE workspace_provider_connections wpc
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         JOIN data_sources ds ON ds.id = wpc.data_source_id
         SET wpc.status = 'active', ds.status = 'active', ds.reconnect_reason = NULL,
             wpc.next_sync_at = UTC_TIMESTAMP(3), ds.next_sync_at = UTC_TIMESTAMP(3),
             wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND pr.provider = ?
           AND JSON_UNQUOTE(JSON_EXTRACT(pr.metadata, '$.discoveryStatus')) = 'available'`,
        [transaction.provider_authorization_id, transaction.provider]
      );
      await connection.query(
        `UPDATE sync_jobs sj
         JOIN workspace_provider_connections wpc ON wpc.data_source_id = sj.data_source_id
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         SET sj.status = 'due', sj.run_after = UTC_TIMESTAMP(3), sj.lease_owner = NULL,
             sj.lease_expires_at = NULL, sj.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND pr.provider = ?
           AND wpc.status = 'active'`,
        [transaction.provider_authorization_id, transaction.provider]
      );
      await connection.query(
        `UPDATE provider_capabilities pc
         JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         SET pc.status = IF(wpc.status = 'active', 'available', 'not_granted'),
             pc.reason = IF(wpc.status = 'active', NULL, 'resource_not_returned'),
             pc.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND pr.provider = ?`,
        [transaction.provider_authorization_id, transaction.provider]
      );
      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        actorUserId: transaction.initiated_by,
        action: `connection.${transaction.provider}.authorized`,
        targetType: 'provider_authorization',
        targetId: transaction.provider_authorization_id,
        metadata: {
          discovered_resource_count: resources.length,
          granted_scope_count: permissionRows.filter(row => row.status === 'granted').length
        }
      });
      await connection.commit();
      return { discoveredResourceCount: resources.length, targetConnectionRestored };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function completeMetaConnection({ provider, code, state, providerError, sessionId, userId }) {
  assertMetaProvider(provider);
  let transaction;
  await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      transaction = await consumeOAuthTransaction(connection, { provider, state, sessionId, userId });
      await connection.commit();
    } catch (error) {
      if (error.code === `${provider}_oauth_state_expired`) await connection.commit();
      else await connection.rollback();
      throw error;
    }
  });
  if (providerError) {
    await markTransactionFailed(transaction, providerError === 'access_denied' ? 'user_denied' : 'provider_error');
    throw createHttpError(400, providerError === 'access_denied'
      ? `${provider}_authorization_denied`
      : `${provider}_authorization_failed`);
  }
  if (!code) {
    await markTransactionFailed(transaction, 'missing_code');
    throw createHttpError(400, `${provider}_authorization_code_missing`);
  }

  const exchange = await meta.exchangeCode(provider, code);
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'oauth.access_token', result: exchange,
    status: exchange.ok && exchange.body && exchange.body.access_token ? 'success' : 'failed'
  });
  const shortToken = exchange.body && exchange.body.access_token;
  if (!exchange.ok || !shortToken) {
    await markTransactionFailed(transaction, exchange.error ? exchange.error.category : 'malformed_response');
    throw createHttpError(502, `${provider}_token_exchange_failed`);
  }
  const longExchange = await meta.exchangeLongLivedToken(shortToken);
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'oauth.long_lived_token', result: longExchange,
    status: longExchange.ok && longExchange.body && longExchange.body.access_token ? 'success' : 'failed'
  });
  const tokenBody = longExchange.body || {};
  if (!longExchange.ok || !tokenBody.access_token || Number(tokenBody.expires_in) <= 0) {
    await meta.revokePermissions(shortToken);
    await markTransactionFailed(transaction, 'long_lived_token_required');
    throw createHttpError(502, `${provider}_long_lived_token_failed`);
  }
  const accessToken = tokenBody.access_token;

  const debug = await meta.debugToken(accessToken);
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'debug_token', result: debug,
    status: debug.ok && debug.body && debug.body.data && debug.body.data.is_valid ? 'success' : 'failed'
  });
  const debugData = debug.body && debug.body.data;
  if (
    !debug.ok || !debugData || debugData.is_valid !== true ||
    String(debugData.app_id) !== String(process.env.META_APP_ID) || !debugData.user_id
  ) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'token_validation_failed');
    throw createHttpError(400, `${provider}_token_validation_failed`);
  }
  if (
    transaction.bound_provider_subject &&
    String(transaction.bound_provider_subject) !== String(debugData.user_id)
  ) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'provider_subject_mismatch');
    throw createHttpError(409, `${provider}_provider_subject_mismatch`);
  }

  const permissions = await meta.getPermissions(accessToken);
  const permissionRows = permissions.body && Array.isArray(permissions.body.data) ? permissions.body.data : null;
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'me.permissions', itemCount: permissionRows ? permissionRows.length : null,
    result: permissions, status: permissions.ok && permissionRows ? 'success' : 'failed'
  });
  if (!permissions.ok || !permissionRows) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'permission_validation_failed');
    throw createHttpError(502, `${provider}_permission_validation_failed`);
  }
  const granted = permissionRows.filter(row => row.status === 'granted').map(row => row.permission);
  if (!meta.hasExactProductScopes(provider, granted) || meta.forbiddenScopes(granted).length > 0) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'missing_or_unapproved_scopes', granted);
    throw createHttpError(400, `${provider}_required_scopes_missing`);
  }

  const userResult = await meta.getUser(accessToken);
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'me.identity', result: userResult,
    status: userResult.ok && userResult.body && userResult.body.id ? 'success' : 'failed'
  });
  if (!userResult.ok || !userResult.body || String(userResult.body.id) !== String(debugData.user_id)) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'subject_validation_failed');
    throw createHttpError(502, `${provider}_subject_validation_failed`);
  }

  let resources;
  try {
    resources = await discoverResources(transaction, accessToken);
  } catch (error) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'resource_discovery_failed');
    throw error;
  }
  const exchangeExpiryMs = Date.now() + Number(tokenBody.expires_in) * 1000;
  const expiryCandidates = [
    Number(debugData.expires_at) * 1000,
    Number(debugData.data_access_expires_at) * 1000,
    exchangeExpiryMs
  ].filter(value => Number.isFinite(value) && value > 0);
  const expiresAt = new Date(Math.min(...expiryCandidates));
  if (expiresAt.getTime() <= Date.now()) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, 'token_expired');
    throw createHttpError(400, `${provider}_token_validation_failed`);
  }
  try {
    const saved = await saveAuthorizationResult(transaction, {
      accessToken,
      expiresAt,
      providerSubject: String(debugData.user_id),
      displayName: String(userResult.body.name || 'Meta user').slice(0, 255),
      permissionRows,
      resources
    });
    return {
      return_path: transaction.return_path,
      outcome: transaction.target_connection_id && !saved.targetConnectionRestored
        ? 'selected_resource_unavailable'
        : resources.length === 0
          ? 'no_resources'
          : transaction.target_connection_id ? 'reconnected' : 'selection_required',
      discovered_resource_count: saved.discoveredResourceCount
    };
  } catch (error) {
    await meta.revokePermissions(accessToken);
    await markTransactionFailed(transaction, error.code || 'storage_failed');
    throw error;
  }
}

async function selectMetaResource(userId, workspaceId, provider, resourceId) {
  assertMetaProvider(provider);
  if (!resourceId) throw createHttpError(400, `${provider}_resource_required`);
  return withConnection(async connection => {
    await requireMetaReady(connection, provider);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      const rows = await connection.query(
        `SELECT pr.*, pauth.status AS authorization_status, pauth.id AS authorization_id,
                prc.revoked_at AS resource_token_revoked_at, prc.access_expires_at
         FROM provider_resources pr
         JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
         LEFT JOIN provider_resource_credentials prc ON prc.provider_resource_id = pr.id
         WHERE pr.id = ? AND pr.workspace_id = ? AND pr.provider = ?
         LIMIT 1 FOR UPDATE`,
        [resourceId, workspaceId, provider]
      );
      const resource = rows[0] || null;
      if (!resource) throw createHttpError(404, `${provider}_resource_not_found`);
      if (resource.authorization_status !== 'active') throw createHttpError(409, `${provider}_authorization_not_active`);
      if (resource.resource_token_revoked_at || !resource.access_expires_at || new Date(resource.access_expires_at) <= new Date()) {
        throw createHttpError(409, `${provider}_resource_token_unavailable`);
      }
      const metadata = parseJson(resource.metadata, {});
      if (metadata.selectable === false) throw createHttpError(409, `${provider}_resource_not_available`);
      const scopeRows = await connection.query(
        `SELECT scope FROM provider_authorization_scopes
         WHERE provider_authorization_id = ? AND status = 'granted'`,
        [resource.authorization_id]
      );
      if (!meta.hasExactProductScopes(provider, scopeRows.map(row => row.scope))) {
        throw createHttpError(409, `${provider}_required_scopes_missing`);
      }
      const existingRows = await connection.query(
        `SELECT id FROM workspace_provider_connections
         WHERE workspace_id = ? AND provider_resource_id = ? LIMIT 1`,
        [workspaceId, resourceId]
      );
      if (existingRows[0]) throw createHttpError(409, `${provider}_resource_already_connected`);

      const dataSourceId = createId();
      const workspaceConnectionId = createId();
      await connection.query(
        `INSERT INTO data_sources (id, workspace_id, provider, status, next_sync_at)
         VALUES (?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
        [dataSourceId, workspaceId, provider]
      );
      await connection.query(
        `INSERT INTO provider_accounts
          (id, workspace_id, data_source_id, provider, provider_account_id, username, display_name, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          createId(), workspaceId, dataSourceId, provider, resource.provider_resource_id,
          metadata.username || null, resource.display_name, JSON.stringify(metadata)
        ]
      );
      await connection.query(
        `INSERT INTO workspace_provider_connections
          (id, workspace_id, provider_resource_id, data_source_id, provider, status, next_sync_at)
         VALUES (?, ?, ?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
        [workspaceConnectionId, workspaceId, resourceId, dataSourceId, provider]
      );
      for (const capability of META_CAPABILITIES[provider]) {
        await connection.query(
          `INSERT INTO provider_capabilities
            (id, workspace_provider_connection_id, capability_key, status)
           VALUES (?, ?, ?, 'available')`,
          [createId(), workspaceConnectionId, capability]
        );
      }
      await connection.query(
        `INSERT INTO provider_sync_states
          (id, workspace_provider_connection_id, sync_key, cursor_state, api_version)
         VALUES (?, ?, ?, JSON_OBJECT(), ?)`,
        [createId(), workspaceConnectionId, `${provider}.sync`, META_GRAPH_API_VERSION]
      );
      await connection.query(
        `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
         VALUES (?, ?, UTC_TIMESTAMP(3), 'due')`,
        [createId(), dataSourceId]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: `connection.${provider}.resource_selected`,
        targetType: 'workspace_provider_connection',
        targetId: workspaceConnectionId,
        metadata: { provider_resource_id: resource.provider_resource_id }
      });
      await connection.commit();
      return {
        connection: {
          id: workspaceConnectionId,
          data_source_id: dataSourceId,
          status: 'active',
          account: {
            id: resource.provider_resource_id,
            username: metadata.username || null,
            display_name: resource.display_name,
            thumbnail_url: metadata.thumbnailUrl || null
          }
        }
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function purgeMetaResource(connection, record, actorUserId, outcomeCategory) {
  await connection.query(
    `INSERT INTO provider_revocation_events
      (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id,
       provider, status, failure_category)
     VALUES (?, ?, ?, ?, ?, 'local_revoked', ?)`,
    [
      createId(), record.authorization_id, record.connection_id, actorUserId,
      record.provider, outcomeCategory
    ]
  );
  await connection.query(
    `DELETE FROM provider_request_events
     WHERE workspace_provider_connection_id = ? AND provider = ?`,
    [record.connection_id, record.provider]
  );
  await connection.query(`DELETE FROM workspace_provider_connections WHERE id = ?`, [record.connection_id]);
  if (record.data_source_id) await connection.query(`DELETE FROM data_sources WHERE id = ?`, [record.data_source_id]);
  await connection.query(`DELETE FROM provider_resources WHERE id = ?`, [record.resource_id]);
  await writeAuditLog(connection, {
    workspaceId: record.workspace_id,
    actorUserId,
    action: `connection.${record.provider}.resource_disconnected`,
    targetType: 'provider_authorization',
    targetId: record.authorization_id,
    metadata: { outcome_category: outcomeCategory }
  });
}

async function purgeMetaAuthorization(connection, authorizationId, outcomeCategory, actorUserId = null) {
  const authRows = await connection.query(
    `SELECT workspace_id, provider FROM provider_authorizations
     WHERE id = ? AND provider IN ('facebook_pages', 'instagram') LIMIT 1 FOR UPDATE`,
    [authorizationId]
  );
  const authorization = authRows[0] || null;
  if (!authorization) return null;
  const resourceRows = await connection.query(
    `SELECT pr.id AS resource_id, wpc.id AS connection_id, wpc.data_source_id
     FROM provider_resources pr
     LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
     WHERE pr.provider_authorization_id = ?`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM provider_request_events WHERE provider_authorization_id = ?`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM oauth_transactions WHERE provider_authorization_id = ?`,
    [authorizationId]
  );
  await connection.query(
    `INSERT INTO provider_revocation_events
      (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id,
       provider, status, failure_category)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(), authorizationId, resourceRows.find(row => row.connection_id)?.connection_id || null,
      actorUserId, authorization.provider,
      outcomeCategory === 'provider_revoked' ? 'provider_revoked' : 'local_revoked',
      outcomeCategory
    ]
  );
  await connection.query(
    `DELETE wpc FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ?`,
    [authorizationId]
  );
  for (const resource of resourceRows) {
    if (resource.data_source_id) await connection.query(`DELETE FROM data_sources WHERE id = ?`, [resource.data_source_id]);
  }
  await connection.query(`DELETE FROM provider_resources WHERE provider_authorization_id = ?`, [authorizationId]);
  await connection.query(
    `DELETE FROM provider_authorization_credentials WHERE provider_authorization_id = ?`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?`,
    [authorizationId]
  );
  await connection.query(
    `UPDATE provider_authorizations
     SET actor_user_id = NULL, provider_subject = NULL, display_name = NULL,
         status = 'revoked', revoked_at = UTC_TIMESTAMP(3), deletion_due_at = NULL,
         updated_at = UTC_TIMESTAMP(3)
     WHERE id = ?`,
    [authorizationId]
  );
  await writeAuditLog(connection, {
    workspaceId: authorization.workspace_id,
    actorUserId,
    action: `connection.${authorization.provider}.revoked_and_purged`,
    targetType: 'provider_authorization',
    targetId: authorizationId,
    metadata: { outcome_category: outcomeCategory }
  });
  return { workspaceId: authorization.workspace_id, deletedSourceCount: resourceRows.filter(row => row.data_source_id).length };
}

async function purgeMetaSubjectAuthorizations(connection, {
  providerSubject,
  primaryAuthorizationId,
  outcomeCategory,
  actorUserId
}) {
  const authorizationIds = new Set([primaryAuthorizationId]);
  if (providerSubject) {
    const siblingRows = await connection.query(
      `SELECT id FROM provider_authorizations
       WHERE provider_subject = ? AND provider IN ('facebook_pages', 'instagram')
         AND status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
       ORDER BY created_at ASC FOR UPDATE`,
      [providerSubject]
    );
    for (const sibling of siblingRows) authorizationIds.add(sibling.id);
  }

  let deletedSourceCount = 0;
  for (const authorizationId of authorizationIds) {
    const result = await purgeMetaAuthorization(
      connection,
      authorizationId,
      outcomeCategory,
      authorizationId === primaryAuthorizationId ? actorUserId : null
    );
    if (result) deletedSourceCount += result.deletedSourceCount;
  }
  return { deletedSourceCount };
}

async function purgeOverdueMetaAuthorizations(limit = 50) {
  return withConnection(async connection => {
    if (!(await metaFoundationReady(connection))) return { purged: 0 };
    const boundedLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const rows = await connection.query(
      `SELECT id FROM provider_authorizations
       WHERE provider IN ('facebook_pages', 'instagram')
         AND status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
         AND deletion_due_at IS NOT NULL
         AND deletion_due_at <= UTC_TIMESTAMP(3)
       ORDER BY deletion_due_at ASC
       LIMIT ?`,
      [boundedLimit]
    );
    let purged = 0;
    for (const row of rows) {
      await connection.beginTransaction();
      try {
        const result = await purgeMetaAuthorization(
          connection,
          row.id,
          'meta_authorization_expired_retention_elapsed'
        );
        await connection.commit();
        if (result) purged += 1;
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    }
    return { purged };
  });
}

async function disconnectMeta(userId, workspaceId, provider, connectionId = null) {
  assertMetaProvider(provider);
  const record = await withConnection(async connection => {
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    const params = connectionId ? [workspaceId, provider, connectionId] : [workspaceId, provider];
    const connectionClause = connectionId ? 'AND wpc.id = ?' : '';
    const rows = await connection.query(
      `SELECT pauth.id AS authorization_id, pauth.workspace_id, pauth.provider, pauth.provider_subject,
              pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag, pac.key_version,
              wpc.id AS connection_id, wpc.data_source_id, pr.id AS resource_id,
              (SELECT COUNT(*) FROM workspace_provider_connections all_wpc
               JOIN provider_resources all_pr ON all_pr.id = all_wpc.provider_resource_id
               WHERE all_pr.provider_authorization_id = pauth.id) AS connection_count,
              (SELECT COUNT(*) FROM provider_authorizations sibling_auth
               JOIN provider_resources sibling_pr
                 ON sibling_pr.provider_authorization_id = sibling_auth.id
               JOIN workspace_provider_connections sibling_wpc
                 ON sibling_wpc.provider_resource_id = sibling_pr.id
               WHERE sibling_auth.id <> pauth.id
                 AND sibling_auth.provider_subject = pauth.provider_subject
                 AND sibling_auth.provider IN ('facebook_pages', 'instagram')
                 AND sibling_auth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled'))
                AS sibling_meta_connection_count
       FROM provider_authorizations pauth
       LEFT JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
       LEFT JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
       LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
       WHERE pauth.workspace_id = ? AND pauth.provider = ? ${connectionClause}
         AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
       ORDER BY pauth.updated_at DESC LIMIT 1`,
      params
    );
    return rows[0] || null;
  });
  if (!record) throw createHttpError(404, `${provider}_connection_not_found`);

  if (connectionId && Number(record.connection_count) > 1) {
    const local = await withConnection(async connection => {
      await connection.beginTransaction();
      try {
        await purgeMetaResource(connection, record, userId, 'resource_disconnected_grant_preserved');
        await connection.commit();
        return { deletedSourceCount: record.data_source_id ? 1 : 0 };
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
    return {
      disconnected: true,
      local_data_deleted: true,
      authorization_preserved: true,
      provider_grant_preserved: true,
      provider_revoke: { attempted: false, success: false, status: null, outcome_category: 'shared_grant_preserved' },
      deleted_source_count: local.deletedSourceCount
    };
  }

  if (record.provider_subject && Number(record.sibling_meta_connection_count) > 0) {
    const local = await withConnection(async connection => {
      await connection.beginTransaction();
      try {
        const result = await purgeMetaAuthorization(
          connection,
          record.authorization_id,
          'sibling_meta_grant_preserved',
          userId
        );
        await connection.commit();
        return result;
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
    return {
      disconnected: true,
      local_data_deleted: true,
      authorization_preserved: false,
      provider_grant_preserved: true,
      provider_revoke: {
        attempted: false,
        success: false,
        status: null,
        outcome_category: 'sibling_meta_grant_preserved'
      },
      deleted_source_count: local ? local.deletedSourceCount : 0
    };
  }

  let token = null;
  try {
    if (record.access_token_ciphertext) {
      token = decryptSecret({
        ciphertext: record.access_token_ciphertext,
        iv: record.access_token_iv,
        tag: record.access_token_tag,
        keyVersion: record.key_version
      });
    }
  } catch {
    token = null;
  }
  const providerRevoke = token
    ? await meta.revokePermissions(token)
    : { attempted: false, success: false, status: null, error: { category: 'credential_unavailable' } };
  const outcomeCategory = providerRevoke.success ? 'provider_revoked' : 'provider_revoke_failed_local_purge';
  const local = await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const result = await purgeMetaSubjectAuthorizations(connection, {
        providerSubject: record.provider_subject,
        primaryAuthorizationId: record.authorization_id,
        outcomeCategory,
        actorUserId: userId
      });
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
  return {
    disconnected: true,
    local_data_deleted: true,
    authorization_preserved: false,
    provider_grant_preserved: false,
    provider_revoke: {
      attempted: providerRevoke.attempted,
      success: providerRevoke.success,
      status: providerRevoke.status,
      outcome_category: outcomeCategory
    },
    deleted_source_count: local ? local.deletedSourceCount : 0
  };
}

async function processMetaSignedCallback(callbackType, signedRequest) {
  if (!['data_deletion', 'deauthorization'].includes(callbackType)) {
    throw createHttpError(400, 'meta_callback_type_invalid');
  }
  let payload;
  try {
    payload = meta.verifySignedRequest(signedRequest);
  } catch (error) {
    throw createHttpError(400, error.message);
  }
  const signedRequestHash = hashSecret(signedRequest);
  const subjectHash = hashSecret(payload.user_id);
  const confirmationCode = callbackType === 'data_deletion' ? randomToken(24) : null;
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const existingRows = await connection.query(
        `SELECT * FROM meta_callback_events WHERE signed_request_hash = ? LIMIT 1 FOR UPDATE`,
        [signedRequestHash]
      );
      if (existingRows[0]) {
        await connection.commit();
        return {
          status: existingRows[0].status,
          confirmation_code: existingRows[0].confirmation_code,
          authorization_count: Number(existingRows[0].authorization_count || 0)
        };
      }
      const eventId = createId();
      await connection.query(
        `INSERT INTO meta_callback_events
          (id, callback_type, signed_request_hash, provider_subject_hash,
           confirmation_code, status)
         VALUES (?, ?, ?, ?, ?, 'processing')`,
        [eventId, callbackType, signedRequestHash, subjectHash, confirmationCode]
      );
      const authorizationRows = await connection.query(
        `SELECT id FROM provider_authorizations
         WHERE provider IN ('facebook_pages', 'instagram') AND provider_subject = ?
           AND status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
         ORDER BY created_at ASC FOR UPDATE`,
        [String(payload.user_id)]
      );
      for (const authorization of authorizationRows) {
        await purgeMetaAuthorization(
          connection,
          authorization.id,
          callbackType === 'data_deletion' ? 'meta_data_deletion_callback' : 'meta_deauthorization_callback'
        );
      }
      await connection.query(
        `UPDATE meta_callback_events
         SET status = 'completed', authorization_count = ?, completed_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [authorizationRows.length, eventId]
      );
      await connection.commit();
      return {
        status: 'completed',
        confirmation_code: confirmationCode,
        authorization_count: authorizationRows.length
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function getMetaDeletionStatus(confirmationCode) {
  if (!confirmationCode) throw createHttpError(404, 'meta_deletion_status_not_found');
  return withConnection(async connection => {
    const rows = await connection.query(
      `SELECT status, authorization_count, completed_at, created_at
       FROM meta_callback_events
       WHERE callback_type = 'data_deletion' AND confirmation_code = ? LIMIT 1`,
      [confirmationCode]
    );
    const row = rows[0] || null;
    if (!row) throw createHttpError(404, 'meta_deletion_status_not_found');
    return {
      status: row.status,
      authorization_count: Number(row.authorization_count || 0),
      completed_at: row.completed_at ? new Date(row.completed_at).toISOString() : null,
      requested_at: new Date(row.created_at).toISOString()
    };
  });
}

module.exports = {
  META_CAPABILITIES,
  META_PROVIDERS,
  completeMetaConnection,
  disconnectMeta,
  getMetaDeletionStatus,
  metaFoundationReady,
  normalizeDiscoveredResources,
  processMetaSignedCallback,
  purgeOverdueMetaAuthorizations,
  purgeMetaAuthorization,
  requireMetaReady,
  selectMetaResource,
  startMetaConnection
};
