const { getConnection } = require('../database');
const ga4 = require('../integrations/google-analytics');
const { normalizeReturnPath } = require('./connection-service');
const { assertCapability } = require('./rbac');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId, hashSecret, randomToken } = require('./security');
const { getGoogleAnalyticsConfiguration } = require('./google-analytics-config');

const GA4_PROVIDER = 'google_analytics_4';
const GA4_CAPABILITIES = Object.freeze([
  'resource_discovery',
  'property_metadata',
  'traffic_metrics',
  'dimension_breakdowns',
  'compatibility_checks',
  'disconnect'
]);

function createHttpError(status, code, details = null) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  error.details = details;
  return error;
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
  if (!rows[0]) throw createHttpError(404, 'workspace_not_found');
  assertCapability(rows[0].role, capability);
  return rows[0].role;
}

async function googleAnalyticsFoundationReady(connection) {
  const rows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN (
         'provider_authorizations',
         'provider_authorization_credentials',
         'provider_authorization_scopes',
         'provider_resources',
         'workspace_provider_connections',
         'provider_resource_observations',
         'provider_metric_observations',
         'provider_dimension_observations',
         'provider_request_events'
       )`
  );
  return Number(rows[0] && rows[0].count) === 9;
}

async function requireGoogleAnalyticsReady(connection, env = process.env) {
  const foundationReady = await googleAnalyticsFoundationReady(connection);
  const status = getGoogleAnalyticsConfiguration(env, {
    databaseReady: true,
    foundationReady,
    workerReady: true
  });
  if (!status.connectable) throw createHttpError(503, 'ga4_not_configured', status.warnings);
  return status;
}

async function writeAuditLog(connection, details) {
  await connection.query(
    `INSERT INTO audit_logs
      (id, workspace_id, actor_user_id, action, target_type, target_id, metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      details.workspaceId,
      details.actorUserId || null,
      details.action,
      details.targetType || null,
      details.targetId || null,
      details.metadata ? JSON.stringify(details.metadata) : null
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

function normalizeProperty(accountSummary, propertySummary, propertyBody = null) {
  const name = String((propertyBody && propertyBody.name) || propertySummary.property || '');
  if (!/^properties\/\d+$/.test(name)) throw createHttpError(502, 'ga4_property_response_malformed');
  const propertyId = name.split('/')[1];
  const account = String((propertyBody && propertyBody.account) || accountSummary.account || propertySummary.parent || '');
  const timezone = propertyBody && typeof propertyBody.timeZone === 'string' ? propertyBody.timeZone.trim() : '';
  const currency = propertyBody && typeof propertyBody.currencyCode === 'string'
    ? propertyBody.currencyCode.trim().toUpperCase()
    : '';
  const displayName = String(
    (propertyBody && propertyBody.displayName) || propertySummary.displayName || `GA4 property ${propertyId}`
  ).slice(0, 255);
  const selectable = Boolean(timezone && /^[A-Z]{3}$/.test(currency));
  return {
    id: name,
    propertyId,
    displayName,
    account,
    accountDisplayName: String(accountSummary.displayName || 'Google Analytics account').slice(0, 255),
    propertyType: String((propertyBody && propertyBody.propertyType) || propertySummary.propertyType || 'PROPERTY_TYPE_UNSPECIFIED'),
    serviceLevel: propertyBody && propertyBody.serviceLevel ? String(propertyBody.serviceLevel) : null,
    timezone: timezone || null,
    currency: /^[A-Z]{3}$/.test(currency) ? currency : null,
    selectable,
    discoveryStatus: selectable ? 'available' : 'property_details_unavailable'
  };
}

async function startGoogleAnalyticsConnection({ userId, sessionId, workspaceId, returnPath = '/', targetConnectionId = null }) {
  return withConnection(async connection => {
    const readiness = await requireGoogleAnalyticsReady(connection);
    const safeReturnPath = normalizeReturnPath(returnPath);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      let authorizationId;
      if (targetConnectionId) {
        const rows = await connection.query(
          `SELECT pauth.id AS authorization_id
           FROM workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
           WHERE wpc.id = ? AND wpc.workspace_id = ? AND wpc.provider = ?
           LIMIT 1 FOR UPDATE`,
          [targetConnectionId, workspaceId, GA4_PROVIDER]
        );
        if (!rows[0]) throw createHttpError(404, 'ga4_connection_not_found');
        authorizationId = rows[0].authorization_id;
        await connection.query(
          `UPDATE provider_authorizations
           SET status = 'authorizing', actor_user_id = ?, updated_at = UTC_TIMESTAMP(3)
           WHERE id = ?`,
          [userId, authorizationId]
        );
        await connection.query(
          `UPDATE sync_jobs sj
           JOIN workspace_provider_connections wpc ON wpc.data_source_id = sj.data_source_id
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           SET sj.status = 'paused', sj.lease_owner = NULL, sj.lease_expires_at = NULL,
               sj.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
          [authorizationId, GA4_PROVIDER]
        );
      } else {
        const rows = await connection.query(
          `SELECT pauth.id,
                  EXISTS(SELECT 1 FROM provider_resources pr WHERE pr.provider_authorization_id = pauth.id) AS has_resources
           FROM provider_authorizations pauth
           WHERE pauth.workspace_id = ? AND pauth.provider = ?
             AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
           ORDER BY FIELD(pauth.status, 'active', 'reconnect_required', 'authorizing', 'disabled'), pauth.updated_at DESC
           LIMIT 1 FOR UPDATE`,
          [workspaceId, GA4_PROVIDER]
        );
        const existing = rows[0] || null;
        if (existing && Number(existing.has_resources) === 1) {
          throw createHttpError(409, 'ga4_authorization_already_exists');
        }
        if (existing) {
          authorizationId = existing.id;
          await connection.query(
            `UPDATE provider_authorizations
             SET actor_user_id = ?, status = 'authorizing', revoked_at = NULL, updated_at = UTC_TIMESTAMP(3)
             WHERE id = ?`,
            [userId, authorizationId]
          );
        } else {
          authorizationId = createId();
          await connection.query(
            `INSERT INTO provider_authorizations
              (id, workspace_id, provider, actor_user_id, status, auth_product, api_version)
             VALUES (?, ?, ?, ?, 'authorizing', 'analytics', 'admin-v1beta/data-v1beta')`,
            [authorizationId, workspaceId, GA4_PROVIDER, userId]
          );
        }
      }

      await connection.query(
        `UPDATE oauth_transactions
         SET status = 'failed', consumed_at = COALESCE(consumed_at, UTC_TIMESTAMP(3)),
             pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
             pkce_verifier_tag = NULL, pkce_key_version = NULL
         WHERE provider_authorization_id = ? AND provider = ? AND status = 'pending'`,
        [authorizationId, GA4_PROVIDER]
      );
      const state = randomToken(32);
      const pkce = ga4.createPkcePair();
      const verifier = encryptSecret(pkce.verifier);
      const credentialRows = await connection.query(
        `SELECT refresh_token_ciphertext
         FROM provider_authorization_credentials
         WHERE provider_authorization_id = ? AND revoked_at IS NULL
           AND (refresh_expires_at IS NULL OR refresh_expires_at > UTC_TIMESTAMP(3))
         LIMIT 1`,
        [authorizationId]
      );
      const promptConsent = !credentialRows[0] || !credentialRows[0].refresh_token_ciphertext;
      await connection.query(
        `INSERT INTO oauth_transactions
          (id, state_hash, provider, workspace_id, initiated_by, session_id,
           provider_authorization_id, target_connection_id, return_path, requested_scopes,
           redirect_uri, pkce_verifier_ciphertext, pkce_verifier_iv, pkce_verifier_tag,
           pkce_key_version, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
        [
          createId(), hashSecret(state), GA4_PROVIDER, workspaceId, userId, sessionId,
          authorizationId, targetConnectionId, safeReturnPath, JSON.stringify(ga4.GA4_SCOPES),
          readiness.redirectUri, verifier.ciphertext, verifier.iv, verifier.tag, verifier.keyVersion,
          readiness.limits.oauthStateTtlSeconds
        ]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: targetConnectionId ? 'connection.ga4.reauthorize_start' : 'connection.ga4.start',
        targetType: 'provider_authorization',
        targetId: authorizationId,
        metadata: { requested_scope_count: ga4.GA4_SCOPES.length }
      });
      await connection.commit();
      return {
        authorization_url: ga4.buildAuthorizationUrl({
          state,
          codeChallenge: pkce.challenge,
          promptConsent
        }),
        expires_in_seconds: readiness.limits.oauthStateTtlSeconds
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function consumeOAuthTransaction(connection, { state, sessionId, userId }) {
  if (!state) throw createHttpError(400, 'ga4_oauth_state_missing');
  const rows = await connection.query(
    `SELECT oauth_transactions.*, expires_at <= UTC_TIMESTAMP(3) AS is_expired
     FROM oauth_transactions WHERE state_hash = ? LIMIT 1 FOR UPDATE`,
    [hashSecret(state)]
  );
  const transaction = rows[0] || null;
  if (!transaction) throw createHttpError(400, 'ga4_oauth_state_invalid');
  if (transaction.provider !== GA4_PROVIDER) throw createHttpError(400, 'ga4_oauth_provider_mismatch');
  if (transaction.consumed_at || transaction.status !== 'pending') throw createHttpError(400, 'ga4_oauth_state_replayed');
  if (Number(transaction.is_expired) === 1) {
    await connection.query(
      `UPDATE oauth_transactions
       SET status = 'expired', consumed_at = UTC_TIMESTAMP(3),
           pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
           pkce_verifier_tag = NULL, pkce_key_version = NULL
       WHERE id = ?`,
      [transaction.id]
    );
    throw createHttpError(400, 'ga4_oauth_state_expired');
  }
  if (transaction.session_id !== sessionId) throw createHttpError(403, 'ga4_oauth_session_mismatch');
  if (transaction.initiated_by !== userId) throw createHttpError(403, 'ga4_oauth_user_mismatch');
  const authRows = await connection.query(
    'SELECT workspace_id, provider FROM provider_authorizations WHERE id = ? LIMIT 1',
    [transaction.provider_authorization_id]
  );
  const authorization = authRows[0] || null;
  if (!authorization || authorization.provider !== GA4_PROVIDER) {
    throw createHttpError(400, 'ga4_oauth_authorization_mismatch');
  }
  if (authorization.workspace_id !== transaction.workspace_id) throw createHttpError(400, 'ga4_oauth_workspace_mismatch');
  await requireWorkspaceRole(connection, transaction.workspace_id, userId, 'manageConnection');
  if (transaction.target_connection_id) {
    const targetRows = await connection.query(
      'SELECT workspace_id, provider FROM workspace_provider_connections WHERE id = ? LIMIT 1',
      [transaction.target_connection_id]
    );
    const target = targetRows[0] || null;
    if (!target || target.workspace_id !== transaction.workspace_id || target.provider !== GA4_PROVIDER) {
      throw createHttpError(400, 'ga4_oauth_workspace_mismatch');
    }
  }
  const requestedScopes = parseJson(transaction.requested_scopes, []);
  if (!ga4.hasExactScopes(requestedScopes)) throw createHttpError(400, 'ga4_oauth_scope_binding_mismatch');
  if (transaction.redirect_uri !== process.env.GA4_REDIRECT_URI) {
    throw createHttpError(400, 'ga4_oauth_redirect_mismatch');
  }
  await connection.query(
    `UPDATE oauth_transactions
     SET status = 'consumed', consumed_at = UTC_TIMESTAMP(3),
         pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
         pkce_verifier_tag = NULL, pkce_key_version = NULL
     WHERE id = ?`,
    [transaction.id]
  );
  return transaction;
}

async function markAuthorizationFailed(transaction, outcome, grantedScopes = null) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await connection.query(
        `UPDATE oauth_transactions
         SET status = 'failed', pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
             pkce_verifier_tag = NULL, pkce_key_version = NULL WHERE id = ?`,
        [transaction.id]
      );
      if (Array.isArray(grantedScopes)) {
        await connection.query(
          'DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?',
          [transaction.provider_authorization_id]
        );
        for (const scope of grantedScopes) {
          await connection.query(
            `INSERT INTO provider_authorization_scopes
              (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
             VALUES (?, ?, 'granted', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
            [transaction.provider_authorization_id, String(scope)]
          );
        }
      }
      const nextStatus = transaction.target_connection_id ? 'reconnect_required' : 'disabled';
      await connection.query(
        'UPDATE provider_authorizations SET status = ?, updated_at = UTC_TIMESTAMP(3) WHERE id = ?',
        [nextStatus, transaction.provider_authorization_id]
      );
      if (transaction.target_connection_id) {
        await connection.query(
          `UPDATE workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           JOIN data_sources ds ON ds.id = wpc.data_source_id
           SET wpc.status = 'reconnect_required', ds.status = 'reconnect_required',
               ds.reconnect_reason = ?, wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
          [`ga4_authorization_${outcome}`.slice(0, 255), transaction.provider_authorization_id, GA4_PROVIDER]
        );
      }
      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        action: 'connection.ga4.authorization_failed',
        targetType: 'provider_authorization',
        targetId: transaction.provider_authorization_id,
        metadata: { outcome_category: outcome }
      });
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function loadExistingRefreshToken(authorizationId) {
  return withConnection(async connection => {
    const rows = await connection.query(
      `SELECT refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version
       FROM provider_authorization_credentials
       WHERE provider_authorization_id = ? AND revoked_at IS NULL
         AND (refresh_expires_at IS NULL OR refresh_expires_at > UTC_TIMESTAMP(3))
       LIMIT 1`,
      [authorizationId]
    );
    const record = rows[0] || null;
    if (!record || !record.refresh_token_ciphertext) return null;
    return decryptSecret({
      ciphertext: record.refresh_token_ciphertext,
      iv: record.refresh_token_iv,
      tag: record.refresh_token_tag,
      keyVersion: record.key_version
    });
  });
}

async function recordAuthorizationRequest(transaction, details) {
  return withConnection(connection => connection.query(
    `INSERT INTO provider_request_events
      (id, workspace_id, provider_authorization_id, provider, request_category,
       method_name, quota_cost_estimate, page_number, item_count, attempts,
       status, failure_category, retry_after_seconds)
     VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)`,
    [
      createId(), transaction.workspace_id, transaction.provider_authorization_id, GA4_PROVIDER,
      details.category, details.method, details.pageNumber || null,
      details.itemCount === undefined ? null : details.itemCount,
      details.result && Number.isInteger(details.result.attempts) ? details.result.attempts : 1,
      details.status,
      details.result && details.result.error ? details.result.error.category : null,
      details.result ? details.result.retryAfterSeconds : null
    ]
  ));
}

async function discoverProperties(transaction, accessToken, limits) {
  const summaries = [];
  let pageToken = null;
  for (let page = 1; page <= limits.maxDiscoveryPages; page += 1) {
    const result = await ga4.listAccountSummaries(accessToken, pageToken, { maxRetries: 0 });
    const items = result.body && Array.isArray(result.body.accountSummaries)
      ? result.body.accountSummaries
      : null;
    await recordAuthorizationRequest(transaction, {
      category: 'data_api',
      method: 'accountSummaries.list',
      pageNumber: page,
      itemCount: items ? items.length : null,
      result,
      status: !result.ok || !items ? 'failed' : items.length > 0 ? 'success' : 'empty'
    });
    if (!result.ok || !items) throw createHttpError(502, 'ga4_property_discovery_failed');
    summaries.push(...items);
    const next = result.body.nextPageToken ? String(result.body.nextPageToken) : null;
    if (!next) break;
    if (next === pageToken || page === limits.maxDiscoveryPages) {
      throw createHttpError(502, 'ga4_property_discovery_incomplete');
    }
    pageToken = next;
  }

  const propertyRefs = [];
  for (const account of summaries) {
    const properties = Array.isArray(account.propertySummaries) ? account.propertySummaries : [];
    for (const property of properties) {
      if (propertyRefs.length >= limits.maxProperties) break;
      if (/^properties\/\d+$/.test(String(property && property.property || ''))) {
        propertyRefs.push({ account, property });
      }
    }
    if (propertyRefs.length >= limits.maxProperties) break;
  }
  const properties = [];
  for (const reference of propertyRefs) {
    const result = await ga4.getProperty(accessToken, reference.property.property, { maxRetries: 0 });
    await recordAuthorizationRequest(transaction, {
      category: 'data_api',
      method: 'properties.get',
      itemCount: result.ok ? 1 : 0,
      result,
      status: result.ok ? 'success' : 'failed'
    });
    properties.push(normalizeProperty(reference.account, reference.property, result.ok ? result.body : null));
  }
  return properties;
}

async function saveAuthorizationResult(transaction, tokenBody, grantedScopes, properties, refreshToken) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      if (transaction.target_connection_id) {
        const targetRows = await connection.query(
          `SELECT pr.provider_resource_id
           FROM workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           WHERE wpc.id = ? AND wpc.workspace_id = ? AND wpc.provider = ?
           LIMIT 1`,
          [transaction.target_connection_id, transaction.workspace_id, GA4_PROVIDER]
        );
        const target = targetRows[0] || null;
        const discoveredTarget = target && properties.find(property => property.id === target.provider_resource_id);
        if (!target || !discoveredTarget || !discoveredTarget.selectable) {
          throw createHttpError(409, 'ga4_reconnect_property_mismatch');
        }
      }

      const access = encryptSecret(tokenBody.access_token);
      const refresh = encryptSecret(refreshToken);
      const accessTtl = Number(tokenBody.expires_in);
      const refreshRotated = Boolean(typeof tokenBody.refresh_token === 'string' && tokenBody.refresh_token.trim());
      const refreshTtl = tokenBody.refresh_token_expires_in ? Number(tokenBody.refresh_token_expires_in) : null;
      await connection.query(
        `INSERT INTO provider_authorization_credentials
          (id, provider_authorization_id, access_token_ciphertext, access_token_iv, access_token_tag,
           refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version, token_type,
           access_expires_at, refresh_expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
                 CASE WHEN ? IS NULL THEN NULL ELSE DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND) END)
         ON DUPLICATE KEY UPDATE
           access_token_ciphertext = VALUES(access_token_ciphertext),
           access_token_iv = VALUES(access_token_iv), access_token_tag = VALUES(access_token_tag),
           refresh_token_ciphertext = VALUES(refresh_token_ciphertext),
           refresh_token_iv = VALUES(refresh_token_iv), refresh_token_tag = VALUES(refresh_token_tag),
           key_version = VALUES(key_version), token_type = VALUES(token_type),
           access_expires_at = VALUES(access_expires_at),
           refresh_expires_at = COALESCE(VALUES(refresh_expires_at), refresh_expires_at),
           revoked_at = NULL, updated_at = UTC_TIMESTAMP(3)`,
        [
          createId(), transaction.provider_authorization_id,
          access.ciphertext, access.iv, access.tag,
          refresh.ciphertext, refresh.iv, refresh.tag, access.keyVersion,
          tokenBody.token_type || 'Bearer', accessTtl, refreshTtl, refreshTtl
        ]
      );
      if (refreshRotated && refreshTtl === null) {
        await connection.query(
          `UPDATE provider_authorization_credentials
           SET refresh_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)
           WHERE provider_authorization_id = ?`,
          [transaction.provider_authorization_id]
        );
      }
      await connection.query(
        'DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?',
        [transaction.provider_authorization_id]
      );
      for (const scope of grantedScopes) {
        await connection.query(
          `INSERT INTO provider_authorization_scopes
            (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
           VALUES (?, ?, 'granted', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
          [transaction.provider_authorization_id, scope]
        );
      }

      const primary = properties.find(property => property.selectable) || properties[0] || null;
      await connection.query(
        `UPDATE provider_authorizations
         SET provider_subject = NULL, display_name = ?, status = 'active',
             granted_at = COALESCE(granted_at, UTC_TIMESTAMP(3)),
             last_validated_at = UTC_TIMESTAMP(3), revoked_at = NULL,
             updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [primary ? primary.accountDisplayName : 'Google Analytics authorization', transaction.provider_authorization_id]
      );

      const discoveredIds = properties.map(property => property.id);
      if (discoveredIds.length > 0) {
        await connection.query(
          `DELETE pr FROM provider_resources pr
           LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
           WHERE pr.provider_authorization_id = ? AND pr.provider = ? AND wpc.id IS NULL
             AND pr.provider_resource_id NOT IN (${discoveredIds.map(() => '?').join(', ')})`,
          [transaction.provider_authorization_id, GA4_PROVIDER, ...discoveredIds]
        );
      } else {
        await connection.query(
          `DELETE pr FROM provider_resources pr
           LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
           WHERE pr.provider_authorization_id = ? AND pr.provider = ? AND wpc.id IS NULL`,
          [transaction.provider_authorization_id, GA4_PROVIDER]
        );
      }
      for (const property of properties) {
        await connection.query(
          `INSERT INTO provider_resources
            (id, provider_authorization_id, workspace_id, provider, resource_type,
             provider_resource_id, display_name, metadata)
           VALUES (?, ?, ?, ?, 'ga4_property', ?, ?, ?)
           ON DUPLICATE KEY UPDATE display_name = VALUES(display_name), metadata = VALUES(metadata),
             updated_at = UTC_TIMESTAMP(3)`,
          [
            createId(), transaction.provider_authorization_id, transaction.workspace_id, GA4_PROVIDER,
            property.id, property.displayName, JSON.stringify(property)
          ]
        );
      }

      const connectedRows = await connection.query(
        `SELECT wpc.id, wpc.data_source_id, pr.provider_resource_id
         FROM workspace_provider_connections wpc
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         WHERE pr.provider_authorization_id = ? AND wpc.provider = ?`,
        [transaction.provider_authorization_id, GA4_PROVIDER]
      );
      for (const connected of connectedRows) {
        const property = properties.find(item => item.id === connected.provider_resource_id && item.selectable);
        const status = property ? 'active' : 'reconnect_required';
        const reason = property ? null : 'ga4_property_not_returned';
        await connection.query(
          `UPDATE workspace_provider_connections SET status = ?, next_sync_at = UTC_TIMESTAMP(3),
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
          [status, connected.id]
        );
        await connection.query(
          `UPDATE data_sources SET status = ?, reconnect_reason = ?, next_sync_at = UTC_TIMESTAMP(3),
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
          [status, reason, connected.data_source_id]
        );
        await connection.query(
          `UPDATE sync_jobs SET status = ?, run_after = UTC_TIMESTAMP(3), lease_owner = NULL,
             lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
          [property ? 'due' : 'paused', connected.data_source_id]
        );
        await connection.query(
          `UPDATE provider_capabilities SET status = ?, reason = ?, updated_at = UTC_TIMESTAMP(3)
           WHERE workspace_provider_connection_id = ?`,
          [property ? 'available' : 'not_granted', reason, connected.id]
        );
      }
      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        actorUserId: transaction.initiated_by,
        action: transaction.target_connection_id ? 'connection.ga4.reauthorized' : 'connection.ga4.authorized',
        targetType: 'provider_authorization',
        targetId: transaction.provider_authorization_id,
        metadata: {
          discovered_property_count: properties.length,
          selectable_property_count: properties.filter(property => property.selectable).length,
          granted_scope_count: grantedScopes.length
        }
      });
      await connection.commit();
      return {
        discoveredPropertyCount: properties.length,
        selectablePropertyCount: properties.filter(property => property.selectable).length
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function completeGoogleAnalyticsConnection({ code, state, providerError, sessionId, userId }) {
  let transaction;
  await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      transaction = await consumeOAuthTransaction(connection, { state, sessionId, userId });
      await connection.commit();
    } catch (error) {
      if (error.code === 'ga4_oauth_state_expired') await connection.commit();
      else await connection.rollback();
      throw error;
    }
  });
  if (providerError) {
    const outcome = providerError === 'access_denied' ? 'user_denied' : 'provider_error';
    await markAuthorizationFailed(transaction, outcome);
    throw createHttpError(400, providerError === 'access_denied' ? 'ga4_authorization_denied' : 'ga4_authorization_failed');
  }
  if (!code) {
    await markAuthorizationFailed(transaction, 'missing_code');
    throw createHttpError(400, 'ga4_authorization_code_missing');
  }
  const verifier = decryptSecret({
    ciphertext: transaction.pkce_verifier_ciphertext,
    iv: transaction.pkce_verifier_iv,
    tag: transaction.pkce_verifier_tag,
    keyVersion: transaction.pkce_key_version
  });
  const exchange = await ga4.exchangeCode(code, verifier);
  const tokenBody = exchange.body || {};
  await recordAuthorizationRequest(transaction, {
    category: 'oauth', method: 'oauth.token', result: exchange,
    status: exchange.ok && tokenBody.access_token && Number(tokenBody.expires_in) > 0 ? 'success' : 'failed'
  });
  if (!exchange.ok || !tokenBody.access_token || Number(tokenBody.expires_in) <= 0) {
    await markAuthorizationFailed(transaction, exchange.error ? exchange.error.category : 'malformed_response');
    throw createHttpError(502, 'ga4_token_exchange_failed');
  }
  const grantedScopes = [...ga4.grantedScopes(tokenBody.scope)];
  if (!ga4.hasExactScopes(grantedScopes)) {
    await ga4.revokeToken(tokenBody.refresh_token || tokenBody.access_token);
    await markAuthorizationFailed(transaction, 'missing_required_scopes', grantedScopes);
    throw createHttpError(400, 'ga4_required_scopes_missing');
  }
  const existingRefresh = await loadExistingRefreshToken(transaction.provider_authorization_id);
  const refreshToken = ga4.chooseRefreshToken(tokenBody.refresh_token, existingRefresh);
  if (!refreshToken) {
    await ga4.revokeToken(tokenBody.access_token);
    await markAuthorizationFailed(transaction, 'refresh_token_missing');
    throw createHttpError(400, 'ga4_refresh_token_missing');
  }
  let properties;
  try {
    properties = await discoverProperties(
      transaction,
      tokenBody.access_token,
      getGoogleAnalyticsConfiguration().limits
    );
  } catch (error) {
    await ga4.revokeToken(refreshToken);
    await markAuthorizationFailed(transaction, error.code || 'property_discovery_failed');
    throw error;
  }
  try {
    const saved = await saveAuthorizationResult(transaction, tokenBody, grantedScopes, properties, refreshToken);
    return {
      return_path: transaction.return_path,
      outcome: saved.selectablePropertyCount === 0
        ? 'no_properties'
        : transaction.target_connection_id ? 'reconnected' : 'selection_required',
      discovered_property_count: saved.discoveredPropertyCount,
      selectable_property_count: saved.selectablePropertyCount
    };
  } catch (error) {
    await ga4.revokeToken(refreshToken);
    await markAuthorizationFailed(transaction, error.code || 'storage_failed');
    throw error;
  }
}

async function selectGoogleAnalyticsResource(userId, workspaceId, resourceId) {
  if (!resourceId) throw createHttpError(400, 'ga4_resource_required');
  return withConnection(async connection => {
    await requireGoogleAnalyticsReady(connection);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      const rows = await connection.query(
        `SELECT pr.*, pauth.status AS authorization_status, pauth.id AS authorization_id
         FROM provider_resources pr
         JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
         WHERE pr.id = ? AND pr.workspace_id = ? AND pr.provider = ?
           AND pr.resource_type = 'ga4_property'
         LIMIT 1 FOR UPDATE`,
        [resourceId, workspaceId, GA4_PROVIDER]
      );
      const resource = rows[0] || null;
      if (!resource) throw createHttpError(404, 'ga4_resource_not_found');
      if (resource.authorization_status !== 'active') throw createHttpError(409, 'ga4_authorization_not_active');
      const metadata = parseJson(resource.metadata, {});
      if (!metadata.selectable || !metadata.timezone || !metadata.currency) {
        throw createHttpError(409, 'ga4_property_details_unavailable');
      }
      const scopes = await connection.query(
        `SELECT scope FROM provider_authorization_scopes
         WHERE provider_authorization_id = ? AND status = 'granted'`,
        [resource.authorization_id]
      );
      if (!ga4.hasExactScopes(scopes.map(row => row.scope))) {
        throw createHttpError(409, 'ga4_required_scopes_missing');
      }
      const existing = await connection.query(
        `SELECT id FROM workspace_provider_connections
         WHERE workspace_id = ? AND provider_resource_id = ? LIMIT 1`,
        [workspaceId, resourceId]
      );
      if (existing[0]) throw createHttpError(409, 'ga4_property_already_connected');
      const dataSourceId = createId();
      const connectionId = createId();
      await connection.query(
        `INSERT INTO data_sources (id, workspace_id, provider, status, next_sync_at)
         VALUES (?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
        [dataSourceId, workspaceId, GA4_PROVIDER]
      );
      await connection.query(
        `INSERT INTO provider_accounts
          (id, workspace_id, data_source_id, provider, provider_account_id, username, display_name, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          createId(), workspaceId, dataSourceId, GA4_PROVIDER, resource.provider_resource_id,
          metadata.accountDisplayName || null, resource.display_name, JSON.stringify(metadata)
        ]
      );
      await connection.query(
        `INSERT INTO workspace_provider_connections
          (id, workspace_id, provider_resource_id, data_source_id, provider, status, next_sync_at)
         VALUES (?, ?, ?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
        [connectionId, workspaceId, resourceId, dataSourceId, GA4_PROVIDER]
      );
      for (const capability of GA4_CAPABILITIES) {
        await connection.query(
          `INSERT INTO provider_capabilities
            (id, workspace_provider_connection_id, capability_key, status)
           VALUES (?, ?, ?, 'available')`,
          [createId(), connectionId, capability]
        );
      }
      for (const syncKey of ['ga4.property', 'ga4.reports', 'ga4.compatibility']) {
        await connection.query(
          `INSERT INTO provider_sync_states
            (id, workspace_provider_connection_id, sync_key, cursor_state, api_version)
           VALUES (?, ?, ?, JSON_OBJECT(), ?)`,
          [createId(), connectionId, syncKey, syncKey === 'ga4.property' ? 'admin-v1beta' : 'data-v1beta']
        );
      }
      await connection.query(
        `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
         VALUES (?, ?, UTC_TIMESTAMP(3), 'due')`,
        [createId(), dataSourceId]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'connection.ga4.resource_selected',
        targetType: 'workspace_provider_connection',
        targetId: connectionId,
        metadata: { provider: GA4_PROVIDER }
      });
      await connection.commit();
      return {
        connection: {
          id: connectionId,
          data_source_id: dataSourceId,
          status: 'active',
          account: {
            id: resource.provider_resource_id,
            display_name: resource.display_name,
            account_name: metadata.accountDisplayName,
            timezone: metadata.timezone,
            currency: metadata.currency
          }
        }
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function loadAuthorizationForDisconnect(connection, workspaceId, connectionId) {
  const params = [workspaceId, GA4_PROVIDER];
  const clause = connectionId ? 'AND wpc.id = ?' : '';
  if (connectionId) params.push(connectionId);
  const rows = await connection.query(
    `SELECT pauth.id AS authorization_id, wpc.id AS connection_id, wpc.data_source_id,
            pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag,
            pac.refresh_token_ciphertext, pac.refresh_token_iv, pac.refresh_token_tag, pac.key_version,
            (SELECT COUNT(*) FROM workspace_provider_connections sibling
             JOIN provider_resources sibling_resource ON sibling_resource.id = sibling.provider_resource_id
             WHERE sibling_resource.provider_authorization_id = pauth.id) AS connection_count
     FROM provider_authorizations pauth
     LEFT JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     LEFT JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
     LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
     WHERE pauth.workspace_id = ? AND pauth.provider = ? ${clause}
       AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
     ORDER BY pauth.updated_at DESC LIMIT 1`,
    params
  );
  return rows[0] || null;
}

async function purgeAuthorization(connection, authorizationId, outcomeCategory, actorUserId = null) {
  const authRows = await connection.query(
    `SELECT workspace_id FROM provider_authorizations
     WHERE id = ? AND provider = ? LIMIT 1 FOR UPDATE`,
    [authorizationId, GA4_PROVIDER]
  );
  const authorization = authRows[0] || null;
  if (!authorization) return null;
  const sourceRows = await connection.query(
    `SELECT DISTINCT wpc.data_source_id
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ? AND wpc.data_source_id IS NOT NULL`,
    [authorizationId]
  );
  const connectionRows = await connection.query(
    `SELECT wpc.id FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ? ORDER BY wpc.created_at`,
    [authorizationId]
  );
  await connection.query(
    'DELETE FROM provider_request_events WHERE provider_authorization_id = ? AND provider = ?',
    [authorizationId, GA4_PROVIDER]
  );
  await connection.query(
    'DELETE FROM oauth_transactions WHERE provider_authorization_id = ? AND provider = ?',
    [authorizationId, GA4_PROVIDER]
  );
  await connection.query(
    `INSERT INTO provider_revocation_events
      (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id,
       provider, status, failure_category)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(), authorizationId, connectionRows[0] ? connectionRows[0].id : null,
      actorUserId, GA4_PROVIDER,
      outcomeCategory === 'provider_revoked' ? 'provider_revoked' : 'local_revoked', outcomeCategory
    ]
  );
  await connection.query(
    `DELETE wpc FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ?`,
    [authorizationId]
  );
  for (const source of sourceRows) {
    await connection.query('DELETE FROM data_sources WHERE id = ?', [source.data_source_id]);
  }
  await connection.query('DELETE FROM provider_resources WHERE provider_authorization_id = ?', [authorizationId]);
  await connection.query('DELETE FROM provider_authorization_credentials WHERE provider_authorization_id = ?', [authorizationId]);
  await connection.query('DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?', [authorizationId]);
  await connection.query(
    `UPDATE provider_authorizations
     SET actor_user_id = NULL, provider_subject = NULL, display_name = NULL,
         status = 'revoked', revoked_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3)
     WHERE id = ?`,
    [authorizationId]
  );
  await writeAuditLog(connection, {
    workspaceId: authorization.workspace_id,
    actorUserId,
    action: 'connection.ga4.revoked_and_purged',
    targetType: 'provider_authorization',
    targetId: authorizationId,
    metadata: { outcome_category: outcomeCategory }
  });
  return { deletedSourceCount: sourceRows.length };
}

function decryptRevocationToken(record) {
  const fields = record.refresh_token_ciphertext
    ? ['refresh_token_ciphertext', 'refresh_token_iv', 'refresh_token_tag']
    : ['access_token_ciphertext', 'access_token_iv', 'access_token_tag'];
  if (!record[fields[0]]) return null;
  return decryptSecret({
    ciphertext: record[fields[0]],
    iv: record[fields[1]],
    tag: record[fields[2]],
    keyVersion: record.key_version
  });
}

async function disconnectGoogleAnalytics(userId, workspaceId, connectionId = null) {
  const record = await withConnection(async connection => {
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    const value = await loadAuthorizationForDisconnect(connection, workspaceId, connectionId);
    if (!value) throw createHttpError(404, 'ga4_connection_not_found');
    return value;
  });
  if (connectionId && Number(record.connection_count) > 1) {
    await withConnection(async connection => {
      await connection.beginTransaction();
      try {
        await connection.query('DELETE FROM workspace_provider_connections WHERE id = ? AND workspace_id = ?', [connectionId, workspaceId]);
        if (record.data_source_id) await connection.query('DELETE FROM data_sources WHERE id = ?', [record.data_source_id]);
        await writeAuditLog(connection, {
          workspaceId,
          actorUserId: userId,
          action: 'connection.ga4.resource_disconnected',
          targetType: 'workspace_provider_connection',
          targetId: connectionId,
          metadata: { provider_grant_preserved: true }
        });
        await connection.commit();
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
    return {
      disconnected: true,
      local_data_deleted: true,
      provider_grant_preserved: true,
      provider_revoke: { attempted: false, success: false, status: null, outcome_category: 'shared_authorization_preserved' }
    };
  }
  let token = null;
  try {
    token = decryptRevocationToken(record);
  } catch {
    token = null;
  }
  const providerRevoke = token
    ? await ga4.revokeToken(token)
    : { attempted: false, success: false, status: null, error: { category: 'credential_unavailable' } };
  const outcome = providerRevoke.success ? 'provider_revoked' : 'provider_revoke_failed_local_purge';
  const local = await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const result = await purgeAuthorization(connection, record.authorization_id, outcome, userId);
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
    provider_grant_preserved: false,
    provider_revoke: {
      attempted: providerRevoke.attempted,
      success: providerRevoke.success,
      status: providerRevoke.status,
      outcome_category: outcome
    },
    deleted_source_count: local ? local.deletedSourceCount : 0
  };
}

async function purgeGoogleAnalyticsAuthorizationBySystem(authorizationId, outcomeCategory) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const result = await purgeAuthorization(connection, authorizationId, outcomeCategory);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

module.exports = {
  GA4_CAPABILITIES,
  GA4_PROVIDER,
  completeGoogleAnalyticsConnection,
  disconnectGoogleAnalytics,
  googleAnalyticsFoundationReady,
  normalizeProperty,
  purgeGoogleAnalyticsAuthorizationBySystem,
  requireGoogleAnalyticsReady,
  selectGoogleAnalyticsResource,
  startGoogleAnalyticsConnection
};
