const { getConnection } = require('../database');
const youtube = require('../integrations/youtube');
const { normalizeReturnPath } = require('./connection-service');
const { assertCapability } = require('./rbac');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId, hashSecret, randomToken } = require('./security');
const {
  YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS,
  getYouTubeConfiguration
} = require('./youtube-config');

const YOUTUBE_CAPABILITIES = [
  'resource_discovery',
  'channel_metrics',
  'video_listing',
  'video_analytics',
  'dimension_breakdowns',
  'disconnect'
];

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
  const membership = rows[0] || null;
  if (!membership) throw createHttpError(404, 'workspace_not_found');
  assertCapability(membership.role, capability);
  return membership.role;
}

async function youtubeFoundationReady(connection) {
  const rows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN (
         'provider_authorizations',
         'provider_authorization_credentials',
         'provider_resources',
         'workspace_provider_connections',
         'youtube_channel_snapshots',
         'youtube_analytics_daily_snapshots',
         'youtube_video_analytics_snapshots',
         'provider_request_events'
       )`
  );
  return Number(rows[0] && rows[0].count) === 8;
}

async function requireYouTubeReady(connection, env = process.env) {
  const foundationReady = await youtubeFoundationReady(connection);
  const status = getYouTubeConfiguration(env, {
    databaseReady: true,
    foundationReady,
    workerReady: true
  });
  if (!status.connectable) {
    throw createHttpError(503, 'youtube_not_configured', status.warnings);
  }
  return status;
}

async function writeAuditLog(connection, { workspaceId, actorUserId = null, action, targetType = null, targetId = null, metadata = null }) {
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

function parseJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function integerOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.trunc(parsed) : null;
}

function channelThumbnail(snippet) {
  const thumbnails = snippet && snippet.thumbnails ? snippet.thumbnails : {};
  return (thumbnails.high && thumbnails.high.url) ||
    (thumbnails.medium && thumbnails.medium.url) ||
    (thumbnails.default && thumbnails.default.url) ||
    null;
}

function normalizeChannel(item) {
  if (!item || typeof item !== 'object' || typeof item.id !== 'string' || !item.id) {
    throw createHttpError(502, 'youtube_channels_response_malformed');
  }
  const snippet = item.snippet && typeof item.snippet === 'object' ? item.snippet : {};
  const statistics = item.statistics && typeof item.statistics === 'object' ? item.statistics : {};
  const contentDetails = item.contentDetails && typeof item.contentDetails === 'object' ? item.contentDetails : {};
  const relatedPlaylists = contentDetails.relatedPlaylists && typeof contentDetails.relatedPlaylists === 'object'
    ? contentDetails.relatedPlaylists
    : {};
  const subscriberHidden = Boolean(statistics.hiddenSubscriberCount);
  return {
    id: item.id,
    title: String(snippet.title || 'YouTube channel').slice(0, 255),
    description: snippet.description ? String(snippet.description) : null,
    customUrl: snippet.customUrl ? String(snippet.customUrl) : null,
    publishedAt: snippet.publishedAt || null,
    country: snippet.country || null,
    thumbnailUrl: channelThumbnail(snippet),
    uploadsPlaylistId: relatedPlaylists.uploads ? String(relatedPlaylists.uploads) : null,
    subscriberCount: subscriberHidden ? null : integerOrNull(statistics.subscriberCount),
    subscriberCountHidden: subscriberHidden,
    lifetimeViewCount: integerOrNull(statistics.viewCount),
    publicVideoCount: integerOrNull(statistics.videoCount)
  };
}

async function startYouTubeConnection({ userId, sessionId, workspaceId, returnPath = '/', targetConnectionId = null }) {
  return withConnection(async connection => {
    const safeReturnPath = normalizeReturnPath(returnPath);
    const readiness = await requireYouTubeReady(connection);
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
           WHERE wpc.id = ? AND wpc.workspace_id = ? AND wpc.provider = 'youtube'
           LIMIT 1
           FOR UPDATE`,
          [targetConnectionId, workspaceId]
        );
        if (!targetRows[0]) throw createHttpError(404, 'youtube_connection_not_found');
        authorizationId = targetRows[0].authorization_id;
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
           WHERE pr.provider_authorization_id = ? AND wpc.provider = 'youtube'`,
          [authorizationId]
        );
      } else {
        const existingRows = await connection.query(
          `SELECT pauth.id, pauth.status,
                  EXISTS(
                    SELECT 1 FROM provider_resources pr
                    WHERE pr.provider_authorization_id = pauth.id
                  ) AS has_resources,
                  EXISTS(
                    SELECT 1
                    FROM workspace_provider_connections wpc
                    JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
                    WHERE pr.provider_authorization_id = pauth.id
                  ) AS has_connections
           FROM provider_authorizations pauth
           WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'
             AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
           ORDER BY FIELD(pauth.status, 'active', 'reconnect_required', 'authorizing', 'disabled'),
                    pauth.updated_at DESC
           LIMIT 1
           FOR UPDATE`,
          [workspaceId]
        );
        const existing = existingRows[0] || null;
        if (existing && (Number(existing.has_resources) === 1 || Number(existing.has_connections) === 1)) {
          throw createHttpError(409, 'youtube_authorization_already_exists');
        }
        if (existing) {
          authorizationId = existing.id;
          await connection.query(
            `UPDATE provider_authorizations
             SET actor_user_id = ?, status = 'authorizing', revoked_at = NULL,
                 deletion_due_at = NULL, updated_at = UTC_TIMESTAMP(3)
             WHERE id = ?`,
            [userId, authorizationId]
          );
        } else {
          authorizationId = createId();
          await connection.query(
            `INSERT INTO provider_authorizations
              (id, workspace_id, provider, actor_user_id, status, auth_product, api_version)
             VALUES (?, ?, 'youtube', ?, 'authorizing', 'analytics', 'data-v3/analytics-v2')`,
            [authorizationId, workspaceId, userId]
          );
        }
      }

      await connection.query(
        `UPDATE oauth_transactions
         SET status = 'failed', consumed_at = COALESCE(consumed_at, UTC_TIMESTAMP(3)),
             pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
             pkce_verifier_tag = NULL, pkce_key_version = NULL
         WHERE provider_authorization_id = ? AND provider = 'youtube' AND status = 'pending'`,
        [authorizationId]
      );

      const state = randomToken(32);
      const pkce = youtube.createPkcePair();
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
         VALUES (?, ?, 'youtube', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
        [
          createId(),
          hashSecret(state),
          workspaceId,
          userId,
          sessionId,
          authorizationId,
          targetConnectionId,
          safeReturnPath,
          JSON.stringify(youtube.YOUTUBE_SCOPES),
          readiness.redirectUri,
          verifier.ciphertext,
          verifier.iv,
          verifier.tag,
          verifier.keyVersion,
          readiness.limits.oauthStateTtlSeconds
        ]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: targetConnectionId ? 'connection.youtube.reauthorize_start' : 'connection.youtube.start',
        targetType: 'provider_authorization',
        targetId: authorizationId,
        metadata: { requested_scope_count: youtube.YOUTUBE_SCOPES.length }
      });
      await connection.commit();
      return {
        authorization_url: youtube.buildAuthorizationUrl({
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
  if (!state) throw createHttpError(400, 'youtube_oauth_state_missing');
  const rows = await connection.query(
    `SELECT oauth_transactions.*,
            expires_at <= UTC_TIMESTAMP(3) AS is_expired
     FROM oauth_transactions
     WHERE state_hash = ?
     LIMIT 1
     FOR UPDATE`,
    [hashSecret(state)]
  );
  const transaction = rows[0] || null;
  if (!transaction) throw createHttpError(400, 'youtube_oauth_state_invalid');
  if (transaction.provider !== 'youtube') throw createHttpError(400, 'youtube_oauth_provider_mismatch');
  if (transaction.consumed_at || transaction.status !== 'pending') {
    throw createHttpError(400, 'youtube_oauth_state_replayed');
  }
  if (Number(transaction.is_expired) === 1) {
    await connection.query(
      `UPDATE oauth_transactions
       SET status = 'expired', consumed_at = UTC_TIMESTAMP(3),
           pkce_verifier_ciphertext = NULL, pkce_verifier_iv = NULL,
           pkce_verifier_tag = NULL, pkce_key_version = NULL
       WHERE id = ?`,
      [transaction.id]
    );
    if (transaction.target_connection_id) {
      await connection.query(
        `UPDATE provider_authorizations SET status = 'reconnect_required', updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [transaction.provider_authorization_id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections wpc
         JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
         JOIN data_sources ds ON ds.id = wpc.data_source_id
         SET wpc.status = 'reconnect_required', ds.status = 'reconnect_required',
             ds.reconnect_reason = 'youtube_authorization_expired',
             wpc.updated_at = UTC_TIMESTAMP(3), ds.updated_at = UTC_TIMESTAMP(3)
         WHERE pr.provider_authorization_id = ? AND wpc.provider = 'youtube'`,
        [transaction.provider_authorization_id]
      );
    }
    throw createHttpError(400, 'youtube_oauth_state_expired');
  }
  if (transaction.session_id !== sessionId) throw createHttpError(403, 'youtube_oauth_session_mismatch');
  if (transaction.initiated_by !== userId) throw createHttpError(403, 'youtube_oauth_user_mismatch');

  const authorizationRows = await connection.query(
    `SELECT workspace_id, provider FROM provider_authorizations WHERE id = ? LIMIT 1`,
    [transaction.provider_authorization_id]
  );
  const authorization = authorizationRows[0] || null;
  if (!authorization || authorization.provider !== 'youtube') {
    throw createHttpError(400, 'youtube_oauth_authorization_mismatch');
  }
  if (authorization.workspace_id !== transaction.workspace_id) {
    throw createHttpError(400, 'youtube_oauth_workspace_mismatch');
  }
  await requireWorkspaceRole(connection, transaction.workspace_id, userId, 'manageConnection');

  if (transaction.target_connection_id) {
    const targetRows = await connection.query(
      `SELECT workspace_id, provider FROM workspace_provider_connections WHERE id = ? LIMIT 1`,
      [transaction.target_connection_id]
    );
    const target = targetRows[0] || null;
    if (!target || target.workspace_id !== transaction.workspace_id || target.provider !== 'youtube') {
      throw createHttpError(400, 'youtube_oauth_workspace_mismatch');
    }
  }

  const requestedScopes = parseJson(transaction.requested_scopes, []);
  if (
    requestedScopes.length !== youtube.YOUTUBE_SCOPES.length ||
    !youtube.YOUTUBE_SCOPES.every(scope => requestedScopes.includes(scope))
  ) {
    throw createHttpError(400, 'youtube_oauth_scope_binding_mismatch');
  }
  if (transaction.redirect_uri !== process.env.YOUTUBE_REDIRECT_URI) {
    throw createHttpError(400, 'youtube_oauth_redirect_mismatch');
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

async function markTransactionFailed(transactionId, authorizationId, targetConnectionId, code, grantedScopes = null) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await connection.query(
        `UPDATE oauth_transactions
         SET status = 'failed', pkce_verifier_ciphertext = NULL,
             pkce_verifier_iv = NULL, pkce_verifier_tag = NULL, pkce_key_version = NULL
         WHERE id = ?`,
        [transactionId]
      );
      if (Array.isArray(grantedScopes)) {
        await connection.query(
          'DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?',
          [authorizationId]
        );
        for (const scope of grantedScopes) {
          await connection.query(
            `INSERT INTO provider_authorization_scopes
              (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
             VALUES (?, ?, 'granted', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
            [authorizationId, String(scope)]
          );
        }
      }
      await connection.query(
        `UPDATE provider_authorizations
         SET status = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [targetConnectionId ? 'reconnect_required' : 'disabled', authorizationId]
      );
      if (targetConnectionId) {
        await connection.query(
          `UPDATE workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           JOIN data_sources ds ON ds.id = wpc.data_source_id
           SET wpc.status = 'reconnect_required', ds.status = 'reconnect_required',
               ds.reconnect_reason = ?, wpc.updated_at = UTC_TIMESTAMP(3),
               ds.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = 'youtube'`,
          [`youtube_authorization_${code}`.slice(0, 255), authorizationId]
        );
        await connection.query(
          `UPDATE provider_capabilities pc
           JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           SET pc.status = 'not_granted', pc.reason = ?, pc.updated_at = UTC_TIMESTAMP(3)
           WHERE pr.provider_authorization_id = ? AND wpc.provider = 'youtube'`,
          [`youtube_authorization_${code}`.slice(0, 255), authorizationId]
        );
      }
      const authorizationRows = await connection.query(
        'SELECT workspace_id FROM provider_authorizations WHERE id = ? LIMIT 1',
        [authorizationId]
      );
      if (!authorizationRows[0]) throw createHttpError(404, 'youtube_authorization_not_found');
      await writeAuditLog(connection, {
        workspaceId: authorizationRows[0].workspace_id,
        action: 'connection.youtube.authorization_failed',
        targetType: 'provider_authorization',
        targetId: authorizationId,
        metadata: { outcome_category: code }
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
       method_name, quota_cost_estimate, item_count, attempts, status,
       failure_category, retry_after_seconds)
     VALUES (?, ?, ?, 'youtube', ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      transaction.workspace_id,
      transaction.provider_authorization_id,
      details.category,
      details.method,
      details.quotaCost || 0,
      details.itemCount === undefined ? null : details.itemCount,
      details.result && Number.isInteger(details.result.attempts) ? details.result.attempts : 1,
      details.status,
      details.result && details.result.error ? details.result.error.category : null,
      details.result ? details.result.retryAfterSeconds : null
    ]
  ));
}

async function saveAuthorizationResult(transaction, tokenBody, grantedScopes, channels, refreshToken) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const duplicateIds = [];
      for (const channel of channels) {
        const rows = await connection.query(
          `SELECT provider_authorization_id
           FROM provider_resources
           WHERE workspace_id = ? AND provider = 'youtube'
             AND resource_type = 'youtube_channel' AND provider_resource_id = ?
           LIMIT 1`,
          [transaction.workspace_id, channel.id]
        );
        if (rows[0] && rows[0].provider_authorization_id !== transaction.provider_authorization_id) {
          duplicateIds.push(channel.id);
        }
      }

      if (duplicateIds.length > 0) throw createHttpError(409, 'youtube_channel_already_discovered');

      const discoveredIds = [...new Set(channels.map(channel => channel.id))];
      const keepDiscoveredClause = discoveredIds.length > 0
        ? `AND pr.provider_resource_id NOT IN (${discoveredIds.map(() => '?').join(', ')})`
        : '';
      await connection.query(
        `DELETE pr
         FROM provider_resources pr
         LEFT JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
         WHERE pr.provider_authorization_id = ? AND pr.provider = 'youtube'
           AND wpc.id IS NULL ${keepDiscoveredClause}`,
        [transaction.provider_authorization_id, ...discoveredIds]
      );

      const access = encryptSecret(tokenBody.access_token);
      const refresh = encryptSecret(refreshToken);
      const accessTtlSeconds = Number(tokenBody.expires_in);
      const refreshTokenRotated = Boolean(
        typeof tokenBody.refresh_token === 'string' && tokenBody.refresh_token.trim()
      );
      const refreshTtlSeconds = tokenBody.refresh_token_expires_in
        ? Number(tokenBody.refresh_token_expires_in)
        : null;
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
           access_token_iv = VALUES(access_token_iv),
           access_token_tag = VALUES(access_token_tag),
           refresh_token_ciphertext = VALUES(refresh_token_ciphertext),
           refresh_token_iv = VALUES(refresh_token_iv),
           refresh_token_tag = VALUES(refresh_token_tag),
           key_version = VALUES(key_version),
           token_type = VALUES(token_type),
           access_expires_at = VALUES(access_expires_at),
           refresh_expires_at = COALESCE(VALUES(refresh_expires_at), refresh_expires_at),
           revoked_at = NULL,
           updated_at = UTC_TIMESTAMP(3)`,
        [
          createId(),
          transaction.provider_authorization_id,
          access.ciphertext,
          access.iv,
          access.tag,
          refresh.ciphertext,
          refresh.iv,
          refresh.tag,
          access.keyVersion,
          tokenBody.token_type || 'Bearer',
          accessTtlSeconds,
          refreshTtlSeconds,
          refreshTtlSeconds
        ]
      );
      if (refreshTokenRotated && refreshTtlSeconds === null) {
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

      const primary = channels[0] || null;
      await connection.query(
        `UPDATE provider_authorizations
         SET provider_subject = ?, display_name = ?, status = 'active',
             granted_at = COALESCE(granted_at, UTC_TIMESTAMP(3)),
             last_validated_at = UTC_TIMESTAMP(3), revoked_at = NULL,
             deletion_due_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? DAY),
             updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [
          primary ? primary.id : null,
          primary ? primary.title : 'YouTube authorization',
          YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS,
          transaction.provider_authorization_id
        ]
      );

      for (const channel of channels) {
        await connection.query(
          `INSERT INTO provider_resources
            (id, provider_authorization_id, workspace_id, provider, resource_type,
             provider_resource_id, display_name, metadata)
           VALUES (?, ?, ?, 'youtube', 'youtube_channel', ?, ?, ?)
           ON DUPLICATE KEY UPDATE
             display_name = VALUES(display_name),
             metadata = VALUES(metadata),
             updated_at = UTC_TIMESTAMP(3)`,
          [
            createId(),
            transaction.provider_authorization_id,
            transaction.workspace_id,
            channel.id,
            channel.title,
            JSON.stringify(channel)
          ]
        );
      }

      if (transaction.target_connection_id) {
        const connectedRows = await connection.query(
          `SELECT wpc.id AS connection_id, wpc.data_source_id, pr.provider_resource_id
           FROM workspace_provider_connections wpc
           JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
           WHERE pr.provider_authorization_id = ? AND wpc.workspace_id = ?
             AND wpc.provider = 'youtube'`,
          [transaction.provider_authorization_id, transaction.workspace_id]
        );
        const target = connectedRows.find(row => row.connection_id === transaction.target_connection_id);
        const targetChannel = target && channels.find(item => item.id === target.provider_resource_id);
        if (!target || !targetChannel) throw createHttpError(409, 'youtube_reconnect_channel_mismatch');

        for (const connected of connectedRows) {
          const channel = channels.find(item => item.id === connected.provider_resource_id);
          if (!channel) {
            await connection.query(
              `UPDATE data_sources
               SET status = 'reconnect_required', reconnect_reason = 'youtube_channel_not_returned',
                   updated_at = UTC_TIMESTAMP(3)
               WHERE id = ?`,
              [connected.data_source_id]
            );
            await connection.query(
              `UPDATE workspace_provider_connections
               SET status = 'reconnect_required', updated_at = UTC_TIMESTAMP(3)
               WHERE id = ?`,
              [connected.connection_id]
            );
            await connection.query(
              `UPDATE sync_jobs
               SET status = 'paused', lease_owner = NULL, lease_expires_at = NULL,
                   updated_at = UTC_TIMESTAMP(3)
               WHERE data_source_id = ?`,
              [connected.data_source_id]
            );
            await connection.query(
              `UPDATE provider_capabilities
               SET status = 'not_granted', reason = 'youtube_channel_not_returned',
                   updated_at = UTC_TIMESTAMP(3)
               WHERE workspace_provider_connection_id = ?`,
              [connected.connection_id]
            );
            continue;
          }
          await connection.query(
            `UPDATE data_sources
             SET status = 'active', reconnect_reason = NULL, next_sync_at = UTC_TIMESTAMP(3),
                 updated_at = UTC_TIMESTAMP(3)
             WHERE id = ?`,
            [connected.data_source_id]
          );
          await connection.query(
            `UPDATE workspace_provider_connections
             SET status = 'active', next_sync_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3)
             WHERE id = ?`,
            [connected.connection_id]
          );
          await connection.query(
            `UPDATE provider_accounts
             SET username = ?, display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
             WHERE data_source_id = ?`,
            [channel.customUrl, channel.title, JSON.stringify(channel), connected.data_source_id]
          );
          await connection.query(
            `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
             VALUES (?, ?, UTC_TIMESTAMP(3), 'due')
             ON DUPLICATE KEY UPDATE status = 'due', run_after = UTC_TIMESTAMP(3),
               lease_owner = NULL, lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)`,
            [createId(), connected.data_source_id]
          );
          await connection.query(
            `UPDATE provider_capabilities
             SET status = 'available', reason = NULL, updated_at = UTC_TIMESTAMP(3)
             WHERE workspace_provider_connection_id = ?`,
            [connected.connection_id]
          );
        }
      }

      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        actorUserId: transaction.initiated_by,
        action: transaction.target_connection_id ? 'connection.youtube.reauthorized' : 'connection.youtube.authorized',
        targetType: 'provider_authorization',
        targetId: transaction.provider_authorization_id,
        metadata: { discovered_channel_count: channels.length, granted_scope_count: grantedScopes.length }
      });
      await connection.commit();
      return { discoveredChannelCount: channels.length };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function completeYouTubeConnection({ code, state, providerError, sessionId, userId }) {
  let transaction;
  await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      transaction = await consumeOAuthTransaction(connection, { state, sessionId, userId });
      await connection.commit();
    } catch (error) {
      if (error.code === 'youtube_oauth_state_expired') await connection.commit();
      else await connection.rollback();
      throw error;
    }
  });

  if (providerError) {
    const codeValue = providerError === 'access_denied' ? 'user_denied' : 'provider_error';
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      codeValue
    );
    throw createHttpError(400, providerError === 'access_denied' ? 'youtube_authorization_denied' : 'youtube_authorization_failed');
  }
  if (!code) {
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      'missing_code'
    );
    throw createHttpError(400, 'youtube_authorization_code_missing');
  }

  const verifier = decryptSecret({
    ciphertext: transaction.pkce_verifier_ciphertext,
    iv: transaction.pkce_verifier_iv,
    tag: transaction.pkce_verifier_tag,
    keyVersion: transaction.pkce_key_version
  });
  const exchange = await youtube.exchangeCode(code, verifier);
  const tokenBody = exchange.body || {};
  await recordAuthorizationRequest(transaction, {
    category: 'oauth',
    method: 'oauth.token',
    quotaCost: 0,
    result: exchange,
    status: exchange.ok && tokenBody.access_token && Number(tokenBody.expires_in) > 0 ? 'success' : 'failed'
  });
  if (!exchange.ok || !tokenBody.access_token || Number(tokenBody.expires_in) <= 0) {
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      exchange.error ? exchange.error.category : 'malformed_response'
    );
    throw createHttpError(502, 'youtube_token_exchange_failed');
  }

  const grantedScopes = [...youtube.grantedScopes(tokenBody.scope)];
  if (!youtube.hasExactScopes(grantedScopes)) {
    await youtube.revokeToken(tokenBody.refresh_token || tokenBody.access_token);
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      'missing_required_scopes',
      grantedScopes
    );
    throw createHttpError(400, 'youtube_required_scopes_missing');
  }

  const existingRefreshToken = await loadExistingRefreshToken(transaction.provider_authorization_id);
  const refreshToken = youtube.chooseRefreshToken(tokenBody.refresh_token, existingRefreshToken);
  if (!refreshToken) {
    await youtube.revokeToken(tokenBody.access_token);
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      'refresh_token_missing'
    );
    throw createHttpError(400, 'youtube_refresh_token_missing');
  }

  const channelResult = await youtube.listMyChannels(tokenBody.access_token, { maxRetries: 0 });
  const channelItems = channelResult.body && Array.isArray(channelResult.body.items)
    ? channelResult.body.items
    : null;
  await recordAuthorizationRequest(transaction, {
    category: 'data_api',
    method: 'channels.list.discovery',
    quotaCost: 1,
    itemCount: channelItems ? channelItems.length : null,
    result: channelResult,
    status: !channelResult.ok || !channelItems ? 'failed' : channelItems.length > 0 ? 'success' : 'empty'
  });
  if (!channelResult.ok || !channelResult.body || !Array.isArray(channelResult.body.items)) {
    await youtube.revokeToken(refreshToken);
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      channelResult.error ? channelResult.error.category : 'malformed_response'
    );
    throw createHttpError(502, 'youtube_channel_discovery_failed');
  }
  const channels = channelResult.body.items.map(normalizeChannel);

  try {
    const saved = await saveAuthorizationResult(
      transaction,
      tokenBody,
      grantedScopes,
      channels,
      refreshToken
    );
    return {
      return_path: transaction.return_path,
      outcome: channels.length === 0 ? 'no_channels' : transaction.target_connection_id ? 'reconnected' : 'selection_required',
      discovered_channel_count: saved.discoveredChannelCount
    };
  } catch (error) {
    await youtube.revokeToken(refreshToken);
    await markTransactionFailed(
      transaction.id,
      transaction.provider_authorization_id,
      transaction.target_connection_id,
      error.code || 'storage_failed'
    );
    throw error;
  }
}

async function selectYouTubeResource(userId, workspaceId, resourceId) {
  if (!resourceId) throw createHttpError(400, 'youtube_resource_required');
  return withConnection(async connection => {
    await requireYouTubeReady(connection);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      const rows = await connection.query(
        `SELECT pr.*, pauth.status AS authorization_status, pauth.id AS authorization_id
         FROM provider_resources pr
         JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
         WHERE pr.id = ? AND pr.workspace_id = ? AND pr.provider = 'youtube'
           AND pr.resource_type = 'youtube_channel'
         LIMIT 1
         FOR UPDATE`,
        [resourceId, workspaceId]
      );
      const resource = rows[0] || null;
      if (!resource) throw createHttpError(404, 'youtube_resource_not_found');
      if (resource.authorization_status !== 'active') throw createHttpError(409, 'youtube_authorization_not_active');

      const scopeRows = await connection.query(
        `SELECT scope FROM provider_authorization_scopes
         WHERE provider_authorization_id = ? AND status = 'granted'`,
        [resource.authorization_id]
      );
      if (!youtube.hasExactScopes(scopeRows.map(row => row.scope))) {
        throw createHttpError(409, 'youtube_required_scopes_missing');
      }
      const existingRows = await connection.query(
        `SELECT id FROM workspace_provider_connections
         WHERE workspace_id = ? AND provider_resource_id = ?
         LIMIT 1`,
        [workspaceId, resourceId]
      );
      if (existingRows[0]) throw createHttpError(409, 'youtube_channel_already_connected');

      const metadata = parseJson(resource.metadata, {});
      const dataSourceId = createId();
      const connectionId = createId();
      await connection.query(
        `INSERT INTO data_sources (id, workspace_id, provider, status, next_sync_at)
         VALUES (?, ?, 'youtube', 'active', UTC_TIMESTAMP(3))`,
        [dataSourceId, workspaceId]
      );
      await connection.query(
        `INSERT INTO provider_accounts
          (id, workspace_id, data_source_id, provider, provider_account_id, username, display_name, metadata)
         VALUES (?, ?, ?, 'youtube', ?, ?, ?, ?)`,
        [
          createId(),
          workspaceId,
          dataSourceId,
          resource.provider_resource_id,
          metadata.customUrl || null,
          resource.display_name,
          JSON.stringify(metadata)
        ]
      );
      await connection.query(
        `INSERT INTO workspace_provider_connections
          (id, workspace_id, provider_resource_id, data_source_id, provider, status, next_sync_at)
         VALUES (?, ?, ?, ?, 'youtube', 'active', UTC_TIMESTAMP(3))`,
        [connectionId, workspaceId, resourceId, dataSourceId]
      );
      for (const capability of YOUTUBE_CAPABILITIES) {
        await connection.query(
          `INSERT INTO provider_capabilities
            (id, workspace_provider_connection_id, capability_key, status)
           VALUES (?, ?, ?, 'available')`,
          [createId(), connectionId, capability]
        );
      }
      for (const syncKey of ['youtube.uploads', 'youtube.analytics']) {
        await connection.query(
          `INSERT INTO provider_sync_states
            (id, workspace_provider_connection_id, sync_key, cursor_state, api_version)
           VALUES (?, ?, ?, JSON_OBJECT(), ?)`,
          [createId(), connectionId, syncKey, syncKey === 'youtube.uploads' ? 'data-v3' : 'analytics-v2']
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
        action: 'connection.youtube.resource_selected',
        targetType: 'workspace_provider_connection',
        targetId: connectionId,
        metadata: { provider: 'youtube' }
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

async function loadAuthorizationForDisconnect(connection, workspaceId, connectionId) {
  if (connectionId) {
    const rows = await connection.query(
      `SELECT pauth.id AS authorization_id,
              pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag,
              pac.refresh_token_ciphertext, pac.refresh_token_iv, pac.refresh_token_tag,
              pac.key_version
       FROM workspace_provider_connections wpc
       JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
       JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
       LEFT JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
       WHERE wpc.workspace_id = ? AND wpc.provider = 'youtube' AND wpc.id = ?
       LIMIT 1`,
      [workspaceId, connectionId]
    );
    return rows[0] || null;
  }
  const rows = await connection.query(
    `SELECT pauth.id AS authorization_id,
            pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag,
            pac.refresh_token_ciphertext, pac.refresh_token_iv, pac.refresh_token_tag,
            pac.key_version
     FROM provider_authorizations pauth
     LEFT JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'
       AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
     ORDER BY FIELD(pauth.status, 'active', 'reconnect_required', 'authorizing', 'disabled'), pauth.updated_at DESC
     LIMIT 1`,
    [workspaceId]
  );
  return rows[0] || null;
}

async function purgeAuthorization(connection, authorizationId, outcomeCategory) {
  const authorizationRows = await connection.query(
    `SELECT workspace_id FROM provider_authorizations
     WHERE id = ? AND provider = 'youtube'
     LIMIT 1
     FOR UPDATE`,
    [authorizationId]
  );
  const authorization = authorizationRows[0] || null;
  if (!authorization) return null;
  const sourceRows = await connection.query(
    `SELECT DISTINCT wpc.data_source_id
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ? AND wpc.data_source_id IS NOT NULL`,
    [authorizationId]
  );
  const connectionRows = await connection.query(
    `SELECT wpc.id
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     WHERE pr.provider_authorization_id = ?
     ORDER BY wpc.created_at ASC`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM provider_request_events
     WHERE provider_authorization_id = ? AND provider = 'youtube'`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM oauth_transactions
     WHERE provider_authorization_id = ? AND provider = 'youtube'`,
    [authorizationId]
  );
  await connection.query(
    `DELETE FROM audit_logs
     WHERE workspace_id = ? AND target_id = ? AND action LIKE 'connection.youtube.%'`,
    [authorization.workspace_id, authorizationId]
  );
  for (const connected of connectionRows) {
    await connection.query(
      `DELETE FROM audit_logs
       WHERE workspace_id = ? AND target_id = ? AND action LIKE 'connection.youtube.%'`,
      [authorization.workspace_id, connected.id]
    );
  }
  await connection.query(
    `INSERT INTO provider_revocation_events
      (id, provider_authorization_id, workspace_provider_connection_id, actor_user_id,
       provider, status, failure_category)
     VALUES (?, ?, ?, NULL, 'youtube', ?, ?)`,
    [
      createId(),
      authorizationId,
      connectionRows[0] ? connectionRows[0].id : null,
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
  for (const source of sourceRows) {
    await connection.query('DELETE FROM data_sources WHERE id = ?', [source.data_source_id]);
  }
  await connection.query('DELETE FROM provider_resources WHERE provider_authorization_id = ?', [authorizationId]);
  await connection.query('DELETE FROM provider_authorization_credentials WHERE provider_authorization_id = ?', [authorizationId]);
  await connection.query('DELETE FROM provider_authorization_scopes WHERE provider_authorization_id = ?', [authorizationId]);
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
    action: 'connection.youtube.revoked_and_purged',
    targetType: 'provider_authorization',
    targetId: authorizationId,
    metadata: { outcome_category: outcomeCategory }
  });
  return { workspaceId: authorization.workspace_id, deletedSourceCount: sourceRows.length };
}

async function disconnectYouTube(userId, workspaceId, connectionId = null) {
  const record = await withConnection(async connection => {
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    const value = await loadAuthorizationForDisconnect(connection, workspaceId, connectionId);
    if (!value) throw createHttpError(404, 'youtube_connection_not_found');
    return value;
  });

  let token = null;
  try {
    if (record.refresh_token_ciphertext) {
      token = decryptSecret({
        ciphertext: record.refresh_token_ciphertext,
        iv: record.refresh_token_iv,
        tag: record.refresh_token_tag,
        keyVersion: record.key_version
      });
    } else if (record.access_token_ciphertext) {
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
    ? await youtube.revokeToken(token)
    : { attempted: false, success: false, status: null, error: { category: 'credential_unavailable' } };
  const outcomeCategory = providerRevoke.success ? 'provider_revoked' : 'provider_revoke_failed_local_purge';

  const local = await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const result = await purgeAuthorization(connection, record.authorization_id, outcomeCategory);
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
    provider_revoke: {
      attempted: providerRevoke.attempted,
      success: providerRevoke.success,
      status: providerRevoke.status,
      outcome_category: outcomeCategory
    },
    deleted_source_count: local ? local.deletedSourceCount : 0
  };
}

async function purgeYouTubeAuthorizationBySystem(authorizationId, outcomeCategory) {
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

async function purgeOverdueYouTubeAuthorizations(limit = 50) {
  return withConnection(async connection => {
    if (!(await youtubeFoundationReady(connection))) return { purged: 0 };
    const boundedLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const rows = await connection.query(
      `SELECT pauth.id
       FROM provider_authorizations pauth
       WHERE pauth.provider = 'youtube'
         AND pauth.status IN ('active', 'authorizing', 'reconnect_required', 'disabled')
         AND pauth.deletion_due_at IS NOT NULL
         AND pauth.deletion_due_at <= UTC_TIMESTAMP(3)
       ORDER BY pauth.deletion_due_at ASC
       LIMIT ?`,
      [boundedLimit]
    );
    let purged = 0;
    for (const row of rows) {
      await connection.beginTransaction();
      try {
        const result = await purgeAuthorization(connection, row.id, 'authorization_validation_window_expired');
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

module.exports = {
  YOUTUBE_CAPABILITIES,
  completeYouTubeConnection,
  disconnectYouTube,
  normalizeChannel,
  purgeOverdueYouTubeAuthorizations,
  purgeYouTubeAuthorizationBySystem,
  requireYouTubeReady,
  selectYouTubeResource,
  startYouTubeConnection,
  youtubeFoundationReady
};
