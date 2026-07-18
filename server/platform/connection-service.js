const { getConnection } = require('../database');
const tiktok = require('../integrations/tiktok');
const { assertCapability } = require('./rbac');
const {
  markTikTokProviderFoundationDisconnected,
  upsertTikTokProviderFoundation
} = require('./provider-foundation');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId, hashSecret, randomToken } = require('./security');

function createHttpError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  return error;
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) {
    const error = new Error('database_not_configured');
    error.status = 503;
    error.code = 'database_not_configured';
    throw error;
  }
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
  if (!membership) {
    const error = new Error('workspace_not_found');
    error.status = 404;
    error.code = 'workspace_not_found';
    throw error;
  }
  assertCapability(membership.role, capability);
  return membership.role;
}

function normalizeReturnPath(returnPath) {
  const value = returnPath || '/';
  if (typeof value !== 'string' || value.length > 512 || !value.startsWith('/') || value.startsWith('//') || value.includes('\\')) {
    throw createHttpError(400, 'invalid_return_path');
  }
  const parsed = new URL(value, 'https://social-insights.local');
  if (parsed.origin !== 'https://social-insights.local') {
    throw createHttpError(400, 'invalid_return_path');
  }
  const legacySuffix = parsed.pathname === '/app'
    ? '/'
    : parsed.pathname.startsWith('/app/')
      ? parsed.pathname.slice('/app'.length)
      : null;
  const pathname = legacySuffix || parsed.pathname;
  const isRoot = pathname === '/';
  const isContentDetail = /^\/workspaces\/[0-9a-f-]{36}\/content\/[0-9a-f-]{36}\/?$/i.test(pathname);
  if (!isRoot && !isContentDetail) {
    throw createHttpError(400, 'invalid_return_path');
  }
  return `${pathname}${parsed.search}${parsed.hash}`;
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

async function findOrCreateTikTokSource(connection, workspaceId) {
  const rows = await connection.query(
    `SELECT * FROM data_sources
     WHERE workspace_id = ? AND provider = 'tiktok' AND deleted_at IS NULL
     ORDER BY created_at ASC LIMIT 1`,
    [workspaceId]
  );
  if (rows[0]) return rows[0];
  const id = createId();
  await connection.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'tiktok', 'connecting')`,
    [id, workspaceId]
  );
  return { id, workspace_id: workspaceId, provider: 'tiktok', status: 'connecting' };
}

async function startTikTokConnection(userId, workspaceId, returnPath = '/') {
  return withConnection(async connection => {
    const safeReturnPath = normalizeReturnPath(returnPath);
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    await connection.beginTransaction();
    try {
      const dataSource = await findOrCreateTikTokSource(connection, workspaceId);
      const state = randomToken(32);
      await connection.query(
        `INSERT INTO oauth_transactions
          (id, state_hash, provider, workspace_id, initiated_by, return_path, expires_at)
         VALUES (?, ?, 'tiktok', ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 600 SECOND))`,
        [createId(), hashSecret(state), workspaceId, userId, safeReturnPath]
      );
      await connection.query(
        `UPDATE data_sources
         SET status = CASE WHEN status = 'active' THEN status ELSE 'connecting' END,
             updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [dataSource.id]
      );
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'connection.tiktok.start',
        targetType: 'data_source',
        targetId: dataSource.id,
        metadata: { return_path: safeReturnPath }
      });
      await connection.commit();
      return { authorization_url: tiktok.buildAuthorizationUrl(state), expires_in_seconds: 600 };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function consumeOAuthTransaction(connection, state) {
  const rows = await connection.query(
    `SELECT * FROM oauth_transactions
     WHERE state_hash = ? AND provider = 'tiktok' AND consumed_at IS NULL AND expires_at > UTC_TIMESTAMP(3)
     LIMIT 1
     FOR UPDATE`,
    [hashSecret(state)]
  );
  const transaction = rows[0] || null;
  if (!transaction) return null;
  await connection.query(
    `UPDATE oauth_transactions SET consumed_at = UTC_TIMESTAMP(3), status = 'consumed'
     WHERE id = ?`,
    [transaction.id]
  );
  return transaction;
}

async function markOAuthTransactionFailed(transactionId, reason) {
  await withConnection(connection => connection.query(
    `UPDATE oauth_transactions SET status = 'failed'
     WHERE id = ? AND consumed_at IS NOT NULL`,
    [transactionId]
  ).then(async () => {
    const rows = await connection.query('SELECT workspace_id, initiated_by FROM oauth_transactions WHERE id = ? LIMIT 1', [transactionId]);
    if (rows[0]) {
      await writeAuditLog(connection, {
        workspaceId: rows[0].workspace_id,
        actorUserId: rows[0].initiated_by,
        action: 'connection.tiktok.callback_failed',
        targetType: 'oauth_transaction',
        targetId: transactionId,
        metadata: { reason }
      });
    }
  }));
}

async function markConnectionFailed(workspaceId, reason) {
  await withConnection(connection => connection.query(
    `UPDATE data_sources
     SET status = CASE WHEN status = 'active' THEN status ELSE 'reconnect_required' END,
         reconnect_reason = ?,
         updated_at = UTC_TIMESTAMP(3)
     WHERE workspace_id = ? AND provider = 'tiktok' AND deleted_at IS NULL`,
    [reason, workspaceId]
  ));
}

function expiresAt(seconds) {
  if (!seconds || Number(seconds) <= 0) return null;
  return new Date(Date.now() + Number(seconds) * 1000);
}

async function completeTikTokConnection({ code, state }) {
  if (!code || !state) {
    throw createHttpError(400, 'invalid_callback');
  }

  let transaction;
  await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      transaction = await consumeOAuthTransaction(connection, state);
      if (!transaction) {
        throw createHttpError(400, 'invalid_or_expired_state');
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });

  const tokenResult = await tiktok.exchangeCode(code);
  if (
    !tokenResult.ok ||
    !tokenResult.payload ||
    typeof tokenResult.payload.access_token !== 'string' ||
    typeof tokenResult.payload.refresh_token !== 'string' ||
    typeof tokenResult.payload.open_id !== 'string' ||
    Number(tokenResult.payload.expires_in) <= 0 ||
    Number(tokenResult.payload.refresh_expires_in) <= 0
  ) {
    await markOAuthTransactionFailed(transaction.id, 'token_exchange_failed');
    await markConnectionFailed(transaction.workspace_id, 'token_exchange_failed');
    throw createHttpError(400, 'token_exchange_failed');
  }

  const profileResult = await tiktok.fetchProfile(tokenResult.payload.access_token);
  if (!profileResult.ok || !profileResult.user || !profileResult.user.open_id || profileResult.user.open_id !== tokenResult.payload.open_id) {
    await markOAuthTransactionFailed(transaction.id, 'profile_fetch_failed');
    await markConnectionFailed(transaction.workspace_id, 'profile_fetch_failed');
    throw createHttpError(400, 'profile_fetch_failed');
  }

  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const missing = tiktok.missingScopes(tokenResult.payload.scope);
      const sourceRows = await connection.query(
        `SELECT * FROM data_sources
         WHERE workspace_id = ? AND provider = 'tiktok' AND deleted_at IS NULL
         ORDER BY created_at ASC LIMIT 1`,
        [transaction.workspace_id]
      );
      const dataSource = sourceRows[0] || await findOrCreateTikTokSource(connection, transaction.workspace_id);
      const status = missing.length > 0 ? 'reconnect_required' : 'active';

      await connection.query(
        `UPDATE data_sources
         SET status = ?, reconnect_reason = ?, next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 6 HOUR), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [status, missing.length ? `missing_scopes:${missing.join(',')}` : null, dataSource.id]
      );

      await connection.query(
        `INSERT INTO provider_accounts
          (id, workspace_id, data_source_id, provider, provider_account_id, union_id, username, display_name, metadata)
         VALUES (?, ?, ?, 'tiktok', ?, ?, ?, ?, JSON_OBJECT('profile_deep_link', ?))
         ON DUPLICATE KEY UPDATE
          union_id = VALUES(union_id),
          username = VALUES(username),
          display_name = VALUES(display_name),
          metadata = VALUES(metadata),
          updated_at = UTC_TIMESTAMP(3)`,
        [
          createId(),
          transaction.workspace_id,
          dataSource.id,
          profileResult.user.open_id,
          profileResult.user.union_id || null,
          profileResult.user.username || null,
          profileResult.user.display_name || null,
          profileResult.user.profile_deep_link || null
        ]
      );

      const access = encryptSecret(tokenResult.payload.access_token);
      const refresh = encryptSecret(tokenResult.payload.refresh_token);
      await connection.query(
        `INSERT INTO oauth_credentials
          (id, data_source_id, access_token_ciphertext, access_token_iv, access_token_tag,
           refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version,
           token_type, access_expires_at, refresh_expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
          revoked_at = NULL,
          updated_at = UTC_TIMESTAMP(3)`,
        [
          createId(),
          dataSource.id,
          access.ciphertext,
          access.iv,
          access.tag,
          refresh.ciphertext,
          refresh.iv,
          refresh.tag,
          access.keyVersion,
          tokenResult.payload.token_type || 'Bearer',
          expiresAt(tokenResult.payload.expires_in),
          expiresAt(tokenResult.payload.refresh_expires_in)
        ]
      );

      await connection.query('DELETE FROM provider_scopes WHERE data_source_id = ?', [dataSource.id]);
      const granted = new Set(String(tokenResult.payload.scope || '').split(/[,\s]+/).filter(Boolean));
      for (const scope of tiktok.TIKTOK_SCOPES) {
        const isGranted = granted.has(scope);
        await connection.query(
          `INSERT INTO provider_scopes (data_source_id, scope, status, granted_at, last_confirmed_at)
           VALUES (?, ?, ?, CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE NULL END, UTC_TIMESTAMP(3))`,
          [dataSource.id, scope, isGranted ? 'granted' : 'missing', isGranted]
        );
      }

      if (missing.length === 0) {
        await connection.query(
          `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
           VALUES (?, ?, UTC_TIMESTAMP(3), 'due')
           ON DUPLICATE KEY UPDATE
             run_after = UTC_TIMESTAMP(3),
             status = 'due',
             lease_owner = NULL,
             lease_expires_at = NULL,
             updated_at = UTC_TIMESTAMP(3)`,
          [createId(), dataSource.id]
        );
      }

      await upsertTikTokProviderFoundation(connection, {
        workspaceId: transaction.workspace_id,
        actorUserId: transaction.initiated_by,
        dataSourceId: dataSource.id,
        profile: profileResult.user,
        status
      });

      await writeAuditLog(connection, {
        workspaceId: transaction.workspace_id,
        actorUserId: transaction.initiated_by,
        action: 'connection.tiktok.connected',
        targetType: 'data_source',
        targetId: dataSource.id,
        metadata: { missing_scopes: missing }
      });

      await connection.commit();
      return { connected: missing.length === 0, missing_scopes: missing, return_path: transaction.return_path };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function disconnectTikTok(userId, workspaceId) {
  return withConnection(async connection => {
    await requireWorkspaceRole(connection, workspaceId, userId, 'manageConnection');
    const rows = await connection.query(
      `SELECT ds.id AS data_source_id,
              oc.access_token_ciphertext,
              oc.access_token_iv,
              oc.access_token_tag,
              oc.key_version,
              oc.revoked_at
       FROM data_sources ds
       LEFT JOIN oauth_credentials oc ON oc.data_source_id = ds.id
       WHERE ds.workspace_id = ? AND ds.provider = 'tiktok' AND ds.deleted_at IS NULL
       LIMIT 1`,
      [workspaceId]
    );
    const record = rows[0] || null;
    if (!record) {
      return { disconnected: false, provider_revoke: { attempted: false, reason: 'not_connected' } };
    }

    let providerRevoke = { attempted: false, reason: 'credential_not_found' };
    if (record.access_token_ciphertext && !record.revoked_at) {
      const accessToken = decryptSecret({
        ciphertext: record.access_token_ciphertext,
        iv: record.access_token_iv,
        tag: record.access_token_tag,
        keyVersion: record.key_version
      });
      providerRevoke = await tiktok.revokeAccess(accessToken);
    }

    await connection.beginTransaction();
    try {
      await connection.query(
        `UPDATE oauth_credentials SET revoked_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [record.data_source_id]
      );
      await connection.query(
        `UPDATE data_sources
         SET status = 'disconnected', next_sync_at = NULL, reconnect_reason = NULL, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [record.data_source_id]
      );
      await connection.query(
        `UPDATE sync_jobs
         SET status = 'disabled', lease_owner = NULL, lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [record.data_source_id]
      );
      await connection.query('DELETE FROM provider_scopes WHERE data_source_id = ?', [record.data_source_id]);
      await markTikTokProviderFoundationDisconnected(connection, {
        dataSourceId: record.data_source_id,
        actorUserId: userId,
        providerRevoke
      });
      await writeAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'connection.tiktok.disconnected',
        targetType: 'data_source',
        targetId: record.data_source_id,
        metadata: {
          provider_revoke: providerRevoke && {
            attempted: providerRevoke.attempted,
            success: providerRevoke.success,
            status: providerRevoke.status,
            category: providerRevoke.error && providerRevoke.error.category
          }
        }
      });
      await connection.commit();
      return { disconnected: true, provider_revoke: providerRevoke };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

module.exports = {
  completeTikTokConnection,
  disconnectTikTok,
  startTikTokConnection
};
