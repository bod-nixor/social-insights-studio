const os = require('os');
const { getConnection } = require('../database');
const tiktok = require('../integrations/tiktok');
const { assertCapability } = require('./rbac');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId } = require('./security');
const { purgeOverdueYouTubeAuthorizations } = require('./youtube-connection-service');
const { performYouTubeSyncForJob } = require('./youtube-sync-service');
const { getYouTubeConfiguration } = require('./youtube-config');
const { performMetaSyncForJob } = require('./meta-sync-service');
const { getMetaConfiguration } = require('./meta-config');
const { purgeOverdueMetaAuthorizations } = require('./meta-connection-service');

const SYNC_INTERVAL_SECONDS = Number(process.env.SYNC_INTERVAL_SECONDS || 6 * 60 * 60);
const MANUAL_COOLDOWN_SECONDS = Number(process.env.MANUAL_SYNC_COOLDOWN_SECONDS || 15 * 60);
const LEASE_SECONDS = Number(process.env.SYNC_LEASE_SECONDS || 5 * 60);

function createHttpError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
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

async function requireWorkspaceCapability(connection, workspaceId, userId, capability) {
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

function syncOwner() {
  return `${os.hostname()}:${process.pid}:${Date.now()}`;
}

function secondsFromNow(seconds) {
  return new Date(Date.now() + seconds * 1000);
}

function nextScheduledRun() {
  const jitterSeconds = Math.floor(Math.random() * Number(process.env.SYNC_STAGGER_SECONDS || 15 * 60));
  return secondsFromNow(SYNC_INTERVAL_SECONDS + jitterSeconds);
}

function retryRunAfter(attempts) {
  const baseSeconds = Number(process.env.SYNC_RETRY_BASE_SECONDS || 60);
  const maxSeconds = Number(process.env.SYNC_RETRY_MAX_SECONDS || 60 * 60);
  const exponent = Math.min(Number(attempts || 1), 6);
  const backoff = Math.min(maxSeconds, baseSeconds * (2 ** (exponent - 1)));
  const jitter = Math.floor(Math.random() * Math.min(300, backoff));
  return secondsFromNow(backoff + jitter);
}

function normalizeMetric(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) return null;
  return Math.trunc(number);
}

function asProviderError(result, fallbackCode) {
  const error = new Error(fallbackCode);
  const provider = result && result.error ? result.error : {};
  error.code = fallbackCode;
  error.syncError = {
    category: provider.category || 'provider',
    provider_code: provider.provider_code || fallbackCode,
    retryable: provider.retryable !== false,
    message: fallbackCode
  };
  return error;
}

function internalSyncError(error) {
  return {
    category: error.syncError && error.syncError.category ? error.syncError.category : 'internal',
    provider_code: error.syncError && error.syncError.provider_code ? error.syncError.provider_code : null,
    retryable: error.syncError ? error.syncError.retryable !== false : false,
    message: error.code || error.message || 'sync_failed'
  };
}

async function getActiveSource(connection, workspaceId, provider = 'tiktok', connectionId = null) {
  const params = [workspaceId, provider];
  const connectionJoin = connectionId
    ? 'JOIN workspace_provider_connections wpc ON wpc.data_source_id = data_sources.id'
    : '';
  const connectionClause = connectionId ? 'AND wpc.id = ?' : '';
  if (connectionId) params.push(connectionId);
  const rows = await connection.query(
    `SELECT data_sources.* FROM data_sources
     ${connectionJoin}
     WHERE data_sources.workspace_id = ? AND data_sources.provider = ?
       AND data_sources.deleted_at IS NULL ${connectionClause}
     ORDER BY data_sources.created_at ASC LIMIT 1`,
    params
  );
  return rows[0] || null;
}

async function loadSourceCredentials(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT ds.*, oc.access_token_ciphertext, oc.access_token_iv, oc.access_token_tag,
            oc.refresh_token_ciphertext, oc.refresh_token_iv, oc.refresh_token_tag,
            oc.key_version, oc.token_type, oc.access_expires_at, oc.refresh_expires_at, oc.revoked_at
     FROM data_sources ds
     JOIN oauth_credentials oc ON oc.data_source_id = ds.id
     WHERE ds.id = ? AND ds.provider = 'tiktok' AND ds.deleted_at IS NULL
     LIMIT 1`,
    [dataSourceId]
  );
  return rows[0] || null;
}

async function claimJob(connection, dataSourceId, owner, ignoreRunAfter = false) {
  await connection.query(
    `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
     VALUES (?, ?, UTC_TIMESTAMP(3), 'due')
     ON DUPLICATE KEY UPDATE updated_at = UTC_TIMESTAMP(3)`,
    [createId(), dataSourceId]
  );
  const runAfterClause = ignoreRunAfter ? '' : 'AND run_after <= UTC_TIMESTAMP(3)';
  const statusClause = ignoreRunAfter
    ? "(status IN ('due', 'paused') OR (status = 'leased' AND lease_expires_at < UTC_TIMESTAMP(3)))"
    : "(status = 'due' OR (status = 'leased' AND lease_expires_at < UTC_TIMESTAMP(3)))";
  const result = await connection.query(
    `UPDATE sync_jobs
     SET status = 'leased',
         lease_owner = ?,
         lease_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
         attempts = attempts + 1,
         updated_at = UTC_TIMESTAMP(3)
     WHERE data_source_id = ?
       ${runAfterClause}
       AND ${statusClause}`,
    [owner, LEASE_SECONDS, dataSourceId]
  );
  if (Number(result.affectedRows || 0) !== 1) return null;
  const rows = await connection.query('SELECT * FROM sync_jobs WHERE data_source_id = ? LIMIT 1', [dataSourceId]);
  return rows[0] || null;
}

async function claimNextDueJob(owner) {
  return withConnection(async connection => {
    const candidates = await connection.query(
      `SELECT j.*, ds.workspace_id
       FROM sync_jobs j
       JOIN data_sources ds ON ds.id = j.data_source_id
       WHERE ds.status = 'active'
         AND ds.deleted_at IS NULL
         AND j.run_after <= UTC_TIMESTAMP(3)
         AND (j.status = 'due' OR (j.status = 'leased' AND j.lease_expires_at < UTC_TIMESTAMP(3)))
       ORDER BY j.run_after ASC
       LIMIT 5`
    );
    for (const candidate of candidates) {
      const claimed = await claimJob(connection, candidate.data_source_id, owner, false);
      if (claimed) return claimed;
    }
    return null;
  });
}

async function finishJob(connection, job, status, syncError = null) {
  const retryable = syncError && syncError.retryable !== false;
  const nonRetryableAuth = syncError && ['authentication', 'scope'].includes(syncError.category);
  const runAfter = status === 'success' ? nextScheduledRun() : retryRunAfter(job.attempts);
  await connection.query(
    `UPDATE sync_jobs
     SET status = ?,
         run_after = ?,
         lease_owner = NULL,
         lease_expires_at = NULL,
         requested_trigger_type = 'scheduled',
         updated_at = UTC_TIMESTAMP(3)
     WHERE data_source_id = ?`,
    [nonRetryableAuth && !retryable ? 'paused' : 'due', runAfter, job.data_source_id]
  );
}

function tokenStillFresh(expiresAt) {
  if (!expiresAt) return false;
  return new Date(expiresAt).getTime() > Date.now() + 60 * 1000;
}

async function refreshCredentialsIfNeeded(connection, credentials) {
  const accessToken = decryptSecret({
    ciphertext: credentials.access_token_ciphertext,
    iv: credentials.access_token_iv,
    tag: credentials.access_token_tag,
    keyVersion: credentials.key_version
  });
  if (tokenStillFresh(credentials.access_expires_at)) {
    return accessToken;
  }

  const refreshToken = decryptSecret({
    ciphertext: credentials.refresh_token_ciphertext,
    iv: credentials.refresh_token_iv,
    tag: credentials.refresh_token_tag,
    keyVersion: credentials.key_version
  });
  const refreshed = await tiktok.refreshAccessToken(refreshToken);
  if (!refreshed.ok || !refreshed.payload || !refreshed.payload.access_token || Number(refreshed.payload.expires_in) <= 0) {
    throw asProviderError(refreshed, 'credential_refresh_failed');
  }

  const access = encryptSecret(refreshed.payload.access_token);
  const refresh = encryptSecret(refreshed.payload.refresh_token || refreshToken);
  await connection.query(
    `UPDATE oauth_credentials
     SET access_token_ciphertext = ?,
         access_token_iv = ?,
         access_token_tag = ?,
         refresh_token_ciphertext = ?,
         refresh_token_iv = ?,
         refresh_token_tag = ?,
         key_version = ?,
         access_expires_at = ?,
         refresh_expires_at = COALESCE(?, refresh_expires_at),
         updated_at = UTC_TIMESTAMP(3)
     WHERE data_source_id = ?`,
    [
      access.ciphertext,
      access.iv,
      access.tag,
      refresh.ciphertext,
      refresh.iv,
      refresh.tag,
      access.keyVersion,
      secondsFromNow(Number(refreshed.payload.expires_in)),
      refreshed.payload.refresh_expires_in ? secondsFromNow(Number(refreshed.payload.refresh_expires_in)) : null,
      credentials.id
    ]
  );
  return refreshed.payload.access_token;
}

async function createSyncRun(connection, source, triggerType, correlationId) {
  const id = createId();
  await connection.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, trigger_type, status, correlation_id)
     VALUES (?, ?, ?, ?, 'running', ?)`,
    [id, source.workspace_id, source.id, triggerType, correlationId || null]
  );
  return id;
}

async function recordSyncError(connection, runId, syncError) {
  await connection.query(
    `INSERT INTO sync_errors
      (id, sync_run_id, category, provider_code, message, retryable)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      runId,
      syncError.category,
      syncError.provider_code || null,
      String(syncError.message || syncError.category).slice(0, 512),
      Boolean(syncError.retryable)
    ]
  );
}

function normalizeVideo(video) {
  const publishedAt = video.create_time ? new Date(Number(video.create_time) * 1000) : null;
  return {
    provider_content_id: String(video.id || ''),
    published_at: publishedAt && Number.isFinite(publishedAt.getTime()) ? publishedAt : null,
    title: (video.title || video.video_description || '').slice(0, 512) || null,
    description: video.video_description || video.title || null,
    share_url: video.share_url || null,
    duration_seconds: normalizeMetric(video.duration),
    height: normalizeMetric(video.height),
    width: normalizeMetric(video.width),
    view_count: normalizeMetric(video.view_count),
    like_count: normalizeMetric(video.like_count),
    comment_count: normalizeMetric(video.comment_count),
    share_count: normalizeMetric(video.share_count)
  };
}

async function fetchAllVideos(accessToken) {
  const videos = [];
  let cursor = 0;
  const maxPages = Number(process.env.TIKTOK_MAX_VIDEO_PAGES || 20);
  for (let page = 0; page < maxPages; page += 1) {
    const result = await tiktok.fetchVideosPage(accessToken, cursor);
    if (!result.ok) throw asProviderError(result, 'content_fetch_failed');
    for (const video of result.videos) {
      if (video && video.id) videos.push(normalizeVideo(video));
    }
    if (!result.has_more) break;
    if (result.cursor === cursor) break;
    cursor = result.cursor;
  }
  return videos;
}

async function insertProfileSnapshot(connection, source, runId, profile) {
  await connection.query(
    `INSERT INTO profile_snapshots
      (id, workspace_id, data_source_id, sync_run_id, observed_at,
       follower_count, following_count, likes_count, video_count, provider_metrics)
     VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?)`,
    [
      createId(),
      source.workspace_id,
      source.id,
      runId,
      normalizeMetric(profile.follower_count),
      normalizeMetric(profile.following_count),
      normalizeMetric(profile.likes_count),
      normalizeMetric(profile.video_count),
      JSON.stringify({ source: 'tiktok' })
    ]
  );
}

async function upsertContentSnapshot(connection, source, runId, video) {
  await connection.query(
    `INSERT INTO content_items
      (id, workspace_id, data_source_id, provider_content_id, published_at, title, description,
       share_url, duration_seconds, height, width, provider_metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       published_at = VALUES(published_at),
       title = VALUES(title),
       description = VALUES(description),
       share_url = VALUES(share_url),
       duration_seconds = VALUES(duration_seconds),
       height = VALUES(height),
       width = VALUES(width),
       provider_metadata = VALUES(provider_metadata),
       last_seen_at = UTC_TIMESTAMP(3)`,
    [
      createId(),
      source.workspace_id,
      source.id,
      video.provider_content_id,
      video.published_at,
      video.title,
      video.description,
      video.share_url,
      video.duration_seconds,
      video.height,
      video.width,
      JSON.stringify({ source: 'tiktok', thumbnail_ephemeral: true })
    ]
  );
  const rows = await connection.query(
    `SELECT id FROM content_items
     WHERE data_source_id = ? AND provider_content_id = ?
     LIMIT 1`,
    [source.id, video.provider_content_id]
  );
  const item = rows[0];
  await connection.query(
    `INSERT INTO content_metric_snapshots
      (id, workspace_id, content_item_id, sync_run_id, observed_at,
       view_count, like_count, comment_count, share_count, provider_metrics)
     VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?)`,
    [
      createId(),
      source.workspace_id,
      item.id,
      runId,
      video.view_count,
      video.like_count,
      video.comment_count,
      video.share_count,
      JSON.stringify({ source: 'tiktok' })
    ]
  );
}

async function finishSyncRun(connection, runId, status, startedMs, counts, syncError = null) {
  await connection.query(
    `UPDATE sync_runs
     SET status = ?,
         finished_at = UTC_TIMESTAMP(3),
         duration_ms = ?,
         profile_count = ?,
         content_seen_count = ?,
         content_snapshot_count = ?
     WHERE id = ?`,
    [
      status,
      Math.max(0, Date.now() - startedMs),
      counts.profile_count || 0,
      counts.content_seen_count || 0,
      counts.content_snapshot_count || 0,
      runId
    ]
  );
  if (syncError) {
    await recordSyncError(connection, runId, syncError);
  }
}

async function updateSourceAfterSync(connection, source, status, syncError = null) {
  const nextRun = status === 'success' ? nextScheduledRun() : retryRunAfter(1);
  const reconnect = syncError && ['authentication', 'scope'].includes(syncError.category);
  await connection.query(
    `UPDATE data_sources
     SET status = ?,
         reconnect_reason = ?,
         last_sync_at = UTC_TIMESTAMP(3),
         last_successful_sync_at = CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE last_successful_sync_at END,
         next_sync_at = ?,
         updated_at = UTC_TIMESTAMP(3)
     WHERE id = ?`,
    [
      reconnect ? 'reconnect_required' : 'active',
      syncError ? `${syncError.category}:${syncError.message}`.slice(0, 255) : null,
      status === 'success',
      nextRun,
      source.id
    ]
  );
  return nextRun;
}

async function performTikTokSyncForJob(job, options = {}) {
  const triggerType = options.triggerType || 'scheduled';
  const startedMs = Date.now();
  let runId = null;
  let source = null;
  let syncError = null;
  let finalStatus = 'failed';
  let counts = { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 };

  try {
    await withConnection(async connection => {
      const credentials = await loadSourceCredentials(connection, job.data_source_id);
      if (!credentials || credentials.status !== 'active' || credentials.revoked_at) {
        throw createHttpError(400, 'source_not_syncable');
      }
      source = credentials;
      runId = await createSyncRun(connection, source, triggerType, options.correlationId);
    });

    const accessToken = await withConnection(connection => refreshCredentialsIfNeeded(connection, source));
    const profileResult = await tiktok.fetchProfile(accessToken);
    if (!profileResult.ok || !profileResult.user) {
      throw asProviderError(profileResult, 'profile_fetch_failed');
    }

    let videos = [];
    try {
      videos = await fetchAllVideos(accessToken);
      finalStatus = 'success';
    } catch (error) {
      syncError = internalSyncError(error);
      finalStatus = 'partial';
    }

    await withConnection(async connection => {
      await connection.beginTransaction();
      try {
        await insertProfileSnapshot(connection, source, runId, profileResult.user);
        counts.profile_count = 1;
        for (const video of videos) {
          await upsertContentSnapshot(connection, source, runId, video);
        }
        counts.content_seen_count = videos.length;
        counts.content_snapshot_count = videos.length;
        await finishSyncRun(connection, runId, finalStatus, startedMs, counts, syncError);
        const nextRun = await updateSourceAfterSync(connection, source, finalStatus, syncError);
        await finishJob(connection, job, finalStatus, syncError);
        await connection.commit();
        source.next_sync_at = nextRun;
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
  } catch (error) {
    syncError = internalSyncError(error);
    finalStatus = 'failed';
    if (runId && source) {
      await withConnection(async connection => {
        await connection.beginTransaction();
        try {
          await finishSyncRun(connection, runId, finalStatus, startedMs, counts, syncError);
          await updateSourceAfterSync(connection, source, finalStatus, syncError);
          await finishJob(connection, job, finalStatus, syncError);
          await connection.commit();
        } catch (finishError) {
          await connection.rollback();
          throw finishError;
        }
      });
    } else {
      await withConnection(connection => finishJob(connection, job, finalStatus, syncError));
    }
  }

  return {
    data_source_id: job.data_source_id,
    sync_run_id: runId,
    status: finalStatus,
    error: syncError,
    counts
  };
}

async function providerForJob(dataSourceId) {
  return withConnection(async connection => {
    const rows = await connection.query('SELECT provider FROM data_sources WHERE id = ? LIMIT 1', [dataSourceId]);
    return rows[0] ? rows[0].provider : null;
  });
}

async function providerAuthorizationIsActive(connection, dataSourceId, provider) {
  const rows = await connection.query(
    `SELECT pauth.status
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     WHERE wpc.data_source_id = ? AND wpc.provider = ?
     LIMIT 1`,
    [dataSourceId, provider]
  );
  return Boolean(rows[0] && rows[0].status === 'active');
}

async function performSyncForJob(job, options = {}) {
  const provider = await providerForJob(job.data_source_id);
  if (provider === 'youtube') return performYouTubeSyncForJob(job, options);
  if (provider === 'facebook_pages' || provider === 'instagram') return performMetaSyncForJob(job, options);
  return performTikTokSyncForJob(job, options);
}

async function requestManualSync(userId, workspaceId, options = {}) {
  const provider = options.provider || 'tiktok';
  if (provider === 'youtube' && !getYouTubeConfiguration().connectable) {
    throw createHttpError(503, 'youtube_not_available');
  }
  if ((provider === 'facebook_pages' || provider === 'instagram') && !getMetaConfiguration(provider).connectable) {
    throw createHttpError(503, `${provider}_not_available`);
  }
  if (provider === 'youtube' || provider === 'facebook_pages' || provider === 'instagram') {
    return withConnection(async connection => {
      await connection.beginTransaction();
      try {
        await requireWorkspaceCapability(connection, workspaceId, userId, 'triggerManualSync');
        const source = await getActiveSource(connection, workspaceId, provider, options.connectionId || null);
        if (!source || source.status !== 'active') throw createHttpError(400, `${provider}_not_connected`);
        if (!(await providerAuthorizationIsActive(connection, source.id, provider))) {
          throw createHttpError(400, `${provider}_not_connected`);
        }
        const recent = await connection.query(
          `SELECT id FROM sync_runs
           WHERE workspace_id = ? AND data_source_id = ? AND trigger_type = 'manual'
             AND started_at > DATE_SUB(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
           LIMIT 1`,
          [workspaceId, source.id, MANUAL_COOLDOWN_SECONDS]
        );
        if (recent[0]) throw createHttpError(429, 'manual_sync_cooldown');
        const jobRows = await connection.query(
          `SELECT status, lease_expires_at FROM sync_jobs
           WHERE data_source_id = ? LIMIT 1 FOR UPDATE`,
          [source.id]
        );
        const job = jobRows[0] || null;
        if (job && job.status === 'leased' && job.lease_expires_at && new Date(job.lease_expires_at) > new Date()) {
          throw createHttpError(409, 'sync_already_running');
        }
        await connection.query(
          `INSERT INTO sync_jobs
            (id, data_source_id, run_after, status, requested_trigger_type)
           VALUES (?, ?, UTC_TIMESTAMP(3), 'due', 'manual')
           ON DUPLICATE KEY UPDATE
             run_after = UTC_TIMESTAMP(3), status = 'due', requested_trigger_type = 'manual',
             lease_owner = NULL, lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)`,
          [createId(), source.id]
        );
        await connection.commit();
        return { status: 'queued', data_source_id: source.id };
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
  }
  const owner = syncOwner();
  let job;
  await withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'triggerManualSync');
    const source = await getActiveSource(connection, workspaceId, provider, options.connectionId || null);
    if (!source || source.status !== 'active') throw createHttpError(400, `${provider}_not_connected`);
    const recent = await connection.query(
      `SELECT id FROM sync_runs
       WHERE workspace_id = ? AND data_source_id = ? AND trigger_type = 'manual'
         AND started_at > DATE_SUB(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
       LIMIT 1`,
      [workspaceId, source.id, MANUAL_COOLDOWN_SECONDS]
    );
    if (recent[0]) throw createHttpError(429, 'manual_sync_cooldown');
    job = await claimJob(connection, source.id, owner, true);
    if (!job) throw createHttpError(409, 'sync_already_running');
  });
  return performSyncForJob(job, { triggerType: 'manual', correlationId: owner });
}

async function runDueSyncs(options = {}) {
  const owner = options.leaseOwner || syncOwner();
  const budgetMs = Number(options.timeBudgetSeconds || 240) * 1000;
  const deadline = Date.now() + budgetMs;
  const results = [];
  const reconciliation = await purgeOverdueYouTubeAuthorizations(50);
  const metaReconciliation = await purgeOverdueMetaAuthorizations(50);
  while (Date.now() < deadline) {
    const job = await claimNextDueJob(owner);
    if (!job) break;
    results.push(await performSyncForJob(job, {
      triggerType: job.requested_trigger_type === 'manual' ? 'manual' : 'scheduled',
      correlationId: owner,
      deadlineMs: deadline
    }));
  }
  return {
    lease_owner: owner,
    processed: results.length,
    reconciled_youtube_authorizations: reconciliation.purged,
    reconciled_meta_authorizations: metaReconciliation.purged,
    results
  };
}

module.exports = {
  MANUAL_COOLDOWN_SECONDS,
  performSyncForJob,
  requestManualSync,
  runDueSyncs
};
