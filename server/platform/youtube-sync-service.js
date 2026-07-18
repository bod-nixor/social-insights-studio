const { getConnection } = require('../database');
const youtube = require('../integrations/youtube');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId } = require('./security');
const { purgeYouTubeAuthorizationBySystem } = require('./youtube-connection-service');
const {
  YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS,
  getYouTubeConfiguration,
  getYouTubeLimits
} = require('./youtube-config');

const DEFAULT_SYNC_INTERVAL_SECONDS = 6 * 60 * 60;

function retryDelaySeconds(syncError, syncIntervalSeconds = Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS)) {
  if (!syncError) return 300;
  if (!syncError.retryable) return Math.max(60, Number(syncIntervalSeconds) || DEFAULT_SYNC_INTERVAL_SECONDS);
  return Math.max(60, Number(syncError.retry_after_seconds) || 300);
}

function createSyncError(code, result = null) {
  const provider = result && result.error ? result.error : {};
  const error = new Error(code);
  error.code = code;
  error.syncError = {
    category: provider.category || 'provider',
    provider_code: provider.provider_code || code,
    retryable: provider.retryable === true,
    terminal: provider.terminal === true,
    retry_after_seconds: result && result.retryAfterSeconds !== null ? result.retryAfterSeconds : null,
    message: code
  };
  return error;
}

function createTimeBudgetError() {
  const error = createSyncError('youtube_time_budget_exhausted');
  error.syncError.category = 'timeout';
  error.syncError.provider_code = 'youtube_time_budget_exhausted';
  error.syncError.retryable = true;
  return error;
}

function internalSyncError(error) {
  const source = error && error.syncError ? error.syncError : {};
  return {
    category: source.category || 'internal',
    provider_code: source.provider_code || null,
    retryable: source.retryable === true,
    terminal: source.terminal === true,
    retry_after_seconds: source.retry_after_seconds === undefined ? null : source.retry_after_seconds,
    message: (error && (error.code || error.message)) || 'youtube_sync_failed'
  };
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) {
    const error = new Error('database_not_configured');
    error.code = 'database_not_configured';
    throw error;
  }
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
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

function metricOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function integerOrNull(value) {
  const parsed = metricOrNull(value);
  return parsed === null ? null : Math.trunc(parsed);
}

function utcDate(offsetDays = 0) {
  const value = new Date();
  value.setUTCHours(0, 0, 0, 0);
  value.setUTCDate(value.getUTCDate() + offsetDays);
  return value.toISOString().slice(0, 10);
}

function parseIsoDuration(value) {
  const match = String(value || '').match(/^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/);
  if (!match) return null;
  const seconds = Number(match[1] || 0) * 86400 + Number(match[2] || 0) * 3600 + Number(match[3] || 0) * 60 + Number(match[4] || 0);
  return Number.isFinite(seconds) ? Math.round(seconds) : null;
}

function thumbnailUrl(snippet) {
  const thumbnails = snippet && snippet.thumbnails ? snippet.thumbnails : {};
  return (thumbnails.high && thumbnails.high.url) ||
    (thumbnails.medium && thumbnails.medium.url) ||
    (thumbnails.default && thumbnails.default.url) ||
    null;
}

function normalizeChannel(item) {
  if (!item || typeof item.id !== 'string') throw createSyncError('youtube_channel_response_malformed');
  const snippet = item.snippet || {};
  const statistics = item.statistics || {};
  const playlists = item.contentDetails && item.contentDetails.relatedPlaylists
    ? item.contentDetails.relatedPlaylists
    : {};
  const subscriberHidden = Boolean(statistics.hiddenSubscriberCount);
  return {
    id: item.id,
    title: String(snippet.title || 'YouTube channel').slice(0, 255),
    customUrl: snippet.customUrl ? String(snippet.customUrl) : null,
    thumbnailUrl: thumbnailUrl(snippet),
    uploadsPlaylistId: playlists.uploads ? String(playlists.uploads) : null,
    subscriberCount: subscriberHidden ? null : integerOrNull(statistics.subscriberCount),
    subscriberHidden,
    lifetimeViewCount: integerOrNull(statistics.viewCount),
    publicVideoCount: integerOrNull(statistics.videoCount),
    availability: {
      subscriber_count: subscriberHidden ? 'hidden_by_channel' : statistics.subscriberCount === undefined ? 'unavailable' : 'available',
      lifetime_view_count: statistics.viewCount === undefined ? 'unavailable' : 'available',
      public_video_count: statistics.videoCount === undefined ? 'unavailable' : 'available',
      uploads_playlist: playlists.uploads ? 'available' : 'unavailable'
    }
  };
}

function normalizePlaylistItem(item) {
  const contentDetails = item && item.contentDetails ? item.contentDetails : {};
  const snippet = item && item.snippet ? item.snippet : {};
  const status = item && item.status ? item.status : {};
  const videoId = contentDetails.videoId || (snippet.resourceId && snippet.resourceId.videoId);
  if (!videoId) return null;
  const title = String(snippet.title || '');
  const unavailableReason = title === 'Deleted video'
    ? 'deleted_video'
    : title === 'Private video' || status.privacyStatus === 'private'
      ? 'private_video'
      : null;
  return {
    videoId: String(videoId),
    title: unavailableReason ? null : title.slice(0, 512) || null,
    description: unavailableReason ? null : snippet.description || null,
    publishedAt: contentDetails.videoPublishedAt || snippet.publishedAt || null,
    thumbnailUrl: unavailableReason ? null : thumbnailUrl(snippet),
    unavailableReason
  };
}

function normalizeVideo(item, playlistItem) {
  const snippet = item && item.snippet ? item.snippet : {};
  const statistics = item && item.statistics ? item.statistics : {};
  const contentDetails = item && item.contentDetails ? item.contentDetails : {};
  const live = item && item.liveStreamingDetails ? item.liveStreamingDetails : null;
  const id = item && item.id ? String(item.id) : playlistItem.videoId;
  return {
    providerContentId: id,
    publishedAt: snippet.publishedAt || playlistItem.publishedAt || null,
    title: String(snippet.title || playlistItem.title || '').slice(0, 512) || null,
    description: snippet.description || playlistItem.description || null,
    shareUrl: `https://www.youtube.com/watch?v=${encodeURIComponent(id)}`,
    durationSeconds: parseIsoDuration(contentDetails.duration),
    thumbnailUrl: thumbnailUrl(snippet) || playlistItem.thumbnailUrl,
    contentType: live ? 'live_video' : 'video',
    privacyStatus: item && item.status ? item.status.privacyStatus || null : null,
    viewCount: integerOrNull(statistics.viewCount),
    likeCount: integerOrNull(statistics.likeCount),
    commentCount: integerOrNull(statistics.commentCount),
    shareCount: null,
    availability: {
      video: item ? 'available' : playlistItem.unavailableReason || 'unavailable',
      views: statistics.viewCount === undefined ? 'unavailable' : 'available',
      likes: statistics.likeCount === undefined ? 'unavailable' : 'available',
      comments: statistics.commentCount === undefined ? 'unavailable' : 'available',
      shares: 'not_available_from_data_api',
      content_type: live ? 'available' : 'generic_video_only'
    }
  };
}

async function loadSource(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT ds.*,
            wpc.id AS workspace_provider_connection_id,
            pr.provider_resource_id AS channel_id,
            pr.metadata AS resource_metadata,
            pauth.id AS provider_authorization_id,
            pauth.status AS authorization_status,
            pauth.last_validated_at, pauth.deletion_due_at,
            pauth.deletion_due_at IS NOT NULL
              AND pauth.deletion_due_at <= UTC_TIMESTAMP(3) AS validation_overdue,
            pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag,
            pac.refresh_token_ciphertext, pac.refresh_token_iv, pac.refresh_token_tag,
            pac.key_version, pac.access_expires_at, pac.refresh_expires_at, pac.revoked_at,
            pac.access_expires_at > DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 60 SECOND) AS access_token_fresh
     FROM data_sources ds
     JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     WHERE ds.id = ? AND ds.provider = 'youtube' AND ds.deleted_at IS NULL
     LIMIT 1`,
    [dataSourceId]
  );
  return rows[0] || null;
}

function tokenFresh(source) {
  return Number(source.access_token_fresh) === 1;
}

async function recordRequestEvent(source, runId, details) {
  return withConnection(connection => connection.query(
    `INSERT INTO provider_request_events
      (id, workspace_id, provider_authorization_id, workspace_provider_connection_id,
       sync_run_id, provider, request_category, method_name, quota_cost_estimate,
       page_number, item_count, attempts, status, failure_category, retry_after_seconds)
     VALUES (?, ?, ?, ?, ?, 'youtube', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      source.workspace_id,
      source.provider_authorization_id,
      source.workspace_provider_connection_id,
      runId,
      details.category,
      details.method,
      details.quotaCost || 0,
      details.pageNumber || null,
      details.itemCount === undefined ? null : details.itemCount,
      details.result && Number.isInteger(details.result.attempts) ? details.result.attempts : 1,
      details.status,
      details.result && details.result.error ? details.result.error.category : null,
      details.result ? details.result.retryAfterSeconds : null
    ]
  ));
}

async function callAndRecord(source, runId, details, fn) {
  const result = await fn();
  const items = result && result.body && Array.isArray(result.body.items) ? result.body.items.length : undefined;
  await recordRequestEvent(source, runId, {
    ...details,
    result,
    itemCount: details.itemCount === undefined ? items : details.itemCount,
    status: result.ok ? (items === 0 ? 'empty' : 'success') : 'failed'
  });
  if (!result.ok) throw createSyncError(`${details.method}_failed`, result);
  return result;
}

async function refreshCredentialsIfNeeded(source, runId, deadlineMs) {
  const accessToken = decryptSecret({
    ciphertext: source.access_token_ciphertext,
    iv: source.access_token_iv,
    tag: source.access_token_tag,
    keyVersion: source.key_version
  });
  if (tokenFresh(source)) return accessToken;
  if (!source.refresh_token_ciphertext) throw createSyncError('youtube_refresh_token_missing');
  const refreshToken = decryptSecret({
    ciphertext: source.refresh_token_ciphertext,
    iv: source.refresh_token_iv,
    tag: source.refresh_token_tag,
    keyVersion: source.key_version
  });
  const result = await youtube.refreshAccessToken(refreshToken, { deadlineMs });
  await recordRequestEvent(source, runId, {
    category: 'oauth',
    method: 'oauth.refresh',
    quotaCost: 0,
    result,
    status: result.ok ? 'success' : 'failed'
  });
  if (!result.ok || !result.body || !result.body.access_token || Number(result.body.expires_in) <= 0) {
    throw createSyncError('youtube_credential_refresh_failed', result);
  }
  const nextRefreshToken = youtube.chooseRefreshToken(result.body.refresh_token, refreshToken);
  const refreshTokenRotated = Boolean(
    typeof result.body.refresh_token === 'string' && result.body.refresh_token.trim()
  );
  const refreshTtlSeconds = result.body.refresh_token_expires_in
    ? Number(result.body.refresh_token_expires_in)
    : null;
  const access = encryptSecret(result.body.access_token);
  const refresh = encryptSecret(nextRefreshToken);
  await withConnection(connection => connection.query(
    `UPDATE provider_authorization_credentials
     SET access_token_ciphertext = ?, access_token_iv = ?, access_token_tag = ?,
         refresh_token_ciphertext = ?, refresh_token_iv = ?, refresh_token_tag = ?,
         key_version = ?, token_type = ?,
         access_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
         refresh_expires_at = CASE
           WHEN ? IS NOT NULL THEN DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
           WHEN ? = 1 THEN NULL
           ELSE refresh_expires_at
         END,
         updated_at = UTC_TIMESTAMP(3)
     WHERE provider_authorization_id = ?`,
    [
      access.ciphertext,
      access.iv,
      access.tag,
      refresh.ciphertext,
      refresh.iv,
      refresh.tag,
      access.keyVersion,
      result.body.token_type || 'Bearer',
      Number(result.body.expires_in),
      refreshTtlSeconds,
      refreshTtlSeconds,
      refreshTokenRotated,
      source.provider_authorization_id
    ]
  ));
  return result.body.access_token;
}

async function createSyncRun(source, triggerType, correlationId) {
  const runId = createId();
  await withConnection(connection => connection.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, trigger_type, status, correlation_id)
     VALUES (?, ?, ?, ?, 'running', ?)`,
    [runId, source.workspace_id, source.id, triggerType, correlationId || null]
  ));
  return runId;
}

async function loadUploadsCursor(source) {
  return withConnection(async connection => {
    const rows = await connection.query(
      `SELECT cursor_state FROM provider_sync_states
       WHERE workspace_provider_connection_id = ? AND sync_key = 'youtube.uploads'
       LIMIT 1`,
      [source.workspace_provider_connection_id]
    );
    return parseJson(rows[0] && rows[0].cursor_state, {});
  });
}

async function fetchUploads(source, runId, accessToken, channel, deadlineMs, limits) {
  if (!channel.uploadsPlaylistId) {
    return { videos: [], nextPageToken: null, pageNumber: 0, complete: true, reason: 'uploads_playlist_unavailable' };
  }
  const cursor = await loadUploadsCursor(source);
  let pageToken = cursor.next_page_token || null;
  let pageNumber = Number(cursor.page_number || 0);
  let pagesThisRun = 0;
  const playlistItems = [];
  const seenVideoIds = new Set();
  let complete = false;
  let reason = null;

  while (pagesThisRun < limits.maxPlaylistPages && playlistItems.length < limits.maxVideos) {
    if (Date.now() >= deadlineMs) {
      reason = 'time_budget';
      break;
    }
    const result = await callAndRecord(source, runId, {
      category: 'data_api',
      method: 'playlistItems.list',
      quotaCost: 1,
      pageNumber: pageNumber + 1
    }, () => youtube.listUploadItems(accessToken, channel.uploadsPlaylistId, pageToken, { deadlineMs }));
    if (!result.body || !Array.isArray(result.body.items)) throw createSyncError('youtube_playlist_response_malformed');
    for (const item of result.body.items) {
      const normalized = normalizePlaylistItem(item);
      if (
        normalized &&
        !seenVideoIds.has(normalized.videoId) &&
        playlistItems.length < limits.maxVideos
      ) {
        seenVideoIds.add(normalized.videoId);
        playlistItems.push(normalized);
      }
    }
    pagesThisRun += 1;
    pageNumber += 1;
    const next = result.body.nextPageToken ? String(result.body.nextPageToken) : null;
    if (!next) {
      complete = true;
      pageToken = null;
      pageNumber = 0;
      break;
    }
    if (next === pageToken) {
      reason = 'pagination_stalled';
      break;
    }
    pageToken = next;
  }
  if (!complete && !reason) reason = playlistItems.length >= limits.maxVideos ? 'video_limit' : 'page_limit';

  const videosById = new Map();
  let videoBatchInterrupted = false;
  for (let offset = 0; offset < playlistItems.length; offset += 50) {
    if (Date.now() >= deadlineMs) {
      complete = false;
      reason = 'time_budget';
      videoBatchInterrupted = true;
      break;
    }
    const batch = playlistItems.slice(offset, offset + 50);
    const result = await callAndRecord(source, runId, {
      category: 'data_api',
      method: 'videos.list',
      quotaCost: 1,
      itemCount: batch.length
    }, () => youtube.listVideos(accessToken, batch.map(item => item.videoId), { deadlineMs }));
    if (!result.body || !Array.isArray(result.body.items)) throw createSyncError('youtube_videos_response_malformed');
    for (const item of result.body.items) {
      if (item && item.id) videosById.set(String(item.id), item);
    }
  }
  return {
    videos: playlistItems.map(item => normalizeVideo(videosById.get(item.videoId) || null, item)),
    nextPageToken: videoBatchInterrupted ? cursor.next_page_token || null : pageToken,
    pageNumber: videoBatchInterrupted ? Number(cursor.page_number || 0) : pageNumber,
    complete,
    reason
  };
}

function analyticsRows(body, dimension) {
  if (!body || !Array.isArray(body.columnHeaders) || !Array.isArray(body.rows)) {
    throw createSyncError('youtube_analytics_response_malformed');
  }
  const columns = body.columnHeaders.map(header => header && header.name).filter(Boolean);
  if (!columns.includes(dimension)) throw createSyncError('youtube_analytics_dimension_missing');
  return body.rows.map(row => {
    if (!Array.isArray(row)) throw createSyncError('youtube_analytics_row_malformed');
    const record = {};
    columns.forEach((column, index) => {
      record[column] = row[index] === undefined ? null : row[index];
    });
    return record;
  });
}

async function fetchAnalytics(source, runId, accessToken, deadlineMs, limits) {
  if (Date.now() >= deadlineMs) throw createTimeBudgetError();
  const endDate = utcDate(-1);
  const startDate = utcDate(-limits.analyticsLookbackDays);
  const dailyResult = await callAndRecord(source, runId, {
    category: 'analytics_api',
    method: 'reports.query.daily',
    quotaCost: 0
  }, () => youtube.queryAnalytics(accessToken, {
    channelId: source.channel_id,
    startDate,
    endDate,
    dimensions: 'day',
    sort: 'day'
  }, { deadlineMs }));
  const daily = analyticsRows(dailyResult.body, 'day');
  const throughDate = daily.length > 0
    ? daily.map(row => String(row.day)).sort().at(-1)
    : null;
  if (daily.length === 0) {
    await recordRequestEvent(source, runId, {
      category: 'analytics_api',
      method: 'reports.query.availability',
      quotaCost: 0,
      itemCount: 0,
      result: dailyResult,
      status: 'delayed'
    });
  }

  const byPeriod = {};
  for (const days of [7, 30, 90]) {
    if (Date.now() >= deadlineMs) {
      throw createTimeBudgetError();
    }
    const periodStart = utcDate(-days);
    const result = await callAndRecord(source, runId, {
      category: 'analytics_api',
      method: `reports.query.video.${days}d`,
      quotaCost: 0
    }, () => youtube.queryAnalytics(accessToken, {
      channelId: source.channel_id,
      startDate: periodStart,
      endDate,
      dimensions: 'video',
      sort: '-views',
      maxResults: limits.analyticsTopVideos
    }, { deadlineMs }));
    byPeriod[`${days}d`] = {
      rows: analyticsRows(result.body, 'video'),
      startDate: periodStart,
      endDate,
      delayed: false,
      reason: null
    };
  }
  return { daily, throughDate, requestedEndDate: endDate, byPeriod };
}

function analyticsMetric(record, name) {
  return metricOrNull(record[name]);
}

function analyticsAvailability(record) {
  const availability = {};
  for (const metric of youtube.YOUTUBE_ANALYTICS_METRICS) {
    availability[metric] = record[metric] === null || record[metric] === undefined ? 'unavailable' : 'available';
  }
  return availability;
}

async function upsertContent(connection, source, runId, video) {
  const metadata = {
    source: 'youtube',
    thumbnail_url: video.thumbnailUrl,
    content_type: video.contentType,
    privacy_status: video.privacyStatus,
    availability: video.availability
  };
  await connection.query(
    `INSERT INTO content_items
      (id, workspace_id, data_source_id, provider_content_id, published_at, title,
       description, share_url, duration_seconds, provider_metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       published_at = COALESCE(VALUES(published_at), published_at),
       title = COALESCE(VALUES(title), title),
       description = COALESCE(VALUES(description), description),
       share_url = COALESCE(VALUES(share_url), share_url),
       duration_seconds = COALESCE(VALUES(duration_seconds), duration_seconds),
       provider_metadata = VALUES(provider_metadata),
       last_seen_at = UTC_TIMESTAMP(3),
       deleted_at = NULL`,
    [
      createId(),
      source.workspace_id,
      source.id,
      video.providerContentId,
      video.publishedAt ? new Date(video.publishedAt) : null,
      video.title,
      video.description,
      video.shareUrl,
      video.durationSeconds,
      JSON.stringify(metadata)
    ]
  );
  const rows = await connection.query(
    `SELECT id FROM content_items WHERE data_source_id = ? AND provider_content_id = ? LIMIT 1`,
    [source.id, video.providerContentId]
  );
  const contentId = rows[0].id;
  await connection.query(
    `INSERT INTO content_metric_snapshots
      (id, workspace_id, content_item_id, sync_run_id, observed_at,
       view_count, like_count, comment_count, share_count, provider_metrics)
     VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?)`,
    [
      createId(),
      source.workspace_id,
      contentId,
      runId,
      video.viewCount,
      video.likeCount,
      video.commentCount,
      video.shareCount,
      JSON.stringify({ source: 'youtube', statistic_semantics: 'lifetime', availability: video.availability })
    ]
  );
  return contentId;
}

async function storeSyncResult(source, runId, channel, uploads, analytics, startedMs, syncError) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await connection.query(
        `UPDATE provider_resources
         SET display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE id = (SELECT provider_resource_id FROM workspace_provider_connections WHERE id = ?)`,
        [
          channel.title,
          JSON.stringify({
            id: channel.id,
            title: channel.title,
            customUrl: channel.customUrl,
            thumbnailUrl: channel.thumbnailUrl,
            uploadsPlaylistId: channel.uploadsPlaylistId,
            subscriberCount: channel.subscriberCount,
            subscriberCountHidden: channel.subscriberHidden,
            lifetimeViewCount: channel.lifetimeViewCount,
            publicVideoCount: channel.publicVideoCount
          }),
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_accounts
         SET username = ?, display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [channel.customUrl, channel.title, JSON.stringify({ thumbnailUrl: channel.thumbnailUrl }), source.id]
      );
      await connection.query(
        `INSERT INTO youtube_channel_snapshots
          (id, workspace_id, data_source_id, workspace_provider_connection_id, sync_run_id,
           observed_at, subscriber_count, subscriber_count_hidden, lifetime_view_count,
           public_video_count, uploads_playlist_id, thumbnail_url, availability)
         VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?, ?, ?)`,
        [
          createId(),
          source.workspace_id,
          source.id,
          source.workspace_provider_connection_id,
          runId,
          channel.subscriberCount,
          channel.subscriberHidden,
          channel.lifetimeViewCount,
          channel.publicVideoCount,
          channel.uploadsPlaylistId,
          channel.thumbnailUrl,
          JSON.stringify(channel.availability)
        ]
      );

      const contentByProviderId = new Map();
      for (const video of uploads.videos) {
        contentByProviderId.set(video.providerContentId, await upsertContent(connection, source, runId, video));
      }

      if (analytics) {
        for (const row of analytics.daily) {
          await connection.query(
            `INSERT INTO youtube_analytics_daily_snapshots
              (id, workspace_id, data_source_id, workspace_provider_connection_id, sync_run_id,
               report_date, observed_at, data_through_date, views, estimated_minutes_watched,
               average_view_duration, average_view_percentage, subscribers_gained,
               subscribers_lost, likes, comments, shares, availability)
             VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              createId(), source.workspace_id, source.id, source.workspace_provider_connection_id, runId,
              row.day, analytics.throughDate, analyticsMetric(row, 'views'),
              analyticsMetric(row, 'estimatedMinutesWatched'), analyticsMetric(row, 'averageViewDuration'),
              analyticsMetric(row, 'averageViewPercentage'), analyticsMetric(row, 'subscribersGained'),
              analyticsMetric(row, 'subscribersLost'), analyticsMetric(row, 'likes'),
              analyticsMetric(row, 'comments'), analyticsMetric(row, 'shares'),
              JSON.stringify(analyticsAvailability(row))
            ]
          );
        }
        for (const [periodKey, period] of Object.entries(analytics.byPeriod)) {
          for (const row of period.rows) {
            let contentId = contentByProviderId.get(String(row.video));
            if (!contentId) {
              const placeholder = normalizeVideo(null, {
                videoId: String(row.video),
                title: null,
                description: null,
                publishedAt: null,
                thumbnailUrl: null,
                unavailableReason: 'metadata_not_in_sync_window'
              });
              contentId = await upsertContent(connection, source, runId, placeholder);
              contentByProviderId.set(String(row.video), contentId);
            }
            await connection.query(
              `INSERT INTO youtube_video_analytics_snapshots
                (id, workspace_id, data_source_id, content_item_id, sync_run_id,
                 period_key, period_start, period_end, data_through_date, observed_at,
                 views, estimated_minutes_watched, average_view_duration, average_view_percentage,
                 subscribers_gained, subscribers_lost, likes, comments, shares, availability)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              [
                createId(), source.workspace_id, source.id, contentId, runId,
                periodKey, period.startDate, period.endDate, analytics.throughDate,
                analyticsMetric(row, 'views'), analyticsMetric(row, 'estimatedMinutesWatched'),
                analyticsMetric(row, 'averageViewDuration'), analyticsMetric(row, 'averageViewPercentage'),
                analyticsMetric(row, 'subscribersGained'), analyticsMetric(row, 'subscribersLost'),
                analyticsMetric(row, 'likes'), analyticsMetric(row, 'comments'), analyticsMetric(row, 'shares'),
                JSON.stringify(analyticsAvailability(row))
              ]
            );
          }
        }
      }

      const status = syncError || !uploads.complete ? 'partial' : 'success';
      if (syncError) {
        await connection.query(
          `INSERT INTO sync_errors
            (id, sync_run_id, category, provider_code, message, retryable)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            createId(), runId, syncError.category, syncError.provider_code,
            String(syncError.message).slice(0, 512), Boolean(syncError.retryable)
          ]
        );
      }
      await connection.query(
        `UPDATE sync_runs
         SET status = ?, finished_at = UTC_TIMESTAMP(3), duration_ms = ?, profile_count = 1,
             content_seen_count = ?, content_snapshot_count = ?
         WHERE id = ?`,
        [status, Math.max(0, Date.now() - startedMs), uploads.videos.length, uploads.videos.length, runId]
      );

      const nextRunSeconds = status === 'success'
        ? Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS)
        : retryDelaySeconds(syncError);
      const reconnect = syncError && ['authentication', 'scope'].includes(syncError.category);
      await connection.query(
        `UPDATE data_sources
         SET status = ?, reconnect_reason = ?, last_sync_at = UTC_TIMESTAMP(3),
             last_successful_sync_at = CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE last_successful_sync_at END,
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [
          reconnect ? 'reconnect_required' : 'active',
          syncError ? `${syncError.category}:${syncError.message}`.slice(0, 255) : null,
          status === 'success',
          nextRunSeconds,
          source.id
        ]
      );
      await connection.query(
        `UPDATE workspace_provider_connections
         SET status = ?, last_sync_at = UTC_TIMESTAMP(3),
             last_successful_sync_at = CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE last_successful_sync_at END,
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             data_through_at = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'active', status === 'success', nextRunSeconds, analytics && analytics.throughDate, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE sync_jobs
         SET status = ?, run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL,
             requested_trigger_type = 'scheduled', updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [reconnect ? 'paused' : 'due', nextRunSeconds, source.id]
      );
      await connection.query(
        `UPDATE provider_sync_states
         SET cursor_state = ?, last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE last_success_at END,
             failure_category = ?, failure_count = CASE WHEN ? IS NULL THEN 0 ELSE failure_count + 1 END,
             retry_after_at = CASE WHEN ? IS NULL THEN NULL ELSE DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND) END
         WHERE workspace_provider_connection_id = ? AND sync_key = 'youtube.uploads'`,
        [
          JSON.stringify({ next_page_token: uploads.nextPageToken, page_number: uploads.pageNumber }),
          status === 'success',
          syncError ? syncError.category : uploads.complete ? null : uploads.reason,
          syncError ? syncError.category : uploads.complete ? null : uploads.reason,
          syncError ? syncError.retry_after_seconds : null,
          syncError ? syncError.retry_after_seconds || 0 : 0,
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_sync_states
         SET last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = CASE WHEN ? THEN UTC_TIMESTAMP(3) ELSE last_success_at END,
             data_through_at = ?, failure_category = ?,
             failure_count = CASE WHEN ? IS NULL THEN 0 ELSE failure_count + 1 END
         WHERE workspace_provider_connection_id = ? AND sync_key = 'youtube.analytics'`,
        [
          Boolean(analytics && analytics.throughDate),
          analytics && analytics.throughDate,
          analytics && !analytics.throughDate ? 'data_delay' : syncError ? syncError.category : null,
          analytics && !analytics.throughDate ? 'data_delay' : syncError ? syncError.category : null,
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_capabilities
         SET status = CASE
               WHEN capability_key IN ('channel_metrics', 'video_listing') THEN 'available'
               WHEN capability_key IN ('video_analytics', 'dimension_breakdowns') AND ? IS NULL THEN 'delayed'
               ELSE 'available'
             END,
             reason = CASE
               WHEN capability_key IN ('video_analytics', 'dimension_breakdowns') AND ? IS NULL THEN 'youtube_reporting_delay'
               ELSE NULL
             END,
             updated_at = UTC_TIMESTAMP(3)
         WHERE workspace_provider_connection_id = ?`,
        [analytics && analytics.throughDate, analytics && analytics.throughDate, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE provider_authorizations
         SET last_validated_at = UTC_TIMESTAMP(3), status = 'active',
             deletion_due_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? DAY),
             updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS, source.provider_authorization_id]
      );
      await connection.commit();
      return status;
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function finishFailedRun(source, runId, startedMs, syncError) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await connection.query(
        `UPDATE sync_runs
         SET status = 'failed', finished_at = UTC_TIMESTAMP(3), duration_ms = ?
         WHERE id = ?`,
        [Math.max(0, Date.now() - startedMs), runId]
      );
      await connection.query(
        `INSERT INTO sync_errors
          (id, sync_run_id, category, provider_code, message, retryable)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
          createId(), runId, syncError.category, syncError.provider_code,
          String(syncError.message).slice(0, 512), Boolean(syncError.retryable)
        ]
      );
      const reconnect = ['authentication', 'scope'].includes(syncError.category) ||
        syncError.provider_code === 'youtube_channel_inaccessible';
      const retrySeconds = retryDelaySeconds(syncError);
      await connection.query(
        `UPDATE data_sources
         SET status = ?, reconnect_reason = ?, last_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'active', `${syncError.category}:${syncError.message}`.slice(0, 255), retrySeconds, source.id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections
         SET status = ?, last_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'active', retrySeconds, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE sync_jobs
         SET status = ?, run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL,
             requested_trigger_type = 'scheduled', updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [reconnect ? 'paused' : 'due', retrySeconds, source.id]
      );
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function performYouTubeSyncForJob(job, options = {}) {
  const startedMs = Date.now();
  const configuration = getYouTubeConfiguration();
  if (!configuration.connectable) {
    await withConnection(connection => connection.query(
      `UPDATE sync_jobs
       SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
           lease_owner = NULL, lease_expires_at = NULL,
           requested_trigger_type = 'scheduled',
           updated_at = UTC_TIMESTAMP(3)
       WHERE data_source_id = ?`,
      [DEFAULT_SYNC_INTERVAL_SECONDS, job.data_source_id]
    ));
    return {
      data_source_id: job.data_source_id,
      sync_run_id: null,
      status: 'disabled',
      error: {
        category: 'configuration',
        provider_code: 'youtube_not_available',
        retryable: false,
        message: 'youtube_not_available'
      },
      counts: { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 }
    };
  }
  const limits = getYouTubeLimits();
  const localDeadline = startedMs + limits.jobTimeBudgetSeconds * 1000;
  const deadlineMs = options.deadlineMs ? Math.min(options.deadlineMs, localDeadline) : localDeadline;
  let source = null;
  let runId = null;
  try {
    source = await withConnection(connection => loadSource(connection, job.data_source_id));
    if (!source || source.status !== 'active' || source.authorization_status !== 'active' || source.revoked_at) {
      throw createSyncError('youtube_source_not_syncable');
    }
    runId = await createSyncRun(source, options.triggerType || 'scheduled', options.correlationId);
    const accessToken = await refreshCredentialsIfNeeded(source, runId, deadlineMs);
    const channelResult = await callAndRecord(source, runId, {
      category: 'data_api',
      method: 'channels.list',
      quotaCost: 1,
      itemCount: 1
    }, () => youtube.getChannel(accessToken, source.channel_id, { deadlineMs }));
    if (!channelResult.body || !Array.isArray(channelResult.body.items)) {
      throw createSyncError('youtube_channel_response_malformed');
    }
    if (channelResult.body.items.length === 0) {
      const error = createSyncError('youtube_channel_inaccessible');
      error.syncError.category = 'provider';
      error.syncError.provider_code = 'youtube_channel_inaccessible';
      error.syncError.retryable = false;
      throw error;
    }
    const channel = normalizeChannel(channelResult.body.items[0]);
    const uploads = await fetchUploads(source, runId, accessToken, channel, deadlineMs, limits);
    let analytics = null;
    let partialError = null;
    try {
      analytics = await fetchAnalytics(source, runId, accessToken, deadlineMs, limits);
    } catch (error) {
      const normalized = internalSyncError(error);
      if (normalized.terminal || ['authentication', 'scope'].includes(normalized.category)) throw error;
      partialError = normalized;
    }
    const status = await storeSyncResult(source, runId, channel, uploads, analytics, startedMs, partialError);
    return {
      data_source_id: source.id,
      sync_run_id: runId,
      status,
      error: partialError,
      counts: { profile_count: 1, content_seen_count: uploads.videos.length, content_snapshot_count: uploads.videos.length },
      pagination: { complete: uploads.complete, reason: uploads.reason, next_page_token_present: Boolean(uploads.nextPageToken) },
      data_through_date: analytics && analytics.throughDate
    };
  } catch (error) {
    const syncError = internalSyncError(error);
    const validationOverdue = source && Number(source.validation_overdue) === 1;
    if (source && (validationOverdue || syncError.terminal || ['authentication', 'scope'].includes(syncError.category))) {
      const outcomeCategory = validationOverdue
        ? 'authorization_validation_window_expired'
        : syncError.provider_code === 'invalid_grant'
          ? 'invalid_grant_external_revocation'
          : 'authorization_unusable_external_revocation';
      await purgeYouTubeAuthorizationBySystem(source.provider_authorization_id, outcomeCategory);
      return {
        data_source_id: job.data_source_id,
        sync_run_id: null,
        status: 'failed',
        error: syncError,
        counts: { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 }
      };
    }
    if (source && runId) await finishFailedRun(source, runId, startedMs, syncError);
    else {
      const reconnect = ['authentication', 'scope'].includes(syncError.category);
      const retrySeconds = retryDelaySeconds(syncError);
      await withConnection(connection => connection.query(
        `UPDATE sync_jobs
         SET status = ?, run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL,
             requested_trigger_type = 'scheduled', updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [reconnect ? 'paused' : 'due', retrySeconds, job.data_source_id]
      ));
    }
    return {
      data_source_id: job.data_source_id,
      sync_run_id: runId,
      status: 'failed',
      error: syncError,
      counts: { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 }
    };
  }
}

module.exports = {
  analyticsRows,
  normalizeChannel,
  normalizePlaylistItem,
  normalizeVideo,
  parseIsoDuration,
  performYouTubeSyncForJob,
  retryDelaySeconds
};
