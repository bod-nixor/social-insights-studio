const { getConnection } = require('../database');
const meta = require('../integrations/meta');
const { decryptSecret } = require('./secret-envelope');
const { createId } = require('./security');
const { getMetaConfiguration, getMetaLimits, META_GRAPH_API_VERSION } = require('./meta-config');

const DEFAULT_SYNC_INTERVAL_SECONDS = 6 * 60 * 60;
const INSTAGRAM_ACCOUNT_PERIOD_DAYS = Object.freeze([7, 30, 90]);
const FACEBOOK_POST_COUNT_AVAILABILITY = Object.freeze({
  reactions: 'unavailable_under_approved_narrow_permissions',
  comments: 'unavailable_under_approved_narrow_permissions'
});

function createSyncError(code, result = null) {
  const error = new Error(code);
  error.code = code;
  const providerError = result && result.error ? result.error : {};
  error.syncError = {
    category: providerError.category || 'provider',
    provider_code: providerError.provider_code || code,
    retryable: providerError.retryable !== false,
    terminal: providerError.terminal === true,
    retry_after_seconds: result && result.retryAfterSeconds !== null ? result.retryAfterSeconds : null,
    message: code
  };
  return error;
}

function internalSyncError(error) {
  return error && error.syncError ? error.syncError : {
    category: error && error.category ? error.category : 'internal',
    provider_code: error && (error.code || error.message) ? error.code || error.message : 'meta_sync_failed',
    retryable: error ? error.retryable !== false : false,
    terminal: false,
    retry_after_seconds: null,
    message: error && (error.code || error.message) ? error.code || error.message : 'meta_sync_failed'
  };
}

function retryDelaySeconds(syncError, interval = Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS)) {
  if (syncError.retry_after_seconds) return Math.max(60, Math.min(Number(syncError.retry_after_seconds), interval));
  if (syncError.category === 'rate_limit') return Math.min(interval, 60 * 60);
  return syncError.retryable === false ? interval : Math.min(interval, 15 * 60);
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) throw createSyncError('database_not_configured');
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
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

function integerOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.trunc(number) : null;
}

function isoDate(value = new Date()) {
  return new Date(value).toISOString().slice(0, 10);
}

function dateDaysAgo(days) {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() - days);
  return isoDate(value);
}

function latestInsightValue(body, metricName) {
  const rows = body && Array.isArray(body.data) ? body.data : [];
  const metric = metricName
    ? rows.find(row => row && row.name === metricName)
    : rows[0];
  if (!metric) return null;
  if (metric.total_value && metric.total_value.value !== undefined) return metric.total_value.value;
  if (Array.isArray(metric.values) && metric.values.length > 0) {
    return metric.values[metric.values.length - 1].value;
  }
  return null;
}

function dailyInsightValues(body, metricName) {
  const rows = body && Array.isArray(body.data) ? body.data : [];
  const metric = metricName
    ? rows.find(row => row && row.name === metricName)
    : rows[0];
  if (!metric) return [];
  if (Array.isArray(metric.values)) {
    return metric.values.flatMap(value => {
      const endTime = value && value.end_time ? new Date(value.end_time) : null;
      if (!endTime || !Number.isFinite(endTime.getTime()) || value.value === null || value.value === undefined) {
        return [];
      }
      return [{
        date: isoDate(new Date(endTime.getTime() - 24 * 60 * 60 * 1000)),
        value: value.value
      }];
    });
  }
  return [];
}

async function loadSource(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT ds.*, wpc.id AS workspace_provider_connection_id, wpc.status AS connection_status,
            pr.id AS provider_resource_row_id, pr.provider_resource_id, pr.display_name,
            pr.metadata AS resource_metadata, pauth.id AS provider_authorization_id,
            pauth.status AS authorization_status, pauth.api_version,
            prc.access_token_ciphertext, prc.access_token_iv, prc.access_token_tag,
            prc.key_version, prc.access_expires_at, prc.revoked_at
     FROM data_sources ds
     JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     JOIN provider_resource_credentials prc ON prc.provider_resource_id = pr.id
     WHERE ds.id = ? AND ds.provider IN ('facebook_pages', 'instagram')
       AND ds.deleted_at IS NULL LIMIT 1`,
    [dataSourceId]
  );
  return rows[0] || null;
}

async function recordRequestEvent(source, runId, details) {
  return withConnection(connection => connection.query(
    `INSERT INTO provider_request_events
      (id, workspace_id, provider_authorization_id, workspace_provider_connection_id,
       sync_run_id, provider, request_category, method_name, quota_cost_estimate,
       page_number, item_count, attempts, status, failure_category, retry_after_seconds)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)`,
    [
      createId(), source.workspace_id, source.provider_authorization_id,
      source.workspace_provider_connection_id, runId, source.provider,
      details.category, details.method,
      details.pageNumber === undefined ? null : details.pageNumber,
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
  const bodyRows = result.body && Array.isArray(result.body.data) ? result.body.data : null;
  await recordRequestEvent(source, runId, {
    ...details,
    itemCount: details.itemCount === undefined ? (bodyRows ? bodyRows.length : null) : details.itemCount,
    result,
    status: result.ok ? (bodyRows && bodyRows.length === 0 ? 'empty' : 'success') : 'failed'
  });
  if (!result.ok) throw createSyncError(`${source.provider}_${details.method}_failed`, result);
  return result;
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

function normalizeFacebookPost(item) {
  const shares = item && item.shares ? item.shares.count : null;
  const message = item && item.message ? String(item.message) : null;
  const attachmentTypes = [...new Set(
    (item && item.attachments && Array.isArray(item.attachments.data) ? item.attachments.data : [])
      .map(attachment => String(attachment && attachment.media_type || '').trim())
      .filter(Boolean)
  )].slice(0, 10);
  return {
    id: String(item.id),
    title: message ? message.slice(0, 160) : 'Facebook Page post',
    description: message ? message.slice(0, 16000) : null,
    publishedAt: item.created_time || null,
    shareUrl: item.permalink_url || null,
    viewCount: null,
    likeCount: null,
    commentCount: null,
    shareCount: integerOrNull(shares),
    metadata: {
      thumbnailUrl: item.full_picture || null,
      attachmentTypes,
      providerType: 'page_post',
      availability: { ...FACEBOOK_POST_COUNT_AVAILABILITY }
    }
  };
}

function normalizeInstagramMedia(item) {
  const caption = item && item.caption ? String(item.caption) : null;
  const product = String(item && item.media_product_type || '').toUpperCase();
  return {
    id: String(item.id),
    title: caption ? caption.slice(0, 160) : `${product || item.media_type || 'Instagram'} media`,
    description: caption ? caption.slice(0, 16000) : null,
    publishedAt: item.timestamp || null,
    shareUrl: item.permalink || null,
    viewCount: null,
    likeCount: integerOrNull(item.like_count),
    commentCount: integerOrNull(item.comments_count),
    shareCount: null,
    metadata: {
      thumbnailUrl: item.thumbnail_url || item.media_url || null,
      mediaType: item.media_type || null,
      mediaProductType: item.media_product_type || null,
      storyHistory: product === 'STORY' ? 'not_collected_without_webhooks' : null
    }
  };
}

async function fetchPaginatedContent(source, runId, accessToken, deadlineMs, limits) {
  const rows = [];
  const seenContentIds = new Set();
  const seenCursors = new Set();
  let after = null;
  let excludedStories = 0;
  for (let pageNumber = 1; pageNumber <= limits.maxContentPages && rows.length < limits.maxContentItems; pageNumber += 1) {
    if (Date.now() >= deadlineMs) throw createSyncError(`${source.provider}_time_budget_exhausted`);
    const result = await callAndRecord(source, runId, {
      category: 'data_api',
      method: source.provider === 'facebook_pages' ? 'page.posts' : 'instagram.media',
      pageNumber
    }, () => source.provider === 'facebook_pages'
      ? meta.listPagePosts(source.provider_resource_id, accessToken, after, { deadlineMs })
      : meta.listInstagramMedia(source.provider_resource_id, accessToken, after, { deadlineMs }));
    const items = Array.isArray(result.body.data) ? result.body.data : [];
    for (const item of items) {
      if (!item || !item.id || seenContentIds.has(String(item.id))) continue;
      seenContentIds.add(String(item.id));
      if (source.provider === 'instagram' && String(item.media_product_type || '').toUpperCase() === 'STORY') {
        excludedStories += 1;
        continue;
      }
      rows.push(source.provider === 'facebook_pages' ? normalizeFacebookPost(item) : normalizeInstagramMedia(item));
      if (rows.length >= limits.maxContentItems) break;
    }
    const cursor = result.body && result.body.paging && result.body.paging.cursors
      ? result.body.paging.cursors.after
      : null;
    if (!cursor || items.length === 0 || seenCursors.has(cursor)) break;
    seenCursors.add(cursor);
    after = cursor;
  }
  return { rows, excludedStories };
}

async function fetchFacebookAccountInsights(source, runId, accessToken, since, until, deadlineMs) {
  const daily = new Map();
  const availability = {};
  let maximumUsage = null;
  let partialError = null;
  for (const metricName of meta.FACEBOOK_PAGE_INSIGHT_METRICS) {
    try {
      const result = await callAndRecord(source, runId, {
        category: 'analytics_api',
        method: `${source.provider}.insights.${metricName}`
      }, () => meta.getPageInsights(
        source.provider_resource_id,
        accessToken,
        [metricName],
        since,
        until,
        { deadlineMs }
      ));
      if (result.usage && result.usage.maximum !== null) {
        maximumUsage = maximumUsage === null ? result.usage.maximum : Math.max(maximumUsage, result.usage.maximum);
      }
      const values = dailyInsightValues(result.body, metricName);
      if (values.length === 0) availability[metricName] = 'not_returned';
      else {
        availability[metricName] = 'available';
        for (const row of values) {
          const metricsForDate = daily.get(row.date) || {};
          metricsForDate[metricName] = row.value;
          daily.set(row.date, metricsForDate);
        }
      }
    } catch (error) {
      const normalized = internalSyncError(error);
      if (normalized.terminal || normalized.category === 'authentication' || normalized.category === 'scope') throw error;
      availability[metricName] = 'provider_unavailable';
      partialError = partialError || normalized;
    }
  }
  return { daily, periods: new Map(), availability, maximumUsage, partialError };
}

async function fetchInstagramAccountInsights(source, runId, accessToken, deadlineMs) {
  const periods = new Map();
  const availability = {};
  let maximumUsage = null;
  let partialError = null;
  const rangeEnd = dateDaysAgo(1);

  for (const rangeDays of INSTAGRAM_ACCOUNT_PERIOD_DAYS) {
    const rangeStart = dateDaysAgo(rangeDays);
    const metricValues = {};
    const periodAvailability = {};
    for (const metricName of meta.INSTAGRAM_ACCOUNT_INSIGHT_METRICS) {
      try {
        const result = await callAndRecord(source, runId, {
          category: 'analytics_api',
          method: `${source.provider}.insights.${rangeDays}d.${metricName}`
        }, () => meta.getInstagramInsights(
          source.provider_resource_id,
          accessToken,
          [metricName],
          rangeStart,
          isoDate(),
          { deadlineMs }
        ));
        if (result.usage && result.usage.maximum !== null) {
          maximumUsage = maximumUsage === null ? result.usage.maximum : Math.max(maximumUsage, result.usage.maximum);
        }
        const value = latestInsightValue(result.body, metricName);
        if (value === null || value === undefined) periodAvailability[metricName] = 'not_returned';
        else {
          periodAvailability[metricName] = 'available';
          metricValues[metricName] = value;
        }
      } catch (error) {
        const normalized = internalSyncError(error);
        if (normalized.terminal || normalized.category === 'authentication' || normalized.category === 'scope') throw error;
        periodAvailability[metricName] = 'provider_unavailable';
        partialError = partialError || normalized;
      }
    }
    availability[`${rangeDays}d`] = periodAvailability;
    periods.set(rangeDays, {
      values: metricValues,
      availability: periodAvailability,
      rangeStart,
      rangeEnd
    });
  }

  return { daily: new Map(), periods, availability, maximumUsage, partialError };
}

async function fetchAccountInsights(source, runId, accessToken, since, until, deadlineMs) {
  if (source.provider === 'instagram') {
    return fetchInstagramAccountInsights(source, runId, accessToken, deadlineMs);
  }
  return fetchFacebookAccountInsights(source, runId, accessToken, since, until, deadlineMs);
}

async function fetchContentInsights(source, runId, accessToken, content, deadlineMs) {
  let partialError = null;
  let maximumUsage = null;
  for (const row of content.rows) {
    if (Date.now() >= deadlineMs) {
      partialError = partialError || {
        category: 'timeout', provider_code: 'meta_time_budget_exhausted', retryable: true,
        terminal: false, retry_after_seconds: null, message: 'meta_time_budget_exhausted'
      };
      row.metadata.insightsAvailability = 'time_budget_exhausted';
      break;
    }
    try {
      let result;
      if (source.provider === 'facebook_pages') {
        result = await callAndRecord(source, runId, {
          category: 'analytics_api', method: 'post.insights', itemCount: 1
        }, () => meta.getPostInsights(row.id, accessToken, { deadlineMs }));
        row.viewCount = integerOrNull(latestInsightValue(result.body, 'post_media_view'));
        row.metadata.uniqueViewCount = integerOrNull(latestInsightValue(result.body, 'post_total_media_view_unique'));
      } else {
        const metrics = meta.instagramMetricsForMedia(row.metadata);
        if (metrics.length === 0) {
          row.metadata.insightsAvailability = 'unsupported_media_type';
          continue;
        }
        result = await callAndRecord(source, runId, {
          category: 'analytics_api', method: 'instagram.media.insights', itemCount: 1
        }, () => meta.getInstagramMediaInsights(row.id, accessToken, metrics, { deadlineMs }));
        row.viewCount = integerOrNull(latestInsightValue(result.body, 'views'));
        row.shareCount = integerOrNull(latestInsightValue(result.body, 'shares'));
        row.metadata.reach = integerOrNull(latestInsightValue(result.body, 'reach'));
        row.metadata.saved = integerOrNull(latestInsightValue(result.body, 'saved'));
      }
      row.metadata.insightsAvailability = 'available';
      if (result.usage && result.usage.maximum !== null) {
        maximumUsage = maximumUsage === null ? result.usage.maximum : Math.max(maximumUsage, result.usage.maximum);
      }
    } catch (error) {
      const normalized = internalSyncError(error);
      if (normalized.terminal || normalized.category === 'authentication' || normalized.category === 'scope') throw error;
      row.metadata.insightsAvailability = 'provider_unavailable';
      partialError = partialError || normalized;
    }
  }
  return { ...content, partialError, maximumUsage };
}

async function fetchMetaData(source, runId, accessToken, deadlineMs, limits) {
  const profileResult = await callAndRecord(source, runId, {
    category: 'data_api',
    method: source.provider === 'facebook_pages' ? 'page.profile' : 'instagram.profile',
    itemCount: 1
  }, () => source.provider === 'facebook_pages'
    ? meta.getPageProfile(source.provider_resource_id, accessToken, { deadlineMs })
    : meta.getInstagramProfile(source.provider_resource_id, accessToken, { deadlineMs }));
  if (!profileResult.body || String(profileResult.body.id) !== String(source.provider_resource_id)) {
    throw createSyncError(`${source.provider}_profile_malformed`);
  }
  const until = isoDate();
  const since = dateDaysAgo(limits.lookbackDays - 1);
  const accountInsights = await fetchAccountInsights(source, runId, accessToken, since, until, deadlineMs);
  const content = await fetchPaginatedContent(source, runId, accessToken, deadlineMs, limits);
  const contentWithInsights = await fetchContentInsights(source, runId, accessToken, content, deadlineMs);
  const maximumUsageValues = [
    profileResult.usage && profileResult.usage.maximum,
    accountInsights.maximumUsage,
    contentWithInsights.maximumUsage
  ].filter(value => value !== null && value !== undefined);
  const maximumUsage = maximumUsageValues.length > 0 ? Math.max(...maximumUsageValues) : null;
  let partialError = accountInsights.partialError || contentWithInsights.partialError;
  if (maximumUsage !== null && maximumUsage >= limits.usageDelayThreshold) {
    partialError = partialError || {
      category: 'rate_limit', provider_code: 'meta_usage_threshold', retryable: true,
      terminal: false, retry_after_seconds: 60 * 60, message: 'meta_usage_threshold'
    };
  }
  return {
    profile: profileResult.body,
    accountInsights,
    content: contentWithInsights,
    partialError,
    maximumUsage,
    since,
    until
  };
}

async function upsertContent(connection, source, runId, row) {
  const existingRows = await connection.query(
    `SELECT id FROM content_items WHERE data_source_id = ? AND provider_content_id = ? LIMIT 1`,
    [source.id, row.id]
  );
  const contentItemId = existingRows[0] ? existingRows[0].id : createId();
  await connection.query(
    `INSERT INTO content_items
      (id, workspace_id, data_source_id, provider_content_id, published_at,
       title, description, share_url, provider_metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       published_at = VALUES(published_at), title = VALUES(title), description = VALUES(description),
       share_url = VALUES(share_url), provider_metadata = VALUES(provider_metadata),
       last_seen_at = UTC_TIMESTAMP(3), deleted_at = NULL`,
    [
      contentItemId, source.workspace_id, source.id, row.id,
      row.publishedAt ? new Date(row.publishedAt) : null,
      row.title, row.description, row.shareUrl, JSON.stringify(row.metadata)
    ]
  );
  await connection.query(
    `INSERT INTO content_metric_snapshots
      (id, workspace_id, content_item_id, sync_run_id, observed_at,
       view_count, like_count, comment_count, share_count, provider_metrics)
     VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?)`,
    [
      createId(), source.workspace_id, contentItemId, runId,
      row.viewCount, row.likeCount, row.commentCount, row.shareCount,
      JSON.stringify(row.metadata)
    ]
  );
}

async function storeSyncResult(source, runId, result, startedMs) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const profile = result.profile;
      const today = isoDate();
      const latestDaily = result.accountInsights.daily.get(today) ||
        [...result.accountInsights.daily.values()].at(-1) || {};
      const preferredInstagramPeriod = result.accountInsights.periods.get(90) ||
        [...result.accountInsights.periods.values()].at(-1) || null;
      const followerCount = source.provider === 'instagram'
        ? integerOrNull(profile.followers_count)
        : integerOrNull(latestDaily.page_follows);
      const mediaCount = source.provider === 'instagram' ? integerOrNull(profile.media_count) : null;
      const profileMetrics = {
        provider: source.provider,
        name: profile.name || null,
        username: profile.username || null,
        followers: followerCount,
        media_count: mediaCount,
        insights: source.provider === 'instagram' && preferredInstagramPeriod
          ? preferredInstagramPeriod.values
          : latestDaily,
        availability: result.accountInsights.availability
      };
      await connection.query(
        `INSERT INTO profile_snapshots
          (id, workspace_id, data_source_id, sync_run_id, observed_at,
           follower_count, video_count, provider_metrics)
         VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?)`,
        [createId(), source.workspace_id, source.id, runId, followerCount, mediaCount, JSON.stringify(profileMetrics)]
      );
      await connection.query(
        `INSERT INTO meta_account_insight_snapshots
          (id, workspace_id, data_source_id, workspace_provider_connection_id,
           sync_run_id, provider, snapshot_kind, report_date, observed_at,
           metric_values, availability)
         VALUES (?, ?, ?, ?, ?, ?, 'profile', ?, UTC_TIMESTAMP(3), ?, ?)`,
        [
          createId(), source.workspace_id, source.id, source.workspace_provider_connection_id,
          runId, source.provider, today, JSON.stringify(profileMetrics),
          JSON.stringify(result.accountInsights.availability)
        ]
      );
      for (const [reportDate, metricValues] of result.accountInsights.daily) {
        await connection.query(
          `INSERT INTO meta_account_insight_snapshots
            (id, workspace_id, data_source_id, workspace_provider_connection_id,
             sync_run_id, provider, snapshot_kind, report_date, observed_at,
             metric_values, availability)
           VALUES (?, ?, ?, ?, ?, ?, 'daily', ?, UTC_TIMESTAMP(3), ?, ?)`,
          [
            createId(), source.workspace_id, source.id, source.workspace_provider_connection_id,
            runId, source.provider, reportDate, JSON.stringify(metricValues),
            JSON.stringify(result.accountInsights.availability)
          ]
        );
      }
      for (const [rangeDays, period] of result.accountInsights.periods) {
        await connection.query(
          `INSERT INTO meta_account_insight_snapshots
            (id, workspace_id, data_source_id, workspace_provider_connection_id,
             sync_run_id, provider, snapshot_kind, report_date, range_days,
             range_start_date, range_end_date, observed_at, metric_values, availability)
           VALUES (?, ?, ?, ?, ?, ?, 'period', ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?)`,
          [
            createId(), source.workspace_id, source.id, source.workspace_provider_connection_id,
            runId, source.provider, period.rangeEnd, rangeDays, period.rangeStart,
            period.rangeEnd, JSON.stringify(period.values), JSON.stringify(period.availability)
          ]
        );
      }
      for (const row of result.content.rows) await upsertContent(connection, source, runId, row);

      const resourceMetadata = parseJson(source.resource_metadata, {});
      const nextMetadata = {
        ...resourceMetadata,
        username: profile.username || resourceMetadata.username || null,
        thumbnailUrl: profile.profile_picture_url ||
          (profile.picture && profile.picture.data && profile.picture.data.url) ||
          resourceMetadata.thumbnailUrl || null,
        followerCount,
        mediaCount,
        lastObservedAt: new Date().toISOString(),
        storyHistory: source.provider === 'instagram' ? 'not_collected_without_webhooks' : undefined
      };
      await connection.query(
        `UPDATE provider_resources SET display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [profile.name || profile.username || source.display_name, JSON.stringify(nextMetadata), source.provider_resource_row_id]
      );
      await connection.query(
        `UPDATE provider_accounts SET username = ?, display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [profile.username || null, profile.name || profile.username || source.display_name, JSON.stringify(nextMetadata), source.id]
      );

      const status = result.partialError ? 'partial' : 'success';
      if (result.partialError) {
        await connection.query(
          `INSERT INTO sync_errors
            (id, sync_run_id, category, provider_code, message, retryable)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            createId(), runId, result.partialError.category,
            String(result.partialError.provider_code || '').slice(0, 120) || null,
            String(result.partialError.message || 'meta_partial_sync').slice(0, 512),
            result.partialError.retryable !== false
          ]
        );
      }
      await connection.query(
        `UPDATE sync_runs SET status = ?, finished_at = UTC_TIMESTAMP(3), duration_ms = ?,
             profile_count = 1, content_seen_count = ?, content_snapshot_count = ?
         WHERE id = ?`,
        [status, Date.now() - startedMs, result.content.rows.length, result.content.rows.length, runId]
      );
      const retrySeconds = result.partialError ? retryDelaySeconds(result.partialError) : Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS);
      const dataThrough = source.provider === 'instagram' && preferredInstagramPeriod
        ? preferredInstagramPeriod.rangeEnd
        : result.accountInsights.daily.size > 0
          ? [...result.accountInsights.daily.keys()].sort().at(-1)
          : today;
      await connection.query(
        `UPDATE data_sources SET status = 'active', reconnect_reason = ?,
             last_sync_at = UTC_TIMESTAMP(3), last_successful_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [result.partialError ? `${result.partialError.category}:${result.partialError.message}`.slice(0, 255) : null, retrySeconds, source.id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections SET status = 'active',
             last_sync_at = UTC_TIMESTAMP(3), last_successful_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), data_through_at = ?,
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [retrySeconds, `${dataThrough} 23:59:59`, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE provider_sync_states SET api_version = ?, last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = UTC_TIMESTAMP(3), data_through_at = ?,
             retry_after_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             failure_category = ?, failure_count = IF(? IS NULL, 0, failure_count + 1)
         WHERE workspace_provider_connection_id = ? AND sync_key = ?`,
        [
          META_GRAPH_API_VERSION, `${dataThrough} 23:59:59`, retrySeconds,
          result.partialError ? result.partialError.category : null,
          result.partialError ? result.partialError.category : null,
          source.workspace_provider_connection_id, `${source.provider}.sync`
        ]
      );
      await connection.query(
        `UPDATE sync_jobs SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
        [retrySeconds, source.id]
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
      const reconnect = syncError.terminal || ['authentication', 'scope'].includes(syncError.category);
      const retrySeconds = retryDelaySeconds(syncError);
      if (runId) {
        await connection.query(
          `INSERT INTO sync_errors
            (id, sync_run_id, category, provider_code, message, retryable)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            createId(), runId, syncError.category,
            String(syncError.provider_code || '').slice(0, 120) || null,
            String(syncError.message || 'meta_sync_failed').slice(0, 512),
            syncError.retryable !== false
          ]
        );
        await connection.query(
          `UPDATE sync_runs SET status = 'failed', finished_at = UTC_TIMESTAMP(3), duration_ms = ? WHERE id = ?`,
          [Date.now() - startedMs, runId]
        );
      }
      await connection.query(
        `UPDATE data_sources SET status = ?, reconnect_reason = ?, last_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'active', `${syncError.category}:${syncError.message}`.slice(0, 255), retrySeconds, source.id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections SET status = ?, last_sync_at = UTC_TIMESTAMP(3),
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [reconnect ? 'reconnect_required' : 'active', retrySeconds, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE provider_authorizations SET status = IF(?, 'reconnect_required', status),
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [reconnect, source.provider_authorization_id]
      );
      if (reconnect) {
        await connection.query(
          `UPDATE provider_capabilities SET status = 'not_granted', reason = ?, updated_at = UTC_TIMESTAMP(3)
           WHERE workspace_provider_connection_id = ?`,
          [`${syncError.category}:${syncError.message}`.slice(0, 255), source.workspace_provider_connection_id]
        );
      }
      await connection.query(
        `UPDATE provider_sync_states SET last_attempt_at = UTC_TIMESTAMP(3),
             retry_after_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             failure_category = ?, failure_count = failure_count + 1
         WHERE workspace_provider_connection_id = ? AND sync_key = ?`,
        [retrySeconds, syncError.category, source.workspace_provider_connection_id, `${source.provider}.sync`]
      );
      await connection.query(
        `UPDATE sync_jobs SET status = ?, run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
        [reconnect ? 'paused' : 'due', retrySeconds, source.id]
      );
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function performMetaSyncForJob(job, options = {}) {
  const startedMs = Date.now();
  let source = null;
  let runId = null;
  try {
    source = await withConnection(connection => loadSource(connection, job.data_source_id));
    if (!source) throw createSyncError('meta_source_not_found');
    const configuration = getMetaConfiguration(source.provider);
    if (!configuration.connectable) {
      await withConnection(connection => connection.query(
        `UPDATE sync_jobs
         SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [DEFAULT_SYNC_INTERVAL_SECONDS, job.data_source_id]
      ));
      return {
        data_source_id: source.id,
        sync_run_id: null,
        status: 'disabled',
        error: {
          category: 'configuration',
          provider_code: `${source.provider}_not_available`,
          retryable: false,
          terminal: false,
          retry_after_seconds: null,
          message: `${source.provider}_not_available`
        },
        counts: { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 }
      };
    }
    if (
      source.status !== 'active' || source.connection_status !== 'active' ||
      source.authorization_status !== 'active' || source.revoked_at
    ) {
      const error = createSyncError(`${source.provider}_source_not_syncable`);
      error.syncError.category = 'authentication';
      error.syncError.retryable = false;
      throw error;
    }
    if (!source.access_expires_at || new Date(source.access_expires_at).getTime() <= Date.now() + 60 * 1000) {
      const error = createSyncError(`${source.provider}_access_token_expired`);
      error.syncError.category = 'authentication';
      error.syncError.retryable = false;
      error.syncError.terminal = true;
      throw error;
    }
    const accessToken = decryptSecret({
      ciphertext: source.access_token_ciphertext,
      iv: source.access_token_iv,
      tag: source.access_token_tag,
      keyVersion: source.key_version
    });
    runId = await createSyncRun(source, options.triggerType || 'scheduled', options.correlationId);
    const limits = getMetaLimits();
    const localDeadline = startedMs + limits.jobTimeBudgetSeconds * 1000;
    const deadlineMs = options.deadlineMs ? Math.min(options.deadlineMs, localDeadline) : localDeadline;
    const result = await fetchMetaData(source, runId, accessToken, deadlineMs, limits);
    const status = await storeSyncResult(source, runId, result, startedMs);
    return {
      data_source_id: source.id,
      sync_run_id: runId,
      status,
      error: result.partialError,
      counts: {
        profile_count: 1,
        content_seen_count: result.content.rows.length,
        content_snapshot_count: result.content.rows.length
      },
      excluded_instagram_stories: result.content.excludedStories,
      maximum_usage_percent: result.maximumUsage
    };
  } catch (error) {
    const syncError = internalSyncError(error);
    if (source) await finishFailedRun(source, runId, startedMs, syncError);
    else {
      await withConnection(connection => connection.query(
        `UPDATE sync_jobs SET status = 'paused', lease_owner = NULL, lease_expires_at = NULL,
             requested_trigger_type = 'scheduled', updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [job.data_source_id]
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
  FACEBOOK_POST_COUNT_AVAILABILITY,
  INSTAGRAM_ACCOUNT_PERIOD_DAYS,
  dailyInsightValues,
  latestInsightValue,
  normalizeFacebookPost,
  normalizeInstagramMedia,
  performMetaSyncForJob,
  retryDelaySeconds
};
