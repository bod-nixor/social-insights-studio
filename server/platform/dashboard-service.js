const { getConnection } = require('../database');
const { compareMetric, engagementRate, resolveDateRange } = require('./analytics');
const { assertCapability } = require('./rbac');

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

async function requireWorkspaceCapability(connection, workspaceId, userId, capability = 'viewDashboard') {
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

function serializeDate(value) {
  return value ? new Date(value).toISOString() : null;
}

function metricCard(label, key, current, baseline) {
  return {
    key,
    label,
    ...compareMetric(current && current[key], baseline && baseline[key])
  };
}

async function getTikTokSource(connection, workspaceId) {
  const rows = await connection.query(
    `SELECT *
     FROM data_sources
     WHERE workspace_id = ? AND provider = 'tiktok' AND deleted_at IS NULL
     ORDER BY created_at ASC LIMIT 1`,
    [workspaceId]
  );
  return rows[0] || null;
}

async function getLatestProfile(connection, workspaceId, to) {
  const rows = await connection.query(
    `SELECT follower_count, following_count, likes_count, video_count, observed_at, provider_metrics
     FROM profile_snapshots
     WHERE workspace_id = ? AND observed_at <= ?
     ORDER BY observed_at DESC LIMIT 1`,
    [workspaceId, to]
  );
  return rows[0] || null;
}

function isDemoProfileSnapshot(snapshot) {
  if (!snapshot || !snapshot.provider_metrics) return false;
  if (typeof snapshot.provider_metrics === 'object') {
    return snapshot.provider_metrics.fixture === true;
  }
  try {
    return JSON.parse(snapshot.provider_metrics).fixture === true;
  } catch (error) {
    return false;
  }
}

async function getBaselineProfile(connection, workspaceId, from) {
  const rows = await connection.query(
    `SELECT follower_count, following_count, likes_count, video_count, observed_at
     FROM profile_snapshots
     WHERE workspace_id = ? AND observed_at <= ?
     ORDER BY observed_at DESC LIMIT 1`,
    [workspaceId, from]
  );
  return rows[0] || null;
}

async function getDashboard(userId, workspaceId, query = {}) {
  const range = resolveDateRange(query);
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'viewDashboard');
    const source = await getTikTokSource(connection, workspaceId);
    const latest = await getLatestProfile(connection, workspaceId, range.to);
    const baseline = await getBaselineProfile(connection, workspaceId, range.from);
    const trend = await connection.query(
      `SELECT observed_at, follower_count, likes_count
       FROM profile_snapshots
       WHERE workspace_id = ? AND observed_at BETWEEN ? AND ?
       ORDER BY observed_at ASC`,
      [workspaceId, range.from, range.to]
    );
    const syncRows = await connection.query(
      `SELECT sr.id, sr.trigger_type, sr.status, sr.started_at, sr.finished_at, sr.duration_ms,
              sr.profile_count, sr.content_seen_count, sr.content_snapshot_count
       FROM sync_runs sr
       JOIN data_sources ds ON ds.id = sr.data_source_id
       WHERE sr.workspace_id = ? AND ds.provider = 'tiktok'
       ORDER BY sr.started_at DESC LIMIT 1`,
      [workspaceId]
    );
    const topContent = await queryContentRows(connection, workspaceId, {
      from: range.from,
      to: range.to,
      sort: query.top_sort || 'views',
      direction: 'desc',
      limit: 5,
      offset: 0
    });
    return {
      range: { from: range.from.toISOString(), to: range.to.toISOString() },
      demo_data: isDemoProfileSnapshot(latest),
      connection: source ? {
        provider: source.provider,
        status: source.status,
        reconnect_reason: source.reconnect_reason,
        last_sync_at: serializeDate(source.last_sync_at),
        last_successful_sync_at: serializeDate(source.last_successful_sync_at),
        next_sync_at: serializeDate(source.next_sync_at)
      } : { provider: 'tiktok', status: 'disconnected' },
      latest_sync: syncRows[0] ? {
        ...syncRows[0],
        started_at: serializeDate(syncRows[0].started_at),
        finished_at: serializeDate(syncRows[0].finished_at)
      } : null,
      metrics: [
        metricCard('Followers', 'follower_count', latest, baseline),
        metricCard('Following', 'following_count', latest, baseline),
        metricCard('Total likes', 'likes_count', latest, baseline),
        metricCard('Total videos', 'video_count', latest, baseline)
      ],
      trend: trend.map(point => ({
        observed_at: serializeDate(point.observed_at),
        follower_count: point.follower_count === null ? null : Number(point.follower_count),
        likes_count: point.likes_count === null ? null : Number(point.likes_count)
      })),
      top_content: topContent.rows
    };
  });
}

const SORT_COLUMNS = {
  published_at: 'ci.published_at',
  views: 'latest.view_count',
  likes: 'latest.like_count',
  comments: 'latest.comment_count',
  shares: 'latest.share_count',
  engagement: 'CASE WHEN latest.view_count IS NULL OR latest.view_count <= 0 THEN NULL ELSE ((COALESCE(latest.like_count, 0) + COALESCE(latest.comment_count, 0) + COALESCE(latest.share_count, 0)) / latest.view_count) END',
  observed_at: 'latest.observed_at'
};

async function queryContentRows(connection, workspaceId, options = {}) {
  const limit = Math.min(Math.max(Number(options.limit || 25), 1), 100);
  const offset = Math.max(Number(options.offset || 0), 0);
  const sortColumn = SORT_COLUMNS[options.sort] || SORT_COLUMNS.views;
  const direction = String(options.direction || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
  const allowedProviders = new Set(['tiktok', 'youtube', 'facebook_pages', 'instagram']);
  const provider = options.provider === 'all' ? 'all' : allowedProviders.has(options.provider) ? options.provider : 'tiktok';
  const where = ['ci.workspace_id = ?', 'ci.deleted_at IS NULL'];
  const params = [workspaceId];
  if (provider !== 'all') {
    where.push('ds.provider = ?');
    params.push(provider);
  } else {
    where.push("ds.provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram')");
  }
  if (options.connectionId) {
    where.push('wpc.id = ?');
    params.push(options.connectionId);
  }
  if (options.dataSourceId) {
    where.push('ds.id = ?');
    params.push(options.dataSourceId);
  }
  if (options.from) {
    where.push('(ci.published_at IS NULL OR ci.published_at >= ?)');
    params.push(options.from);
  }
  if (options.to) {
    where.push('(ci.published_at IS NULL OR ci.published_at <= ?)');
    params.push(options.to);
  }
  const search = String(options.search || '').trim().toLowerCase();
  if (search) {
    where.push('(LOWER(COALESCE(ci.title, \'\')) LIKE ? OR LOWER(COALESCE(ci.description, \'\')) LIKE ? OR LOWER(ci.provider_content_id) LIKE ?)');
    const pattern = `%${search.replace(/[\\%_]/g, value => `\\${value}`)}%`;
    params.push(pattern, pattern, pattern);
  }
  const rows = await connection.query(
    `SELECT ci.id, ds.provider, wpc.id AS connection_id,
            COALESCE(pr.display_name, pa.display_name, pa.username) AS resource_name,
            ci.provider_content_id, ci.published_at, ci.title, ci.description,
            ci.share_url, ci.duration_seconds, ci.height, ci.width,
            latest.observed_at, latest.view_count, latest.like_count,
            latest.comment_count, latest.share_count
     FROM content_items ci
     JOIN data_sources ds ON ds.id = ci.data_source_id
     LEFT JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
     LEFT JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     LEFT JOIN provider_accounts pa ON pa.data_source_id = ds.id
     LEFT JOIN (
       SELECT cms.*
       FROM content_metric_snapshots cms
       JOIN (
         SELECT content_item_id, MAX(observed_at) AS observed_at
         FROM content_metric_snapshots
         GROUP BY content_item_id
       ) newest ON newest.content_item_id = cms.content_item_id
               AND newest.observed_at = cms.observed_at
     ) latest ON latest.content_item_id = ci.id
     WHERE ${where.join(' AND ')}
     ORDER BY ${sortColumn} IS NULL ASC, ${sortColumn} ${direction}, ci.id ASC
     LIMIT ? OFFSET ?`,
    [...params, limit, offset]
  );
  const countRows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM content_items ci
     JOIN data_sources ds ON ds.id = ci.data_source_id
     LEFT JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
     WHERE ${where.join(' AND ')}`,
    params
  );
  return {
    rows: rows.map(row => ({
      id: row.id,
      provider: row.provider,
      connection_id: row.connection_id || null,
      resource_name: row.resource_name || null,
      provider_content_id: row.provider_content_id,
      published_at: serializeDate(row.published_at),
      title: row.title,
      description: row.description,
      share_url: row.share_url,
      duration_seconds: row.duration_seconds,
      height: row.height,
      width: row.width,
      observed_at: serializeDate(row.observed_at),
      view_count: row.view_count === null ? null : Number(row.view_count),
      like_count: row.like_count === null ? null : Number(row.like_count),
      comment_count: row.comment_count === null ? null : Number(row.comment_count),
      share_count: row.share_count === null ? null : Number(row.share_count),
      engagement_rate: engagementRate(row)
    })),
    total: Number(countRows[0].count || 0),
    limit,
    offset
  };
}

async function getContent(userId, workspaceId, query = {}) {
  const range = resolveDateRange(query);
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'viewDashboard');
    return queryContentRows(connection, workspaceId, {
      from: range.from,
      to: range.to,
      sort: query.sort,
      direction: query.direction,
      search: query.search,
      limit: query.limit,
      offset: query.offset,
      provider: query.provider,
      connectionId: query.connection_id
    });
  });
}

function parseJsonField(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch (error) {
    return null;
  }
}

async function getContentDetail(userId, workspaceId, contentItemId) {
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'viewDashboard');
    const rows = await connection.query(
      `SELECT ci.*, ds.provider, wpc.id AS connection_id,
              COALESCE(pr.display_name, pa.display_name, pa.username) AS resource_name
       FROM content_items ci
       JOIN data_sources ds ON ds.id = ci.data_source_id
       LEFT JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
       LEFT JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
       LEFT JOIN provider_accounts pa ON pa.data_source_id = ds.id
       WHERE ci.id = ? AND ci.workspace_id = ? AND ci.deleted_at IS NULL
         AND ds.provider IN ('tiktok', 'youtube', 'facebook_pages', 'instagram')
       LIMIT 1`,
      [contentItemId, workspaceId]
    );
    const item = rows[0] || null;
    if (!item) throw createHttpError(404, 'content_not_found');
    const history = await connection.query(
      `SELECT observed_at, view_count, like_count, comment_count, share_count, provider_metrics
       FROM content_metric_snapshots
       WHERE workspace_id = ? AND content_item_id = ?
       ORDER BY observed_at ASC`,
      [workspaceId, contentItemId]
    );
    const latest = history[history.length - 1] || null;
    return {
      item: {
        id: item.id,
        provider: item.provider,
        connection_id: item.connection_id || null,
        resource_name: item.resource_name || null,
        provider_content_id: item.provider_content_id,
        published_at: serializeDate(item.published_at),
        title: item.title,
        description: item.description,
        share_url: item.share_url,
        duration_seconds: item.duration_seconds,
        height: item.height,
        width: item.width,
        provider_metadata: parseJsonField(item.provider_metadata)
      },
      current_metrics: latest ? {
        observed_at: serializeDate(latest.observed_at),
        view_count: latest.view_count === null ? null : Number(latest.view_count),
        like_count: latest.like_count === null ? null : Number(latest.like_count),
        comment_count: latest.comment_count === null ? null : Number(latest.comment_count),
        share_count: latest.share_count === null ? null : Number(latest.share_count),
        engagement_rate: engagementRate(latest)
      } : null,
      history: history.map(row => ({
        observed_at: serializeDate(row.observed_at),
        view_count: row.view_count === null ? null : Number(row.view_count),
        like_count: row.like_count === null ? null : Number(row.like_count),
        comment_count: row.comment_count === null ? null : Number(row.comment_count),
        share_count: row.share_count === null ? null : Number(row.share_count),
        engagement_rate: engagementRate(row),
        provider_metrics: parseJsonField(row.provider_metrics)
      }))
    };
  });
}

async function getSyncHistory(userId, workspaceId, options = {}) {
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'viewDashboard');
    const limit = Math.min(Math.max(Number(options.limit || 25), 1), 100);
    const offset = Math.max(Number(options.offset || 0), 0);
    const rows = await connection.query(
      `SELECT sr.id, sr.trigger_type, sr.status, sr.started_at, sr.finished_at, sr.duration_ms,
              sr.attempt, sr.profile_count, sr.content_seen_count, sr.content_snapshot_count,
              se.category AS error_category, se.provider_code, se.retryable
       FROM sync_runs sr
       LEFT JOIN sync_errors se ON se.sync_run_id = sr.id
       WHERE sr.workspace_id = ?
       ORDER BY sr.started_at DESC LIMIT ? OFFSET ?`,
      [workspaceId, limit, offset]
    );
    const countRows = await connection.query(
      `SELECT COUNT(*) AS count
       FROM sync_runs
       WHERE workspace_id = ?`,
      [workspaceId]
    );
    return {
      total: Number(countRows[0].count || 0),
      limit,
      offset,
      sync_runs: rows.map(row => ({
        ...row,
        started_at: serializeDate(row.started_at),
        finished_at: serializeDate(row.finished_at)
      }))
    };
  });
}

module.exports = {
  getContent,
  getContentDetail,
  getDashboard,
  getSyncHistory,
  queryContentRows,
  requireWorkspaceCapability
};
