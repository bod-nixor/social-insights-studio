const { getConnection } = require('../database');
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

async function requireWorkspace(connection, workspaceId, userId) {
  const rows = await connection.query(
    `SELECT role FROM workspace_memberships
     WHERE workspace_id = ? AND user_id = ? AND status = 'active'
     LIMIT 1`,
    [workspaceId, userId]
  );
  if (!rows[0]) throw createHttpError(404, 'workspace_not_found');
  assertCapability(rows[0].role, 'viewDashboard');
  return rows[0].role;
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(value, days) {
  const date = new Date(`${value}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function parseRange(query = {}) {
  const range = ['7d', '30d', '90d', 'custom'].includes(query.range) ? query.range : '30d';
  const yesterday = new Date();
  yesterday.setUTCHours(0, 0, 0, 0);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  let to = isoDate(yesterday);
  let from;
  if (range === 'custom') {
    from = String(query.from || '');
    to = String(query.to || '');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      throw createHttpError(400, 'invalid_date_range');
    }
    const fromTime = new Date(`${from}T00:00:00.000Z`).getTime();
    const toTime = new Date(`${to}T00:00:00.000Z`).getTime();
    const normalizedFrom = Number.isFinite(fromTime) ? isoDate(new Date(fromTime)) : null;
    const normalizedTo = Number.isFinite(toTime) ? isoDate(new Date(toTime)) : null;
    const dayCount = Number.isFinite(fromTime) && Number.isFinite(toTime)
      ? Math.floor((toTime - fromTime) / 86400000) + 1
      : 0;
    if (
      normalizedFrom !== from ||
      normalizedTo !== to ||
      fromTime > toTime ||
      dayCount < 1 ||
      dayCount > 366
    ) {
      throw createHttpError(400, 'invalid_date_range');
    }
  } else {
    const days = Number(range.slice(0, -1));
    from = addDays(to, -(days - 1));
  }
  const dayCount = Math.floor((new Date(`${to}T00:00:00.000Z`) - new Date(`${from}T00:00:00.000Z`)) / 86400000) + 1;
  return {
    key: range,
    from,
    to,
    days: dayCount,
    previousFrom: addDays(from, -dayCount),
    previousTo: addDays(from, -1),
    videoPeriodKey: range === 'custom' ? null : range
  };
}

function numeric(value) {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
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

function databaseDate(value) {
  if (!value) return null;
  if (value instanceof Date) {
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, '0');
    const day = String(value.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }
  const text = String(value);
  const match = text.match(/^\d{4}-\d{2}-\d{2}/);
  if (match) return match[0];
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : isoDate(parsed);
}

function metric(key, label, value, baseline, semantics) {
  const current = numeric(value);
  const previous = numeric(baseline);
  const delta = current === null || previous === null ? null : current - previous;
  const percentChange = delta === null || previous === 0 ? null : (delta / previous) * 100;
  return {
    key,
    label,
    value: current,
    baseline: previous,
    delta,
    percent_change: percentChange,
    semantics,
    available: current !== null
  };
}

async function findConnection(connection, workspaceId, connectionId) {
  const params = [workspaceId];
  const selected = connectionId ? 'AND wpc.id = ?' : '';
  if (connectionId) params.push(connectionId);
  const rows = await connection.query(
    `SELECT wpc.id, wpc.status, wpc.data_source_id, wpc.last_sync_at,
            wpc.last_successful_sync_at, wpc.next_sync_at, wpc.data_through_at,
            ds.reconnect_reason, pr.provider_resource_id, pr.display_name, pr.metadata,
            pauth.status AS authorization_status
     FROM workspace_provider_connections wpc
     JOIN data_sources ds ON ds.id = wpc.data_source_id
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     WHERE wpc.workspace_id = ? AND wpc.provider = 'youtube' ${selected}
     ORDER BY FIELD(wpc.status, 'active', 'reconnect_required', 'connecting', 'disconnected'), wpc.created_at ASC
     LIMIT 1`,
    params
  );
  return rows[0] || null;
}

async function latestChannelSnapshot(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT * FROM youtube_channel_snapshots
     WHERE data_source_id = ?
     ORDER BY observed_at DESC
     LIMIT 1`,
    [dataSourceId]
  );
  return rows[0] || null;
}

async function aggregateRange(connection, dataSourceId, from, to) {
  const rows = await connection.query(
    `SELECT SUM(y.views) AS views,
            SUM(y.estimated_minutes_watched) AS estimated_minutes_watched,
            SUM(y.subscribers_gained) AS subscribers_gained,
            SUM(y.subscribers_lost) AS subscribers_lost,
            SUM(y.likes) AS likes,
            SUM(y.comments) AS comments,
            SUM(y.shares) AS shares,
            MAX(y.data_through_date) AS data_through_date
     FROM youtube_analytics_daily_snapshots y
     JOIN (
       SELECT report_date, MAX(observed_at) AS observed_at
       FROM youtube_analytics_daily_snapshots
       WHERE data_source_id = ? AND report_date BETWEEN ? AND ?
       GROUP BY report_date
     ) latest ON latest.report_date = y.report_date AND latest.observed_at = y.observed_at
     WHERE y.data_source_id = ? AND y.report_date BETWEEN ? AND ?`,
    [dataSourceId, from, to, dataSourceId, from, to]
  );
  return rows[0] || {};
}

async function trendRows(connection, dataSourceId, from, to) {
  const rows = await connection.query(
    `SELECT y.report_date, y.views, y.estimated_minutes_watched,
            y.subscribers_gained, y.subscribers_lost, y.data_through_date,
            y.availability
     FROM youtube_analytics_daily_snapshots y
     JOIN (
       SELECT report_date, MAX(observed_at) AS observed_at
       FROM youtube_analytics_daily_snapshots
       WHERE data_source_id = ? AND report_date BETWEEN ? AND ?
       GROUP BY report_date
     ) latest ON latest.report_date = y.report_date AND latest.observed_at = y.observed_at
     WHERE y.data_source_id = ? AND y.report_date BETWEEN ? AND ?
     ORDER BY y.report_date ASC`,
    [dataSourceId, from, to, dataSourceId, from, to]
  );
  return rows.map(row => ({
    date: databaseDate(row.report_date),
    views: numeric(row.views),
    watch_time_minutes: numeric(row.estimated_minutes_watched),
    subscribers_gained: numeric(row.subscribers_gained),
    subscribers_lost: numeric(row.subscribers_lost),
    net_subscribers: row.subscribers_gained === null || row.subscribers_lost === null
      ? null
      : Number(row.subscribers_gained) - Number(row.subscribers_lost),
    availability: parseJson(row.availability, {})
  }));
}

async function videoRows(connection, dataSourceId, periodKey) {
  if (!periodKey) return [];
  const rows = await connection.query(
    `SELECT ci.id, ci.provider_content_id, ci.title, ci.published_at, ci.share_url,
            ci.provider_metadata, y.period_key, y.period_start, y.period_end,
            y.data_through_date, y.views, y.estimated_minutes_watched,
            y.average_view_duration, y.average_view_percentage,
            y.likes, y.comments, y.shares, y.availability
     FROM youtube_video_analytics_snapshots y
     JOIN content_items ci ON ci.id = y.content_item_id
     JOIN (
       SELECT content_item_id, MAX(observed_at) AS observed_at
       FROM youtube_video_analytics_snapshots
       WHERE data_source_id = ? AND period_key = ?
       GROUP BY content_item_id
     ) latest ON latest.content_item_id = y.content_item_id AND latest.observed_at = y.observed_at
     WHERE y.data_source_id = ? AND y.period_key = ?
     ORDER BY y.views IS NULL, y.views DESC, ci.published_at DESC
     LIMIT 200`,
    [dataSourceId, periodKey, dataSourceId, periodKey]
  );
  return rows.map(row => {
    const metadata = parseJson(row.provider_metadata, {});
    return {
      id: row.id,
      provider_content_id: row.provider_content_id,
      title: row.title,
      thumbnail_url: metadata.thumbnail_url || null,
      published_at: row.published_at,
      share_url: row.share_url,
      period: { key: row.period_key, from: databaseDate(row.period_start), to: databaseDate(row.period_end) },
      data_through_date: databaseDate(row.data_through_date),
      views: numeric(row.views),
      watch_time_minutes: numeric(row.estimated_minutes_watched),
      average_view_duration_seconds: numeric(row.average_view_duration),
      average_view_percentage: numeric(row.average_view_percentage),
      likes: numeric(row.likes),
      comments: numeric(row.comments),
      shares: numeric(row.shares),
      availability: parseJson(row.availability, {})
    };
  });
}

async function getYouTubeDashboard(userId, workspaceId, query = {}) {
  const range = parseRange(query);
  return withConnection(async connection => {
    await requireWorkspace(connection, workspaceId, userId);
    const foundationRows = await connection.query(
      `SELECT COUNT(*) AS count
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_NAME IN (
           'youtube_channel_snapshots',
           'youtube_analytics_daily_snapshots',
           'youtube_video_analytics_snapshots'
         )`
    );
    if (Number(foundationRows[0] && foundationRows[0].count) !== 3) {
      return {
        provider: 'youtube',
        range,
        connection: { status: 'disconnected' },
        channel: null,
        metrics: [],
        trend: [],
        content: [],
        availability: {
          state: 'configuration_required',
          data_through_date: null,
          requested_through_date: range.to,
          note: 'youtube_database_foundation_missing'
        }
      };
    }
    const selectedConnection = await findConnection(connection, workspaceId, query.connection_id || null);
    if (!selectedConnection) {
      return {
        provider: 'youtube',
        range,
        connection: { status: 'disconnected' },
        channel: null,
        metrics: [],
        trend: [],
        content: [],
        availability: { state: 'empty', data_through_date: null, requested_through_date: range.to }
      };
    }

    const [channelSnapshot, current, previous, trend, content] = await Promise.all([
      latestChannelSnapshot(connection, selectedConnection.data_source_id),
      aggregateRange(connection, selectedConnection.data_source_id, range.from, range.to),
      aggregateRange(connection, selectedConnection.data_source_id, range.previousFrom, range.previousTo),
      trendRows(connection, selectedConnection.data_source_id, range.from, range.to),
      videoRows(connection, selectedConnection.data_source_id, range.videoPeriodKey)
    ]);
    const metadata = parseJson(selectedConnection.metadata, {});
    const connectionStatus = selectedConnection.authorization_status === 'authorizing'
      ? 'connecting'
      : ['reconnect_required', 'disabled'].includes(selectedConnection.authorization_status)
        ? 'reconnect_required'
        : selectedConnection.status;
    const currentNet = current.subscribers_gained === null || current.subscribers_lost === null
      ? null
      : Number(current.subscribers_gained) - Number(current.subscribers_lost);
    const previousNet = previous.subscribers_gained === null || previous.subscribers_lost === null
      ? null
      : Number(previous.subscribers_gained) - Number(previous.subscribers_lost);
    const dataThrough = databaseDate(current.data_through_date) || databaseDate(selectedConnection.data_through_at);
    return {
      provider: 'youtube',
      range,
      connection: {
        id: selectedConnection.id,
        status: connectionStatus,
        reconnect_reason: selectedConnection.reconnect_reason,
        last_sync_at: selectedConnection.last_sync_at,
        last_successful_sync_at: selectedConnection.last_successful_sync_at,
        next_sync_at: selectedConnection.next_sync_at
      },
      channel: {
        id: selectedConnection.provider_resource_id,
        display_name: selectedConnection.display_name,
        thumbnail_url: metadata.thumbnailUrl || (channelSnapshot && channelSnapshot.thumbnail_url) || null,
        subscriber_count_hidden: Boolean(channelSnapshot && channelSnapshot.subscriber_count_hidden),
        availability: parseJson(channelSnapshot && channelSnapshot.availability, {})
      },
      metrics: [
        metric('subscribers_current', 'Subscribers', channelSnapshot && channelSnapshot.subscriber_count, null, 'current_snapshot'),
        metric('channel_views_lifetime', 'Channel views', channelSnapshot && channelSnapshot.lifetime_view_count, null, 'lifetime'),
        metric('videos_current', 'Videos', channelSnapshot && channelSnapshot.public_video_count, null, 'current_public_count'),
        metric('views_period', 'Views', current.views, previous.views, 'selected_period'),
        metric('watch_time_period', 'Watch time', current.estimated_minutes_watched, previous.estimated_minutes_watched, 'selected_period_minutes'),
        metric('net_subscribers_period', 'Net subscribers', currentNet, previousNet, 'selected_period')
      ],
      trend,
      content,
      availability: {
        state: !dataThrough ? 'delayed' : dataThrough < range.to ? 'delayed' : trend.length === 0 ? 'empty' : 'ready',
        data_through_date: dataThrough,
        requested_through_date: range.to,
        video_period_supported: Boolean(range.videoPeriodKey),
        note: !dataThrough || dataThrough < range.to ? 'youtube_reporting_delay' : null
      }
    };
  });
}

module.exports = {
  getYouTubeDashboard,
  parseRange
};
