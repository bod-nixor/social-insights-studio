const { getConnection } = require('../database');
const { assertCapability } = require('./rbac');
const { getMetricDefinitions } = require('./provider-registry');
const { dateInTimeZone } = require('./google-analytics-sync-service');

const GA4_METRIC_KEYS = Object.freeze([
  'ga4.active_users',
  'ga4.new_users',
  'ga4.sessions',
  'ga4.screen_page_views',
  'ga4.engagement_rate',
  'ga4.bounce_rate',
  'ga4.average_session_duration',
  'ga4.sessions_per_user',
  'ga4.screen_page_views_per_user'
]);

const ADDITIVE_METRICS = new Set(['ga4.sessions', 'ga4.screen_page_views']);

const BREAKDOWN_LABELS = Object.freeze({
  'ga4.session_source_medium': 'Session source / medium',
  'ga4.page_path_title': 'Page path and title',
  'ga4.landing_page': 'Landing pages',
  'ga4.device_category': 'Device category',
  'ga4.country': 'Countries',
  'ga4.city': 'Cities'
});

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
     WHERE workspace_id = ? AND user_id = ? AND status = 'active' LIMIT 1`,
    [workspaceId, userId]
  );
  if (!rows[0]) throw createHttpError(404, 'workspace_not_found');
  assertCapability(rows[0].role, 'viewDashboard');
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

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(value, days) {
  const date = new Date(`${value}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function validDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ''))) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) && isoDate(parsed) === value;
}

function parseRange(query, timezone) {
  const key = ['7d', '30d', '90d', 'custom'].includes(query.range) ? query.range : '30d';
  const propertyToday = dateInTimeZone(timezone);
  let to = addDays(propertyToday, -1);
  let from;
  if (key === 'custom') {
    from = String(query.from || '');
    to = String(query.to || '');
    if (!validDate(from) || !validDate(to)) throw createHttpError(400, 'invalid_date_range');
  } else {
    from = addDays(to, -(Number(key.slice(0, -1)) - 1));
  }
  const fromTime = new Date(`${from}T00:00:00.000Z`).getTime();
  const toTime = new Date(`${to}T00:00:00.000Z`).getTime();
  const days = Math.floor((toTime - fromTime) / 86400000) + 1;
  if (fromTime > toTime || days < 1 || days > 366) throw createHttpError(400, 'invalid_date_range');
  return {
    key,
    from,
    to,
    days,
    previousFrom: addDays(from, -days),
    previousTo: addDays(from, -1),
    timezone
  };
}

function databaseDate(value) {
  if (!value) return null;
  if (value instanceof Date) return isoDate(value);
  const match = String(value).match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : null;
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

async function findConnection(connection, workspaceId, connectionId) {
  const params = [workspaceId];
  const clause = connectionId ? 'AND wpc.id = ?' : '';
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
     WHERE wpc.workspace_id = ? AND wpc.provider = 'google_analytics_4' ${clause}
     ORDER BY FIELD(wpc.status, 'active', 'reconnect_required', 'connecting', 'disconnected'), wpc.created_at
     LIMIT 1`,
    params
  );
  return rows[0] || null;
}

async function latestRangeMetrics(connection, connectionId, from, to) {
  const rows = await connection.query(
    `SELECT observation.metric_key, observation.numeric_value, observation.unit,
            observation.availability_status, observation.availability_reason,
            observation.definition_version, observation.data_through_at
     FROM provider_metric_observations observation
     JOIN (
       SELECT metric_key, MAX(observed_at) AS observed_at
       FROM provider_metric_observations
       WHERE workspace_provider_connection_id = ? AND grain = 'range'
         AND period_start = ? AND period_end = ?
       GROUP BY metric_key
     ) latest ON latest.metric_key = observation.metric_key
       AND latest.observed_at = observation.observed_at
     WHERE observation.workspace_provider_connection_id = ? AND observation.grain = 'range'
       AND observation.period_start = ? AND observation.period_end = ?`,
    [connectionId, from, to, connectionId, from, to]
  );
  return new Map(rows.map(row => [row.metric_key, row]));
}

async function dailyMetrics(connection, connectionId, from, to) {
  const rows = await connection.query(
    `SELECT observation.metric_key, observation.period_start AS report_date,
            observation.numeric_value, observation.unit, observation.availability_status,
            observation.availability_reason, observation.data_through_at
     FROM provider_metric_observations observation
     JOIN (
       SELECT metric_key, period_start, MAX(observed_at) AS observed_at
       FROM provider_metric_observations
       WHERE workspace_provider_connection_id = ? AND grain = 'daily'
         AND period_start BETWEEN ? AND ?
       GROUP BY metric_key, period_start
     ) latest ON latest.metric_key = observation.metric_key
       AND latest.period_start = observation.period_start
       AND latest.observed_at = observation.observed_at
     WHERE observation.workspace_provider_connection_id = ? AND observation.grain = 'daily'
       AND observation.period_start BETWEEN ? AND ?
     ORDER BY observation.period_start, observation.metric_key`,
    [connectionId, from, to, connectionId, from, to]
  );
  return rows;
}

function aggregateDaily(rows, metricKey) {
  const matching = rows.filter(row => row.metric_key === metricKey);
  if (matching.length === 0 || matching.some(row => row.availability_status !== 'available')) return null;
  return matching.reduce((sum, row) => sum + Number(row.numeric_value), 0);
}

function metricResult(key, exact, daily, previousExact, previousDaily) {
  const definition = getMetricDefinitions()[key];
  let value = exact && exact.availability_status === 'available' ? numberOrNull(exact.numeric_value) : null;
  let status = exact ? exact.availability_status : 'not_reported';
  let reason = exact ? exact.availability_reason : 'exact_range_not_stored';
  let baseline = previousExact && previousExact.availability_status === 'available'
    ? numberOrNull(previousExact.numeric_value)
    : null;
  let baselineStatus = previousExact ? previousExact.availability_status : 'not_reported';
  if (!exact && ADDITIVE_METRICS.has(key)) {
    value = aggregateDaily(daily, key);
    status = value === null ? 'not_reported' : 'available';
    reason = value === null ? 'daily_values_unavailable' : null;
  }
  if (!previousExact && ADDITIVE_METRICS.has(key)) {
    baseline = aggregateDaily(previousDaily, key);
    baselineStatus = baseline === null ? 'not_reported' : 'available';
  }
  const delta = value === null || baseline === null ? null : value - baseline;
  return {
    key,
    label: definition.label,
    unit: definition.unit,
    value,
    baseline,
    delta,
    percent_change: delta === null || baseline === 0 ? null : (delta / baseline) * 100,
    availability_status: status,
    availability_reason: reason,
    baseline_availability_status: baselineStatus,
    definition: definition.definition,
    definition_version: definition.version,
    available: status === 'available' && value !== null
  };
}

function trendRows(rows) {
  const dates = new Map();
  for (const row of rows) {
    const date = databaseDate(row.report_date);
    if (!date) continue;
    const value = dates.get(date) || { date, availability: {} };
    value[row.metric_key.replace('ga4.', '')] = row.availability_status === 'available'
      ? numberOrNull(row.numeric_value)
      : null;
    value.availability[row.metric_key] = {
      status: row.availability_status,
      reason: row.availability_reason
    };
    dates.set(date, value);
  }
  return [...dates.values()].sort((left, right) => left.date.localeCompare(right.date));
}

async function breakdownRows(connection, connectionId, from, to) {
  const rows = await connection.query(
    `SELECT observation.breakdown_key, observation.dimension_hash,
            observation.dimension_values, observation.metric_values,
            observation.availability, observation.thresholded, observation.row_position,
            observation.data_through_at
     FROM provider_dimension_observations observation
     JOIN (
       SELECT breakdown_key, dimension_hash, MAX(observed_at) AS observed_at
       FROM provider_dimension_observations
       WHERE workspace_provider_connection_id = ? AND period_start = ? AND period_end = ?
       GROUP BY breakdown_key, dimension_hash
     ) latest ON latest.breakdown_key = observation.breakdown_key
       AND latest.dimension_hash = observation.dimension_hash
       AND latest.observed_at = observation.observed_at
     WHERE observation.workspace_provider_connection_id = ?
       AND observation.period_start = ? AND observation.period_end = ?
     ORDER BY observation.breakdown_key, observation.row_position
     LIMIT 1500`,
    [connectionId, from, to, connectionId, from, to]
  );
  const groups = new Map();
  for (const row of rows) {
    const group = groups.get(row.breakdown_key) || {
      key: row.breakdown_key,
      label: BREAKDOWN_LABELS[row.breakdown_key] || row.breakdown_key,
      rows: [],
      subject_to_thresholding: false,
      data_through_date: null
    };
    group.rows.push({
      dimensions: parseJson(row.dimension_values, {}),
      metrics: parseJson(row.metric_values, {}),
      availability: parseJson(row.availability, {}),
      thresholded: Boolean(row.thresholded)
    });
    group.subject_to_thresholding ||= Boolean(row.thresholded);
    group.data_through_date = databaseDate(row.data_through_at) || group.data_through_date;
    groups.set(row.breakdown_key, group);
  }
  return [...groups.values()];
}

async function getGoogleAnalyticsDashboard(userId, workspaceId, query = {}) {
  return withConnection(async connection => {
    await requireWorkspace(connection, workspaceId, userId);
    const foundation = await connection.query(
      `SELECT COUNT(*) AS count FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_NAME IN ('provider_resource_observations', 'provider_metric_observations', 'provider_dimension_observations')`
    );
    if (Number(foundation[0] && foundation[0].count) !== 3) {
      const range = parseRange(query, 'UTC');
      return {
        provider: 'google_analytics_4', range, connection: { status: 'disconnected' },
        property: null, metrics: [], trend: [], breakdowns: [],
        availability: { state: 'configuration_required', data_through_date: null, requested_through_date: range.to }
      };
    }
    const selected = await findConnection(connection, workspaceId, query.connection_id || null);
    if (!selected) {
      const range = parseRange(query, 'UTC');
      return {
        provider: 'google_analytics_4', range, connection: { status: 'disconnected' },
        property: null, metrics: [], trend: [], breakdowns: [],
        availability: { state: 'empty', data_through_date: null, requested_through_date: range.to }
      };
    }
    const metadata = parseJson(selected.metadata, {});
    const timezone = metadata.timezone || 'UTC';
    if (query.timezone && query.timezone !== timezone) {
      throw createHttpError(400, 'ga4_dashboard_uses_property_timezone');
    }
    const range = parseRange(query, timezone);
    const [current, previous, daily, previousDaily, breakdowns] = await Promise.all([
      latestRangeMetrics(connection, selected.id, range.from, range.to),
      latestRangeMetrics(connection, selected.id, range.previousFrom, range.previousTo),
      dailyMetrics(connection, selected.id, range.from, range.to),
      dailyMetrics(connection, selected.id, range.previousFrom, range.previousTo),
      breakdownRows(connection, selected.id, range.from, range.to)
    ]);
    const metrics = GA4_METRIC_KEYS.map(key => metricResult(
      key, current.get(key), daily, previous.get(key), previousDaily
    ));
    const dataThrough = databaseDate(selected.data_through_at) ||
      [...current.values()].map(row => databaseDate(row.data_through_at)).filter(Boolean).sort().at(-1) || null;
    const anyThresholded = breakdowns.some(group => group.subject_to_thresholding);
    const anyUnavailable = metrics.some(item => !item.available);
    const connectionStatus = selected.authorization_status === 'authorizing'
      ? 'connecting'
      : ['reconnect_required', 'disabled'].includes(selected.authorization_status)
        ? 'reconnect_required'
        : selected.status;
    return {
      provider: 'google_analytics_4',
      range,
      connection: {
        id: selected.id,
        status: connectionStatus,
        reconnect_reason: selected.reconnect_reason,
        last_sync_at: selected.last_sync_at,
        last_successful_sync_at: selected.last_successful_sync_at,
        next_sync_at: selected.next_sync_at
      },
      property: {
        id: selected.provider_resource_id,
        display_name: selected.display_name,
        account_name: metadata.accountDisplayName || null,
        timezone,
        currency: metadata.currency || null,
        property_type: metadata.propertyType || null,
        service_level: metadata.serviceLevel || null
      },
      metrics,
      trend: trendRows(daily),
      breakdowns,
      availability: {
        state: !dataThrough
          ? 'delayed'
          : dataThrough < range.to
            ? 'delayed'
            : anyThresholded
              ? 'thresholded'
              : anyUnavailable ? 'partial' : 'ready',
        data_through_date: dataThrough,
        requested_through_date: range.to,
        subject_to_thresholding: anyThresholded,
        exact_range_available: current.size > 0,
        note: !dataThrough || dataThrough < range.to
          ? 'ga4_reporting_delay'
          : anyThresholded ? 'ga4_privacy_thresholding' : anyUnavailable ? 'ga4_partial_metrics' : null
      }
    };
  });
}

module.exports = {
  BREAKDOWN_LABELS,
  GA4_METRIC_KEYS,
  getGoogleAnalyticsDashboard,
  parseRange
};
