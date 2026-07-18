const { getConnection } = require('../database');
const { compareMetric, resolveDateRange } = require('./analytics');
const { queryContentRows, requireWorkspaceCapability } = require('./dashboard-service');

const META_DASHBOARD_METRICS = Object.freeze({
  facebook_pages: Object.freeze([
    { key: 'page_follows', label: 'Page follows', aggregation: 'latest' },
    { key: 'page_daily_follows_unique', label: 'New follows', aggregation: 'sum' },
    { key: 'page_daily_unfollows_unique', label: 'Unfollows', aggregation: 'sum' },
    { key: 'page_post_engagements', label: 'Post engagements', aggregation: 'sum' },
    { key: 'page_media_view', label: 'Media views', aggregation: 'sum' },
    { key: 'page_total_media_view_unique', label: 'Unique media viewers', aggregation: 'sum' }
  ]),
  instagram: Object.freeze([
    { key: 'followers', label: 'Followers', aggregation: 'latest_profile' },
    { key: 'views', label: 'Views', aggregation: 'sum' },
    { key: 'reach', label: 'Reach', aggregation: 'sum' },
    { key: 'accounts_engaged', label: 'Accounts engaged', aggregation: 'sum' },
    { key: 'total_interactions', label: 'Interactions', aggregation: 'sum' },
    { key: 'likes', label: 'Likes', aggregation: 'sum' },
    { key: 'comments', label: 'Comments', aggregation: 'sum' },
    { key: 'saves', label: 'Saves', aggregation: 'sum' },
    { key: 'shares', label: 'Shares', aggregation: 'sum' }
  ])
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

function parseJson(value, fallback = {}) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function numeric(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function serializeDate(value) {
  return value ? new Date(value).toISOString() : null;
}

function dateOnly(value) {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value || '').slice(0, 10);
}

function previousRange(range) {
  const from = new Date(range.from);
  const to = new Date(range.to);
  const days = Math.max(1, Math.ceil((to.getTime() - from.getTime()) / (24 * 60 * 60 * 1000)) + 1);
  const previousTo = new Date(from);
  previousTo.setUTCDate(previousTo.getUTCDate() - 1);
  const previousFrom = new Date(previousTo);
  previousFrom.setUTCDate(previousFrom.getUTCDate() - days + 1);
  return { from: previousFrom, to: previousTo };
}

function supportedPeriodDays(range) {
  const days = Math.round((new Date(range.to).getTime() - new Date(range.from).getTime()) / (24 * 60 * 60 * 1000));
  return [7, 30, 90].includes(days) ? days : null;
}

function latestRowsByDate(rows) {
  const byDate = new Map();
  for (const row of rows) byDate.set(dateOnly(row.report_date), row);
  return [...byDate.values()].sort((left, right) => dateOnly(left.report_date).localeCompare(dateOnly(right.report_date)));
}

function aggregateMetric(rows, key, aggregation) {
  const values = rows
    .map(row => numeric(parseJson(row.metric_values, {})[key]))
    .filter(value => value !== null);
  if (values.length === 0) return null;
  return aggregation === 'latest' ? values[values.length - 1] : values.reduce((sum, value) => sum + value, 0);
}

async function findConnection(connection, workspaceId, provider, connectionId) {
  const params = [workspaceId, provider];
  const connectionClause = connectionId ? 'AND wpc.id = ?' : '';
  if (connectionId) params.push(connectionId);
  const rows = await connection.query(
    `SELECT wpc.*, ds.reconnect_reason, pr.provider_resource_id AS external_resource_id,
            pr.display_name, pr.metadata,
            pauth.status AS authorization_status,
            prc.access_expires_at, prc.revoked_at AS resource_token_revoked_at
     FROM workspace_provider_connections wpc
     JOIN data_sources ds ON ds.id = wpc.data_source_id
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     LEFT JOIN provider_resource_credentials prc ON prc.provider_resource_id = pr.id
     WHERE wpc.workspace_id = ? AND wpc.provider = ? ${connectionClause}
       AND ds.deleted_at IS NULL
     ORDER BY wpc.created_at ASC LIMIT 1`,
    params
  );
  return rows[0] || null;
}

async function dailyRows(connection, dataSourceId, from, to) {
  const rows = await connection.query(
    `SELECT report_date, observed_at, metric_values, availability
     FROM meta_account_insight_snapshots
     WHERE data_source_id = ? AND snapshot_kind = 'daily'
       AND report_date BETWEEN ? AND ?
     ORDER BY report_date ASC, observed_at ASC`,
    [dataSourceId, dateOnly(from), dateOnly(to)]
  );
  return latestRowsByDate(rows);
}

async function latestProfile(connection, dataSourceId, to) {
  const rows = await connection.query(
    `SELECT report_date, observed_at, metric_values, availability
     FROM meta_account_insight_snapshots
     WHERE data_source_id = ? AND snapshot_kind = 'profile' AND report_date <= ?
     ORDER BY report_date DESC, observed_at DESC LIMIT 1`,
    [dataSourceId, dateOnly(to)]
  );
  return rows[0] || null;
}

async function latestPeriod(connection, dataSourceId, rangeDays, to) {
  if (!rangeDays) return null;
  const rows = await connection.query(
    `SELECT report_date, range_days, range_start_date, range_end_date,
            observed_at, metric_values, availability
     FROM meta_account_insight_snapshots
     WHERE data_source_id = ? AND snapshot_kind = 'period' AND range_days = ?
       AND range_end_date <= ?
     ORDER BY range_end_date DESC, observed_at DESC LIMIT 1`,
    [dataSourceId, rangeDays, dateOnly(to)]
  );
  return rows[0] || null;
}

async function getMetaDashboard(userId, workspaceId, provider, query = {}) {
  if (!META_DASHBOARD_METRICS[provider]) throw createHttpError(400, 'meta_provider_invalid');
  const range = resolveDateRange(query);
  const previous = previousRange(range);
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'viewDashboard');
    const selected = await findConnection(connection, workspaceId, provider, query.connection_id || null);
    if (!selected) {
      return {
        provider,
        range: { from: range.from.toISOString(), to: range.to.toISOString() },
        connection: { status: 'disconnected' },
        account: null,
        metrics: [],
        trend: [],
        content: [],
        latest_sync: null,
        availability: {
          state: 'empty',
          note: provider === 'instagram' ? 'Stories are not collected without webhooks.' : null
        }
      };
    }
    const customRange = query.range === 'custom' || Boolean(query.from) || Boolean(query.to);
    const rangeDays = customRange ? null : supportedPeriodDays(range);
    const currentRows = provider === 'facebook_pages'
      ? await dailyRows(connection, selected.data_source_id, range.from, range.to)
      : [];
    const previousRows = provider === 'facebook_pages'
      ? await dailyRows(connection, selected.data_source_id, previous.from, previous.to)
      : [];
    const profile = await latestProfile(connection, selected.data_source_id, range.to);
    const previousProfile = await latestProfile(connection, selected.data_source_id, previous.to);
    const currentPeriod = provider === 'instagram'
      ? await latestPeriod(connection, selected.data_source_id, rangeDays, range.to)
      : null;
    const previousPeriod = provider === 'instagram'
      ? await latestPeriod(connection, selected.data_source_id, rangeDays, previous.to)
      : null;
    const profileValues = parseJson(profile && profile.metric_values, {});
    const previousProfileValues = parseJson(previousProfile && previousProfile.metric_values, {});
    const currentPeriodValues = parseJson(currentPeriod && currentPeriod.metric_values, {});
    const previousPeriodValues = parseJson(previousPeriod && previousPeriod.metric_values, {});
    const metrics = META_DASHBOARD_METRICS[provider].map(definition => {
      const current = definition.aggregation === 'latest_profile'
        ? numeric(profileValues.followers)
        : provider === 'instagram'
          ? numeric(currentPeriodValues[definition.key])
          : aggregateMetric(currentRows, definition.key, definition.aggregation);
      const baseline = definition.aggregation === 'latest_profile'
        ? numeric(previousProfileValues.followers)
        : provider === 'instagram'
          ? numeric(previousPeriodValues[definition.key])
          : aggregateMetric(previousRows, definition.key, definition.aggregation);
      return {
        key: definition.key,
        label: definition.label,
        ...compareMetric(current, baseline),
        available: current !== null,
        semantics: definition.aggregation === 'latest_profile'
          ? 'latest_profile_snapshot'
          : provider === 'instagram'
            ? rangeDays ? `provider_total_over_${rangeDays}_days` : 'unsupported_custom_period'
            : definition.aggregation === 'latest' ? 'latest_provider_snapshot' : 'sum_of_provider_daily_values'
      };
    });
    const content = await queryContentRows(connection, workspaceId, {
      provider,
      from: range.from,
      to: range.to,
      sort: query.top_sort || 'views',
      direction: 'desc',
      limit: 25,
      offset: 0
    });
    const syncRows = await connection.query(
      `SELECT id, trigger_type, status, started_at, finished_at, duration_ms,
              profile_count, content_seen_count, content_snapshot_count
       FROM sync_runs WHERE data_source_id = ? ORDER BY started_at DESC LIMIT 1`,
      [selected.data_source_id]
    );
    const resourceMetadata = parseJson(selected.metadata, {});
    const credentialExpiry = selected.access_expires_at
      ? new Date(selected.access_expires_at).getTime()
      : Number.NaN;
    const credentialUnavailable = Boolean(
      selected.resource_token_revoked_at ||
      !Number.isFinite(credentialExpiry) ||
      credentialExpiry <= Date.now()
    );
    return {
      provider,
      range: {
        from: range.from.toISOString(),
        to: range.to.toISOString(),
        previous_from: previous.from.toISOString(),
        previous_to: previous.to.toISOString(),
        provider_period_days: provider === 'instagram' ? rangeDays : null
      },
      connection: {
        id: selected.id,
        status: selected.authorization_status === 'authorizing'
          ? 'connecting'
          : selected.authorization_status !== 'active' || credentialUnavailable
            ? 'reconnect_required'
            : selected.status,
        reconnect_reason: selected.reconnect_reason || (credentialUnavailable ? 'resource_token_unavailable' : null),
        last_sync_at: serializeDate(selected.last_sync_at),
        last_successful_sync_at: serializeDate(selected.last_successful_sync_at),
        next_sync_at: serializeDate(selected.next_sync_at),
        data_through_at: serializeDate(selected.data_through_at)
      },
      account: {
        id: selected.external_resource_id,
        display_name: selected.display_name,
        username: resourceMetadata.username || null,
        thumbnail_url: resourceMetadata.thumbnailUrl || null,
        source_page_name: resourceMetadata.sourcePageName || null
      },
      metrics,
      trend: currentRows.map(row => ({
        date: dateOnly(row.report_date),
        ...parseJson(row.metric_values, {}),
        availability: parseJson(row.availability, {})
      })),
      content: content.rows,
      latest_sync: syncRows[0] ? {
        ...syncRows[0],
        started_at: serializeDate(syncRows[0].started_at),
        finished_at: serializeDate(syncRows[0].finished_at)
      } : null,
      availability: {
        state: currentRows.length > 0 || currentPeriod || profile ? 'available' : 'empty',
        note: provider === 'instagram'
          ? rangeDays
            ? 'Instagram account metrics are provider-reported totals for the selected 7, 30, or 90-day window. Stories are excluded because this slice does not request webhooks.'
            : 'Instagram account metrics are unavailable for custom ranges because Meta exposes most of them as total values, not daily time series. Content and profile snapshots remain available.'
          : null
      }
    };
  });
}

module.exports = {
  META_DASHBOARD_METRICS,
  aggregateMetric,
  getMetaDashboard,
  latestPeriod,
  latestRowsByDate,
  previousRange,
  supportedPeriodDays
};
