const { getDashboard } = require('./dashboard-service');
const { getGoogleAnalyticsDashboard } = require('./google-analytics-dashboard-service');
const { getMetaDashboard } = require('./meta-dashboard-service');
const { listWorkspaceProviderCatalog } = require('./provider-registry');
const { getYouTubeDashboard, parseRange } = require('./youtube-dashboard-service');

const PROVIDER_ORDER = Object.freeze([
  'tiktok',
  'youtube',
  'facebook_pages',
  'instagram',
  'google_analytics_4'
]);

const PROVIDER_NAMES = Object.freeze({
  tiktok: 'TikTok',
  youtube: 'YouTube',
  facebook_pages: 'Facebook Pages',
  instagram: 'Instagram',
  google_analytics_4: 'Website Analytics'
});

const METRIC_SELECTIONS = Object.freeze({
  tiktok: Object.freeze([
    { key: 'follower_count', label: 'Followers', family: 'audience', unit: 'count' },
    { key: 'likes_count', label: 'Total likes', family: 'provider_engagement', unit: 'count' },
    { key: 'video_count', label: 'Total videos', family: 'published_content', unit: 'count' }
  ]),
  youtube: Object.freeze([
    { key: 'subscribers_current', label: 'Subscribers', family: 'audience', unit: 'count' },
    { key: 'views_period', label: 'Views', family: 'provider_views', unit: 'count' },
    { key: 'watch_time_period', label: 'Watch time', family: 'watch_time', unit: 'minutes' },
    { key: 'net_subscribers_period', label: 'Net subscribers', family: 'audience_change', unit: 'count' }
  ]),
  facebook_pages: Object.freeze([
    { key: 'page_follows', label: 'Page follows', family: 'audience', unit: 'count' },
    { key: 'page_post_engagements', label: 'Post engagements', family: 'provider_engagement', unit: 'count' },
    { key: 'page_media_view', label: 'Media views', family: 'provider_views', unit: 'count' }
  ]),
  instagram: Object.freeze([
    { key: 'followers', label: 'Followers', family: 'audience', unit: 'count' },
    { key: 'views', label: 'Views', family: 'provider_views', unit: 'count' },
    { key: 'reach', label: 'Reach', family: 'provider_reach', unit: 'count' },
    { key: 'total_interactions', label: 'Interactions', family: 'provider_engagement', unit: 'count' }
  ]),
  google_analytics_4: Object.freeze([
    { key: 'ga4.active_users', label: 'Active users', family: 'website_audience', unit: 'count' },
    { key: 'ga4.sessions', label: 'Sessions', family: 'website_traffic', unit: 'count' },
    { key: 'ga4.screen_page_views', label: 'Views', family: 'website_views', unit: 'count' },
    { key: 'ga4.engagement_rate', label: 'Engagement rate', family: 'website_engagement', unit: 'ratio' }
  ])
});

const TREND_SELECTIONS = Object.freeze({
  tiktok: Object.freeze([
    { key: 'follower_count', label: 'Followers', unit: 'count' },
    { key: 'likes_count', label: 'Total likes', unit: 'count' }
  ]),
  youtube: Object.freeze([
    { key: 'views', label: 'Views', unit: 'count' },
    { key: 'watch_time_minutes', label: 'Watch time', unit: 'minutes' },
    { key: 'net_subscribers', label: 'Net subscribers', unit: 'count' }
  ]),
  facebook_pages: Object.freeze([
    { key: 'page_post_engagements', label: 'Post engagements', unit: 'count' },
    { key: 'page_media_view', label: 'Media views', unit: 'count' },
    { key: 'page_follows', label: 'Page follows', unit: 'count' }
  ]),
  instagram: Object.freeze([]),
  google_analytics_4: Object.freeze([
    { key: 'active_users', label: 'Active users', unit: 'count' },
    { key: 'sessions', label: 'Sessions', unit: 'count' },
    { key: 'screen_page_views', label: 'Views', unit: 'count' }
  ])
});

function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function dateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const match = String(value).match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : null;
}

function isoDate(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function metricAvailable(metric) {
  if (!metric) return false;
  if (metric.available === false) return false;
  if (metric.availability_status && metric.availability_status !== 'available') return false;
  return numberOrNull(metric.value) !== null;
}

function selectedMetrics(provider, dashboard) {
  const metrics = Array.isArray(dashboard && dashboard.metrics) ? dashboard.metrics : [];
  const byKey = new Map(metrics.map(metric => [metric.key, metric]));
  return (METRIC_SELECTIONS[provider] || []).map(definition => {
    const metric = byKey.get(definition.key) || {};
    return {
      key: definition.key,
      label: metric.label || definition.label || definition.key,
      family: definition.family,
      unit: metric.unit || definition.unit,
      value: numberOrNull(metric.value),
      baseline: numberOrNull(metric.baseline),
      delta: numberOrNull(metric.delta),
      percent_change: numberOrNull(metric.percent_change),
      available: metricAvailable(metric),
      availability_status: metric.availability_status || (metricAvailable(metric) ? 'available' : 'not_reported'),
      availability_reason: metric.availability_reason || null,
      semantics: metric.semantics || null,
      definition: metric.definition || null,
      definition_version: metric.definition_version || null
    };
  });
}

function normalizeRange(dashboard) {
  const range = dashboard && dashboard.range ? dashboard.range : {};
  return {
    key: range.key || null,
    from: dateOnly(range.from),
    to: dateOnly(range.to),
    previous_from: dateOnly(range.previousFrom || range.previous_from),
    previous_to: dateOnly(range.previousTo || range.previous_to),
    timezone: range.timezone || null,
    provider_period_days: range.provider_period_days || null
  };
}

function normalizeTrend(provider, dashboard) {
  const definitions = TREND_SELECTIONS[provider] || [];
  const rows = provider === 'tiktok'
    ? (dashboard && dashboard.trend || []).map(row => ({ ...row, date: dateOnly(row.observed_at) }))
    : dashboard && dashboard.trend || [];
  const points = rows.map(row => {
    const values = {};
    for (const definition of definitions) values[definition.key] = numberOrNull(row[definition.key]);
    return { date: dateOnly(row.date), values };
  }).filter(point => point.date);
  return {
    series: definitions.map(definition => ({ ...definition })),
    points
  };
}

function storedDimensionMetric(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value.status === 'available' ? numberOrNull(value.value) : null;
  }
  return numberOrNull(value);
}

function normalizeSocialContent(provider, dashboard) {
  const rows = provider === 'tiktok'
    ? dashboard && dashboard.top_content || []
    : dashboard && dashboard.content || [];
  return rows.slice(0, 3).map(row => ({
    id: row.id || row.provider_content_id,
    kind: 'social_content',
    title: row.title || row.description || 'Untitled content',
    published_at: isoDate(row.published_at),
    share_url: row.share_url || null,
    primary_metric: {
      key: 'views',
      label: 'Views',
      unit: 'count',
      value: numberOrNull(row.views === undefined ? row.view_count : row.views)
    }
  }));
}

function normalizeWebsiteContent(dashboard) {
  const groups = Array.isArray(dashboard && dashboard.breakdowns) ? dashboard.breakdowns : [];
  const group = groups.find(item => item.key === 'ga4.landing_page') ||
    groups.find(item => item.key === 'ga4.page_path_title');
  if (!group) return [];
  return group.rows.slice(0, 3).map((row, index) => {
    const dimensions = row.dimensions || {};
    const landingPage = dimensions.landingPagePlusQueryString || dimensions.pagePath || '(not set)';
    const title = dimensions.pageTitle && dimensions.pageTitle !== '(not set)'
      ? `${dimensions.pageTitle} — ${landingPage}`
      : landingPage;
    const sessionValue = storedDimensionMetric(row.metrics && row.metrics['ga4.sessions']);
    const viewValue = storedDimensionMetric(row.metrics && row.metrics['ga4.screen_page_views']);
    return {
      id: `${group.key}:${index}:${landingPage}`,
      kind: 'website_path',
      title,
      published_at: null,
      share_url: null,
      primary_metric: sessionValue !== null
        ? { key: 'ga4.sessions', label: 'Sessions', unit: 'count', value: sessionValue }
        : { key: 'ga4.screen_page_views', label: 'Views', unit: 'count', value: viewValue }
    };
  });
}

function normalizeTopContent(provider, dashboard) {
  return provider === 'google_analytics_4'
    ? normalizeWebsiteContent(dashboard)
    : normalizeSocialContent(provider, dashboard);
}

function connectionFor(provider, dashboard, catalogProvider) {
  const dashboardConnection = dashboard && dashboard.connection || {};
  const catalogConnection = catalogProvider && catalogProvider.connection || {};
  return { ...catalogConnection, ...dashboardConnection };
}

function resourceFor(provider, dashboard, catalogProvider, connectionId) {
  const catalogConnections = Array.isArray(catalogProvider && catalogProvider.connections)
    ? catalogProvider.connections
    : [];
  const catalogConnection = catalogConnections.find(connection => connection.id === connectionId) ||
    catalogProvider && catalogProvider.connection || null;
  if (provider === 'youtube' && dashboard && dashboard.channel) {
    return {
      connection_id: connectionId || dashboard.connection && dashboard.connection.id || null,
      id: dashboard.channel.id,
      display_name: dashboard.channel.display_name,
      account_name: null,
      timezone: null
    };
  }
  if ((provider === 'facebook_pages' || provider === 'instagram') && dashboard && dashboard.account) {
    return {
      connection_id: connectionId || dashboard.connection && dashboard.connection.id || null,
      id: dashboard.account.id,
      display_name: dashboard.account.display_name,
      account_name: dashboard.account.source_page_name || null,
      timezone: null
    };
  }
  if (provider === 'google_analytics_4' && dashboard && dashboard.property) {
    return {
      connection_id: connectionId || dashboard.connection && dashboard.connection.id || null,
      id: dashboard.property.id,
      display_name: dashboard.property.display_name,
      account_name: dashboard.property.account_name,
      timezone: dashboard.property.timezone
    };
  }
  const account = catalogConnection && catalogConnection.account;
  return account ? {
    connection_id: connectionId || catalogConnection.id || null,
    id: account.id,
    display_name: account.display_name || account.username || account.id,
    account_name: account.account_name || null,
    timezone: account.timezone || null
  } : null;
}

function dataThroughDate(provider, dashboard, connection) {
  const availability = dashboard && dashboard.availability || {};
  if (availability.data_through_date) return dateOnly(availability.data_through_date);
  if (connection.data_through_at) return dateOnly(connection.data_through_at);
  if (provider === 'instagram' && dashboard && dashboard.range && dashboard.range.to) {
    return dashboard.metrics && dashboard.metrics.some(metricAvailable) ? dateOnly(dashboard.range.to) : null;
  }
  return null;
}

function hasStoredData(provider, dashboard, metrics, topContent) {
  if (metrics.some(metric => metric.available)) return true;
  if (topContent.length > 0) return true;
  if (dashboard && dashboard.demo_data) return true;
  if (provider === 'instagram') return false;
  return Boolean(dashboard && dashboard.trend && dashboard.trend.length > 0);
}

function freshnessState({ status, availabilityState, lastSuccessfulSyncAt, hasData, demoData, latestSyncStatus, now }) {
  if (demoData && hasData) return 'sample';
  if (status === 'reconnect_required') return 'reconnect_required';
  if (latestSyncStatus === 'failed') return 'failed';
  if (latestSyncStatus === 'partial') return 'partial';
  if (['configuration_required', 'disabled', 'not_approved'].includes(availabilityState)) return 'configuration_required';
  if (['delayed', 'thresholded', 'partial'].includes(availabilityState)) return availabilityState;
  const lastSuccessful = lastSuccessfulSyncAt ? new Date(lastSuccessfulSyncAt).getTime() : Number.NaN;
  if (status === 'active' && Number.isFinite(lastSuccessful) && now.getTime() - lastSuccessful > 30 * 60 * 60 * 1000) {
    return 'stale';
  }
  if (status === 'active' && !hasData) return 'empty';
  if (hasData) return 'ready';
  if (['connecting', 'authorizing', 'selection_required'].includes(status)) return 'pending';
  return 'disconnected';
}

function alertFor(source) {
  const label = source.resource && source.resource.display_name
    ? `${source.provider_name} · ${source.resource.display_name}`
    : source.provider_name;
  const alerts = {
    reconnect_required: { severity: 'critical', code: 'reconnect_required', message: `${label} needs authorization before sync can resume.` },
    failed: { severity: 'critical', code: 'latest_sync_failed', message: `${label} did not complete its latest sync.` },
    stale: { severity: 'warning', code: 'stale_data', message: `${label} has not completed a successful sync within 30 hours.` },
    delayed: { severity: 'warning', code: 'provider_delay', message: `${label} is reporting data later than the selected range.` },
    thresholded: { severity: 'warning', code: 'privacy_thresholding', message: `${label} contains privacy-thresholded website analytics.` },
    partial: { severity: 'warning', code: 'partial_data', message: `${label} has some unavailable metrics.` },
    empty: { severity: 'info', code: 'awaiting_first_sync', message: `${label} is connected and waiting for stored analytics.` },
    pending: { severity: 'info', code: 'connection_pending', message: `${label} needs connection setup to finish.` }
  };
  return alerts[source.freshness.state] || null;
}

function buildSource(entry, now) {
  const { provider, dashboard, catalogProvider, connectionId } = entry;
  const metrics = selectedMetrics(provider, dashboard);
  const topContent = normalizeTopContent(provider, dashboard);
  const connection = connectionFor(provider, dashboard, catalogProvider);
  const status = connection.status || catalogProvider && catalogProvider.status || 'disconnected';
  const hasData = hasStoredData(provider, dashboard, metrics, topContent);
  const availabilityState = dashboard && dashboard.availability && dashboard.availability.state || null;
  const latestSyncStatus = dashboard && dashboard.latest_sync && dashboard.latest_sync.status || null;
  const freshness = {
    state: freshnessState({
      status,
      availabilityState,
      lastSuccessfulSyncAt: connection.last_successful_sync_at,
      hasData,
      demoData: Boolean(dashboard && dashboard.demo_data),
      latestSyncStatus,
      now
    }),
    last_successful_sync_at: isoDate(connection.last_successful_sync_at),
    data_through_date: dataThroughDate(provider, dashboard, connection),
    next_sync_at: isoDate(connection.next_sync_at)
  };
  const source = {
    id: `${provider}:${connectionId || connection.id || 'default'}`,
    provider,
    provider_name: PROVIDER_NAMES[provider] || provider,
    status,
    configuration_status: catalogProvider && catalogProvider.configuration
      ? catalogProvider.configuration.status
      : null,
    connected_resource_count: Array.isArray(catalogProvider && catalogProvider.connections)
      ? catalogProvider.connections.length
      : connection && connection.status !== 'disconnected' ? 1 : 0,
    resource: resourceFor(provider, dashboard, catalogProvider, connectionId),
    range: normalizeRange(dashboard),
    has_data: hasData,
    demo_data: Boolean(dashboard && dashboard.demo_data),
    freshness,
    metrics,
    trend: normalizeTrend(provider, dashboard),
    top_content: topContent,
    availability: {
      state: availabilityState,
      note: dashboard && dashboard.availability && dashboard.availability.note || null,
      subject_to_thresholding: Boolean(
        dashboard && dashboard.availability && dashboard.availability.subject_to_thresholding
      )
    },
    alert: null
  };
  source.alert = alertFor(source);
  return source;
}

function buildCrossPlatformDashboard({ entries, range, now = new Date() }) {
  const sources = entries
    .map(entry => buildSource(entry, now))
    .sort((left, right) => {
      const providerDifference = PROVIDER_ORDER.indexOf(left.provider) - PROVIDER_ORDER.indexOf(right.provider);
      if (providerDifference !== 0) return providerDifference;
      return String(left.resource && left.resource.display_name || '').localeCompare(
        String(right.resource && right.resource.display_name || '')
      );
    });
  const alerts = sources.filter(source => source.alert).map(source => ({ source_id: source.id, ...source.alert }));
  const critical = alerts.some(alert => alert.severity === 'critical');
  const hasData = sources.some(source => source.has_data);
  const materialAttention = alerts.some(alert => ['critical', 'warning'].includes(alert.severity));
  return {
    range,
    state: hasData ? materialAttention ? 'partial' : 'ready' : critical ? 'reconnect' : 'empty',
    demo_data: sources.some(source => source.demo_data),
    summary: {
      connected_resources: sources.filter(source => source.status === 'active').length,
      resources_with_data: sources.filter(source => source.has_data).length,
      attention_count: alerts.length
    },
    sources,
    alerts,
    methodology: [
      'Provider metrics are shown side by side and are never summed into a universal total.',
      'Each metric keeps its provider definition, unit, selected resource, reporting range, and availability state.',
      'Previous-period changes are shown only when a matching stored baseline exists.',
      'Delayed, missing, partial, and privacy-thresholded values remain explicitly unavailable.'
    ]
  };
}

async function runWithConcurrency(tasks, limit = 4) {
  const results = new Array(tasks.length);
  let index = 0;
  async function worker() {
    while (index < tasks.length) {
      const current = index;
      index += 1;
      results[current] = await tasks[current]();
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, () => worker()));
  return results;
}

function dashboardTasks(userId, workspaceId, query, catalog, loaders, requestedRange = null) {
  const catalogByProvider = new Map(catalog.map(provider => [provider.id, provider]));
  const tasks = [];
  const add = (provider, connectionId, load) => tasks.push(async () => ({
    provider,
    connectionId,
    catalogProvider: catalogByProvider.get(provider) || null,
    dashboard: await load()
  }));
  const exactRangeQuery = requestedRange ? {
    ...query,
    from: `${requestedRange.from}T00:00:00.000Z`,
    to: `${requestedRange.to}T23:59:59.999Z`
  } : query;
  add('tiktok', null, () => loaders.tiktok(userId, workspaceId, exactRangeQuery));
  for (const provider of PROVIDER_ORDER.slice(1)) {
    const catalogProvider = catalogByProvider.get(provider);
    const connections = Array.isArray(catalogProvider && catalogProvider.connections)
      ? catalogProvider.connections
      : [];
    const selectedConnections = connections.length > 0 ? connections : [{ id: null }];
    for (const connection of selectedConnections) {
      const providerQuery = connection.id ? { ...query, connection_id: connection.id } : query;
      if (provider === 'youtube') {
        add(provider, connection.id, () => loaders.youtube(userId, workspaceId, providerQuery));
      } else if (provider === 'google_analytics_4') {
        add(provider, connection.id, () => loaders.googleAnalytics(userId, workspaceId, providerQuery));
      } else {
        const metaQuery = provider === 'facebook_pages'
          ? { ...providerQuery, from: exactRangeQuery.from, to: exactRangeQuery.to }
          : providerQuery;
        add(provider, connection.id, () => loaders.meta(userId, workspaceId, provider, metaQuery));
      }
    }
  }
  return tasks;
}

async function getCrossPlatformDashboard(userId, workspaceId, query = {}, dependencies = {}) {
  const loaders = {
    catalog: dependencies.catalog || listWorkspaceProviderCatalog,
    tiktok: dependencies.tiktok || getDashboard,
    youtube: dependencies.youtube || getYouTubeDashboard,
    meta: dependencies.meta || getMetaDashboard,
    googleAnalytics: dependencies.googleAnalytics || getGoogleAnalyticsDashboard
  };
  const requestedRange = parseRange(query);
  const catalog = await loaders.catalog(userId, workspaceId);
  const entries = await runWithConcurrency(
    dashboardTasks(userId, workspaceId, query, catalog, loaders, requestedRange)
  );
  return buildCrossPlatformDashboard({
    entries,
    range: {
      key: requestedRange.key,
      from: requestedRange.from,
      to: requestedRange.to,
      previous_from: requestedRange.previousFrom,
      previous_to: requestedRange.previousTo,
      comparison: 'previous_period'
    }
  });
}

module.exports = {
  METRIC_SELECTIONS,
  PROVIDER_NAMES,
  PROVIDER_ORDER,
  TREND_SELECTIONS,
  buildCrossPlatformDashboard,
  buildSource,
  dashboardTasks,
  freshnessState,
  getCrossPlatformDashboard,
  normalizeTopContent,
  runWithConcurrency,
  selectedMetrics
};
