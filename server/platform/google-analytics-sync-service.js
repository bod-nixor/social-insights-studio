const { getConnection } = require('../database');
const ga4 = require('../integrations/google-analytics');
const { createDimensionObservation, createMetricObservation } = require('./observation-contract');
const { getMetricDefinitions } = require('./provider-registry');
const { decryptSecret, encryptSecret } = require('./secret-envelope');
const { createId } = require('./security');
const { purgeGoogleAnalyticsAuthorizationBySystem } = require('./google-analytics-connection-service');
const { getGoogleAnalyticsConfiguration, getGoogleAnalyticsLimits } = require('./google-analytics-config');

const DEFAULT_SYNC_INTERVAL_SECONDS = 6 * 60 * 60;
const RANGE_DAYS = Object.freeze([7, 30, 90]);
const GA4_API_VERSION = 'admin-v1beta/data-v1beta';

const METRIC_MAP = Object.freeze({
  activeUsers: Object.freeze({ key: 'ga4.active_users', unit: 'count' }),
  newUsers: Object.freeze({ key: 'ga4.new_users', unit: 'count' }),
  sessions: Object.freeze({ key: 'ga4.sessions', unit: 'count' }),
  screenPageViews: Object.freeze({ key: 'ga4.screen_page_views', unit: 'count' }),
  engagementRate: Object.freeze({ key: 'ga4.engagement_rate', unit: 'ratio' }),
  bounceRate: Object.freeze({ key: 'ga4.bounce_rate', unit: 'ratio' }),
  averageSessionDuration: Object.freeze({ key: 'ga4.average_session_duration', unit: 'seconds' }),
  sessionsPerUser: Object.freeze({ key: 'ga4.sessions_per_user', unit: 'ratio' }),
  screenPageViewsPerUser: Object.freeze({ key: 'ga4.screen_page_views_per_user', unit: 'ratio' })
});

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

function internalSyncError(error) {
  const value = error && error.syncError ? error.syncError : {};
  return {
    category: value.category || 'internal',
    provider_code: value.provider_code || null,
    retryable: value.retryable === true,
    terminal: value.terminal === true,
    retry_after_seconds: value.retry_after_seconds === undefined ? null : value.retry_after_seconds,
    message: (error && (error.code || error.message)) || 'ga4_sync_failed'
  };
}

function retryDelaySeconds(error) {
  if (error && error.retry_after_seconds) return Math.max(60, Number(error.retry_after_seconds));
  if (error && error.retryable) return 300;
  return Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS);
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

function numeric(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function addDays(value, days) {
  const date = new Date(`${value}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function dateInTimeZone(timezone, date = new Date()) {
  try {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).formatToParts(date);
    const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
    const result = `${values.year}-${values.month}-${values.day}`;
    if (/^\d{4}-\d{2}-\d{2}$/.test(result)) return result;
  } catch {
    // The property was validated during discovery; a later invalid value fails safely to UTC.
  }
  return isoDate(date);
}

function normalizeGa4Date(value) {
  const text = String(value || '');
  if (!/^\d{8}$/.test(text)) return null;
  const result = `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  const parsed = new Date(`${result}T00:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) || isoDate(parsed) !== result ? null : result;
}

function buildDateWindows(timezone, lookbackDays) {
  const propertyToday = dateInTimeZone(timezone);
  const endDate = addDays(propertyToday, -1);
  const dailyStart = addDays(endDate, -(lookbackDays - 1));
  const ranges = [];
  for (const days of RANGE_DAYS) {
    const currentStart = addDays(endDate, -(days - 1));
    const previousEnd = addDays(currentStart, -1);
    ranges.push({ key: `${days}d`, days, kind: 'current', startDate: currentStart, endDate });
    ranges.push({
      key: `${days}d_previous`,
      days,
      kind: 'previous',
      startDate: addDays(previousEnd, -(days - 1)),
      endDate: previousEnd
    });
  }
  return { propertyToday, endDate, dailyStart, ranges };
}

function expectedReportColumns(expected, field) {
  if (!expected || expected[field] === undefined) return null;
  if (!Array.isArray(expected[field]) || expected[field].some(name => typeof name !== 'string' || !name)) {
    throw createSyncError('ga4_report_request_malformed');
  }
  return expected[field];
}

function responseHeaderNames(body, field, allowMissing) {
  if (!Object.hasOwn(body, field)) {
    if (allowMissing) return [];
    throw createSyncError('ga4_report_response_malformed');
  }
  if (!Array.isArray(body[field])) throw createSyncError('ga4_report_response_malformed');
  return body[field].map(header => {
    if (!header || typeof header !== 'object' || Array.isArray(header) || typeof header.name !== 'string' || !header.name) {
      throw createSyncError('ga4_report_response_malformed');
    }
    return header.name;
  });
}

function reportValueArray(row, field) {
  if (!Object.hasOwn(row, field)) return [];
  if (!Array.isArray(row[field])) throw createSyncError('ga4_report_row_malformed');
  for (const value of row[field]) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw createSyncError('ga4_report_row_malformed');
    }
    if (Object.hasOwn(value, 'value') && typeof value.value !== 'string') {
      throw createSyncError('ga4_report_row_malformed');
    }
  }
  return row[field];
}

function optionalReportObject(body, field, fallback) {
  if (!Object.hasOwn(body, field)) return fallback;
  const value = body[field];
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw createSyncError('ga4_report_response_malformed');
  }
  return value;
}

function parseReportRows(body, expected = null) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw createSyncError('ga4_report_response_malformed');
  }
  const expectedDimensions = expectedReportColumns(expected, 'dimensions');
  const expectedMetrics = expectedReportColumns(expected, 'metrics');

  const hasNoReportTableFields =
    !Object.hasOwn(body, 'dimensionHeaders') &&
    !Object.hasOwn(body, 'metricHeaders') &&
    !Object.hasOwn(body, 'rows') &&
    !Object.hasOwn(body, 'rowCount');
  const hasKnownEmptyEnvelope =
    body.kind === 'analyticsData#runReport' &&
    Object.hasOwn(body, 'metadata') &&
    Object.hasOwn(body, 'propertyQuota');

  if (
    expectedDimensions !== null &&
    expectedMetrics !== null &&
    hasNoReportTableFields &&
    hasKnownEmptyEnvelope
  ) {
    return {
      dimensions: [...expectedDimensions],
      metrics: [...expectedMetrics],
      rows: [],
      rowCount: 0,
      metadata: optionalReportObject(body, 'metadata', {}),
      propertyQuota: optionalReportObject(body, 'propertyQuota', null)
    };
  }

  const dimensions = responseHeaderNames(body, 'dimensionHeaders', true);
  const metrics = responseHeaderNames(
    body,
    'metricHeaders',
    expectedMetrics !== null && expectedMetrics.length === 0
  );
  if (
    (expectedDimensions && (
      dimensions.length !== expectedDimensions.length ||
      dimensions.some((name, index) => name !== expectedDimensions[index])
    )) ||
    (expectedMetrics && (
      metrics.length !== expectedMetrics.length ||
      metrics.some((name, index) => name !== expectedMetrics[index])
    ))
  ) {
    throw createSyncError('ga4_report_response_malformed');
  }
  const sourceRows = Object.hasOwn(body, 'rows') ? body.rows : [];
  if (!Array.isArray(sourceRows)) throw createSyncError('ga4_report_response_malformed');
  const rows = sourceRows.map(row => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      throw createSyncError('ga4_report_row_malformed');
    }
    const dimensionValues = reportValueArray(row, 'dimensionValues');
    const metricValues = reportValueArray(row, 'metricValues');
    if (dimensionValues.length !== dimensions.length || metricValues.length !== metrics.length) {
      throw createSyncError('ga4_report_row_malformed');
    }
    const record = { dimensions: {}, metrics: {} };
    dimensions.forEach((name, index) => {
      record.dimensions[name] = String(dimensionValues[index] && dimensionValues[index].value || '');
    });
    metrics.forEach((name, index) => {
      record.metrics[name] = metricValues[index] && metricValues[index].value !== undefined
        ? metricValues[index].value
        : null;
    });
    return record;
  });
  let rowCount = rows.length;
  if (Object.hasOwn(body, 'rowCount')) {
    rowCount = body.rowCount;
    if (typeof rowCount !== 'number' || !Number.isSafeInteger(rowCount) || rowCount < rows.length) {
      throw createSyncError('ga4_report_response_malformed');
    }
  }
  return {
    dimensions,
    metrics,
    rows,
    rowCount,
    metadata: optionalReportObject(body, 'metadata', {}),
    propertyQuota: optionalReportObject(body, 'propertyQuota', null)
  };
}

function normalizeMetadata(body) {
  if (!body || !Array.isArray(body.dimensions) || !Array.isArray(body.metrics)) {
    throw createSyncError('ga4_metadata_response_malformed');
  }
  const dimensions = new Map();
  const metrics = new Map();
  for (const item of body.dimensions) {
    if (item && item.apiName) dimensions.set(String(item.apiName), item);
  }
  for (const item of body.metrics) {
    if (item && item.apiName) metrics.set(String(item.apiName), item);
  }
  return { dimensions, metrics };
}

function compatibilitySet(body, kind) {
  const field = kind === 'dimension' ? 'dimensionCompatibilities' : 'metricCompatibilities';
  const metadataField = kind === 'dimension' ? 'dimensionMetadata' : 'metricMetadata';
  if (!body || (body[field] !== undefined && !Array.isArray(body[field]))) {
    throw createSyncError('ga4_compatibility_response_malformed');
  }
  return new Set(
    (body[field] || [])
      .filter(item => item && item.compatibility === 'COMPATIBLE' && item[metadataField] && item[metadataField].apiName)
      .map(item => String(item[metadataField].apiName))
  );
}

function metadataAvailability(metadata, apiName, kind) {
  const source = kind === 'metric' ? metadata.metrics : metadata.dimensions;
  const item = source.get(apiName);
  if (!item) return { status: 'not_supported', reason: 'not_returned_by_ga4_metadata' };
  const blockedReasons = Array.isArray(item.blockedReasons) ? item.blockedReasons.filter(Boolean) : [];
  if (blockedReasons.length > 0) {
    return { status: 'not_granted', reason: `ga4_${String(blockedReasons[0]).toLowerCase()}` };
  }
  return { status: 'available', reason: null };
}

function quotaSummary(propertyQuota) {
  if (!propertyQuota || typeof propertyQuota !== 'object') return null;
  const names = [
    'tokensPerDay',
    'tokensPerHour',
    'concurrentRequests',
    'serverErrorsPerProjectPerHour',
    'potentiallyThresholdedRequestsPerHour',
    'tokensPerProjectPerHour'
  ];
  const summary = {};
  for (const name of names) {
    const source = propertyQuota[name];
    if (!source || typeof source !== 'object') continue;
    summary[name] = {
      consumed: Number(source.consumed || 0),
      remaining: Number(source.remaining || 0)
    };
  }
  return Object.keys(summary).length > 0 ? summary : null;
}

async function loadSource(connection, dataSourceId) {
  const rows = await connection.query(
    `SELECT ds.*, wpc.id AS workspace_provider_connection_id,
            pr.id AS provider_resource_row_id, pr.provider_resource_id AS property_name,
            pr.display_name AS property_display_name, pr.metadata AS resource_metadata,
            pauth.id AS provider_authorization_id, pauth.status AS authorization_status,
            pac.access_token_ciphertext, pac.access_token_iv, pac.access_token_tag,
            pac.refresh_token_ciphertext, pac.refresh_token_iv, pac.refresh_token_tag,
            pac.key_version, pac.access_expires_at, pac.refresh_expires_at, pac.revoked_at,
            pac.access_expires_at > DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 60 SECOND) AS access_token_fresh
     FROM data_sources ds
     JOIN workspace_provider_connections wpc ON wpc.data_source_id = ds.id
     JOIN provider_resources pr ON pr.id = wpc.provider_resource_id
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     WHERE ds.id = ? AND ds.provider = 'google_analytics_4' AND ds.deleted_at IS NULL
     LIMIT 1`,
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
     VALUES (?, ?, ?, ?, ?, 'google_analytics_4', ?, ?, 0, ?, ?, ?, ?, ?, ?)`,
    [
      createId(), source.workspace_id, source.provider_authorization_id,
      source.workspace_provider_connection_id, runId, details.category, details.method,
      details.pageNumber || null, details.itemCount === undefined ? null : details.itemCount,
      details.result && Number.isInteger(details.result.attempts) ? details.result.attempts : 1,
      details.status,
      details.result && details.result.error ? details.result.error.category : null,
      details.result ? details.result.retryAfterSeconds : null
    ]
  ));
}

async function callAndRecord(source, runId, details, fn) {
  const result = await fn();
  await recordRequestEvent(source, runId, {
    ...details,
    result,
    status: result.ok ? 'success' : 'failed'
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
  if (Number(source.access_token_fresh) === 1) return accessToken;
  if (!source.refresh_token_ciphertext) throw createSyncError('ga4_refresh_token_missing');
  const refreshToken = decryptSecret({
    ciphertext: source.refresh_token_ciphertext,
    iv: source.refresh_token_iv,
    tag: source.refresh_token_tag,
    keyVersion: source.key_version
  });
  const result = await ga4.refreshAccessToken(refreshToken, { deadlineMs });
  await recordRequestEvent(source, runId, {
    category: 'oauth', method: 'oauth.refresh', result, status: result.ok ? 'success' : 'failed'
  });
  if (!result.ok || !result.body || !result.body.access_token || Number(result.body.expires_in) <= 0) {
    throw createSyncError('ga4_credential_refresh_failed', result);
  }
  const nextRefreshToken = ga4.chooseRefreshToken(result.body.refresh_token, refreshToken);
  const access = encryptSecret(result.body.access_token);
  const refresh = encryptSecret(nextRefreshToken);
  const refreshRotated = Boolean(typeof result.body.refresh_token === 'string' && result.body.refresh_token.trim());
  const refreshTtl = result.body.refresh_token_expires_in ? Number(result.body.refresh_token_expires_in) : null;
  await withConnection(connection => connection.query(
    `UPDATE provider_authorization_credentials
     SET access_token_ciphertext = ?, access_token_iv = ?, access_token_tag = ?,
         refresh_token_ciphertext = ?, refresh_token_iv = ?, refresh_token_tag = ?,
         key_version = ?, token_type = ?,
         access_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
         refresh_expires_at = CASE
           WHEN ? IS NOT NULL THEN DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
           WHEN ? = 1 THEN NULL ELSE refresh_expires_at END,
         updated_at = UTC_TIMESTAMP(3)
     WHERE provider_authorization_id = ?`,
    [
      access.ciphertext, access.iv, access.tag,
      refresh.ciphertext, refresh.iv, refresh.tag, access.keyVersion,
      result.body.token_type || 'Bearer', Number(result.body.expires_in),
      refreshTtl, refreshTtl, refreshRotated, source.provider_authorization_id
    ]
  ));
  return result.body.access_token;
}

async function createSyncRun(source, triggerType, correlationId) {
  const id = createId();
  await withConnection(connection => connection.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, workspace_provider_connection_id,
       trigger_type, status, correlation_id, provider_api_version)
     VALUES (?, ?, ?, ?, ?, 'running', ?, ?)`,
    [
      id, source.workspace_id, source.id, source.workspace_provider_connection_id,
      triggerType, correlationId || null, GA4_API_VERSION
    ]
  ));
  return id;
}

async function fetchCompatibility(source, runId, accessToken, metadata, dimensions, metrics, deadlineMs, name) {
  const requestedDimensions = dimensions.filter(value => metadataAvailability(metadata, value, 'dimension').status === 'available');
  const requestedMetrics = metrics.filter(value => metadataAvailability(metadata, value, 'metric').status === 'available');
  if (requestedDimensions.length !== dimensions.length || requestedMetrics.length === 0) {
    return { dimensions: new Set(), metrics: new Set(), skipped: true };
  }
  const result = await callAndRecord(source, runId, {
    category: 'analytics_api', method: `checkCompatibility.${name}`
  }, () => ga4.checkCompatibility(
    accessToken,
    source.property_name,
    requestedDimensions,
    requestedMetrics,
    { deadlineMs }
  ));
  return {
    dimensions: compatibilitySet(result.body, 'dimension'),
    metrics: compatibilitySet(result.body, 'metric'),
    skipped: false
  };
}

async function fetchReport(source, runId, accessToken, report, deadlineMs, method) {
  const result = await callAndRecord(source, runId, {
    category: 'analytics_api', method
  }, () => ga4.runReport(accessToken, source.property_name, report, { deadlineMs }));
  return parseReportRows(result.body, {
    dimensions: report.dimensions || [],
    metrics: report.metrics || []
  });
}

async function fetchAnalytics(source, runId, accessToken, property, deadlineMs, limits) {
  const metadataResult = await callAndRecord(source, runId, {
    category: 'analytics_api', method: 'properties.getMetadata'
  }, () => ga4.getMetadata(accessToken, source.property_name, { deadlineMs }));
  const metadata = normalizeMetadata(metadataResult.body);
  const windows = buildDateWindows(property.timezone, limits.analyticsLookbackDays);
  const errors = [];
  let latestQuota = null;

  const summaryCompatibility = await fetchCompatibility(
    source, runId, accessToken, metadata, [], ga4.GA4_METRICS, deadlineMs, 'summary'
  );
  const dailyCompatibility = await fetchCompatibility(
    source, runId, accessToken, metadata, ['date'], ga4.GA4_METRICS, deadlineMs, 'daily'
  );
  const summaryMetrics = ga4.GA4_METRICS.filter(metric => summaryCompatibility.metrics.has(metric));
  const dailyMetrics = ga4.GA4_METRICS.filter(metric => dailyCompatibility.metrics.has(metric));

  let daily = { dimensions: ['date'], metrics: [], rows: [], metadata: {}, propertyQuota: null };
  if (dailyCompatibility.dimensions.has('date') && dailyMetrics.length > 0) {
    daily = await fetchReport(source, runId, accessToken, {
      dateRanges: [{ startDate: windows.dailyStart, endDate: windows.endDate }],
      dimensions: ['date'],
      metrics: dailyMetrics,
      limit: limits.analyticsLookbackDays + 5,
      orderBys: [{ dimension: { dimensionName: 'date' } }]
    }, deadlineMs, 'runReport.daily');
    latestQuota = quotaSummary(daily.propertyQuota) || latestQuota;
  }

  const rangeReports = [];
  for (const window of windows.ranges) {
    if (Date.now() >= deadlineMs) throw createSyncError('ga4_time_budget_exhausted');
    if (summaryMetrics.length === 0) {
      rangeReports.push({ window, report: null });
      continue;
    }
    try {
      const report = await fetchReport(source, runId, accessToken, {
        dateRanges: [{ startDate: window.startDate, endDate: window.endDate }],
        dimensions: [], metrics: summaryMetrics, limit: 1
      }, deadlineMs, `runReport.range.${window.key}`);
      latestQuota = quotaSummary(report.propertyQuota) || latestQuota;
      rangeReports.push({ window, report });
    } catch (error) {
      const normalized = internalSyncError(error);
      if (normalized.terminal || ['authentication', 'scope'].includes(normalized.category)) throw error;
      errors.push(normalized);
      rangeReports.push({ window, report: null });
    }
  }

  const breakdownReports = [];
  for (const breakdown of ga4.GA4_BREAKDOWNS) {
    let compatibility;
    try {
      compatibility = await fetchCompatibility(
        source, runId, accessToken, metadata,
        [...breakdown.dimensions], [...breakdown.metrics], deadlineMs,
        breakdown.key.replace('ga4.', '')
      );
    } catch (error) {
      const normalized = internalSyncError(error);
      if (normalized.terminal || ['authentication', 'scope'].includes(normalized.category)) throw error;
      errors.push(normalized);
      continue;
    }
    const compatibleDimensions = breakdown.dimensions.filter(value => compatibility.dimensions.has(value));
    const compatibleMetrics = breakdown.metrics.filter(value => compatibility.metrics.has(value));
    if (compatibleDimensions.length !== breakdown.dimensions.length || compatibleMetrics.length === 0) {
      breakdownReports.push({ breakdown, window: null, report: null, compatibility });
      continue;
    }
    for (const days of RANGE_DAYS) {
      const window = windows.ranges.find(value => value.days === days && value.kind === 'current');
      try {
        const report = await fetchReport(source, runId, accessToken, {
          dateRanges: [{ startDate: window.startDate, endDate: window.endDate }],
          dimensions: compatibleDimensions,
          metrics: compatibleMetrics,
          limit: limits.maxDimensionRows,
          orderBys: [{ metric: { metricName: compatibleMetrics[0] }, desc: true }]
        }, deadlineMs, `runReport.breakdown.${breakdown.key.replace('ga4.', '')}.${days}d`);
        latestQuota = quotaSummary(report.propertyQuota) || latestQuota;
        breakdownReports.push({ breakdown, window, report, compatibility });
      } catch (error) {
        const normalized = internalSyncError(error);
        if (normalized.terminal || ['authentication', 'scope'].includes(normalized.category)) throw error;
        errors.push(normalized);
        breakdownReports.push({ breakdown, window, report: null, compatibility });
      }
    }
  }
  return {
    metadata,
    windows,
    daily,
    dailyCompatibility,
    summaryCompatibility,
    rangeReports,
    breakdownReports,
    latestQuota,
    errors
  };
}

function metricAvailability(metadata, compatibility, apiName) {
  const metadataState = metadataAvailability(metadata, apiName, 'metric');
  if (metadataState.status !== 'available') return metadataState;
  if (!compatibility.metrics.has(apiName)) return { status: 'not_supported', reason: 'ga4_report_incompatible' };
  return { status: 'available', reason: null };
}

function createStoredMetric(apiName, grain, periodStart, periodEnd, rawValue, state) {
  const definition = METRIC_MAP[apiName];
  const registryDefinition = getMetricDefinitions()[definition.key];
  const value = state.status === 'available' ? numeric(rawValue) : null;
  const availabilityStatus = state.status === 'available' && value === null ? 'not_reported' : state.status;
  return createMetricObservation({
    provider: 'google_analytics_4',
    metricKey: definition.key,
    grain,
    periodStart,
    periodEnd,
    numericValue: availabilityStatus === 'available' ? value : null,
    unit: definition.unit,
    availabilityStatus,
    availabilityReason: availabilityStatus === 'available' ? state.reason : state.reason || 'ga4_value_not_reported',
    definitionVersion: registryDefinition.version
  });
}

function metricObservations(analytics) {
  const observations = [];
  for (const row of analytics.daily.rows) {
    const date = normalizeGa4Date(row.dimensions.date);
    if (!date) continue;
    for (const apiName of ga4.GA4_METRICS) {
      const state = metricAvailability(analytics.metadata, analytics.dailyCompatibility, apiName);
      observations.push(createStoredMetric(apiName, 'daily', date, date, row.metrics[apiName], state));
    }
  }
  for (const value of analytics.rangeReports) {
    const row = value.report && value.report.rows[0] ? value.report.rows[0] : { metrics: {} };
    const thresholded = Boolean(value.report && value.report.metadata.subjectToThresholding);
    for (const apiName of ga4.GA4_METRICS) {
      let state = metricAvailability(analytics.metadata, analytics.summaryCompatibility, apiName);
      if (state.status === 'available' && !value.report) {
        state = { status: thresholded ? 'thresholded' : 'provider_error', reason: thresholded ? 'ga4_thresholded' : 'ga4_report_unavailable' };
      }
      observations.push(createStoredMetric(
        apiName, 'range', value.window.startDate, value.window.endDate, row.metrics[apiName], state
      ));
    }
  }
  return observations;
}

function dimensionObservations(analytics) {
  const observations = [];
  for (const value of analytics.breakdownReports) {
    if (!value.window || !value.report) continue;
    const thresholded = Boolean(value.report.metadata.subjectToThresholding);
    value.report.rows.forEach((row, position) => {
      const dimensions = {};
      for (const name of value.breakdown.dimensions) {
        dimensions[name] = String(row.dimensions[name] || '(not set)').slice(0, 512);
      }
      const metrics = {};
      for (const apiName of value.breakdown.metrics) {
        const definition = METRIC_MAP[apiName];
        const state = metricAvailability(analytics.metadata, value.compatibility, apiName);
        const parsed = state.status === 'available' ? numeric(row.metrics[apiName]) : null;
        metrics[definition.key] = {
          value: state.status === 'available' && parsed !== null ? parsed : null,
          status: state.status === 'available' && parsed === null ? 'not_reported' : state.status,
          reason: state.status === 'available' && parsed === null ? 'ga4_value_not_reported' : state.reason
        };
      }
      observations.push(createDimensionObservation({
        provider: 'google_analytics_4',
        breakdownKey: value.breakdown.key,
        periodStart: value.window.startDate,
        periodEnd: value.window.endDate,
        dimensionValues: dimensions,
        metrics,
        thresholded,
        rowPosition: position
      }));
    });
  }
  return observations;
}

async function storeSyncResult(source, runId, property, analytics, startedMs) {
  const metrics = metricObservations(analytics);
  const dimensions = dimensionObservations(analytics);
  const validDailyDates = analytics.daily.rows
    .map(row => normalizeGa4Date(row.dimensions.date))
    .filter(Boolean)
    .sort();
  const dataThroughDate = validDailyDates[validDailyDates.length - 1] || null;
  const status = analytics.errors.length > 0 ? 'partial' : 'success';
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const resourceMetadata = {
        ...parseJson(source.resource_metadata, {}),
        displayName: property.displayName,
        timezone: property.timezone,
        currency: property.currency,
        propertyType: property.propertyType,
        serviceLevel: property.serviceLevel,
        selectable: true,
        discoveryStatus: 'available'
      };
      await connection.query(
        `UPDATE provider_resources SET display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [property.displayName, JSON.stringify(resourceMetadata), source.provider_resource_row_id]
      );
      await connection.query(
        `UPDATE provider_accounts SET display_name = ?, metadata = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE data_source_id = ?`,
        [property.displayName, JSON.stringify(resourceMetadata), source.id]
      );
      await connection.query(
        `INSERT INTO provider_resource_observations
          (id, workspace_id, workspace_provider_connection_id, sync_run_id, provider,
           observed_at, data_through_at, source_timezone, observed_values, availability)
         VALUES (?, ?, ?, ?, 'google_analytics_4', UTC_TIMESTAMP(3), ?, ?, ?, ?)`,
        [
          createId(), source.workspace_id, source.workspace_provider_connection_id, runId,
          dataThroughDate ? `${dataThroughDate} 23:59:59` : null,
          property.timezone,
          JSON.stringify({
            property_id: property.id,
            display_name: property.displayName,
            account: property.account,
            account_display_name: property.accountDisplayName,
            timezone: property.timezone,
            currency: property.currency,
            property_type: property.propertyType,
            service_level: property.serviceLevel
          }),
          JSON.stringify({
            state: dataThroughDate ? 'available' : 'delayed',
            subject_to_thresholding: Boolean(analytics.daily.metadata.subjectToThresholding),
            data_loss_from_other_row: Boolean(analytics.daily.metadata.dataLossFromOtherRow)
          })
        ]
      );
      for (const item of metrics) {
        await connection.query(
          `INSERT INTO provider_metric_observations
            (id, workspace_id, workspace_provider_connection_id, sync_run_id, provider,
             metric_key, grain, period_start, period_end, observed_at, data_through_at,
             numeric_value, unit, availability_status, availability_reason, definition_version)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?, ?)`,
          [
            createId(), source.workspace_id, source.workspace_provider_connection_id, runId,
            item.provider, item.metricKey, item.grain, item.periodStart, item.periodEnd,
            dataThroughDate ? `${dataThroughDate} 23:59:59` : null,
            item.numericValue, item.unit, item.availabilityStatus, item.availabilityReason,
            item.definitionVersion
          ]
        );
      }
      for (const item of dimensions) {
        await connection.query(
          `INSERT INTO provider_dimension_observations
            (id, workspace_id, workspace_provider_connection_id, sync_run_id, provider,
             breakdown_key, period_start, period_end, observed_at, data_through_at,
             dimension_hash, dimension_values, metric_values, availability, thresholded, row_position)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?, ?, ?, ?, ?, ?, ?)`,
          [
            createId(), source.workspace_id, source.workspace_provider_connection_id, runId,
            item.provider, item.breakdownKey, item.periodStart, item.periodEnd,
            dataThroughDate ? `${dataThroughDate} 23:59:59` : null,
            item.dimensionHash, JSON.stringify(item.dimensionValues), JSON.stringify(item.metricValues),
            JSON.stringify(item.availability), item.thresholded, item.rowPosition
          ]
        );
      }
      for (const error of analytics.errors.slice(0, 20)) {
        await connection.query(
          `INSERT INTO sync_errors
            (id, sync_run_id, category, provider_code, message, retryable)
           VALUES (?, ?, ?, ?, ?, ?)`,
          [
            createId(), runId, error.category, error.provider_code,
            String(error.message).slice(0, 512), Boolean(error.retryable)
          ]
        );
      }
      await connection.query(
        `UPDATE sync_runs SET status = ?, finished_at = UTC_TIMESTAMP(3), duration_ms = ?,
             profile_count = 1, content_seen_count = 0, content_snapshot_count = 0
         WHERE id = ?`,
        [status, Math.max(0, Date.now() - startedMs), runId]
      );
      const nextSeconds = Number(process.env.SYNC_INTERVAL_SECONDS || DEFAULT_SYNC_INTERVAL_SECONDS);
      await connection.query(
        `UPDATE data_sources SET status = 'active', reconnect_reason = NULL,
             last_sync_at = UTC_TIMESTAMP(3),
             last_successful_sync_at = CASE WHEN ? = 'success' THEN UTC_TIMESTAMP(3) ELSE last_successful_sync_at END,
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [status, nextSeconds, source.id]
      );
      await connection.query(
        `UPDATE workspace_provider_connections SET status = 'active', last_sync_at = UTC_TIMESTAMP(3),
             last_successful_sync_at = CASE WHEN ? = 'success' THEN UTC_TIMESTAMP(3) ELSE last_successful_sync_at END,
             next_sync_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), data_through_at = ?,
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [
          status, nextSeconds, dataThroughDate ? `${dataThroughDate} 23:59:59` : null,
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE sync_jobs SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
        [nextSeconds, source.id]
      );
      await connection.query(
        `UPDATE provider_sync_states SET cursor_state = ?, last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = CASE WHEN ? = 'success' THEN UTC_TIMESTAMP(3) ELSE last_success_at END,
             data_through_at = ?, failure_category = ?,
             failure_count = CASE WHEN ? = 'success' THEN 0 ELSE failure_count + 1 END
         WHERE workspace_provider_connection_id = ? AND sync_key = 'ga4.reports'`,
        [
          JSON.stringify({ property_quota: analytics.latestQuota, range_days: RANGE_DAYS }),
          status, dataThroughDate ? `${dataThroughDate} 23:59:59` : null,
          analytics.errors[0] ? analytics.errors[0].category : null,
          status, source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_sync_states SET cursor_state = ?, last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = UTC_TIMESTAMP(3), failure_category = NULL, failure_count = 0
         WHERE workspace_provider_connection_id = ? AND sync_key = 'ga4.compatibility'`,
        [
          JSON.stringify({
            metric_count: analytics.metadata.metrics.size,
            dimension_count: analytics.metadata.dimensions.size,
            checked_at: new Date().toISOString()
          }),
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_sync_states SET cursor_state = ?, last_attempt_at = UTC_TIMESTAMP(3),
             last_success_at = UTC_TIMESTAMP(3), failure_category = NULL, failure_count = 0
         WHERE workspace_provider_connection_id = ? AND sync_key = 'ga4.property'`,
        [
          JSON.stringify({ timezone: property.timezone, currency: property.currency }),
          source.workspace_provider_connection_id
        ]
      );
      await connection.query(
        `UPDATE provider_capabilities SET status = CASE
             WHEN capability_key = 'dimension_breakdowns' AND ? > 0 THEN 'delayed'
             ELSE 'available' END,
             reason = CASE WHEN capability_key = 'dimension_breakdowns' AND ? > 0
               THEN 'ga4_partial_breakdowns' ELSE NULL END,
             updated_at = UTC_TIMESTAMP(3)
         WHERE workspace_provider_connection_id = ?`,
        [analytics.errors.length, analytics.errors.length, source.workspace_provider_connection_id]
      );
      await connection.query(
        `UPDATE provider_authorizations SET status = 'active', last_validated_at = UTC_TIMESTAMP(3),
             updated_at = UTC_TIMESTAMP(3) WHERE id = ?`,
        [source.provider_authorization_id]
      );
      await connection.commit();
      return { status, dataThroughDate, metricCount: metrics.length, dimensionCount: dimensions.length };
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
        `UPDATE sync_runs SET status = 'failed', finished_at = UTC_TIMESTAMP(3), duration_ms = ? WHERE id = ?`,
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
      const reconnect = ['authentication', 'scope'].includes(syncError.category);
      const retrySeconds = retryDelaySeconds(syncError);
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
        `UPDATE sync_jobs SET status = ?, run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
        [reconnect ? 'paused' : 'due', retrySeconds, source.id]
      );
      await connection.query(
        `UPDATE provider_capabilities SET status = ?, reason = ?, updated_at = UTC_TIMESTAMP(3)
         WHERE workspace_provider_connection_id = ?`,
        [reconnect ? 'not_granted' : 'provider_error', syncError.message, source.workspace_provider_connection_id]
      );
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

function propertyFromResponse(source, body) {
  if (!body || body.name !== source.property_name || !body.displayName || !body.timeZone || !body.currencyCode) {
    throw createSyncError('ga4_property_response_malformed');
  }
  return {
    id: body.name,
    displayName: String(body.displayName).slice(0, 255),
    account: body.account || null,
    accountDisplayName: parseJson(source.resource_metadata, {}).accountDisplayName || null,
    timezone: String(body.timeZone),
    currency: String(body.currencyCode).toUpperCase(),
    propertyType: body.propertyType || null,
    serviceLevel: body.serviceLevel || null
  };
}

async function performGoogleAnalyticsSyncForJob(job, options = {}) {
  const startedMs = Date.now();
  const configuration = getGoogleAnalyticsConfiguration();
  if (!configuration.connectable) {
    await withConnection(connection => connection.query(
      `UPDATE sync_jobs SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
           lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
           updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
      [DEFAULT_SYNC_INTERVAL_SECONDS, job.data_source_id]
    ));
    return {
      data_source_id: job.data_source_id,
      sync_run_id: null,
      status: 'disabled',
      error: { category: 'configuration', provider_code: 'ga4_not_available', retryable: false, message: 'ga4_not_available' },
      counts: { profile_count: 0, content_seen_count: 0, content_snapshot_count: 0 }
    };
  }
  const limits = getGoogleAnalyticsLimits();
  const localDeadline = startedMs + limits.jobTimeBudgetSeconds * 1000;
  const deadlineMs = options.deadlineMs ? Math.min(options.deadlineMs, localDeadline) : localDeadline;
  let source = null;
  let runId = null;
  try {
    source = await withConnection(connection => loadSource(connection, job.data_source_id));
    if (!source || source.status !== 'active' || source.authorization_status !== 'active' || source.revoked_at) {
      throw createSyncError('ga4_source_not_syncable');
    }
    runId = await createSyncRun(source, options.triggerType || 'scheduled', options.correlationId);
    const accessToken = await refreshCredentialsIfNeeded(source, runId, deadlineMs);
    const propertyResult = await callAndRecord(source, runId, {
      category: 'data_api', method: 'properties.get'
    }, () => ga4.getProperty(accessToken, source.property_name, { deadlineMs }));
    const property = propertyFromResponse(source, propertyResult.body);
    const analytics = await fetchAnalytics(source, runId, accessToken, property, deadlineMs, limits);
    const result = await storeSyncResult(source, runId, property, analytics, startedMs);
    return {
      data_source_id: source.id,
      sync_run_id: runId,
      status: result.status,
      error: analytics.errors[0] || null,
      counts: { profile_count: 1, content_seen_count: 0, content_snapshot_count: 0 },
      metric_observation_count: result.metricCount,
      dimension_observation_count: result.dimensionCount,
      data_through_date: result.dataThroughDate
    };
  } catch (error) {
    const syncError = internalSyncError(error);
    if (source && (syncError.terminal || ['authentication', 'scope'].includes(syncError.category))) {
      const outcome = syncError.provider_code === 'invalid_grant'
        ? 'invalid_grant_external_revocation'
        : 'authorization_unusable_external_revocation';
      await purgeGoogleAnalyticsAuthorizationBySystem(source.provider_authorization_id, outcome);
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
      const retrySeconds = retryDelaySeconds(syncError);
      await withConnection(connection => connection.query(
        `UPDATE sync_jobs SET status = 'due', run_after = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             lease_owner = NULL, lease_expires_at = NULL, requested_trigger_type = 'scheduled',
             updated_at = UTC_TIMESTAMP(3) WHERE data_source_id = ?`,
        [retrySeconds, job.data_source_id]
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
  GA4_API_VERSION,
  METRIC_MAP,
  RANGE_DAYS,
  buildDateWindows,
  compatibilitySet,
  dateInTimeZone,
  normalizeGa4Date,
  normalizeMetadata,
  parseReportRows,
  performGoogleAnalyticsSyncForJob,
  quotaSummary,
  retryDelaySeconds
};
