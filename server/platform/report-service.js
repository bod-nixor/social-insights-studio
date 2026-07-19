const crypto = require('crypto');
const fs = require('fs/promises');
const { getConnection } = require('../database');
const { getCrossPlatformDashboard } = require('./cross-platform-dashboard-service');
const { getMetricDefinitions } = require('./provider-registry');
const { assertCapability } = require('./rbac');
const { getReportConfiguration } = require('./report-config');
const {
  assertUuid,
  removeStoredArtifact,
  resolveStoragePath
} = require('./report-storage');
const { createId, hashSecret, randomToken } = require('./security');
const { parseRange } = require('./youtube-dashboard-service');

const REPORT_SECTIONS = Object.freeze([
  'executive_summary',
  'cross_platform_summary',
  'resource_sections',
  'methodology'
]);
const PROVIDERS = new Set(['tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4']);

function createReportError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  return error;
}

function parseJson(value, fallback = null) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function isoDateTime(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function dateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const match = String(value).match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : null;
}

function getReadyConfiguration(env = process.env) {
  const configuration = getReportConfiguration(env);
  if (!configuration.enabled) throw createReportError(503, 'pdf_reports_disabled');
  if (!configuration.ready) throw createReportError(503, 'pdf_reports_not_configured');
  return configuration;
}

function getPublicReportConfiguration(env = process.env) {
  const configuration = getReportConfiguration(env);
  return {
    enabled: configuration.enabled,
    ready: configuration.ready,
    retention_days: configuration.retentionDays,
    max_resources: configuration.maxResources,
    max_range_days: configuration.maxRangeDays,
    supported_sections: REPORT_SECTIONS,
    renderer: 'Pure Node PDF worker'
  };
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) throw createReportError(503, 'database_not_configured');
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
}

async function requireReportCapability(connection, workspaceId, userId) {
  const rows = await connection.query(
    `SELECT wm.role, w.name
     FROM workspace_memberships wm
     JOIN workspaces w ON w.id = wm.workspace_id AND w.deleted_at IS NULL
     WHERE wm.workspace_id = ? AND wm.user_id = ? AND wm.status = 'active'
     LIMIT 1`,
    [workspaceId, userId]
  );
  if (!rows[0]) throw createReportError(404, 'workspace_not_found');
  assertCapability(rows[0].role, 'manageReports');
  return rows[0];
}

function cleanInputText(value, maximum, required, code) {
  const result = String(value || '')
    .split('')
    .map(character => {
      const characterCode = character.charCodeAt(0);
      return characterCode <= 31 || characterCode === 127 ? ' ' : character;
    })
    .join('')
    .replace(/\s+/g, ' ')
    .trim();
  if ((required && !result) || result.length > maximum) throw createReportError(400, code);
  return result || null;
}

function validTimezone(timezone) {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: timezone }).format(new Date());
    return true;
  } catch {
    return false;
  }
}

function normalizeReportRequest(body = {}, configuration = getReadyConfiguration()) {
  const title = cleanInputText(body.title, 180, true, 'invalid_report_title');
  const subtitle = cleanInputText(body.subtitle, 300, false, 'invalid_report_subtitle');
  const timezone = String(body.timezone || 'UTC').trim();
  if (timezone.length > 64 || !validTimezone(timezone)) throw createReportError(400, 'invalid_report_timezone');
  const requestedRange = String(body.range || '30d');
  const range = parseRange({ range: requestedRange, from: body.from, to: body.to });
  if (range.days > configuration.maxRangeDays) throw createReportError(400, 'report_range_too_large');
  const resources = Array.isArray(body.resources) ? body.resources : [];
  if (resources.length < 1 || resources.length > configuration.maxResources) {
    throw createReportError(400, 'invalid_report_resource_count');
  }
  const seen = new Set();
  const normalizedResources = resources.map(resource => {
    const provider = String(resource && resource.provider || '');
    const connectionId = assertUuid(resource && resource.connection_id, 'invalid_report_connection');
    if (!PROVIDERS.has(provider)) throw createReportError(400, 'invalid_report_provider');
    const key = `${provider}:${connectionId}`;
    if (seen.has(key)) throw createReportError(400, 'duplicate_report_resource');
    seen.add(key);
    return { provider, connection_id: connectionId };
  });
  const requestedSections = Array.isArray(body.sections) && body.sections.length
    ? body.sections.map(value => String(value))
    : [...REPORT_SECTIONS];
  if (requestedSections.some(section => !REPORT_SECTIONS.includes(section))) {
    throw createReportError(400, 'invalid_report_section');
  }
  const sections = REPORT_SECTIONS.filter(section => requestedSections.includes(section));
  if (!sections.includes('resource_sections')) sections.push('resource_sections');
  const requestId = body.request_id === undefined ? null : String(body.request_id || '');
  if (requestId && !/^[A-Za-z0-9_-]{8,128}$/.test(requestId)) throw createReportError(400, 'invalid_report_request_id');
  return {
    title,
    subtitle,
    timezone,
    range: range.key,
    from: range.from,
    to: range.to,
    previous_from: range.previousFrom,
    previous_to: range.previousTo,
    comparison_enabled: body.comparison_enabled !== false,
    sections,
    resources: normalizedResources,
    request_id: requestId
  };
}

async function resolveResources(connection, workspaceId, requested) {
  const placeholders = requested.map(() => '?').join(', ');
  const rows = await connection.query(
    `SELECT wpc.id AS connection_id, wpc.provider, wpc.status, wpc.data_through_at,
            pr.provider_resource_id, pr.display_name
     FROM workspace_provider_connections wpc
     JOIN provider_resources pr
       ON pr.id = wpc.provider_resource_id
      AND pr.workspace_id = wpc.workspace_id
      AND pr.provider = wpc.provider
     WHERE wpc.workspace_id = ? AND wpc.id IN (${placeholders})`,
    [workspaceId, ...requested.map(resource => resource.connection_id)]
  );
  const byId = new Map(rows.map(row => [row.connection_id, row]));
  return requested.map(resource => {
    const row = byId.get(resource.connection_id);
    if (!row || row.provider !== resource.provider) throw createReportError(404, 'report_resource_not_found');
    if (['disconnected', 'revoked', 'disabled'].includes(row.status)) {
      throw createReportError(409, 'report_resource_not_connected');
    }
    return {
      connection_id: row.connection_id,
      provider: row.provider,
      status: row.status,
      provider_resource_id: row.provider_resource_id,
      resource_name: row.display_name,
      data_through_at: row.data_through_at || null
    };
  });
}

function sourceConnectionId(source) {
  return source && source.resource && source.resource.connection_id || null;
}

function stripUnrenderedFields(source, configuration) {
  const result = JSON.parse(JSON.stringify(source));
  result.top_content = (Array.isArray(result.top_content) ? result.top_content : [])
    .slice(0, configuration.maxContentRowsPerResource)
    .map(row => ({ ...row, share_url: null }));
  result.trend = result.trend || { series: [], points: [] };
  if (Array.isArray(result.trend.points)) result.trend.points = result.trend.points.slice(0, configuration.maxRangeDays);
  return result;
}

function earliestDataThrough(sources) {
  const dates = sources
    .map(source => source && source.freshness && source.freshness.data_through_date)
    .filter(Boolean)
    .map(value => new Date(`${value}T23:59:59.000Z`))
    .filter(value => !Number.isNaN(value.getTime()))
    .sort((left, right) => left - right);
  return dates[0] || null;
}

async function buildReportSnapshot({ userId, workspaceId, input, resources, configuration, workspaceName }) {
  const dashboard = await getCrossPlatformDashboard(userId, workspaceId, {
    range: input.range,
    from: input.from,
    to: input.to
  });
  const sources = resources.map(resource => {
    const source = (dashboard.sources || []).find(candidate => (
      candidate.provider === resource.provider && sourceConnectionId(candidate) === resource.connection_id
    ));
    if (!source) throw createReportError(409, 'report_source_snapshot_unavailable');
    return stripUnrenderedFields(source, configuration);
  });
  const definitions = getMetricDefinitions();
  const selectedProviders = new Set(resources.map(resource => resource.provider));
  const metricDefinitions = Object.fromEntries(
    Object.entries(definitions).filter(([, definition]) => selectedProviders.has(definition.provider))
  );
  const snapshot = {
    snapshot_version: '1',
    renderer_version: configuration.rendererVersion,
    captured_at: new Date().toISOString(),
    workspace: { id: workspaceId, name: workspaceName },
    report: {
      title: input.title,
      subtitle: input.subtitle,
      timezone: input.timezone,
      range: {
        key: input.range,
        from: input.from,
        to: input.to,
        previous_from: input.previous_from,
        previous_to: input.previous_to
      },
      comparison_enabled: input.comparison_enabled,
      sections: input.sections
    },
    dashboard: {
      range: dashboard.range,
      state: dashboard.state,
      summary: dashboard.summary,
      sources,
      alerts: (dashboard.alerts || []).filter(alert => sources.some(source => source.id === alert.source_id)),
      methodology: dashboard.methodology
    },
    resources,
    metric_definitions: metricDefinitions
  };
  const encoded = JSON.stringify(snapshot);
  if (Buffer.byteLength(encoded) > configuration.maxSnapshotBytes) {
    throw createReportError(413, 'report_snapshot_too_large');
  }
  return { snapshot, metricDefinitions, dataThroughAt: earliestDataThrough(sources) };
}

function reportStatus(status, expiresAt, artifactUnexpired = true) {
  if (status === 'complete' && expiresAt && !artifactUnexpired) return 'expired';
  return status === 'complete' ? 'completed' : status;
}

function serializeReport(row, resources = []) {
  const configuration = parseJson(row.configuration_snapshot, {});
  return {
    id: row.id,
    definition_id: row.report_definition_id,
    title: row.title,
    subtitle: row.subtitle,
    timezone: row.timezone,
    range: { from: dateOnly(row.range_start), to: dateOnly(row.range_end) },
    comparison_enabled: Boolean(row.comparison_enabled),
    status: reportStatus(row.status, row.expires_at, Boolean(row.artifact_unexpired)),
    progress_percent: Number(row.progress_percent || 0),
    failure_category: row.failure_category,
    failure_code: row.failure_code,
    queued_at: isoDateTime(row.queued_at),
    started_at: isoDateTime(row.started_at),
    finished_at: isoDateTime(row.finished_at),
    expires_at: isoDateTime(row.expires_at),
    data_through_at: isoDateTime(row.data_through_at),
    artifact: row.artifact_id && row.artifact_status === 'active' && Boolean(row.artifact_unexpired) ? {
      id: row.artifact_id,
      filename: row.download_filename,
      byte_size: Number(row.byte_size),
      page_count: Number(row.page_count),
      sha256: row.sha256
    } : null,
    sections: configuration.report && configuration.report.sections || [],
    resources
  };
}

async function fetchReportRows(connection, workspaceId, reportRunId = null) {
  const params = [workspaceId];
  const runClause = reportRunId ? 'AND rr.id = ?' : '';
  if (reportRunId) params.push(reportRunId);
  const rows = await connection.query(
    `SELECT rr.*, rd.title, rd.subtitle, rd.timezone, rd.range_start, rd.range_end,
            rd.comparison_enabled, rd.deleted_at AS definition_deleted_at,
            ra.id AS artifact_id, ra.download_filename, ra.byte_size, ra.page_count,
            ra.sha256, ra.expires_at, ra.status AS artifact_status,
            CASE WHEN ra.expires_at > UTC_TIMESTAMP(3) THEN 1 ELSE 0 END AS artifact_unexpired
     FROM report_runs rr
     JOIN report_definitions rd ON rd.id = rr.report_definition_id AND rd.workspace_id = rr.workspace_id
     LEFT JOIN report_artifacts ra ON ra.report_run_id = rr.id AND ra.workspace_id = rr.workspace_id
     WHERE rr.workspace_id = ? AND rd.deleted_at IS NULL ${runClause}
     ORDER BY rr.created_at DESC
     ${reportRunId ? 'LIMIT 1' : 'LIMIT 100'}`,
    params
  );
  if (!rows.length) return [];
  const placeholders = rows.map(() => '?').join(', ');
  const resourceRows = await connection.query(
    `SELECT report_run_id, workspace_provider_connection_id AS connection_id, provider,
            provider_resource_id, resource_name, data_through_at, position
     FROM report_run_resources
     WHERE workspace_id = ? AND report_run_id IN (${placeholders})
     ORDER BY report_run_id, position`,
    [workspaceId, ...rows.map(row => row.id)]
  );
  const byRun = new Map();
  for (const resource of resourceRows) {
    const values = byRun.get(resource.report_run_id) || [];
    values.push({
      connection_id: resource.connection_id,
      provider: resource.provider,
      provider_resource_id: resource.provider_resource_id,
      resource_name: resource.resource_name,
      data_through_at: isoDateTime(resource.data_through_at)
    });
    byRun.set(resource.report_run_id, values);
  }
  return rows.map(row => serializeReport(row, byRun.get(row.id) || []));
}

async function previewReport(userId, workspaceId, body) {
  const configuration = getReadyConfiguration();
  const input = normalizeReportRequest(body, configuration);
  return withConnection(async connection => {
    const membership = await requireReportCapability(connection, workspaceId, userId);
    const resources = await resolveResources(connection, workspaceId, input.resources);
    const built = await buildReportSnapshot({
      userId,
      workspaceId,
      input,
      resources,
      configuration,
      workspaceName: membership.name
    });
    return {
      preview: {
        title: input.title,
        subtitle: input.subtitle,
        timezone: input.timezone,
        range: { from: input.from, to: input.to },
        comparison_enabled: input.comparison_enabled,
        sections: input.sections.map(section => ({
          key: section,
          included: section !== 'cross_platform_summary' || new Set(resources.map(resource => resource.provider)).size > 1
        })),
        resources: built.snapshot.dashboard.sources.map(source => ({
          provider: source.provider,
          provider_name: source.provider_name,
          connection_id: source.resource && source.resource.connection_id,
          resource_name: source.resource && source.resource.display_name,
          status: source.freshness && source.freshness.state,
          data_through_date: source.freshness && source.freshness.data_through_date,
          available_metric_count: (source.metrics || []).filter(metric => metric.available).length
        })),
        estimated_page_count: 3 + resources.length + (new Set(resources.map(resource => resource.provider)).size > 1 ? 1 : 0),
        retention_days: configuration.retentionDays
      }
    };
  });
}

async function enqueueReport(userId, workspaceId, body) {
  const configuration = getReadyConfiguration();
  const input = normalizeReportRequest(body, configuration);
  if (!input.request_id) throw createReportError(400, 'report_request_id_required');
  const idempotencyKey = crypto.createHash('sha256').update(`${workspaceId}\0${userId}\0${input.request_id}`).digest('hex');
  return withConnection(async connection => {
    const membership = await requireReportCapability(connection, workspaceId, userId);
    const existingRows = await connection.query(
      `SELECT id FROM report_runs WHERE workspace_id = ? AND idempotency_key = ? LIMIT 1`,
      [workspaceId, idempotencyKey]
    );
    if (existingRows[0]) {
      const reports = await fetchReportRows(connection, workspaceId, existingRows[0].id);
      return { report: reports[0], idempotent: true };
    }
    const resources = await resolveResources(connection, workspaceId, input.resources);
    const built = await buildReportSnapshot({
      userId,
      workspaceId,
      input,
      resources,
      configuration,
      workspaceName: membership.name
    });
    const definitionId = createId();
    const runId = createId();
    try {
      await connection.beginTransaction();
      await connection.query(
        `INSERT INTO report_definitions
          (id, workspace_id, created_by_user_id, title, subtitle, timezone,
           range_start, range_end, comparison_enabled, configuration)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          definitionId,
          workspaceId,
          userId,
          input.title,
          input.subtitle,
          input.timezone,
          input.from,
          input.to,
          input.comparison_enabled,
          JSON.stringify({ sections: input.sections, range_key: input.range })
        ]
      );
      for (let position = 0; position < resources.length; position += 1) {
        const resource = resources[position];
        await connection.query(
          `INSERT INTO report_definition_resources
            (report_definition_id, workspace_id, workspace_provider_connection_id, provider, position)
           VALUES (?, ?, ?, ?, ?)`,
          [definitionId, workspaceId, resource.connection_id, resource.provider, position]
        );
      }
      await connection.query(
        `INSERT INTO report_runs
          (id, report_definition_id, workspace_id, requested_by_user_id, idempotency_key,
           configuration_snapshot, metric_definitions_snapshot, data_through_at, run_after, max_attempts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), ?)`,
        [
          runId,
          definitionId,
          workspaceId,
          userId,
          idempotencyKey,
          JSON.stringify(built.snapshot),
          JSON.stringify(built.metricDefinitions),
          built.dataThroughAt,
          configuration.maxAttempts
        ]
      );
      for (let position = 0; position < resources.length; position += 1) {
        const resource = resources[position];
        await connection.query(
          `INSERT INTO report_run_resources
            (report_run_id, workspace_id, workspace_provider_connection_id, provider,
             provider_resource_id, resource_name, data_through_at, position)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            runId,
            workspaceId,
            resource.connection_id,
            resource.provider,
            resource.provider_resource_id,
            resource.resource_name,
            resource.data_through_at,
            position
          ]
        );
      }
      await connection.query(
        `INSERT INTO audit_logs (id, workspace_id, actor_user_id, action, target_type, target_id, metadata)
         VALUES (?, ?, ?, 'report.queued', 'report_run', ?, ?)`,
        [createId(), workspaceId, userId, runId, JSON.stringify({ resource_count: resources.length, renderer: configuration.rendererVersion })]
      );
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      if (error && error.code === 'ER_DUP_ENTRY') {
        const duplicateRows = await connection.query(
          `SELECT id FROM report_runs WHERE workspace_id = ? AND idempotency_key = ? LIMIT 1`,
          [workspaceId, idempotencyKey]
        );
        if (duplicateRows[0]) {
          const reports = await fetchReportRows(connection, workspaceId, duplicateRows[0].id);
          return { report: reports[0], idempotent: true };
        }
      }
      throw error;
    }
    const reports = await fetchReportRows(connection, workspaceId, runId);
    return { report: reports[0], idempotent: false };
  });
}

async function listReports(userId, workspaceId) {
  getReadyConfiguration();
  return withConnection(async connection => {
    await requireReportCapability(connection, workspaceId, userId);
    return { reports: await fetchReportRows(connection, workspaceId) };
  });
}

async function getReport(userId, workspaceId, reportRunId) {
  getReadyConfiguration();
  assertUuid(reportRunId, 'invalid_report_id');
  return withConnection(async connection => {
    await requireReportCapability(connection, workspaceId, userId);
    const reports = await fetchReportRows(connection, workspaceId, reportRunId);
    if (!reports[0]) throw createReportError(404, 'report_not_found');
    return { report: reports[0] };
  });
}

async function createDownloadGrant(userId, workspaceId, reportRunId) {
  const configuration = getReadyConfiguration();
  assertUuid(reportRunId, 'invalid_report_id');
  return withConnection(async connection => {
    await requireReportCapability(connection, workspaceId, userId);
    const rows = await connection.query(
      `SELECT ra.id AS artifact_id, ra.download_filename
       FROM report_runs rr
       JOIN report_definitions rd ON rd.id = rr.report_definition_id AND rd.deleted_at IS NULL
       JOIN report_artifacts ra ON ra.report_run_id = rr.id AND ra.workspace_id = rr.workspace_id
       WHERE rr.id = ? AND rr.workspace_id = ? AND rr.status = 'complete'
         AND ra.status = 'active' AND ra.expires_at > UTC_TIMESTAMP(3)
       LIMIT 1`,
      [reportRunId, workspaceId]
    );
    if (!rows[0]) throw createReportError(404, 'report_artifact_not_available');
    const token = randomToken(32);
    await connection.query(
      `INSERT INTO report_download_grants
        (id, report_artifact_id, requested_by_user_id, token_hash, expires_at)
       VALUES (?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
      [createId(), rows[0].artifact_id, userId, hashSecret(token), configuration.grantTtlSeconds]
    );
    return {
      download_url: `/api/report-downloads/${token}`,
      expires_in_seconds: configuration.grantTtlSeconds,
      filename: rows[0].download_filename
    };
  });
}

async function consumeDownloadGrant(userId, token) {
  const configuration = getReadyConfiguration();
  if (!/^[A-Za-z0-9_-]{32,128}$/.test(String(token || ''))) throw createReportError(404, 'download_grant_not_found');
  return withConnection(async connection => {
    try {
      await connection.beginTransaction();
      const rows = await connection.query(
        `SELECT rdg.id AS grant_id, rdg.requested_by_user_id, rdg.expires_at AS grant_expires_at,
                rdg.consumed_at,
                CASE WHEN rdg.expires_at > UTC_TIMESTAMP(3) THEN 1 ELSE 0 END AS grant_unexpired,
                ra.storage_key, ra.download_filename, ra.mime_type,
                ra.byte_size, ra.status AS artifact_status, ra.expires_at AS artifact_expires_at,
                ra.workspace_id, wm.role,
                CASE WHEN ra.expires_at > UTC_TIMESTAMP(3) THEN 1 ELSE 0 END AS artifact_unexpired
         FROM report_download_grants rdg
         JOIN report_artifacts ra ON ra.id = rdg.report_artifact_id
         LEFT JOIN workspace_memberships wm
           ON wm.workspace_id = ra.workspace_id AND wm.user_id = ? AND wm.status = 'active'
         WHERE rdg.token_hash = ?
         LIMIT 1
         FOR UPDATE`,
        [userId, hashSecret(token)]
      );
      const grant = rows[0];
      if (!grant || grant.requested_by_user_id !== userId || !grant.role) {
        throw createReportError(404, 'download_grant_not_found');
      }
      assertCapability(grant.role, 'manageReports');
      if (grant.consumed_at) throw createReportError(410, 'download_grant_consumed');
      if (!grant.grant_unexpired) throw createReportError(410, 'download_grant_expired');
      if (grant.artifact_status !== 'active' || !grant.artifact_unexpired) {
        throw createReportError(410, 'report_artifact_expired');
      }
      if (Number(grant.byte_size) > configuration.maxArtifactBytes) throw createReportError(413, 'report_artifact_too_large');
      const artifactPath = resolveStoragePath(configuration.artifactRoot, grant.storage_key);
      const stat = await fs.stat(artifactPath).catch(() => null);
      if (!stat || !stat.isFile() || stat.size !== Number(grant.byte_size)) {
        throw createReportError(404, 'report_artifact_missing');
      }
      await connection.query(
        `UPDATE report_download_grants SET consumed_at = UTC_TIMESTAMP(3) WHERE id = ? AND consumed_at IS NULL`,
        [grant.grant_id]
      );
      await connection.commit();
      return {
        path: artifactPath,
        filename: grant.download_filename,
        mimeType: grant.mime_type,
        byteSize: Number(grant.byte_size)
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function deleteReport(userId, workspaceId, reportRunId) {
  const configuration = getReadyConfiguration();
  assertUuid(reportRunId, 'invalid_report_id');
  await withConnection(async connection => {
    await requireReportCapability(connection, workspaceId, userId);
    try {
      await connection.beginTransaction();
      const rows = await connection.query(
        `SELECT rr.id, rr.report_definition_id, ra.storage_key
         FROM report_runs rr
         JOIN report_definitions rd ON rd.id = rr.report_definition_id AND rd.workspace_id = rr.workspace_id
         LEFT JOIN report_artifacts ra ON ra.report_run_id = rr.id AND ra.workspace_id = rr.workspace_id
         WHERE rr.id = ? AND rr.workspace_id = ? AND rd.deleted_at IS NULL
         LIMIT 1 FOR UPDATE`,
        [reportRunId, workspaceId]
      );
      if (!rows[0]) throw createReportError(404, 'report_not_found');
      if (rows[0].storage_key) {
        try {
          await removeStoredArtifact(configuration.artifactRoot, rows[0].storage_key);
        } catch {
          throw createReportError(503, 'report_artifact_delete_failed');
        }
      }
      await connection.query(
        `UPDATE report_runs
         SET status = CASE WHEN status IN ('queued', 'running') THEN 'failed' ELSE status END,
             failure_category = CASE WHEN status IN ('queued', 'running') THEN 'cancelled' ELSE failure_category END,
             failure_code = CASE WHEN status IN ('queued', 'running') THEN 'report_deleted' ELSE failure_code END,
             lease_owner = NULL, lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)
         WHERE id = ? AND workspace_id = ?`,
        [reportRunId, workspaceId]
      );
      await connection.query(
        `UPDATE report_definitions SET status = 'deleted', deleted_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3)
         WHERE id = ? AND workspace_id = ?`,
        [rows[0].report_definition_id, workspaceId]
      );
      await connection.query(
        `UPDATE report_artifacts SET status = 'deleted', deleted_at = UTC_TIMESTAMP(3)
         WHERE report_run_id = ? AND workspace_id = ?`,
        [reportRunId, workspaceId]
      );
      await connection.query(
        `DELETE rdg FROM report_download_grants rdg
         JOIN report_artifacts ra ON ra.id = rdg.report_artifact_id
         WHERE ra.report_run_id = ? AND ra.workspace_id = ?`,
        [reportRunId, workspaceId]
      );
      await connection.query(
        `INSERT INTO audit_logs (id, workspace_id, actor_user_id, action, target_type, target_id)
         VALUES (?, ?, ?, 'report.deleted', 'report_run', ?)`,
        [createId(), workspaceId, userId, reportRunId]
      );
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
  return { deleted: true, report_id: reportRunId };
}

module.exports = {
  REPORT_SECTIONS,
  buildReportSnapshot,
  consumeDownloadGrant,
  createDownloadGrant,
  createReportError,
  deleteReport,
  enqueueReport,
  getPublicReportConfiguration,
  getReport,
  listReports,
  normalizeReportRequest,
  previewReport,
  reportStatus,
  resolveResources
};
