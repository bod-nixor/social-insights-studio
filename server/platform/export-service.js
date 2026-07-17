const { getConnection } = require('../database');
const { resolveDateRange } = require('./analytics');
const { queryContentRows, requireWorkspaceCapability } = require('./dashboard-service');
const { createId } = require('./security');

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

function safeCsvCell(value) {
  if (value === null || value === undefined) return '';
  let text = String(value);
  if (/^[=+\-@\t\r]/.test(text)) {
    text = `'${text}`;
  }
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function rowsToCsv(rows) {
  const headers = [
    'published_at',
    'title',
    'views',
    'likes',
    'comments',
    'shares',
    'engagement_rate',
    'observed_at',
    'share_url'
  ];
  const lines = [headers.join(',')];
  for (const row of rows) {
    lines.push([
      row.published_at,
      row.title || row.description || '',
      row.view_count,
      row.like_count,
      row.comment_count,
      row.share_count,
      row.engagement_rate === null ? '' : row.engagement_rate.toFixed(4),
      row.observed_at,
      row.share_url
    ].map(safeCsvCell).join(','));
  }
  return `${lines.join('\n')}\n`;
}

async function createContentCsvExport(userId, workspaceId, query = {}) {
  const range = resolveDateRange(query);
  return withConnection(async connection => {
    await requireWorkspaceCapability(connection, workspaceId, userId, 'exportCsv');
    await connection.beginTransaction();
    try {
      const exportId = createId();
      const runId = createId();
      await connection.query(
        `INSERT INTO exports (id, workspace_id, type, configuration, created_by)
         VALUES (?, ?, 'csv', ?, ?)`,
        [exportId, workspaceId, JSON.stringify({ range, sort: query.sort || 'views' }), userId]
      );
      await connection.query(
        `INSERT INTO export_runs (id, export_id, status)
         VALUES (?, ?, 'running')`,
        [runId, exportId]
      );
      const result = await queryContentRows(connection, workspaceId, {
        from: range.from,
        to: range.to,
        sort: query.sort,
        direction: query.direction,
        limit: Math.min(Number(query.limit || 1000), 1000),
        offset: 0
      });
      const body = rowsToCsv(result.rows);
      await connection.query(
        `UPDATE export_runs
         SET status = 'success', finished_at = UTC_TIMESTAMP(3), row_count = ?
         WHERE id = ?`,
        [result.rows.length, runId]
      );
      await connection.commit();
      return {
        filename: `social-insights-content-${workspaceId}.csv`,
        contentType: 'text/csv; charset=utf-8',
        body,
        row_count: result.rows.length,
        export_id: exportId,
        export_run_id: runId
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

module.exports = {
  createContentCsvExport,
  safeCsvCell
};
