const crypto = require('crypto');
const fs = require('fs/promises');
const os = require('os');
const path = require('path');
const { getConnection } = require('../database');
const { getReportConfiguration } = require('./report-config');
const { renderReportPdf } = require('./report-renderer');
const {
  removeStoredArtifact,
  resolveStoragePath,
  safeDownloadFilename,
  storageKeyForRun
} = require('./report-storage');
const { createId } = require('./security');

function workerError(code) {
  const error = new Error(code);
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

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) throw workerError('database_not_configured');
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
}

function defaultLeaseOwner() {
  return `${os.hostname()}:${process.pid}:${Date.now()}`.slice(0, 128);
}

async function cleanupExpiredReports({ configuration = getReportConfiguration() } = {}) {
  if (!configuration.ready) return { expired: 0, grants_deleted: 0 };
  const artifacts = await withConnection(async connection => {
    try {
      await connection.beginTransaction();
      const rows = await connection.query(
        `SELECT id, report_run_id, workspace_id, storage_key
         FROM report_artifacts
         WHERE status = 'active' AND expires_at <= UTC_TIMESTAMP(3)
         ORDER BY expires_at ASC
         LIMIT 100
         FOR UPDATE`
      );
      if (rows.length) {
        for (const artifact of rows) {
          try {
            await removeStoredArtifact(configuration.artifactRoot, artifact.storage_key);
          } catch {
            throw workerError('report_storage_cleanup_failed');
          }
        }
        const placeholders = rows.map(() => '?').join(', ');
        const artifactIds = rows.map(row => row.id);
        const runIds = rows.map(row => row.report_run_id);
        await connection.query(
          `DELETE FROM report_download_grants WHERE report_artifact_id IN (${placeholders})`,
          artifactIds
        );
        await connection.query(
          `UPDATE report_artifacts
           SET status = 'expired', deleted_at = UTC_TIMESTAMP(3)
           WHERE id IN (${placeholders})`,
          artifactIds
        );
        await connection.query(
          `UPDATE report_runs
           SET status = 'expired', lease_owner = NULL, lease_expires_at = NULL,
               progress_percent = 100, updated_at = UTC_TIMESTAMP(3)
           WHERE id IN (${runIds.map(() => '?').join(', ')}) AND status = 'complete'`,
          runIds
        );
      }
      const grants = await connection.query(
        `DELETE FROM report_download_grants
         WHERE expires_at <= UTC_TIMESTAMP(3) OR consumed_at IS NOT NULL
         LIMIT 1000`
      );
      await connection.commit();
      return { rows, grantsDeleted: Number(grants.affectedRows || 0) };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
  return { expired: artifacts.rows.length, grants_deleted: artifacts.grantsDeleted };
}

async function claimNextReport(owner, configuration) {
  return withConnection(async connection => {
    const candidates = await connection.query(
      `SELECT rr.id
       FROM report_runs rr
       JOIN report_definitions rd
         ON rd.id = rr.report_definition_id AND rd.workspace_id = rr.workspace_id
       WHERE rd.status = 'active' AND rd.deleted_at IS NULL
         AND rr.run_after <= UTC_TIMESTAMP(3)
         AND (
           rr.status = 'queued'
           OR (rr.status = 'running' AND rr.lease_expires_at < UTC_TIMESTAMP(3))
         )
         AND rr.attempts < rr.max_attempts
       ORDER BY rr.run_after ASC, rr.queued_at ASC
       LIMIT 5`
    );
    for (const candidate of candidates) {
      const result = await connection.query(
        `UPDATE report_runs rr
         JOIN report_definitions rd
           ON rd.id = rr.report_definition_id AND rd.workspace_id = rr.workspace_id
         SET rr.status = 'running', rr.started_at = COALESCE(rr.started_at, UTC_TIMESTAMP(3)),
             rr.finished_at = NULL, rr.lease_owner = ?,
             rr.lease_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
             rr.attempts = rr.attempts + 1, rr.progress_percent = 5,
             rr.failure_category = NULL, rr.failure_code = NULL,
             rr.updated_at = UTC_TIMESTAMP(3)
         WHERE rr.id = ? AND rd.status = 'active' AND rd.deleted_at IS NULL
           AND rr.run_after <= UTC_TIMESTAMP(3)
           AND (
             rr.status = 'queued'
             OR (rr.status = 'running' AND rr.lease_expires_at < UTC_TIMESTAMP(3))
           )
           AND rr.attempts < rr.max_attempts`,
        [owner, configuration.leaseSeconds, candidate.id]
      );
      if (Number(result.affectedRows || 0) !== 1) continue;
      const rows = await connection.query(
        `SELECT rr.*, rd.title
         FROM report_runs rr
         JOIN report_definitions rd ON rd.id = rr.report_definition_id
         WHERE rr.id = ? LIMIT 1`,
        [candidate.id]
      );
      if (rows[0]) return rows[0];
    }
    return null;
  });
}

async function updateProgress(runId, owner, progress) {
  await withConnection(connection => connection.query(
    `UPDATE report_runs
     SET progress_percent = ?, lease_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND),
         updated_at = UTC_TIMESTAMP(3)
     WHERE id = ? AND status = 'running' AND lease_owner = ?`,
    [progress.value, progress.leaseSeconds, runId, owner]
  ));
}

function failureDetails(error) {
  const code = String(error && (error.code || error.message) || 'report_render_failed');
  if (code.includes('page_limit') || code.includes('size_limit') || code.includes('snapshot')) {
    return { category: 'limit', code: code.slice(0, 120), retryable: false };
  }
  if (code.includes('storage') || ['EACCES', 'ENOSPC', 'EROFS', 'EMFILE'].includes(code)) {
    return { category: 'storage', code: 'report_storage_unavailable', retryable: true };
  }
  if (code.includes('time_limit')) return { category: 'limit', code: 'report_render_time_limit_exceeded', retryable: false };
  if (code.includes('invalid') || code.includes('no_resources')) {
    return { category: 'rendering', code: code.slice(0, 120), retryable: false };
  }
  return { category: 'internal', code: 'report_render_failed', retryable: true };
}

async function markReportFailure(run, owner, failure) {
  return withConnection(async connection => {
    const retry = failure.retryable && Number(run.attempts) < Number(run.max_attempts);
    const backoffSeconds = Math.min(15 * 60, 30 * (2 ** Math.max(Number(run.attempts) - 1, 0)));
    await connection.query(
      `UPDATE report_runs
       SET status = ?, run_after = CASE WHEN ? THEN DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND) ELSE run_after END,
           finished_at = CASE WHEN ? THEN NULL ELSE UTC_TIMESTAMP(3) END,
           lease_owner = NULL, lease_expires_at = NULL,
           progress_percent = CASE WHEN ? THEN 0 ELSE progress_percent END,
           failure_category = ?, failure_code = ?, updated_at = UTC_TIMESTAMP(3)
       WHERE id = ? AND status = 'running' AND lease_owner = ?`,
      [
        retry ? 'queued' : 'failed',
        retry,
        backoffSeconds,
        retry,
        retry,
        failure.category,
        failure.code,
        run.id,
        owner
      ]
    );
    return retry;
  });
}

async function fileSha256(filePath) {
  const buffer = await fs.readFile(filePath);
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

async function renderClaimedReport(run, owner, configuration, deadlineMs) {
  const snapshot = parseJson(run.configuration_snapshot);
  if (!snapshot) throw workerError('invalid_report_snapshot');
  const storageKey = storageKeyForRun(run.workspace_id, run.id);
  const finalPath = resolveStoragePath(configuration.artifactRoot, storageKey);
  const runDirectory = path.dirname(finalPath);
  await fs.mkdir(runDirectory, { recursive: true, mode: 0o700 });
  await fs.chmod(runDirectory, 0o700).catch(() => {});
  const tempPath = path.join(runDirectory, `report-${crypto.randomBytes(12).toString('hex')}.tmp`);
  await fs.rm(finalPath, { force: true });
  try {
    await updateProgress(run.id, owner, { value: 20, leaseSeconds: configuration.leaseSeconds });
    const rendered = await renderReportPdf({
      snapshot,
      outputPath: tempPath,
      limits: {
        maxPages: configuration.maxPages,
        maxContentRowsPerResource: configuration.maxContentRowsPerResource,
        deadlineMs
      }
    });
    const stat = await fs.stat(tempPath);
    if (!stat.isFile() || stat.size < 100) throw workerError('report_artifact_invalid');
    if (stat.size > configuration.maxArtifactBytes) throw workerError('report_size_limit_exceeded');
    if (rendered.pageCount < 1 || rendered.pageCount > configuration.maxPages) {
      throw workerError('report_page_limit_exceeded');
    }
    const sha256 = await fileSha256(tempPath);
    await fs.rename(tempPath, finalPath);
    await fs.chmod(finalPath, 0o600).catch(() => {});
    await updateProgress(run.id, owner, { value: 90, leaseSeconds: configuration.leaseSeconds });
    await withConnection(async connection => {
      try {
        await connection.beginTransaction();
        const currentRows = await connection.query(
          `SELECT status, lease_owner FROM report_runs WHERE id = ? LIMIT 1 FOR UPDATE`,
          [run.id]
        );
        if (!currentRows[0] || currentRows[0].status !== 'running' || currentRows[0].lease_owner !== owner) {
          throw workerError('report_lease_lost');
        }
        await connection.query(
          `INSERT INTO report_artifacts
            (id, report_run_id, workspace_id, storage_key, download_filename,
             mime_type, byte_size, sha256, page_count, expires_at)
           VALUES (?, ?, ?, ?, ?, 'application/pdf', ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 7 DAY))`,
          [
            createId(),
            run.id,
            run.workspace_id,
            storageKey,
            safeDownloadFilename(run.title),
            stat.size,
            sha256,
            rendered.pageCount
          ]
        );
        await connection.query(
          `UPDATE report_runs
           SET status = 'complete', progress_percent = 100, finished_at = UTC_TIMESTAMP(3),
               lease_owner = NULL, lease_expires_at = NULL, updated_at = UTC_TIMESTAMP(3)
           WHERE id = ? AND status = 'running' AND lease_owner = ?`,
          [run.id, owner]
        );
        await connection.query(
          `INSERT INTO audit_logs (id, workspace_id, actor_user_id, action, target_type, target_id, metadata)
           VALUES (?, ?, ?, 'report.completed', 'report_run', ?, ?)`,
          [
            createId(),
            run.workspace_id,
            run.requested_by_user_id,
            run.id,
            JSON.stringify({ byte_size: stat.size, page_count: rendered.pageCount, sha256 })
          ]
        );
        await connection.commit();
      } catch (error) {
        await connection.rollback();
        throw error;
      }
    });
    return {
      report_run_id: run.id,
      status: 'completed',
      page_count: rendered.pageCount,
      byte_size: stat.size,
      storage_key: storageKey
    };
  } catch (error) {
    await fs.rm(tempPath, { force: true }).catch(() => {});
    const artifactExists = await withConnection(async connection => {
      const rows = await connection.query('SELECT id FROM report_artifacts WHERE report_run_id = ? LIMIT 1', [run.id]);
      return Boolean(rows[0]);
    }).catch(() => false);
    if (!artifactExists) await fs.rm(finalPath, { force: true }).catch(() => {});
    throw error;
  }
}

async function runDueReports({ timeBudgetSeconds = 240, leaseOwner, now = Date.now() } = {}) {
  const configuration = getReportConfiguration();
  if (!configuration.enabled) return { processed: 0, expired: 0, results: [], disabled: true };
  if (!configuration.ready) throw workerError('pdf_reports_not_configured');
  const budget = Math.min(Math.max(Number(timeBudgetSeconds) || 240, 5), 900) * 1000;
  const deadlineMs = Number(now) + budget;
  const owner = String(leaseOwner || defaultLeaseOwner()).slice(0, 128);
  const cleanup = await cleanupExpiredReports({ configuration });
  const results = [];
  while (Date.now() < deadlineMs - 1000) {
    const run = await claimNextReport(owner, configuration);
    if (!run) break;
    try {
      results.push(await renderClaimedReport(run, owner, configuration, deadlineMs));
    } catch (error) {
      const failure = failureDetails(error);
      const retrying = await markReportFailure(run, owner, failure);
      results.push({
        report_run_id: run.id,
        status: retrying ? 'queued' : 'failed',
        failure_category: failure.category,
        failure_code: failure.code
      });
    }
  }
  return {
    processed: results.length,
    expired: cleanup.expired,
    grants_deleted: cleanup.grants_deleted,
    results,
    disabled: false
  };
}

module.exports = {
  claimNextReport,
  cleanupExpiredReports,
  failureDetails,
  renderClaimedReport,
  runDueReports
};
