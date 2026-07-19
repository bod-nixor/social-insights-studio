const fs = require('fs/promises');
const path = require('path');

function createReportError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  return error;
}

function assertUuid(value, code = 'invalid_report_identifier') {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || ''))) {
    throw createReportError(400, code);
  }
  return String(value).toLowerCase();
}

function resolveStoragePath(root, storageKey) {
  const normalizedKey = String(storageKey || '').replace(/\\/g, '/');
  if (!normalizedKey || normalizedKey.startsWith('/') || normalizedKey.includes('\0')) {
    throw createReportError(400, 'invalid_report_storage_key');
  }
  const segments = normalizedKey.split('/');
  if (segments.some(segment => !segment || segment === '.' || segment === '..')) {
    throw createReportError(400, 'invalid_report_storage_key');
  }
  const resolvedRoot = path.resolve(root);
  const resolved = path.resolve(resolvedRoot, ...segments);
  const relative = path.relative(resolvedRoot, resolved);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw createReportError(400, 'invalid_report_storage_key');
  }
  return resolved;
}

function storageKeyForRun(workspaceId, reportRunId) {
  return `${assertUuid(workspaceId, 'invalid_workspace_id')}/${assertUuid(reportRunId)}/report.pdf`;
}

function safeDownloadFilename(title) {
  const slug = String(title || 'analytics-report')
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'analytics-report';
  return `${slug}.pdf`;
}

async function removeStoredArtifact(root, storageKey) {
  const artifactPath = resolveStoragePath(root, storageKey);
  await fs.rm(artifactPath, { force: true });
  const runDirectory = path.dirname(artifactPath);
  const workspaceDirectory = path.dirname(runDirectory);
  await fs.rmdir(runDirectory).catch(() => {});
  await fs.rmdir(workspaceDirectory).catch(() => {});
}

module.exports = {
  assertUuid,
  removeStoredArtifact,
  resolveStoragePath,
  safeDownloadFilename,
  storageKeyForRun
};
