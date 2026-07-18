const os = require('os');
const path = require('path');

const DEFAULT_ARTIFACT_ROOT = path.resolve(__dirname, '..', 'data', 'report-artifacts');
const RETENTION_DAYS = 7;

function flagEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function flagDisabled(value) {
  return ['0', 'false', 'no', 'off'].includes(String(value || '').trim().toLowerCase());
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) return fallback;
  return Math.min(Math.max(parsed, minimum), maximum);
}

function pathIsInside(parent, child) {
  const relative = path.relative(path.resolve(parent), path.resolve(child));
  return !relative || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function getReportConfiguration(env = process.env) {
  const production = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const explicitlyEnabled = flagEnabled(env.FEATURE_PDF_REPORTS);
  const enabled = production ? explicitlyEnabled : !flagDisabled(env.FEATURE_PDF_REPORTS);
  const artifactRoot = path.resolve(env.REPORT_ARTIFACT_ROOT || DEFAULT_ARTIFACT_ROOT);
  const applicationRoot = path.resolve(__dirname, '..', '..');
  const publicRoots = [
    path.resolve(__dirname, '..', 'public'),
    path.resolve(__dirname, '..', '..', 'apps', 'web', 'dist')
  ];
  const errors = [];
  if (enabled && publicRoots.some(publicRoot => (
    pathIsInside(publicRoot, artifactRoot) || pathIsInside(artifactRoot, publicRoot)
  ))) {
    errors.push('REPORT_ARTIFACT_ROOT must be outside every public web root.');
  }
  if (production && enabled && !env.REPORT_ARTIFACT_ROOT) {
    errors.push('REPORT_ARTIFACT_ROOT is required when PDF reports are enabled in production.');
  }
  if (production && enabled && env.REPORT_ARTIFACT_ROOT && !path.isAbsolute(env.REPORT_ARTIFACT_ROOT)) {
    errors.push('REPORT_ARTIFACT_ROOT must be an absolute path in production.');
  }
  if (production && enabled && artifactRoot === path.parse(artifactRoot).root) {
    errors.push('REPORT_ARTIFACT_ROOT must not be a filesystem root.');
  }
  if (production && enabled && pathIsInside(os.tmpdir(), artifactRoot)) {
    errors.push('REPORT_ARTIFACT_ROOT must not use the shared temporary directory in production.');
  }
  if (production && enabled && pathIsInside(applicationRoot, artifactRoot)) {
    errors.push('REPORT_ARTIFACT_ROOT must be outside the deployed application source tree in production.');
  }
  return {
    enabled,
    ready: enabled && errors.length === 0,
    errors,
    artifactRoot,
    retentionDays: RETENTION_DAYS,
    grantTtlSeconds: boundedInteger(env.REPORT_DOWNLOAD_GRANT_TTL_SECONDS, 120, 30, 300),
    leaseSeconds: boundedInteger(env.REPORT_LEASE_SECONDS, 300, 60, 900),
    maxAttempts: boundedInteger(env.REPORT_MAX_ATTEMPTS, 3, 1, 5),
    maxResources: boundedInteger(env.REPORT_MAX_RESOURCES, 20, 1, 20),
    maxRangeDays: boundedInteger(env.REPORT_MAX_RANGE_DAYS, 366, 1, 366),
    maxSnapshotBytes: boundedInteger(env.REPORT_MAX_SNAPSHOT_BYTES, 2 * 1024 * 1024, 64 * 1024, 4 * 1024 * 1024),
    maxArtifactBytes: boundedInteger(env.REPORT_MAX_ARTIFACT_BYTES, 20 * 1024 * 1024, 256 * 1024, 25 * 1024 * 1024),
    maxPages: boundedInteger(env.REPORT_MAX_PAGES, 80, 5, 100),
    maxContentRowsPerResource: boundedInteger(env.REPORT_MAX_CONTENT_ROWS_PER_RESOURCE, 30, 1, 50),
    rendererVersion: 'pdfkit-v1'
  };
}

function getReportProductionErrors(env = process.env) {
  return getReportConfiguration({ ...env, NODE_ENV: 'production' }).errors;
}

module.exports = {
  DEFAULT_ARTIFACT_ROOT,
  RETENTION_DAYS,
  getReportConfiguration,
  getReportProductionErrors,
  pathIsInside
};
