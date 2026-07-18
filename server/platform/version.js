function cleanValue(value, maxLength = 128) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  if (!normalized || normalized.length > maxLength) return null;
  if (!/^[A-Za-z0-9._:/@+-]+$/.test(normalized)) return null;
  return normalized;
}

function getDeploymentVersion(env = process.env) {
  const commitSha = cleanValue(env.APP_COMMIT_SHA, 64);
  const buildTime = cleanValue(env.APP_BUILD_TIME, 64);
  const release = cleanValue(env.APP_RELEASE || env.APP_RELEASE_ID, 128);
  const production = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const warnings = [];
  if (production && !commitSha) {
    warnings.push('APP_COMMIT_SHA_missing');
  }
  return {
    commit_sha: commitSha,
    build_time: buildTime,
    release,
    metadata_present: Boolean(commitSha || release),
    warnings
  };
}

function getDeploymentReadinessCheck(env = process.env) {
  const version = getDeploymentVersion(env);
  return {
    status: version.metadata_present ? 'configured' : 'missing',
    warnings: version.warnings
  };
}

module.exports = {
  getDeploymentReadinessCheck,
  getDeploymentVersion
};
