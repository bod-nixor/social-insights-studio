const { validateEncryptionConfiguration } = require('./secret-envelope');

const YOUTUBE_CALLBACK_PATH = '/api/integrations/youtube/callback';
const YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS = 30;

function flagEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) return fallback;
  return parsed;
}

function boundedVideoLimit(value, fallback) {
  const parsed = boundedInteger(value, fallback, 50, 1000);
  return Math.floor(parsed / 50) * 50;
}

function expectedRedirectUri(env = process.env) {
  const base = String(env.BASE_URL || '').replace(/\/+$/, '');
  return base ? `${base}${YOUTUBE_CALLBACK_PATH}` : null;
}

function validateRedirectUri(env = process.env) {
  const raw = String(env.YOUTUBE_REDIRECT_URI || '').trim();
  if (!raw) return { ready: false, reason: 'YOUTUBE_REDIRECT_URI_missing', value: null };
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return { ready: false, reason: 'YOUTUBE_REDIRECT_URI_invalid', value: null };
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== YOUTUBE_CALLBACK_PATH) {
    return { ready: false, reason: 'YOUTUBE_REDIRECT_URI_invalid', value: null };
  }
  const production = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const localhost = ['localhost', '127.0.0.1', '::1'].includes(parsed.hostname);
  if (parsed.protocol !== 'https:' && (production || !localhost || parsed.protocol !== 'http:')) {
    return { ready: false, reason: 'YOUTUBE_REDIRECT_URI_https_required', value: null };
  }
  const expected = expectedRedirectUri(env);
  if (expected && raw !== expected) {
    return { ready: false, reason: 'YOUTUBE_REDIRECT_URI_mismatch', value: null };
  }
  return { ready: true, reason: null, value: raw };
}

function getYouTubeLimits(env = process.env) {
  return {
    requestTimeoutMs: boundedInteger(env.YOUTUBE_REQUEST_TIMEOUT_MS, 10000, 1000, 30000),
    oauthStateTtlSeconds: boundedInteger(env.YOUTUBE_OAUTH_STATE_TTL_SECONDS, 600, 120, 900),
    maxPlaylistPages: boundedInteger(env.YOUTUBE_SYNC_MAX_PLAYLIST_PAGES, 5, 1, 20),
    maxVideos: boundedVideoLimit(env.YOUTUBE_SYNC_MAX_VIDEOS, 250),
    maxRetries: boundedInteger(env.YOUTUBE_SYNC_MAX_RETRIES, 2, 0, 5),
    jobTimeBudgetSeconds: boundedInteger(env.YOUTUBE_SYNC_TIME_BUDGET_SECONDS, 180, 15, 240),
    analyticsLookbackDays: boundedInteger(env.YOUTUBE_ANALYTICS_LOOKBACK_DAYS, 180, 90, 366),
    analyticsTopVideos: boundedInteger(env.YOUTUBE_ANALYTICS_TOP_VIDEOS, 200, 1, 200),
    authorizationMaxAgeDays: YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS
  };
}

function looksLikePlaceholder(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return !normalized || /(^|[_-])(replace|placeholder|example|your)([_-]|$)/.test(normalized);
}

function getYouTubeProductionErrors(env = process.env) {
  const configuration = getYouTubeConfiguration(env);
  if (!configuration.enabled) return [];
  const errors = configuration.configured
    ? []
    : configuration.warnings.map(warning => `youtube_configuration:${warning}`);
  if (looksLikePlaceholder(env.YOUTUBE_CLIENT_ID)) errors.push('youtube_configuration:YOUTUBE_CLIENT_ID_placeholder');
  if (looksLikePlaceholder(env.YOUTUBE_CLIENT_SECRET)) errors.push('youtube_configuration:YOUTUBE_CLIENT_SECRET_placeholder');
  return [...new Set(errors)];
}

function getYouTubeConfiguration(env = process.env, runtime = {}) {
  const enabled = flagEnabled(env.YOUTUBE_ENABLED);
  const redirect = validateRedirectUri(env);
  const encryption = validateEncryptionConfiguration(env);
  const missing = [];
  if (!String(env.YOUTUBE_CLIENT_ID || '').trim()) missing.push('YOUTUBE_CLIENT_ID_missing');
  if (!String(env.YOUTUBE_CLIENT_SECRET || '').trim()) missing.push('YOUTUBE_CLIENT_SECRET_missing');
  if (!redirect.ready) missing.push(redirect.reason);
  if (!encryption.ready) missing.push('ENCRYPTION_KEY_invalid');
  if (runtime.databaseReady === false) missing.push('database_unavailable');
  if (runtime.foundationReady === false) missing.push('youtube_database_foundation_missing');
  if (runtime.workerReady === false) missing.push('youtube_worker_support_missing');

  const configured = missing.length === 0;
  return {
    enabled,
    configured,
    connectable: enabled && configured,
    status: !enabled ? 'disabled' : configured ? 'available' : 'configuration_required',
    warnings: enabled ? [...new Set(missing)] : [],
    redirectUri: redirect.value,
    limits: getYouTubeLimits(env)
  };
}

module.exports = {
  YOUTUBE_AUTHORIZATION_MAX_AGE_DAYS,
  YOUTUBE_CALLBACK_PATH,
  expectedRedirectUri,
  flagEnabled,
  getYouTubeConfiguration,
  getYouTubeLimits,
  getYouTubeProductionErrors,
  validateRedirectUri
};
