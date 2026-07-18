const { validateEncryptionConfiguration } = require('./secret-envelope');

const GA4_CALLBACK_PATH = '/api/integrations/google-analytics/callback';

function flagEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) return fallback;
  return parsed;
}

function expectedRedirectUri(env = process.env) {
  const base = String(env.BASE_URL || '').replace(/\/+$/, '');
  return base ? `${base}${GA4_CALLBACK_PATH}` : null;
}

function validateRedirectUri(env = process.env) {
  const raw = String(env.GA4_REDIRECT_URI || '').trim();
  if (!raw) return { ready: false, reason: 'GA4_REDIRECT_URI_missing', value: null };
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return { ready: false, reason: 'GA4_REDIRECT_URI_invalid', value: null };
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== GA4_CALLBACK_PATH) {
    return { ready: false, reason: 'GA4_REDIRECT_URI_invalid', value: null };
  }
  const production = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const localhost = ['localhost', '127.0.0.1', '::1'].includes(parsed.hostname);
  if (parsed.protocol !== 'https:' && (production || !localhost || parsed.protocol !== 'http:')) {
    return { ready: false, reason: 'GA4_REDIRECT_URI_https_required', value: null };
  }
  const expected = expectedRedirectUri(env);
  if (expected && raw !== expected) {
    return { ready: false, reason: 'GA4_REDIRECT_URI_mismatch', value: null };
  }
  return { ready: true, reason: null, value: raw };
}

function getGoogleAnalyticsLimits(env = process.env) {
  return {
    requestTimeoutMs: boundedInteger(env.GA4_REQUEST_TIMEOUT_MS, 10000, 1000, 30000),
    oauthStateTtlSeconds: boundedInteger(env.GA4_OAUTH_STATE_TTL_SECONDS, 600, 120, 900),
    maxRetries: boundedInteger(env.GA4_SYNC_MAX_RETRIES, 2, 0, 5),
    jobTimeBudgetSeconds: boundedInteger(env.GA4_SYNC_TIME_BUDGET_SECONDS, 180, 30, 240),
    analyticsLookbackDays: boundedInteger(env.GA4_ANALYTICS_LOOKBACK_DAYS, 180, 90, 366),
    maxDimensionRows: boundedInteger(env.GA4_SYNC_MAX_DIMENSION_ROWS, 100, 10, 250),
    maxDiscoveryPages: boundedInteger(env.GA4_DISCOVERY_MAX_PAGES, 10, 1, 20),
    maxProperties: boundedInteger(env.GA4_DISCOVERY_MAX_PROPERTIES, 100, 1, 200)
  };
}

function looksLikePlaceholder(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return !normalized || /(^|[_-])(replace|placeholder|example|your)([_-]|$)/.test(normalized);
}

function getGoogleAnalyticsConfiguration(env = process.env, runtime = {}) {
  const enabled = flagEnabled(env.FEATURE_GA4_CONNECTOR);
  const redirect = validateRedirectUri(env);
  const encryption = validateEncryptionConfiguration(env);
  const missing = [];
  const clientId = String(env.GA4_CLIENT_ID || '').trim();
  if (!clientId) missing.push('GA4_CLIENT_ID_missing');
  if (!String(env.GA4_CLIENT_SECRET || '').trim()) missing.push('GA4_CLIENT_SECRET_missing');
  if (!redirect.ready) missing.push(redirect.reason);
  if (!encryption.ready) missing.push('ENCRYPTION_KEY_invalid');
  if (clientId && clientId === String(env.GOOGLE_OIDC_CLIENT_ID || '').trim()) {
    missing.push('GA4_CLIENT_ID_must_differ_from_sign_in');
  }
  if (clientId && clientId === String(env.YOUTUBE_CLIENT_ID || '').trim()) {
    missing.push('GA4_CLIENT_ID_must_differ_from_youtube');
  }
  if (runtime.databaseReady === false) missing.push('database_unavailable');
  if (runtime.foundationReady === false) missing.push('ga4_database_foundation_missing');
  if (runtime.workerReady === false) missing.push('ga4_worker_support_missing');

  const configured = missing.length === 0;
  return {
    enabled,
    configured,
    connectable: enabled && configured,
    status: !enabled ? 'disabled' : configured ? 'available' : 'configuration_required',
    warnings: enabled ? [...new Set(missing)] : [],
    redirectUri: redirect.value,
    limits: getGoogleAnalyticsLimits(env)
  };
}

function getGoogleAnalyticsProductionErrors(env = process.env) {
  const configuration = getGoogleAnalyticsConfiguration(env);
  if (!configuration.enabled) return [];
  const errors = configuration.configured
    ? []
    : configuration.warnings.map(warning => `ga4_configuration:${warning}`);
  if (looksLikePlaceholder(env.GA4_CLIENT_ID)) errors.push('ga4_configuration:GA4_CLIENT_ID_placeholder');
  if (looksLikePlaceholder(env.GA4_CLIENT_SECRET)) errors.push('ga4_configuration:GA4_CLIENT_SECRET_placeholder');
  return [...new Set(errors)];
}

module.exports = {
  GA4_CALLBACK_PATH,
  expectedRedirectUri,
  flagEnabled,
  getGoogleAnalyticsConfiguration,
  getGoogleAnalyticsLimits,
  getGoogleAnalyticsProductionErrors,
  validateRedirectUri
};
