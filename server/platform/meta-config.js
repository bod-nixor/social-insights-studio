const { validateEncryptionConfiguration } = require('./secret-envelope');

const META_GRAPH_API_VERSION = 'v25.0';
const META_CALLBACK_PATHS = Object.freeze({
  facebook_pages: '/api/integrations/facebook/callback',
  instagram: '/api/integrations/instagram/callback'
});
const META_FEATURE_FLAGS = Object.freeze({
  facebook_pages: 'FEATURE_FACEBOOK_PAGES_CONNECTOR',
  instagram: 'FEATURE_INSTAGRAM_CONNECTOR'
});
const META_LOGIN_CONFIG_ENV = Object.freeze({
  facebook_pages: 'META_FACEBOOK_LOGIN_CONFIG_ID',
  instagram: 'META_INSTAGRAM_LOGIN_CONFIG_ID'
});
const META_REDIRECT_ENV = Object.freeze({
  facebook_pages: 'FACEBOOK_REDIRECT_URI',
  instagram: 'INSTAGRAM_REDIRECT_URI'
});
const META_SCOPE_ASSERTION_ENV = Object.freeze({
  facebook_pages: 'META_FACEBOOK_APPROVED_SCOPES',
  instagram: 'META_INSTAGRAM_APPROVED_SCOPES'
});
const META_REQUIRED_SCOPES = Object.freeze({
  facebook_pages: Object.freeze(['pages_show_list', 'pages_read_engagement', 'read_insights']),
  instagram: Object.freeze([
    'instagram_basic',
    'instagram_manage_insights',
    'pages_show_list',
    'pages_read_engagement'
  ])
});

function flagEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) return fallback;
  return parsed;
}

function parseScopeAssertion(value) {
  return [...new Set(String(value || '').split(/[\s,]+/).map(scope => scope.trim()).filter(Boolean))];
}

function hasExactScopeAssertion(provider, value) {
  const required = META_REQUIRED_SCOPES[provider] || [];
  const configured = parseScopeAssertion(value);
  return configured.length === required.length && required.every(scope => configured.includes(scope));
}

function expectedRedirectUri(provider, env = process.env) {
  const base = String(env.BASE_URL || '').replace(/\/+$/, '');
  const path = META_CALLBACK_PATHS[provider];
  return base && path ? `${base}${path}` : null;
}

function validateRedirectUri(provider, env = process.env) {
  const envName = META_REDIRECT_ENV[provider];
  const path = META_CALLBACK_PATHS[provider];
  if (!envName || !path) return { ready: false, reason: 'meta_provider_invalid', value: null };
  const raw = String(env[envName] || '').trim();
  if (!raw) return { ready: false, reason: `${envName}_missing`, value: null };
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return { ready: false, reason: `${envName}_invalid`, value: null };
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash || parsed.pathname !== path) {
    return { ready: false, reason: `${envName}_invalid`, value: null };
  }
  const production = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const localhost = ['localhost', '127.0.0.1', '::1'].includes(parsed.hostname);
  if (parsed.protocol !== 'https:' && (production || !localhost || parsed.protocol !== 'http:')) {
    return { ready: false, reason: `${envName}_https_required`, value: null };
  }
  const expected = expectedRedirectUri(provider, env);
  if (expected && raw !== expected) {
    return { ready: false, reason: `${envName}_mismatch`, value: null };
  }
  return { ready: true, reason: null, value: raw };
}

function getMetaLimits(env = process.env) {
  return {
    requestTimeoutMs: boundedInteger(env.META_REQUEST_TIMEOUT_MS, 10000, 1000, 30000),
    oauthStateTtlSeconds: boundedInteger(env.META_OAUTH_STATE_TTL_SECONDS, 600, 120, 900),
    maxRetries: boundedInteger(env.META_SYNC_MAX_RETRIES, 2, 0, 5),
    jobTimeBudgetSeconds: boundedInteger(env.META_SYNC_TIME_BUDGET_SECONDS, 180, 15, 240),
    lookbackDays: boundedInteger(env.META_INSIGHTS_LOOKBACK_DAYS, 90, 7, 93),
    maxContentItems: boundedInteger(env.META_SYNC_MAX_CONTENT_ITEMS, 100, 1, 500),
    maxContentPages: boundedInteger(env.META_SYNC_MAX_CONTENT_PAGES, 5, 1, 20),
    usageDelayThreshold: boundedInteger(env.META_USAGE_DELAY_PERCENT, 80, 50, 99)
  };
}

function getMetaConfiguration(provider, env = process.env, runtime = {}) {
  if (!META_REQUIRED_SCOPES[provider]) {
    return {
      provider,
      enabled: false,
      configured: false,
      connectable: false,
      status: 'configuration_required',
      warnings: ['meta_provider_invalid'],
      redirectUri: null,
      requiredScopes: []
    };
  }
  const enabled = flagEnabled(env[META_FEATURE_FLAGS[provider]]);
  const redirect = validateRedirectUri(provider, env);
  const encryption = validateEncryptionConfiguration(env);
  const loginConfigEnv = META_LOGIN_CONFIG_ENV[provider];
  const scopeEnv = META_SCOPE_ASSERTION_ENV[provider];
  const missing = [];
  if (!String(env.META_APP_ID || '').trim()) missing.push('META_APP_ID_missing');
  if (!String(env.META_APP_SECRET || '').trim()) missing.push('META_APP_SECRET_missing');
  if (!String(env[loginConfigEnv] || '').trim()) missing.push(`${loginConfigEnv}_missing`);
  if (
    Object.values(META_FEATURE_FLAGS).every(flag => flagEnabled(env[flag])) &&
    String(env.META_FACEBOOK_LOGIN_CONFIG_ID || '').trim() ===
      String(env.META_INSTAGRAM_LOGIN_CONFIG_ID || '').trim()
  ) {
    missing.push('META_LOGIN_CONFIG_IDS_must_be_distinct');
  }
  if (!redirect.ready) missing.push(redirect.reason);
  if (!hasExactScopeAssertion(provider, env[scopeEnv])) missing.push(`${scopeEnv}_must_match_exact_read_only_set`);
  if (!encryption.ready) missing.push('ENCRYPTION_KEY_invalid');
  if (env.META_GRAPH_API_VERSION && String(env.META_GRAPH_API_VERSION).trim() !== META_GRAPH_API_VERSION) {
    missing.push('META_GRAPH_API_VERSION_must_be_v25.0');
  }
  if (runtime.databaseReady === false) missing.push('database_unavailable');
  if (runtime.foundationReady === false) missing.push('meta_database_foundation_missing');
  if (runtime.workerReady === false) missing.push('meta_worker_support_missing');

  const configured = missing.length === 0;
  return {
    provider,
    enabled,
    configured,
    connectable: enabled && configured,
    status: !enabled ? 'disabled' : configured ? 'available' : 'configuration_required',
    warnings: enabled ? [...new Set(missing)] : [],
    redirectUri: redirect.value,
    requiredScopes: [...META_REQUIRED_SCOPES[provider]],
    graphApiVersion: META_GRAPH_API_VERSION,
    limits: getMetaLimits(env)
  };
}

function looksLikePlaceholder(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return !normalized || /(^|[_-])(replace|placeholder|example|your)([_-]|$)/.test(normalized);
}

function getMetaProductionErrors(env = process.env) {
  const errors = [];
  for (const provider of Object.keys(META_REQUIRED_SCOPES)) {
    const configuration = getMetaConfiguration(provider, env);
    if (!configuration.enabled) continue;
    for (const warning of configuration.warnings) errors.push(`meta_configuration:${provider}:${warning}`);
    const loginConfigEnv = META_LOGIN_CONFIG_ENV[provider];
    if (looksLikePlaceholder(env[loginConfigEnv])) {
      errors.push(`meta_configuration:${loginConfigEnv}_placeholder`);
    }
  }
  if (Object.keys(META_REQUIRED_SCOPES).some(provider => flagEnabled(env[META_FEATURE_FLAGS[provider]]))) {
    if (looksLikePlaceholder(env.META_APP_ID)) errors.push('meta_configuration:META_APP_ID_placeholder');
    if (looksLikePlaceholder(env.META_APP_SECRET)) errors.push('meta_configuration:META_APP_SECRET_placeholder');
  }
  return [...new Set(errors)];
}

module.exports = {
  META_CALLBACK_PATHS,
  META_FEATURE_FLAGS,
  META_GRAPH_API_VERSION,
  META_LOGIN_CONFIG_ENV,
  META_REDIRECT_ENV,
  META_REQUIRED_SCOPES,
  META_SCOPE_ASSERTION_ENV,
  expectedRedirectUri,
  flagEnabled,
  getMetaConfiguration,
  getMetaLimits,
  getMetaProductionErrors,
  hasExactScopeAssertion,
  parseScopeAssertion,
  validateRedirectUri
};
