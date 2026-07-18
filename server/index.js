require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
let fetchImpl = require('node-fetch');
const jwt = require('jsonwebtoken');
const path = require('path');
const fs = require('fs');
const { FileStateStore, FileTokenStore } = require('./store');
const { getConnection } = require('./database');
const { createPlatformRouter } = require('./platform/routes');
const { validateMailConfiguration } = require('./platform/mail');
const { getDeploymentReadinessCheck, getDeploymentVersion } = require('./platform/version');
const { getYouTubeConfiguration } = require('./platform/youtube-config');
const { getMetaConfiguration } = require('./platform/meta-config');

const BASE_URL = process.env.BASE_URL;
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY;
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET;
const BACKEND_JWT_SECRET = process.env.BACKEND_JWT_SECRET;
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_REVOKE_URL = 'https://open.tiktokapis.com/v2/oauth/revoke/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
const OAUTH_STATE_TTL_MS = Number(process.env.OAUTH_STATE_TTL_MS) || 10 * 60 * 1000;
const AUTH_CODE_TTL_MS = Number(process.env.AUTH_CODE_TTL_MS) || 10 * 60 * 1000;
const BACKEND_TOKEN_TTL_SECONDS = 60 * 60;
const DEFAULT_PROVIDER_HTTP_TIMEOUT_MS = 10 * 1000;
const DEFAULT_BODY_LIMIT = '10kb';
const ALLOWED_USER_FIELDS = [
  'open_id',
  'union_id',
  'username',
  'display_name',
  'bio_description',
  'profile_deep_link',
  'avatar_url',
  'avatar_url_100',
  'avatar_large_url',
  'is_verified',
  'follower_count',
  'following_count',
  'likes_count',
  'video_count'
];
const ALLOWED_VIDEO_FIELDS = [
  'id',
  'create_time',
  'cover_image_url',
  'share_url',
  'video_description',
  'duration',
  'height',
  'width',
  'title',
  'like_count',
  'comment_count',
  'share_count',
  'view_count',
  'embed_html',
  'embed_link'
];

function isPlaceholderValue(value) {
  return /replace_with|your_|placeholder|changeme|unused/i.test(String(value || ''));
}

function normalizeHostname(hostname) {
  return String(hostname || '').replace(/^\[|\]$/g, '').toLowerCase();
}

function isLocalHostname(hostname) {
  const normalized = normalizeHostname(hostname);
  return normalized === 'localhost' || normalized === '127.0.0.1' || normalized === '::1' || normalized.endsWith('.localhost');
}

function validateRequiredEnv(env = process.env) {
  const isProduction = String(env.NODE_ENV || '').toLowerCase() === 'production';
  const baseUrlValue = env.BASE_URL;
  const tiktokClientKey = env.TIKTOK_CLIENT_KEY;
  const tiktokClientSecret = env.TIKTOK_CLIENT_SECRET;
  const backendJwtSecret = env.BACKEND_JWT_SECRET;
  if (!baseUrlValue) {
    throw new Error('Missing BASE_URL environment variable.');
  }
  let parsedBaseUrl;
  try {
    parsedBaseUrl = new URL(baseUrlValue);
  } catch (error) {
    throw new Error('BASE_URL must be a valid absolute URL.');
  }
  if (isProduction) {
    if (parsedBaseUrl.protocol !== 'https:') {
      throw new Error('BASE_URL must be https:// in production.');
    }
    if (isLocalHostname(parsedBaseUrl.hostname)) {
      throw new Error('BASE_URL must not use localhost in production.');
    }
  }
  if (!env.ENCRYPTION_KEY) {
    throw new Error('Missing ENCRYPTION_KEY environment variable.');
  }
  if (!backendJwtSecret) {
    throw new Error('Missing BACKEND_JWT_SECRET environment variable.');
  }
  const weakSecrets = new Set(['changeme', 'change-me', 'secret', 'password', 'default', 'unused']);
  if (
    backendJwtSecret.length < 32 ||
    weakSecrets.has(backendJwtSecret.toLowerCase()) ||
    (isProduction && isPlaceholderValue(backendJwtSecret))
  ) {
    throw new Error(
      'BACKEND_JWT_SECRET must be at least 32 characters and not a common placeholder. ' +
      'Generate a cryptographically random secret (e.g., crypto.randomBytes(32).toString("hex")).'
    );
  }
  if (isProduction) {
    if (!env.DATABASE_URL) {
      throw new Error('DATABASE_URL is required in production.');
    }
    if (isPlaceholderValue(env.DATABASE_URL)) {
      throw new Error('DATABASE_URL must not contain placeholders in production.');
    }
    if (env.AUTH_DEV_MAGIC_LINKS === 'true') {
      throw new Error('AUTH_DEV_MAGIC_LINKS must be disabled in production.');
    }
    validateMailConfiguration(env);
    if (isPlaceholderValue(env.ENCRYPTION_KEY) || new Set(env.ENCRYPTION_KEY).size === 1) {
      throw new Error('ENCRYPTION_KEY must be a real random 32-byte key in production.');
    }
    if (!env.ENCRYPTION_KEY_VERSION || env.ENCRYPTION_KEY_VERSION === 'local-v1' || isPlaceholderValue(env.ENCRYPTION_KEY_VERSION)) {
      throw new Error('ENCRYPTION_KEY_VERSION must be set to a production key version.');
    }
    if ((env.ALLOWED_ORIGINS || '').split(',').map(value => value.trim()).includes('*')) {
      throw new Error('ALLOWED_ORIGINS must not contain wildcard origins in production.');
    }
    const expectedTikTokRedirect = `${parsedBaseUrl.origin}/api/integrations/tiktok/callback`;
    const configuredTikTokRedirect = env.TIKTOK_REDIRECT_URI || expectedTikTokRedirect;
    if (configuredTikTokRedirect !== expectedTikTokRedirect) {
      throw new Error('TIKTOK_REDIRECT_URI must be the exact standalone callback URL for BASE_URL.');
    }
    const trustProxy = parseTrustProxyValue(env.TRUST_PROXY);
    const allowedTrustProxyNames = new Set(['loopback', 'linklocal', 'uniquelocal']);
    if (typeof trustProxy === 'string' && !allowedTrustProxyNames.has(trustProxy)) {
      throw new Error('TRUST_PROXY must be a numeric hop count, false, or a recognized proxy range in production.');
    }
  }
  if (!tiktokClientKey || !tiktokClientSecret) {
    throw new Error('Missing TikTok client credentials. Set TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET.');
  }
  if (isProduction && (isPlaceholderValue(tiktokClientKey) || isPlaceholderValue(tiktokClientSecret))) {
    throw new Error('TikTok client credentials must not contain placeholders in production.');
  }
  if (env.LOOKER_CLIENT_SECRET && weakSecrets.has(env.LOOKER_CLIENT_SECRET.toLowerCase())) {
    throw new Error('LOOKER_CLIENT_SECRET must be omitted for the public legacy connector or set to a real secret.');
  }
  if ((env.LOOKER_REDIRECT_URIS || '').includes('*')) {
    throw new Error('LOOKER_REDIRECT_URIS must contain exact callback URLs, not wildcards.');
  }
}

validateRequiredEnv();

const tokenStorePath = process.env.TOKEN_STORE_PATH || path.join(__dirname, 'data', 'tokens.json');
const tokenLockPath = process.env.TOKEN_LOCK_PATH || path.join(__dirname, 'data', 'tokens.json.lock');
const stateStorePath = process.env.STATE_STORE_PATH || path.join(__dirname, 'data', 'oauth-state.json');
const stateLockPath = process.env.STATE_LOCK_PATH || `${stateStorePath}.lock`;
const tokenStore = new FileTokenStore({
  filePath: tokenStorePath,
  lockPath: tokenLockPath,
  encryptionKey: process.env.ENCRYPTION_KEY,
  pruneAfterDays: process.env.TOKEN_PRUNE_DAYS ? Number(process.env.TOKEN_PRUNE_DAYS) : undefined
});
const stateStore = new FileStateStore({
  filePath: stateStorePath,
  lockPath: stateLockPath,
  ttlMs: OAUTH_STATE_TTL_MS,
  namespace: 'tiktok_oauth_state'
});
const authCodeStore = new FileStateStore({
  filePath: stateStorePath,
  lockPath: stateLockPath,
  ttlMs: AUTH_CODE_TTL_MS,
  namespace: 'backend_authorization_code'
});

const REQUIRED_SCOPES = [
  'user.info.basic',
  'user.info.profile',
  'user.info.stats',
  'video.list'
].join(',');

const PUBLIC_DIR = path.join(__dirname, 'public');
const WEB_DIST_DIR = process.env.WEB_DIST_DIR
  ? path.resolve(process.env.WEB_DIST_DIR)
  : path.join(__dirname, '..', 'apps', 'web', 'dist');
const WEB_INDEX_FILE = path.join(WEB_DIST_DIR, 'index.html');
const WEB_ASSETS_DIR = path.join(WEB_DIST_DIR, 'assets');
const PUBLIC_PAGE_FILES = new Map([
  ['privacy', 'privacy.html'],
  ['terms', 'terms.html'],
  ['support', 'support.html'],
  ['data-deletion', 'data-deletion.html'],
  ['status', 'status.html']
]);
const CLIENT_ROUTE_PATTERN = /^\/workspaces\/[0-9a-f-]{36}\/content\/[0-9a-f-]{36}\/?$/i;

function ensureStoreOutsidePublic(storePath, envName) {
  const resolvedStore = path.resolve(storePath);
  const resolvedPublic = path.resolve(PUBLIC_DIR);
  const relative = path.relative(resolvedPublic, resolvedStore);
  if (!relative || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    throw new Error(`${envName} must be outside the public web root.`);
  }
}

ensureStoreOutsidePublic(tokenStorePath, 'TOKEN_STORE_PATH');
ensureStoreOutsidePublic(stateStorePath, 'STATE_STORE_PATH');

const app = express();
const trustProxySetting = getTrustProxySetting();
if (trustProxySetting !== false) {
  app.set('trust proxy', trustProxySetting);
}

app.use((req, res, next) => {
  const incoming = req.get('x-correlation-id');
  req.correlationId = /^[A-Za-z0-9._-]{8,128}$/.test(incoming || '')
    ? incoming
    : crypto.randomUUID();
  res.set('x-correlation-id', req.correlationId);
  next();
});

app.use((req, res, next) => {
  // Normalize repeated slashes (but keep query string)
  if (req.url.startsWith('//')) {
    const normalized = req.url.replace(/^\/+/, '/');
    return res.redirect(308, normalized);
  }
  next();
});

app.disable('x-powered-by');
app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        imgSrc: ["'self'", 'https:', 'data:'],
        styleSrc: ["'self'", "'unsafe-inline'"],
        scriptSrc: ["'self'"],
        connectSrc: ["'self'"],
        objectSrc: ["'none'"],
        baseUri: ["'self'"],
        frameAncestors: ["'none'"],
        upgradeInsecureRequests: []
      }
    },
    referrerPolicy: { policy: 'no-referrer' }
  })
);

const allowedOrigins = new Set(
  (process.env.ALLOWED_ORIGINS || '')
    .split(',')
    .map(origin => origin.trim())
    .filter(Boolean)
);
if (BASE_URL) {
  allowedOrigins.add(new URL(BASE_URL).origin);
}
const corsMiddleware = cors({
  origin(origin, callback) {
    if (!origin) {
      return callback(null, true);
    }
    if (allowedOrigins.has(origin)) {
      return callback(null, true);
    }
    return callback(new Error('CORS origin not allowed'));
  },
  methods: ['GET', 'POST'],
  allowedHeaders: ['Authorization', 'Content-Type'],
  maxAge: 600
});

const authLimiter = rateLimit({
  windowMs: (Number(process.env.RATE_LIMIT_WINDOW_MINUTES) || 15) * 60 * 1000,
  max: Number(process.env.RATE_LIMIT_MAX) || 60,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: 'rate_limited' })
});
const apiLimiter = rateLimit({
  windowMs: (Number(process.env.API_RATE_LIMIT_WINDOW_MINUTES) || 5) * 60 * 1000,
  max: Number(process.env.API_RATE_LIMIT_MAX) || 120,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req, res) => res.status(429).json({ error: 'rate_limited' })
});

app.use(express.urlencoded({ extended: false, limit: DEFAULT_BODY_LIMIT }));
app.use(express.json({ limit: DEFAULT_BODY_LIMIT }));

app.use('/oauth', corsMiddleware, authLimiter);
app.use('/auth', authLimiter);
app.use('/api', corsMiddleware, apiLimiter);

app.get('/health/live', (req, res) => {
  res.json({ status: 'live' });
});

app.get('/health/version', (req, res) => {
  res.json(getDeploymentVersion());
});

app.get('/health/ready', async (req, res) => {
  let database = 'not_configured';
  let youtubeFoundationReady = false;
  let metaFoundationReady = false;
  if (process.env.DATABASE_URL) {
    let connection;
    try {
      connection = await getConnection();
      await connection.query('SELECT 1');
      database = 'ready';
      const foundationRows = await connection.query(
        `SELECT COUNT(*) AS count FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = DATABASE()
           AND TABLE_NAME IN (
             'provider_authorizations',
             'provider_authorization_credentials',
             'provider_resources',
             'workspace_provider_connections',
             'youtube_channel_snapshots',
             'youtube_analytics_daily_snapshots',
             'youtube_video_analytics_snapshots',
             'provider_request_events'
           )`
      );
      youtubeFoundationReady = Number(foundationRows[0] && foundationRows[0].count) === 8;
      const metaFoundationRows = await connection.query(
        `SELECT COUNT(*) AS count FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = DATABASE()
           AND TABLE_NAME IN (
             'provider_authorizations',
             'provider_authorization_credentials',
             'provider_resources',
             'provider_resource_credentials',
             'workspace_provider_connections',
             'meta_account_insight_snapshots',
             'meta_callback_events',
             'provider_request_events'
           )`
      );
      if (Number(metaFoundationRows[0] && metaFoundationRows[0].count) === 8) {
        const metaFoundationColumnRows = await connection.query(
          `SELECT COUNT(*) AS count FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE()
             AND (
               (TABLE_NAME = 'meta_account_insight_snapshots'
                AND COLUMN_NAME IN ('range_days', 'range_start_date', 'range_end_date'))
               OR (TABLE_NAME = 'oauth_transactions' AND COLUMN_NAME = 'provider_config_id')
             )`
        );
        metaFoundationReady = Number(metaFoundationColumnRows[0] && metaFoundationColumnRows[0].count) === 4;
      }
    } catch (error) {
      database = 'unavailable';
    } finally {
      if (connection) await connection.release();
    }
  }
  const deployment = getDeploymentReadinessCheck();
  const youtube = getYouTubeConfiguration(process.env, {
    databaseReady: database === 'ready',
    foundationReady: youtubeFoundationReady,
    workerReady: true
  });
  const facebookPages = getMetaConfiguration('facebook_pages', process.env, {
    databaseReady: database === 'ready',
    foundationReady: metaFoundationReady,
    workerReady: true
  });
  const instagram = getMetaConfiguration('instagram', process.env, {
    databaseReady: database === 'ready',
    foundationReady: metaFoundationReady,
    workerReady: true
  });
  const body = {
    status: database === 'unavailable' ? 'not_ready' : 'ready',
    checks: {
      database,
      legacy_file_store: 'configured',
      deployment_metadata: deployment.status,
      youtube: youtube.status,
      facebook_pages: facebookPages.status,
      instagram: instagram.status
    }
  };
  const warnings = [
    ...deployment.warnings,
    ...youtube.warnings,
    ...facebookPages.warnings,
    ...instagram.warnings
  ];
  if (warnings.length > 0) {
    body.warnings = [...new Set(warnings)];
  }
  res.status(database === 'unavailable' ? 503 : 200).json(body);
});

app.use('/api', createPlatformRouter());

const LOOKER_CLIENT_ID = process.env.LOOKER_CLIENT_ID || 'looker-studio-connector';
const LOOKER_CLIENT_SECRET = process.env.LOOKER_CLIENT_SECRET || null;

class ProviderRequestError extends Error {
  constructor(category, message, retryable = false) {
    super(message);
    this.name = 'ProviderRequestError';
    this.category = category;
    this.retryable = retryable;
  }
}

function requireEnv() {
  if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET) {
    throw new Error('Missing TikTok client credentials. Set TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET.');
  }
}

function requireBackendJwt() {
  if (!BACKEND_JWT_SECRET) {
    throw new Error('Missing BACKEND_JWT_SECRET for backend OAuth JWTs.');
  }
}

function generateRandomToken(size = 32) {
  return crypto.randomBytes(size).toString('base64url');
}

function isPassengerRuntime() {
  return Boolean(
    process.env.PASSENGER_APP_ENV
    || process.env.PASSENGER_APP_ROOT
    || process.env.PASSENGER_BASE_URI
    || process.env.PASSENGER_SPAWN_METHOD
  );
}

function parseTrustProxyValue(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['0', 'false', 'off', 'no'].includes(normalized)) {
    return false;
  }
  if (['1', 'true', 'on', 'yes'].includes(normalized)) {
    return 1;
  }
  const numeric = Number(value);
  if (Number.isInteger(numeric) && numeric > 0) {
    return numeric;
  }
  return value;
}

function getTrustProxySetting() {
  const configured = parseTrustProxyValue(process.env.TRUST_PROXY);
  if (configured !== null) {
    return configured;
  }
  return isPassengerRuntime() ? 1 : false;
}

function getStateFingerprint(value) {
  if (!value) {
    return null;
  }
  return crypto
    .createHash('sha256')
    .update(String(value))
    .digest('hex')
    .slice(0, 12);
}

function logEvent(level, event, details = {}) {
  const payload = {
    event,
    ...details
  };
  const method = level === 'error' ? 'error' : level === 'warn' ? 'warn' : 'info';
  console[method](JSON.stringify(payload));
}

function logOAuthState(event, state, details = {}) {
  logEvent('info', 'oauth_state', {
    event,
    state_fingerprint: getStateFingerprint(state),
    ...details
  });
}

function getRedirectUri() {
  return `${BASE_URL}/auth/tiktok/callback`;
}

function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function readJsonResponse(response) {
  const text = await response.text();
  if (!text) {
    return { ok: true, data: {} };
  }
  try {
    return { ok: true, data: JSON.parse(text) };
  } catch (error) {
    return { ok: false, data: { error: 'invalid_json_response' } };
  }
}

function getProviderHttpTimeoutMs() {
  const configured = Number(process.env.PROVIDER_HTTP_TIMEOUT_MS);
  if (Number.isFinite(configured) && configured > 0) {
    return configured;
  }
  return DEFAULT_PROVIDER_HTTP_TIMEOUT_MS;
}

async function fetchWithProviderTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), getProviderHttpTimeoutMs());
  try {
    return await fetchImpl(url, {
      ...options,
      signal: controller.signal
    });
  } catch (error) {
    if (error && error.name === 'AbortError') {
      throw new ProviderRequestError('timeout', 'Provider request timed out.', true);
    }
    throw new ProviderRequestError('network', 'Provider request failed.', true);
  } finally {
    clearTimeout(timeout);
  }
}

function categorizeProviderStatus(status) {
  if (status === 401) {
    return { category: 'authentication', retryable: false };
  }
  if (status === 403) {
    return { category: 'scope', retryable: false };
  }
  if (status === 429) {
    return { category: 'rate_limit', retryable: true };
  }
  if (status >= 500) {
    return { category: 'provider', retryable: true };
  }
  return { category: 'provider', retryable: false };
}

function getProviderFailure(error) {
  if (error instanceof ProviderRequestError) {
    return {
      status: error.category === 'timeout' ? 504 : 502,
      category: error.category,
      retryable: error.retryable
    };
  }
  return { status: 502, category: 'network', retryable: true };
}

function getTokenPayload(tokenResponseBody) {
  if (!tokenResponseBody || typeof tokenResponseBody !== 'object') {
    return {};
  }
  return tokenResponseBody.data || tokenResponseBody;
}

function hasUsableTokenPayload(tokenPayload, fallback = {}) {
  return Boolean(
    tokenPayload
    && tokenPayload.access_token
    && (tokenPayload.refresh_token || fallback.refreshToken)
  );
}

function buildAuthUrl(state) {
  const params = new URLSearchParams({
    client_key: TIKTOK_CLIENT_KEY,
    response_type: 'code',
    scope: REQUIRED_SCOPES,
    redirect_uri: getRedirectUri(),
    state
  });

  return `${TIKTOK_AUTH_URL}?${params.toString()}`;
}

async function exchangeCodeForToken(code) {
  const payload = new URLSearchParams({
    client_key: TIKTOK_CLIENT_KEY,
    client_secret: TIKTOK_CLIENT_SECRET,
    grant_type: 'authorization_code',
    code,
    redirect_uri: getRedirectUri()
  });

  let response;
  try {
    response = await fetchWithProviderTimeout(TIKTOK_TOKEN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: payload.toString()
    });
  } catch (error) {
    const failure = getProviderFailure(error);
    return { ok: false, data: { error: 'provider_request_failed' }, ...failure };
  }

  const parsed = await readJsonResponse(response);
  if (!parsed.ok) {
    return { ok: false, data: parsed.data, category: 'malformed_response', retryable: false };
  }
  const statusCategory = categorizeProviderStatus(response.status);
  return {
    ok: response.ok,
    data: parsed.data,
    status: response.status,
    category: response.ok ? null : statusCategory.category,
    retryable: response.ok ? false : statusCategory.retryable
  };
}

async function refreshAccessToken(refreshToken) {
  const payload = new URLSearchParams({
    client_key: TIKTOK_CLIENT_KEY,
    client_secret: TIKTOK_CLIENT_SECRET,
    grant_type: 'refresh_token',
    refresh_token: refreshToken
  });

  let response;
  try {
    response = await fetchWithProviderTimeout(TIKTOK_TOKEN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: payload.toString()
    });
  } catch (error) {
    const failure = getProviderFailure(error);
    return { ok: false, data: { error: 'provider_request_failed' }, ...failure };
  }

  const parsed = await readJsonResponse(response);
  if (!parsed.ok) {
    return { ok: false, data: parsed.data, category: 'malformed_response', retryable: false };
  }
  const statusCategory = categorizeProviderStatus(response.status);
  return {
    ok: response.ok,
    data: parsed.data,
    status: response.status,
    category: response.ok ? null : statusCategory.category,
    retryable: response.ok ? false : statusCategory.retryable
  };
}

function buildTokenRecord(tokenResponse, fallback = {}) {
  const now = Date.now();
  const expiresIn = Number(tokenResponse.expires_in || 0);
  const refreshExpiresIn = Number(tokenResponse.refresh_expires_in || 0);
  return {
    accessToken: tokenResponse.access_token,
    refreshToken: tokenResponse.refresh_token || fallback.refreshToken,
    expiresAt: expiresIn > 0 ? now + expiresIn * 1000 : fallback.expiresAt || null,
    refreshExpiresAt: refreshExpiresIn > 0 ? now + refreshExpiresIn * 1000 : fallback.refreshExpiresAt || null,
    openId: tokenResponse.open_id || fallback.openId || null,
    scopes: tokenResponse.scope || fallback.scopes || null,
    tokenType: tokenResponse.token_type || 'Bearer'
  };
}

async function getAccessTokenForSubject(subject) {
  const tokenData = await tokenStore.getConnectorToken(subject);
  if (!tokenData) {
    return { error: 'invalid_subject', status: 401 };
  }

  if (tokenStore.isAccessTokenValid(tokenData)) {
    return { accessToken: tokenData.accessToken };
  }

  if (!tokenStore.isRefreshTokenValid(tokenData)) {
    return { error: 'refresh_token_expired', status: 401 };
  }

  const refreshResult = await refreshAccessToken(tokenData.refreshToken);
  const refreshedPayload = getTokenPayload(refreshResult.data);
  if (!refreshResult.ok || refreshedPayload.error || !hasUsableTokenPayload(refreshedPayload, tokenData)) {
    return {
      error: 'token_refresh_failed',
      status: 401,
      category: refreshResult.category || 'authentication',
      retryable: refreshResult.retryable === true
    };
  }

  const updated = buildTokenRecord(refreshedPayload, tokenData);
  await tokenStore.saveConnectorToken(subject, updated);

  return { accessToken: updated.accessToken };
}

function getBearerTokenFromRequest(req) {
  const authHeader = req.get('authorization') || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

function buildJsonError(res, status, error, message, extras = {}) {
  const payload = { error, ...extras };
  if (message) {
    payload.message = message;
  }
  return res.status(status).json(payload);
}

function parseFieldsParam(fieldParam, allowedFields) {
  if (!fieldParam) {
    return allowedFields;
  }
  const requested = fieldParam
    .split(',')
    .map(value => value.trim())
    .filter(Boolean);
  if (requested.length === 0) {
    return null;
  }
  const invalid = requested.filter(field => !allowedFields.includes(field));
  if (invalid.length > 0) {
    return null;
  }
  return requested;
}

async function buildAuthorizationCode(subject, scopes, clientId, redirectUri) {
  const code = generateRandomToken(24);
  await authCodeStore.save(code, { subject, scopes, clientId, redirectUri, createdAt: Date.now() });
  return code;
}

async function consumeAuthorizationCode(code) {
  return authCodeStore.consume(code);
}

function issueBackendTokens(subject, scopes, refreshTokenOverride) {
  requireBackendJwt();
  const accessToken = jwt.sign(
    { sub: subject, scopes: scopes || null, typ: 'access' },
    BACKEND_JWT_SECRET,
    { expiresIn: BACKEND_TOKEN_TTL_SECONDS }
  );
  const refreshToken = refreshTokenOverride || jwt.sign(
    { sub: subject, scopes: scopes || null, typ: 'refresh' },
    BACKEND_JWT_SECRET,
    { expiresIn: '30d' }
  );

  return {
    access_token: accessToken,
    refresh_token: refreshToken,
    token_type: 'Bearer',
    expires_in: BACKEND_TOKEN_TTL_SECONDS,
    scope: scopes || ''
  };
}

function verifyBackendJwt(token) {
  requireBackendJwt();
  const payload = jwt.verify(token, BACKEND_JWT_SECRET);
  if (!payload || payload.typ !== 'access') {
    throw new Error('invalid_token_type');
  }
  return payload;
}

function normalizeLegacyRedirectUri(redirectUri) {
  try {
    const raw = String(redirectUri);
    const parsed = new URL(redirectUri);
    if (raw !== parsed.toString()) {
      return null;
    }
    if (
      parsed.protocol !== 'https:'
      || parsed.hostname !== 'script.google.com'
      || parsed.username
      || parsed.password
      || parsed.port
      || parsed.search
      || parsed.hash
    ) {
      return null;
    }
    const isOAuth2LibraryCallback = /^\/macros\/d\/[^/]+\/usercallback$/.test(parsed.pathname);
    const isWebAppDeployment = /^\/macros\/s\/[^/]+\/(exec|dev)$/.test(parsed.pathname);
    if (!isOAuth2LibraryCallback && !isWebAppDeployment) {
      return null;
    }
    return parsed.toString();
  } catch (error) {
    return null;
  }
}

function getAllowedLegacyRedirects() {
  const raw = process.env.LOOKER_REDIRECT_URIS || process.env.LEGACY_LOOKER_REDIRECT_URIS || '';
  return new Set(
    raw
      .split(/[\n,]/)
      .map(value => value.trim())
      .filter(Boolean)
      .map(normalizeLegacyRedirectUri)
      .filter(Boolean)
  );
}

function isAllowedRedirect(redirectUri, clientId = LOOKER_CLIENT_ID) {
  if (clientId !== LOOKER_CLIENT_ID) {
    return false;
  }
  const normalized = normalizeLegacyRedirectUri(redirectUri);
  if (!normalized) {
    return false;
  }
  return getAllowedLegacyRedirects().has(normalized);
}

function validateConnectorClient(clientId, clientSecret) {
  if (!clientId || clientId !== LOOKER_CLIENT_ID) {
    return false;
  }
  if (LOOKER_CLIENT_SECRET) {
    return clientSecret === LOOKER_CLIENT_SECRET;
  }
  return !clientSecret;
}

async function revokeProviderAccess(accessToken) {
  const payload = new URLSearchParams({
    client_key: TIKTOK_CLIENT_KEY,
    client_secret: TIKTOK_CLIENT_SECRET,
    token: accessToken
  });

  let response;
  try {
    response = await fetchWithProviderTimeout(TIKTOK_REVOKE_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: payload.toString()
    });
  } catch (error) {
    const failure = getProviderFailure(error);
    return { attempted: true, success: false, ...failure };
  }

  const parsed = await readJsonResponse(response);
  if (!parsed.ok) {
    return {
      attempted: true,
      success: false,
      status: response.status,
      category: 'malformed_response',
      retryable: false
    };
  }

  const statusCategory = categorizeProviderStatus(response.status);
  return {
    attempted: true,
    success: response.ok,
    status: response.status,
    category: response.ok ? null : statusCategory.category,
    retryable: response.ok ? false : statusCategory.retryable
  };
}

app.get('/auth/tiktok/start', async (req, res) => {
  try {
    requireEnv();
    const state = generateRandomToken(16);
    await stateStore.save(state, { flow: 'direct', createdAt: Date.now() });
    logOAuthState('create', state, { flow: 'direct', outcome: 'saved' });
    res.redirect(buildAuthUrl(state));
  } catch (error) {
    logEvent('error', 'oauth_authorize_configuration_error', {
      correlation_id: req.correlationId,
      message: error && error.message,
      nodeEnv: process.env.NODE_ENV,
      trustProxy: app.get('trust proxy')
    });

    res.status(500).send('<h1>Configuration error</h1><p>Backend configuration error.</p>');
  }
});

app.get('/auth/tiktok/callback', async (req, res) => {
  try {
    requireEnv();
    const { code, state, error, error_description: errorDescription } = req.query;

    if (error) {
      return res.status(400).send(
        `<h1>Authorization Error</h1><p>${escapeHtml(error)}: ${escapeHtml(errorDescription || 'No description provided.')}</p>`
      );
    }

    if (!code || !state) {
      return res.status(400).send('<h1>Missing code or state</h1><p>Please restart authentication.</p>');
    }

    const stateResult = await stateStore.consumeWithResult(state);
    logOAuthState('consume', state, {
      outcome: stateResult.status,
      flow: stateResult.entry ? stateResult.entry.flow : stateResult.metadata && stateResult.metadata.flow
    });
    if (stateResult.status !== 'consumed') {
      return res.status(400).send('<h1>Invalid state</h1><p>Please restart authentication.</p>');
    }
    const stateEntry = stateResult.entry;
    if (!stateEntry || !['direct', 'oauth'].includes(stateEntry.flow)) {
      logOAuthState('consume', state, { outcome: 'mismatched', flow: stateEntry && stateEntry.flow });
      return res.status(400).send('<h1>Invalid state</h1><p>Please restart authentication.</p>');
    }

    const tokenResult = await exchangeCodeForToken(code);
    const tokenPayload = getTokenPayload(tokenResult.data);

    if (!tokenResult.ok || tokenPayload.error || !hasUsableTokenPayload(tokenPayload)) {
      logEvent('warn', 'tiktok_token_exchange_failed', {
        correlation_id: req.correlationId,
        category: tokenResult.category || 'provider',
        retryable: tokenResult.retryable === true
      });
      return res.status(400).send(
        '<h1>Token Exchange Failed</h1><p>Please retry authentication.</p>'
      );
    }

    const record = buildTokenRecord(tokenPayload);
    if (!record.openId) {
      return res.status(400).send('<h1>Missing open_id</h1><p>Unable to determine TikTok account.</p>');
    }
    await tokenStore.saveConnectorToken(record.openId, record);

    if (stateEntry.flow === 'oauth') {
      const authCode = await buildAuthorizationCode(
        record.openId,
        record.scopes,
        stateEntry.clientId,
        stateEntry.redirectUri
      );
      const redirect = new URL(stateEntry.redirectUri);
      redirect.searchParams.set('code', authCode);
      redirect.searchParams.set('state', stateEntry.lookerState);
      return res.redirect(302, redirect.toString());
    }

    return res.send(`
      <!doctype html>
      <html lang="en">
        <head>
          <meta charset="UTF-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>TikTok Connected</title>
          <style>
            body { font-family: Arial, sans-serif; margin: 2rem; }
          </style>
        </head>
        <body>
          <h1>Success!</h1>
          <p>Your TikTok account is connected. You can close this window.</p>
        </body>
      </html>
    `);
  } catch (error) {
    res.status(500).send('<h1>Unexpected Error</h1><p>An unexpected error occurred.</p>');
  }
});

app.get('/oauth/authorize', async (req, res) => {
  try {
    requireEnv();
    const { state, redirect_uri: redirectUri, response_type: responseType, client_id: clientId } = req.query;
    if (!state || !redirectUri || !responseType || !clientId) {
      return res.status(400).send('<h1>Missing OAuth parameters</h1><p>Required parameters are missing.</p>');
    }
    if (responseType !== 'code') {
      return res.status(400).send('<h1>Invalid response type</h1><p>Response type must be "code".</p>');
    }
    if (clientId !== LOOKER_CLIENT_ID) {
      return res.status(400).send('<h1>Invalid client</h1><p>Client ID not allowed.</p>');
    }
    if (!isAllowedRedirect(redirectUri, clientId)) {
      return res.status(400).send('<h1>Invalid redirect URI</h1><p>Redirect URI not allowed.</p>');
    }
    const tiktokState = generateRandomToken(16);
    await stateStore.save(tiktokState, {
      flow: 'oauth',
      clientId,
      redirectUri,
      lookerState: state
    });
    logOAuthState('create', tiktokState, { flow: 'oauth', outcome: 'saved' });
    res.redirect(buildAuthUrl(tiktokState));
  } catch (error) {
    logEvent('error', 'oauth_authorize_configuration_error', {
      correlation_id: req.correlationId,
      message: error && error.message,
      nodeEnv: process.env.NODE_ENV,
      trustProxy: app.get('trust proxy')
    });

    res.status(500).send('<h1>Configuration error</h1><p>Backend configuration error.</p>');
  }
});

app.post('/oauth/token', async (req, res) => {
  try {
    requireBackendJwt();
    const clientId = req.body.client_id;
    const clientSecret = req.body.client_secret;
    if (!validateConnectorClient(clientId, clientSecret)) {
      return res.status(400).json({ error: 'invalid_client' });
    }
    const grantType = req.body.grant_type;
    if (grantType === 'authorization_code') {
      const code = req.body.code;
      if (!code) {
        return res.status(400).json({ error: 'invalid_request', error_description: 'Missing code.' });
      }
      const redirectUri = req.body.redirect_uri;
      if (!redirectUri) {
        return res.status(400).json({ error: 'invalid_request', error_description: 'Missing redirect_uri.' });
      }
      const entry = await consumeAuthorizationCode(code);
      if (!entry) {
        return res.status(400).json({ error: 'invalid_grant', error_description: 'Code expired or invalid.' });
      }
      if (entry.clientId !== clientId || entry.redirectUri !== redirectUri) {
        return res.status(400).json({ error: 'invalid_grant', error_description: 'Code client or redirect mismatch.' });
      }
      return res.json(issueBackendTokens(entry.subject, entry.scopes));
    }

    if (grantType === 'refresh_token') {
      const refreshToken = req.body.refresh_token;
      if (!refreshToken) {
        return res.status(400).json({ error: 'invalid_request', error_description: 'Missing refresh_token.' });
      }
      let payload;
      try {
        payload = jwt.verify(refreshToken, BACKEND_JWT_SECRET);
      } catch (error) {
        return res.status(400).json({ error: 'invalid_grant', error_description: 'Invalid refresh token.' });
      }
      if (!payload || payload.typ !== 'refresh') {
        return res.status(400).json({ error: 'invalid_grant', error_description: 'Invalid refresh token type.' });
      }
      return res.json(issueBackendTokens(payload.sub, payload.scopes, refreshToken));
    }

    return res.status(400).json({ error: 'unsupported_grant_type' });
  } catch (error) {
    res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/tiktok/user', async (req, res) => {
  try {
    requireEnv();
    const bearerToken = getBearerTokenFromRequest(req);
    if (!bearerToken) {
      return res.status(401).json({ error: 'missing_access_token' });
    }

    let payload;
    try {
      payload = verifyBackendJwt(bearerToken);
    } catch (error) {
      return res.status(401).json({ error: 'invalid_access_token' });
    }

    const tokenResult = await getAccessTokenForSubject(payload.sub);
    if (tokenResult.error) {
      return res.status(tokenResult.status || 401).json({
        error: tokenResult.error,
        category: tokenResult.category,
        retryable: tokenResult.retryable
      });
    }

    const fieldList = parseFieldsParam(req.query.fields, ALLOWED_USER_FIELDS);
    if (!fieldList) {
      return buildJsonError(res, 400, 'invalid_fields');
    }

    const url = `${TIKTOK_API_BASE_URL}user/info/?fields=${encodeURIComponent(fieldList.join(','))}`;
    let response;
    try {
      response = await fetchWithProviderTimeout(url, {
        headers: {
          Authorization: `Bearer ${tokenResult.accessToken}`
        }
      });
    } catch (error) {
      const failure = getProviderFailure(error);
      return buildJsonError(res, failure.status, 'tiktok_request_failed', null, {
        category: failure.category,
        retryable: failure.retryable
      });
    }

    const parsed = await readJsonResponse(response);
    if (!parsed.ok) {
      return buildJsonError(res, 502, 'invalid_tiktok_response', null, {
        category: 'malformed_response',
        retryable: false
      });
    }
    res.status(response.status).json(parsed.data);
  } catch (error) {
    res.status(500).json({ error: 'server_error' });
  }
});

app.get('/api/tiktok/videos', async (req, res) => {
  try {
    requireEnv();
    const bearerToken = getBearerTokenFromRequest(req);
    if (!bearerToken) {
      return res.status(401).json({ error: 'missing_access_token' });
    }

    let payloadJwt;
    try {
      payloadJwt = verifyBackendJwt(bearerToken);
    } catch (error) {
      return res.status(401).json({ error: 'invalid_access_token' });
    }

    const tokenResult = await getAccessTokenForSubject(payloadJwt.sub);
    if (tokenResult.error) {
      return res.status(tokenResult.status || 401).json({
        error: tokenResult.error,
        category: tokenResult.category,
        retryable: tokenResult.retryable
      });
    }

    const fieldList = parseFieldsParam(req.query.fields, ALLOWED_VIDEO_FIELDS);
    if (!fieldList) {
      return buildJsonError(res, 400, 'invalid_fields');
    }

    const rawMaxCount = Number(req.query.max_count || 20);
    if (!Number.isFinite(rawMaxCount) || rawMaxCount <= 0) {
      return buildJsonError(res, 400, 'invalid_max_count');
    }
    const maxCount = Math.min(rawMaxCount, 20);
    const payload = {
      max_count: maxCount
    };

    if (req.query.cursor) {
      const cursor = Number(req.query.cursor);
      if (!Number.isFinite(cursor) || cursor < 0) {
        return buildJsonError(res, 400, 'invalid_cursor');
      }
      payload.cursor = cursor;
    }

    const url = `${TIKTOK_API_BASE_URL}video/list/?fields=${encodeURIComponent(fieldList.join(','))}`;
    let response;
    try {
      response = await fetchWithProviderTimeout(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${tokenResult.accessToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });
    } catch (error) {
      const failure = getProviderFailure(error);
      return buildJsonError(res, failure.status, 'tiktok_request_failed', null, {
        category: failure.category,
        retryable: failure.retryable
      });
    }

    const parsed = await readJsonResponse(response);
    if (!parsed.ok) {
      return buildJsonError(res, 502, 'invalid_tiktok_response', null, {
        category: 'malformed_response',
        retryable: false
      });
    }
    res.status(response.status).json(parsed.data);
  } catch (error) {
    res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/connector/revoke', async (req, res) => {
  const bearerToken = getBearerTokenFromRequest(req);
  if (!bearerToken) {
    return res.status(401).json({ error: 'missing_access_token' });
  }
  let payload;
  try {
    payload = verifyBackendJwt(bearerToken);
  } catch (error) {
    return res.status(401).json({ error: 'invalid_access_token' });
  }
  try {
    const tokenData = await tokenStore.getConnectorToken(payload.sub);
    if (!tokenData) {
      return res.status(200).json({
        revoked: false,
        provider_revoke: { attempted: false, reason: 'credential_not_found' }
      });
    }

    const providerRevoke = await revokeProviderAccess(tokenData.accessToken);
    const revoked = await tokenStore.revokeConnectorToken(payload.sub);
    logEvent(providerRevoke.success ? 'info' : 'warn', 'connector_revoke', {
      correlation_id: req.correlationId,
      subject_fingerprint: getStateFingerprint(payload.sub),
      local_revoked: revoked,
      provider_success: providerRevoke.success,
      provider_category: providerRevoke.category || null
    });

    return res.status(200).json({ revoked, provider_revoke: providerRevoke });
  } catch (error) {
    return res.status(500).json({ error: 'server_error' });
  }
});

app.use('/api', (req, res) => {
  res.status(404).json({ error: 'not_found' });
});

app.use('/health', (req, res) => {
  res.status(404).json({ error: 'not_found' });
});

function originalQuery(req) {
  const queryIndex = req.originalUrl.indexOf('?');
  return queryIndex === -1 ? '' : req.originalUrl.slice(queryIndex);
}

function legacyAppDestination(req) {
  const queryIndex = req.originalUrl.indexOf('?');
  const requestPath = queryIndex === -1 ? req.originalUrl : req.originalUrl.slice(0, queryIndex);
  const suffix = requestPath.slice('/app'.length);
  const destinationPath = CLIENT_ROUTE_PATTERN.test(suffix) ? suffix : '/';
  return `${destinationPath}${originalQuery(req)}`;
}

function sendWebIndex(req, res) {
  if (!fs.existsSync(WEB_INDEX_FILE)) {
    return res.status(503).type('text/plain').send('Application build unavailable.');
  }
  res.set('cache-control', 'no-cache');
  return res.sendFile(WEB_INDEX_FILE);
}

app.get(['/app', '/app/*'], (req, res) => {
  res.redirect(308, legacyAppDestination(req));
});

app.get('/index.html', (req, res) => {
  res.redirect(308, `/${originalQuery(req)}`);
});

for (const [slug, filename] of PUBLIC_PAGE_FILES) {
  app.get([`/${slug}`, `/${slug}/`, `/${filename}`], (req, res) => {
    res.set('cache-control', 'no-cache');
    res.sendFile(path.join(PUBLIC_DIR, filename));
  });
}

app.get(['/logo.png', '/favicon.ico'], (req, res) => {
  res.set('cache-control', 'public, max-age=86400');
  res.sendFile(path.join(PUBLIC_DIR, 'logo.png'));
});

const WELL_KNOWN_DIR = path.join(PUBLIC_DIR, '.well-known');
if (fs.existsSync(WELL_KNOWN_DIR)) {
  app.use('/.well-known', express.static(WELL_KNOWN_DIR, {
    dotfiles: 'deny',
    fallthrough: false,
    index: false
  }));
}

if (fs.existsSync(WEB_ASSETS_DIR)) {
  app.use('/assets', express.static(WEB_ASSETS_DIR, {
    dotfiles: 'deny',
    fallthrough: false,
    immutable: true,
    index: false,
    maxAge: '1y'
  }));
}

app.get('/', sendWebIndex);
app.get(CLIENT_ROUTE_PATTERN, sendWebIndex);

app.use((req, res) => {
  res.status(404).type('text/plain').send('Not found.');
});

app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') {
    return res.status(413).json({ error: 'payload_too_large' });
  }
  if (err && err.message === 'CORS origin not allowed') {
    return res.status(403).json({ error: 'cors_not_allowed' });
  }
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    return res.status(400).json({ error: 'invalid_json' });
  }

  const status = Number.isInteger(err && err.status) ? err.status : 500;
  if (status >= 500) {
    logEvent('error', 'request_failed', {
      correlation_id: req.correlationId,
      method: req.method,
      path: req.path,
      status
    });
  }
  if (req.path.startsWith('/api/') || req.path.startsWith('/health/')) {
    return res.status(status).json({ error: status === 404 ? 'not_found' : 'server_error' });
  }
  if (res.headersSent) {
    return next(err);
  }
  return res.status(status).type('text/plain').send(status === 404 ? 'Not found.' : 'Request failed.');
});

function getListenTarget() {
  if (typeof PhusionPassenger !== 'undefined') {
    return 'passenger';
  }

  const raw = process.env.PORT || process.env.PASSENGER_PORT || '3001';

  if (/^\d+$/.test(String(raw))) {
    return Number(raw);
  }

  return raw;
}

let server;

if (process.env.NODE_ENV !== 'test') {
  const listenTarget = getListenTarget();

  server = app.listen(listenTarget, () => {
    console.log(`Backend listening on ${BASE_URL} via ${listenTarget}`);
  });
}

function shutdown() {
  console.log('Shutting down gracefully...');
  authCodeStore.stop();

  if (typeof stateStore.stop === 'function') {
    stateStore.stop();
  }

  if (server) {
    server.close(() => process.exit(0));
    return;
  }

  process.exit(0);
}

if (process.env.NODE_ENV !== 'test') {
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function stopStores() {
  authCodeStore.stop();
  if (typeof stateStore.stop === 'function') {
    stateStore.stop();
  }
}

function setFetchImplementation(fetchImplementation) {
  fetchImpl = fetchImplementation || require('node-fetch');
}

module.exports = {
  app,
  categorizeProviderStatus,
  escapeHtml,
  getAllowedLegacyRedirects,
  getStateFingerprint,
  getTrustProxySetting,
  isAllowedRedirect,
  parseTrustProxyValue,
  parseFieldsParam,
  readJsonResponse,
  setFetchImplementation,
  stopStores,
  validateRequiredEnv
};
