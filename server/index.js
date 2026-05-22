require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const fetch = require('node-fetch');
const jwt = require('jsonwebtoken');
const path = require('path');
const { FileStateStore, FileTokenStore } = require('./store');

const PORT = Number(process.env.PORT || 3001);
const BASE_URL = process.env.BASE_URL;
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY;
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET;
const BACKEND_JWT_SECRET = process.env.BACKEND_JWT_SECRET;
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
const OAUTH_STATE_TTL_MS = 10 * 60 * 1000;
const AUTH_CODE_TTL_MS = 10 * 60 * 1000;
const BACKEND_TOKEN_TTL_SECONDS = 60 * 60;
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

function validateRequiredEnv() {
  if (!BASE_URL) {
    throw new Error('Missing BASE_URL environment variable.');
  }
  if (process.env.NODE_ENV === 'production' && !BASE_URL.startsWith('https://')) {
    throw new Error('BASE_URL must be https:// in production.');
  }
  if (!process.env.ENCRYPTION_KEY) {
    throw new Error('Missing ENCRYPTION_KEY environment variable.');
  }
  if (!BACKEND_JWT_SECRET) {
    throw new Error('Missing BACKEND_JWT_SECRET environment variable.');
  }
  const weakSecrets = new Set(['changeme', 'change-me', 'secret', 'password', 'default']);
  if (BACKEND_JWT_SECRET.length < 32 || weakSecrets.has(BACKEND_JWT_SECRET.toLowerCase())) {
    throw new Error(
      'BACKEND_JWT_SECRET must be at least 32 characters and not a common placeholder. ' +
      'Generate a cryptographically random secret (e.g., crypto.randomBytes(32).toString("hex")).'
    );
  }
  if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET) {
    throw new Error('Missing TikTok client credentials. Set TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET.');
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
  legacyHeaders: false
});
const apiLimiter = rateLimit({
  windowMs: (Number(process.env.API_RATE_LIMIT_WINDOW_MINUTES) || 5) * 60 * 1000,
  max: Number(process.env.API_RATE_LIMIT_MAX) || 120,
  standardHeaders: true,
  legacyHeaders: false
});

app.use(express.static(PUBLIC_DIR));
app.use(express.urlencoded({ extended: false, limit: DEFAULT_BODY_LIMIT }));
app.use(express.json({ limit: DEFAULT_BODY_LIMIT }));

app.use('/oauth', corsMiddleware, authLimiter);
app.use('/auth', authLimiter);
app.use('/api', corsMiddleware, apiLimiter);

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
  return next(err);
});

const LOOKER_CLIENT_ID = process.env.LOOKER_CLIENT_ID || 'looker-studio-connector';
const LOOKER_CLIENT_SECRET = process.env.LOOKER_CLIENT_SECRET || 'unused';

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

function logOAuthState(event, state, details = {}) {
  console.info('oauth_state', {
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

  const response = await fetch(TIKTOK_TOKEN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: payload.toString()
  });

  const parsed = await readJsonResponse(response);
  return { ok: response.ok && parsed.ok, data: parsed.data };
}

async function refreshAccessToken(refreshToken) {
  const payload = new URLSearchParams({
    client_key: TIKTOK_CLIENT_KEY,
    client_secret: TIKTOK_CLIENT_SECRET,
    grant_type: 'refresh_token',
    refresh_token: refreshToken
  });

  const response = await fetch(TIKTOK_TOKEN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: payload.toString()
  });

  const parsed = await readJsonResponse(response);
  return { ok: response.ok && parsed.ok, data: parsed.data };
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
    return { error: 'token_refresh_failed', status: 401, details: refreshResult.data };
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

function buildJsonError(res, status, error, message) {
  const payload = { error };
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

async function buildAuthorizationCode(subject, scopes) {
  const code = generateRandomToken(24);
  await authCodeStore.save(code, { subject, scopes, createdAt: Date.now() });
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

function isAllowedRedirect(redirectUri) {
  try {
    const parsed = new URL(redirectUri);
    if (parsed.protocol !== 'https:' || parsed.hostname !== 'script.google.com') {
      return false;
    }
    const isOAuth2LibraryCallback = /^\/macros\/d\/[^/]+\/usercallback$/.test(parsed.pathname);
    const isWebAppDeployment = /^\/macros\/s\/[^/]+\/(exec|dev)$/.test(parsed.pathname);
    return isOAuth2LibraryCallback || isWebAppDeployment;
  } catch (error) {
    return false;
  }
}

app.get('/auth/tiktok/start', async (req, res) => {
  try {
    requireEnv();
    const state = generateRandomToken(16);
    await stateStore.save(state, { flow: 'direct', createdAt: Date.now() });
    logOAuthState('create', state, { flow: 'direct', outcome: 'saved' });
    res.redirect(buildAuthUrl(state));
  } catch (error) {
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
      console.error('Token exchange failed.');
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
      const authCode = await buildAuthorizationCode(record.openId, record.scopes);
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
    if (!isAllowedRedirect(redirectUri)) {
      return res.status(400).send('<h1>Invalid redirect URI</h1><p>Redirect URI not allowed.</p>');
    }
    const tiktokState = generateRandomToken(16);
    await stateStore.save(tiktokState, {
      flow: 'oauth',
      redirectUri,
      lookerState: state
    });
    logOAuthState('create', tiktokState, { flow: 'oauth', outcome: 'saved' });
    res.redirect(buildAuthUrl(tiktokState));
  } catch (error) {
    res.status(500).send('<h1>Configuration error</h1><p>Backend configuration error.</p>');
  }
});

app.post('/oauth/token', async (req, res) => {
  try {
    requireBackendJwt();
    const clientId = req.body.client_id;
    const clientSecret = req.body.client_secret;
    if (clientId && clientId !== LOOKER_CLIENT_ID) {
      return res.status(400).json({ error: 'invalid_client' });
    }
    if (clientSecret && clientSecret !== LOOKER_CLIENT_SECRET) {
      return res.status(400).json({ error: 'invalid_client' });
    }
    const grantType = req.body.grant_type;
    if (grantType === 'authorization_code') {
      const code = req.body.code;
      if (!code) {
        return res.status(400).json({ error: 'invalid_request', error_description: 'Missing code.' });
      }
      const entry = await consumeAuthorizationCode(code);
      if (!entry) {
        return res.status(400).json({ error: 'invalid_grant', error_description: 'Code expired or invalid.' });
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
      return res.status(tokenResult.status || 401).json({ error: tokenResult.error });
    }

    const fieldList = parseFieldsParam(req.query.fields, ALLOWED_USER_FIELDS);
    if (!fieldList) {
      return buildJsonError(res, 400, 'invalid_fields');
    }

    const url = `${TIKTOK_API_BASE_URL}user/info/?fields=${encodeURIComponent(fieldList.join(','))}`;
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${tokenResult.accessToken}`
      }
    });

    const parsed = await readJsonResponse(response);
    if (!parsed.ok) {
      return buildJsonError(res, 502, 'invalid_tiktok_response');
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
      return res.status(tokenResult.status || 401).json({ error: tokenResult.error });
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
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${tokenResult.accessToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    const parsed = await readJsonResponse(response);
    if (!parsed.ok) {
      return buildJsonError(res, 502, 'invalid_tiktok_response');
    }
    res.status(response.status).json(parsed.data);
  } catch (error) {
    res.status(500).json({ error: 'server_error' });
  }
});

app.post('/api/connector/revoke', (req, res) => {
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
  tokenStore.revokeConnectorToken(payload.sub)
    .then(revoked => res.status(revoked ? 200 : 404).json({ revoked }))
    .catch(() => res.status(500).json({ error: 'server_error' }));
});

let server;

if (require.main === module) {
  server = app.listen(PORT, () => {
    console.log(`Backend listening on ${BASE_URL}`);
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

if (require.main === module) {
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function stopStores() {
  authCodeStore.stop();
  if (typeof stateStore.stop === 'function') {
    stateStore.stop();
  }
}

module.exports = {
  app,
  escapeHtml,
  getStateFingerprint,
  getTrustProxySetting,
  isAllowedRedirect,
  parseTrustProxyValue,
  parseFieldsParam,
  readJsonResponse,
  stopStores
};
