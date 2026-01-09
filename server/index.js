require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const fetch = require('node-fetch');
const jwt = require('jsonwebtoken');
const path = require('path');
const { FileTokenStore, StateStore } = require('./store');

const PORT = Number(process.env.PORT || 3001);
const BASE_URL = process.env.BASE_URL;
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY;
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET;
const BACKEND_JWT_SECRET = process.env.BACKEND_JWT_SECRET;
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
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
const tokenStore = new FileTokenStore({
  filePath: tokenStorePath,
  lockPath: tokenLockPath,
  encryptionKey: process.env.ENCRYPTION_KEY,
  pruneAfterDays: process.env.TOKEN_PRUNE_DAYS ? Number(process.env.TOKEN_PRUNE_DAYS) : undefined
});
const stateStore = new StateStore();
const authCodeStore = new StateStore(AUTH_CODE_TTL_MS);

const REQUIRED_SCOPES = [
  'user.info.basic',
  'user.info.profile',
  'user.info.stats',
  'video.list'
].join(',');

const PUBLIC_DIR = path.join(__dirname, 'public');

function ensureTokenStoreOutsidePublic() {
  const resolvedStore = path.resolve(tokenStorePath);
  const resolvedPublic = path.resolve(PUBLIC_DIR);
  const relative = path.relative(resolvedPublic, resolvedStore);
  if (!relative || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    throw new Error('TOKEN_STORE_PATH must be outside the public web root.');
  }
}

ensureTokenStoreOutsidePublic();

const app = express();
const trustProxyValue = process.env.TRUST_PROXY;
if (trustProxyValue) {
  const numeric = Number(trustProxyValue);
  app.set('trust proxy', Number.isNaN(numeric) ? trustProxyValue : numeric);
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

function getRedirectUri() {
  return `${BASE_URL}/auth/tiktok/callback`;
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

  const data = await response.json();
  return { ok: response.ok, data };
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

  const data = await response.json();
  return { ok: response.ok, data };
}

function buildTokenRecord(tokenResponse) {
  return {
    accessToken: tokenResponse.access_token,
    refreshToken: tokenResponse.refresh_token,
    expiresAt: Date.now() + Number(tokenResponse.expires_in || 0) * 1000,
    refreshExpiresAt: Date.now() + Number(tokenResponse.refresh_expires_in || 0) * 1000,
    openId: tokenResponse.open_id || null,
    scopes: tokenResponse.scope || null,
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
  if (!refreshResult.ok) {
    return { error: 'token_refresh_failed', status: 401, details: refreshResult.data };
  }

  const updated = buildTokenRecord(refreshResult.data.data || refreshResult.data);
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

function buildAuthorizationCode(subject, scopes) {
  const code = generateRandomToken(24);
  authCodeStore.save(code, { subject, scopes, createdAt: Date.now() });
  return code;
}

function consumeAuthorizationCode(code) {
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
    return parsed.protocol === 'https:'
      && parsed.hostname === 'script.google.com'
      && parsed.pathname.startsWith('/macros/s/');
  } catch (error) {
    return false;
  }
}

app.get('/auth/tiktok/start', (req, res) => {
  try {
    requireEnv();
    const state = generateRandomToken(16);
    stateStore.save(state, { flow: 'direct', createdAt: Date.now() });
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
        `<h1>Authorization Error</h1><p>${error}: ${errorDescription || 'No description provided.'}</p>`
      );
    }

    if (!code || !state) {
      return res.status(400).send('<h1>Missing code or state</h1><p>Please restart authentication.</p>');
    }

    const stateEntry = stateStore.consume(state);
    if (!stateEntry) {
      return res.status(400).send('<h1>Invalid state</h1><p>Please restart authentication.</p>');
    }

    const tokenResult = await exchangeCodeForToken(code);
    const tokenPayload = tokenResult.data.data || tokenResult.data;

    if (!tokenResult.ok || tokenPayload.error) {
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
      const authCode = buildAuthorizationCode(record.openId, record.scopes);
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

app.get('/oauth/authorize', (req, res) => {
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
    stateStore.save(tiktokState, {
      flow: 'oauth',
      redirectUri,
      lookerState: state
    });
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
      const entry = consumeAuthorizationCode(code);
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

    const data = await response.json();
    res.status(response.status).json(data);
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

    const data = await response.json();
    res.status(response.status).json(data);
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

app.listen(PORT, () => {
  console.log(`Backend listening on ${BASE_URL}`);
});

function shutdown() {
  console.log('Shutting down gracefully...');
  authCodeStore.stop();
  if (typeof stateStore.stop === 'function') {
    stateStore.stop();
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
