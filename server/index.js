require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const fetch = require('node-fetch');
const jwt = require('jsonwebtoken');
const path = require('path');
const { FileTokenStore, StateStore } = require('./store');

const PORT = process.env.PORT || 3001;
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY;
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET;
const BACKEND_JWT_SECRET = process.env.BACKEND_JWT_SECRET;
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
const AUTH_CODE_TTL_MS = 10 * 60 * 1000;
const BACKEND_TOKEN_TTL_SECONDS = 60 * 60;

const app = express();
const tokenStore = new FileTokenStore({
  filePath: path.join(__dirname, 'data', 'tokens.json'),
  lockPath: path.join(__dirname, 'data', 'tokens.json.lock'),
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

app.use((req, res, next) => {
  // Normalize repeated slashes (but keep query string)
  if (req.url.startsWith('//')) {
    const normalized = req.url.replace(/^\/+/, '/');
    return res.redirect(308, normalized);
  }
  next();
});

app.use(express.static(PUBLIC_DIR));
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

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
    return parsed.hostname === 'script.google.com';
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
    res.status(500).send(`<h1>Configuration error</h1><p>${error.message}</p>`);
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
      const safeLog = {
        ok: tokenResult.ok,
        tokenResultError: tokenResult.data && tokenResult.data.error,
        tokenResultErrorDescription: tokenResult.data && tokenResult.data.error_description,
        tokenPayloadError: tokenPayload && tokenPayload.error,
        tokenPayloadErrorDescription: tokenPayload && tokenPayload.error_description
      };
      console.error('Token exchange failed', safeLog);
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
    res.status(500).send(`<h1>Unexpected Error</h1><p>${error.message}</p>`);
  }
});

app.get('/oauth/authorize', (req, res) => {
  try {
    requireEnv();
    const { state, redirect_uri: redirectUri } = req.query;
    if (!state || !redirectUri) {
      return res.status(400).send('<h1>Missing OAuth parameters</h1><p>State and redirect_uri are required.</p>');
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
    res.status(500).send(`<h1>Configuration error</h1><p>${error.message}</p>`);
  }
});

app.post('/oauth/token', async (req, res) => {
  try {
    requireBackendJwt();
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
    res.status(500).json({ error: 'server_error', message: error.message });
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
      return res.status(tokenResult.status || 401).json({ error: tokenResult.error, details: tokenResult.details });
    }

    const fields = req.query.fields || [
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
    ].join(',');

    const url = `${TIKTOK_API_BASE_URL}user/info/?fields=${encodeURIComponent(fields)}`;
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${tokenResult.accessToken}`
      }
    });

    const data = await response.json();
    res.status(response.status).json(data);
  } catch (error) {
    res.status(500).json({ error: 'server_error', message: error.message });
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
      return res.status(tokenResult.status || 401).json({ error: tokenResult.error, details: tokenResult.details });
    }

    const fields = req.query.fields || [
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
    ].join(',');

    const maxCount = Math.min(Number(req.query.max_count || 20), 20);
    const payload = {
      max_count: maxCount
    };

    if (req.query.cursor) {
      payload.cursor = Number(req.query.cursor);
    }

    const url = `${TIKTOK_API_BASE_URL}video/list/?fields=${encodeURIComponent(fields)}`;
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
    res.status(500).json({ error: 'server_error', message: error.message });
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
    .catch(error => res.status(500).json({ error: 'server_error', message: error.message }));
});

app.listen(PORT, () => {
  console.log(`Backend listening on ${BASE_URL}`);
});

function shutdown() {
  authCodeStore.stop();
  if (typeof stateStore.stop === 'function') {
    stateStore.stop();
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
