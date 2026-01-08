require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const { InMemoryTokenStore } = require('./store');

const app = express();
const tokenStore = new InMemoryTokenStore();

const PORT = process.env.PORT || 3001;
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY;
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET;
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';

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

function requireEnv() {
  if (!TIKTOK_CLIENT_KEY || !TIKTOK_CLIENT_SECRET) {
    throw new Error('Missing TikTok client credentials. Set TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET.');
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
    scope: tokenResponse.scope || null,
    tokenType: tokenResponse.token_type || 'Bearer'
  };
}

async function getAccessTokenForConnector(connectorToken) {
  const tokenData = tokenStore.getConnectorToken(connectorToken);
  if (!tokenData) {
    return { error: 'invalid_connector_token', status: 401 };
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
  tokenStore.saveConnectorToken(connectorToken, updated);

  return { accessToken: updated.accessToken };
}

function getConnectorTokenFromRequest(req) {
  const authHeader = req.get('authorization') || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

app.get('/auth/tiktok/start', (req, res) => {
  try {
    requireEnv();
    const state = generateRandomToken(16);
    tokenStore.saveState(state, { createdAt: Date.now() });
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

    const stateEntry = tokenStore.consumeState(state);
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

    const connectorToken = generateRandomToken(32);
    const record = buildTokenRecord(tokenPayload);
    tokenStore.saveConnectorToken(connectorToken, record);
    console.log('Saving connector token prefix=', connectorToken.slice(0, 6),
            'totalTokens=', tokenStore.connectorStore.size);

    return res.send(`
      <!doctype html>
      <html lang="en">
        <head>
          <meta charset="UTF-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>TikTok Connected</title>
          <style>
            body { font-family: Arial, sans-serif; margin: 2rem; }
            code { background: #f3f3f3; padding: 0.2rem 0.4rem; border-radius: 4px; }
            .token { font-size: 1.1rem; word-break: break-all; }
          </style>
        </head>
        <body>
          <h1>Success!</h1>
          <p>Your TikTok account is connected.</p>
          <p>Your connector token:</p>
          <p class="token"><code>${connectorToken}</code></p>
          <p>Copy this token and paste it into the Looker Studio connector configuration.</p>
        </body>
      </html>
    `);
  } catch (error) {
    res.status(500).send(`<h1>Unexpected Error</h1><p>${error.message}</p>`);
  }
});

app.get('/api/tiktok/user', async (req, res) => {
  try {
    requireEnv();
    const connectorToken = getConnectorTokenFromRequest(req);
    if (!connectorToken) {
      return res.status(401).json({ error: 'missing_connector_token' });
    }

    const tokenResult = await getAccessTokenForConnector(connectorToken);
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
    const connectorToken = getConnectorTokenFromRequest(req);
    if (!connectorToken) {
      return res.status(401).json({ error: 'missing_connector_token' });
    }

    const tokenResult = await getAccessTokenForConnector(connectorToken);
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
  const connectorToken = getConnectorTokenFromRequest(req);
  if (!connectorToken) {
    return res.status(401).json({ error: 'missing_connector_token' });
  }

  const revoked = tokenStore.revokeConnectorToken(connectorToken);
  res.status(revoked ? 200 : 404).json({ revoked });
});

app.listen(PORT, () => {
  console.log(`Backend listening on ${BASE_URL}`);
});
