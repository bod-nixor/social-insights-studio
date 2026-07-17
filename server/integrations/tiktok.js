const crypto = require('crypto');
let fetchImpl = require('node-fetch');

const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const TIKTOK_REVOKE_URL = 'https://open.tiktokapis.com/v2/oauth/revoke/';
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
const TIKTOK_VIDEO_LIST_URL = `${TIKTOK_API_BASE_URL}video/list/`;

const TIKTOK_SCOPES = [
  'user.info.basic',
  'user.info.profile',
  'user.info.stats',
  'video.list'
];

function setTikTokFetchImplementation(nextFetch) {
  fetchImpl = nextFetch || require('node-fetch');
}

function getCallbackUrl() {
  return process.env.TIKTOK_REDIRECT_URI || `${process.env.BASE_URL}/api/integrations/tiktok/callback`;
}

function buildAuthorizationUrl(state) {
  const params = new URLSearchParams({
    client_key: process.env.TIKTOK_CLIENT_KEY,
    response_type: 'code',
    scope: TIKTOK_SCOPES.join(','),
    redirect_uri: getCallbackUrl(),
    state
  });
  return `${TIKTOK_AUTH_URL}?${params.toString()}`;
}

async function readJson(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (error) {
    const providerError = new Error('malformed_provider_response');
    providerError.category = 'malformed_response';
    providerError.retryable = false;
    throw providerError;
  }
}

async function providerFetch(url, options = {}) {
  const timeoutMs = Number(process.env.PROVIDER_HTTP_TIMEOUT_MS || 10000);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetchImpl(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error && error.name === 'AbortError') {
      const timeoutError = new Error('provider_timeout');
      timeoutError.category = 'timeout';
      timeoutError.retryable = true;
      throw timeoutError;
    }
    const networkError = new Error('provider_network_error');
    networkError.category = 'network';
    networkError.retryable = true;
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

function getTokenPayload(body) {
  return body && typeof body === 'object' ? body.data || body : {};
}

function extractProviderCode(body) {
  if (!body || typeof body !== 'object') return null;
  if (body.error && typeof body.error === 'object') return body.error.code || body.error.message || null;
  if (typeof body.error === 'string') return body.error;
  return body.code || null;
}

function categorizeProviderFailure(status, body) {
  const code = extractProviderCode(body);
  if (status === 401 || status === 403) {
    return { category: 'authentication', retryable: false, provider_code: code };
  }
  if (status === 429) {
    return { category: 'rate_limit', retryable: true, provider_code: code };
  }
  if (status >= 500) {
    return { category: 'provider', retryable: true, provider_code: code };
  }
  if (status >= 400) {
    const text = String(code || '').toLowerCase();
    const category = text.includes('scope') || text.includes('permission') ? 'scope' : 'provider';
    return { category, retryable: false, provider_code: code };
  }
  return { category: 'malformed_response', retryable: false, provider_code: code };
}

function providerFailureResult(status, body) {
  return {
    ok: false,
    status,
    body,
    error: categorizeProviderFailure(status, body)
  };
}

async function callProvider(url, options) {
  try {
    const response = await providerFetch(url, options);
    const body = await readJson(response);
    if (!response.ok) return providerFailureResult(response.status, body);
    return { ok: true, status: response.status, body };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      body: null,
      error: {
        category: error.category || 'network',
        retryable: error.retryable !== false,
        provider_code: error.message
      }
    };
  }
}

async function exchangeCode(code) {
  const payload = new URLSearchParams({
    client_key: process.env.TIKTOK_CLIENT_KEY,
    client_secret: process.env.TIKTOK_CLIENT_SECRET,
    grant_type: 'authorization_code',
    code,
    redirect_uri: getCallbackUrl()
  });
  const result = await callProvider(TIKTOK_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: payload.toString()
  });
  return { ...result, payload: getTokenPayload(result.body) };
}

async function refreshAccessToken(refreshToken) {
  const payload = new URLSearchParams({
    client_key: process.env.TIKTOK_CLIENT_KEY,
    client_secret: process.env.TIKTOK_CLIENT_SECRET,
    grant_type: 'refresh_token',
    refresh_token: refreshToken
  });
  const result = await callProvider(TIKTOK_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: payload.toString()
  });
  return { ...result, payload: getTokenPayload(result.body) };
}

async function fetchProfile(accessToken) {
  const fields = [
    'open_id',
    'union_id',
    'username',
    'display_name',
    'avatar_url',
    'profile_deep_link',
    'follower_count',
    'following_count',
    'likes_count',
    'video_count'
  ];
  const result = await callProvider(`${TIKTOK_API_BASE_URL}user/info/?fields=${encodeURIComponent(fields.join(','))}`, {
    headers: { Authorization: `Bearer ${accessToken}` }
  });
  return { ...result, user: result.body && result.body.data && result.body.data.user };
}

async function fetchVideosPage(accessToken, cursor = 0) {
  const fields = [
    'id',
    'create_time',
    'title',
    'video_description',
    'duration',
    'height',
    'width',
    'share_url',
    'view_count',
    'like_count',
    'comment_count',
    'share_count'
  ];
  const url = `${TIKTOK_VIDEO_LIST_URL}?fields=${encodeURIComponent(fields.join(','))}`;
  const result = await callProvider(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      max_count: Number(process.env.TIKTOK_VIDEO_PAGE_SIZE || 20),
      cursor
    })
  });
  const data = result.body && result.body.data ? result.body.data : {};
  return {
    ...result,
    videos: Array.isArray(data.videos) ? data.videos : [],
    cursor: data.cursor || 0,
    has_more: Boolean(data.has_more)
  };
}

async function revokeAccess(accessToken) {
  const payload = new URLSearchParams({
    client_key: process.env.TIKTOK_CLIENT_KEY,
    client_secret: process.env.TIKTOK_CLIENT_SECRET,
    token: accessToken
  });
  const result = await callProvider(TIKTOK_REVOKE_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: payload.toString()
  });
  return {
    attempted: true,
    success: result.ok,
    status: result.status,
    error: result.ok ? undefined : result.error
  };
}

function grantedScopes(scopeValue) {
  return new Set(String(scopeValue || '').split(/[,\s]+/).filter(Boolean));
}

function missingScopes(scopeValue) {
  const granted = grantedScopes(scopeValue);
  return TIKTOK_SCOPES.filter(scope => !granted.has(scope));
}

module.exports = {
  TIKTOK_SCOPES,
  buildAuthorizationUrl,
  categorizeProviderFailure,
  exchangeCode,
  fetchVideosPage,
  fetchProfile,
  missingScopes,
  refreshAccessToken,
  revokeAccess,
  setTikTokFetchImplementation
};
