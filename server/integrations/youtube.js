const crypto = require('crypto');
let fetchImpl = require('node-fetch');
let sleepImpl = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
let randomImpl = Math.random;

const { getYouTubeLimits } = require('../platform/youtube-config');

const GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_REVOKE_URL = 'https://oauth2.googleapis.com/revoke';
const YOUTUBE_DATA_API_URL = 'https://www.googleapis.com/youtube/v3';
const YOUTUBE_ANALYTICS_API_URL = 'https://youtubeanalytics.googleapis.com/v2/reports';

const YOUTUBE_SCOPES = [
  'https://www.googleapis.com/auth/youtube.readonly',
  'https://www.googleapis.com/auth/yt-analytics.readonly'
];

const YOUTUBE_ANALYTICS_METRICS = [
  'views',
  'estimatedMinutesWatched',
  'averageViewDuration',
  'averageViewPercentage',
  'subscribersGained',
  'subscribersLost',
  'likes',
  'comments',
  'shares'
];

function setYouTubeTestHooks(hooks = {}) {
  fetchImpl = hooks.fetch || require('node-fetch');
  sleepImpl = hooks.sleep || (milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)));
  randomImpl = hooks.random || Math.random;
}

function createPkcePair() {
  const verifier = crypto.randomBytes(64).toString('base64url');
  const challenge = crypto.createHash('sha256').update(verifier).digest('base64url');
  return { verifier, challenge };
}

function buildAuthorizationUrl({ state, codeChallenge, promptConsent = false }, env = process.env) {
  const params = new URLSearchParams({
    client_id: env.YOUTUBE_CLIENT_ID,
    redirect_uri: env.YOUTUBE_REDIRECT_URI,
    response_type: 'code',
    scope: YOUTUBE_SCOPES.join(' '),
    access_type: 'offline',
    include_granted_scopes: 'true',
    state,
    code_challenge: codeChallenge,
    code_challenge_method: 'S256'
  });
  if (promptConsent) params.set('prompt', 'consent');
  return `${GOOGLE_AUTH_URL}?${params.toString()}`;
}

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds);
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return null;
  return Math.max(0, Math.ceil((timestamp - Date.now()) / 1000));
}

function extractProviderCode(body) {
  if (!body || typeof body !== 'object') return null;
  if (typeof body.error === 'string') return body.error;
  if (body.error && typeof body.error === 'object') {
    const detail = Array.isArray(body.error.errors) ? body.error.errors[0] : null;
    return (detail && detail.reason) || body.error.status || body.error.message || null;
  }
  return null;
}

function categorizeProviderFailure(status, body) {
  const providerCode = extractProviderCode(body);
  const normalized = String(providerCode || '').toLowerCase();
  if (normalized === 'invalid_grant') {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: providerCode };
  }
  if (normalized === 'access_denied') {
    return { category: 'authentication', retryable: false, denied: true, provider_code: providerCode };
  }
  if (
    normalized.includes('insufficientpermission') ||
    normalized.includes('forbidden') ||
    normalized.includes('scope')
  ) {
    return { category: 'scope', retryable: false, provider_code: providerCode };
  }
  if (normalized.includes('quotaexceeded') || normalized.includes('dailylimit')) {
    return { category: 'quota', retryable: false, provider_code: providerCode };
  }
  if (status === 429 || normalized.includes('ratelimit')) {
    return { category: 'rate_limit', retryable: true, provider_code: providerCode };
  }
  if (status === 401) {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: providerCode };
  }
  if (status === 403) {
    return { category: 'authentication', retryable: false, provider_code: providerCode };
  }
  if (status >= 500) {
    return { category: 'provider', retryable: true, provider_code: providerCode };
  }
  if (status >= 400) {
    return { category: 'provider', retryable: false, provider_code: providerCode };
  }
  return { category: 'malformed_response', retryable: false, provider_code: providerCode };
}

async function readJson(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    const error = new Error('malformed_provider_response');
    error.category = 'malformed_response';
    error.retryable = false;
    throw error;
  }
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetchImpl(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error && error.name === 'AbortError') {
      const timeoutError = new Error('youtube_request_timeout');
      timeoutError.category = 'timeout';
      timeoutError.retryable = true;
      throw timeoutError;
    }
    const networkError = new Error('youtube_network_error');
    networkError.category = 'network';
    networkError.retryable = true;
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

async function callGoogle(url, options = {}, requestOptions = {}) {
  const limits = getYouTubeLimits(requestOptions.env || process.env);
  const maxRetries = requestOptions.maxRetries === undefined ? limits.maxRetries : requestOptions.maxRetries;
  let lastResult = null;
  for (let attempt = 1; attempt <= maxRetries + 1; attempt += 1) {
    const remainingMs = requestOptions.deadlineMs ? requestOptions.deadlineMs - Date.now() : null;
    if (remainingMs !== null && remainingMs <= 0) {
      return {
        ok: false,
        status: 0,
        body: null,
        attempts: attempt - 1,
        retryAfterSeconds: null,
        budgetExhausted: true,
        error: {
          category: 'timeout',
          retryable: true,
          provider_code: 'youtube_time_budget_exhausted'
        }
      };
    }
    try {
      const timeoutMs = remainingMs === null
        ? limits.requestTimeoutMs
        : Math.max(1, Math.min(limits.requestTimeoutMs, remainingMs));
      const response = await fetchWithTimeout(url, options, timeoutMs);
      const body = await readJson(response);
      if (response.ok) {
        return { ok: true, status: response.status, body, attempts: attempt, retryAfterSeconds: null };
      }
      const retryAfterSeconds = parseRetryAfter(response.headers && response.headers.get('retry-after'));
      const error = categorizeProviderFailure(response.status, body);
      lastResult = {
        ok: false,
        status: response.status,
        body,
        attempts: attempt,
        retryAfterSeconds,
        error
      };
    } catch (error) {
      lastResult = {
        ok: false,
        status: 0,
        body: null,
        attempts: attempt,
        retryAfterSeconds: null,
        error: {
          category: error.category || 'network',
          retryable: error.retryable !== false,
          provider_code: error.message
        }
      };
    }

    if (!lastResult.error.retryable || attempt > maxRetries) return lastResult;
    const retryDelay = lastResult.retryAfterSeconds === null
      ? Math.min(5000, 250 * (2 ** (attempt - 1)) + Math.floor(randomImpl() * 250))
      : Math.min(30000, lastResult.retryAfterSeconds * 1000);
    if (requestOptions.deadlineMs && Date.now() + retryDelay >= requestOptions.deadlineMs) {
      return { ...lastResult, budgetExhausted: true };
    }
    await sleepImpl(retryDelay);
  }
  return lastResult;
}

function formHeaders() {
  return { 'Content-Type': 'application/x-www-form-urlencoded' };
}

async function exchangeCode(code, codeVerifier, env = process.env) {
  const body = new URLSearchParams({
    client_id: env.YOUTUBE_CLIENT_ID,
    client_secret: env.YOUTUBE_CLIENT_SECRET,
    code,
    code_verifier: codeVerifier,
    grant_type: 'authorization_code',
    redirect_uri: env.YOUTUBE_REDIRECT_URI
  });
  return callGoogle(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: formHeaders(),
    body: body.toString()
  }, { env, maxRetries: 0 });
}

async function refreshAccessToken(refreshToken, requestOptions = {}) {
  const env = requestOptions.env || process.env;
  const body = new URLSearchParams({
    client_id: env.YOUTUBE_CLIENT_ID,
    client_secret: env.YOUTUBE_CLIENT_SECRET,
    refresh_token: refreshToken,
    grant_type: 'refresh_token'
  });
  return callGoogle(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: formHeaders(),
    body: body.toString()
  }, { ...requestOptions, env, maxRetries: Math.min(1, getYouTubeLimits(env).maxRetries) });
}

async function revokeToken(token, requestOptions = {}) {
  const body = new URLSearchParams({ token });
  const result = await callGoogle(GOOGLE_REVOKE_URL, {
    method: 'POST',
    headers: formHeaders(),
    body: body.toString()
  }, { ...requestOptions, maxRetries: 0 });
  return {
    attempted: true,
    success: result.ok,
    status: result.status,
    error: result.ok ? null : result.error
  };
}

function authorizedGet(url, accessToken, requestOptions) {
  return callGoogle(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: 'application/json'
    }
  }, requestOptions);
}

function dataApiUrl(resource, params) {
  return `${YOUTUBE_DATA_API_URL}/${resource}?${new URLSearchParams(params).toString()}`;
}

async function listMyChannels(accessToken, requestOptions = {}) {
  return authorizedGet(dataApiUrl('channels', {
    part: 'id,snippet,statistics,contentDetails',
    mine: 'true',
    maxResults: '50'
  }), accessToken, requestOptions);
}

async function getChannel(accessToken, channelId, requestOptions = {}) {
  return authorizedGet(dataApiUrl('channels', {
    part: 'id,snippet,statistics,contentDetails',
    id: String(channelId),
    maxResults: '1'
  }), accessToken, requestOptions);
}

async function listUploadItems(accessToken, playlistId, pageToken, requestOptions = {}) {
  const params = {
    part: 'contentDetails,snippet,status',
    playlistId: String(playlistId),
    maxResults: '50'
  };
  if (pageToken) params.pageToken = String(pageToken);
  return authorizedGet(dataApiUrl('playlistItems', params), accessToken, requestOptions);
}

async function listVideos(accessToken, videoIds, requestOptions = {}) {
  if (!Array.isArray(videoIds) || videoIds.length === 0 || videoIds.length > 50) {
    throw new Error('youtube_video_batch_invalid');
  }
  return authorizedGet(dataApiUrl('videos', {
    part: 'snippet,contentDetails,statistics,status,liveStreamingDetails',
    id: videoIds.map(String).join(',')
  }), accessToken, requestOptions);
}

async function queryAnalytics(accessToken, query, requestOptions = {}) {
  const params = {
    ids: `channel==${String(query.channelId)}`,
    startDate: query.startDate,
    endDate: query.endDate,
    metrics: (query.metrics || YOUTUBE_ANALYTICS_METRICS).join(','),
    dimensions: query.dimensions
  };
  if (query.sort) params.sort = query.sort;
  if (query.maxResults) params.maxResults = String(query.maxResults);
  if (query.filters) params.filters = query.filters;
  return authorizedGet(`${YOUTUBE_ANALYTICS_API_URL}?${new URLSearchParams(params).toString()}`, accessToken, requestOptions);
}

function grantedScopes(scopeValue) {
  const values = Array.isArray(scopeValue)
    ? scopeValue.map(String)
    : String(scopeValue || '').split(/[\s,]+/);
  return new Set(values.filter(Boolean));
}

function missingScopes(scopeValue) {
  const granted = grantedScopes(scopeValue);
  return YOUTUBE_SCOPES.filter(scope => !granted.has(scope));
}

function hasExactScopes(scopeValue) {
  const granted = grantedScopes(scopeValue);
  return granted.size === YOUTUBE_SCOPES.length && YOUTUBE_SCOPES.every(scope => granted.has(scope));
}

function chooseRefreshToken(responseToken, existingToken = null) {
  const rotated = typeof responseToken === 'string' ? responseToken.trim() : '';
  if (rotated) return rotated;
  const existing = typeof existingToken === 'string' ? existingToken.trim() : '';
  return existing || null;
}

module.exports = {
  GOOGLE_AUTH_URL,
  GOOGLE_REVOKE_URL,
  GOOGLE_TOKEN_URL,
  YOUTUBE_ANALYTICS_METRICS,
  YOUTUBE_SCOPES,
  buildAuthorizationUrl,
  categorizeProviderFailure,
  chooseRefreshToken,
  createPkcePair,
  exchangeCode,
  getChannel,
  grantedScopes,
  hasExactScopes,
  listMyChannels,
  listUploadItems,
  listVideos,
  missingScopes,
  parseRetryAfter,
  queryAnalytics,
  refreshAccessToken,
  revokeToken,
  setYouTubeTestHooks
};
