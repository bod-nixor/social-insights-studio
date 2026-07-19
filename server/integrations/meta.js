const crypto = require('crypto');
let fetchImpl = require('node-fetch');
let sleepImpl = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
let randomImpl = Math.random;

const {
  META_GRAPH_API_VERSION,
  META_LOGIN_CONFIG_ENV,
  META_REQUIRED_SCOPES,
  getMetaLimits
} = require('../platform/meta-config');

const META_AUTH_URL = `https://www.facebook.com/${META_GRAPH_API_VERSION}/dialog/oauth`;
const META_GRAPH_URL = `https://graph.facebook.com/${META_GRAPH_API_VERSION}`;
const META_AUTOMATIC_SCOPES = Object.freeze(['public_profile']);
const META_FORBIDDEN_SCOPE_TERMS = Object.freeze([
  'ads',
  'business_management',
  'comment',
  'manage_messages',
  'messaging',
  'pages_manage',
  'publish',
  'read_page_mailboxes',
  'webhook'
]);

const FACEBOOK_PAGE_INSIGHT_METRICS = Object.freeze([
  'page_follows',
  'page_daily_follows_unique',
  'page_daily_unfollows_unique',
  'page_post_engagements',
  'page_media_view',
  'page_total_media_view_unique'
]);
const FACEBOOK_POST_INSIGHT_METRICS = Object.freeze([
  'post_media_view',
  'post_total_media_view_unique'
]);
const INSTAGRAM_ACCOUNT_INSIGHT_METRICS = Object.freeze([
  'views',
  'reach',
  'accounts_engaged',
  'total_interactions',
  'likes',
  'comments',
  'saves',
  'shares'
]);
const INSTAGRAM_MEDIA_INSIGHT_METRICS = Object.freeze({
  IMAGE: Object.freeze(['views', 'reach', 'saved', 'shares']),
  VIDEO: Object.freeze(['views', 'reach', 'saved', 'shares']),
  CAROUSEL_ALBUM: Object.freeze(['views', 'reach', 'saved', 'shares']),
  REELS: Object.freeze([
    'views',
    'reach',
    'saved',
    'shares'
  ])
});

function setMetaTestHooks(hooks = {}) {
  fetchImpl = hooks.fetch || require('node-fetch');
  sleepImpl = hooks.sleep || (milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)));
  randomImpl = hooks.random || Math.random;
}

function scopeValues(value) {
  return [...new Set(
    (Array.isArray(value) ? value : String(value || '').split(/[\s,]+/))
      .map(scope => String(scope).trim())
      .filter(Boolean)
  )];
}

function forbiddenScopes(value) {
  return scopeValues(value).filter(scope => {
    const normalized = scope.toLowerCase();
    return META_FORBIDDEN_SCOPE_TERMS.some(term => normalized.includes(term));
  });
}

function hasExactProductScopes(provider, value) {
  const required = META_REQUIRED_SCOPES[provider] || [];
  const granted = scopeValues(value);
  const permitted = new Set([
    ...Object.values(META_REQUIRED_SCOPES).flat(),
    ...META_AUTOMATIC_SCOPES
  ]);
  return required.every(scope => granted.includes(scope)) && granted.every(scope => permitted.has(scope));
}

function buildAuthorizationUrl(provider, { state }, env = process.env) {
  const required = META_REQUIRED_SCOPES[provider];
  if (!required) throw new Error('meta_provider_invalid');
  if (forbiddenScopes(required).length > 0) throw new Error('meta_scope_policy_violation');
  const redirectEnv = provider === 'facebook_pages' ? 'FACEBOOK_REDIRECT_URI' : 'INSTAGRAM_REDIRECT_URI';
  const params = new URLSearchParams({
    client_id: env.META_APP_ID,
    config_id: env[META_LOGIN_CONFIG_ENV[provider]],
    redirect_uri: env[redirectEnv],
    response_type: 'code',
    override_default_response_type: 'true',
    state
  });
  return `${META_AUTH_URL}?${params.toString()}`;
}

function parseRetryAfter(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds);
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return null;
  return Math.max(0, Math.ceil((timestamp - Date.now()) / 1000));
}

function providerCode(body) {
  const error = body && typeof body === 'object' ? body.error : null;
  if (!error) return null;
  if (typeof error === 'string') return error;
  if (error.code !== undefined) {
    return error.error_subcode === undefined ? String(error.code) : `${error.code}:${error.error_subcode}`;
  }
  return error.type || null;
}

function categorizeMetaFailure(status, body) {
  const error = body && typeof body === 'object' && body.error && typeof body.error === 'object'
    ? body.error
    : {};
  const code = Number(error.code);
  const subcode = Number(error.error_subcode);
  const normalized = String(providerCode(body) || '').toLowerCase();
  if (code === 190 || status === 401) {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: providerCode(body) };
  }
  if (code === 10 || code === 200 || normalized.includes('permission')) {
    return { category: 'scope', retryable: false, terminal: false, provider_code: providerCode(body) };
  }
  if ([4, 17, 32, 613].includes(code) || status === 429) {
    return { category: 'rate_limit', retryable: true, terminal: false, provider_code: providerCode(body) };
  }
  if (subcode === 458 || subcode === 459 || subcode === 460 || subcode === 463 || subcode === 467) {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: providerCode(body) };
  }
  if (status >= 500 || code === 1 || code === 2) {
    return { category: 'provider', retryable: true, terminal: false, provider_code: providerCode(body) };
  }
  if (status >= 400) {
    return { category: 'provider', retryable: false, terminal: false, provider_code: providerCode(body) };
  }
  return { category: 'malformed_response', retryable: false, terminal: false, provider_code: providerCode(body) };
}

function parseUsageHeader(value) {
  if (!value) return null;
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function numericUsageValues(value, output = []) {
  if (typeof value === 'number' && Number.isFinite(value)) output.push(value);
  else if (Array.isArray(value)) value.forEach(item => numericUsageValues(item, output));
  else if (value && typeof value === 'object') Object.values(value).forEach(item => numericUsageValues(item, output));
  return output;
}

function usageFromHeaders(headers) {
  const get = headers && typeof headers.get === 'function' ? name => headers.get(name) : () => null;
  const usage = {
    app: parseUsageHeader(get('x-app-usage')),
    page: parseUsageHeader(get('x-page-usage')),
    business: parseUsageHeader(get('x-business-use-case-usage'))
  };
  const values = numericUsageValues(usage);
  return { ...usage, maximum: values.length > 0 ? Math.max(...values) : null };
}

async function readJson(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    const error = new Error('meta_malformed_response');
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
    const normalized = new Error(error && error.name === 'AbortError' ? 'meta_request_timeout' : 'meta_network_error');
    normalized.category = error && error.name === 'AbortError' ? 'timeout' : 'network';
    normalized.retryable = true;
    throw normalized;
  } finally {
    clearTimeout(timeout);
  }
}

async function callMeta(url, options = {}, requestOptions = {}) {
  const env = requestOptions.env || process.env;
  const limits = getMetaLimits(env);
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
        usage: { maximum: null },
        budgetExhausted: true,
        error: { category: 'timeout', retryable: true, terminal: false, provider_code: 'meta_time_budget_exhausted' }
      };
    }
    try {
      const timeoutMs = remainingMs === null
        ? limits.requestTimeoutMs
        : Math.max(1, Math.min(limits.requestTimeoutMs, remainingMs));
      const response = await fetchWithTimeout(url, options, timeoutMs);
      const body = await readJson(response);
      const usage = usageFromHeaders(response.headers);
      if (response.ok) {
        return { ok: true, status: response.status, body, attempts: attempt, retryAfterSeconds: null, usage };
      }
      const retryAfterSeconds = parseRetryAfter(response.headers && response.headers.get('retry-after'));
      lastResult = {
        ok: false,
        status: response.status,
        body,
        attempts: attempt,
        retryAfterSeconds,
        usage,
        error: categorizeMetaFailure(response.status, body)
      };
    } catch (error) {
      lastResult = {
        ok: false,
        status: 0,
        body: null,
        attempts: attempt,
        retryAfterSeconds: null,
        usage: { maximum: null },
        error: {
          category: error.category || 'network',
          retryable: error.retryable !== false,
          terminal: false,
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

function graphUrl(path, params = {}) {
  const normalized = String(path || '').replace(/^\/+/, '');
  const url = new URL(`${META_GRAPH_URL}/${normalized}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== '') url.searchParams.set(key, String(value));
  }
  return url.toString();
}

function appSecretProof(accessToken, env = process.env) {
  return crypto.createHmac('sha256', env.META_APP_SECRET).update(String(accessToken)).digest('hex');
}

function authorizedRequest(path, accessToken, params = {}, requestOptions = {}, method = 'GET') {
  const env = requestOptions.env || process.env;
  return callMeta(graphUrl(path, {
    ...params,
    appsecret_proof: appSecretProof(accessToken, env)
  }), {
    method,
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: 'application/json'
    }
  }, requestOptions);
}

async function exchangeCode(provider, code, env = process.env) {
  const redirectUri = provider === 'facebook_pages' ? env.FACEBOOK_REDIRECT_URI : env.INSTAGRAM_REDIRECT_URI;
  return callMeta(graphUrl('oauth/access_token', {
    client_id: env.META_APP_ID,
    client_secret: env.META_APP_SECRET,
    redirect_uri: redirectUri,
    code
  }), {}, { env, maxRetries: 0 });
}

async function exchangeLongLivedToken(accessToken, requestOptions = {}) {
  const env = requestOptions.env || process.env;
  return callMeta(graphUrl('oauth/access_token', {
    grant_type: 'fb_exchange_token',
    client_id: env.META_APP_ID,
    client_secret: env.META_APP_SECRET,
    fb_exchange_token: accessToken
  }), {}, { ...requestOptions, env, maxRetries: 0 });
}

async function debugToken(accessToken, requestOptions = {}) {
  const env = requestOptions.env || process.env;
  return callMeta(graphUrl('debug_token', {
    input_token: accessToken,
    access_token: `${env.META_APP_ID}|${env.META_APP_SECRET}`
  }), {}, { ...requestOptions, env, maxRetries: 0 });
}

function getPermissions(accessToken, requestOptions = {}) {
  return authorizedRequest('me/permissions', accessToken, {}, { ...requestOptions, maxRetries: 0 });
}

function getUser(accessToken, requestOptions = {}) {
  return authorizedRequest('me', accessToken, { fields: 'id,name' }, { ...requestOptions, maxRetries: 0 });
}

function listManagedPages(accessToken, after = null, requestOptions = {}) {
  return authorizedRequest('me/accounts', accessToken, {
    fields: 'id,name,tasks,access_token,picture{url},instagram_business_account{id,username,name,profile_picture_url,followers_count,media_count}',
    limit: 100,
    after
  }, { ...requestOptions, maxRetries: 0 });
}

function getPageProfile(pageId, pageToken, requestOptions = {}) {
  return authorizedRequest(pageId, pageToken, { fields: 'id,name,picture{url}' }, requestOptions);
}

function getPageInsights(pageId, pageToken, metrics, since, until, requestOptions = {}) {
  return authorizedRequest(`${pageId}/insights`, pageToken, {
    metric: scopeValues(metrics).join(','),
    period: 'day',
    since,
    until
  }, requestOptions);
}

function listPagePosts(pageId, pageToken, after = null, requestOptions = {}) {
  return authorizedRequest(`${pageId}/posts`, pageToken, {
    fields: 'id,message,created_time,permalink_url,full_picture,attachments{media_type},shares',
    limit: 50,
    after
  }, requestOptions);
}

function getPostInsights(postId, pageToken, requestOptions = {}) {
  return authorizedRequest(`${postId}/insights`, pageToken, {
    metric: FACEBOOK_POST_INSIGHT_METRICS.join(',')
  }, requestOptions);
}

function getInstagramProfile(accountId, pageToken, requestOptions = {}) {
  return authorizedRequest(accountId, pageToken, {
    fields: 'id,username,name,profile_picture_url,followers_count,media_count'
  }, requestOptions);
}

function getInstagramInsights(accountId, pageToken, metrics, since, until, requestOptions = {}) {
  return authorizedRequest(`${accountId}/insights`, pageToken, {
    metric: scopeValues(metrics).join(','),
    period: 'day',
    metric_type: 'total_value',
    since,
    until
  }, requestOptions);
}

function listInstagramMedia(accountId, pageToken, after = null, requestOptions = {}) {
  return authorizedRequest(`${accountId}/media`, pageToken, {
    fields: 'id,caption,media_type,media_product_type,permalink,thumbnail_url,media_url,timestamp,like_count,comments_count',
    limit: 50,
    after
  }, requestOptions);
}

function instagramMetricsForMedia(media) {
  const product = String(media && (media.media_product_type || media.mediaProductType) || '').toUpperCase();
  if (product === 'STORY') return [];
  if (product === 'REELS') return [...INSTAGRAM_MEDIA_INSIGHT_METRICS.REELS];
  const type = String(media && (media.media_type || media.mediaType) || '').toUpperCase();
  return [...(INSTAGRAM_MEDIA_INSIGHT_METRICS[type] || [])];
}

function getInstagramMediaInsights(mediaId, pageToken, metrics, requestOptions = {}) {
  const selected = scopeValues(metrics);
  if (selected.length === 0) throw new Error('instagram_media_insights_unsupported');
  return authorizedRequest(`${mediaId}/insights`, pageToken, { metric: selected.join(',') }, requestOptions);
}

async function revokePermissions(accessToken, requestOptions = {}) {
  const result = await authorizedRequest('me/permissions', accessToken, {}, {
    ...requestOptions,
    maxRetries: 0
  }, 'DELETE');
  return {
    attempted: true,
    success: result.ok && result.body && result.body.success !== false,
    status: result.status,
    error: result.ok ? null : result.error
  };
}

function decodeBase64Url(value) {
  const normalized = String(value || '').replace(/-/g, '+').replace(/_/g, '/');
  const padding = normalized.length % 4 === 0 ? '' : '='.repeat(4 - (normalized.length % 4));
  return Buffer.from(normalized + padding, 'base64');
}

function verifySignedRequest(signedRequest, env = process.env, options = {}) {
  const parts = String(signedRequest || '').split('.');
  if (parts.length !== 2 || !parts[0] || !parts[1]) throw new Error('meta_signed_request_invalid');
  const provided = decodeBase64Url(parts[0]);
  const expected = crypto.createHmac('sha256', env.META_APP_SECRET).update(parts[1]).digest();
  if (provided.length !== expected.length || !crypto.timingSafeEqual(provided, expected)) {
    throw new Error('meta_signed_request_signature_invalid');
  }
  let payload;
  try {
    payload = JSON.parse(decodeBase64Url(parts[1]).toString('utf8'));
  } catch {
    throw new Error('meta_signed_request_payload_invalid');
  }
  if (String(payload.algorithm || '').toUpperCase() !== 'HMAC-SHA256') {
    throw new Error('meta_signed_request_algorithm_invalid');
  }
  if (!payload.user_id) throw new Error('meta_signed_request_subject_missing');
  const nowSeconds = options.nowSeconds || Math.floor(Date.now() / 1000);
  const maxAgeSeconds = options.maxAgeSeconds || 60 * 60;
  const issuedAt = Number(payload.issued_at);
  if (!Number.isFinite(issuedAt) || issuedAt > nowSeconds + 300 || nowSeconds - issuedAt > maxAgeSeconds) {
    throw new Error('meta_signed_request_expired');
  }
  if (payload.expires && Number(payload.expires) < nowSeconds) throw new Error('meta_signed_request_expired');
  return payload;
}

module.exports = {
  FACEBOOK_PAGE_INSIGHT_METRICS,
  FACEBOOK_POST_INSIGHT_METRICS,
  INSTAGRAM_ACCOUNT_INSIGHT_METRICS,
  INSTAGRAM_MEDIA_INSIGHT_METRICS,
  META_AUTH_URL,
  META_AUTOMATIC_SCOPES,
  META_FORBIDDEN_SCOPE_TERMS,
  META_GRAPH_URL,
  appSecretProof,
  buildAuthorizationUrl,
  callMeta,
  categorizeMetaFailure,
  debugToken,
  exchangeCode,
  exchangeLongLivedToken,
  forbiddenScopes,
  getInstagramInsights,
  getInstagramMediaInsights,
  getInstagramProfile,
  getPageInsights,
  getPageProfile,
  getPermissions,
  getPostInsights,
  getUser,
  hasExactProductScopes,
  instagramMetricsForMedia,
  listInstagramMedia,
  listManagedPages,
  listPagePosts,
  parseRetryAfter,
  revokePermissions,
  scopeValues,
  setMetaTestHooks,
  usageFromHeaders,
  verifySignedRequest
};
