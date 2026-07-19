const crypto = require('crypto');
let fetchImpl = require('node-fetch');
let sleepImpl = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
let randomImpl = Math.random;

const { getGoogleAnalyticsLimits } = require('../platform/google-analytics-config');

const GOOGLE_AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_REVOKE_URL = 'https://oauth2.googleapis.com/revoke';
const GA4_ADMIN_API_URL = 'https://analyticsadmin.googleapis.com/v1beta';
const GA4_DATA_API_URL = 'https://analyticsdata.googleapis.com/v1beta';
const GA4_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly'];

const GA4_METRICS = Object.freeze([
  'activeUsers',
  'newUsers',
  'sessions',
  'screenPageViews',
  'engagementRate',
  'bounceRate',
  'averageSessionDuration',
  'sessionsPerUser',
  'screenPageViewsPerUser'
]);

const GA4_BREAKDOWNS = Object.freeze([
  Object.freeze({
    key: 'ga4.session_source_medium',
    dimensions: Object.freeze(['sessionSource', 'sessionMedium']),
    metrics: Object.freeze(['sessions', 'activeUsers', 'engagementRate'])
  }),
  Object.freeze({
    key: 'ga4.page_path_title',
    dimensions: Object.freeze(['pagePath', 'pageTitle']),
    metrics: Object.freeze(['screenPageViews', 'activeUsers'])
  }),
  Object.freeze({
    key: 'ga4.landing_page',
    dimensions: Object.freeze(['landingPagePlusQueryString']),
    metrics: Object.freeze(['sessions', 'activeUsers', 'engagementRate', 'bounceRate'])
  }),
  Object.freeze({
    key: 'ga4.device_category',
    dimensions: Object.freeze(['deviceCategory']),
    metrics: Object.freeze(['sessions', 'activeUsers', 'screenPageViews'])
  }),
  Object.freeze({
    key: 'ga4.country',
    dimensions: Object.freeze(['country']),
    metrics: Object.freeze(['sessions', 'activeUsers'])
  }),
  Object.freeze({
    key: 'ga4.city',
    dimensions: Object.freeze(['city']),
    metrics: Object.freeze(['sessions', 'activeUsers'])
  })
]);

function setGoogleAnalyticsTestHooks(hooks = {}) {
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
    client_id: env.GA4_CLIENT_ID,
    redirect_uri: env.GA4_REDIRECT_URI,
    response_type: 'code',
    scope: GA4_SCOPES.join(' '),
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

function providerErrorDetails(body) {
  if (!body || typeof body !== 'object') return { code: null, reason: null };
  if (typeof body.error === 'string') return { code: body.error, reason: body.error };
  const error = body.error && typeof body.error === 'object' ? body.error : {};
  const details = Array.isArray(error.details) ? error.details : [];
  const errorInfo = details.find(detail => detail && detail.reason);
  return {
    code: error.status || error.code || error.message || null,
    reason: (errorInfo && errorInfo.reason) || error.status || error.message || null
  };
}

function categorizeProviderFailure(status, body) {
  const details = providerErrorDetails(body);
  const normalized = `${details.code || ''} ${details.reason || ''}`.toLowerCase();
  if (normalized.includes('invalid_grant')) {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: details.reason || details.code };
  }
  if (normalized.includes('access_denied')) {
    return { category: 'authentication', retryable: false, denied: true, provider_code: details.reason || details.code };
  }
  if (
    normalized.includes('insufficient') ||
    normalized.includes('permission_denied') ||
    normalized.includes('insufficient authentication scopes')
  ) {
    return { category: 'scope', retryable: false, provider_code: details.reason || details.code };
  }
  if (
    normalized.includes('quota') ||
    normalized.includes('resource_exhausted') ||
    normalized.includes('dailylimit')
  ) {
    return { category: 'quota', retryable: status === 429, provider_code: details.reason || details.code };
  }
  if (status === 429 || normalized.includes('rate')) {
    return { category: 'rate_limit', retryable: true, provider_code: details.reason || details.code };
  }
  if (status === 401) {
    return { category: 'authentication', retryable: false, terminal: true, provider_code: details.reason || details.code };
  }
  if (status === 403) {
    return { category: 'authentication', retryable: false, provider_code: details.reason || details.code };
  }
  if (status >= 500) {
    return { category: 'provider', retryable: true, provider_code: details.reason || details.code };
  }
  if (status >= 400) {
    return { category: 'provider', retryable: false, provider_code: details.reason || details.code };
  }
  return { category: 'malformed_response', retryable: false, provider_code: details.reason || details.code };
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
    const wrapped = new Error(error && error.name === 'AbortError' ? 'ga4_request_timeout' : 'ga4_network_error');
    wrapped.category = error && error.name === 'AbortError' ? 'timeout' : 'network';
    wrapped.retryable = true;
    throw wrapped;
  } finally {
    clearTimeout(timeout);
  }
}

async function callGoogle(url, options = {}, requestOptions = {}) {
  const limits = getGoogleAnalyticsLimits(requestOptions.env || process.env);
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
        error: { category: 'timeout', retryable: true, provider_code: 'ga4_time_budget_exhausted' }
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
      lastResult = {
        ok: false,
        status: response.status,
        body,
        attempts: attempt,
        retryAfterSeconds,
        error: categorizeProviderFailure(response.status, body)
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
    const delayMs = lastResult.retryAfterSeconds === null
      ? Math.min(5000, 250 * (2 ** (attempt - 1)) + Math.floor(randomImpl() * 250))
      : Math.min(30000, lastResult.retryAfterSeconds * 1000);
    if (requestOptions.deadlineMs && Date.now() + delayMs >= requestOptions.deadlineMs) {
      return { ...lastResult, budgetExhausted: true };
    }
    await sleepImpl(delayMs);
  }
  return lastResult;
}

function formHeaders() {
  return { 'Content-Type': 'application/x-www-form-urlencoded' };
}

async function exchangeCode(code, codeVerifier, env = process.env) {
  const body = new URLSearchParams({
    client_id: env.GA4_CLIENT_ID,
    client_secret: env.GA4_CLIENT_SECRET,
    code,
    code_verifier: codeVerifier,
    grant_type: 'authorization_code',
    redirect_uri: env.GA4_REDIRECT_URI
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
    client_id: env.GA4_CLIENT_ID,
    client_secret: env.GA4_CLIENT_SECRET,
    refresh_token: refreshToken,
    grant_type: 'refresh_token'
  });
  return callGoogle(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: formHeaders(),
    body: body.toString()
  }, { ...requestOptions, env, maxRetries: Math.min(1, getGoogleAnalyticsLimits(env).maxRetries) });
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

function authorizationHeaders(accessToken, json = false) {
  return {
    Authorization: `Bearer ${accessToken}`,
    Accept: 'application/json',
    ...(json ? { 'Content-Type': 'application/json' } : {})
  };
}

function authorizedGet(url, accessToken, requestOptions) {
  return callGoogle(url, { headers: authorizationHeaders(accessToken) }, requestOptions);
}

function authorizedPost(url, accessToken, body, requestOptions) {
  return callGoogle(url, {
    method: 'POST',
    headers: authorizationHeaders(accessToken, true),
    body: JSON.stringify(body)
  }, requestOptions);
}

async function listAccountSummaries(accessToken, pageToken = null, requestOptions = {}) {
  const params = new URLSearchParams({ pageSize: '200' });
  if (pageToken) params.set('pageToken', String(pageToken));
  return authorizedGet(`${GA4_ADMIN_API_URL}/accountSummaries?${params.toString()}`, accessToken, requestOptions);
}

async function getProperty(accessToken, propertyName, requestOptions = {}) {
  if (!/^properties\/\d+$/.test(String(propertyName || ''))) throw new Error('ga4_property_name_invalid');
  return authorizedGet(`${GA4_ADMIN_API_URL}/${propertyName}`, accessToken, requestOptions);
}

function propertyPath(propertyName, suffix) {
  if (!/^properties\/\d+$/.test(String(propertyName || ''))) throw new Error('ga4_property_name_invalid');
  return `${GA4_DATA_API_URL}/${propertyName}${suffix}`;
}

async function getMetadata(accessToken, propertyName, requestOptions = {}) {
  return authorizedGet(propertyPath(propertyName, '/metadata'), accessToken, requestOptions);
}

async function checkCompatibility(accessToken, propertyName, dimensions, metrics, requestOptions = {}) {
  return authorizedPost(propertyPath(propertyName, ':checkCompatibility'), accessToken, {
    dimensions: (dimensions || []).map(name => ({ name })),
    metrics: (metrics || []).map(name => ({ name }))
  }, requestOptions);
}

async function runReport(accessToken, propertyName, report, requestOptions = {}) {
  const body = {
    dateRanges: (report.dateRanges || []).map(range => ({ startDate: range.startDate, endDate: range.endDate })),
    dimensions: (report.dimensions || []).map(name => ({ name })),
    metrics: (report.metrics || []).map(name => ({ name })),
    limit: String(report.limit || 100),
    offset: String(report.offset || 0),
    returnPropertyQuota: true,
    keepEmptyRows: false
  };
  if (Array.isArray(report.orderBys) && report.orderBys.length > 0) body.orderBys = report.orderBys;
  return authorizedPost(propertyPath(propertyName, ':runReport'), accessToken, body, requestOptions);
}

function grantedScopes(scopeValue) {
  const values = Array.isArray(scopeValue) ? scopeValue.map(String) : String(scopeValue || '').split(/[\s,]+/);
  return new Set(values.filter(Boolean));
}

function missingScopes(scopeValue) {
  const granted = grantedScopes(scopeValue);
  return GA4_SCOPES.filter(scope => !granted.has(scope));
}

function hasExactScopes(scopeValue) {
  const granted = grantedScopes(scopeValue);
  return granted.size === GA4_SCOPES.length && GA4_SCOPES.every(scope => granted.has(scope));
}

function chooseRefreshToken(responseToken, existingToken = null) {
  const rotated = typeof responseToken === 'string' ? responseToken.trim() : '';
  if (rotated) return rotated;
  const existing = typeof existingToken === 'string' ? existingToken.trim() : '';
  return existing || null;
}

module.exports = {
  GA4_ADMIN_API_URL,
  GA4_BREAKDOWNS,
  GA4_DATA_API_URL,
  GA4_METRICS,
  GA4_SCOPES,
  GOOGLE_AUTH_URL,
  GOOGLE_REVOKE_URL,
  GOOGLE_TOKEN_URL,
  buildAuthorizationUrl,
  categorizeProviderFailure,
  checkCompatibility,
  chooseRefreshToken,
  createPkcePair,
  exchangeCode,
  getMetadata,
  getProperty,
  grantedScopes,
  hasExactScopes,
  listAccountSummaries,
  missingScopes,
  parseRetryAfter,
  refreshAccessToken,
  revokeToken,
  runReport,
  setGoogleAnalyticsTestHooks
};
