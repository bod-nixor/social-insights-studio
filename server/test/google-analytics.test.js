const assert = require('node:assert/strict');
const test = require('node:test');

const ga4 = require('../integrations/google-analytics');
const {
  GA4_CALLBACK_PATH,
  getGoogleAnalyticsConfiguration,
  getGoogleAnalyticsLimits,
  getGoogleAnalyticsProductionErrors,
  validateRedirectUri
} = require('../platform/google-analytics-config');
const { normalizeProperty } = require('../platform/google-analytics-connection-service');
const {
  buildDateWindows,
  compatibilitySet,
  normalizeGa4Date,
  normalizeMetadata,
  parseReportRows,
  quotaSummary
} = require('../platform/google-analytics-sync-service');

const encryptionKey = Buffer.alloc(32, 8).toString('base64');
const testEnv = {
  NODE_ENV: 'test',
  BASE_URL: 'http://localhost:3001',
  FEATURE_GA4_CONNECTOR: 'true',
  GA4_CLIENT_ID: 'ga4-client.apps.googleusercontent.com',
  GA4_CLIENT_SECRET: 'ga4-client-secret',
  GA4_REDIRECT_URI: 'http://localhost:3001/api/integrations/google-analytics/callback',
  GOOGLE_OIDC_CLIENT_ID: 'sign-in-client.apps.googleusercontent.com',
  YOUTUBE_CLIENT_ID: 'youtube-client.apps.googleusercontent.com',
  ENCRYPTION_KEY: encryptionKey,
  ENCRYPTION_KEY_VERSION: 'test-v1'
};

function response(status, body, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: name => headers[String(name).toLowerCase()] || null },
    text: async () => body === null ? '' : JSON.stringify(body)
  };
}

test.afterEach(() => ga4.setGoogleAnalyticsTestHooks());

test('GA4 configuration is disabled by default and requires a dedicated exact callback', () => {
  assert.equal(getGoogleAnalyticsConfiguration({}).status, 'disabled');
  const ready = getGoogleAnalyticsConfiguration(testEnv, {
    databaseReady: true,
    foundationReady: true,
    workerReady: true
  });
  assert.equal(ready.connectable, true);
  assert.equal(ready.redirectUri, testEnv.GA4_REDIRECT_URI);
  assert.equal(GA4_CALLBACK_PATH, '/api/integrations/google-analytics/callback');

  assert.equal(validateRedirectUri({ ...testEnv, GA4_REDIRECT_URI: `${testEnv.GA4_REDIRECT_URI}?unsafe=1` }).ready, false);
  assert.equal(getGoogleAnalyticsConfiguration({
    ...testEnv,
    GA4_CLIENT_ID: testEnv.YOUTUBE_CLIENT_ID
  }).warnings.includes('GA4_CLIENT_ID_must_differ_from_youtube'), true);
  assert.equal(getGoogleAnalyticsConfiguration({
    ...testEnv,
    GA4_CLIENT_ID: testEnv.GOOGLE_OIDC_CLIENT_ID
  }).warnings.includes('GA4_CLIENT_ID_must_differ_from_sign_in'), true);
});

test('GA4 production validation fails closed on placeholders only when enabled', () => {
  assert.deepEqual(getGoogleAnalyticsProductionErrors({ FEATURE_GA4_CONNECTOR: '0' }), []);
  const errors = getGoogleAnalyticsProductionErrors({
    ...testEnv,
    NODE_ENV: 'production',
    BASE_URL: 'https://example.com',
    GA4_REDIRECT_URI: 'https://example.com/api/integrations/google-analytics/callback',
    GA4_CLIENT_ID: 'your_ga4_client',
    GA4_CLIENT_SECRET: 'replace_with_secret'
  });
  assert.equal(errors.some(value => value.includes('GA4_CLIENT_ID_placeholder')), true);
  assert.equal(errors.some(value => value.includes('GA4_CLIENT_SECRET_placeholder')), true);
});

test('GA4 limits remain bounded', () => {
  const limits = getGoogleAnalyticsLimits({
    GA4_REQUEST_TIMEOUT_MS: '999999',
    GA4_SYNC_MAX_DIMENSION_ROWS: '9999',
    GA4_DISCOVERY_MAX_PROPERTIES: '0',
    GA4_ANALYTICS_LOOKBACK_DAYS: '366'
  });
  assert.equal(limits.requestTimeoutMs, 10000);
  assert.equal(limits.maxDimensionRows, 100);
  assert.equal(limits.maxProperties, 100);
  assert.equal(limits.analyticsLookbackDays, 366);
});

test('GA4 authorization URL binds PKCE, offline access, and only analytics.readonly', () => {
  const url = new URL(ga4.buildAuthorizationUrl({
    state: 'state-value',
    codeChallenge: 'challenge-value',
    promptConsent: true
  }, testEnv));
  assert.equal(url.origin + url.pathname, ga4.GOOGLE_AUTH_URL);
  assert.equal(url.searchParams.get('client_id'), testEnv.GA4_CLIENT_ID);
  assert.equal(url.searchParams.get('redirect_uri'), testEnv.GA4_REDIRECT_URI);
  assert.equal(url.searchParams.get('scope'), 'https://www.googleapis.com/auth/analytics.readonly');
  assert.equal(url.searchParams.get('access_type'), 'offline');
  assert.equal(url.searchParams.get('include_granted_scopes'), 'true');
  assert.equal(url.searchParams.get('code_challenge_method'), 'S256');
  assert.equal(url.searchParams.get('prompt'), 'consent');
});

test('GA4 scope inspection requires the exact one-scope product grant', () => {
  assert.equal(ga4.hasExactScopes(ga4.GA4_SCOPES), true);
  assert.equal(ga4.hasExactScopes('https://www.googleapis.com/auth/analytics.readonly'), true);
  assert.equal(ga4.hasExactScopes('openid https://www.googleapis.com/auth/analytics.readonly'), false);
  assert.deepEqual(ga4.missingScopes('openid'), ga4.GA4_SCOPES);
  assert.equal(ga4.chooseRefreshToken('', 'existing-refresh'), 'existing-refresh');
  assert.equal(ga4.chooseRefreshToken('rotated-refresh', 'existing-refresh'), 'rotated-refresh');
});

test('GA4 Admin discovery and Data API request builders are read only and property bound', async () => {
  const calls = [];
  ga4.setGoogleAnalyticsTestHooks({
    fetch: async (url, options = {}) => {
      calls.push({ url: String(url), options });
      return response(200, String(url).includes(':runReport')
        ? { dimensionHeaders: [], metricHeaders: [{ name: 'sessions' }], rows: [{ metricValues: [{ value: '4' }] }] }
        : {});
    },
    sleep: async () => {},
    random: () => 0
  });
  await ga4.listAccountSummaries('access-token', 'next', { env: testEnv, maxRetries: 0 });
  await ga4.getProperty('access-token', 'properties/123', { env: testEnv, maxRetries: 0 });
  await ga4.getMetadata('access-token', 'properties/123', { env: testEnv, maxRetries: 0 });
  await ga4.checkCompatibility('access-token', 'properties/123', ['date'], ['sessions'], { env: testEnv, maxRetries: 0 });
  await ga4.runReport('access-token', 'properties/123', {
    dateRanges: [{ startDate: '2026-07-01', endDate: '2026-07-07' }],
    dimensions: ['date'],
    metrics: ['sessions'],
    limit: 10
  }, { env: testEnv, maxRetries: 0 });
  assert.equal(calls.length, 5);
  assert.match(calls[0].url, /analyticsadmin\.googleapis\.com\/v1beta\/accountSummaries/);
  assert.match(calls[1].url, /analyticsadmin\.googleapis\.com\/v1beta\/properties\/123$/);
  assert.match(calls[2].url, /analyticsdata\.googleapis\.com\/v1beta\/properties\/123\/metadata$/);
  assert.match(calls[3].url, /properties\/123:checkCompatibility$/);
  assert.match(calls[4].url, /properties\/123:runReport$/);
  assert.equal(calls.every(call => call.options.headers.Authorization === 'Bearer access-token'), true);
  const reportBody = JSON.parse(calls[4].options.body);
  assert.deepEqual(reportBody.metrics, [{ name: 'sessions' }]);
  assert.equal(reportBody.returnPropertyQuota, true);
  assert.equal(Object.hasOwn(reportBody, 'userId'), false);
  await assert.rejects(ga4.getProperty('token', '../properties/123'), /ga4_property_name_invalid/);
});

test('GA4 retries bounded provider failures and honors Retry-After', async () => {
  let attempts = 0;
  const delays = [];
  ga4.setGoogleAnalyticsTestHooks({
    fetch: async () => {
      attempts += 1;
      return attempts === 1
        ? response(429, { error: { status: 'RESOURCE_EXHAUSTED' } }, { 'retry-after': '2' })
        : response(200, { accountSummaries: [] });
    },
    sleep: async milliseconds => delays.push(milliseconds),
    random: () => 0
  });
  const result = await ga4.listAccountSummaries('token', null, { env: testEnv, maxRetries: 1 });
  assert.equal(result.ok, true);
  assert.equal(result.attempts, 2);
  assert.deepEqual(delays, [2000]);
});

test('GA4 failures distinguish terminal auth, scope, quota, rate limit, and transient service errors', () => {
  assert.deepEqual(ga4.categorizeProviderFailure(400, { error: 'invalid_grant' }).category, 'authentication');
  assert.equal(ga4.categorizeProviderFailure(400, { error: 'invalid_grant' }).terminal, true);
  assert.equal(ga4.categorizeProviderFailure(403, { error: { status: 'PERMISSION_DENIED' } }).category, 'scope');
  assert.equal(ga4.categorizeProviderFailure(429, { error: { status: 'RESOURCE_EXHAUSTED' } }).category, 'quota');
  assert.equal(ga4.categorizeProviderFailure(429, { error: { status: 'RESOURCE_EXHAUSTED' } }).retryable, true);
  assert.equal(ga4.categorizeProviderFailure(503, {}).category, 'provider');
});

test('GA4 property normalization requires timezone and currency before selection', () => {
  const normalized = normalizeProperty(
    { account: 'accounts/5', displayName: 'Example account' },
    { property: 'properties/123', displayName: 'Summary name', propertyType: 'PROPERTY_TYPE_ORDINARY' },
    {
      name: 'properties/123',
      account: 'accounts/5',
      displayName: 'Website',
      timeZone: 'Asia/Karachi',
      currencyCode: 'PKR',
      propertyType: 'PROPERTY_TYPE_ORDINARY',
      serviceLevel: 'GOOGLE_ANALYTICS_STANDARD'
    }
  );
  assert.equal(normalized.selectable, true);
  assert.equal(normalized.propertyId, '123');
  assert.equal(normalized.timezone, 'Asia/Karachi');
  assert.equal(normalized.currency, 'PKR');
  const incomplete = normalizeProperty(
    { account: 'accounts/5', displayName: 'Example account' },
    { property: 'properties/124', displayName: 'Incomplete' }
  );
  assert.equal(incomplete.selectable, false);
});

test('GA4 summary report parsing accepts omitted dimension headers and preserves provider zeroes', () => {
  const parsed = parseReportRows({
    metricHeaders: [{ name: 'sessions' }, { name: 'engagementRate' }],
    rows: [{
      metricValues: [{ value: '0' }, { value: '0.5' }]
    }],
    rowCount: 1,
    metadata: { subjectToThresholding: true }
  }, {
    dimensions: [],
    metrics: ['sessions', 'engagementRate']
  });
  assert.deepEqual(parsed.dimensions, []);
  assert.equal(parsed.rows[0].metrics.sessions, '0');
  assert.equal(parsed.metadata.subjectToThresholding, true);
});

test('GA4 summary report parsing accepts an explicitly empty dimension header array', () => {
  const parsed = parseReportRows({
    dimensionHeaders: [],
    metricHeaders: [{ name: 'sessions' }],
    rows: [{ dimensionValues: [], metricValues: [{ value: '4' }] }]
  }, {
    dimensions: [],
    metrics: ['sessions']
  });
  assert.deepEqual(parsed.dimensions, []);
  assert.equal(parsed.rows[0].metrics.sessions, '4');
});

test('GA4 daily report parsing requires and maps the requested date dimension', () => {
  const parsed = parseReportRows({
    dimensionHeaders: [{ name: 'date' }],
    metricHeaders: [{ name: 'sessions' }],
    rows: [{
      dimensionValues: [{ value: '20260717' }],
      metricValues: [{ value: '3' }]
    }]
  }, {
    dimensions: ['date'],
    metrics: ['sessions']
  });
  assert.equal(parsed.rows[0].dimensions.date, '20260717');
  assert.equal(parsed.rows[0].metrics.sessions, '3');
});

test('GA4 report parsing treats omitted rows and rowCount zero as an empty result', () => {
  const parsed = parseReportRows({
    metricHeaders: [{ name: 'sessions' }],
    rowCount: 0
  }, {
    dimensions: [],
    metrics: ['sessions']
  });
  assert.deepEqual(parsed.rows, []);
  assert.equal(parsed.rowCount, 0);
});

test('GA4 report parsing accepts the known empty 200 envelope without inventing zeroes', () => {
  const expected = {
    dimensions: [],
    metrics: ['sessions', 'activeUsers']
  };
  const parsed = parseReportRows({
    metadata: {
      currencyCode: 'PKR',
      timeZone: 'Asia/Karachi'
    },
    propertyQuota: {
      tokensPerDay: {
        consumed: 1,
        remaining: 99999
      }
    },
    kind: 'analyticsData#runReport'
  }, expected);

  assert.deepEqual(parsed.dimensions, []);
  assert.deepEqual(parsed.metrics, ['sessions', 'activeUsers']);
  assert.deepEqual(parsed.rows, []);
  assert.equal(parsed.rowCount, 0);
  assert.equal(parsed.metadata.currencyCode, 'PKR');
  assert.equal(parsed.propertyQuota.tokensPerDay.consumed, 1);

  assert.throws(() => parseReportRows({
    metadata: {}
  }, expected), /ga4_report_response_malformed/);
});

test('GA4 report parsing rejects present non-array or missing required headers', () => {
  assert.throws(() => parseReportRows({
    dimensionHeaders: {},
    metricHeaders: [{ name: 'sessions' }]
  }, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [],
    metricHeaders: {}
  }, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({}, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [],
    metricHeaders: [{ name: 'sessions' }],
    rows: {}
  }, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [],
    metricHeaders: [{ name: 'sessions' }],
    rowCount: '0'
  }, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
});

test('GA4 report parsing rejects returned header count, name, and order mismatches', () => {
  assert.throws(() => parseReportRows({
    dimensionHeaders: [],
    metricHeaders: [{ name: 'activeUsers' }]
  }, {
    dimensions: [],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [{ name: 'country' }],
    metricHeaders: [{ name: 'sessions' }, { name: 'activeUsers' }]
  }, {
    dimensions: ['date'],
    metrics: ['activeUsers', 'sessions']
  }), /ga4_report_response_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [{ name: 'date' }],
    metricHeaders: [{ name: 'sessions' }, { name: 'activeUsers' }]
  }, {
    dimensions: ['date'],
    metrics: ['sessions']
  }), /ga4_report_response_malformed/);
});

test('GA4 report parsing rejects row dimension and metric cardinality mismatches', () => {
  assert.throws(() => parseReportRows({
    dimensionHeaders: [{ name: 'date' }],
    metricHeaders: [{ name: 'sessions' }],
    rows: [{ dimensionValues: [], metricValues: [{ value: '3' }] }]
  }, {
    dimensions: ['date'],
    metrics: ['sessions']
  }), /ga4_report_row_malformed/);
  assert.throws(() => parseReportRows({
    dimensionHeaders: [],
    metricHeaders: [{ name: 'sessions' }, { name: 'activeUsers' }],
    rows: [{ metricValues: [{ value: '3' }] }]
  }, {
    dimensions: [],
    metrics: ['sessions', 'activeUsers']
  }), /ga4_report_row_malformed/);
});

test('GA4 metadata and compatibility parsing preserve blocked and incompatible states', () => {
  const metadata = normalizeMetadata({
    dimensions: [{ apiName: 'date' }, { apiName: 'city' }],
    metrics: [{ apiName: 'sessions' }, { apiName: 'activeUsers', blockedReasons: ['NO_REVENUE_METRICS'] }]
  });
  assert.equal(metadata.dimensions.has('city'), true);
  assert.equal(metadata.metrics.get('activeUsers').blockedReasons[0], 'NO_REVENUE_METRICS');
  assert.deepEqual([...compatibilitySet({
    metricCompatibilities: [
      { metricMetadata: { apiName: 'sessions' }, compatibility: 'COMPATIBLE' },
      { metricMetadata: { apiName: 'activeUsers' }, compatibility: 'INCOMPATIBLE' }
    ]
  }, 'metric')], ['sessions']);
});

test('GA4 date windows use property-day boundaries and retain exact previous periods', () => {
  const windows = buildDateWindows('UTC', 180);
  assert.equal(windows.ranges.length, 6);
  for (const days of [7, 30, 90]) {
    const current = windows.ranges.find(value => value.days === days && value.kind === 'current');
    const previous = windows.ranges.find(value => value.days === days && value.kind === 'previous');
    assert.equal(new Date(`${current.endDate}T00:00:00Z`) - new Date(`${current.startDate}T00:00:00Z`), (days - 1) * 86400000);
    assert.equal(previous.endDate, new Date(new Date(`${current.startDate}T00:00:00Z`).getTime() - 86400000).toISOString().slice(0, 10));
  }
  assert.equal(normalizeGa4Date('20260718'), '2026-07-18');
  assert.equal(normalizeGa4Date('20260231'), null);
});

test('GA4 quota summaries retain counts without raw responses', () => {
  assert.deepEqual(quotaSummary({
    tokensPerDay: { consumed: 7, remaining: 99993 },
    concurrentRequests: { consumed: 1, remaining: 9 },
    unknownField: { raw: 'ignored' }
  }), {
    tokensPerDay: { consumed: 7, remaining: 99993 },
    concurrentRequests: { consumed: 1, remaining: 9 }
  });
  assert.equal(quotaSummary(null), null);
});
