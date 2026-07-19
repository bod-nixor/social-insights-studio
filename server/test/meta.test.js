const { afterEach, test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const meta = require('../integrations/meta');
const {
  META_CALLBACK_PATHS,
  META_REQUIRED_SCOPES,
  getMetaConfiguration,
  getMetaLimits,
  hasExactScopeAssertion,
  validateRedirectUri
} = require('../platform/meta-config');
const {
  normalizeDiscoveredResources
} = require('../platform/meta-connection-service');
const {
  FACEBOOK_POST_COUNT_AVAILABILITY,
  dailyInsightValues,
  latestInsightValue,
  normalizeFacebookPost,
  normalizeInstagramMedia,
  retryDelaySeconds
} = require('../platform/meta-sync-service');

const testEnv = {
  NODE_ENV: 'test',
  BASE_URL: 'http://localhost:3001',
  FEATURE_FACEBOOK_PAGES_CONNECTOR: 'true',
  FEATURE_INSTAGRAM_CONNECTOR: 'true',
  META_APP_ID: 'meta-app-id',
  META_APP_SECRET: 'meta-app-secret',
  META_FACEBOOK_LOGIN_CONFIG_ID: 'facebook-login-config-id',
  META_INSTAGRAM_LOGIN_CONFIG_ID: 'instagram-login-config-id',
  META_GRAPH_API_VERSION: 'v25.0',
  FACEBOOK_REDIRECT_URI: 'http://localhost:3001/api/integrations/facebook/callback',
  INSTAGRAM_REDIRECT_URI: 'http://localhost:3001/api/integrations/instagram/callback',
  META_FACEBOOK_APPROVED_SCOPES: 'pages_show_list,pages_read_engagement,read_insights',
  META_INSTAGRAM_APPROVED_SCOPES: 'instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement',
  ENCRYPTION_KEY: '5'.repeat(64),
  META_SYNC_MAX_RETRIES: '2'
};

function response(status, body, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: name => headers[String(name).toLowerCase()] || null },
    text: async () => (body === null ? '' : JSON.stringify(body))
  };
}

function signedRequest(payload, secret = testEnv.META_APP_SECRET) {
  const encodedPayload = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const signature = crypto.createHmac('sha256', secret).update(encodedPayload).digest('base64url');
  return `${signature}.${encodedPayload}`;
}

afterEach(() => {
  meta.setMetaTestHooks();
});

test('Meta configuration is disabled by default and fails closed unless exact assertions and callbacks exist', () => {
  assert.equal(getMetaConfiguration('facebook_pages', {}).status, 'disabled');
  assert.equal(getMetaConfiguration('facebook_pages', testEnv).connectable, true);
  assert.equal(getMetaConfiguration('instagram', testEnv).connectable, true);
  assert.equal(validateRedirectUri('facebook_pages', testEnv).ready, true);
  assert.equal(META_CALLBACK_PATHS.facebook_pages, '/api/integrations/facebook/callback');
  assert.equal(META_CALLBACK_PATHS.instagram, '/api/integrations/instagram/callback');
  assert.equal(hasExactScopeAssertion('facebook_pages', 'read_insights,pages_show_list,pages_read_engagement'), true);
  assert.equal(hasExactScopeAssertion('facebook_pages', 'pages_show_list,pages_read_engagement'), false);
  assert.equal(
    getMetaConfiguration('instagram', {
      ...testEnv,
      META_INSTAGRAM_APPROVED_SCOPES: `${testEnv.META_INSTAGRAM_APPROVED_SCOPES},ads_read`
    }).connectable,
    false
  );
  assert.equal(
    getMetaConfiguration('facebook_pages', {
      ...testEnv,
      META_INSTAGRAM_LOGIN_CONFIG_ID: testEnv.META_FACEBOOK_LOGIN_CONFIG_ID
    }).connectable,
    false
  );
  assert.equal(validateRedirectUri('instagram', {
    ...testEnv,
    INSTAGRAM_REDIRECT_URI: `${testEnv.INSTAGRAM_REDIRECT_URI}?unsafe=1`
  }).ready, false);
  assert.equal(getMetaLimits({ META_SYNC_MAX_CONTENT_ITEMS: '9999' }).maxContentItems, 100);
});

test('Facebook Login for Business URLs bind state, provider config, callback, and version', () => {
  const facebook = new URL(meta.buildAuthorizationUrl('facebook_pages', { state: 'facebook-state' }, testEnv));
  const instagram = new URL(meta.buildAuthorizationUrl('instagram', { state: 'instagram-state' }, testEnv));

  assert.equal(`${facebook.origin}${facebook.pathname}`, meta.META_AUTH_URL);
  assert.equal(facebook.searchParams.get('config_id'), testEnv.META_FACEBOOK_LOGIN_CONFIG_ID);
  assert.equal(instagram.searchParams.get('config_id'), testEnv.META_INSTAGRAM_LOGIN_CONFIG_ID);
  assert.equal(facebook.searchParams.get('state'), 'facebook-state');
  assert.equal(facebook.searchParams.get('redirect_uri'), testEnv.FACEBOOK_REDIRECT_URI);
  assert.equal(facebook.searchParams.get('override_default_response_type'), 'true');
  assert.equal(facebook.searchParams.has('scope'), false);
  assert.equal(instagram.searchParams.has('scope'), false);
  assert.equal(meta.META_GRAPH_URL.endsWith('/v25.0'), true);
  assert.deepEqual(meta.forbiddenScopes([
    ...META_REQUIRED_SCOPES.facebook_pages,
    'pages_manage_posts',
    'ads_read',
    'business_management'
  ]), ['pages_manage_posts', 'ads_read', 'business_management']);
});

test('scope validation accepts only the approved read-only Meta universe plus automatic public_profile', () => {
  assert.equal(meta.hasExactProductScopes('facebook_pages', META_REQUIRED_SCOPES.facebook_pages), true);
  assert.equal(meta.hasExactProductScopes('instagram', [
    ...META_REQUIRED_SCOPES.instagram,
    'public_profile'
  ]), true);
  assert.equal(meta.hasExactProductScopes('facebook_pages', [
    ...META_REQUIRED_SCOPES.facebook_pages,
    ...META_REQUIRED_SCOPES.instagram
  ]), true);
  assert.equal(meta.hasExactProductScopes('instagram', ['instagram_basic', 'pages_show_list']), false);
  assert.equal(meta.hasExactProductScopes('facebook_pages', [
    ...META_REQUIRED_SCOPES.facebook_pages,
    'pages_manage_posts'
  ]), false);
});

test('Graph calls use the pinned version, appsecret proof, bearer header, bounded retries, and usage headers', async () => {
  const calls = [];
  const sleeps = [];
  meta.setMetaTestHooks({
    fetch: async (url, options) => {
      calls.push({ url: new URL(String(url)), options });
      if (calls.length === 1) return response(429, { error: { code: 4 } }, { 'retry-after': '1' });
      return response(200, { id: 'page-1', name: 'Page' }, { 'x-app-usage': '{"call_count":81}' });
    },
    sleep: async milliseconds => sleeps.push(milliseconds),
    random: () => 0
  });

  const result = await meta.getPageProfile('page-1', 'page-token', { env: testEnv });
  assert.equal(result.ok, true);
  assert.equal(result.attempts, 2);
  assert.deepEqual(sleeps, [1000]);
  assert.equal(calls[0].url.pathname.startsWith('/v25.0/page-1'), true);
  assert.equal(calls[0].options.headers.Authorization, 'Bearer page-token');
  assert.equal(
    calls[0].url.searchParams.get('appsecret_proof'),
    crypto.createHmac('sha256', testEnv.META_APP_SECRET).update('page-token').digest('hex')
  );
  assert.equal(result.usage.maximum, 81);
});

test('Facebook Page post listing requests only narrow-scope fields', async () => {
  let requestUrl;
  meta.setMetaTestHooks({
    fetch: async url => {
      requestUrl = new URL(String(url));
      return response(200, { data: [] });
    }
  });

  const result = await meta.listPagePosts('page-1', 'page-token', null, { env: testEnv });

  assert.equal(result.ok, true);
  assert.equal(requestUrl.pathname, '/v25.0/page-1/posts');
  assert.equal(
    requestUrl.searchParams.get('fields'),
    'id,message,created_time,permalink_url,full_picture,attachments{media_type},shares'
  );
  assert.equal(requestUrl.searchParams.get('fields').includes('reactions'), false);
  assert.equal(requestUrl.searchParams.get('fields').includes('comments'), false);
});

test('Meta failures distinguish token, permission, throttling, invalid metric, and transient provider states', () => {
  assert.equal(meta.categorizeMetaFailure(400, { error: { code: 190 } }).category, 'authentication');
  assert.equal(meta.categorizeMetaFailure(403, { error: { code: 10 } }).category, 'scope');
  assert.equal(meta.categorizeMetaFailure(429, { error: { code: 4 } }).category, 'rate_limit');
  assert.equal(meta.categorizeMetaFailure(400, { error: { code: 100 } }).category, 'provider');
  assert.equal(meta.categorizeMetaFailure(503, { error: { code: 2 } }).retryable, true);
  assert.equal(retryDelaySeconds({ category: 'rate_limit', retryable: true }, 21600), 3600);
});

test('signed_request validation enforces HMAC-SHA256, freshness, subject, and tamper detection', () => {
  const nowSeconds = 1_800_000_000;
  const valid = signedRequest({
    algorithm: 'HMAC-SHA256',
    issued_at: nowSeconds - 30,
    user_id: 'app-scoped-user'
  });
  assert.equal(meta.verifySignedRequest(valid, testEnv, { nowSeconds }).user_id, 'app-scoped-user');
  assert.throws(
    () => meta.verifySignedRequest(`${valid.slice(0, -1)}x`, testEnv, { nowSeconds }),
    /signature_invalid|payload_invalid/
  );
  assert.throws(() => meta.verifySignedRequest(signedRequest({
    algorithm: 'HMAC-SHA256', issued_at: nowSeconds - 7200, user_id: 'old'
  }), testEnv, { nowSeconds }), /expired/);
  assert.throws(() => meta.verifySignedRequest(signedRequest({
    algorithm: 'HMAC-SHA1', issued_at: nowSeconds, user_id: 'wrong-algorithm'
  }), testEnv, { nowSeconds }), /algorithm_invalid/);
});

test('resource and content normalization requires Page ANALYZE and excludes unsupported Story insight history', () => {
  const pages = [{
    id: 'page-1',
    name: 'Page One',
    tasks: ['ANALYZE'],
    access_token: 'page-token',
    picture: { data: { url: 'https://img.example/page.jpg' } },
    instagram_business_account: {
      id: 'ig-1', username: 'studio', name: 'Studio', followers_count: 20, media_count: 3
    }
  }, {
    id: 'page-2', name: 'No analytics task', tasks: ['CREATE_CONTENT'], access_token: 'other-token'
  }];
  const facebook = normalizeDiscoveredResources('facebook_pages', pages);
  const instagram = normalizeDiscoveredResources('instagram', pages);
  assert.equal(facebook.length, 1);
  assert.equal(instagram.length, 1);
  assert.equal(facebook[0].metadata.analyzeAccess, true);
  assert.equal(Object.hasOwn(facebook[0].metadata, 'tasks'), false);
  assert.equal(instagram[0].metadata.sourcePageId, 'page-1');
  assert.equal(instagram[0].metadata.storyHistory, 'not_collected_without_webhooks');
  assert.deepEqual(meta.instagramMetricsForMedia({ media_product_type: 'STORY' }), []);
  assert.deepEqual(meta.instagramMetricsForMedia({ media_type: 'UNKNOWN' }), []);
  assert.equal(meta.instagramMetricsForMedia({ media_product_type: 'REELS' }).includes('ig_reels_avg_watch_time'), false);

  const pagePost = normalizeFacebookPost({
    id: 'post-1', message: 'Page post', reactions: { summary: { total_count: 5 } },
    comments: { summary: { total_count: 2 } }, shares: { count: 1 },
    attachments: { data: [{ media_type: 'photo' }] }
  });
  assert.equal(pagePost.likeCount, null);
  assert.equal(pagePost.commentCount, null);
  assert.equal(pagePost.shareCount, 1);
  assert.deepEqual(pagePost.metadata.attachmentTypes, ['photo']);
  assert.deepEqual(pagePost.metadata.availability, {
    reactions: 'unavailable_under_approved_narrow_permissions',
    comments: 'unavailable_under_approved_narrow_permissions'
  });
  assert.deepEqual(pagePost.metadata.availability, FACEBOOK_POST_COUNT_AVAILABILITY);
  const media = normalizeInstagramMedia({
    id: 'media-1', media_product_type: 'FEED', media_type: 'IMAGE', like_count: 4, comments_count: 1
  });
  assert.equal(media.likeCount, 4);
});

test('insight parsers preserve daily date semantics and structured values without inventing zeroes', () => {
  const body = {
    data: [{
      name: 'reach',
      values: [
        { value: 10, end_time: '2026-07-17T07:00:00+0000' },
        { value: 12, end_time: '2026-07-18T07:00:00+0000' }
      ]
    }]
  };
  assert.equal(latestInsightValue(body, 'reach'), 12);
  assert.deepEqual(dailyInsightValues(body, 'reach'), [
    { date: '2026-07-16', value: 10 },
    { date: '2026-07-17', value: 12 }
  ]);
  assert.equal(latestInsightValue(body, 'views'), null);
  assert.deepEqual(dailyInsightValues(body, 'views'), []);
  assert.equal(latestInsightValue({ data: [] }, 'reach'), null);
});
