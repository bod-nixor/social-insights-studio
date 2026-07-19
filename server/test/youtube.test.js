const { afterEach, test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const youtube = require('../integrations/youtube');
const { normalizeReturnPath } = require('../platform/connection-service');
const { getYouTubeConfiguration, getYouTubeLimits, validateRedirectUri } = require('../platform/youtube-config');
const { parseRange } = require('../platform/youtube-dashboard-service');
const {
  analyticsRows,
  normalizeChannel,
  normalizePlaylistItem,
  normalizeVideo,
  parseIsoDuration,
  retryDelaySeconds
} = require('../platform/youtube-sync-service');

const testEnv = {
  NODE_ENV: 'test',
  BASE_URL: 'http://localhost:3001',
  YOUTUBE_ENABLED: 'true',
  YOUTUBE_CLIENT_ID: 'youtube-client-id',
  YOUTUBE_CLIENT_SECRET: 'youtube-client-secret',
  YOUTUBE_REDIRECT_URI: 'http://localhost:3001/api/integrations/youtube/callback',
  ENCRYPTION_KEY: '4'.repeat(64),
  YOUTUBE_SYNC_MAX_RETRIES: '2'
};

function response(status, body, headers = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: name => headers[String(name).toLowerCase()] || null },
    text: async () => (body === null ? '' : JSON.stringify(body))
  };
}

afterEach(() => {
  youtube.setYouTubeTestHooks();
});

test('YouTube configuration is disabled by default and requires exact callback and encryption settings', () => {
  assert.deepEqual(getYouTubeConfiguration({}).warnings, []);
  assert.equal(getYouTubeConfiguration({}).status, 'disabled');
  assert.equal(getYouTubeConfiguration(testEnv).connectable, true);
  assert.equal(validateRedirectUri(testEnv).ready, true);
  assert.equal(validateRedirectUri({
    ...testEnv,
    YOUTUBE_REDIRECT_URI: 'http://localhost:3001/api/integrations/youtube/callback?next=/unsafe'
  }).ready, false);
  assert.equal(validateRedirectUri({
    ...testEnv,
    NODE_ENV: 'production',
    BASE_URL: 'http://localhost:3001'
  }).ready, false);
  assert.equal(getYouTubeLimits({ YOUTUBE_SYNC_MAX_VIDEOS: '275' }).maxVideos, 250);
  assert.equal(getYouTubeLimits({ YOUTUBE_SYNC_MAX_VIDEOS: '49' }).maxVideos, 250);
});

test('authorization URL binds state, S256 PKCE, offline access, and only the approved scopes', () => {
  const pkce = youtube.createPkcePair();
  const expectedChallenge = crypto.createHash('sha256').update(pkce.verifier).digest('base64url');
  const url = new URL(youtube.buildAuthorizationUrl({ state: 'bound-state', codeChallenge: pkce.challenge }, testEnv));

  assert.equal(url.origin + url.pathname, youtube.GOOGLE_AUTH_URL);
  assert.equal(url.searchParams.get('state'), 'bound-state');
  assert.equal(url.searchParams.get('code_challenge_method'), 'S256');
  assert.equal(url.searchParams.get('code_challenge'), expectedChallenge);
  assert.equal(url.searchParams.get('access_type'), 'offline');
  assert.equal(url.searchParams.get('include_granted_scopes'), 'true');
  assert.deepEqual(url.searchParams.get('scope').split(' '), youtube.YOUTUBE_SCOPES);
  assert.deepEqual(youtube.YOUTUBE_SCOPES, [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
  ]);

  const reconnectUrl = new URL(youtube.buildAuthorizationUrl({
    state: 'reconnect-state',
    codeChallenge: pkce.challenge,
    promptConsent: false
  }, testEnv));
  const firstGrantUrl = new URL(youtube.buildAuthorizationUrl({
    state: 'first-grant-state',
    codeChallenge: pkce.challenge,
    promptConsent: true
  }, testEnv));
  assert.equal(reconnectUrl.searchParams.has('prompt'), false);
  assert.equal(firstGrantUrl.searchParams.get('prompt'), 'consent');
});

test('only the exact approved YouTube scope pair is accepted', () => {
  assert.equal(youtube.hasExactScopes(youtube.YOUTUBE_SCOPES.join(' ')), true);
  assert.equal(youtube.hasExactScopes([...youtube.YOUTUBE_SCOPES].reverse()), true);
  assert.equal(youtube.hasExactScopes([youtube.YOUTUBE_SCOPES[0]]), false);
  assert.equal(
    youtube.hasExactScopes([...youtube.YOUTUBE_SCOPES, 'https://www.googleapis.com/auth/youtube.upload']),
    false
  );
});

test('refresh tokens are preserved when omitted and rotated when Google returns a replacement', () => {
  assert.equal(youtube.chooseRefreshToken(undefined, 'existing-refresh'), 'existing-refresh');
  assert.equal(youtube.chooseRefreshToken('', 'existing-refresh'), 'existing-refresh');
  assert.equal(youtube.chooseRefreshToken('rotated-refresh', 'existing-refresh'), 'rotated-refresh');
  assert.equal(youtube.chooseRefreshToken(null, null), null);
});

test('post-callback return paths remain local and reject open redirects', () => {
  const contentPath = '/workspaces/10000000-0000-4000-8000-000000000001/content/50000000-0000-4000-8000-000000000001?range=30d#metrics';
  assert.equal(normalizeReturnPath('/app?view=connections'), '/?view=connections');
  assert.equal(normalizeReturnPath(contentPath), contentPath);
  for (const unsafe of [
    '//evil.example/steal',
    'https://evil.example/steal',
    '/\\evil.example/steal',
    '/unsupported/path',
    `/workspaces/not-a-uuid/content/not-a-uuid`
  ]) {
    assert.throws(() => normalizeReturnPath(unsafe), /invalid_return_path/);
  }
});

test('code exchange sends the verifier and credentials in a form body without retrying', async () => {
  const calls = [];
  youtube.setYouTubeTestHooks({
    fetch: async (url, options) => {
      calls.push({ url: String(url), options });
      return response(200, { access_token: 'access', expires_in: 3600 });
    }
  });

  const result = await youtube.exchangeCode('provider-code', 'pkce-verifier', testEnv);
  assert.equal(result.ok, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, youtube.GOOGLE_TOKEN_URL);
  const body = new URLSearchParams(calls[0].options.body);
  assert.equal(body.get('code'), 'provider-code');
  assert.equal(body.get('code_verifier'), 'pkce-verifier');
  assert.equal(body.get('grant_type'), 'authorization_code');
  assert.equal(body.get('redirect_uri'), testEnv.YOUTUBE_REDIRECT_URI);
});

test('Google calls retry only bounded retryable failures and honor Retry-After', async () => {
  const sleeps = [];
  let attempts = 0;
  youtube.setYouTubeTestHooks({
    fetch: async () => {
      attempts += 1;
      if (attempts === 1) return response(429, { error: { errors: [{ reason: 'rateLimitExceeded' }] } }, { 'retry-after': '1' });
      return response(200, { items: [{ id: 'channel-1' }] });
    },
    sleep: async milliseconds => sleeps.push(milliseconds),
    random: () => 0
  });

  const result = await youtube.getChannel('access', 'channel-1', { env: testEnv });
  assert.equal(result.ok, true);
  assert.equal(result.attempts, 2);
  assert.deepEqual(sleeps, [1000]);
});

test('Google calls bound 5xx retries, do not retry quota failures, and stop before an expired job deadline', async () => {
  let attempts = 0;
  const sleeps = [];
  youtube.setYouTubeTestHooks({
    fetch: async () => {
      attempts += 1;
      return response(503, { error: { status: 'UNAVAILABLE' } });
    },
    sleep: async milliseconds => sleeps.push(milliseconds),
    random: () => 0
  });
  const unavailable = await youtube.getChannel('access', 'channel-1', { env: testEnv });
  assert.equal(unavailable.ok, false);
  assert.equal(unavailable.error.category, 'provider');
  assert.equal(attempts, 3);
  assert.deepEqual(sleeps, [250, 500]);

  attempts = 0;
  youtube.setYouTubeTestHooks({
    fetch: async () => {
      attempts += 1;
      return response(403, { error: { errors: [{ reason: 'quotaExceeded' }] } });
    }
  });
  const quota = await youtube.getChannel('access', 'channel-1', { env: testEnv });
  assert.equal(quota.error.category, 'quota');
  assert.equal(attempts, 1);

  attempts = 0;
  youtube.setYouTubeTestHooks({ fetch: async () => { attempts += 1; return response(200, {}); } });
  const expired = await youtube.getChannel('access', 'channel-1', {
    env: testEnv,
    deadlineMs: Date.now() - 1
  });
  assert.equal(expired.ok, false);
  assert.equal(expired.budgetExhausted, true);
  assert.equal(expired.attempts, 0);
  assert.equal(attempts, 0);
});

test('malformed Google JSON is sanitized without exposing the response body', async () => {
  youtube.setYouTubeTestHooks({
    fetch: async () => ({
      ok: true,
      status: 200,
      headers: { get: () => null },
      text: async () => '{"access_token":"secret-fragment"'
    })
  });
  const result = await youtube.listMyChannels('access', { env: testEnv, maxRetries: 0 });
  assert.equal(result.ok, false);
  assert.equal(result.error.category, 'malformed_response');
  assert.equal(JSON.stringify(result).includes('secret-fragment'), false);
});

test('provider failures distinguish terminal auth, scope, quota, rate limits, and transient service errors', () => {
  assert.deepEqual(youtube.categorizeProviderFailure(400, { error: 'invalid_grant' }), {
    category: 'authentication', retryable: false, terminal: true, provider_code: 'invalid_grant'
  });
  assert.equal(youtube.categorizeProviderFailure(403, { error: { errors: [{ reason: 'insufficientPermissions' }] } }).category, 'scope');
  assert.equal(youtube.categorizeProviderFailure(403, { error: { errors: [{ reason: 'quotaExceeded' }] } }).category, 'quota');
  assert.equal(youtube.categorizeProviderFailure(429, {}).retryable, true);
  assert.equal(youtube.categorizeProviderFailure(503, {}).retryable, true);
  assert.equal(retryDelaySeconds({ category: 'quota', retryable: false }, 21600), 21600);
  assert.equal(retryDelaySeconds({ category: 'provider', retryable: true, retry_after_seconds: 45 }, 21600), 60);
  assert.equal(retryDelaySeconds({ category: 'rate_limit', retryable: true, retry_after_seconds: 90 }, 21600), 90);
});

test('Data and Analytics request builders use bounded pages, batches, and non-monetary metrics', async () => {
  const calls = [];
  youtube.setYouTubeTestHooks({
    fetch: async (url, options) => {
      calls.push({ url: new URL(String(url)), options });
      return response(200, { columnHeaders: [], rows: [] });
    }
  });

  await youtube.listMyChannels('access', { env: testEnv, maxRetries: 0 });
  await youtube.listUploadItems('access', 'uploads-id', 'next-page', { env: testEnv, maxRetries: 0 });
  await youtube.listVideos('access', ['video-1', 'video-2'], { env: testEnv, maxRetries: 0 });
  await youtube.queryAnalytics('access', {
    channelId: 'channel-1', startDate: '2026-07-01', endDate: '2026-07-17', dimensions: 'video', sort: '-views', maxResults: 200
  }, { env: testEnv, maxRetries: 0 });

  assert.equal(calls[0].url.searchParams.get('mine'), 'true');
  assert.equal(calls[0].url.searchParams.get('maxResults'), '50');
  assert.equal(calls[1].url.searchParams.get('maxResults'), '50');
  assert.equal(calls[1].url.searchParams.get('pageToken'), 'next-page');
  assert.equal(calls[2].url.searchParams.get('id'), 'video-1,video-2');
  assert.equal(calls[3].url.searchParams.get('ids'), 'channel==channel-1');
  assert.equal(calls[3].url.searchParams.get('maxResults'), '200');
  assert.deepEqual(calls[3].url.searchParams.get('metrics').split(','), youtube.YOUTUBE_ANALYTICS_METRICS);
  assert.equal(youtube.YOUTUBE_ANALYTICS_METRICS.some(metric => /revenue|ad|monetary/i.test(metric)), false);
  await assert.rejects(() => youtube.listVideos('access', Array.from({ length: 51 }, (_, index) => String(index))), /youtube_video_batch_invalid/);
});

test('YouTube normalizers preserve hidden, deleted, private, and unavailable provider states', () => {
  const channel = normalizeChannel({
    id: 'channel-1',
    snippet: { title: 'Studio', thumbnails: { high: { url: 'https://img.example/channel.jpg' } } },
    statistics: { hiddenSubscriberCount: true, viewCount: '90', videoCount: '4' },
    contentDetails: { relatedPlaylists: { uploads: 'uploads-1' } }
  });
  assert.equal(channel.subscriberCount, null);
  assert.equal(channel.availability.subscriber_count, 'hidden_by_channel');
  assert.equal(channel.uploadsPlaylistId, 'uploads-1');

  const deleted = normalizePlaylistItem({
    snippet: { title: 'Deleted video', resourceId: { videoId: 'deleted-1' } },
    contentDetails: { videoId: 'deleted-1' }
  });
  assert.equal(deleted.title, null);
  assert.equal(deleted.unavailableReason, 'deleted_video');

  const video = normalizeVideo(null, deleted);
  assert.equal(video.providerContentId, 'deleted-1');
  assert.equal(video.availability.video, 'deleted_video');
  assert.equal(parseIsoDuration('PT1H2M3S'), 3723);
  assert.equal(parseIsoDuration('not-a-duration'), null);
});

test('Analytics rows and dashboard ranges preserve provider date semantics', () => {
  const rows = analyticsRows({
    columnHeaders: [{ name: 'day' }, { name: 'views' }],
    rows: [['2026-07-17', 42]]
  }, 'day');
  assert.deepEqual(rows, [{ day: '2026-07-17', views: 42 }]);
  assert.throws(() => analyticsRows({ columnHeaders: [{ name: 'views' }], rows: [[42]] }, 'day'), /youtube_analytics_dimension_missing/);

  const custom = parseRange({ range: 'custom', from: '2026-07-01', to: '2026-07-10' });
  assert.equal(custom.days, 10);
  assert.equal(custom.previousFrom, '2026-06-21');
  assert.equal(custom.previousTo, '2026-06-30');
  assert.equal(custom.videoPeriodKey, null);
  assert.throws(() => parseRange({ range: 'custom', from: '2026-07-10', to: '2026-07-01' }), /invalid_date_range/);
  assert.throws(() => parseRange({ range: 'custom', from: '2026-02-30', to: '2026-03-02' }), /invalid_date_range/);
  assert.throws(() => parseRange({ range: 'custom', from: '2025-01-01', to: '2026-01-02' }), /invalid_date_range/);
});
