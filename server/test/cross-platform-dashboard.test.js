const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildCrossPlatformDashboard,
  freshnessState,
  getCrossPlatformDashboard
} = require('../platform/cross-platform-dashboard-service');

function catalogProvider(id, connections = []) {
  return {
    id,
    name: id,
    status: connections.length > 0 ? 'active' : 'available',
    configuration: { status: 'available' },
    connections,
    connection: connections[0] || { status: 'disconnected' }
  };
}

function metric(key, label, value, baseline, extra = {}) {
  return {
    key,
    label,
    value,
    baseline,
    delta: value === null || baseline === null ? null : value - baseline,
    percent_change: value === null || baseline === null || baseline === 0
      ? null
      : ((value - baseline) / baseline) * 100,
    available: value !== null,
    ...extra
  };
}

test('cross-platform overview preserves provider metric boundaries and availability', () => {
  const now = new Date('2026-07-18T12:00:00.000Z');
  const entries = [
    {
      provider: 'tiktok',
      connectionId: null,
      catalogProvider: catalogProvider('tiktok', [{
        status: 'active',
        last_successful_sync_at: '2026-07-18T10:00:00.000Z',
        account: { id: 'tt-account', display_name: 'Studio TikTok' }
      }]),
      dashboard: {
        range: { from: '2026-07-12T00:00:00.000Z', to: '2026-07-18T00:00:00.000Z' },
        connection: { status: 'active', last_successful_sync_at: '2026-07-18T10:00:00.000Z' },
        metrics: [
          metric('follower_count', 'Followers', 1200, 1100),
          metric('likes_count', 'Total likes', 5200, 5000),
          metric('video_count', 'Total videos', 31, 29)
        ],
        trend: [{ observed_at: '2026-07-18T10:00:00.000Z', follower_count: 1200, likes_count: 5200 }],
        top_content: [{ id: 'tt-video', title: 'Launch', view_count: 900, published_at: '2026-07-17' }]
      }
    },
    {
      provider: 'youtube',
      connectionId: 'yt-connection',
      catalogProvider: catalogProvider('youtube', [{
        id: 'yt-connection', status: 'active', last_successful_sync_at: '2026-07-18T09:00:00.000Z'
      }]),
      dashboard: {
        range: { key: '7d', from: '2026-07-11', to: '2026-07-17' },
        connection: { id: 'yt-connection', status: 'active', last_successful_sync_at: '2026-07-18T09:00:00.000Z' },
        channel: { id: 'channel-1', display_name: 'Studio Channel' },
        metrics: [
          metric('subscribers_current', 'Subscribers', 400, null),
          metric('views_period', 'Views', 1000, 800),
          metric('watch_time_period', 'Watch time', 210, 190),
          metric('net_subscribers_period', 'Net subscribers', 20, 15)
        ],
        trend: [{ date: '2026-07-17', views: 180, watch_time_minutes: 40, net_subscribers: 3 }],
        content: [{ id: 'yt-video', title: 'Tutorial', views: 700, published_at: '2026-07-16' }],
        availability: { state: 'ready', data_through_date: '2026-07-17' }
      }
    },
    {
      provider: 'facebook_pages',
      connectionId: 'fb-connection',
      catalogProvider: catalogProvider('facebook_pages', [{
        id: 'fb-connection', status: 'active', last_successful_sync_at: '2026-07-18T08:00:00.000Z'
      }]),
      dashboard: {
        range: { from: '2026-07-11', to: '2026-07-17' },
        connection: { id: 'fb-connection', status: 'active', last_successful_sync_at: '2026-07-18T08:00:00.000Z' },
        account: { id: 'page-1', display_name: 'Studio Page' },
        metrics: [
          metric('page_follows', 'Page follows', 800, 780),
          metric('page_post_engagements', 'Post engagements', 240, 210),
          metric('page_media_view', 'Media views', 3000, 2700)
        ],
        trend: [{ date: '2026-07-17', page_follows: 800, page_post_engagements: 40, page_media_view: 500 }],
        content: [],
        availability: { state: 'available' }
      }
    },
    {
      provider: 'instagram',
      connectionId: 'ig-connection',
      catalogProvider: catalogProvider('instagram', [{
        id: 'ig-connection', status: 'active', last_successful_sync_at: '2026-07-18T08:00:00.000Z'
      }]),
      dashboard: {
        range: { from: '2026-07-11', to: '2026-07-17', provider_period_days: 7 },
        connection: { id: 'ig-connection', status: 'active', last_successful_sync_at: '2026-07-18T08:00:00.000Z' },
        account: { id: 'ig-1', display_name: '@studio' },
        metrics: [
          metric('followers', 'Followers', 2000, 1900),
          metric('views', 'Views', 10000, 9000),
          metric('reach', 'Reach', null, null),
          metric('total_interactions', 'Interactions', 450, 400)
        ],
        trend: [],
        content: [],
        availability: { state: 'available' }
      }
    },
    {
      provider: 'google_analytics_4',
      connectionId: 'ga-connection',
      catalogProvider: catalogProvider('google_analytics_4', [{
        id: 'ga-connection', status: 'active', last_successful_sync_at: '2026-07-18T07:00:00.000Z'
      }]),
      dashboard: {
        range: { key: '7d', from: '2026-07-11', to: '2026-07-17', timezone: 'Asia/Karachi' },
        connection: { id: 'ga-connection', status: 'active', last_successful_sync_at: '2026-07-18T07:00:00.000Z' },
        property: { id: 'properties/123', display_name: 'Studio Web', account_name: 'Studio', timezone: 'Asia/Karachi' },
        metrics: [
          metric('ga4.active_users', 'Active users', 500, 450, { unit: 'count', availability_status: 'available' }),
          metric('ga4.sessions', 'Sessions', 700, 620, { unit: 'count', availability_status: 'available' }),
          metric('ga4.screen_page_views', 'Views', 1100, 900, { unit: 'count', availability_status: 'available' }),
          metric('ga4.engagement_rate', 'Engagement rate', 0.61, 0.58, { unit: 'ratio', availability_status: 'available' })
        ],
        trend: [{ date: '2026-07-17', active_users: 90, sessions: 120, screen_page_views: 180 }],
        breakdowns: [{
          key: 'ga4.landing_page',
          rows: [{
            dimensions: { landingPagePlusQueryString: '/launch' },
            metrics: { 'ga4.sessions': { value: 210, status: 'available', reason: null } }
          }]
        }],
        availability: { state: 'thresholded', data_through_date: '2026-07-17', subject_to_thresholding: true }
      }
    }
  ];

  const overview = buildCrossPlatformDashboard({
    entries,
    range: { key: '7d', from: '2026-07-11', to: '2026-07-17' },
    now
  });

  assert.equal(overview.sources.length, 5);
  assert.equal(overview.summary.connected_resources, 5);
  assert.equal(overview.state, 'partial');
  assert.ok(!Object.hasOwn(overview, 'total'));
  assert.ok(!Object.hasOwn(overview.summary, 'reach'));
  assert.match(overview.methodology[0], /never summed/i);

  const instagram = overview.sources.find(source => source.provider === 'instagram');
  assert.equal(instagram.metrics.find(item => item.key === 'reach').available, false);
  assert.equal(instagram.metrics.find(item => item.key === 'reach').value, null);
  assert.equal(instagram.range.provider_period_days, 7);

  const website = overview.sources.find(source => source.provider === 'google_analytics_4');
  assert.equal(website.metrics.find(item => item.key === 'ga4.engagement_rate').unit, 'ratio');
  assert.equal(website.top_content[0].kind, 'website_path');
  assert.equal(website.top_content[0].primary_metric.value, 210);
  assert.equal(website.alert.code, 'privacy_thresholding');

  const serialized = JSON.stringify(overview);
  assert.doesNotMatch(serialized, /total[_ ]reach/i);
  assert.doesNotMatch(serialized, /combined[_ ]views/i);
});

test('cross-platform overview loads every selected resource explicitly', async () => {
  const calls = [];
  const catalog = [
    catalogProvider('tiktok'),
    catalogProvider('youtube', [
      { id: 'youtube-one', status: 'active', account: { id: 'one', display_name: 'One' } },
      { id: 'youtube-two', status: 'active', account: { id: 'two', display_name: 'Two' } }
    ]),
    catalogProvider('facebook_pages'),
    catalogProvider('instagram'),
    catalogProvider('google_analytics_4')
  ];
  const disconnected = provider => ({
    provider,
    range: { key: '7d', from: '2026-07-11', to: '2026-07-17' },
    connection: { status: 'disconnected' },
    metrics: [], trend: [], content: [], top_content: [], breakdowns: [],
    availability: { state: 'empty' }
  });
  const dependencies = {
    catalog: async () => catalog,
    tiktok: async (userId, workspaceId, query) => {
      calls.push({ provider: 'tiktok', userId, workspaceId, query });
      return disconnected('tiktok');
    },
    youtube: async (userId, workspaceId, query) => {
      calls.push({ provider: 'youtube', userId, workspaceId, query });
      return {
        ...disconnected('youtube'),
        connection: { id: query.connection_id, status: 'active' },
        channel: { id: query.connection_id, display_name: query.connection_id }
      };
    },
    meta: async (userId, workspaceId, provider, query) => {
      calls.push({ provider, userId, workspaceId, query });
      return disconnected(provider);
    },
    googleAnalytics: async (userId, workspaceId, query) => {
      calls.push({ provider: 'google_analytics_4', userId, workspaceId, query });
      return disconnected('google_analytics_4');
    }
  };

  const overview = await getCrossPlatformDashboard('user-1', 'workspace-1', { range: '7d' }, dependencies);
  const youtubeCalls = calls.filter(call => call.provider === 'youtube');
  assert.deepEqual(youtubeCalls.map(call => call.query.connection_id).sort(), ['youtube-one', 'youtube-two']);
  assert.equal(overview.sources.filter(source => source.provider === 'youtube').length, 2);
  assert.deepEqual(
    overview.sources.filter(source => source.provider === 'youtube').map(source => source.resource.connection_id).sort(),
    ['youtube-one', 'youtube-two']
  );
  assert.equal(calls.length, 6);
  assert.match(calls.find(call => call.provider === 'tiktok').query.from, /T00:00:00\.000Z$/);
  assert.match(calls.find(call => call.provider === 'facebook_pages').query.to, /T23:59:59\.999Z$/);
  assert.equal(Object.hasOwn(calls.find(call => call.provider === 'instagram').query, 'from'), false);
});

test('freshness classification is deterministic and does not fabricate data', () => {
  const now = new Date('2026-07-18T12:00:00.000Z');
  assert.equal(freshnessState({
    status: 'active', availabilityState: 'ready', lastSuccessfulSyncAt: '2026-07-16T00:00:00.000Z',
    hasData: true, demoData: false, latestSyncStatus: 'success', now
  }), 'stale');
  assert.equal(freshnessState({
    status: 'active', availabilityState: 'delayed', lastSuccessfulSyncAt: null,
    hasData: false, demoData: false, latestSyncStatus: null, now
  }), 'delayed');
  assert.equal(freshnessState({
    status: 'active', availabilityState: 'available', lastSuccessfulSyncAt: '2026-07-18T10:00:00.000Z',
    hasData: true, demoData: false, latestSyncStatus: 'partial', now
  }), 'partial');
  assert.equal(freshnessState({
    status: 'disconnected', availabilityState: 'empty', lastSuccessfulSyncAt: null,
    hasData: false, demoData: false, latestSyncStatus: null, now
  }), 'disconnected');
});
