const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  getMetricDefinitions,
  getProviderCatalog,
  getPublicProviderCatalog
} = require('../platform/provider-registry');
const {
  assertReadOnlyScopes,
  getProviderAdapter,
  listProviderAdapters
} = require('../platform/provider-adapters');

test('provider catalog covers the requested production providers', () => {
  const providers = getProviderCatalog({
    TIKTOK_CLIENT_KEY: 'key',
    TIKTOK_CLIENT_SECRET: 'secret'
  });
  const ids = providers.map(provider => provider.id).sort();

  assert.deepEqual(ids, [
    'facebook_pages',
    'google_analytics_4',
    'instagram',
    'tiktok',
    'youtube'
  ]);
});

test('implemented providers remain gated until their runtime configuration is complete', () => {
  const providers = getPublicProviderCatalog({
    TIKTOK_CLIENT_KEY: 'key',
    TIKTOK_CLIENT_SECRET: 'secret',
    FEATURE_INSTAGRAM_CONNECTOR: '1',
    FEATURE_FACEBOOK_PAGES_CONNECTOR: '1',
    YOUTUBE_ENABLED: '1',
    FEATURE_GA4_CONNECTOR: '1'
  });
  const byId = Object.fromEntries(providers.map(provider => [provider.id, provider]));

  assert.equal(byId.tiktok.implemented, true);
  assert.equal(byId.instagram.implemented, true);
  assert.equal(byId.instagram.status, 'configuration_required');
  assert.equal(byId.facebook_pages.implemented, true);
  assert.equal(byId.facebook_pages.status, 'configuration_required');
  assert.equal(byId.youtube.implemented, true);
  assert.equal(byId.youtube.status, 'configuration_required');
  assert.equal(byId.google_analytics_4.implemented, true);
  assert.equal(providers.every(provider => provider.connectable === false), true);
});

test('YouTube uses only the approved read-only scope pair and is disabled by default', () => {
  const disabled = getPublicProviderCatalog({}).find(provider => provider.id === 'youtube');
  const enabled = getProviderCatalog({ YOUTUBE_ENABLED: 'true' }).find(provider => provider.id === 'youtube');

  assert.equal(disabled.enabled, false);
  assert.equal(disabled.status, 'disabled');
  assert.deepEqual(enabled.requestedScopes.map(scope => scope.name), [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly'
  ]);
});

test('Meta catalog uses only the dashboard-evidenced Facebook Login read-only scopes', () => {
  const catalog = getProviderCatalog({});
  const facebook = catalog.find(provider => provider.id === 'facebook_pages');
  const instagram = catalog.find(provider => provider.id === 'instagram');

  assert.deepEqual(facebook.requestedScopes.map(scope => scope.name), [
    'pages_show_list',
    'pages_read_engagement',
    'read_insights'
  ]);
  assert.deepEqual(instagram.requestedScopes.map(scope => scope.name), [
    'instagram_basic',
    'instagram_manage_insights',
    'pages_show_list',
    'pages_read_engagement'
  ]);
  assert.equal(facebook.metrics.includes('facebook.page_impressions'), false);
});

test('provider catalog does not include write, ads, upload, messaging, or monetary scopes', () => {
  const deniedScopeTerms = [
    'ads',
    'business_management',
    'comments_manage',
    'force-ssl',
    'manage_messages',
    'messaging',
    'monetary',
    'pages_manage',
    'publish',
    'upload',
    'youtube.force-ssl'
  ];
  const publicCatalog = getPublicProviderCatalog({
    FEATURE_INSTAGRAM_CONNECTOR: '1',
    FEATURE_FACEBOOK_PAGES_CONNECTOR: '1',
    YOUTUBE_ENABLED: '1',
    FEATURE_GA4_CONNECTOR: '1'
  });

  for (const provider of publicCatalog) {
    for (const scope of provider.requestedScopes) {
      assert.equal(scope.access, 'read', `${provider.id}:${scope.name} must be read-only`);
      const normalized = scope.name.toLowerCase();
      assert.equal(
        deniedScopeTerms.some(term => normalized.includes(term)),
        false,
        `${provider.id}:${scope.name} should not be requested for analytics-only features`
      );
    }
  }
});

test('GA4 metric dictionary keeps users, sessions, and views distinct', () => {
  const metrics = getMetricDefinitions();

  assert.equal(metrics['ga4.active_users'].label, 'Active users');
  assert.equal(metrics['ga4.new_users'].label, 'New users');
  assert.equal(metrics['ga4.sessions'].label, 'Sessions');
  assert.equal(metrics['ga4.screen_page_views'].label, 'Views');
  assert.notEqual(metrics['ga4.new_users'].label, metrics['ga4.screen_page_views'].label);
  assert.notEqual(metrics['ga4.sessions'].label, metrics['ga4.screen_page_views'].label);
  assert.equal(metrics['ga4.active_users'].aggregation, 'provider_reported_range');
  assert.equal(metrics['ga4.average_session_duration'].unit, 'seconds');
  assert.equal(metrics['ga4.sessions_per_user'].label, 'Sessions per user');
  assert.equal(metrics['ga4.screen_page_views_per_user'].label, 'Views per user');
});

test('GA4 uses only analytics.readonly and remains disabled until its dedicated OAuth client is complete', () => {
  const disabled = getPublicProviderCatalog({}).find(provider => provider.id === 'google_analytics_4');
  const configured = getPublicProviderCatalog({
    NODE_ENV: 'test',
    BASE_URL: 'http://localhost:3001',
    FEATURE_GA4_CONNECTOR: 'true',
    GA4_CLIENT_ID: 'ga4-client-id',
    GA4_CLIENT_SECRET: 'ga4-client-secret',
    GA4_REDIRECT_URI: 'http://localhost:3001/api/integrations/google-analytics/callback',
    ENCRYPTION_KEY: '2'.repeat(64)
  }).find(provider => provider.id === 'google_analytics_4');

  assert.equal(disabled.enabled, false);
  assert.equal(disabled.status, 'disabled');
  assert.deepEqual(configured.requestedScopes.map(scope => scope.name), [
    'https://www.googleapis.com/auth/analytics.readonly'
  ]);
  assert.equal(configured.implemented, true);
  assert.equal(configured.connectable, true);
  assert.equal(configured.status, 'available');
});

test('every advertised metric has a versioned provider-specific definition and unit', () => {
  const definitions = getMetricDefinitions();
  const providers = getProviderCatalog({});

  for (const provider of providers) {
    for (const metricKey of provider.metrics) {
      const definition = definitions[metricKey];
      assert.ok(definition, `missing definition for ${metricKey}`);
      assert.equal(definition.provider, provider.id);
      assert.ok(definition.label);
      assert.ok(definition.unit);
      assert.ok(definition.aggregation);
      assert.ok(definition.dateSemantics);
      assert.ok(definition.definition);
      assert.ok(definition.unavailableWhen);
      assert.match(definition.version, /^\d{4}-\d{2}-\d{2}$/);
    }
  }
});

test('provider adapter contract keeps product auth separate from sign-in auth', () => {
  const adapters = listProviderAdapters();
  assert.equal(adapters.length, 5);
  assert.equal(getProviderAdapter('tiktok').implemented, true);
  assert.equal(getProviderAdapter('youtube').authorizationProvider, 'google');
  assert.equal(getProviderAdapter('google_analytics_4').incrementalAuthorization, true);
  assert.equal(adapters.every(adapter => adapter.productAuthOnly), true);
  assert.equal(adapters.every(adapter => adapter.reuseSignInTokens === false), true);
  assert.doesNotThrow(() => assertReadOnlyScopes(adapters));
});
