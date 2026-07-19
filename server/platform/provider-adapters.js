const READ_ONLY_DENYLIST = [
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

const PROVIDER_ADAPTERS = {
  tiktok: {
    provider: 'tiktok',
    authorizationProvider: 'tiktok',
    implemented: true,
    featureFlag: 'FEATURE_TIKTOK_CONNECTOR',
    productAuthOnly: true,
    reuseSignInTokens: false,
    resources: ['tiktok_account'],
    requiredScopes: ['user.info.basic', 'user.info.profile', 'user.info.stats', 'video.list'],
    capabilities: [
      'profile_identity',
      'profile_snapshot_metrics',
      'content_listing',
      'content_snapshot_metrics',
      'disconnect'
    ]
  },
  instagram: {
    provider: 'instagram',
    authorizationProvider: 'meta',
    implemented: true,
    featureFlag: 'FEATURE_INSTAGRAM_CONNECTOR',
    productAuthOnly: true,
    reuseSignInTokens: false,
    resources: ['instagram_account'],
    requiredScopes: [
      'instagram_basic',
      'instagram_manage_insights',
      'pages_show_list',
      'pages_read_engagement'
    ],
    capabilities: ['resource_discovery', 'profile_insights', 'media_listing', 'media_insights', 'disconnect']
  },
  facebook_pages: {
    provider: 'facebook_pages',
    authorizationProvider: 'meta',
    implemented: true,
    featureFlag: 'FEATURE_FACEBOOK_PAGES_CONNECTOR',
    productAuthOnly: true,
    reuseSignInTokens: false,
    resources: ['facebook_page'],
    requiredScopes: ['pages_show_list', 'pages_read_engagement', 'read_insights'],
    capabilities: ['resource_discovery', 'page_insights', 'post_listing', 'post_insights', 'disconnect']
  },
  youtube: {
    provider: 'youtube',
    authorizationProvider: 'google',
    implemented: true,
    featureFlag: 'YOUTUBE_ENABLED',
    incrementalAuthorization: true,
    productAuthOnly: true,
    reuseSignInTokens: false,
    resources: ['youtube_channel'],
    requiredScopes: [
      'https://www.googleapis.com/auth/youtube.readonly',
      'https://www.googleapis.com/auth/yt-analytics.readonly'
    ],
    capabilities: [
      'resource_discovery',
      'channel_metrics',
      'video_listing',
      'video_analytics',
      'dimension_breakdowns',
      'disconnect'
    ]
  },
  google_analytics_4: {
    provider: 'google_analytics_4',
    authorizationProvider: 'google',
    implemented: true,
    featureFlag: 'FEATURE_GA4_CONNECTOR',
    incrementalAuthorization: true,
    productAuthOnly: true,
    reuseSignInTokens: false,
    resources: ['ga4_property'],
    requiredScopes: ['https://www.googleapis.com/auth/analytics.readonly'],
    capabilities: [
      'resource_discovery',
      'property_metadata',
      'traffic_metrics',
      'dimension_breakdowns',
      'compatibility_checks',
      'disconnect'
    ]
  }
};

function getProviderAdapter(provider) {
  return PROVIDER_ADAPTERS[provider] || null;
}

function listProviderAdapters() {
  return Object.values(PROVIDER_ADAPTERS).map(adapter => ({
    ...adapter,
    requiredScopes: [...adapter.requiredScopes],
    resources: [...adapter.resources],
    capabilities: [...adapter.capabilities]
  }));
}

function assertReadOnlyScopes(adapters = listProviderAdapters()) {
  for (const adapter of adapters) {
    for (const scope of adapter.requiredScopes) {
      const normalized = scope.toLowerCase();
      if (READ_ONLY_DENYLIST.some(term => normalized.includes(term))) {
        throw new Error(`write_or_sensitive_scope:${adapter.provider}:${scope}`);
      }
    }
  }
}

module.exports = {
  READ_ONLY_DENYLIST,
  assertReadOnlyScopes,
  getProviderAdapter,
  listProviderAdapters
};
