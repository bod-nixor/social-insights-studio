const { getConnection } = require('../database');
const { assertCapability } = require('./rbac');
const { getYouTubeConfiguration } = require('./youtube-config');

const PROVIDERS = [
  {
    id: 'tiktok',
    name: 'TikTok',
    resourceName: 'TikTok account',
    featureFlag: 'FEATURE_TIKTOK_CONNECTOR',
    configuredByDefault: true,
    statusWhenDisabled: 'configuration_required',
    authModel: 'OAuth 2.0 authorization code through TikTok Login Kit for Web',
    selectedResourceModel: 'one selected creator account per workspace connection',
    requestedScopes: [
      {
        name: 'user.info.basic',
        access: 'read',
        purpose: 'Read public profile identity fields for account cards and reports.'
      },
      {
        name: 'user.info.profile',
        access: 'read',
        purpose: 'Read approved profile fields exposed to this TikTok app.'
      },
      {
        name: 'user.info.stats',
        access: 'read',
        purpose: 'Read follower, following, total likes, and video count profile statistics.'
      },
      {
        name: 'video.list',
        access: 'read',
        purpose: 'Read authorized public video metadata and available video statistics.'
      }
    ],
    capabilities: [
      'profile_identity',
      'profile_snapshot_metrics',
      'content_listing',
      'content_snapshot_metrics',
      'disconnect'
    ],
    metrics: [
      'tiktok.followers',
      'tiktok.following',
      'tiktok.total_likes',
      'tiktok.video_count',
      'tiktok.video_views',
      'tiktok.video_likes',
      'tiktok.video_comments',
      'tiktok.video_shares'
    ],
    docs: [
      'https://developers.tiktok.com/doc/login-kit-web/',
      'https://developers.tiktok.com/doc/display-api-overview/',
      'https://developers.tiktok.com/doc/oauth-user-access-token-management'
    ]
  },
  {
    id: 'instagram',
    name: 'Instagram',
    resourceName: 'Instagram professional account',
    featureFlag: 'FEATURE_INSTAGRAM_CONNECTOR',
    statusWhenDisabled: 'coming_soon',
    authModel: 'Meta OAuth through the current Instagram Platform route',
    selectedResourceModel: 'one workspace connection per selected professional account',
    requestedScopes: [
      {
        name: 'instagram_business_basic',
        access: 'read',
        purpose: 'Discover and display authorized professional account identity and media basics.'
      },
      {
        name: 'instagram_business_manage_insights',
        access: 'read',
        purpose: 'Read implemented account and media insight metrics.'
      }
    ],
    capabilities: ['resource_discovery', 'profile_insights', 'media_listing', 'media_insights', 'disconnect'],
    metrics: [
      'instagram.reach',
      'instagram.views',
      'instagram.profile_activity',
      'instagram.likes',
      'instagram.comments',
      'instagram.shares',
      'instagram.saves'
    ],
    docs: [
      'https://developers.facebook.com/documentation/instagram-platform/overview',
      'https://developers.facebook.com/documentation/instagram-platform/insights',
      'https://developers.facebook.com/docs/permissions/'
    ]
  },
  {
    id: 'facebook_pages',
    name: 'Facebook Pages',
    resourceName: 'Facebook Page',
    featureFlag: 'FEATURE_FACEBOOK_PAGES_CONNECTOR',
    statusWhenDisabled: 'coming_soon',
    authModel: 'Meta OAuth for Pages access',
    selectedResourceModel: 'one workspace connection per selected Page',
    requestedScopes: [
      {
        name: 'pages_show_list',
        access: 'read',
        purpose: 'Discover Pages the authorizing user can access.'
      },
      {
        name: 'pages_read_engagement',
        access: 'read',
        purpose: 'Read Page content and engagement needed for analytics views.'
      },
      {
        name: 'read_insights',
        access: 'read',
        purpose: 'Read implemented Page and post insight metrics.'
      }
    ],
    capabilities: ['resource_discovery', 'page_insights', 'post_listing', 'post_insights', 'disconnect'],
    metrics: [
      'facebook.followers',
      'facebook.page_impressions',
      'facebook.page_engagement',
      'facebook.post_reactions',
      'facebook.post_comments',
      'facebook.post_shares'
    ],
    docs: [
      'https://developers.facebook.com/docs/permissions/',
      'https://developers.facebook.com/docs/graph-api/reference/page/insights/'
    ]
  },
  {
    id: 'youtube',
    name: 'YouTube',
    resourceName: 'YouTube channel',
    featureFlag: 'YOUTUBE_ENABLED',
    statusWhenDisabled: 'disabled',
    authModel: 'Google OAuth incremental authorization',
    selectedResourceModel: 'one authorization can discover multiple channels; selected channels become workspace connections',
    requestedScopes: [
      {
        name: 'https://www.googleapis.com/auth/youtube.readonly',
        access: 'read',
        purpose: 'Discover owned or managed channels and read video/channel metadata.'
      },
      {
        name: 'https://www.googleapis.com/auth/yt-analytics.readonly',
        access: 'read',
        purpose: 'Read implemented YouTube Analytics reports for selected channels.'
      }
    ],
    capabilities: [
      'resource_discovery',
      'channel_metrics',
      'video_listing',
      'video_analytics',
      'dimension_breakdowns',
      'disconnect'
    ],
    metrics: [
      'youtube.subscribers',
      'youtube.views',
      'youtube.watch_time_minutes',
      'youtube.average_view_duration',
      'youtube.likes',
      'youtube.comments',
      'youtube.shares'
    ],
    docs: [
      'https://developers.google.com/youtube/v3/docs/channels/list',
      'https://developers.google.com/youtube/analytics/channel_reports',
      'https://developers.google.com/youtube/terms/developer-policies'
    ]
  },
  {
    id: 'google_analytics_4',
    name: 'Website Analytics',
    resourceName: 'GA4 property',
    featureFlag: 'FEATURE_GA4_CONNECTOR',
    statusWhenDisabled: 'coming_soon',
    authModel: 'Google OAuth incremental authorization',
    selectedResourceModel: 'one authorization can discover multiple GA4 properties; selected properties become workspace connections',
    requestedScopes: [
      {
        name: 'https://www.googleapis.com/auth/analytics.readonly',
        access: 'read',
        purpose: 'Discover accessible GA4 properties and read implemented reporting data.'
      }
    ],
    capabilities: [
      'resource_discovery',
      'property_metadata',
      'traffic_metrics',
      'dimension_breakdowns',
      'compatibility_checks',
      'disconnect'
    ],
    metrics: [
      'ga4.active_users',
      'ga4.new_users',
      'ga4.sessions',
      'ga4.screen_page_views',
      'ga4.engagement_rate',
      'ga4.bounce_rate',
      'ga4.average_session_duration'
    ],
    docs: [
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/checkCompatibility'
    ]
  }
];

const METRIC_DEFINITIONS = {
  'tiktok.followers': {
    label: 'Followers',
    provider: 'tiktok',
    unit: 'count',
    aggregation: 'latest_snapshot',
    dateSemantics: 'snapshot_at_sync_time',
    unavailableWhen: 'TikTok scope missing or profile statistic not returned'
  },
  'tiktok.video_views': {
    label: 'Video views',
    provider: 'tiktok',
    unit: 'count',
    aggregation: 'latest_video_snapshot',
    dateSemantics: 'video_snapshot_at_sync_time',
    unavailableWhen: 'No videos returned or video statistic not returned'
  },
  'instagram.reach': {
    label: 'Reach',
    provider: 'instagram',
    unit: 'count',
    aggregation: 'provider_reported_period',
    dateSemantics: 'provider_insight_period',
    unavailableWhen: 'Permission missing, media/account type unsupported, or privacy thresholding'
  },
  'facebook.page_impressions': {
    label: 'Page impressions',
    provider: 'facebook_pages',
    unit: 'count',
    aggregation: 'provider_reported_period',
    dateSemantics: 'provider_insight_period',
    unavailableWhen: 'Permission missing, metric deprecated, or Page ineligible'
  },
  'youtube.watch_time_minutes': {
    label: 'Watch time',
    provider: 'youtube',
    unit: 'minutes',
    aggregation: 'sum',
    dateSemantics: 'youtube_analytics_available_date_range',
    unavailableWhen: 'Analytics scope missing or YouTube data-through date is earlier than requested'
  },
  'ga4.active_users': {
    label: 'Active users',
    provider: 'google_analytics_4',
    unit: 'count',
    aggregation: 'sum',
    dateSemantics: 'property_timezone_date_range',
    unavailableWhen: 'Metric incompatible with requested dimensions or thresholded'
  },
  'ga4.new_users': {
    label: 'New users',
    provider: 'google_analytics_4',
    unit: 'count',
    aggregation: 'sum',
    dateSemantics: 'property_timezone_date_range',
    unavailableWhen: 'Metric incompatible with requested dimensions or thresholded'
  },
  'ga4.sessions': {
    label: 'Sessions',
    provider: 'google_analytics_4',
    unit: 'count',
    aggregation: 'sum',
    dateSemantics: 'property_timezone_date_range',
    unavailableWhen: 'Metric incompatible with requested dimensions or thresholded'
  },
  'ga4.screen_page_views': {
    label: 'Views',
    provider: 'google_analytics_4',
    unit: 'count',
    aggregation: 'sum',
    dateSemantics: 'property_timezone_date_range',
    unavailableWhen: 'Metric incompatible with requested dimensions or thresholded'
  },
  'ga4.engagement_rate': {
    label: 'Engagement rate',
    provider: 'google_analytics_4',
    unit: 'ratio',
    aggregation: 'provider_computed',
    dateSemantics: 'property_timezone_date_range',
    unavailableWhen: 'Metric incompatible with requested dimensions or thresholded'
  }
};

function flagEnabled(value) {
  return ['1', 'true', 'yes', 'on'].includes(String(value || '').trim().toLowerCase());
}

function flagDisabled(value) {
  return ['0', 'false', 'no', 'off'].includes(String(value || '').trim().toLowerCase());
}

function parseJson(value, fallback = {}) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function isEnabled(env, provider) {
  if (provider.configuredByDefault) {
    return !flagDisabled(env[provider.featureFlag]);
  }
  return flagEnabled(env[provider.featureFlag]);
}

function getProviderStatus(env, provider) {
  if (!isEnabled(env, provider)) {
    return provider.statusWhenDisabled;
  }
  if (provider.id === 'tiktok') {
    return env.TIKTOK_CLIENT_KEY && env.TIKTOK_CLIENT_SECRET ? 'available' : 'configuration_required';
  }
  if (provider.id === 'youtube') {
    return getYouTubeConfiguration(env).status;
  }
  return 'feature_flagged';
}

function providerIsImplemented(provider) {
  return provider.id === 'tiktok' || provider.id === 'youtube';
}

function toPublicProvider(provider, env) {
  const enabled = isEnabled(env, provider);
  const implemented = providerIsImplemented(provider);
  return {
    id: provider.id,
    name: provider.name,
    resourceName: provider.resourceName,
    enabled,
    implemented,
    connectable: false,
    status: getProviderStatus(env, provider),
    featureFlag: provider.featureFlag,
    authModel: provider.authModel,
    selectedResourceModel: provider.selectedResourceModel,
    capabilities: provider.capabilities,
    requestedScopes: provider.requestedScopes.map(scope => ({
      name: scope.name,
      access: scope.access,
      purpose: scope.purpose
    })),
    docs: provider.docs
  };
}

function getProviderCatalog(env = process.env) {
  return PROVIDERS.map(provider => ({
    ...provider,
    enabled: isEnabled(env, provider),
    implemented: providerIsImplemented(provider),
    status: getProviderStatus(env, provider)
  }));
}

function getPublicProviderCatalog(env = process.env) {
  return PROVIDERS.map(provider => toPublicProvider(provider, env));
}

function toWorkspaceProvider(provider, source, env) {
  const publicProvider = toPublicProvider(provider, env);
  const status = source ? source.status : 'disconnected';
  if (provider.id !== 'tiktok') {
    return {
      ...publicProvider,
      connectable: false,
      connection: null
    };
  }
  const configured = publicProvider.enabled && publicProvider.status === 'available';
  const connection = {
    status,
    reconnect_reason: source ? source.reconnect_reason : null,
    last_sync_at: source ? source.last_sync_at : null,
    last_successful_sync_at: source ? source.last_successful_sync_at : null,
    next_sync_at: source ? source.next_sync_at : null,
    account: source && source.provider_account_id
      ? {
          id: source.provider_account_id,
          username: source.username,
          display_name: source.display_name
        }
      : null
  };
  return {
    ...publicProvider,
    status: source ? status : publicProvider.status,
    connectable: configured && status !== 'connecting',
    connection
  };
}

async function getYouTubeWorkspaceProvider(connection, workspaceId, userId, env) {
  const tableRows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN (
         'provider_authorizations',
         'provider_authorization_credentials',
         'provider_resources',
         'workspace_provider_connections',
         'youtube_channel_snapshots',
         'youtube_analytics_daily_snapshots',
         'youtube_video_analytics_snapshots',
         'provider_request_events'
       )`
  );
  const foundationReady = Number(tableRows[0] && tableRows[0].count) === 8;
  const configuration = getYouTubeConfiguration(env, {
    databaseReady: true,
    foundationReady,
    workerReady: true
  });
  const provider = PROVIDERS.find(item => item.id === 'youtube');
  const publicProvider = toPublicProvider(provider, env);
  if (!foundationReady) {
    return {
      ...publicProvider,
      enabled: configuration.enabled,
      implemented: true,
      connectable: false,
      status: configuration.status,
      configuration: { status: configuration.status, warnings: configuration.warnings },
      authorization: null,
      resources: [],
      connections: [],
      connection: null
    };
  }

  const authorizationRows = await connection.query(
    `SELECT pauth.id, pauth.status, pauth.granted_at, pauth.last_validated_at, pauth.revoked_at,
            (
              SELECT JSON_UNQUOTE(JSON_EXTRACT(al.metadata, '$.outcome_category'))
              FROM audit_logs al
              WHERE al.target_id = pauth.id
                AND al.action = 'connection.youtube.authorization_failed'
              ORDER BY al.created_at DESC
              LIMIT 1
            ) AS failure_category
     FROM provider_authorizations pauth
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'
     ORDER BY FIELD(pauth.status, 'active', 'authorizing', 'reconnect_required', 'disabled', 'revoked'), pauth.updated_at DESC
     LIMIT 1`,
    [workspaceId]
  );
  const authorization = authorizationRows[0] || null;
  const scopeRows = authorization
    ? await connection.query(
        `SELECT scope, status FROM provider_authorization_scopes
         WHERE provider_authorization_id = ? ORDER BY scope`,
        [authorization.id]
      )
    : [];
  const resourceRows = await connection.query(
    `SELECT pr.id AS resource_id, pr.provider_resource_id, pr.display_name, pr.metadata,
            wpc.id AS connection_id, wpc.data_source_id, wpc.status AS connection_status,
            wpc.last_sync_at, wpc.last_successful_sync_at, wpc.next_sync_at, wpc.data_through_at,
            ds.reconnect_reason, pa.username
     FROM provider_resources pr
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     LEFT JOIN workspace_provider_connections wpc
       ON wpc.provider_resource_id = pr.id AND wpc.workspace_id = pr.workspace_id
     LEFT JOIN data_sources ds ON ds.id = wpc.data_source_id
     LEFT JOIN provider_accounts pa ON pa.data_source_id = wpc.data_source_id
     WHERE pr.workspace_id = ? AND pr.provider = 'youtube' AND pr.resource_type = 'youtube_channel'
     ORDER BY pr.display_name, pr.created_at`,
    [workspaceId]
  );
  const capabilityRows = await connection.query(
    `SELECT pc.workspace_provider_connection_id, pc.capability_key, pc.status, pc.reason
     FROM provider_capabilities pc
     JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
     WHERE wpc.workspace_id = ? AND wpc.provider = 'youtube'
     ORDER BY pc.capability_key`,
    [workspaceId]
  );
  const otherWorkspaceRows = await connection.query(
    `SELECT pr.provider_resource_id, COUNT(DISTINCT wpc.workspace_id) AS workspace_count
     FROM provider_resources pr
     JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
     JOIN workspace_memberships wm
       ON wm.workspace_id = wpc.workspace_id AND wm.user_id = ? AND wm.status = 'active'
     WHERE pr.provider = 'youtube' AND pr.resource_type = 'youtube_channel'
       AND wpc.workspace_id <> ?
     GROUP BY pr.provider_resource_id`,
    [userId, workspaceId]
  );
  const otherWorkspaceCount = new Map(
    otherWorkspaceRows.map(row => [String(row.provider_resource_id), Number(row.workspace_count || 0)])
  );
  const capabilitiesByConnection = new Map();
  for (const capability of capabilityRows) {
    const values = capabilitiesByConnection.get(capability.workspace_provider_connection_id) || [];
    values.push({ key: capability.capability_key, status: capability.status, reason: capability.reason });
    capabilitiesByConnection.set(capability.workspace_provider_connection_id, values);
  }
  const resources = resourceRows.map(row => {
    const metadata = parseJson(row.metadata);
    return {
      id: row.resource_id,
      provider_resource_id: row.provider_resource_id,
      display_name: row.display_name,
      thumbnail_url: metadata.thumbnailUrl || null,
      subscriber_count_hidden: Boolean(metadata.subscriberCountHidden),
      attached_elsewhere_count: otherWorkspaceCount.get(String(row.provider_resource_id)) || 0,
      selected: Boolean(row.connection_id)
    };
  });
  const connections = resourceRows.filter(row => row.connection_id).map(row => {
    const metadata = parseJson(row.metadata);
    const authorizationConnectionStatus = authorization && authorization.status === 'authorizing'
      ? 'connecting'
      : authorization && ['reconnect_required', 'disabled'].includes(authorization.status)
        ? 'reconnect_required'
        : row.connection_status;
    return {
      id: row.connection_id,
      data_source_id: row.data_source_id,
      status: authorizationConnectionStatus,
      reconnect_reason: row.reconnect_reason,
      last_sync_at: row.last_sync_at,
      last_successful_sync_at: row.last_successful_sync_at,
      next_sync_at: row.next_sync_at,
      data_through_at: row.data_through_at,
      account: {
        id: row.provider_resource_id,
        username: row.username,
        display_name: row.display_name,
        thumbnail_url: metadata.thumbnailUrl || null
      },
      capabilities: capabilitiesByConnection.get(row.connection_id) || []
    };
  });
  const primaryConnection = connections[0] || null;
  let status = configuration.status;
  if (!configuration.connectable) status = configuration.status;
  else if (authorization && authorization.status === 'authorizing') status = 'authorizing';
  else if (
    authorization &&
    authorization.status !== 'active' &&
    authorization.failure_category === 'missing_required_scopes'
  ) status = 'missing_scopes';
  else if (
    authorization &&
    authorization.status !== 'active' &&
    authorization.failure_category === 'user_denied'
  ) status = 'authorization_denied';
  else if (authorization && authorization.status === 'disabled') status = 'provider_error';
  else if (authorization && authorization.status === 'reconnect_required') status = 'reconnect_required';
  else if (primaryConnection) status = primaryConnection.status;
  else if (authorization && authorization.status === 'active') status = resources.length > 0 ? 'selection_required' : 'no_channels';
  else if (configuration.connectable) status = 'available';
  const grantedScopeNames = new Set(scopeRows.filter(scope => scope.status === 'granted').map(scope => scope.scope));
  const missingScopes = provider.requestedScopes
    .map(scope => scope.name)
    .filter(scope => !grantedScopeNames.has(scope));
  return {
    ...publicProvider,
    enabled: configuration.enabled,
    implemented: true,
    connectable: configuration.connectable,
    status,
    configuration: { status: configuration.status, warnings: configuration.warnings },
    authorization: authorization ? {
      id: authorization.id,
      status: authorization.status,
      granted_at: authorization.granted_at,
      last_validated_at: authorization.last_validated_at,
      failure_category: authorization.failure_category,
      missing_scopes: missingScopes,
      scopes: scopeRows
    } : null,
    resources,
    connections,
    connection: primaryConnection
  };
}

async function listWorkspaceProviderCatalog(userId, workspaceId, env = process.env) {
  const connection = await getConnection();
  if (!connection) {
    const error = new Error('database_not_configured');
    error.status = 503;
    error.code = 'database_not_configured';
    throw error;
  }
  try {
    const memberRows = await connection.query(
      `SELECT role FROM workspace_memberships
       WHERE workspace_id = ? AND user_id = ? AND status = 'active'
       LIMIT 1`,
      [workspaceId, userId]
    );
    const membership = memberRows[0] || null;
    if (!membership) {
      const error = new Error('workspace_not_found');
      error.status = 404;
      error.code = 'workspace_not_found';
      throw error;
    }
    assertCapability(membership.role, 'viewDashboard');
    const sourceRows = await connection.query(
      `SELECT ds.provider,
              ds.status,
              ds.reconnect_reason,
              ds.last_sync_at,
              ds.last_successful_sync_at,
              ds.next_sync_at,
              pa.provider_account_id,
              pa.username,
              pa.display_name
       FROM data_sources ds
       LEFT JOIN provider_accounts pa ON pa.data_source_id = ds.id
       WHERE ds.workspace_id = ? AND ds.deleted_at IS NULL`,
      [workspaceId]
    );
    const sourceByProvider = new Map(sourceRows.map(row => [row.provider, row]));
    const youtubeProvider = await getYouTubeWorkspaceProvider(connection, workspaceId, userId, env);
    return PROVIDERS.map(provider => provider.id === 'youtube'
      ? youtubeProvider
      : toWorkspaceProvider(provider, sourceByProvider.get(provider.id), env));
  } finally {
    await connection.release();
  }
}

function getMetricDefinitions() {
  return { ...METRIC_DEFINITIONS };
}

module.exports = {
  PROVIDERS,
  getMetricDefinitions,
  getProviderCatalog,
  getPublicProviderCatalog,
  listWorkspaceProviderCatalog
};
