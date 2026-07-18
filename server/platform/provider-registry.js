const { getConnection } = require('../database');
const { assertCapability } = require('./rbac');
const { getYouTubeConfiguration } = require('./youtube-config');
const { getMetaConfiguration } = require('./meta-config');

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
    statusWhenDisabled: 'disabled',
    authModel: 'Facebook Login for Business with the Instagram API and an exact read-only scope set',
    selectedResourceModel: 'one workspace connection per selected professional account',
    requestedScopes: [
      {
        name: 'instagram_basic',
        access: 'read',
        purpose: 'Discover and display authorized professional account identity and media basics.'
      },
      {
        name: 'instagram_manage_insights',
        access: 'read',
        purpose: 'Read implemented account and media insight metrics.'
      },
      {
        name: 'pages_show_list',
        access: 'read',
        purpose: 'Discover Pages available to the authorizing user so linked professional accounts can be selected.'
      },
      {
        name: 'pages_read_engagement',
        access: 'read',
        purpose: 'Read the linked Page relationship and the Page token required by Instagram API with Facebook Login.'
      }
    ],
    capabilities: ['resource_discovery', 'profile_insights', 'media_listing', 'media_insights', 'disconnect'],
    metrics: [
      'instagram.reach',
      'instagram.views',
      'instagram.likes',
      'instagram.comments',
      'instagram.shares',
      'instagram.saves'
    ],
    docs: [
      'https://developers.facebook.com/docs/facebook-login/facebook-login-for-business/',
      'https://developers.facebook.com/documentation/instagram-platform/overview',
      'https://developers.facebook.com/documentation/instagram-platform/api-reference/instagram-user/insights',
      'https://developers.facebook.com/docs/permissions/'
    ]
  },
  {
    id: 'facebook_pages',
    name: 'Facebook Pages',
    resourceName: 'Facebook Page',
    featureFlag: 'FEATURE_FACEBOOK_PAGES_CONNECTOR',
    statusWhenDisabled: 'disabled',
    authModel: 'Facebook Login for Business with exact read-only Pages permissions',
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
      'facebook.page_follows',
      'facebook.page_post_engagements',
      'facebook.page_media_views',
      'facebook.post_reactions',
      'facebook.post_comments',
      'facebook.post_shares'
    ],
    docs: [
      'https://developers.facebook.com/docs/facebook-login/facebook-login-for-business/',
      'https://developers.facebook.com/docs/permissions/',
      'https://developers.facebook.com/documentation/pages-api/manage-pages',
      'https://developers.facebook.com/documentation/pages-api/platforminsights/page'
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
      'youtube.average_view_percentage',
      'youtube.subscribers_gained',
      'youtube.subscribers_lost',
      'youtube.net_subscribers',
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
      'ga4.average_session_duration',
      'ga4.sessions_per_user',
      'ga4.screen_page_views_per_user'
    ],
    docs: [
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/getMetadata',
      'https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/checkCompatibility'
    ]
  }
];

const METRIC_DEFINITION_VERSION = '2026-07-18';

function metric(provider, label, unit, aggregation, dateSemantics, definition, unavailableWhen) {
  return Object.freeze({
    label,
    provider,
    unit,
    aggregation,
    dateSemantics,
    definition,
    unavailableWhen,
    version: METRIC_DEFINITION_VERSION
  });
}

const PROFILE_UNAVAILABLE = 'Required access missing or the provider did not return the profile statistic';
const CONTENT_UNAVAILABLE = 'No eligible content returned or the provider did not report this content statistic';
const META_UNAVAILABLE = 'Required access missing, resource or media type unsupported, or provider data withheld';
const YOUTUBE_UNAVAILABLE = 'Analytics access missing, report incompatible, or data-through date earlier than requested';
const GA4_UNAVAILABLE = 'Metric incompatible with selected dimensions, delayed, or withheld by Google thresholding';

const METRIC_DEFINITIONS = Object.freeze({
  'tiktok.followers': metric('tiktok', 'Followers', 'count', 'latest_snapshot', 'snapshot_at_sync_time', 'Provider-reported follower total at observation time.', PROFILE_UNAVAILABLE),
  'tiktok.following': metric('tiktok', 'Following', 'count', 'latest_snapshot', 'snapshot_at_sync_time', 'Provider-reported accounts-followed total at observation time.', PROFILE_UNAVAILABLE),
  'tiktok.total_likes': metric('tiktok', 'Total likes', 'count', 'latest_snapshot', 'snapshot_at_sync_time', 'Provider-reported lifetime profile likes total at observation time.', PROFILE_UNAVAILABLE),
  'tiktok.video_count': metric('tiktok', 'Videos', 'count', 'latest_snapshot', 'snapshot_at_sync_time', 'Provider-reported published video total at observation time.', PROFILE_UNAVAILABLE),
  'tiktok.video_views': metric('tiktok', 'Video views', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported view total for one video at observation time.', CONTENT_UNAVAILABLE),
  'tiktok.video_likes': metric('tiktok', 'Video likes', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported like total for one video at observation time.', CONTENT_UNAVAILABLE),
  'tiktok.video_comments': metric('tiktok', 'Video comments', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported comment total for one video at observation time.', CONTENT_UNAVAILABLE),
  'tiktok.video_shares': metric('tiktok', 'Video shares', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported share total for one video at observation time.', CONTENT_UNAVAILABLE),

  'instagram.reach': metric('instagram', 'Reach', 'count', 'provider_reported_period', 'meta_insight_period', 'Accounts reached for the eligible account or media and provider-reported period.', META_UNAVAILABLE),
  'instagram.views': metric('instagram', 'Views', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported views for the eligible account or media and period.', META_UNAVAILABLE),
  'instagram.likes': metric('instagram', 'Likes', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported like total for eligible media.', META_UNAVAILABLE),
  'instagram.comments': metric('instagram', 'Comments', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported comment total for eligible media.', META_UNAVAILABLE),
  'instagram.shares': metric('instagram', 'Shares', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported share count for eligible media and period.', META_UNAVAILABLE),
  'instagram.saves': metric('instagram', 'Saves', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported save count for eligible media and period.', META_UNAVAILABLE),

  'facebook.followers': metric('facebook_pages', 'Followers', 'count', 'latest_snapshot', 'meta_insight_snapshot', 'Provider-reported Page follower total at observation time.', META_UNAVAILABLE),
  'facebook.page_follows': metric('facebook_pages', 'Page follows', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported follows for the selected Page and period.', META_UNAVAILABLE),
  'facebook.page_post_engagements': metric('facebook_pages', 'Post engagements', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported Page post engagements for the period.', META_UNAVAILABLE),
  'facebook.page_media_views': metric('facebook_pages', 'Media views', 'count', 'provider_reported_period', 'meta_insight_period', 'Provider-reported eligible Page media views for the period.', META_UNAVAILABLE),
  'facebook.post_reactions': metric('facebook_pages', 'Post reactions', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported reaction total for one Page post.', META_UNAVAILABLE),
  'facebook.post_comments': metric('facebook_pages', 'Post comments', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported comment total for one Page post.', META_UNAVAILABLE),
  'facebook.post_shares': metric('facebook_pages', 'Post shares', 'count', 'latest_content_snapshot', 'content_snapshot_at_sync_time', 'Provider-reported share total for one Page post.', META_UNAVAILABLE),

  'youtube.subscribers': metric('youtube', 'Subscribers', 'count', 'latest_snapshot', 'channel_snapshot_at_sync_time', 'Public channel subscriber total at observation time; it may be hidden by the channel.', YOUTUBE_UNAVAILABLE),
  'youtube.views': metric('youtube', 'Views', 'count', 'sum', 'youtube_analytics_available_date_range', 'Views reported by YouTube Analytics for the selected date range.', YOUTUBE_UNAVAILABLE),
  'youtube.watch_time_minutes': metric('youtube', 'Watch time', 'minutes', 'sum', 'youtube_analytics_available_date_range', 'Estimated minutes watched reported by YouTube Analytics.', YOUTUBE_UNAVAILABLE),
  'youtube.average_view_duration': metric('youtube', 'Average view duration', 'seconds', 'provider_computed', 'youtube_analytics_available_date_range', 'Average seconds watched per view reported by YouTube Analytics.', YOUTUBE_UNAVAILABLE),
  'youtube.average_view_percentage': metric('youtube', 'Average percentage viewed', 'percent', 'provider_computed', 'youtube_analytics_available_date_range', 'Average percentage of a video watched as reported by YouTube Analytics.', YOUTUBE_UNAVAILABLE),
  'youtube.subscribers_gained': metric('youtube', 'Subscribers gained', 'count', 'sum', 'youtube_analytics_available_date_range', 'Subscribers gained during the selected YouTube Analytics range.', YOUTUBE_UNAVAILABLE),
  'youtube.subscribers_lost': metric('youtube', 'Subscribers lost', 'count', 'sum', 'youtube_analytics_available_date_range', 'Subscribers lost during the selected YouTube Analytics range.', YOUTUBE_UNAVAILABLE),
  'youtube.net_subscribers': metric('youtube', 'Net subscribers', 'count', 'derived_difference', 'youtube_analytics_available_date_range', 'Subscribers gained minus subscribers lost for the same range.', YOUTUBE_UNAVAILABLE),
  'youtube.likes': metric('youtube', 'Likes', 'count', 'sum', 'youtube_analytics_available_date_range', 'Likes reported by YouTube Analytics for the selected range.', YOUTUBE_UNAVAILABLE),
  'youtube.comments': metric('youtube', 'Comments', 'count', 'sum', 'youtube_analytics_available_date_range', 'Comments reported by YouTube Analytics for the selected range.', YOUTUBE_UNAVAILABLE),
  'youtube.shares': metric('youtube', 'Shares', 'count', 'sum', 'youtube_analytics_available_date_range', 'Shares reported by YouTube Analytics for the selected range.', YOUTUBE_UNAVAILABLE),

  'ga4.active_users': metric('google_analytics_4', 'Active users', 'count', 'provider_reported_range', 'ga4_property_timezone_date_range', 'Distinct users who engaged with the site or app during the selected GA4 range.', GA4_UNAVAILABLE),
  'ga4.new_users': metric('google_analytics_4', 'New users', 'count', 'provider_reported_range', 'ga4_property_timezone_date_range', 'Users who interacted for the first time during the selected GA4 range.', GA4_UNAVAILABLE),
  'ga4.sessions': metric('google_analytics_4', 'Sessions', 'count', 'sum', 'ga4_property_timezone_date_range', 'Sessions that began on the site or app during the selected range.', GA4_UNAVAILABLE),
  'ga4.screen_page_views': metric('google_analytics_4', 'Views', 'count', 'sum', 'ga4_property_timezone_date_range', 'Page views and screen views reported by GA4, including repeated views.', GA4_UNAVAILABLE),
  'ga4.engagement_rate': metric('google_analytics_4', 'Engagement rate', 'ratio', 'provider_computed', 'ga4_property_timezone_date_range', 'Engaged sessions divided by sessions as reported by GA4.', GA4_UNAVAILABLE),
  'ga4.bounce_rate': metric('google_analytics_4', 'Bounce rate', 'ratio', 'provider_computed', 'ga4_property_timezone_date_range', 'Sessions that were not engaged divided by sessions as reported by GA4.', GA4_UNAVAILABLE),
  'ga4.average_session_duration': metric('google_analytics_4', 'Average session duration', 'seconds', 'provider_computed', 'ga4_property_timezone_date_range', 'Average session duration in seconds as reported by GA4.', GA4_UNAVAILABLE),
  'ga4.sessions_per_user': metric('google_analytics_4', 'Sessions per user', 'ratio', 'provider_computed', 'ga4_property_timezone_date_range', 'Sessions divided by active users as reported by GA4.', GA4_UNAVAILABLE),
  'ga4.screen_page_views_per_user': metric('google_analytics_4', 'Views per user', 'ratio', 'provider_computed', 'ga4_property_timezone_date_range', 'Views divided by active users as reported by GA4.', GA4_UNAVAILABLE)
});

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
  if (provider.id === 'facebook_pages' || provider.id === 'instagram') {
    return getMetaConfiguration(provider.id, env).status;
  }
  return 'feature_flagged';
}

function providerIsImplemented(provider) {
  return ['tiktok', 'youtube', 'facebook_pages', 'instagram'].includes(provider.id);
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
    connectable: provider.id === 'facebook_pages' || provider.id === 'instagram'
      ? getMetaConfiguration(provider.id, env).connectable
      : false,
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

async function getMetaWorkspaceProvider(connection, workspaceId, providerId, env) {
  const provider = PROVIDERS.find(item => item.id === providerId);
  const publicProvider = toPublicProvider(provider, env);
  const tableRows = await connection.query(
    `SELECT COUNT(*) AS count
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME IN (
         'provider_authorizations',
         'provider_authorization_credentials',
         'provider_resources',
         'provider_resource_credentials',
         'workspace_provider_connections',
         'meta_account_insight_snapshots',
         'meta_callback_events',
         'provider_request_events'
       )`
  );
  const columnRows = Number(tableRows[0] && tableRows[0].count) === 8
    ? await connection.query(
        `SELECT COUNT(*) AS count
         FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE()
           AND (
             (TABLE_NAME = 'meta_account_insight_snapshots'
              AND COLUMN_NAME IN ('range_days', 'range_start_date', 'range_end_date'))
             OR (TABLE_NAME = 'oauth_transactions' AND COLUMN_NAME = 'provider_config_id')
           )`
      )
    : [];
  const foundationReady = Number(tableRows[0] && tableRows[0].count) === 8 &&
    Number(columnRows[0] && columnRows[0].count) === 4;
  const configuration = getMetaConfiguration(providerId, env, {
    databaseReady: true,
    foundationReady,
    workerReady: true
  });
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
                AND al.action = CONCAT('connection.', ?, '.authorization_failed')
              ORDER BY al.created_at DESC LIMIT 1
            ) AS failure_category
     FROM provider_authorizations pauth
     WHERE pauth.workspace_id = ? AND pauth.provider = ?
     ORDER BY FIELD(pauth.status, 'active', 'authorizing', 'reconnect_required', 'disabled', 'revoked'),
              pauth.updated_at DESC LIMIT 1`,
    [providerId, workspaceId, providerId]
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
            prc.access_expires_at, prc.revoked_at AS resource_token_revoked_at,
            wpc.id AS connection_id, wpc.data_source_id, wpc.status AS connection_status,
            wpc.last_sync_at, wpc.last_successful_sync_at, wpc.next_sync_at, wpc.data_through_at,
            ds.reconnect_reason, pa.username
     FROM provider_resources pr
     JOIN provider_authorizations pauth ON pauth.id = pr.provider_authorization_id
     LEFT JOIN provider_resource_credentials prc ON prc.provider_resource_id = pr.id
     LEFT JOIN workspace_provider_connections wpc
       ON wpc.provider_resource_id = pr.id AND wpc.workspace_id = pr.workspace_id
     LEFT JOIN data_sources ds ON ds.id = wpc.data_source_id
     LEFT JOIN provider_accounts pa ON pa.data_source_id = wpc.data_source_id
     WHERE pr.workspace_id = ? AND pr.provider = ?
     ORDER BY pr.display_name, pr.created_at`,
    [workspaceId, providerId]
  );
  const capabilityRows = await connection.query(
    `SELECT pc.workspace_provider_connection_id, pc.capability_key, pc.status, pc.reason
     FROM provider_capabilities pc
     JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
     WHERE wpc.workspace_id = ? AND wpc.provider = ? ORDER BY pc.capability_key`,
    [workspaceId, providerId]
  );
  const capabilitiesByConnection = new Map();
  for (const capability of capabilityRows) {
    const values = capabilitiesByConnection.get(capability.workspace_provider_connection_id) || [];
    values.push({ key: capability.capability_key, status: capability.status, reason: capability.reason });
    capabilitiesByConnection.set(capability.workspace_provider_connection_id, values);
  }
  const resources = resourceRows.map(row => {
    const metadata = parseJson(row.metadata);
    const expiryTime = row.access_expires_at ? new Date(row.access_expires_at).getTime() : Number.NaN;
    const tokenAvailable = Boolean(
      !row.resource_token_revoked_at &&
      Number.isFinite(expiryTime) &&
      expiryTime > Date.now()
    );
    const unavailableReason = metadata.selectable === false
      ? metadata.discoveryStatus || 'not_returned'
      : row.resource_token_revoked_at
        ? 'authorization_revoked'
        : !Number.isFinite(expiryTime)
          ? 'token_expiry_unknown'
          : expiryTime <= Date.now()
            ? 'token_expired'
            : null;
    return {
      id: row.resource_id,
      provider_resource_id: row.provider_resource_id,
      display_name: row.display_name,
      username: metadata.username || null,
      thumbnail_url: metadata.thumbnailUrl || null,
      source_page_name: metadata.sourcePageName || null,
      available: metadata.selectable !== false && tokenAvailable,
      unavailable_reason: unavailableReason,
      selected: Boolean(row.connection_id)
    };
  });
  const connections = resourceRows.filter(row => row.connection_id).map(row => {
    const metadata = parseJson(row.metadata);
    const expiryTime = row.access_expires_at ? new Date(row.access_expires_at).getTime() : Number.NaN;
    const credentialUnavailable = Boolean(
      row.resource_token_revoked_at ||
      !Number.isFinite(expiryTime) ||
      expiryTime <= Date.now()
    );
    const status = authorization && authorization.status === 'authorizing'
      ? 'connecting'
      : authorization && ['reconnect_required', 'disabled'].includes(authorization.status)
        ? 'reconnect_required'
        : credentialUnavailable
          ? 'reconnect_required'
          : row.connection_status;
    return {
      id: row.connection_id,
      data_source_id: row.data_source_id,
      status,
      reconnect_reason: row.reconnect_reason || (credentialUnavailable ? 'resource_token_unavailable' : null),
      last_sync_at: row.last_sync_at,
      last_successful_sync_at: row.last_successful_sync_at,
      next_sync_at: row.next_sync_at,
      data_through_at: row.data_through_at,
      account: {
        id: row.provider_resource_id,
        username: row.username || metadata.username || null,
        display_name: row.display_name,
        thumbnail_url: metadata.thumbnailUrl || null,
        source_page_name: metadata.sourcePageName || null
      },
      capabilities: capabilitiesByConnection.get(row.connection_id) || []
    };
  });
  const primaryConnection = connections[0] || null;
  let status = configuration.status;
  if (!configuration.connectable) status = configuration.status;
  else if (authorization && authorization.status === 'authorizing') status = 'authorizing';
  else if (authorization && authorization.status !== 'active' && authorization.failure_category === 'user_denied') {
    status = 'authorization_denied';
  } else if (
    authorization && authorization.status !== 'active' &&
    ['missing_or_unapproved_scopes', 'permission_validation_failed'].includes(authorization.failure_category)
  ) status = 'missing_scopes';
  else if (authorization && authorization.status === 'reconnect_required') status = 'reconnect_required';
  else if (authorization && authorization.status === 'disabled') status = 'provider_error';
  else if (primaryConnection) status = primaryConnection.status;
  else if (authorization && authorization.status === 'active') status = resources.length > 0 ? 'selection_required' : 'no_resources';
  else if (configuration.connectable) status = 'available';
  const grantedScopeNames = new Set(scopeRows.filter(scope => scope.status === 'granted').map(scope => scope.scope));
  const missingScopes = provider.requestedScopes.map(scope => scope.name).filter(scope => !grantedScopeNames.has(scope));
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
    const facebookProvider = await getMetaWorkspaceProvider(connection, workspaceId, 'facebook_pages', env);
    const instagramProvider = await getMetaWorkspaceProvider(connection, workspaceId, 'instagram', env);
    return PROVIDERS.map(provider => {
      if (provider.id === 'youtube') return youtubeProvider;
      if (provider.id === 'facebook_pages') return facebookProvider;
      if (provider.id === 'instagram') return instagramProvider;
      return toWorkspaceProvider(provider, sourceByProvider.get(provider.id), env);
    });
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
