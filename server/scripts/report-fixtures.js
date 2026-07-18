const PROVIDERS = Object.freeze([
  { id: 'tiktok', name: 'TikTok', resource: '@northstarstudio', colorMetric: 'Followers' },
  { id: 'youtube', name: 'YouTube', resource: 'Northstar Studio', colorMetric: 'Views' },
  { id: 'facebook_pages', name: 'Facebook Pages', resource: 'Northstar Studio Page', colorMetric: 'Page follows' },
  { id: 'instagram', name: 'Instagram', resource: '@northstar.studio', colorMetric: 'Reach' },
  { id: 'google_analytics_4', name: 'Website Analytics', resource: 'Northstar Website', colorMetric: 'Active users' }
]);

const PROVIDER_RESOURCE_IDS = Object.freeze({
  tiktok: 'tt_account_fixture_01',
  youtube: 'UC_fixture_channel_01',
  facebook_pages: 'fb_page_fixture_01',
  instagram: 'ig_account_fixture_01',
  google_analytics_4: 'properties/123456789'
});

function fixtureUuid(index) {
  return `10000000-0000-4000-8000-${String(index).padStart(12, '0')}`;
}

function trendPoints(providerIndex) {
  return Array.from({ length: 14 }, (_, index) => ({
    date: `2026-06-${String(17 + index).padStart(2, '0')}`,
    values: { primary: 800 + providerIndex * 200 + index * (18 + providerIndex) }
  }));
}

function sourceFixture(provider, index, overrides = {}) {
  const primaryValue = 1250 + index * 1840;
  return {
    id: `${provider.id}:${fixtureUuid(index + 1)}`,
    provider: provider.id,
    provider_name: provider.name,
    status: 'active',
    resource: {
      connection_id: fixtureUuid(index + 1),
      id: PROVIDER_RESOURCE_IDS[provider.id],
      display_name: provider.resource,
      account_name: provider.id === 'google_analytics_4' ? 'Northstar Digital' : null,
      timezone: provider.id === 'google_analytics_4' ? 'America/New_York' : null
    },
    range: {
      key: '30d',
      from: '2026-06-01',
      to: '2026-06-30',
      previous_from: '2026-05-02',
      previous_to: '2026-05-31'
    },
    has_data: true,
    demo_data: true,
    freshness: {
      state: 'sample',
      last_successful_sync_at: '2026-07-01T08:30:00.000Z',
      data_through_date: provider.id === 'instagram' ? '2026-06-29' : '2026-06-30',
      next_sync_at: '2026-07-01T14:30:00.000Z'
    },
    metrics: [
      {
        key: 'primary',
        label: provider.colorMetric,
        family: 'provider_metric',
        unit: 'count',
        value: primaryValue,
        baseline: primaryValue - 120,
        delta: 120,
        percent_change: 10.6,
        available: true,
        availability_status: 'available',
        definition: `${provider.colorMetric} reported for the selected resource and provider reporting period.`,
        definition_version: '2026-07-18'
      },
      {
        key: 'engagement',
        label: provider.id === 'google_analytics_4' ? 'Engagement rate' : 'Provider engagement',
        family: 'provider_engagement',
        unit: provider.id === 'google_analytics_4' ? 'ratio' : 'count',
        value: provider.id === 'google_analytics_4' ? 0.624 : 420 + index * 95,
        baseline: provider.id === 'google_analytics_4' ? 0.59 : 390 + index * 85,
        delta: null,
        percent_change: 5.8,
        available: true,
        availability_status: 'available',
        definition: 'Provider-specific engagement measure retained without cross-provider aggregation.',
        definition_version: '2026-07-18'
      },
      {
        key: 'unavailable_example',
        label: 'Additional metric',
        family: 'provider_metric',
        unit: 'count',
        value: null,
        baseline: null,
        delta: null,
        percent_change: null,
        available: false,
        availability_status: index === 4 ? 'thresholded' : 'not_reported',
        availability_reason: index === 4 ? 'Privacy thresholding applied' : 'Not reported for this resource',
        definition: 'An optional provider metric that may be unavailable.',
        definition_version: '2026-07-18'
      }
    ],
    trend: {
      series: [{ key: 'primary', label: provider.colorMetric, unit: 'count' }],
      points: trendPoints(index)
    },
    top_content: Array.from({ length: 3 }, (_, contentIndex) => ({
      id: `${provider.id}-content-${contentIndex + 1}`,
      kind: provider.id === 'google_analytics_4' ? 'website_path' : 'social_content',
      title: provider.id === 'google_analytics_4'
        ? `/insights/campaign-${contentIndex + 1}`
        : `Campaign story ${contentIndex + 1}: audience highlights and creative performance`,
      published_at: provider.id === 'google_analytics_4' ? null : `2026-06-${String(26 - contentIndex).padStart(2, '0')}T12:00:00.000Z`,
      share_url: null,
      primary_metric: {
        key: provider.id === 'google_analytics_4' ? 'ga4.sessions' : 'views',
        label: provider.id === 'google_analytics_4' ? 'Sessions' : 'Views',
        unit: 'count',
        value: 2400 - contentIndex * 310 + index * 120
      }
    })),
    availability: {
      state: index === 4 ? 'thresholded' : 'ready',
      note: index === 4 ? 'Some low-volume website rows can be withheld by privacy thresholds.' : null,
      subject_to_thresholding: index === 4
    },
    alert: index === 4 ? {
      severity: 'warning',
      code: 'privacy_thresholding',
      message: 'Northstar Website contains privacy-thresholded website analytics.'
    } : null,
    ...overrides
  };
}

function createFixtureSnapshot(options = {}) {
  const requestedProviders = options.providers || PROVIDERS.map(provider => provider.id);
  const sources = PROVIDERS
    .filter(provider => requestedProviders.includes(provider.id))
    .map((provider, index) => sourceFixture(provider, PROVIDERS.findIndex(item => item.id === provider.id)));
  if (options.noContentProvider) {
    const target = sources.find(source => source.provider === options.noContentProvider);
    if (target) target.top_content = [];
  }
  if (options.missingMetricsProvider) {
    const target = sources.find(source => source.provider === options.missingMetricsProvider);
    if (target) {
      target.metrics = target.metrics.map(metric => ({
        ...metric,
        value: null,
        baseline: null,
        delta: null,
        percent_change: null,
        available: false,
        availability_status: 'not_reported',
        availability_reason: 'Fixture exercises an explicitly missing provider value'
      }));
    }
  }
  const title = options.title || (sources.length > 1 ? 'Cross-platform performance report' : `${sources[0].provider_name} performance report`);
  return {
    snapshot_version: '1',
    renderer_version: 'pdfkit-v1',
    captured_at: '2026-07-01T09:00:00.000Z',
    workspace: { id: fixtureUuid(99), name: 'Northstar Studio' },
    report: {
      title,
      subtitle: options.subtitle || 'Read-only analytics prepared for the monthly performance review',
      timezone: 'America/New_York',
      range: {
        key: '30d',
        from: '2026-06-01',
        to: '2026-06-30',
        previous_from: '2026-05-02',
        previous_to: '2026-05-31'
      },
      comparison_enabled: true,
      sections: ['executive_summary', 'cross_platform_summary', 'resource_sections', 'methodology']
    },
    dashboard: {
      range: { key: '30d', from: '2026-06-01', to: '2026-06-30' },
      state: 'partial',
      summary: {
        connected_resources: sources.length,
        resources_with_data: sources.filter(source => source.has_data).length,
        attention_count: sources.filter(source => source.alert).length
      },
      sources,
      alerts: sources.filter(source => source.alert).map(source => ({ source_id: source.id, ...source.alert })),
      methodology: [
        'Provider metrics are shown side by side and are never summed into a universal total.',
        'Each metric keeps its provider definition, unit, selected resource, reporting range, and availability state.',
        'Previous-period changes are shown only when a matching stored baseline exists.',
        'Delayed, missing, partial, and privacy-thresholded values remain explicitly unavailable.'
      ]
    },
    resources: sources.map(source => ({
      connection_id: source.resource.connection_id,
      provider: source.provider,
      provider_resource_id: source.resource.id,
      resource_name: source.resource.display_name,
      data_through_at: `${source.freshness.data_through_date}T23:59:59.000Z`
    })),
    metric_definitions: {}
  };
}

module.exports = {
  PROVIDERS,
  createFixtureSnapshot,
  fixtureUuid,
  sourceFixture
};
