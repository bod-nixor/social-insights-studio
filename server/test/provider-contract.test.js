const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  createDimensionObservation,
  createMetricObservation,
  stableDimensionHash
} = require('../platform/observation-contract');
const {
  REQUIRED_OPERATION_GROUPS,
  assertProviderAdapterContract,
  defineProviderAdapter
} = require('../platform/provider-contract');

function executableAdapter() {
  const operation = async () => ({});
  return {
    provider: 'google_analytics_4',
    contractVersion: 1,
    requiredScopes: ['https://www.googleapis.com/auth/analytics.readonly'],
    resourceTypes: ['ga4_property'],
    capabilities: ['resource_discovery', 'traffic_metrics', 'dimension_breakdowns'],
    authorization: {
      buildAuthorizationUrl: operation,
      completeAuthorization: operation,
      refreshAuthorization: operation,
      inspectScopes: operation,
      revokeAuthorization: operation
    },
    resources: {
      discoverResources: operation,
      selectResource: operation
    },
    sync: { synchronize: operation },
    deletion: { deleteConnectionData: operation }
  };
}

test('executable provider contract requires every authorization, resource, sync, and deletion operation', () => {
  const adapter = executableAdapter();
  assert.equal(assertProviderAdapterContract(adapter), true);
  assert.deepEqual(Object.keys(REQUIRED_OPERATION_GROUPS), ['authorization', 'resources', 'sync', 'deletion']);

  for (const [group, methods] of Object.entries(REQUIRED_OPERATION_GROUPS)) {
    for (const method of methods) {
      const incomplete = executableAdapter();
      delete incomplete[group][method];
      assert.throws(
        () => assertProviderAdapterContract(incomplete),
        new RegExp(`missing_adapter_operation:${group}\\.${method}`)
      );
    }
  }
});

test('provider adapter definition is immutable and rejects malformed metadata', () => {
  const adapter = defineProviderAdapter(executableAdapter());
  assert.equal(Object.isFrozen(adapter), true);
  assert.equal(Object.isFrozen(adapter.authorization), true);
  assert.equal(Object.isFrozen(adapter.requiredScopes), true);

  assert.throws(() => assertProviderAdapterContract(null), /invalid_provider_adapter/);
  assert.throws(
    () => assertProviderAdapterContract({ ...executableAdapter(), provider: '' }),
    /invalid_adapter_provider/
  );
  assert.throws(
    () => assertProviderAdapterContract({ ...executableAdapter(), contractVersion: 2 }),
    /unsupported_adapter_contract_version/
  );
  assert.throws(
    () => assertProviderAdapterContract({ ...executableAdapter(), requiredScopes: [] }),
    /invalid_adapter_required_scopes/
  );
  assert.throws(
    () => assertProviderAdapterContract({ ...executableAdapter(), resources: null }),
    /missing_adapter_group:resources/
  );
});

test('metric observations preserve explicit zeroes and require null for unavailable values', () => {
  const observation = createMetricObservation({
    provider: 'google_analytics_4',
    metricKey: 'ga4.sessions',
    grain: 'daily',
    periodStart: '2026-07-01',
    periodEnd: '2026-07-01',
    numericValue: 0,
    unit: 'count',
    availabilityStatus: 'available',
    definitionVersion: 'ga4-data-api-v1'
  });
  assert.equal(observation.numericValue, 0);
  assert.equal(observation.availabilityReason, null);
  assert.equal(Object.isFrozen(observation), true);

  const thresholded = createMetricObservation({
    provider: 'google_analytics_4',
    metricKey: 'ga4.active_users',
    grain: 'range',
    periodStart: '2026-07-01',
    periodEnd: '2026-07-07',
    numericValue: null,
    unit: 'count',
    availabilityStatus: 'thresholded',
    availabilityReason: 'provider privacy threshold',
    definitionVersion: 'ga4-data-api-v1'
  });
  assert.equal(thresholded.numericValue, null);
  assert.equal(thresholded.availabilityStatus, 'thresholded');

  assert.throws(
    () => createMetricObservation({ ...observation, numericValue: null }),
    /available_metric_requires_value/
  );
  assert.throws(
    () => createMetricObservation({ ...thresholded, numericValue: 1 }),
    /unavailable_metric_must_be_null/
  );
});

test('observation contract rejects cross-provider keys, invalid dates, grains, units, and statuses', () => {
  const valid = {
    provider: 'youtube',
    metricKey: 'youtube.views',
    grain: 'range',
    periodStart: '2026-07-01',
    periodEnd: '2026-07-07',
    numericValue: 10,
    unit: 'count',
    availabilityStatus: 'available',
    definitionVersion: 'youtube-analytics-v2'
  };
  assert.throws(() => createMetricObservation({ ...valid, provider: 'unknown' }), /unsupported_provider/);
  assert.throws(() => createMetricObservation({ ...valid, metricKey: 'ga4.sessions' }), /invalid_metric_key/);
  assert.throws(() => createMetricObservation({ ...valid, grain: 'hourly' }), /invalid_observation_grain/);
  assert.throws(() => createMetricObservation({ ...valid, periodStart: '2026-02-30' }), /invalid_period_start/);
  assert.throws(
    () => createMetricObservation({ ...valid, periodStart: '2026-07-08' }),
    /invalid_period/
  );
  assert.throws(() => createMetricObservation({ ...valid, unit: '' }), /invalid_metric_unit/);
  assert.throws(
    () => createMetricObservation({ ...valid, availabilityStatus: 'maybe' }),
    /invalid_availability_status/
  );
  assert.throws(
    () => createMetricObservation({ ...valid, definitionVersion: '' }),
    /invalid_definition_version/
  );
});

test('dimension observations hash canonical aggregate dimensions and keep per-metric availability', () => {
  const first = createDimensionObservation({
    provider: 'google_analytics_4',
    breakdownKey: 'ga4.session_source_medium',
    periodStart: '2026-07-01',
    periodEnd: '2026-07-07',
    dimensionValues: { sessionMedium: 'organic', sessionSource: 'google' },
    metrics: {
      'ga4.sessions': { value: 12, status: 'available' },
      'ga4.active_users': { value: null, status: 'thresholded', reason: 'privacy threshold' }
    },
    thresholded: true,
    rowPosition: 2
  });
  const secondHash = stableDimensionHash({ sessionSource: 'google', sessionMedium: 'organic' });

  assert.equal(first.dimensionHash, secondHash);
  assert.deepEqual(first.dimensionValues, { sessionMedium: 'organic', sessionSource: 'google' });
  assert.equal(first.metricValues['ga4.sessions'], 12);
  assert.equal(first.metricValues['ga4.active_users'], null);
  assert.equal(first.availability['ga4.active_users'].status, 'thresholded');
  assert.equal(first.thresholded, true);
  assert.equal(first.rowPosition, 2);
});

test('dimension observations reject raw or ambiguous aggregate shapes', () => {
  const valid = {
    provider: 'google_analytics_4',
    breakdownKey: 'ga4.device_category',
    periodStart: '2026-07-01',
    periodEnd: '2026-07-07',
    dimensionValues: { deviceCategory: 'desktop' },
    metrics: { 'ga4.sessions': { value: 12, status: 'available' } }
  };
  assert.throws(
    () => createDimensionObservation({ ...valid, breakdownKey: 'youtube.device' }),
    /invalid_breakdown_key/
  );
  assert.throws(() => createDimensionObservation({ ...valid, dimensionValues: [] }), /invalid_dimension_values/);
  assert.throws(() => createDimensionObservation({ ...valid, dimensionValues: {} }), /invalid_dimension_count/);
  assert.throws(
    () => createDimensionObservation({ ...valid, dimensionValues: { deviceCategory: null } }),
    /invalid_dimension_value/
  );
  assert.throws(() => createDimensionObservation({ ...valid, metrics: {} }), /invalid_breakdown_metric_count/);
  assert.throws(
    () => createDimensionObservation({ ...valid, metrics: { 'youtube.views': { value: 1, status: 'available' } } }),
    /invalid_metric_key/
  );
  assert.throws(
    () => createDimensionObservation({ ...valid, metrics: { 'ga4.sessions': null } }),
    /invalid_breakdown_metric/
  );
  assert.throws(() => createDimensionObservation({ ...valid, rowPosition: -1 }), /invalid_row_position/);
});
