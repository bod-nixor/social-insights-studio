const crypto = require('node:crypto');

const PROVIDER_METRIC_PREFIXES = Object.freeze({
  tiktok: 'tiktok.',
  youtube: 'youtube.',
  facebook_pages: 'facebook.',
  instagram: 'instagram.',
  google_analytics_4: 'ga4.'
});

const AVAILABILITY_STATUSES = Object.freeze([
  'available',
  'not_granted',
  'not_supported',
  'not_reported',
  'delayed',
  'thresholded',
  'provider_error'
]);

const GRAINS = Object.freeze(['snapshot', 'daily', 'range', 'lifetime']);

function contractError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function assertIsoDate(value, field) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ''))) {
    throw contractError(`invalid_${field}`);
  }
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== value) {
    throw contractError(`invalid_${field}`);
  }
}

function assertProviderKey(provider, key, field = 'metric_key') {
  const prefix = PROVIDER_METRIC_PREFIXES[provider];
  if (!prefix) throw contractError('unsupported_provider');
  if (!String(key || '').startsWith(prefix) || String(key).length > 160) {
    throw contractError(`invalid_${field}`);
  }
}

function assertPeriod(periodStart, periodEnd) {
  assertIsoDate(periodStart, 'period_start');
  assertIsoDate(periodEnd, 'period_end');
  if (periodStart > periodEnd) throw contractError('invalid_period');
}

function assertAvailability(status, numericValue) {
  if (!AVAILABILITY_STATUSES.includes(status)) throw contractError('invalid_availability_status');
  if (status === 'available') {
    if (typeof numericValue !== 'number' || !Number.isFinite(numericValue)) {
      throw contractError('available_metric_requires_value');
    }
    return;
  }
  if (numericValue !== null) throw contractError('unavailable_metric_must_be_null');
}

function cleanReason(value) {
  const reason = String(value || '').trim();
  return reason ? reason.slice(0, 255) : null;
}

function createMetricObservation(input) {
  const {
    provider,
    metricKey,
    grain,
    periodStart,
    periodEnd,
    numericValue,
    unit,
    availabilityStatus,
    availabilityReason,
    definitionVersion
  } = input || {};
  assertProviderKey(provider, metricKey);
  if (!GRAINS.includes(grain)) throw contractError('invalid_observation_grain');
  assertPeriod(periodStart, periodEnd);
  assertAvailability(availabilityStatus, numericValue);
  if (!String(unit || '').trim() || String(unit).length > 32) throw contractError('invalid_metric_unit');
  if (!String(definitionVersion || '').trim() || String(definitionVersion).length > 64) {
    throw contractError('invalid_definition_version');
  }
  return Object.freeze({
    provider,
    metricKey,
    grain,
    periodStart,
    periodEnd,
    numericValue,
    unit: String(unit),
    availabilityStatus,
    availabilityReason: cleanReason(availabilityReason),
    definitionVersion: String(definitionVersion)
  });
}

function stableObject(value) {
  if (Array.isArray(value)) return value.map(stableObject);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map(key => [key, stableObject(value[key])])
  );
}

function stableDimensionHash(dimensionValues) {
  const canonical = JSON.stringify(stableObject(dimensionValues));
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function createDimensionObservation(input) {
  const {
    provider,
    breakdownKey,
    periodStart,
    periodEnd,
    dimensionValues,
    metrics,
    thresholded = false,
    rowPosition = 0
  } = input || {};
  assertProviderKey(provider, breakdownKey, 'breakdown_key');
  assertPeriod(periodStart, periodEnd);
  if (!dimensionValues || typeof dimensionValues !== 'object' || Array.isArray(dimensionValues)) {
    throw contractError('invalid_dimension_values');
  }
  const dimensionEntries = Object.entries(dimensionValues);
  if (dimensionEntries.length < 1 || dimensionEntries.length > 5) {
    throw contractError('invalid_dimension_count');
  }
  for (const [key, value] of dimensionEntries) {
    if (!key || key.length > 120 || typeof value !== 'string' || value.length > 512) {
      throw contractError('invalid_dimension_value');
    }
  }
  if (!metrics || typeof metrics !== 'object' || Array.isArray(metrics)) {
    throw contractError('invalid_breakdown_metrics');
  }
  const metricEntries = Object.entries(metrics);
  if (metricEntries.length < 1 || metricEntries.length > 20) {
    throw contractError('invalid_breakdown_metric_count');
  }
  const metricValues = {};
  const availability = {};
  for (const [metricKey, metric] of metricEntries) {
    assertProviderKey(provider, metricKey);
    if (!metric || typeof metric !== 'object') throw contractError('invalid_breakdown_metric');
    assertAvailability(metric.status, metric.value);
    metricValues[metricKey] = metric.value;
    availability[metricKey] = {
      status: metric.status,
      reason: cleanReason(metric.reason)
    };
  }
  if (!Number.isInteger(rowPosition) || rowPosition < 0) throw contractError('invalid_row_position');
  const canonicalDimensions = stableObject(dimensionValues);
  return Object.freeze({
    provider,
    breakdownKey,
    periodStart,
    periodEnd,
    dimensionHash: stableDimensionHash(canonicalDimensions),
    dimensionValues: canonicalDimensions,
    metricValues: stableObject(metricValues),
    availability: stableObject(availability),
    thresholded: Boolean(thresholded),
    rowPosition
  });
}

module.exports = {
  AVAILABILITY_STATUSES,
  GRAINS,
  PROVIDER_METRIC_PREFIXES,
  createDimensionObservation,
  createMetricObservation,
  stableDimensionHash
};
