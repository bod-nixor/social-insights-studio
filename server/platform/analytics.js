function toNullableNumber(value) {
  if (value === null || value === undefined) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function engagementRate({ view_count: views, like_count: likes, comment_count: comments, share_count: shares }) {
  const viewCount = toNullableNumber(views);
  if (!viewCount || viewCount <= 0) return null;
  const interactions =
    (toNullableNumber(likes) || 0) +
    (toNullableNumber(comments) || 0) +
    (toNullableNumber(shares) || 0);
  return (interactions / viewCount) * 100;
}

function compareMetric(currentValue, baselineValue) {
  const current = toNullableNumber(currentValue);
  const baseline = toNullableNumber(baselineValue);
  if (current === null) {
    return { value: null, baseline, delta: null, percent_change: null };
  }
  if (baseline === null) {
    return { value: current, baseline: null, delta: null, percent_change: null };
  }
  const delta = current - baseline;
  return {
    value: current,
    baseline,
    delta,
    percent_change: baseline === 0 ? null : (delta / Math.abs(baseline)) * 100
  };
}

function resolveDateRange(input = {}) {
  const now = input.now ? new Date(input.now) : new Date();
  const range = input.range || '30d';
  let days = 30;
  if (range === '7d') days = 7;
  if (range === '90d') days = 90;
  let to = input.to ? new Date(input.to) : now;
  let from = input.from ? new Date(input.from) : new Date(to.getTime() - days * 24 * 60 * 60 * 1000);
  if (!Number.isFinite(from.getTime()) || !Number.isFinite(to.getTime()) || from > to) {
    const error = new Error('invalid_date_range');
    error.status = 400;
    error.code = 'invalid_date_range';
    throw error;
  }
  const maxDays = Number(process.env.DASHBOARD_MAX_RANGE_DAYS || 366);
  if (to.getTime() - from.getTime() > maxDays * 24 * 60 * 60 * 1000) {
    const error = new Error('date_range_too_large');
    error.status = 400;
    error.code = 'date_range_too_large';
    throw error;
  }
  return { from, to };
}

module.exports = {
  compareMetric,
  engagementRate,
  resolveDateRange,
  toNullableNumber
};
