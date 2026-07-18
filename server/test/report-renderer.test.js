const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs/promises');
const os = require('os');
const path = require('path');
const { getReportConfiguration } = require('../platform/report-config');
const { renderReportPdf, safePdfText } = require('../platform/report-renderer');
const { normalizeReportRequest } = require('../platform/report-service');
const { resolveStoragePath, safeDownloadFilename, storageKeyForRun } = require('../platform/report-storage');
const { createFixtureSnapshot, fixtureUuid } = require('../scripts/report-fixtures');

const tempDirectory = path.join(os.tmpdir(), `sis-report-renderer-${process.pid}`);

after(async () => {
  await fs.rm(tempDirectory, { recursive: true, force: true });
});

test('PDF text and storage boundaries remove active links and reject traversal', () => {
  assert.equal(
    safePdfText('Quarterly – report https://evil.example/a file:///etc/passwd\nnext'),
    'Quarterly - report [link removed] [link removed] next'
  );
  assert.equal(safeDownloadFilename('../../Board Report'), 'board-report.pdf');
  assert.equal(
    storageKeyForRun(fixtureUuid(1), fixtureUuid(2)),
    `${fixtureUuid(1)}/${fixtureUuid(2)}/report.pdf`
  );
  assert.throws(() => resolveStoragePath('/tmp/report-root', '../secret.pdf'), /invalid_report_storage_key/);
  assert.throws(() => resolveStoragePath('/tmp/report-root', '/etc/passwd'), /invalid_report_storage_key/);
  assert.throws(() => resolveStoragePath('/tmp/report-root', 'workspace/../../secret'), /invalid_report_storage_key/);
});

test('production report storage rejects public, broad, temporary, and application-owned paths', () => {
  const production = { NODE_ENV: 'production', FEATURE_PDF_REPORTS: 'true' };
  const safe = getReportConfiguration({
    ...production,
    REPORT_ARTIFACT_ROOT: '/srv/social-insights/private/report-artifacts'
  });
  assert.equal(safe.ready, true);
  assert.deepEqual(safe.errors, []);

  const filesystemRoot = getReportConfiguration({ ...production, REPORT_ARTIFACT_ROOT: '/' });
  assert.ok(filesystemRoot.errors.some(error => error.includes('filesystem root')));
  const temporary = getReportConfiguration({
    ...production,
    REPORT_ARTIFACT_ROOT: path.join(os.tmpdir(), 'social-insights-reports')
  });
  assert.ok(temporary.errors.some(error => error.includes('temporary directory')));
  const applicationOwned = getReportConfiguration({
    ...production,
    REPORT_ARTIFACT_ROOT: path.resolve(__dirname, '..', 'data', 'production-reports')
  });
  assert.ok(applicationOwned.errors.some(error => error.includes('application source tree')));
  const publicOwned = getReportConfiguration({
    ...production,
    REPORT_ARTIFACT_ROOT: path.resolve(__dirname, '..', 'public', 'reports')
  });
  assert.ok(publicOwned.errors.some(error => error.includes('public web root')));
});

test('report request validation bounds ranges, resources, timezones, and identifiers', () => {
  const configuration = { maxRangeDays: 366, maxResources: 20 };
  const base = {
    title: 'Bounded report',
    timezone: 'UTC',
    range: 'custom',
    from: '2026-01-01',
    to: '2026-12-31',
    resources: [{ provider: 'tiktok', connection_id: fixtureUuid(1) }]
  };
  assert.equal(normalizeReportRequest(base, configuration).from, '2026-01-01');
  assert.throws(
    () => normalizeReportRequest({ ...base, from: '2025-01-01' }, configuration),
    /invalid_date_range/
  );
  assert.throws(
    () => normalizeReportRequest({
      ...base,
      resources: Array.from({ length: 21 }, (_, index) => ({
        provider: 'tiktok',
        connection_id: fixtureUuid(index + 1)
      }))
    }, configuration),
    /invalid_report_resource_count/
  );
  assert.throws(() => normalizeReportRequest({ ...base, timezone: 'Not/A_Timezone' }, configuration), /invalid_report_timezone/);
  assert.throws(
    () => normalizeReportRequest({ ...base, resources: [{ provider: 'tiktok', connection_id: '../escape' }] }, configuration),
    /invalid_report_connection/
  );
});

test('renderer creates a bounded all-provider PDF and explicit edge-state reports', async () => {
  await fs.mkdir(tempDirectory, { recursive: true });
  const cases = [
    ['all.pdf', createFixtureSnapshot()],
    ['empty.pdf', createFixtureSnapshot({ providers: ['tiktok'], noContentProvider: 'tiktok' })],
    ['missing.pdf', createFixtureSnapshot({ providers: ['instagram'], missingMetricsProvider: 'instagram' })],
    ['long.pdf', createFixtureSnapshot({
      providers: ['facebook_pages'],
      title: 'A deliberately long report title used to verify bounded wrapping and safe pagination without clipping or overlap in the generated analytics artifact'
    })]
  ];
  for (const [name, snapshot] of cases) {
    const outputPath = path.join(tempDirectory, name);
    const result = await renderReportPdf({ snapshot, outputPath, limits: { maxPages: 80, maxContentRowsPerResource: 30 } });
    const content = await fs.readFile(outputPath);
    assert.equal(content.subarray(0, 5).toString(), '%PDF-');
    assert.ok(content.length > 1000);
    assert.ok(result.pageCount >= 4);
    assert.ok(result.pageCount <= 80);
  }
});

test('renderer fails closed on invalid snapshots and page thresholds', async () => {
  await fs.mkdir(tempDirectory, { recursive: true });
  await assert.rejects(
    renderReportPdf({ snapshot: {}, outputPath: path.join(tempDirectory, 'invalid.pdf') }),
    error => error.code === 'invalid_report_snapshot'
  );
  await assert.rejects(
    renderReportPdf({
      snapshot: createFixtureSnapshot(),
      outputPath: path.join(tempDirectory, 'limited.pdf'),
      limits: { maxPages: 2 }
    }),
    error => error.code === 'report_page_limit_exceeded'
  );
});
