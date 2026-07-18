#!/usr/bin/env node
const fs = require('fs/promises');
const path = require('path');
const { renderReportPdf } = require('../platform/report-renderer');
const { PROVIDERS, createFixtureSnapshot } = require('./report-fixtures');

const outputDirectory = path.resolve(__dirname, '..', '..', 'output', 'pdf');
const generatedAt = new Date('2026-07-01T09:00:00.000Z');

async function generate(name, snapshot) {
  const outputPath = path.join(outputDirectory, name);
  await fs.rm(outputPath, { force: true });
  const result = await renderReportPdf({
    snapshot,
    outputPath,
    limits: { maxPages: 80, maxContentRowsPerResource: 30 },
    now: generatedAt
  });
  const stat = await fs.stat(outputPath);
  return { path: outputPath, page_count: result.pageCount, byte_size: stat.size };
}

async function main() {
  await fs.mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  const samples = [];
  samples.push(await generate('all-platform-report.pdf', createFixtureSnapshot()));
  samples.push(await generate('tiktok-no-content-report.pdf', createFixtureSnapshot({
    providers: ['tiktok'],
    noContentProvider: 'tiktok'
  })));
  samples.push(await generate('missing-metric-report.pdf', createFixtureSnapshot({
    providers: ['instagram'],
    missingMetricsProvider: 'instagram'
  })));
  samples.push(await generate('long-title-report.pdf', createFixtureSnapshot({
    providers: ['facebook_pages'],
    title: 'A deliberately long but bounded executive analytics report title that verifies safe wrapping across the cover without clipping, overlap, broken pagination, or unreadable report metadata'
  })));
  const longContent = createFixtureSnapshot({ providers: ['tiktok'] });
  longContent.dashboard.sources[0].top_content = Array.from({ length: 35 }, (_, index) => ({
    id: `long-content-${index + 1}`,
    kind: 'social_content',
    title: `Extended content row ${index + 1}: pagination fixture with a deliberately descriptive title`,
    published_at: `2026-06-${String(30 - (index % 29)).padStart(2, '0')}T12:00:00.000Z`,
    share_url: 'https://untrusted.example/content',
    primary_metric: { key: 'views', label: 'Views', unit: 'count', value: 5000 - index * 73 }
  }));
  samples.push(await generate('long-content-report.pdf', longContent));
  for (const provider of PROVIDERS) {
    samples.push(await generate(`${provider.id.replace(/_/g, '-')}-report.pdf`, createFixtureSnapshot({
      providers: [provider.id]
    })));
  }
  console.log(JSON.stringify({ samples }, null, 2));
}

main().catch(error => {
  console.error(JSON.stringify({ error: error.code || error.message || 'sample_generation_failed' }));
  process.exitCode = 1;
});
