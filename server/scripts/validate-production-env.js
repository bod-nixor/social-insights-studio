const originalNodeEnv = process.env.NODE_ENV;

process.env.NODE_ENV = 'test';

const { stopStores, validateRequiredEnv } = require('../index');
const { getDeploymentReadinessCheck } = require('../platform/version');
const { getYouTubeProductionErrors } = require('../platform/youtube-config');
const { getMetaProductionErrors } = require('../platform/meta-config');
const { getGoogleAnalyticsProductionErrors } = require('../platform/google-analytics-config');
const { getReportProductionErrors } = require('../platform/report-config');

try {
  validateRequiredEnv({ ...process.env, NODE_ENV: 'production' });
  const youtubeErrors = getYouTubeProductionErrors({ ...process.env, NODE_ENV: 'production' });
  if (youtubeErrors.length > 0) {
    throw new Error(`YouTube production configuration is incomplete: ${youtubeErrors.join(', ')}`);
  }
  const metaErrors = getMetaProductionErrors({ ...process.env, NODE_ENV: 'production' });
  if (metaErrors.length > 0) {
    throw new Error(`Meta production configuration is incomplete: ${metaErrors.join(', ')}`);
  }
  const ga4Errors = getGoogleAnalyticsProductionErrors({ ...process.env, NODE_ENV: 'production' });
  if (ga4Errors.length > 0) {
    throw new Error(`GA4 production configuration is incomplete: ${ga4Errors.join(', ')}`);
  }
  const reportErrors = getReportProductionErrors({ ...process.env, NODE_ENV: 'production' });
  if (reportErrors.length > 0) {
    throw new Error(`PDF report production configuration is incomplete: ${reportErrors.join(', ')}`);
  }
  const deployment = getDeploymentReadinessCheck({ ...process.env, NODE_ENV: 'production' });
  for (const warning of deployment.warnings) {
    console.warn(`Production environment warning: ${warning}`);
  }
  console.log('Production environment validation passed.');
} finally {
  stopStores();
  if (originalNodeEnv === undefined) {
    delete process.env.NODE_ENV;
  } else {
    process.env.NODE_ENV = originalNodeEnv;
  }
}
