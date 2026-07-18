const originalNodeEnv = process.env.NODE_ENV;

process.env.NODE_ENV = 'test';

const { stopStores, validateRequiredEnv } = require('../index');
const { getDeploymentReadinessCheck } = require('../platform/version');

try {
  validateRequiredEnv({ ...process.env, NODE_ENV: 'production' });
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
