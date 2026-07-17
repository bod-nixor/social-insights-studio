const originalNodeEnv = process.env.NODE_ENV;

process.env.NODE_ENV = 'test';

const { stopStores, validateRequiredEnv } = require('../index');

try {
  validateRequiredEnv({ ...process.env, NODE_ENV: 'production' });
  console.log('Production environment validation passed.');
} finally {
  stopStores();
  if (originalNodeEnv === undefined) {
    delete process.env.NODE_ENV;
  } else {
    process.env.NODE_ENV = originalNodeEnv;
  }
}
