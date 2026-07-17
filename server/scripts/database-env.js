const path = require('path');
const dotenv = require('dotenv');

const ROOT_DIR = path.resolve(__dirname, '..', '..');
dotenv.config({ path: path.join(ROOT_DIR, '.env') });
if (String(process.env.NODE_ENV || '').toLowerCase() !== 'production') {
  dotenv.config({ path: path.join(ROOT_DIR, '.env.database.local') });
}

function getDatabaseUrl(target = 'dev') {
  const url = target === 'test'
    ? process.env.DATABASE_TEST_URL || process.env.TEST_DATABASE_URL
    : process.env.DATABASE_URL;
  return normalizeMariaDbUrl(url);
}

function normalizeMariaDbUrl(databaseUrl) {
  if (!databaseUrl) {
    return databaseUrl;
  }
  if (databaseUrl.startsWith('mysql://')) {
    return `mariadb://${databaseUrl.slice('mysql://'.length)}`;
  }
  return databaseUrl;
}

function parseArgs(argv) {
  const args = { _: [] };
  for (let index = 2; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--database') {
      args.database = argv[index + 1];
      index += 1;
    } else if (value.startsWith('--database=')) {
      args.database = value.slice('--database='.length);
    } else {
      args._.push(value);
    }
  }
  return args;
}

function assertLocalDatabaseUrl(databaseUrl) {
  const parsed = new URL(databaseUrl);
  const localHosts = new Set(['127.0.0.1', 'localhost', '::1']);
  const hostname = parsed.hostname.replace(/^\[|\]$/g, '');
  if (!localHosts.has(hostname)) {
    throw new Error('Refusing to reset a non-local database host.');
  }
  if (parsed.port && parsed.port !== '3307') {
    throw new Error('Refusing to reset a database outside the local development port.');
  }
}

function assertNotProductionCommand(commandName) {
  if (String(process.env.NODE_ENV || '').toLowerCase() === 'production') {
    throw new Error(`Refusing to run ${commandName} in production.`);
  }
}

module.exports = {
  ROOT_DIR,
  assertLocalDatabaseUrl,
  assertNotProductionCommand,
  getDatabaseUrl,
  normalizeMariaDbUrl,
  parseArgs
};
