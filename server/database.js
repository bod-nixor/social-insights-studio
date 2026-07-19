const path = require('path');
const dotenv = require('dotenv');
const mariadb = require('mariadb');
const { normalizeMariaDbUrl } = require('./scripts/database-env');

if (process.env.NODE_ENV !== 'production') {
  dotenv.config({ path: path.resolve(__dirname, '..', '.env.database.local') });
}

let pool = null;

function getDatabaseUrl() {
  return normalizeMariaDbUrl(process.env.DATABASE_URL);
}

function getPool() {
  const databaseUrl = getDatabaseUrl();
  if (!databaseUrl) {
    return null;
  }
  if (!pool) {
    pool = mariadb.createPool(databaseUrl);
  }
  return pool;
}

async function getConnection() {
  const currentPool = getPool();
  if (!currentPool) {
    return null;
  }
  const connection = await currentPool.getConnection();
  try {
    await connection.query("SET time_zone = '+00:00'");
    return connection;
  } catch (error) {
    connection.release();
    throw error;
  }
}

async function closePool() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

module.exports = {
  closePool,
  getConnection,
  getDatabaseUrl,
  getPool
};
