const mariadb = require('mariadb');
const { getDatabaseUrl } = require('./database-env');

const timeoutMs = Number(process.env.DB_WAIT_TIMEOUT_MS || 60000);
const start = Date.now();

async function wait() {
  const databaseUrl = getDatabaseUrl('dev');
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is not configured.');
  }

  while (Date.now() - start < timeoutMs) {
    let connection;
    try {
      connection = await mariadb.createConnection(databaseUrl);
      await connection.query('SELECT 1 AS ready');
      console.log('MariaDB is ready.');
      return;
    } catch (error) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    } finally {
      if (connection) {
        await connection.end();
      }
    }
  }

  throw new Error('Timed out waiting for MariaDB readiness.');
}

wait().catch(error => {
  console.error(error.message);
  process.exit(1);
});
