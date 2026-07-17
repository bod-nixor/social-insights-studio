const mariadb = require('mariadb');
const { assertLocalDatabaseUrl, getDatabaseUrl } = require('./database-env');

async function main() {
  const databaseUrl = getDatabaseUrl('dev');
  const testDatabaseUrl = getDatabaseUrl('test');
  if (!databaseUrl || !testDatabaseUrl) {
    throw new Error('DATABASE_URL and DATABASE_TEST_URL are required.');
  }
  assertLocalDatabaseUrl(databaseUrl);
  assertLocalDatabaseUrl(testDatabaseUrl);

  const rootPassword = process.env.MARIADB_ROOT_PASSWORD;
  const appUser = process.env.MARIADB_USER;
  if (!rootPassword || !appUser) {
    throw new Error('MARIADB_ROOT_PASSWORD and MARIADB_USER are required for local reset.');
  }

  const rootUrl = new URL(databaseUrl);
  rootUrl.username = 'root';
  rootUrl.password = rootPassword;
  rootUrl.pathname = '/';

  const connection = await mariadb.createConnection(rootUrl.toString());
  try {
    await connection.query('DROP DATABASE IF EXISTS social_insights_dev');
    await connection.query('DROP DATABASE IF EXISTS social_insights_test');
    await connection.query('CREATE DATABASE social_insights_dev CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci');
    await connection.query('CREATE DATABASE social_insights_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci');
    await connection.query('GRANT ALL PRIVILEGES ON social_insights_dev.* TO ?@?', [appUser, '%']);
    await connection.query('GRANT ALL PRIVILEGES ON social_insights_test.* TO ?@?', [appUser, '%']);
    await connection.query('FLUSH PRIVILEGES');
    console.log('Reset local development and test databases.');
  } finally {
    await connection.end();
  }
}

main().catch(error => {
  console.error(error.message);
  process.exit(1);
});
