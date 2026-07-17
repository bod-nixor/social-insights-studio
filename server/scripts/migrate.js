const crypto = require('crypto');
const fs = require('fs/promises');
const path = require('path');
const mariadb = require('mariadb');
const { ROOT_DIR, getDatabaseUrl, parseArgs } = require('./database-env');

const MIGRATIONS_DIR = path.join(ROOT_DIR, 'server', 'migrations');

function splitSqlStatements(sql) {
  return sql
    .split(/;\s*(?:\r?\n|$)/)
    .map(statement => statement.trim())
    .filter(Boolean);
}

async function ensureMigrationsTable(connection) {
  await connection.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version VARCHAR(64) PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      checksum CHAR(64) NOT NULL,
      applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
}

async function loadMigrations() {
  const files = (await fs.readdir(MIGRATIONS_DIR))
    .filter(file => file.endsWith('.sql'))
    .sort();
  return Promise.all(files.map(async file => {
    const sql = await fs.readFile(path.join(MIGRATIONS_DIR, file), 'utf8');
    return {
      file,
      version: file.replace(/\.sql$/, ''),
      checksum: crypto.createHash('sha256').update(sql).digest('hex'),
      statements: splitSqlStatements(sql)
    };
  }));
}

async function getAppliedMigrations(connection) {
  const rows = await connection.query('SELECT version, checksum FROM schema_migrations ORDER BY version');
  return new Map(rows.map(row => [row.version, row.checksum]));
}

async function migrate(target) {
  const databaseUrl = getDatabaseUrl(target);
  if (!databaseUrl) {
    throw new Error(`${target === 'test' ? 'DATABASE_TEST_URL' : 'DATABASE_URL'} is not configured.`);
  }
  const connection = await mariadb.createConnection(databaseUrl);
  try {
    await ensureMigrationsTable(connection);
    const applied = await getAppliedMigrations(connection);
    const migrations = await loadMigrations();

    for (const migration of migrations) {
      const existingChecksum = applied.get(migration.version);
      if (existingChecksum && existingChecksum !== migration.checksum) {
        throw new Error(`Migration checksum changed after apply: ${migration.version}`);
      }
      if (existingChecksum) {
        continue;
      }
      for (const statement of migration.statements) {
        await connection.query(statement);
      }
      await connection.query(
        'INSERT INTO schema_migrations (version, name, checksum) VALUES (?, ?, ?)',
        [migration.version, migration.file, migration.checksum]
      );
      console.log(`Applied ${migration.file} to ${target}.`);
    }
  } finally {
    await connection.end();
  }
}

async function status(target) {
  const databaseUrl = getDatabaseUrl(target);
  if (!databaseUrl) {
    throw new Error(`${target === 'test' ? 'DATABASE_TEST_URL' : 'DATABASE_URL'} is not configured.`);
  }
  const connection = await mariadb.createConnection(databaseUrl);
  try {
    await ensureMigrationsTable(connection);
    const applied = await getAppliedMigrations(connection);
    const migrations = await loadMigrations();
    for (const migration of migrations) {
      const state = applied.has(migration.version) ? 'applied' : 'pending';
      console.log(`${migration.version} ${state}`);
    }
  } finally {
    await connection.end();
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const command = args._[0] || 'status';
  const target = args.database || 'dev';
  if (!['dev', 'test'].includes(target)) {
    throw new Error('Database target must be dev or test.');
  }
  if (command === 'up') {
    await migrate(target);
  } else if (command === 'status') {
    await status(target);
  } else {
    throw new Error(`Unsupported migration command: ${command}`);
  }
}

main().catch(error => {
  console.error(error.message);
  process.exit(1);
});
