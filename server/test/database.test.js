const { after, before, test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const http = require('node:http');
const path = require('node:path');
const dotenv = require('dotenv');
const jwt = require('jsonwebtoken');
const mariadb = require('mariadb');

dotenv.config({ path: path.resolve(__dirname, '..', '..', '.env.database.local') });

process.env.NODE_ENV = 'test';
process.env.AUTH_DEV_MAGIC_LINKS = 'true';
process.env.DATABASE_URL = process.env.DATABASE_TEST_URL;
process.env.BASE_URL = 'http://localhost:3001';
process.env.TIKTOK_CLIENT_KEY = 'test-client-key';
process.env.TIKTOK_CLIENT_SECRET = 'test-client-secret';
process.env.BACKEND_JWT_SECRET = 'b'.repeat(64);
process.env.ENCRYPTION_KEY = '2'.repeat(64);
process.env.LOOKER_CLIENT_ID = 'looker-studio-connector';
delete process.env.LOOKER_CLIENT_SECRET;
process.env.LOOKER_REDIRECT_URIS = 'https://script.google.com/macros/d/abc123/usercallback';

const { app, stopStores } = require('../index');
const { closePool } = require('../database');
const { setGoogleOidcFetchImplementation } = require('../platform/google-oidc');
const { setTikTokFetchImplementation, TIKTOK_SCOPES } = require('../integrations/tiktok');
const { setMailTransportFactory } = require('../platform/mail');
const { compareMetric, engagementRate } = require('../platform/analytics');
const { assertCapability, hasCapability, canAssignRole } = require('../platform/rbac');
const { runDueSyncs } = require('../platform/sync-service');
const { safeCsvCell } = require('../platform/export-service');
const { decryptSecret, encryptSecret, parsePreviousKeys } = require('../platform/secret-envelope');
const {
  assertLocalDatabaseUrl,
  assertNotProductionCommand,
  getDatabaseUrl,
  normalizeMariaDbUrl,
  parseArgs
} = require('../scripts/database-env');

let db;

before(async () => {
  db = await mariadb.createConnection(process.env.DATABASE_TEST_URL);
});

after(async () => {
  setGoogleOidcFetchImplementation(null);
  setTikTokFetchImplementation(null);
  setMailTransportFactory(null);
  stopStores();
  await closePool();
  if (db) await db.end();
});

async function clearDatabase() {
  const rows = await db.query(
    `SELECT TABLE_NAME AS table_name
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME <> 'schema_migrations'`
  );
  await db.query('SET FOREIGN_KEY_CHECKS = 0');
  for (const row of rows) {
    await db.query(`DELETE FROM \`${row.table_name}\``);
  }
  await db.query('SET FOREIGN_KEY_CHECKS = 1');
}

function splitSqlStatements(sql) {
  return sql
    .split(/;\s*(?:\r?\n|$)/)
    .map(statement => statement.trim())
    .filter(Boolean);
}

async function applyMigrationFile(connection, fileName) {
  const sql = await fs.readFile(path.resolve(__dirname, '..', 'migrations', fileName), 'utf8');
  for (const statement of splitSqlStatements(sql)) {
    await connection.query(statement);
  }
}

function databaseUrlFor(databaseUrl, databaseName, credentials = {}) {
  const parsed = new URL(databaseUrl);
  parsed.pathname = `/${databaseName}`;
  if (credentials.username) parsed.username = credentials.username;
  if (credentials.password !== undefined) parsed.password = credentials.password;
  return parsed.toString();
}

function requestApp(pathname, options = {}) {
  const method = options.method || 'GET';
  const headers = { ...(options.headers || {}) };
  let body = options.body || null;
  if (body && typeof body !== 'string') {
    body = JSON.stringify(body);
    headers['content-type'] = 'application/json';
  }

  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      const req = http.request({
        hostname: '127.0.0.1',
        port,
        path: pathname,
        method,
        headers: body
          ? { 'content-length': Buffer.byteLength(body), ...headers }
          : headers
      }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          server.close(() => {
            const responseBody = Buffer.concat(chunks).toString('utf8');
            resolve({
              statusCode: res.statusCode,
              headers: res.headers,
              body: responseBody,
              json: () => JSON.parse(responseBody)
            });
          });
        });
      });
      req.on('error', error => server.close(() => reject(error)));
      if (body) req.write(body);
      req.end();
    });
  });
}

function mergeCookies(jar, setCookieHeaders) {
  const headers = Array.isArray(setCookieHeaders) ? setCookieHeaders : [setCookieHeaders].filter(Boolean);
  for (const header of headers) {
    const [pair] = header.split(';');
    const index = pair.indexOf('=');
    jar[pair.slice(0, index)] = pair.slice(index + 1);
  }
}

function cookieHeader(jar) {
  return Object.entries(jar).map(([key, value]) => `${key}=${value}`).join('; ');
}

async function signIn(email) {
  const request = await requestApp('/api/auth/magic-link/request', {
    method: 'POST',
    body: { email }
  });
  assert.equal(request.statusCode, 200);
  const token = request.json().dev_token;
  assert.ok(token);

  const verify = await requestApp('/api/auth/magic-link/verify', {
    method: 'POST',
    body: { token }
  });
  assert.equal(verify.statusCode, 200);
  const jar = {};
  mergeCookies(jar, verify.headers['set-cookie']);
  const body = verify.json();
  return { body, csrf: body.csrf_token, cookies: jar, user: body.user };
}

async function createWorkspace(session, name = 'Workspace') {
  const response = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(session.cookies),
      'x-csrf-token': session.csrf
    },
    body: { name }
  });
  assert.equal(response.statusCode, 201);
  return response.json().workspace;
}

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body)
  };
}

function installTikTokQueue(handlers) {
  setTikTokFetchImplementation(async (url, options = {}) => {
    assert.ok(handlers.length > 0, `unexpected TikTok call to ${url}`);
    return handlers.shift()(String(url), options);
  });
}

test('RBAC role matrix matches Phase 1 policy', () => {
  assert.equal(hasCapability('viewer', 'viewDashboard'), true);
  assert.equal(hasCapability('viewer', 'triggerManualSync'), false);
  assert.equal(hasCapability('analyst', 'exportCsv'), true);
  assert.equal(hasCapability('analyst', 'manageConnection'), false);
  assert.equal(hasCapability('admin', 'manageMembers'), true);
  assert.equal(hasCapability('admin', 'deleteWorkspace'), false);
  assert.equal(hasCapability('owner', 'deleteWorkspace'), true);
  assert.equal(canAssignRole('admin', 'owner'), false);
  assert.equal(canAssignRole('owner', 'owner'), true);
});

test('RBAC helpers fail closed for unknown roles, capabilities, and invalid assignments', () => {
  assert.equal(hasCapability('ghost', 'viewDashboard'), false);
  assert.equal(hasCapability('owner', 'unknownCapability'), false);
  assert.doesNotThrow(() => assertCapability('analyst', 'exportCsv'));
  assert.throws(() => assertCapability('viewer', 'manageMembers'), error => {
    assert.equal(error.status, 403);
    assert.equal(error.code, 'permission_denied');
    return true;
  });
  assert.equal(canAssignRole('viewer', 'viewer'), false);
  assert.equal(canAssignRole('owner', 'superadmin'), false);
});

test('database command helpers normalize MariaDB URLs and refuse unsafe reset targets', () => {
  const previousEnv = {
    databaseUrl: process.env.DATABASE_URL,
    databaseTestUrl: process.env.DATABASE_TEST_URL,
    testDatabaseUrl: process.env.TEST_DATABASE_URL
  };
  try {
    process.env.DATABASE_URL = 'mysql://local_user:local_password@127.0.0.1:3307/social_insights_dev';
    process.env.DATABASE_TEST_URL = 'mariadb://test_user:test_password@localhost:3307/social_insights_test';
    delete process.env.TEST_DATABASE_URL;

    assert.equal(
      getDatabaseUrl('dev'),
      'mariadb://local_user:local_password@127.0.0.1:3307/social_insights_dev'
    );
    assert.equal(
      getDatabaseUrl('test'),
      'mariadb://test_user:test_password@localhost:3307/social_insights_test'
    );
    assert.equal(normalizeMariaDbUrl('postgres://example.invalid/db'), 'postgres://example.invalid/db');
    assert.deepEqual(parseArgs(['node', 'script', 'up', '--database', 'test', '--dry-run', '--database=dev']), {
      _: ['up', '--dry-run'],
      database: 'dev'
    });

    delete process.env.DATABASE_URL;
    delete process.env.DATABASE_TEST_URL;
    process.env.TEST_DATABASE_URL = 'mysql://fallback_user:fallback_password@127.0.0.1:3307/social_insights_fallback';
    assert.equal(getDatabaseUrl('dev'), undefined);
    assert.equal(
      getDatabaseUrl('test'),
      'mariadb://fallback_user:fallback_password@127.0.0.1:3307/social_insights_fallback'
    );

    assert.doesNotThrow(() => assertLocalDatabaseUrl('mariadb://user:password@localhost:3307/social_insights_dev'));
    assert.doesNotThrow(() => assertLocalDatabaseUrl('mariadb://user:password@[::1]:3307/social_insights_dev'));
    assert.throws(
      () => assertLocalDatabaseUrl('mariadb://user:password@db.example.com:3307/social_insights_dev'),
      /non-local database host/
    );
    assert.throws(
      () => assertLocalDatabaseUrl('mariadb://user:password@127.0.0.1:3306/social_insights_dev'),
      /outside the local development port/
    );
  } finally {
    if (previousEnv.databaseUrl === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = previousEnv.databaseUrl;
    if (previousEnv.databaseTestUrl === undefined) delete process.env.DATABASE_TEST_URL;
    else process.env.DATABASE_TEST_URL = previousEnv.databaseTestUrl;
    if (previousEnv.testDatabaseUrl === undefined) delete process.env.TEST_DATABASE_URL;
    else process.env.TEST_DATABASE_URL = previousEnv.testDatabaseUrl;
  }
});

test('database-backed service wrappers fail closed when the pool is unavailable', async () => {
  const databasePath = require.resolve('../database');
  const connectionServicePath = require.resolve('../platform/connection-service');
  const servicesPath = require.resolve('../platform/services');
  const originalDatabaseExports = require(databasePath);
  const originalConnectionServiceCache = require.cache[connectionServicePath];
  const originalServicesCache = require.cache[servicesPath];
  try {
    require.cache[databasePath].exports = {
      ...originalDatabaseExports,
      getConnection: async () => null
    };
    delete require.cache[connectionServicePath];
    delete require.cache[servicesPath];
    const isolatedConnectionService = require('../platform/connection-service');
    const isolatedServices = require('../platform/services');

    await assert.rejects(
      () => isolatedConnectionService.disconnectTikTok('user-id', 'workspace-id'),
      error => {
        assert.equal(error.status, 503);
        assert.equal(error.code, 'database_not_configured');
        return true;
      }
    );
    await assert.rejects(
      () => isolatedServices.listWorkspaces('user-id'),
      error => {
        assert.equal(error.status, 503);
        assert.equal(error.code, 'database_not_configured');
        return true;
      }
    );
  } finally {
    require.cache[databasePath].exports = originalDatabaseExports;
    if (originalConnectionServiceCache) {
      require.cache[connectionServicePath] = originalConnectionServiceCache;
    } else {
      delete require.cache[connectionServicePath];
    }
    if (originalServicesCache) {
      require.cache[servicesPath] = originalServicesCache;
    } else {
      delete require.cache[servicesPath];
    }
  }
});

test('TikTok connection start rolls back and releases the connection when a write fails', async () => {
  const databasePath = require.resolve('../database');
  const connectionServicePath = require.resolve('../platform/connection-service');
  const originalDatabaseExports = require(databasePath);
  const originalConnectionServiceCache = require.cache[connectionServicePath];
  const calls = [];
  const fakeConnection = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes('SELECT role FROM workspace_memberships')) {
        return [{ role: 'owner' }];
      }
      if (sql.includes('SELECT * FROM data_sources')) {
        return [];
      }
      if (sql.includes('INSERT INTO data_sources')) {
        return {};
      }
      if (sql.includes('INSERT INTO oauth_transactions')) {
        throw new Error('insert_failed');
      }
      return {};
    },
    async beginTransaction() {
      calls.push('begin');
    },
    async commit() {
      calls.push('commit');
    },
    async rollback() {
      calls.push('rollback');
    },
    async release() {
      calls.push('release');
    }
  };
  try {
    require.cache[databasePath].exports = {
      ...originalDatabaseExports,
      getConnection: async () => fakeConnection
    };
    delete require.cache[connectionServicePath];
    const isolatedConnectionService = require('../platform/connection-service');
    await assert.rejects(
      () => isolatedConnectionService.startTikTokConnection('user-id', 'workspace-id', '/'),
      /insert_failed/
    );
    assert.ok(calls.includes('begin'));
    assert.ok(calls.includes('rollback'));
    assert.ok(calls.includes('release'));
    assert.equal(calls.includes('commit'), false);
  } finally {
    require.cache[databasePath].exports = originalDatabaseExports;
    if (originalConnectionServiceCache) {
      require.cache[connectionServicePath] = originalConnectionServiceCache;
    } else {
      delete require.cache[connectionServicePath];
    }
  }
});

test('TikTok disconnect rolls back and releases the connection when local revocation writes fail', async () => {
  const databasePath = require.resolve('../database');
  const connectionServicePath = require.resolve('../platform/connection-service');
  const originalDatabaseExports = require(databasePath);
  const originalConnectionServiceCache = require.cache[connectionServicePath];
  const calls = [];
  const fakeConnection = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes('SELECT role FROM workspace_memberships')) {
        return [{ role: 'owner' }];
      }
      if (sql.includes('SELECT ds.id AS data_source_id')) {
        return [{
          data_source_id: 'data-source-id',
          access_token_ciphertext: null,
          access_token_iv: null,
          access_token_tag: null,
          key_version: null,
          revoked_at: null
        }];
      }
      if (sql.includes('UPDATE oauth_credentials SET revoked_at')) {
        throw new Error('local_revoke_failed');
      }
      return {};
    },
    async beginTransaction() {
      calls.push('begin');
    },
    async commit() {
      calls.push('commit');
    },
    async rollback() {
      calls.push('rollback');
    },
    async release() {
      calls.push('release');
    }
  };
  try {
    require.cache[databasePath].exports = {
      ...originalDatabaseExports,
      getConnection: async () => fakeConnection
    };
    delete require.cache[connectionServicePath];
    const isolatedConnectionService = require('../platform/connection-service');
    await assert.rejects(
      () => isolatedConnectionService.disconnectTikTok('user-id', 'workspace-id'),
      /local_revoke_failed/
    );
    assert.ok(calls.includes('begin'));
    assert.ok(calls.includes('rollback'));
    assert.ok(calls.includes('release'));
    assert.equal(calls.includes('commit'), false);
  } finally {
    require.cache[databasePath].exports = originalDatabaseExports;
    if (originalConnectionServiceCache) {
      require.cache[connectionServicePath] = originalConnectionServiceCache;
    } else {
      delete require.cache[connectionServicePath];
    }
  }
});

test('migrations are applied to the real MariaDB test database', async () => {
  const rows = await db.query('SELECT version FROM schema_migrations ORDER BY version');
  assert.deepEqual(rows.map(row => row.version), [
    '001_phase1_foundation',
    '002_session_csrf',
    '003_provider_authorization_resources'
  ]);

  const tableRows = await db.query(
    `SELECT TABLE_NAME AS table_name FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (
       'users',
       'workspaces',
       'oauth_credentials',
       'profile_snapshots',
       'provider_authorizations',
       'provider_authorization_credentials',
       'provider_resources',
       'workspace_provider_connections'
     )`
  );
  assert.equal(tableRows.length, 8);
});

test('provider foundation migration preserves existing TikTok credentials without token rewrite', async () => {
  const databaseName = `sis_migration_${process.pid}_${Date.now()}`.replace(/[^a-zA-Z0-9_]/g, '_');
  const rootPassword = process.env.MARIADB_ROOT_PASSWORD;
  const appUser = process.env.MARIADB_USER || new URL(process.env.DATABASE_TEST_URL).username;
  assert.ok(rootPassword, 'MARIADB_ROOT_PASSWORD is required for the migration preservation test');
  assert.match(appUser, /^[A-Za-z0-9_]+$/);

  const root = await mariadb.createConnection(databaseUrlFor(process.env.DATABASE_TEST_URL, 'mysql', {
    username: 'root',
    password: rootPassword
  }));
  let shadow;
  try {
    await root.query(`CREATE DATABASE \`${databaseName}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`);
    await root.query(`GRANT ALL PRIVILEGES ON \`${databaseName}\`.* TO '${appUser}'@'%'`);
    shadow = await mariadb.createConnection(databaseUrlFor(process.env.DATABASE_TEST_URL, databaseName));

    await applyMigrationFile(shadow, '001_phase1_foundation.sql');
    await applyMigrationFile(shadow, '002_session_csrf.sql');
    await shadow.query(
      `INSERT INTO users (id, email) VALUES
       ('01000000-0000-4000-8000-000000000001', 'migration-owner@example.com')`
    );
    await shadow.query(
      `INSERT INTO workspaces (id, name, slug, created_by) VALUES
       ('11000000-0000-4000-8000-000000000001', 'Migration Workspace', 'migration-workspace', '01000000-0000-4000-8000-000000000001')`
    );
    await shadow.query(
      `INSERT INTO data_sources
        (id, workspace_id, provider, status, last_sync_at, last_successful_sync_at, next_sync_at)
       VALUES
        ('21000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000001', 'tiktok', 'active',
         '2026-07-17 01:02:03.000', '2026-07-17 01:02:03.000', '2026-07-17 07:02:03.000')`
    );
    await shadow.query(
      `INSERT INTO provider_accounts
        (id, workspace_id, data_source_id, provider, provider_account_id, username, display_name)
       VALUES
        ('22000000-0000-4000-8000-000000000001', '11000000-0000-4000-8000-000000000001',
         '21000000-0000-4000-8000-000000000001', 'tiktok', 'legacy-open-id', 'legacyuser', 'Legacy User')`
    );
    await shadow.query(
      `INSERT INTO oauth_credentials
        (id, data_source_id, access_token_ciphertext, access_token_iv, access_token_tag,
         refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version,
         token_type, access_expires_at, refresh_expires_at)
       VALUES
        ('23000000-0000-4000-8000-000000000001', '21000000-0000-4000-8000-000000000001',
         'legacy-access-ciphertext', 'legacy-access-iv', 'legacy-access-tag',
         'legacy-refresh-ciphertext', 'legacy-refresh-iv', 'legacy-refresh-tag',
         'legacy-key-v3', 'Bearer', '2026-07-17 02:02:03.000', '2026-07-18 01:02:03.000')`
    );
    await shadow.query(
      `INSERT INTO provider_scopes (data_source_id, scope, status, granted_at, last_confirmed_at)
       VALUES
        ('21000000-0000-4000-8000-000000000001', 'user.info.basic', 'granted', '2026-07-17 01:02:03.000', '2026-07-17 01:02:03.000'),
        ('21000000-0000-4000-8000-000000000001', 'video.list', 'granted', '2026-07-17 01:02:03.000', '2026-07-17 01:02:03.000')`
    );
    await shadow.query(
      `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
       VALUES ('24000000-0000-4000-8000-000000000001', '21000000-0000-4000-8000-000000000001', '2026-07-17 07:02:03.000', 'due')`
    );

    await applyMigrationFile(shadow, '003_provider_authorization_resources.sql');

    const rows = await shadow.query(
      `SELECT pauth.source_data_source_id,
              pauth.provider_subject,
              pac.access_token_ciphertext,
              pac.refresh_token_ciphertext,
              pac.key_version,
              pr.provider_resource_id,
              wpc.data_source_id,
              wpc.status
       FROM provider_authorizations pauth
       JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
       JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
       JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
       WHERE pauth.source_data_source_id = '21000000-0000-4000-8000-000000000001'`
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].provider_subject, 'legacy-open-id');
    assert.equal(rows[0].access_token_ciphertext, 'legacy-access-ciphertext');
    assert.equal(rows[0].refresh_token_ciphertext, 'legacy-refresh-ciphertext');
    assert.equal(rows[0].key_version, 'legacy-key-v3');
    assert.equal(rows[0].provider_resource_id, 'legacy-open-id');
    assert.equal(rows[0].data_source_id, '21000000-0000-4000-8000-000000000001');
    assert.equal(rows[0].status, 'active');

    const scopeRows = await shadow.query(
      `SELECT scope, status FROM provider_authorization_scopes ORDER BY scope`
    );
    assert.deepEqual(scopeRows.map(row => `${row.scope}:${row.status}`), [
      'user.info.basic:granted',
      'video.list:granted'
    ]);
    const capabilityRows = await shadow.query('SELECT COUNT(*) AS count FROM provider_capabilities');
    const syncRows = await shadow.query('SELECT COUNT(*) AS count FROM provider_sync_states');
    assert.equal(Number(capabilityRows[0].count), 5);
    assert.equal(Number(syncRows[0].count), 1);
  } finally {
    if (shadow) await shadow.end();
    await root.query(`DROP DATABASE IF EXISTS \`${databaseName}\``);
    await root.end();
  }
});

test('schema constraints and nullable metrics behave on MariaDB', async () => {
  await clearDatabase();
  await assert.rejects(
    () => db.query(
      `INSERT INTO users (id, email, status) VALUES
       ('00000000-0000-4000-8000-000000000001', 'bad@example.com', 'sleeping')`
    ),
    /constraint|CONSTRAINT|check/i
  );

  await db.query(
    `INSERT INTO users (id, email) VALUES
     ('00000000-0000-4000-8000-000000000002', 'owner@example.com')`
  );
  await db.query(
    `INSERT INTO workspaces (id, name, slug, created_by) VALUES
     ('10000000-0000-4000-8000-000000000001', 'Acme', 'acme', '00000000-0000-4000-8000-000000000002')`
  );
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status) VALUES
     ('20000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'tiktok', 'active')`
  );
  await db.query(
    `INSERT INTO sync_runs (id, workspace_id, data_source_id, trigger_type, status) VALUES
     ('30000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '20000000-0000-4000-8000-000000000001', 'manual', 'success')`
  );
  await db.query(
    `INSERT INTO profile_snapshots
      (id, workspace_id, data_source_id, sync_run_id, observed_at, follower_count)
     VALUES
      ('40000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', '20000000-0000-4000-8000-000000000001', '30000000-0000-4000-8000-000000000001', UTC_TIMESTAMP(3), NULL)`
  );
  const snapshots = await db.query('SELECT follower_count FROM profile_snapshots');
  assert.equal(snapshots[0].follower_count, null);
});

test('magic-link session, CSRF, workspace, cross-workspace, and role checks work through HTTP', async () => {
  await clearDatabase();
  const unauthenticatedSession = await requestApp('/api/session');
  assert.equal(unauthenticatedSession.statusCode, 401);

  const invalidEmail = await requestApp('/api/auth/magic-link/request', {
    method: 'POST',
    body: { email: 'not-an-email' }
  });
  assert.equal(invalidEmail.statusCode, 400);
  assert.equal(invalidEmail.json().error, 'invalid_email');

  const invalidToken = await requestApp('/api/auth/magic-link/verify', {
    method: 'POST',
    body: { token: 'not-a-real-token' }
  });
  assert.equal(invalidToken.statusCode, 400);
  assert.equal(invalidToken.json().error, 'invalid_or_expired_token');

  const missingToken = await requestApp('/api/auth/magic-link/verify', {
    method: 'POST',
    body: {}
  });
  assert.equal(missingToken.statusCode, 400);
  assert.equal(missingToken.json().error, 'invalid_token');

  for (let index = 0; index < 5; index += 1) {
    const limitedRequest = await requestApp('/api/auth/magic-link/request', {
      method: 'POST',
      body: { email: 'limited@example.com' }
    });
    assert.equal(limitedRequest.statusCode, 200);
  }
  const rateLimited = await requestApp('/api/auth/magic-link/request', {
    method: 'POST',
    body: { email: 'limited@example.com' }
  });
  assert.equal(rateLimited.statusCode, 429);
  assert.equal(rateLimited.json().error, 'rate_limited');

  const owner = await signIn('owner@example.com');

  const noCsrf = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies) },
    body: { name: 'Owner Workspace' }
  });
  assert.equal(noCsrf.statusCode, 403);
  assert.equal(noCsrf.json().error, 'csrf_required');

  const tamperedCsrfJar = { ...owner.cookies, sis_csrf: 'forged-csrf-token' };
  const tamperedCsrf = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(tamperedCsrfJar),
      'x-csrf-token': 'forged-csrf-token'
    },
    body: { name: 'Owner Workspace' }
  });
  assert.equal(tamperedCsrf.statusCode, 403);
  assert.equal(tamperedCsrf.json().error, 'csrf_invalid');

  const invalidWorkspace = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { name: '   ' }
  });
  assert.equal(invalidWorkspace.statusCode, 400);
  assert.equal(invalidWorkspace.json().error, 'invalid_workspace_name');

  const created = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { name: 'Owner Workspace' }
  });
  assert.equal(created.statusCode, 201);
  const ownerWorkspaceId = created.json().workspace.id;

  const workspaces = await requestApp('/api/workspaces', {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(workspaces.statusCode, 200);
  assert.equal(workspaces.json().workspaces[0].role, 'owner');

  const resumed = await requestApp('/api/session', {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(resumed.statusCode, 200);
  assert.equal(resumed.json().user.email, 'owner@example.com');

  const signOut = await requestApp('/api/sign-out', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(signOut.statusCode, 200);
  const afterSignOut = await requestApp('/api/session', {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(afterSignOut.statusCode, 401);

  const ownerAgain = await signIn('owner@example.com');

  const viewer = await signIn('viewer@example.com');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'viewer', 'active')`,
    [ownerWorkspaceId, viewer.user.id]
  );

  const deniedInvite = await requestApp(`/api/workspaces/${ownerWorkspaceId}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { email: 'new@example.com', role: 'analyst' }
  });
  assert.equal(deniedInvite.statusCode, 403);
  assert.equal(deniedInvite.json().error, 'permission_denied');

  const viewerWorkspace = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { name: 'Viewer Private Workspace' }
  });
  assert.equal(viewerWorkspace.statusCode, 201);

  const crossWorkspace = await requestApp(`/api/workspaces/${viewerWorkspace.json().workspace.id}/members`, {
    headers: { cookie: cookieHeader(ownerAgain.cookies) }
  });
  assert.equal(crossWorkspace.statusCode, 404);
  assert.equal(crossWorkspace.json().error, 'workspace_not_found');
});

test('member invitations, role changes, admin limits, and last-owner protection work through HTTP', async () => {
  await clearDatabase();
  const owner = await signIn('member-owner@example.com');
  const admin = await signIn('member-admin@example.com');
  const viewer = await signIn('member-viewer@example.com');
  const workspace = await createWorkspace(owner, 'Members Workspace');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'admin', 'active'), (?, ?, 'viewer', 'active')`,
    [workspace.id, admin.user.id, workspace.id, viewer.user.id]
  );

  const invite = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { email: 'analyst@example.com', role: 'analyst' }
  });
  assert.equal(invite.statusCode, 201);
  assert.equal(invite.json().invited, true);
  assert.ok(invite.json().dev_token);

  const memberList = await requestApp(`/api/workspaces/${workspace.id}/members`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(memberList.statusCode, 200);
  assert.equal(memberList.json().members.length, 3);
  assert.equal(memberList.json().invitations.length, 1);
  assert.equal(memberList.json().invitations[0].email, 'analyst@example.com');
  assert.equal(memberList.json().invitations[0].role, 'analyst');

  const adminCannotInviteOwner = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(admin.cookies),
      'x-csrf-token': admin.csrf
    },
    body: { email: 'owner2@example.com', role: 'owner' }
  });
  assert.equal(adminCannotInviteOwner.statusCode, 403);
  assert.equal(adminCannotInviteOwner.json().error, 'invalid_role_assignment');

  const promoteViewer = await requestApp(`/api/workspaces/${workspace.id}/members/${viewer.user.id}`, {
    method: 'PATCH',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { role: 'analyst' }
  });
  assert.equal(promoteViewer.statusCode, 200);
  assert.equal(promoteViewer.json().updated, true);
  const promoted = await db.query(
    `SELECT role FROM workspace_memberships
     WHERE workspace_id = ? AND user_id = ?`,
    [workspace.id, viewer.user.id]
  );
  assert.equal(promoted[0].role, 'analyst');

  const missingMemberRole = await requestApp('/api/workspaces/' + workspace.id + '/members/00000000-0000-4000-8000-000000000404', {
    method: 'PATCH',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { role: 'viewer' }
  });
  assert.equal(missingMemberRole.statusCode, 404);
  assert.equal(missingMemberRole.json().error, 'member_not_found');

  const adminCannotPromoteOwner = await requestApp(`/api/workspaces/${workspace.id}/members/${viewer.user.id}`, {
    method: 'PATCH',
    headers: {
      cookie: cookieHeader(admin.cookies),
      'x-csrf-token': admin.csrf
    },
    body: { role: 'owner' }
  });
  assert.equal(adminCannotPromoteOwner.statusCode, 403);
  assert.equal(adminCannotPromoteOwner.json().error, 'invalid_role_assignment');

  const ownerCannotDemoteSelf = await requestApp(`/api/workspaces/${workspace.id}/members/${owner.user.id}`, {
    method: 'PATCH',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { role: 'admin' }
  });
  assert.equal(ownerCannotDemoteSelf.statusCode, 400);
  assert.equal(ownerCannotDemoteSelf.json().error, 'last_owner_required');

  const ownerCannotRemoveSelf = await requestApp(`/api/workspaces/${workspace.id}/members/${owner.user.id}`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(ownerCannotRemoveSelf.statusCode, 400);
  assert.equal(ownerCannotRemoveSelf.json().error, 'last_owner_required');

  const missingMemberRemove = await requestApp('/api/workspaces/' + workspace.id + '/members/00000000-0000-4000-8000-000000000404', {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(missingMemberRemove.statusCode, 404);
  assert.equal(missingMemberRemove.json().error, 'member_not_found');

  const removeViewer = await requestApp(`/api/workspaces/${workspace.id}/members/${viewer.user.id}`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(removeViewer.statusCode, 200);
  assert.equal(removeViewer.json().removed, true);
});

test('Google OIDC route fails closed when credentials are unavailable', async () => {
  const stateResponse = await requestApp('/api/auth/google/state');
  assert.equal(stateResponse.statusCode, 503);
  assert.equal(stateResponse.json().error, 'google_oidc_not_configured');

  const response = await requestApp('/api/auth/google', {
    method: 'POST',
    body: { id_token: 'fake', state: 'state', nonce: 'nonce' }
  });
  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error, 'google_oidc_not_configured');
});

test('production-only safety guards refuse development paths', async () => {
  const names = [
    'NODE_ENV',
    'MAIL_ADAPTER',
    'MAIL_FROM',
    'SMTP_HOST',
    'SMTP_PORT',
    'SMTP_SECURE',
    'SMTP_USER',
    'SMTP_PASSWORD'
  ];
  const previous = Object.fromEntries(names.map(name => [name, process.env[name]]));
  try {
    process.env.NODE_ENV = 'production';
    process.env.MAIL_ADAPTER = 'development';
    assert.throws(() => assertNotProductionCommand('db:reset'), /Refusing to run db:reset in production/);
    const response = await requestApp('/api/auth/magic-link/request', {
      method: 'POST',
      body: { email: 'prod@example.com' }
    });
    assert.equal(response.statusCode, 503);
    assert.equal(response.json().error, 'mail_not_configured');

    const sentMessages = [];
    setMailTransportFactory(options => ({
      options,
      sendMail: async message => {
        sentMessages.push({ options, message });
        return { messageId: 'test-message-id' };
      }
    }));
    process.env.MAIL_ADAPTER = 'smtp';
    process.env.MAIL_FROM = 'Social Insights Studio <no-reply@example.com>';
    process.env.SMTP_HOST = 'smtp.example.com';
    process.env.SMTP_PORT = '465';
    process.env.SMTP_SECURE = 'true';
    process.env.SMTP_USER = 'no-reply@example.com';
    process.env.SMTP_PASSWORD = 'smtp-password';

    const smtpResponse = await requestApp('/api/auth/magic-link/request', {
      method: 'POST',
      body: { email: 'prod-smtp@example.com' }
    });
    assert.equal(smtpResponse.statusCode, 200);
    assert.deepEqual(smtpResponse.json(), { sent: true });
    assert.equal(sentMessages.length, 1);
    assert.equal(sentMessages[0].options.host, 'smtp.example.com');
    assert.equal(sentMessages[0].options.secure, true);
    assert.equal(sentMessages[0].message.to, 'prod-smtp@example.com');
    assert.equal(sentMessages[0].message.text.includes('one-time sign-in code'), true);
    assert.match(sentMessages[0].message.text, /Open http:\/\/localhost:3001\/ and paste the code/);
    assert.doesNotMatch(sentMessages[0].message.text, /\/app\//);
  } finally {
    setMailTransportFactory(null);
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  }
});

test('AES-GCM secret envelopes decrypt previous key versions and write the current version', () => {
  const previousEnv = {
    key: process.env.ENCRYPTION_KEY,
    version: process.env.ENCRYPTION_KEY_VERSION,
    previous: process.env.ENCRYPTION_PREVIOUS_KEYS
  };
  try {
    const oldKey = '3'.repeat(64);
    const newKey = '4'.repeat(64);
    process.env.ENCRYPTION_KEY = oldKey;
    process.env.ENCRYPTION_KEY_VERSION = 'old-v1';
    delete process.env.ENCRYPTION_PREVIOUS_KEYS;
    const oldEnvelope = encryptSecret('old-secret');
    assert.equal(oldEnvelope.keyVersion, 'old-v1');

    process.env.ENCRYPTION_KEY = newKey;
    process.env.ENCRYPTION_KEY_VERSION = 'new-v2';
    process.env.ENCRYPTION_PREVIOUS_KEYS = `old-v1:${oldKey}`;
    const newEnvelope = encryptSecret('new-secret');
    assert.equal(newEnvelope.keyVersion, 'new-v2');
    assert.equal(decryptSecret({ ...oldEnvelope, keyVersion: 'old-v1' }), 'old-secret');
    assert.equal(decryptSecret(newEnvelope), 'new-secret');
  } finally {
    process.env.ENCRYPTION_KEY = previousEnv.key;
    if (previousEnv.version === undefined) {
      delete process.env.ENCRYPTION_KEY_VERSION;
    } else {
      process.env.ENCRYPTION_KEY_VERSION = previousEnv.version;
    }
    if (previousEnv.previous === undefined) {
      delete process.env.ENCRYPTION_PREVIOUS_KEYS;
    } else {
      process.env.ENCRYPTION_PREVIOUS_KEYS = previousEnv.previous;
    }
  }
});

test('secret envelopes reject missing, malformed, and unavailable key versions', () => {
  const previousEnv = {
    key: process.env.ENCRYPTION_KEY,
    version: process.env.ENCRYPTION_KEY_VERSION,
    previous: process.env.ENCRYPTION_PREVIOUS_KEYS
  };
  try {
    delete process.env.ENCRYPTION_KEY;
    assert.throws(() => encryptSecret('missing-key'), /Missing ENCRYPTION_KEY/);

    process.env.ENCRYPTION_KEY = 'not-a-valid-key';
    assert.throws(() => encryptSecret('bad-key'), /32 bytes/);

    process.env.ENCRYPTION_KEY = '5'.repeat(64);
    process.env.ENCRYPTION_KEY_VERSION = 'current-v1';
    process.env.ENCRYPTION_PREVIOUS_KEYS = 'malformed-entry';
    assert.throws(() => parsePreviousKeys(), /version:key/);

    process.env.ENCRYPTION_PREVIOUS_KEYS = '';
    assert.throws(
      () => decryptSecret({
        ciphertext: Buffer.from('ciphertext').toString('base64'),
        iv: Buffer.alloc(12).toString('base64'),
        tag: Buffer.alloc(16).toString('base64'),
        keyVersion: 'unknown-v0'
      }),
      /key version is not available/
    );
  } finally {
    if (previousEnv.key === undefined) delete process.env.ENCRYPTION_KEY;
    else process.env.ENCRYPTION_KEY = previousEnv.key;
    if (previousEnv.version === undefined) delete process.env.ENCRYPTION_KEY_VERSION;
    else process.env.ENCRYPTION_KEY_VERSION = previousEnv.version;
    if (previousEnv.previous === undefined) delete process.env.ENCRYPTION_PREVIOUS_KEYS;
    else process.env.ENCRYPTION_PREVIOUS_KEYS = previousEnv.previous;
  }
});

test('Google OIDC verifies state, nonce, issuer, audience, and creates an opaque session', async () => {
  await clearDatabase();
  process.env.GOOGLE_OIDC_CLIENT_ID = 'google-client-id';
  const { publicKey, privateKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
  const jwk = publicKey.export({ format: 'jwk' });
  jwk.kid = 'test-google-key';
  jwk.alg = 'RS256';
  jwk.use = 'sig';
  setGoogleOidcFetchImplementation(async () => jsonResponse(200, { keys: [jwk] }));

  try {
    const invalidRequest = await requestApp('/api/auth/google', {
      method: 'POST',
      body: {}
    });
    assert.equal(invalidRequest.statusCode, 400);
    assert.equal(invalidRequest.json().error, 'invalid_oidc_request');

    const start = await requestApp('/api/auth/google/state');
    assert.equal(start.statusCode, 200);
    const oidcJar = {};
    mergeCookies(oidcJar, start.headers['set-cookie']);
    const stateBody = start.json();
    assert.equal(stateBody.client_id, 'google-client-id');

    const denied = await requestApp('/api/auth/google', {
      method: 'POST',
      headers: { cookie: cookieHeader(oidcJar) },
      body: { id_token: 'not-a-jwt', state: 'wrong-state', nonce: stateBody.nonce }
    });
    assert.equal(denied.statusCode, 403);
    assert.equal(denied.json().error, 'oidc_state_invalid');

    const idToken = jwt.sign({
      iss: 'https://accounts.google.com',
      aud: 'google-client-id',
      sub: 'google-subject-1',
      email: 'google-user@example.com',
      email_verified: true,
      nonce: stateBody.nonce,
      name: 'Google User'
    }, privateKey, {
      algorithm: 'RS256',
      keyid: 'test-google-key',
      expiresIn: '5m'
    });

    const verified = await requestApp('/api/auth/google', {
      method: 'POST',
      headers: { cookie: cookieHeader(oidcJar) },
      body: {
        id_token: idToken,
        state: stateBody.state,
        nonce: stateBody.nonce
      }
    });
    assert.equal(verified.statusCode, 200);
    const appJar = {};
    mergeCookies(appJar, verified.headers['set-cookie']);
    assert.ok(appJar.sis_session);
    assert.ok(verified.json().csrf_token);

    const session = await requestApp('/api/session', {
      headers: { cookie: cookieHeader(appJar) }
    });
    assert.equal(session.statusCode, 200);
    assert.equal(session.json().user.email, 'google-user@example.com');

    const secondStart = await requestApp('/api/auth/google/state');
    assert.equal(secondStart.statusCode, 200);
    const secondJar = {};
    mergeCookies(secondJar, secondStart.headers['set-cookie']);
    const secondState = secondStart.json();
    const secondIdToken = jwt.sign({
      iss: 'https://accounts.google.com',
      aud: 'google-client-id',
      sub: 'google-subject-1',
      email: 'google-user@example.com',
      email_verified: true,
      nonce: secondState.nonce,
      name: 'Renamed Google User'
    }, privateKey, {
      algorithm: 'RS256',
      keyid: 'test-google-key',
      expiresIn: '5m'
    });
    const secondVerified = await requestApp('/api/auth/google', {
      method: 'POST',
      headers: { cookie: cookieHeader(secondJar) },
      body: {
        id_token: secondIdToken,
        state: secondState.state,
        nonce: secondState.nonce
      }
    });
    assert.equal(secondVerified.statusCode, 200);
    assert.equal(secondVerified.json().user.display_name, 'Google User');
  } finally {
    delete process.env.GOOGLE_OIDC_CLIENT_ID;
    setGoogleOidcFetchImplementation(null);
  }
});

test('TikTok connection lifecycle is workspace-bound, encrypted, replay-safe, and auditable', async () => {
  await clearDatabase();
  const owner = await signIn('owner-tiktok@example.com');
  const viewer = await signIn('viewer-tiktok@example.com');
  const outsider = await signIn('outsider-tiktok@example.com');
  const workspace = await createWorkspace(owner, 'TikTok Workspace');
  const otherWorkspace = await createWorkspace(owner, 'Other Workspace');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'viewer', 'active')`,
    [workspace.id, viewer.user.id]
  );

  const openReturn = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: 'https://evil.example/app' }
  });
  assert.equal(openReturn.statusCode, 400);
  assert.equal(openReturn.json().error, 'invalid_return_path');

  const nonCanonicalReturn = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/settings' }
  });
  assert.equal(nonCanonicalReturn.statusCode, 400);
  assert.equal(nonCanonicalReturn.json().error, 'invalid_return_path');

  const outsiderStart = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(outsider.cookies),
      'x-csrf-token': outsider.csrf
    },
    body: { return_path: '/' }
  });
  assert.equal(outsiderStart.statusCode, 404);
  assert.equal(outsiderStart.json().error, 'workspace_not_found');

  const outsiderDisconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(outsider.cookies),
      'x-csrf-token': outsider.csrf
    },
    body: {}
  });
  assert.equal(outsiderDisconnect.statusCode, 404);
  assert.equal(outsiderDisconnect.json().error, 'workspace_not_found');

  const viewerStart = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { return_path: '/' }
  });
  assert.equal(viewerStart.statusCode, 403);

  const calls = [];
  installTikTokQueue([
    (url, options) => {
      calls.push({ type: 'exchange', url, body: String(options.body) });
      assert.match(String(options.body), /code=valid-code/);
      return jsonResponse(200, {
        data: {
          access_token: 'access-token-one',
          refresh_token: 'refresh-token-one',
          open_id: 'open-123',
          scope: TIKTOK_SCOPES.join(','),
          expires_in: 3600,
          refresh_expires_in: 86400,
          token_type: 'Bearer'
        }
      });
    },
    () => {
      calls.push({ type: 'profile' });
      return jsonResponse(200, {
        data: {
          user: {
            open_id: 'open-123',
            union_id: 'union-123',
            username: 'studio',
            display_name: 'Studio Account',
            profile_deep_link: 'https://www.tiktok.com/@studio'
          }
        }
      });
    },
    (url, options) => {
      calls.push({ type: 'revoke', url, body: String(options.body) });
      assert.match(String(options.body), /access-token-one/);
      return jsonResponse(200, { data: {} });
    },
    () => jsonResponse(200, {
      data: {
        access_token: 'access-token-missing',
        refresh_token: 'refresh-token-missing',
        open_id: 'open-123',
        scope: 'user.info.basic',
        expires_in: 3600,
        refresh_expires_in: 86400
      }
    }),
    () => jsonResponse(200, { data: { user: { open_id: 'open-123', username: 'studio' } } })
  ]);

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/app/?view=connections' }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  const state = authorizationUrl.searchParams.get('state');
  assert.ok(state);

  const transactionRows = await db.query('SELECT state_hash, return_path FROM oauth_transactions WHERE workspace_id = ?', [workspace.id]);
  assert.equal(transactionRows.length, 1);
  assert.notEqual(transactionRows[0].state_hash, state);
  assert.equal(transactionRows[0].return_path, '/?view=connections');

  const callback = await requestApp(`/api/integrations/tiktok/callback?code=valid-code&state=${encodeURIComponent(state)}`);
  assert.equal(callback.statusCode, 200);
  assert.match(callback.body, /window\.location\.href="\/\?view=connections"/);

  const replay = await requestApp(`/api/integrations/tiktok/callback?code=valid-code&state=${encodeURIComponent(state)}`);
  assert.equal(replay.statusCode, 400);

  const credentialRows = await db.query(
    `SELECT ds.id AS data_source_id, oc.access_token_ciphertext, oc.revoked_at, ds.status
     FROM oauth_credentials oc
     JOIN data_sources ds ON ds.id = oc.data_source_id
     WHERE ds.workspace_id = ?`,
    [workspace.id]
  );
  assert.equal(credentialRows.length, 1);
  assert.notEqual(credentialRows[0].access_token_ciphertext, 'access-token-one');
  assert.equal(credentialRows[0].status, 'active');
  assert.equal(credentialRows[0].revoked_at, null);

  const providerCatalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(providerCatalog.statusCode, 200);
  const providerRows = providerCatalog.json().providers;
  const tiktokProvider = providerRows.find(provider => provider.id === 'tiktok');
  const youtubeProvider = providerRows.find(provider => provider.id === 'youtube');
  assert.equal(tiktokProvider.connection.status, 'active');
  assert.equal(tiktokProvider.connectable, true);
  assert.equal(youtubeProvider.connectable, false);

  const foundationRows = await db.query(
    `SELECT pauth.source_data_source_id,
            pauth.provider_subject,
            pac.access_token_ciphertext,
            pac.key_version,
            pr.resource_type,
            wpc.status
     FROM provider_authorizations pauth
     JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
     JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'tiktok'`,
    [workspace.id]
  );
  assert.equal(foundationRows.length, 1);
  assert.equal(foundationRows[0].source_data_source_id, credentialRows[0].data_source_id);
  assert.equal(foundationRows[0].provider_subject, 'open-123');
  assert.equal(foundationRows[0].access_token_ciphertext, credentialRows[0].access_token_ciphertext);
  assert.equal(foundationRows[0].key_version, process.env.ENCRYPTION_KEY_VERSION || 'local-v1');
  assert.equal(foundationRows[0].resource_type, 'tiktok_account');
  assert.equal(foundationRows[0].status, 'active');

  const capabilityRows = await db.query(
    `SELECT pc.capability_key, pc.status
     FROM provider_capabilities pc
     JOIN workspace_provider_connections wpc ON wpc.id = pc.workspace_provider_connection_id
     WHERE wpc.workspace_id = ? AND wpc.provider = 'tiktok'
     ORDER BY pc.capability_key`,
    [workspace.id]
  );
  assert.equal(capabilityRows.length, 5);
  assert.equal(capabilityRows.every(row => row.status === 'available'), true);

  const otherAccounts = await db.query('SELECT * FROM provider_accounts WHERE workspace_id = ?', [otherWorkspace.id]);
  assert.equal(otherAccounts.length, 0);

  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(disconnect.statusCode, 200);
  assert.deepEqual(calls.map(call => call.type), ['exchange', 'profile', 'revoke']);

  const disconnectedRows = await db.query(
    `SELECT oc.revoked_at, ds.status, sj.status AS job_status
     FROM oauth_credentials oc
     JOIN data_sources ds ON ds.id = oc.data_source_id
     LEFT JOIN sync_jobs sj ON sj.data_source_id = ds.id
     WHERE ds.workspace_id = ?`,
    [workspace.id]
  );
  assert.ok(disconnectedRows[0].revoked_at);
  assert.equal(disconnectedRows[0].status, 'disconnected');
  assert.equal(disconnectedRows[0].job_status, 'disabled');
  const disconnectedFoundationRows = await db.query(
    `SELECT pauth.status AS auth_status, wpc.status AS connection_status, COUNT(pre.id) AS revocation_count
     FROM provider_authorizations pauth
     JOIN workspace_provider_connections wpc ON wpc.data_source_id = pauth.source_data_source_id
     LEFT JOIN provider_revocation_events pre ON pre.provider_authorization_id = pauth.id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'tiktok'
     GROUP BY pauth.status, wpc.status`,
    [workspace.id]
  );
  assert.equal(disconnectedFoundationRows[0].auth_status, 'disconnected');
  assert.equal(disconnectedFoundationRows[0].connection_status, 'disconnected');
  assert.equal(Number(disconnectedFoundationRows[0].revocation_count) >= 1, true);

  const missingStart = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/' }
  });
  assert.equal(missingStart.statusCode, 200);
  const missingState = new URL(missingStart.json().authorization_url).searchParams.get('state');
  const missingCallback = await requestApp(`/api/integrations/tiktok/callback?code=missing-scope&state=${encodeURIComponent(missingState)}`);
  assert.equal(missingCallback.statusCode, 200);
  const scopeRows = await db.query(
    `SELECT ds.status, ps.scope, ps.status AS scope_status
     FROM data_sources ds
     JOIN provider_scopes ps ON ps.data_source_id = ds.id
     WHERE ds.workspace_id = ? AND ps.status = 'missing'
     ORDER BY ps.scope`,
    [workspace.id]
  );
  assert.equal(scopeRows[0].status, 'reconnect_required');
  assert.ok(scopeRows.some(row => row.scope === 'video.list' && row.scope_status === 'missing'));

  const auditRows = await db.query(
    `SELECT action FROM audit_logs
     WHERE workspace_id = ?
     ORDER BY created_at ASC`,
    [workspace.id]
  );
  assert.ok(auditRows.map(row => row.action).includes('connection.tiktok.disconnected'));
});

test('TikTok disconnect records provider revoke failure but still disables the local connection', async () => {
  await clearDatabase();
  const owner = await signIn('revoke-failure-owner@example.com');
  const workspace = await createWorkspace(owner, 'Revoke Failure Workspace');
  installTikTokQueue([
    () => jsonResponse(200, {
      data: {
        access_token: 'revoke-failure-access',
        refresh_token: 'revoke-failure-refresh',
        open_id: 'revoke-open-id',
        scope: TIKTOK_SCOPES.join(','),
        expires_in: 3600,
        refresh_expires_in: 86400,
        token_type: 'Bearer'
      }
    }),
    () => jsonResponse(200, { data: { user: { open_id: 'revoke-open-id', username: 'revokeacct' } } }),
    () => jsonResponse(500, { error: { code: 'provider_down' } })
  ]);

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/' }
  });
  assert.equal(start.statusCode, 200);
  const state = new URL(start.json().authorization_url).searchParams.get('state');
  const callback = await requestApp(`/api/integrations/tiktok/callback?code=connect&state=${encodeURIComponent(state)}`);
  assert.equal(callback.statusCode, 200);

  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(disconnect.statusCode, 200);
  assert.equal(disconnect.json().disconnected, true);
  assert.equal(disconnect.json().provider_revoke.attempted, true);
  assert.equal(disconnect.json().provider_revoke.success, false);
  assert.equal(disconnect.json().provider_revoke.error.category, 'provider');

  const rows = await db.query(
    `SELECT ds.status, oc.revoked_at, sj.status AS job_status
     FROM data_sources ds
     JOIN oauth_credentials oc ON oc.data_source_id = ds.id
     LEFT JOIN sync_jobs sj ON sj.data_source_id = ds.id
     WHERE ds.workspace_id = ?`,
    [workspace.id]
  );
  assert.equal(rows[0].status, 'disconnected');
  assert.ok(rows[0].revoked_at);
  assert.equal(rows[0].job_status, 'disabled');

  const auditRows = await db.query(
    `SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.provider_revoke.category')) AS category
     FROM audit_logs
     WHERE workspace_id = ? AND action = 'connection.tiktok.disconnected'
     ORDER BY created_at DESC LIMIT 1`,
    [workspace.id]
  );
  assert.equal(auditRows[0].category, 'provider');
});

test('TikTok callback failures and disconnected edge states fail closed without leaking credentials', async () => {
  await clearDatabase();
  const owner = await signIn('callback-failure-owner@example.com');
  const noSourceWorkspace = await createWorkspace(owner, 'No Source Workspace');

  const missingCode = await requestApp('/api/integrations/tiktok/callback?state=missing-code-state');
  assert.equal(missingCode.statusCode, 400);

  const notConnectedDisconnect = await requestApp(`/api/workspaces/${noSourceWorkspace.id}/connections/tiktok`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(notConnectedDisconnect.statusCode, 200);
  assert.deepEqual(notConnectedDisconnect.json(), {
    disconnected: false,
    provider_revoke: { attempted: false, reason: 'not_connected' }
  });

  const tokenFailureWorkspace = await createWorkspace(owner, 'Token Failure Workspace');
  installTikTokQueue([
    () => jsonResponse(401, { error: { code: 'invalid_code' } })
  ]);
  const tokenStart = await requestApp(`/api/workspaces/${tokenFailureWorkspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/' }
  });
  const tokenState = new URL(tokenStart.json().authorization_url).searchParams.get('state');
  const tokenCallback = await requestApp(`/api/integrations/tiktok/callback?code=bad-code&state=${encodeURIComponent(tokenState)}`);
  assert.equal(tokenCallback.statusCode, 400);
  const tokenRows = await db.query(
    `SELECT ot.status AS transaction_status, ds.status AS source_status, ds.reconnect_reason
     FROM oauth_transactions ot
     JOIN data_sources ds ON ds.workspace_id = ot.workspace_id
     WHERE ot.workspace_id = ?`,
    [tokenFailureWorkspace.id]
  );
  assert.equal(tokenRows[0].transaction_status, 'failed');
  assert.equal(tokenRows[0].source_status, 'reconnect_required');
  assert.equal(tokenRows[0].reconnect_reason, 'token_exchange_failed');

  const profileFailureWorkspace = await createWorkspace(owner, 'Profile Failure Workspace');
  installTikTokQueue([
    () => jsonResponse(200, {
      data: {
        access_token: 'profile-failure-access',
        refresh_token: 'profile-failure-refresh',
        open_id: 'expected-open-id',
        scope: TIKTOK_SCOPES.join(','),
        expires_in: 3600,
        refresh_expires_in: 86400,
        token_type: 'Bearer'
      }
    }),
    () => jsonResponse(200, { data: { user: { open_id: 'different-open-id', username: 'mismatch' } } })
  ]);
  const profileStart = await requestApp(`/api/workspaces/${profileFailureWorkspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/' }
  });
  const profileState = new URL(profileStart.json().authorization_url).searchParams.get('state');
  const profileCallback = await requestApp(`/api/integrations/tiktok/callback?code=profile-code&state=${encodeURIComponent(profileState)}`);
  assert.equal(profileCallback.statusCode, 400);
  const profileRows = await db.query(
    `SELECT ot.status AS transaction_status, ds.status AS source_status, ds.reconnect_reason
     FROM oauth_transactions ot
     JOIN data_sources ds ON ds.workspace_id = ot.workspace_id
     WHERE ot.workspace_id = ?`,
    [profileFailureWorkspace.id]
  );
  assert.equal(profileRows[0].transaction_status, 'failed');
  assert.equal(profileRows[0].source_status, 'reconnect_required');
  assert.equal(profileRows[0].reconnect_reason, 'profile_fetch_failed');

  const noCredentialWorkspace = await createWorkspace(owner, 'No Credential Workspace');
  const dataSourceId = '20000000-0000-4000-8000-000000000905';
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'tiktok', 'active')`,
    [dataSourceId, noCredentialWorkspace.id]
  );
  const noCredentialDisconnect = await requestApp(`/api/workspaces/${noCredentialWorkspace.id}/connections/tiktok`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(noCredentialDisconnect.statusCode, 200);
  assert.equal(noCredentialDisconnect.json().disconnected, true);
  assert.deepEqual(noCredentialDisconnect.json().provider_revoke, {
    attempted: false,
    reason: 'credential_not_found'
  });
});

test('dashboard, manual sync, stale partial state, and CSV export use stored snapshots', async () => {
  await clearDatabase();
  const owner = await signIn('dashboard-owner@example.com');
  const viewer = await signIn('dashboard-viewer@example.com');
  const workspace = await createWorkspace(owner, 'Dashboard Workspace');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'viewer', 'active')`,
    [workspace.id, viewer.user.id]
  );

  installTikTokQueue([
    () => jsonResponse(200, {
      data: {
        access_token: 'sync-access-token',
        refresh_token: 'sync-refresh-token',
        open_id: 'sync-open-id',
        scope: TIKTOK_SCOPES.join(','),
        expires_in: 3600,
        refresh_expires_in: 86400
      }
    }),
    () => jsonResponse(200, { data: { user: { open_id: 'sync-open-id', username: 'syncacct' } } }),
    () => jsonResponse(200, {
      data: {
        user: {
          open_id: 'sync-open-id',
          follower_count: 150,
          following_count: 40,
          likes_count: 900,
          video_count: 3
        }
      }
    }),
    () => jsonResponse(200, {
      data: {
        videos: [{
          id: 'video-1',
          create_time: Math.floor(Date.now() / 1000),
          title: '=SUM(1,1)',
          video_description: '=SUM(1,1)',
          share_url: 'https://www.tiktok.com/@syncacct/video/1',
          view_count: 100,
          like_count: 10,
          comment_count: 2,
          share_count: 1
        }],
        cursor: 0,
        has_more: false
      }
    })
  ]);

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/' }
  });
  const state = new URL(start.json().authorization_url).searchParams.get('state');
  const callback = await requestApp(`/api/integrations/tiktok/callback?code=connect&state=${encodeURIComponent(state)}`);
  assert.equal(callback.statusCode, 200);

  const manual = await requestApp(`/api/workspaces/${workspace.id}/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(manual.statusCode, 202);
  assert.equal(manual.json().status, 'success');

  const cooldown = await requestApp(`/api/workspaces/${workspace.id}/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(cooldown.statusCode, 429);
  assert.equal(cooldown.json().error, 'manual_sync_cooldown');

  const deniedManual = await requestApp(`/api/workspaces/${workspace.id}/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: {}
  });
  assert.equal(deniedManual.statusCode, 403);

  const dashboard = await requestApp(`/api/workspaces/${workspace.id}/dashboard?range=7d`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(dashboard.statusCode, 200);
  const dashboardBody = dashboard.json();
  assert.equal(dashboardBody.demo_data, false);
  assert.equal(dashboardBody.connection.status, 'active');
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'follower_count').value, 150);
  assert.equal(dashboardBody.top_content[0].title, '=SUM(1,1)');

  const content = await requestApp(`/api/workspaces/${workspace.id}/content?sort=views`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(content.statusCode, 200);
  const contentBody = content.json();
  assert.equal(contentBody.rows[0].engagement_rate, 13);

  const searchContent = await requestApp(`/api/workspaces/${workspace.id}/content?search=${encodeURIComponent('sum')}&sort=engagement&direction=asc&limit=1&offset=0`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(searchContent.statusCode, 200);
  assert.equal(searchContent.json().total, 1);
  assert.equal(searchContent.json().rows[0].provider_content_id, 'video-1');

  const contentDetail = await requestApp(`/api/workspaces/${workspace.id}/content/${contentBody.rows[0].id}`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(contentDetail.statusCode, 200);
  assert.equal(contentDetail.json().history[0].engagement_rate, 13);
  assert.equal(contentDetail.json().current_metrics.engagement_rate, 13);

  const missingContent = await requestApp(`/api/workspaces/${workspace.id}/content/00000000-0000-4000-8000-000000000999`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(missingContent.statusCode, 404);
  assert.equal(missingContent.json().error, 'content_not_found');

  const otherContentWorkspace = await createWorkspace(owner, 'Other Content Workspace');
  const otherSourceId = '20000000-0000-4000-8000-000000000901';
  const otherContentId = '50000000-0000-4000-8000-000000000901';
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'tiktok', 'disconnected')`,
    [otherSourceId, otherContentWorkspace.id]
  );
  await db.query(
    `INSERT INTO content_items (id, workspace_id, data_source_id, provider_content_id, title)
     VALUES (?, ?, ?, 'foreign-video', 'Foreign content')`,
    [otherContentId, otherContentWorkspace.id, otherSourceId]
  );
  const crossWorkspaceContent = await requestApp(`/api/workspaces/${workspace.id}/content/${otherContentId}`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(crossWorkspaceContent.statusCode, 404);
  assert.equal(crossWorkspaceContent.json().error, 'content_not_found');

  const deniedCsv = await requestApp(`/api/workspaces/${workspace.id}/exports/content.csv`, {
    headers: { cookie: cookieHeader(viewer.cookies) }
  });
  assert.equal(deniedCsv.statusCode, 403);

  const csv = await requestApp(`/api/workspaces/${workspace.id}/exports/content.csv`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(csv.statusCode, 200);
  assert.match(csv.body, /published_at,title,views/);
  assert.ok(csv.body.includes("'=SUM(1,1)"));

  const sourceRows = await db.query('SELECT id, last_successful_sync_at FROM data_sources WHERE workspace_id = ?', [workspace.id]);
  const firstSuccess = sourceRows[0].last_successful_sync_at;
  await db.query(
    `UPDATE sync_jobs SET run_after = UTC_TIMESTAMP(3), status = 'due', lease_owner = NULL, lease_expires_at = NULL
     WHERE data_source_id = ?`,
    [sourceRows[0].id]
  );
  installTikTokQueue([
    () => jsonResponse(200, {
      data: {
        user: {
          open_id: 'sync-open-id',
          follower_count: 155,
          following_count: 40,
          likes_count: 950,
          video_count: 3
        }
      }
    }),
    () => jsonResponse(500, { error: { code: 'provider_down' } })
  ]);
  const due = await runDueSyncs({ timeBudgetSeconds: 5, leaseOwner: 'test-worker' });
  assert.equal(due.processed, 1);
  assert.equal(due.results[0].status, 'partial');

  const afterPartial = await requestApp(`/api/workspaces/${workspace.id}/dashboard?range=7d`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(afterPartial.statusCode, 200);
  const partialBody = afterPartial.json();
  assert.equal(partialBody.latest_sync.status, 'partial');
  assert.equal(partialBody.top_content[0].provider_content_id, 'video-1');
  const sourceAfterPartial = await db.query('SELECT last_successful_sync_at FROM data_sources WHERE id = ?', [sourceRows[0].id]);
  assert.deepEqual(sourceAfterPartial[0].last_successful_sync_at, firstSuccess);
});

test('seeded fixture snapshots are exposed as clearly labeled demo data', async () => {
  await clearDatabase();
  const owner = await signIn('fixture-owner@example.com');
  const workspace = await createWorkspace(owner, 'Fixture Workspace');
  const sourceId = '20000000-0000-4000-8000-000000000501';
  const runId = '30000000-0000-4000-8000-000000000501';
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'tiktok', 'disconnected')`,
    [sourceId, workspace.id]
  );
  await db.query(
    `INSERT INTO sync_runs (id, workspace_id, data_source_id, trigger_type, status, finished_at, profile_count)
     VALUES (?, ?, ?, 'manual', 'success', UTC_TIMESTAMP(3), 1)`,
    [runId, workspace.id, sourceId]
  );
  await db.query(
    `INSERT INTO profile_snapshots
      (id, workspace_id, data_source_id, sync_run_id, observed_at, follower_count, provider_metrics)
     VALUES
      ('40000000-0000-4000-8000-000000000501', ?, ?, ?, UTC_TIMESTAMP(3), 1200, JSON_OBJECT('fixture', TRUE))`,
    [workspace.id, sourceId, runId]
  );

  const dashboard = await requestApp(`/api/workspaces/${workspace.id}/dashboard?range=7d`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(dashboard.statusCode, 200);
  assert.equal(dashboard.json().demo_data, true);
});

test('analytics and CSV safety helpers preserve nulls and avoid fabricated baselines', () => {
  assert.deepEqual(compareMetric(10, null), {
    value: 10,
    baseline: null,
    delta: null,
    percent_change: null
  });
  assert.deepEqual(compareMetric(10, 0), {
    value: 10,
    baseline: 0,
    delta: 10,
    percent_change: null
  });
  assert.equal(engagementRate({ view_count: 0, like_count: 10, comment_count: 1, share_count: 1 }), null);
  assert.equal(engagementRate({ view_count: 100, like_count: 10, comment_count: 1, share_count: 1 }), 12);
  assert.equal(safeCsvCell('=IMPORTXML("https://example.com")'), `"\'=IMPORTXML(""https://example.com"")"`);
  assert.equal(safeCsvCell('+SUM(1,1)'), `"'+SUM(1,1)"`);
  assert.equal(safeCsvCell('-10'), "'-10");
  assert.equal(safeCsvCell('@cmd'), "'@cmd");
  assert.equal(safeCsvCell('\t=SUM(1,1)'), `"'\t=SUM(1,1)"`);
  assert.equal(safeCsvCell('line one\nline two'), '"line one\nline two"');
  assert.equal(safeCsvCell(null), '');
});
