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
process.env.YOUTUBE_ENABLED = 'true';
process.env.YOUTUBE_CLIENT_ID = 'youtube-test-client-id';
process.env.YOUTUBE_CLIENT_SECRET = 'youtube-test-client-secret';
process.env.YOUTUBE_REDIRECT_URI = 'http://localhost:3001/api/integrations/youtube/callback';
process.env.FEATURE_FACEBOOK_PAGES_CONNECTOR = 'true';
process.env.FEATURE_INSTAGRAM_CONNECTOR = 'true';
process.env.META_APP_ID = 'meta-test-app-id';
process.env.META_APP_SECRET = 'meta-test-app-secret';
process.env.META_FACEBOOK_LOGIN_CONFIG_ID = 'meta-test-facebook-login-config-id';
process.env.META_INSTAGRAM_LOGIN_CONFIG_ID = 'meta-test-instagram-login-config-id';
process.env.META_GRAPH_API_VERSION = 'v25.0';
process.env.FACEBOOK_REDIRECT_URI = 'http://localhost:3001/api/integrations/facebook/callback';
process.env.INSTAGRAM_REDIRECT_URI = 'http://localhost:3001/api/integrations/instagram/callback';
process.env.META_FACEBOOK_APPROVED_SCOPES = 'pages_show_list,pages_read_engagement,read_insights';
process.env.META_INSTAGRAM_APPROVED_SCOPES = 'instagram_basic,instagram_manage_insights,pages_show_list,pages_read_engagement';
process.env.FEATURE_GA4_CONNECTOR = 'true';
process.env.GA4_CLIENT_ID = 'ga4-test-client-id';
process.env.GA4_CLIENT_SECRET = 'ga4-test-client-secret';
process.env.GA4_REDIRECT_URI = 'http://localhost:3001/api/integrations/google-analytics/callback';
process.env.BACKEND_JWT_SECRET = 'b'.repeat(64);
process.env.ENCRYPTION_KEY = '2'.repeat(64);
process.env.LOOKER_CLIENT_ID = 'looker-studio-connector';
delete process.env.LOOKER_CLIENT_SECRET;
process.env.LOOKER_REDIRECT_URIS = 'https://script.google.com/macros/d/abc123/usercallback';
process.env.RATE_LIMIT_MAX = '1000';
process.env.API_RATE_LIMIT_MAX = '2000';

const { app, stopStores } = require('../index');
const { closePool } = require('../database');
const { setGoogleOidcFetchImplementation } = require('../platform/google-oidc');
const { setTikTokFetchImplementation, TIKTOK_SCOPES } = require('../integrations/tiktok');
const { setYouTubeTestHooks, YOUTUBE_SCOPES } = require('../integrations/youtube');
const { setMetaTestHooks } = require('../integrations/meta');
const {
  GA4_BREAKDOWNS,
  GA4_METRICS,
  GA4_SCOPES,
  setGoogleAnalyticsTestHooks
} = require('../integrations/google-analytics');
const { sendInvitationEmail, setMailTransportFactory } = require('../platform/mail');
const { compareMetric, engagementRate } = require('../platform/analytics');
const { assertCapability, hasCapability, canAssignRole } = require('../platform/rbac');
const { hashSecret } = require('../platform/security');
const { runDueSyncs } = require('../platform/sync-service');
const { buildDateWindows } = require('../platform/google-analytics-sync-service');
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
  setYouTubeTestHooks();
  setMetaTestHooks();
  setGoogleAnalyticsTestHooks();
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

function installYouTubeQueue(handlers) {
  setYouTubeTestHooks({
    fetch: async (url, options = {}) => {
      assert.ok(handlers.length > 0, `unexpected YouTube call to ${url}`);
      return handlers.shift()(String(url), options);
    },
    sleep: async () => {},
    random: () => 0
  });
  return handlers;
}

function installMetaMock(handler) {
  const calls = [];
  setMetaTestHooks({
    fetch: async (url, options = {}) => {
      const call = { url: new URL(String(url)), options };
      calls.push(call);
      return handler(call, calls);
    },
    sleep: async () => {},
    random: () => 0
  });
  return calls;
}

function installGoogleAnalyticsMock(handler) {
  const calls = [];
  setGoogleAnalyticsTestHooks({
    fetch: async (url, options = {}) => {
      const call = { url: new URL(String(url)), options };
      calls.push(call);
      return handler(call, calls);
    },
    sleep: async () => {},
    random: () => 0
  });
  return calls;
}

function metaSignedRequest(userId, issuedAt = Math.floor(Date.now() / 1000)) {
  const payload = Buffer.from(JSON.stringify({
    algorithm: 'HMAC-SHA256',
    issued_at: issuedAt,
    user_id: userId
  })).toString('base64url');
  const signature = crypto.createHmac('sha256', process.env.META_APP_SECRET).update(payload).digest('base64url');
  return `${signature}.${payload}`;
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

    assert.doesNotThrow(() => assertLocalDatabaseUrl('mariadb://user:password@localhost:3317/social_insights_dev'));
    assert.doesNotThrow(() => assertLocalDatabaseUrl('mariadb://user:password@[::1]:3317/social_insights_dev'));
    assert.doesNotThrow(() => assertLocalDatabaseUrl(
      'mariadb://user:password@localhost:3307/social_insights_dev',
      '3307'
    ));
    assert.throws(
      () => assertLocalDatabaseUrl('mariadb://user:password@db.example.com:3317/social_insights_dev'),
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
    '003_provider_authorization_resources',
    '004_youtube_readonly_vertical',
    '005_meta_readonly_vertical',
    '006_meta_period_snapshots',
    '007_meta_oauth_config_binding',
    '008_account_invitation_lifecycle',
    '009_observations_and_report_foundation',
    '010_provider_report_tenant_integrity'
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
       'workspace_provider_connections',
       'youtube_channel_snapshots',
       'youtube_analytics_daily_snapshots',
       'youtube_video_analytics_snapshots',
       'provider_request_events'
     )`
  );
  assert.equal(tableRows.length, 12);

  const architectureRows = await db.query(
    `SELECT TABLE_NAME AS table_name FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (
       'provider_resource_observations',
       'provider_metric_observations',
       'provider_dimension_observations',
       'report_definitions',
       'report_definition_resources',
       'report_runs',
       'report_run_resources',
       'report_artifacts',
       'report_download_grants'
     )`
  );
  assert.equal(architectureRows.length, 9);

  const syncColumns = await db.query(
    `SELECT COLUMN_NAME AS column_name FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sync_runs'
       AND COLUMN_NAME IN ('workspace_provider_connection_id', 'provider_api_version')`
  );
  assert.equal(syncColumns.length, 2);
});

test('provider observations and report definitions enforce workspace ownership in the schema', async () => {
  await clearDatabase();
  const firstOwner = await signIn('architecture-first@example.com');
  const secondOwner = await signIn('architecture-second@example.com');
  const firstWorkspace = await createWorkspace(firstOwner, 'Architecture First');
  const secondWorkspace = await createWorkspace(secondOwner, 'Architecture Second');
  const firstSourceId = crypto.randomUUID();
  const secondSourceId = crypto.randomUUID();
  const firstAuthorizationId = crypto.randomUUID();
  const secondAuthorizationId = crypto.randomUUID();
  const firstResourceId = crypto.randomUUID();
  const secondResourceId = crypto.randomUUID();
  const firstConnectionId = crypto.randomUUID();
  const secondConnectionId = crypto.randomUUID();
  const firstRunId = crypto.randomUUID();

  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status) VALUES
     (?, ?, 'tiktok', 'active'), (?, ?, 'tiktok', 'active')`,
    [firstSourceId, firstWorkspace.id, secondSourceId, secondWorkspace.id]
  );
  await db.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, source_data_source_id, status)
     VALUES (?, ?, 'tiktok', ?, ?, 'active'), (?, ?, 'tiktok', ?, ?, 'active')`,
    [
      firstAuthorizationId,
      firstWorkspace.id,
      firstOwner.user.id,
      firstSourceId,
      secondAuthorizationId,
      secondWorkspace.id,
      secondOwner.user.id,
      secondSourceId
    ]
  );
  await db.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type, provider_resource_id, display_name)
     VALUES (?, ?, ?, 'tiktok', 'tiktok_account', 'first-account', 'First Account'),
            (?, ?, ?, 'tiktok', 'tiktok_account', 'second-account', 'Second Account')`,
    [
      firstResourceId,
      firstAuthorizationId,
      firstWorkspace.id,
      secondResourceId,
      secondAuthorizationId,
      secondWorkspace.id
    ]
  );
  await db.query(
    `INSERT INTO workspace_provider_connections
      (id, workspace_id, provider_resource_id, data_source_id, provider, status)
     VALUES (?, ?, ?, ?, 'tiktok', 'active'), (?, ?, ?, ?, 'tiktok', 'active')`,
    [
      firstConnectionId,
      firstWorkspace.id,
      firstResourceId,
      firstSourceId,
      secondConnectionId,
      secondWorkspace.id,
      secondResourceId,
      secondSourceId
    ]
  );
  await db.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, workspace_provider_connection_id, trigger_type, status)
     VALUES (?, ?, ?, ?, 'scheduled', 'success')`,
    [firstRunId, firstWorkspace.id, firstSourceId, firstConnectionId]
  );

  await assert.rejects(
    db.query(
      `INSERT INTO provider_resources
        (id, provider_authorization_id, workspace_id, provider, resource_type, provider_resource_id, display_name)
       VALUES (?, ?, ?, 'tiktok', 'tiktok_account', 'cross-resource', 'Cross Resource')`,
      [crypto.randomUUID(), firstAuthorizationId, secondWorkspace.id]
    ),
    /foreign key constraint/i
  );

  await assert.rejects(
    db.query(
      `INSERT INTO provider_metric_observations
        (id, workspace_id, workspace_provider_connection_id, sync_run_id, provider, metric_key,
         grain, period_start, period_end, observed_at, numeric_value, unit,
         availability_status, definition_version)
       VALUES (?, ?, ?, ?, 'tiktok', 'tiktok.followers', 'daily', '2026-07-01', '2026-07-01',
               UTC_TIMESTAMP(3), 10, 'count', 'available', 'test-v1')`,
      [crypto.randomUUID(), secondWorkspace.id, firstConnectionId, firstRunId]
    ),
    /foreign key constraint/i
  );

  const definitionId = crypto.randomUUID();
  await db.query(
    `INSERT INTO report_definitions
      (id, workspace_id, created_by_user_id, title, timezone, range_start, range_end, configuration)
     VALUES (?, ?, ?, 'First report', 'UTC', '2026-07-01', '2026-07-07', JSON_OBJECT())`,
    [definitionId, firstWorkspace.id, firstOwner.user.id]
  );
  await assert.rejects(
    db.query(
      `INSERT INTO report_definition_resources
        (report_definition_id, workspace_id, workspace_provider_connection_id, provider)
       VALUES (?, ?, ?, 'tiktok')`,
      [definitionId, firstWorkspace.id, secondConnectionId]
    ),
    /foreign key constraint/i
  );
  await assert.rejects(
    db.query(
      `INSERT INTO report_runs
        (id, report_definition_id, workspace_id, requested_by_user_id, idempotency_key,
         configuration_snapshot, metric_definitions_snapshot)
       VALUES (?, ?, ?, ?, ?, JSON_OBJECT(), JSON_OBJECT())`,
      [
        crypto.randomUUID(),
        definitionId,
        secondWorkspace.id,
        secondOwner.user.id,
        crypto.randomBytes(32).toString('hex')
      ]
    ),
    /foreign key constraint/i
  );
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

    await applyMigrationFile(shadow, '004_youtube_readonly_vertical.sql');
    const youtubeTableRows = await shadow.query(
      `SELECT TABLE_NAME AS table_name
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (
         'youtube_channel_snapshots',
         'youtube_analytics_daily_snapshots',
         'youtube_video_analytics_snapshots',
         'provider_request_events'
       )`
    );
    assert.equal(youtubeTableRows.length, 4);
    const validationIndexRows = await shadow.query(
      `SELECT INDEX_NAME AS index_name
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'provider_authorizations'
         AND INDEX_NAME = 'provider_authorizations_youtube_validation_due_idx'`
    );
    assert.equal(validationIndexRows.length, 3);
    const preservedCredentials = await shadow.query(
      `SELECT access_token_ciphertext, refresh_token_ciphertext
       FROM provider_authorization_credentials
       WHERE provider_authorization_id = (
         SELECT id FROM provider_authorizations
         WHERE source_data_source_id = '21000000-0000-4000-8000-000000000001'
       )`
    );
    assert.equal(preservedCredentials[0].access_token_ciphertext, 'legacy-access-ciphertext');
    assert.equal(preservedCredentials[0].refresh_token_ciphertext, 'legacy-refresh-ciphertext');
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
  const secondOwner = await signIn('member-owner-two@example.com');
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

  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'owner', 'active')`,
    [workspace.id, secondOwner.user.id]
  );
  const adminCannotDemoteOwner = await requestApp(
    `/api/workspaces/${workspace.id}/members/${owner.user.id}`,
    {
      method: 'PATCH',
      headers: {
        cookie: cookieHeader(admin.cookies),
        'x-csrf-token': admin.csrf
      },
      body: { role: 'viewer' }
    }
  );
  assert.equal(adminCannotDemoteOwner.statusCode, 403);
  assert.equal(adminCannotDemoteOwner.json().error, 'owner_management_requires_owner');

  const adminCannotRemoveOwner = await requestApp(`/api/workspaces/${workspace.id}/members/${owner.user.id}`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(admin.cookies),
      'x-csrf-token': admin.csrf
    },
    body: {}
  });
  assert.equal(adminCannotRemoveOwner.statusCode, 403);
  assert.equal(adminCannotRemoveOwner.json().error, 'owner_management_requires_owner');
});

test('invitation lifecycle is email-bound, rate-limited, auditable, and explicit', async () => {
  await clearDatabase();
  const owner = await signIn('invitation-owner@example.com');
  const wrongUser = await signIn('wrong-invitee@example.com');
  const workspace = await createWorkspace(owner, 'Invitation Lifecycle');

  const invalidEmail = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { email: 'invalid', role: 'viewer' }
  });
  assert.equal(invalidEmail.statusCode, 400);
  assert.equal(invalidEmail.json().error, 'invalid_email');

  const invite = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { email: 'invited-analyst@example.com', role: 'analyst' }
  });
  assert.equal(invite.statusCode, 201);
  const invitationToken = invite.json().dev_token;
  assert.ok(invitationToken);

  const duplicate = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { email: 'invited-analyst@example.com', role: 'viewer' }
  });
  assert.equal(duplicate.statusCode, 409);
  assert.equal(duplicate.json().error, 'invitation_pending');

  const invitationRows = await db.query(
    'SELECT id FROM workspace_invitations WHERE workspace_id = ? AND email = ?',
    [workspace.id, 'invited-analyst@example.com']
  );
  const invitationId = invitationRows[0].id;
  const cooldown = await requestApp(`/api/workspaces/${workspace.id}/invitations/${invitationId}/resend`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(cooldown.statusCode, 429);
  assert.equal(cooldown.json().error, 'invitation_resend_cooldown');

  await db.query(
    'UPDATE workspace_invitations SET last_sent_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 2 MINUTE) WHERE id = ?',
    [invitationId]
  );
  const resend = await requestApp(`/api/workspaces/${workspace.id}/invitations/${invitationId}/resend`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(resend.statusCode, 200);
  assert.ok(resend.json().dev_token);

  const mismatch = await requestApp('/api/invitations/accept', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(wrongUser.cookies),
      'x-csrf-token': wrongUser.csrf
    },
    body: { token: resend.json().dev_token }
  });
  assert.equal(mismatch.statusCode, 403);
  assert.equal(mismatch.json().error, 'invitation_email_mismatch');

  const invited = await signIn('invited-analyst@example.com');
  const accepted = await requestApp('/api/invitations/accept', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(invited.cookies),
      'x-csrf-token': invited.csrf
    },
    body: { token: resend.json().dev_token }
  });
  assert.equal(accepted.statusCode, 200);
  assert.equal(accepted.json().workspace.id, workspace.id);
  assert.equal(accepted.json().workspace.role, 'analyst');

  const replay = await requestApp('/api/invitations/accept', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(invited.cookies),
      'x-csrf-token': invited.csrf
    },
    body: { token: resend.json().dev_token }
  });
  assert.equal(replay.statusCode, 400);
  assert.equal(replay.json().error, 'invitation_invalid_or_expired');

  const revokeInvite = await requestApp(`/api/workspaces/${workspace.id}/invitations`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { email: 'revoked-invitee@example.com', role: 'viewer' }
  });
  assert.equal(revokeInvite.statusCode, 201);
  const revokeRows = await db.query(
    'SELECT id FROM workspace_invitations WHERE workspace_id = ? AND email = ?',
    [workspace.id, 'revoked-invitee@example.com']
  );
  const revoked = await requestApp(`/api/workspaces/${workspace.id}/invitations/${revokeRows[0].id}`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(revoked.statusCode, 200);
  assert.equal(revoked.json().revoked, true);

  const auditRows = await db.query(
    `SELECT action FROM audit_logs WHERE workspace_id = ?
     AND action IN ('member_invited', 'invitation_resent', 'invitation_accepted', 'invitation_revoked')`,
    [workspace.id]
  );
  assert.deepEqual(new Set(auditRows.map(row => row.action)), new Set([
    'member_invited',
    'invitation_resent',
    'invitation_accepted',
    'invitation_revoked'
  ]));
});

test('account profile, active sessions, and deletion requests are user-bound', async () => {
  await clearDatabase();
  const first = await signIn('account-owner@example.com');
  const second = await signIn('account-owner@example.com');
  const viewer = await signIn('account-viewer@example.com');
  const workspace = await createWorkspace(first, 'Deletion Review Workspace');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'viewer', 'active')`,
    [workspace.id, viewer.user.id]
  );

  const account = await requestApp('/api/account', {
    headers: { cookie: cookieHeader(first.cookies) }
  });
  assert.equal(account.statusCode, 200);
  assert.equal(account.json().sessions.length, 2);
  assert.equal(account.json().sessions.filter(session => session.current).length, 1);
  assert.equal(account.json().sessions.every(session => session.device_label), true);
  assert.deepEqual(account.json().authentication_methods.map(method => method.provider), ['email']);

  const profile = await requestApp('/api/account/profile', {
    method: 'PATCH',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { display_name: 'Account Owner' }
  });
  assert.equal(profile.statusCode, 200);
  assert.equal(profile.json().profile.display_name, 'Account Owner');

  const wrongAccountConfirmation = await requestApp('/api/account/deletion-requests', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { confirmation: 'wrong@example.com' }
  });
  assert.equal(wrongAccountConfirmation.statusCode, 400);
  assert.equal(wrongAccountConfirmation.json().error, 'account_deletion_confirmation_invalid');

  const accountDeletion = await requestApp('/api/account/deletion-requests', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { confirmation: 'account-owner@example.com' }
  });
  assert.equal(accountDeletion.statusCode, 202);
  assert.equal(accountDeletion.json().status, 'verified');
  const accountDeletionAgain = await requestApp('/api/account/deletion-requests', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { confirmation: 'account-owner@example.com' }
  });
  assert.equal(accountDeletionAgain.statusCode, 202);
  assert.equal(accountDeletionAgain.json().existing, true);

  const viewerDeletion = await requestApp(`/api/workspaces/${workspace.id}/deletion-requests`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { confirmation: workspace.name }
  });
  assert.equal(viewerDeletion.statusCode, 403);
  assert.equal(viewerDeletion.json().error, 'permission_denied');

  const wrongWorkspaceConfirmation = await requestApp(`/api/workspaces/${workspace.id}/deletion-requests`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { confirmation: 'Wrong workspace' }
  });
  assert.equal(wrongWorkspaceConfirmation.statusCode, 400);
  assert.equal(wrongWorkspaceConfirmation.json().error, 'workspace_deletion_confirmation_invalid');

  const workspaceDeletion = await requestApp(`/api/workspaces/${workspace.id}/deletion-requests`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: { confirmation: workspace.name }
  });
  assert.equal(workspaceDeletion.statusCode, 202);
  assert.equal(workspaceDeletion.json().status, 'verified');

  const currentSessionId = account.json().sessions.find(session => session.current).id;
  const otherSessionId = account.json().sessions.find(session => !session.current).id;
  const revokeOther = await requestApp(`/api/account/sessions/${otherSessionId}`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: {}
  });
  assert.equal(revokeOther.statusCode, 200);
  assert.equal(revokeOther.json().signed_out, false);
  assert.notEqual(otherSessionId, currentSessionId);
  const secondAfterRevoke = await requestApp('/api/session', {
    headers: { cookie: cookieHeader(second.cookies) }
  });
  assert.equal(secondAfterRevoke.statusCode, 401);

  const third = await signIn('account-owner@example.com');
  const revokeOthers = await requestApp('/api/account/sessions/revoke-others', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: {}
  });
  assert.equal(revokeOthers.statusCode, 200);
  assert.equal(revokeOthers.json().revoked, 1);
  const thirdAfterRevoke = await requestApp('/api/session', {
    headers: { cookie: cookieHeader(third.cookies) }
  });
  assert.equal(thirdAfterRevoke.statusCode, 401);

  const fourth = await signIn('account-owner@example.com');
  const revokeAll = await requestApp('/api/account/sessions/revoke-all', {
    method: 'POST',
    headers: {
      cookie: cookieHeader(first.cookies),
      'x-csrf-token': first.csrf
    },
    body: {}
  });
  assert.equal(revokeAll.statusCode, 200);
  assert.equal(revokeAll.json().signed_out, true);
  assert.ok(revokeAll.json().revoked >= 2);
  for (const session of [first, fourth]) {
    const response = await requestApp('/api/session', {
      headers: { cookie: cookieHeader(session.cookies) }
    });
    assert.equal(response.statusCode, 401);
  }
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

    await sendInvitationEmail({
      email: 'invitee@example.com',
      token: 'invitation-token',
      workspaceName: 'Review Workspace',
      inviterEmail: 'owner@example.com'
    });
    assert.equal(sentMessages.length, 2);
    assert.equal(sentMessages[1].message.to, 'invitee@example.com');
    assert.match(sentMessages[1].message.text, /Review Workspace/);
    assert.match(sentMessages[1].message.text, /http:\/\/localhost:3001\/\?invitation=invitation-token/);
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
  assert.equal(youtubeProvider.connectable, true);
  assert.equal(youtubeProvider.status, 'available');

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

  const youtubeSourceId = '20000000-0000-4000-8000-000000000777';
  const youtubeRunId = '30000000-0000-4000-8000-000000000777';
  const youtubeContentId = '50000000-0000-4000-8000-000000000777';
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'youtube', 'active')`,
    [youtubeSourceId, workspace.id]
  );
  await db.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, trigger_type, status, started_at, finished_at, content_seen_count)
     VALUES (?, ?, ?, 'scheduled', 'success',
             DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 1 SECOND),
             DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 1 SECOND), 1)`,
    [youtubeRunId, workspace.id, youtubeSourceId]
  );
  await db.query(
    `INSERT INTO content_items
      (id, workspace_id, data_source_id, provider_content_id, published_at, title, share_url)
     VALUES (?, ?, ?, 'youtube-video', UTC_TIMESTAMP(3), 'YouTube must stay isolated',
             'https://www.youtube.com/watch?v=youtube-video')`,
    [youtubeContentId, workspace.id, youtubeSourceId]
  );
  await db.query(
    `INSERT INTO content_metric_snapshots
      (id, workspace_id, content_item_id, sync_run_id, observed_at,
       view_count, like_count, comment_count, share_count)
     VALUES (?, ?, ?, ?, UTC_TIMESTAMP(3), 999999, 999999, 999999, 999999)`,
    [crypto.randomUUID(), workspace.id, youtubeContentId, youtubeRunId]
  );

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
  assert.notEqual(dashboardBody.latest_sync.id, youtubeRunId);
  assert.equal(dashboardBody.latest_sync.status, 'success');

  const content = await requestApp(`/api/workspaces/${workspace.id}/content?sort=views`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(content.statusCode, 200);
  const contentBody = content.json();
  assert.equal(contentBody.total, 1);
  assert.equal(contentBody.rows[0].engagement_rate, 13);
  assert.equal(contentBody.rows.some(row => row.provider_content_id === 'youtube-video'), false);

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
  assert.equal(csv.body.includes('YouTube must stay isolated'), false);

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

test('YouTube OAuth state rejects missing, expired, and cross-binding callbacks without Google calls', async () => {
  await clearDatabase();
  const owner = await signIn('youtube-state-owner@example.com');
  const secondOwnerSession = await signIn('youtube-state-owner@example.com');
  const otherUser = await signIn('youtube-state-other@example.com');
  let googleCalls = 0;
  setYouTubeTestHooks({
    fetch: async () => {
      googleCalls += 1;
      throw new Error('google_must_not_be_called_for_rejected_state');
    }
  });

  async function startCase(label) {
    const workspace = await createWorkspace(owner, `YouTube ${label}`);
    const start = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
      method: 'POST',
      headers: {
        cookie: cookieHeader(owner.cookies),
        'x-csrf-token': owner.csrf
      },
      body: { return_path: `/?workspace=${workspace.id}&view=connections` }
    });
    assert.equal(start.statusCode, 200);
    const state = new URL(start.json().authorization_url).searchParams.get('state');
    const rows = await db.query(
      `SELECT id, provider_authorization_id
       FROM oauth_transactions WHERE state_hash = ? LIMIT 1`,
      [crypto.createHash('sha256').update(state).digest('hex')]
    );
    return { workspace, state, transaction: rows[0] };
  }

  async function expectRejected(state, session = owner, outcome = 'failed') {
    const callback = await requestApp(
      `/api/integrations/youtube/callback?code=must-not-exchange&state=${encodeURIComponent(state)}`,
      { headers: { cookie: cookieHeader(session.cookies) } }
    );
    assert.equal(callback.statusCode, 303);
    assert.equal(
      new URL(callback.headers.location, 'http://localhost').searchParams.get('youtube'),
      outcome
    );
    assert.equal(callback.body.includes('must-not-exchange'), false);
  }

  const missingState = await requestApp('/api/integrations/youtube/callback?code=must-not-exchange', {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(missingState.headers.location, '/?view=connections&youtube=failed');

  const expired = await startCase('Expired State');
  await db.query(
    `UPDATE oauth_transactions SET expires_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND)
     WHERE id = ?`,
    [expired.transaction.id]
  );
  await expectRejected(expired.state);
  const expiredRows = await db.query(
    `SELECT status, pkce_verifier_ciphertext FROM oauth_transactions WHERE id = ?`,
    [expired.transaction.id]
  );
  assert.equal(expiredRows[0].status, 'expired');
  assert.equal(expiredRows[0].pkce_verifier_ciphertext, null);

  const sessionMismatch = await startCase('Session Mismatch');
  await expectRejected(sessionMismatch.state, secondOwnerSession);

  const userMismatch = await startCase('User Mismatch');
  await db.query('UPDATE oauth_transactions SET initiated_by = ? WHERE id = ?', [
    otherUser.user.id,
    userMismatch.transaction.id
  ]);
  await expectRejected(userMismatch.state);

  const workspaceMismatch = await startCase('Workspace Mismatch');
  const movedWorkspace = await createWorkspace(owner, 'YouTube Moved Authorization');
  await db.query('UPDATE provider_authorizations SET workspace_id = ? WHERE id = ?', [
    movedWorkspace.id,
    workspaceMismatch.transaction.provider_authorization_id
  ]);
  await expectRejected(workspaceMismatch.state);

  const providerMismatch = await startCase('Provider Mismatch');
  await db.query("UPDATE oauth_transactions SET provider = 'tiktok' WHERE id = ?", [
    providerMismatch.transaction.id
  ]);
  await expectRejected(providerMismatch.state);

  const redirectMismatch = await startCase('Redirect Mismatch');
  await db.query(
    "UPDATE oauth_transactions SET redirect_uri = 'https://evil.example/callback' WHERE id = ?",
    [redirectMismatch.transaction.id]
  );
  await expectRejected(redirectMismatch.state, owner, 'configuration_error');

  const scopeMismatch = await startCase('Scope Mismatch');
  await db.query('UPDATE oauth_transactions SET requested_scopes = ? WHERE id = ?', [
    JSON.stringify([YOUTUBE_SCOPES[0]]),
    scopeMismatch.transaction.id
  ]);
  await expectRejected(scopeMismatch.state);

  assert.equal(googleCalls, 0);
});

test('YouTube OAuth failures distinguish denial, missing code, missing scopes, and provider timeout', async () => {
  await clearDatabase();
  const owner = await signIn('youtube-failure-owner@example.com');

  async function startCase(label) {
    const workspace = await createWorkspace(owner, `YouTube ${label}`);
    const start = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
      method: 'POST',
      headers: {
        cookie: cookieHeader(owner.cookies),
        'x-csrf-token': owner.csrf
      },
      body: { return_path: `/?workspace=${workspace.id}&view=connections` }
    });
    assert.equal(start.statusCode, 200);
    return {
      workspace,
      state: new URL(start.json().authorization_url).searchParams.get('state')
    };
  }

  async function latestFailure(workspaceId) {
    const rows = await db.query(
      `SELECT JSON_UNQUOTE(JSON_EXTRACT(al.metadata, '$.outcome_category')) AS outcome
       FROM audit_logs al
       WHERE al.workspace_id = ? AND al.action = 'connection.youtube.authorization_failed'
       ORDER BY al.created_at DESC LIMIT 1`,
      [workspaceId]
    );
    return rows[0] && rows[0].outcome;
  }

  const denied = await startCase('Denied');
  const deniedCallback = await requestApp(
    `/api/integrations/youtube/callback?error=access_denied&state=${encodeURIComponent(denied.state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(deniedCallback.headers.location, 'http://localhost').searchParams.get('youtube'), 'denied');
  assert.equal(await latestFailure(denied.workspace.id), 'user_denied');

  const missingCode = await startCase('Missing Code');
  const missingCodeCallback = await requestApp(
    `/api/integrations/youtube/callback?state=${encodeURIComponent(missingCode.state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(missingCodeCallback.headers.location, 'http://localhost').searchParams.get('youtube'), 'failed');
  assert.equal(await latestFailure(missingCode.workspace.id), 'missing_code');

  const partial = await startCase('Partial Scopes');
  const partialHandlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'partial-access-token',
      refresh_token: 'partial-refresh-token',
      expires_in: 3600,
      scope: YOUTUBE_SCOPES[0]
    }),
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/revoke');
      assert.equal(new URLSearchParams(options.body).get('token'), 'partial-refresh-token');
      return jsonResponse(200, {});
    }
  ]);
  const partialCallback = await requestApp(
    `/api/integrations/youtube/callback?code=partial-code&state=${encodeURIComponent(partial.state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(partialCallback.headers.location, 'http://localhost').searchParams.get('youtube'), 'missing_scopes');
  assert.equal(partialHandlers.length, 0);
  const partialRows = await db.query(
    `SELECT pauth.status,
            (SELECT GROUP_CONCAT(scope ORDER BY scope)
             FROM provider_authorization_scopes pas
             WHERE pas.provider_authorization_id = pauth.id) AS scopes,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac
             WHERE pac.provider_authorization_id = pauth.id) AS credentials
     FROM provider_authorizations pauth
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'`,
    [partial.workspace.id]
  );
  assert.equal(partialRows[0].status, 'disabled');
  assert.equal(partialRows[0].scopes, YOUTUBE_SCOPES[0]);
  assert.equal(Number(partialRows[0].credentials), 0);
  assert.equal(await latestFailure(partial.workspace.id), 'missing_required_scopes');

  const expanded = await startCase('Expanded Scopes');
  const expandedHandlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'expanded-access-token',
      refresh_token: 'expanded-refresh-token',
      expires_in: 3600,
      scope: [...YOUTUBE_SCOPES, 'https://www.googleapis.com/auth/youtube.upload'].join(' ')
    }),
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/revoke');
      assert.equal(new URLSearchParams(options.body).get('token'), 'expanded-refresh-token');
      return jsonResponse(200, {});
    }
  ]);
  const expandedCallback = await requestApp(
    `/api/integrations/youtube/callback?code=expanded-code&state=${encodeURIComponent(expanded.state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(expandedCallback.headers.location, 'http://localhost').searchParams.get('youtube'), 'missing_scopes');
  assert.equal(expandedHandlers.length, 0);
  const expandedCredentials = await db.query(
    `SELECT COUNT(*) AS count
     FROM provider_authorization_credentials pac
     JOIN provider_authorizations pauth ON pauth.id = pac.provider_authorization_id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'`,
    [expanded.workspace.id]
  );
  assert.equal(Number(expandedCredentials[0].count), 0);
  assert.equal(await latestFailure(expanded.workspace.id), 'missing_required_scopes');

  const timeout = await startCase('Token Timeout');
  setYouTubeTestHooks({
    fetch: async () => {
      const error = new Error('synthetic_abort_detail');
      error.name = 'AbortError';
      throw error;
    }
  });
  const timeoutCallback = await requestApp(
    `/api/integrations/youtube/callback?code=timeout-code&state=${encodeURIComponent(timeout.state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(timeoutCallback.headers.location, 'http://localhost').searchParams.get('youtube'), 'provider_error');
  assert.equal(timeoutCallback.body.includes('synthetic_abort_detail'), false);
  assert.equal(timeoutCallback.body.includes('timeout-code'), false);
  assert.equal(await latestFailure(timeout.workspace.id), 'timeout');
});

test('YouTube read-only lifecycle is bound, encrypted, bounded, reportable, and purgeable', async () => {
  await clearDatabase();
  const owner = await signIn('owner-youtube@example.com');
  const viewer = await signIn('viewer-youtube@example.com');
  const workspace = await createWorkspace(owner, 'YouTube Workspace');
  await db.query(
    `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
     VALUES (?, ?, 'viewer', 'active')`,
    [workspace.id, viewer.user.id]
  );

  const viewerStart = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { return_path: '/?view=connections' }
  });
  assert.equal(viewerStart.statusCode, 403);

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {
      return_path: `/?workspace=${workspace.id}&view=connections&provider=youtube`
    }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  const state = authorizationUrl.searchParams.get('state');
  assert.ok(state);
  assert.equal(authorizationUrl.searchParams.get('access_type'), 'offline');
  assert.equal(authorizationUrl.searchParams.get('include_granted_scopes'), 'true');
  assert.equal(authorizationUrl.searchParams.get('prompt'), 'consent');
  assert.equal(authorizationUrl.searchParams.get('code_challenge_method'), 'S256');
  assert.deepEqual(authorizationUrl.searchParams.get('scope').split(' '), YOUTUBE_SCOPES);

  const transactionRows = await db.query(
    `SELECT ot.*, pa.workspace_id AS authorization_workspace_id
     FROM oauth_transactions ot
     JOIN provider_authorizations pa ON pa.id = ot.provider_authorization_id
     WHERE ot.workspace_id = ? AND ot.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(transactionRows.length, 1);
  const transaction = transactionRows[0];
  assert.notEqual(transaction.state_hash, state);
  assert.equal(transaction.state_hash, crypto.createHash('sha256').update(state).digest('hex'));
  assert.equal(transaction.workspace_id, workspace.id);
  assert.equal(transaction.authorization_workspace_id, workspace.id);
  assert.equal(transaction.initiated_by, owner.user.id);
  assert.equal(transaction.redirect_uri, process.env.YOUTUBE_REDIRECT_URI);
  assert.deepEqual(
    typeof transaction.requested_scopes === 'string'
      ? JSON.parse(transaction.requested_scopes)
      : transaction.requested_scopes,
    YOUTUBE_SCOPES
  );
  const verifier = decryptSecret({
    ciphertext: transaction.pkce_verifier_ciphertext,
    iv: transaction.pkce_verifier_iv,
    tag: transaction.pkce_verifier_tag,
    keyVersion: transaction.pkce_key_version
  });
  assert.equal(
    authorizationUrl.searchParams.get('code_challenge'),
    crypto.createHash('sha256').update(verifier).digest('base64url')
  );

  const noSessionCallback = await requestApp(
    `/api/integrations/youtube/callback?code=must-not-run&state=${encodeURIComponent(state)}`
  );
  assert.equal(noSessionCallback.statusCode, 303);
  assert.equal(noSessionCallback.headers.location, '/?view=connections&youtube=failed');
  assert.equal(noSessionCallback.body.includes('must-not-run'), false);

  const oauthHandlers = installYouTubeQueue([
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/token');
      const body = new URLSearchParams(options.body);
      assert.equal(body.get('code'), 'youtube-provider-code');
      assert.equal(body.get('code_verifier'), verifier);
      return jsonResponse(200, {
        access_token: 'youtube-access-token',
        refresh_token: 'youtube-refresh-token',
        expires_in: 3600,
        token_type: 'Bearer',
        scope: YOUTUBE_SCOPES.join(' ')
      });
    },
    (url, options) => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.pathname, '/youtube/v3/channels');
      assert.equal(requestUrl.searchParams.get('mine'), 'true');
      assert.equal(options.headers.Authorization, 'Bearer youtube-access-token');
      return jsonResponse(200, {
        items: [
          {
            id: 'channel-alpha',
            snippet: {
              title: 'Alpha Studio',
              customUrl: '@alpha',
              thumbnails: { high: { url: 'https://img.example/alpha.jpg' } }
            },
            statistics: { subscriberCount: '1200', viewCount: '5000', videoCount: '2' },
            contentDetails: { relatedPlaylists: { uploads: 'uploads-alpha' } }
          },
          {
            id: 'channel-beta',
            snippet: {
              title: 'Beta Studio',
              thumbnails: { default: { url: 'https://img.example/beta.jpg' } }
            },
            statistics: { hiddenSubscriberCount: true, viewCount: '900', videoCount: '1' },
            contentDetails: { relatedPlaylists: { uploads: 'uploads-beta' } }
          }
        ]
      });
    }
  ]);
  const callback = await requestApp(
    `/api/integrations/youtube/callback?code=youtube-provider-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(callback.statusCode, 303);
  const callbackLocation = new URL(callback.headers.location, 'http://localhost');
  const callbackDiagnostic = await db.query(
    `SELECT pa.status,
            (SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.outcome_category'))
             FROM audit_logs
             WHERE target_id = pa.id AND action = 'connection.youtube.authorization_failed'
             ORDER BY created_at DESC LIMIT 1) AS failure_category
     FROM provider_authorizations pa
     WHERE pa.id = ?`,
    [transaction.provider_authorization_id]
  );
  assert.equal(
    callbackLocation.searchParams.get('youtube'),
    'selection_required',
    JSON.stringify({
      handlersRemaining: oauthHandlers.length,
      authorization: callbackDiagnostic[0] || null
    })
  );
  assert.equal(callbackLocation.searchParams.has('code'), false);
  assert.equal(callbackLocation.searchParams.has('state'), false);
  assert.equal(callback.body.includes('youtube-provider-code'), false);
  assert.equal(callback.body.includes(state), false);
  assert.equal(oauthHandlers.length, 0);

  const consumedTransactions = await db.query(
    `SELECT status, pkce_verifier_ciphertext, pkce_verifier_iv, pkce_verifier_tag, pkce_key_version
     FROM oauth_transactions WHERE id = ?`,
    [transaction.id]
  );
  assert.equal(consumedTransactions[0].status, 'consumed');
  assert.equal(consumedTransactions[0].pkce_verifier_ciphertext, null);
  assert.equal(consumedTransactions[0].pkce_verifier_iv, null);
  assert.equal(consumedTransactions[0].pkce_verifier_tag, null);
  assert.equal(consumedTransactions[0].pkce_key_version, null);

  const replay = await requestApp(
    `/api/integrations/youtube/callback?code=replayed-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(replay.statusCode, 303);
  assert.equal(replay.headers.location, '/?view=connections&youtube=failed');

  const credentialRows = await db.query(
    `SELECT pac.*, pa.status AS authorization_status
     FROM provider_authorization_credentials pac
     JOIN provider_authorizations pa ON pa.id = pac.provider_authorization_id
     WHERE pa.workspace_id = ? AND pa.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(credentialRows.length, 1);
  assert.equal(credentialRows[0].authorization_status, 'active');
  assert.equal(credentialRows[0].key_version, process.env.ENCRYPTION_KEY_VERSION || 'local-v1');
  assert.notEqual(credentialRows[0].access_token_ciphertext, 'youtube-access-token');
  assert.notEqual(credentialRows[0].refresh_token_ciphertext, 'youtube-refresh-token');
  assert.equal(decryptSecret({
    ciphertext: credentialRows[0].access_token_ciphertext,
    iv: credentialRows[0].access_token_iv,
    tag: credentialRows[0].access_token_tag,
    keyVersion: credentialRows[0].key_version
  }), 'youtube-access-token');
  assert.equal(decryptSecret({
    ciphertext: credentialRows[0].refresh_token_ciphertext,
    iv: credentialRows[0].refresh_token_iv,
    tag: credentialRows[0].refresh_token_tag,
    keyVersion: credentialRows[0].key_version
  }), 'youtube-refresh-token');

  const catalogBeforeSelection = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(catalogBeforeSelection.statusCode, 200);
  const youtubeBeforeSelection = catalogBeforeSelection.json().providers.find(provider => provider.id === 'youtube');
  assert.equal(youtubeBeforeSelection.status, 'selection_required');
  assert.equal(youtubeBeforeSelection.resources.length, 2);
  assert.equal(youtubeBeforeSelection.resources.every(resource => resource.selected === false), true);
  assert.deepEqual(youtubeBeforeSelection.authorization.scopes.map(scope => scope.scope).sort(), [...YOUTUBE_SCOPES].sort());

  const alphaResource = youtubeBeforeSelection.resources.find(resource => resource.provider_resource_id === 'channel-alpha');
  const selection = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/select`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { resource_id: alphaResource.id }
  });
  assert.equal(selection.statusCode, 201);
  const selectedConnection = selection.json().connection;
  assert.equal(selectedConnection.account.id, 'channel-alpha');
  const duplicateSelection = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/select`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { resource_id: alphaResource.id }
  });
  assert.equal(duplicateSelection.statusCode, 409);
  assert.equal(duplicateSelection.json().error, 'youtube_channel_already_connected');
  await db.query(
    `UPDATE provider_authorization_credentials pac
     JOIN provider_authorizations pa ON pa.id = pac.provider_authorization_id
     SET pac.access_expires_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 MINUTE)
     WHERE pa.workspace_id = ? AND pa.provider = 'youtube'`,
    [workspace.id]
  );

  const yesterday = new Date();
  yesterday.setUTCHours(0, 0, 0, 0);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  const priorDay = new Date(yesterday);
  priorDay.setUTCDate(priorDay.getUTCDate() - 1);
  const yesterdayText = yesterday.toISOString().slice(0, 10);
  const priorDayText = priorDay.toISOString().slice(0, 10);
  const analyticsHeaders = ['views', 'estimatedMinutesWatched', 'averageViewDuration',
    'averageViewPercentage', 'subscribersGained', 'subscribersLost', 'likes', 'comments', 'shares'];
  const syncHandlers = installYouTubeQueue([
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/token');
      const body = new URLSearchParams(options.body);
      assert.equal(body.get('grant_type'), 'refresh_token');
      assert.equal(body.get('refresh_token'), 'youtube-refresh-token');
      return jsonResponse(200, {
        access_token: 'youtube-refreshed-access-token',
        expires_in: 3600,
        token_type: 'Bearer'
      });
    },
    (url, options) => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.pathname, '/youtube/v3/channels');
      assert.equal(requestUrl.searchParams.get('id'), 'channel-alpha');
      assert.equal(options.headers.Authorization, 'Bearer youtube-refreshed-access-token');
      return jsonResponse(200, {
        items: [{
          id: 'channel-alpha',
          snippet: {
            title: 'Alpha Studio', customUrl: '@alpha',
            thumbnails: { high: { url: 'https://img.example/alpha.jpg' } }
          },
          statistics: { subscriberCount: '1210', viewCount: '5300', videoCount: '2' },
          contentDetails: { relatedPlaylists: { uploads: 'uploads-alpha' } }
        }]
      });
    },
    url => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.pathname, '/youtube/v3/playlistItems');
      assert.equal(requestUrl.searchParams.get('playlistId'), 'uploads-alpha');
      assert.equal(requestUrl.searchParams.get('maxResults'), '50');
      return jsonResponse(200, {
        items: [
          {
            contentDetails: { videoId: 'video-one', videoPublishedAt: `${priorDayText}T08:00:00Z` },
            snippet: { title: 'First video', description: 'First description', thumbnails: { high: { url: 'https://img.example/one.jpg' } } }
          },
          {
            contentDetails: { videoId: 'video-two', videoPublishedAt: `${yesterdayText}T09:00:00Z` },
            snippet: { title: 'Second video', description: 'Second description', thumbnails: { medium: { url: 'https://img.example/two.jpg' } } }
          },
          {
            contentDetails: { videoId: 'video-one', videoPublishedAt: `${priorDayText}T08:00:00Z` },
            snippet: { title: 'Duplicate playlist entry must be deduplicated' }
          }
        ]
      });
    },
    url => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.pathname, '/youtube/v3/videos');
      assert.equal(requestUrl.searchParams.get('id'), 'video-one,video-two');
      return jsonResponse(200, {
        items: [
          {
            id: 'video-one',
            snippet: { title: 'First video', publishedAt: `${priorDayText}T08:00:00Z`, thumbnails: { high: { url: 'https://img.example/one.jpg' } } },
            contentDetails: { duration: 'PT2M3S' },
            statistics: { viewCount: '1000', likeCount: '100', commentCount: '12' },
            status: { privacyStatus: 'public' }
          },
          {
            id: 'video-two',
            snippet: { title: 'Second video', publishedAt: `${yesterdayText}T09:00:00Z`, thumbnails: { medium: { url: 'https://img.example/two.jpg' } } },
            contentDetails: { duration: 'PT45S' },
            statistics: { viewCount: '500', likeCount: '40', commentCount: '5' },
            status: { privacyStatus: 'public' }
          }
        ]
      });
    },
    url => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.searchParams.get('dimensions'), 'day');
      return jsonResponse(200, {
        columnHeaders: [{ name: 'day' }, ...analyticsHeaders.map(name => ({ name }))],
        rows: [
          [priorDayText, 100, 500, 300, 60, 5, 2, 10, 3, 1],
          [yesterdayText, 150, 600, 320, 64, 3, 1, 15, 4, 2]
        ]
      });
    },
    ...[7, 30, 90].map(days => url => {
      const requestUrl = new URL(url);
      assert.equal(requestUrl.searchParams.get('dimensions'), 'video');
      assert.equal(requestUrl.searchParams.get('sort'), '-views');
      assert.equal(requestUrl.searchParams.get('maxResults'), '200');
      return jsonResponse(200, {
        columnHeaders: [{ name: 'video' }, ...analyticsHeaders.map(name => ({ name }))],
        rows: [
          ['video-one', 90 + days, 400 + days, 123, 61.5, 2, 1, 9, 2, 1],
          ['video-two', 40 + days, 200 + days, 45, 72.2, 1, 0, 4, 1, 0]
        ]
      });
    })
  ]);
  const manualSyncResponse = await requestApp(`/api/workspaces/${workspace.id}/providers/youtube/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(manualSyncResponse.statusCode, 202);
  assert.equal(manualSyncResponse.json().status, 'queued');
  assert.equal(syncHandlers.length > 0, true);

  const deniedYouTubeSync = await requestApp(`/api/workspaces/${workspace.id}/providers/youtube/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(deniedYouTubeSync.statusCode, 403);
  const csrfDeniedYouTubeSync = await requestApp(`/api/workspaces/${workspace.id}/providers/youtube/sync-runs`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies) },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(csrfDeniedYouTubeSync.statusCode, 403);

  const manualWorker = await runDueSyncs({
    timeBudgetSeconds: 5,
    leaseOwner: 'youtube-manual-worker'
  });
  assert.equal(manualWorker.processed, 1);
  assert.equal(manualWorker.results[0].status, 'success');
  assert.equal(syncHandlers.length, 0);
  const manualRunRows = await db.query(
    `SELECT trigger_type FROM sync_runs WHERE id = ?`,
    [manualWorker.results[0].sync_run_id]
  );
  assert.equal(manualRunRows[0].trigger_type, 'manual');
  const manualCooldown = await requestApp(`/api/workspaces/${workspace.id}/providers/youtube/sync-runs`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(manualCooldown.statusCode, 429);
  assert.equal(manualCooldown.json().error, 'manual_sync_cooldown');

  const snapshotCounts = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM youtube_channel_snapshots WHERE workspace_id = ?) AS channels,
       (SELECT COUNT(*) FROM youtube_analytics_daily_snapshots WHERE workspace_id = ?) AS daily,
       (SELECT COUNT(*) FROM youtube_video_analytics_snapshots WHERE workspace_id = ?) AS videos,
       (SELECT COUNT(*) FROM provider_request_events WHERE workspace_id = ?) AS requests`,
    [workspace.id, workspace.id, workspace.id, workspace.id]
  );
  assert.equal(Number(snapshotCounts[0].channels), 1);
  assert.equal(Number(snapshotCounts[0].daily), 2);
  assert.equal(Number(snapshotCounts[0].videos), 6);
  assert.equal(Number(snapshotCounts[0].requests), 10);
  const requestSummary = await db.query(
    `SELECT SUM(quota_cost_estimate) AS quota_cost, MAX(attempts) AS max_attempts,
            GROUP_CONCAT(method_name ORDER BY method_name) AS methods
     FROM provider_request_events WHERE workspace_id = ?`,
    [workspace.id]
  );
  assert.equal(Number(requestSummary[0].quota_cost), 4);
  assert.equal(Number(requestSummary[0].max_attempts), 1);
  assert.match(requestSummary[0].methods, /channels\.list/);
  assert.match(requestSummary[0].methods, /channels\.list\.discovery/);
  assert.match(requestSummary[0].methods, /oauth\.token/);
  assert.match(requestSummary[0].methods, /oauth\.refresh/);
  assert.match(requestSummary[0].methods, /reports\.query\.video\.90d/);
  const refreshedCredentialRows = await db.query(
    `SELECT pac.*
     FROM provider_authorization_credentials pac
     JOIN provider_authorizations pa ON pa.id = pac.provider_authorization_id
     WHERE pa.workspace_id = ? AND pa.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(decryptSecret({
    ciphertext: refreshedCredentialRows[0].access_token_ciphertext,
    iv: refreshedCredentialRows[0].access_token_iv,
    tag: refreshedCredentialRows[0].access_token_tag,
    keyVersion: refreshedCredentialRows[0].key_version
  }), 'youtube-refreshed-access-token');
  assert.equal(decryptSecret({
    ciphertext: refreshedCredentialRows[0].refresh_token_ciphertext,
    iv: refreshedCredentialRows[0].refresh_token_iv,
    tag: refreshedCredentialRows[0].refresh_token_tag,
    keyVersion: refreshedCredentialRows[0].key_version
  }), 'youtube-refresh-token');

  setYouTubeTestHooks({ fetch: async () => { throw new Error('dashboard_must_not_call_google'); } });
  const dashboard = await requestApp(`/api/workspaces/${workspace.id}/providers/youtube/dashboard?range=7d`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(dashboard.statusCode, 200);
  const dashboardBody = dashboard.json();
  assert.equal(dashboardBody.channel.display_name, 'Alpha Studio');
  assert.equal(dashboardBody.availability.data_through_date, yesterdayText);
  assert.equal(dashboardBody.trend.length, 2);
  assert.equal(dashboardBody.content.length, 2);
  const metrics = Object.fromEntries(dashboardBody.metrics.map(metric => [metric.key, metric.value]));
  assert.equal(metrics.subscribers_current, 1210);
  assert.equal(metrics.channel_views_lifetime, 5300);
  assert.equal(metrics.views_period, 250);
  assert.equal(metrics.watch_time_period, 1100);
  assert.equal(metrics.net_subscribers_period, 5);

  const customDashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/youtube/dashboard?range=custom&from=${priorDayText}&to=${yesterdayText}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(customDashboard.statusCode, 200);
  assert.equal(customDashboard.json().availability.video_period_supported, false);
  assert.deepEqual(customDashboard.json().content, []);

  await db.query(
    `UPDATE provider_authorization_credentials pac
     JOIN provider_authorizations pauth ON pauth.id = pac.provider_authorization_id
     SET pac.refresh_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 1 DAY)
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'`,
    [workspace.id]
  );
  const reauthorize = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {
      connection_id: selectedConnection.id,
      return_path: `/?workspace=${workspace.id}&view=connections&provider=youtube`
    }
  });
  assert.equal(reauthorize.statusCode, 200);
  const reauthorizeUrl = new URL(reauthorize.json().authorization_url);
  assert.equal(reauthorizeUrl.searchParams.has('prompt'), false);
  const reauthorizeState = reauthorizeUrl.searchParams.get('state');
  const authorizingCatalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  const authorizingYouTube = authorizingCatalog.json().providers.find(provider => provider.id === 'youtube');
  assert.equal(authorizingYouTube.status, 'authorizing');
  assert.equal(authorizingYouTube.connectable, true);
  assert.equal(authorizingYouTube.connections[0].status, 'connecting');
  const authorizingDashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/youtube/dashboard?range=7d`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(authorizingDashboard.statusCode, 200);
  assert.equal(authorizingDashboard.json().connection.status, 'connecting');
  const authorizingManualSync = await requestApp(
    `/api/workspaces/${workspace.id}/providers/youtube/sync-runs`,
    {
      method: 'POST',
      headers: {
        cookie: cookieHeader(owner.cookies),
        'x-csrf-token': owner.csrf
      },
      body: { connection_id: selectedConnection.id }
    }
  );
  assert.equal(authorizingManualSync.statusCode, 400);
  assert.equal(authorizingManualSync.json().error, 'youtube_not_connected');
  const pausedJobRows = await db.query('SELECT status FROM sync_jobs WHERE data_source_id = ?', [
    selectedConnection.data_source_id
  ]);
  assert.equal(pausedJobRows[0].status, 'paused');

  const reauthorizeHandlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'youtube-reauthorized-access-token',
      refresh_token: 'youtube-rotated-refresh-token',
      expires_in: 3600,
      token_type: 'Bearer',
      scope: YOUTUBE_SCOPES.join(' ')
    }),
    () => jsonResponse(200, {
      items: [
        {
          id: 'channel-alpha',
          snippet: {
            title: 'Alpha Studio',
            customUrl: '@alpha',
            thumbnails: { high: { url: 'https://img.example/alpha.jpg' } }
          },
          statistics: { subscriberCount: '1210', viewCount: '5300', videoCount: '2' },
          contentDetails: { relatedPlaylists: { uploads: 'uploads-alpha' } }
        },
        {
          id: 'channel-beta',
          snippet: { title: 'Beta Studio' },
          statistics: { hiddenSubscriberCount: true, viewCount: '900', videoCount: '1' },
          contentDetails: { relatedPlaylists: { uploads: 'uploads-beta' } }
        }
      ]
    })
  ]);
  const reauthorizeCallback = await requestApp(
    `/api/integrations/youtube/callback?code=youtube-reauthorize-code&state=${encodeURIComponent(reauthorizeState)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(
    new URL(reauthorizeCallback.headers.location, 'http://localhost').searchParams.get('youtube'),
    'reconnected'
  );
  assert.equal(reauthorizeHandlers.length, 0);
  const reauthorizedRows = await db.query(
    `SELECT pac.*, wpc.id AS connection_id, wpc.status AS connection_status, sj.status AS job_status
     FROM provider_authorization_credentials pac
     JOIN provider_authorizations pauth ON pauth.id = pac.provider_authorization_id
     JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
       AND pr.provider_resource_id = 'channel-alpha'
     JOIN workspace_provider_connections wpc ON wpc.provider_resource_id = pr.id
     JOIN sync_jobs sj ON sj.data_source_id = wpc.data_source_id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(reauthorizedRows[0].connection_id, selectedConnection.id);
  assert.equal(reauthorizedRows[0].connection_status, 'active');
  assert.equal(reauthorizedRows[0].job_status, 'due');
  assert.equal(reauthorizedRows[0].refresh_expires_at, null);
  assert.equal(decryptSecret({
    ciphertext: reauthorizedRows[0].access_token_ciphertext,
    iv: reauthorizedRows[0].access_token_iv,
    tag: reauthorizedRows[0].access_token_tag,
    keyVersion: reauthorizedRows[0].key_version
  }), 'youtube-reauthorized-access-token');
  assert.equal(decryptSecret({
    ciphertext: reauthorizedRows[0].refresh_token_ciphertext,
    iv: reauthorizedRows[0].refresh_token_iv,
    tag: reauthorizedRows[0].refresh_token_tag,
    keyVersion: reauthorizedRows[0].key_version
  }), 'youtube-rotated-refresh-token');

  await db.query(
    `UPDATE provider_authorization_credentials
     SET refresh_expires_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND)
     WHERE provider_authorization_id = ?`,
    [reauthorizedRows[0].provider_authorization_id]
  );
  const expiredRefreshReauthorize = await requestApp(
    `/api/workspaces/${workspace.id}/connections/youtube/start`,
    {
      method: 'POST',
      headers: {
        cookie: cookieHeader(owner.cookies),
        'x-csrf-token': owner.csrf
      },
      body: {
        connection_id: selectedConnection.id,
        return_path: `/?workspace=${workspace.id}&view=connections&provider=youtube`
      }
    }
  );
  assert.equal(expiredRefreshReauthorize.statusCode, 200);
  assert.equal(new URL(expiredRefreshReauthorize.json().authorization_url).searchParams.get('prompt'), 'consent');

  const storedTextRows = await db.query(
    `SELECT CAST(metadata AS CHAR) AS text FROM audit_logs WHERE workspace_id = ?`,
    [workspace.id]
  );
  assert.equal(storedTextRows.some(row => String(row.text).includes('youtube-access-token')), false);
  assert.equal(storedTextRows.some(row => String(row.text).includes('youtube-refresh-token')), false);
  assert.equal(storedTextRows.some(row => String(row.text).includes('youtube-rotated-refresh-token')), false);

  const revokeHandlers = installYouTubeQueue([
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/revoke');
      assert.equal(new URLSearchParams(options.body).get('token'), 'youtube-rotated-refresh-token');
      return jsonResponse(503, { error: { status: 'UNAVAILABLE' } });
    }
  ]);
  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(disconnect.statusCode, 200);
  assert.equal(disconnect.json().provider_revoke.success, false);
  assert.equal(disconnect.json().provider_revoke.outcome_category, 'provider_revoke_failed_local_purge');
  assert.equal(disconnect.json().local_data_deleted, true);
  assert.equal(revokeHandlers.length, 0);

  const purgeCounts = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM data_sources WHERE workspace_id = ? AND provider = 'youtube') AS sources,
       (SELECT COUNT(*) FROM provider_resources WHERE workspace_id = ? AND provider = 'youtube') AS resources,
       (SELECT COUNT(*) FROM youtube_channel_snapshots WHERE workspace_id = ?) AS channel_snapshots,
       (SELECT COUNT(*) FROM youtube_analytics_daily_snapshots WHERE workspace_id = ?) AS daily_snapshots,
       (SELECT COUNT(*) FROM youtube_video_analytics_snapshots WHERE workspace_id = ?) AS video_snapshots`,
    [workspace.id, workspace.id, workspace.id, workspace.id, workspace.id]
  );
  assert.deepEqual({
    sources: Number(purgeCounts[0].sources),
    resources: Number(purgeCounts[0].resources),
    channelSnapshots: Number(purgeCounts[0].channel_snapshots),
    dailySnapshots: Number(purgeCounts[0].daily_snapshots),
    videoSnapshots: Number(purgeCounts[0].video_snapshots)
  }, { sources: 0, resources: 0, channelSnapshots: 0, dailySnapshots: 0, videoSnapshots: 0 });
  const revokedRows = await db.query(
    `SELECT pa.status, pa.actor_user_id, pa.provider_subject, pa.display_name,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac WHERE pac.provider_authorization_id = pa.id) AS credential_count,
            (SELECT COUNT(*) FROM provider_revocation_events pre WHERE pre.provider_authorization_id = pa.id) AS revocation_count
     FROM provider_authorizations pa
     WHERE pa.workspace_id = ? AND pa.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(revokedRows.length, 1);
  assert.equal(revokedRows[0].status, 'revoked');
  assert.equal(revokedRows[0].actor_user_id, null);
  assert.equal(revokedRows[0].provider_subject, null);
  assert.equal(revokedRows[0].display_name, null);
  assert.equal(Number(revokedRows[0].credential_count), 0);
  assert.equal(Number(revokedRows[0].revocation_count), 1);
});

test('YouTube no-channel authorization can still be revoked and purged', async () => {
  await clearDatabase();
  const owner = await signIn('youtube-no-channel@example.com');
  const workspace = await createWorkspace(owner, 'No Channel Workspace');
  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: `/?workspace=${workspace.id}&view=connections` }
  });
  assert.equal(start.statusCode, 200);
  const state = new URL(start.json().authorization_url).searchParams.get('state');
  const oauthHandlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'no-channel-access',
      refresh_token: 'no-channel-refresh',
      expires_in: 3600,
      scope: YOUTUBE_SCOPES.join(' ')
    }),
    () => jsonResponse(200, { items: [] })
  ]);
  const callback = await requestApp(
    `/api/integrations/youtube/callback?code=no-channel-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(callback.statusCode, 303);
  assert.equal(new URL(callback.headers.location, 'http://localhost').searchParams.get('youtube'), 'no_channels');
  assert.equal(oauthHandlers.length, 0);

  const catalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  const youtubeProvider = catalog.json().providers.find(provider => provider.id === 'youtube');
  assert.equal(youtubeProvider.status, 'no_channels');
  assert.equal(youtubeProvider.authorization.status, 'active');
  assert.deepEqual(youtubeProvider.resources, []);
  assert.deepEqual(youtubeProvider.connections, []);
  const discoveryRows = await db.query(
    `SELECT status, quota_cost_estimate, item_count
     FROM provider_request_events
     WHERE workspace_id = ? AND method_name = 'channels.list.discovery'`,
    [workspace.id]
  );
  assert.equal(discoveryRows.length, 1);
  assert.equal(discoveryRows[0].status, 'empty');
  assert.equal(Number(discoveryRows[0].quota_cost_estimate), 1);
  assert.equal(Number(discoveryRows[0].item_count), 0);

  const revokeHandlers = installYouTubeQueue([
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/revoke');
      assert.equal(new URLSearchParams(options.body).get('token'), 'no-channel-refresh');
      return jsonResponse(200, {});
    }
  ]);
  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube`, {
    method: 'DELETE',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: {}
  });
  assert.equal(disconnect.statusCode, 200);
  assert.equal(disconnect.json().provider_revoke.success, true);
  assert.equal(revokeHandlers.length, 0);
  const authorizationRows = await db.query(
    `SELECT pa.status,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac WHERE pac.provider_authorization_id = pa.id) AS credentials
     FROM provider_authorizations pa
     WHERE pa.workspace_id = ? AND pa.provider = 'youtube'`,
    [workspace.id]
  );
  assert.equal(authorizationRows[0].status, 'revoked');
  assert.equal(Number(authorizationRows[0].credentials), 0);
});

test('YouTube authorizations are purged at the validation deadline with or without selected channels', async () => {
  await clearDatabase();
  const owner = await signIn('youtube-expired-window@example.com');
  const workspace = await createWorkspace(owner, 'Expired YouTube Authorization');
  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: `/?workspace=${workspace.id}&view=connections` }
  });
  const state = new URL(start.json().authorization_url).searchParams.get('state');
  const handlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'validation-window-access',
      refresh_token: 'validation-window-refresh',
      expires_in: 3600,
      scope: YOUTUBE_SCOPES.join(' ')
    }),
    () => jsonResponse(200, { items: [] })
  ]);
  const callback = await requestApp(
    `/api/integrations/youtube/callback?code=validation-window-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(new URL(callback.headers.location, 'http://localhost').searchParams.get('youtube'), 'no_channels');
  assert.equal(handlers.length, 0);

  const beforeRows = await db.query(
    `SELECT id, last_validated_at, deletion_due_at
     FROM provider_authorizations
     WHERE workspace_id = ? AND provider = 'youtube'`,
    [workspace.id]
  );
  assert.ok(beforeRows[0].last_validated_at);
  assert.ok(beforeRows[0].deletion_due_at > beforeRows[0].last_validated_at);
  await db.query(
    `UPDATE provider_authorizations
     SET deletion_due_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND)
     WHERE id = ?`,
    [beforeRows[0].id]
  );

  setYouTubeTestHooks({ fetch: async () => { throw new Error('overdue_purge_must_not_call_google'); } });
  const worker = await runDueSyncs({ timeBudgetSeconds: 1, leaseOwner: 'youtube-validation-window-worker' });
  assert.equal(worker.processed, 0);
  assert.equal(worker.reconciled_youtube_authorizations, 1);
  const afterRows = await db.query(
    `SELECT pauth.status, pauth.actor_user_id, pauth.provider_subject,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac
             WHERE pac.provider_authorization_id = pauth.id) AS credentials,
            (SELECT COUNT(*) FROM provider_authorization_scopes pas
             WHERE pas.provider_authorization_id = pauth.id) AS scopes,
            (SELECT failure_category FROM provider_revocation_events pre
             WHERE pre.provider_authorization_id = pauth.id
             ORDER BY pre.created_at DESC LIMIT 1) AS outcome
     FROM provider_authorizations pauth WHERE pauth.id = ?`,
    [beforeRows[0].id]
  );
  assert.equal(afterRows[0].status, 'revoked');
  assert.equal(afterRows[0].actor_user_id, null);
  assert.equal(afterRows[0].provider_subject, null);
  assert.equal(Number(afterRows[0].credentials), 0);
  assert.equal(Number(afterRows[0].scopes), 0);
  assert.equal(afterRows[0].outcome, 'authorization_validation_window_expired');

  const connectedStart = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: `/?workspace=${workspace.id}&view=connections` }
  });
  const connectedState = new URL(connectedStart.json().authorization_url).searchParams.get('state');
  const connectedHandlers = installYouTubeQueue([
    () => jsonResponse(200, {
      access_token: 'connected-window-access',
      refresh_token: 'connected-window-refresh',
      expires_in: 3600,
      scope: YOUTUBE_SCOPES.join(' ')
    }),
    () => jsonResponse(200, {
      items: [{
        id: 'connected-window-channel',
        snippet: { title: 'Connected Window Channel' },
        statistics: { subscriberCount: '4', viewCount: '20', videoCount: '1' },
        contentDetails: { relatedPlaylists: { uploads: 'connected-window-uploads' } }
      }]
    })
  ]);
  const connectedCallback = await requestApp(
    `/api/integrations/youtube/callback?code=connected-window-code&state=${encodeURIComponent(connectedState)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(
    new URL(connectedCallback.headers.location, 'http://localhost').searchParams.get('youtube'),
    'selection_required'
  );
  assert.equal(connectedHandlers.length, 0);
  const connectedCatalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  const connectedYouTube = connectedCatalog.json().providers.find(provider => provider.id === 'youtube');
  const connectedSelection = await requestApp(`/api/workspaces/${workspace.id}/connections/youtube/select`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { resource_id: connectedYouTube.resources[0].id }
  });
  assert.equal(connectedSelection.statusCode, 201);
  const connectedAuthorizationRows = await db.query(
    `SELECT id FROM provider_authorizations
     WHERE workspace_id = ? AND provider = 'youtube' AND status = 'active'
     LIMIT 1`,
    [workspace.id]
  );
  await db.query(
    `UPDATE provider_authorizations
     SET status = 'reconnect_required',
         deletion_due_at = DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND)
     WHERE id = ?`,
    [connectedAuthorizationRows[0].id]
  );

  setYouTubeTestHooks({ fetch: async () => { throw new Error('connected_overdue_purge_must_not_call_google'); } });
  const connectedWorker = await runDueSyncs({
    timeBudgetSeconds: 1,
    leaseOwner: 'youtube-connected-validation-window-worker'
  });
  assert.equal(connectedWorker.processed, 0);
  assert.equal(connectedWorker.reconciled_youtube_authorizations, 1);
  const connectedAfterRows = await db.query(
    `SELECT pauth.status,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac
             WHERE pac.provider_authorization_id = pauth.id) AS credentials,
            (SELECT COUNT(*) FROM provider_resources pr
             WHERE pr.provider_authorization_id = pauth.id) AS resources,
            (SELECT COUNT(*) FROM data_sources ds
             WHERE ds.workspace_id = pauth.workspace_id AND ds.provider = 'youtube') AS sources
     FROM provider_authorizations pauth WHERE pauth.id = ?`,
    [connectedAuthorizationRows[0].id]
  );
  assert.equal(connectedAfterRows[0].status, 'revoked');
  assert.equal(Number(connectedAfterRows[0].credentials), 0);
  assert.equal(Number(connectedAfterRows[0].resources), 0);
  assert.equal(Number(connectedAfterRows[0].sources), 0);
});

test('YouTube invalid_grant is terminal and immediately purges the authorization', async () => {
  await clearDatabase();
  const owner = await signIn('youtube-invalid-grant@example.com');
  const workspace = await createWorkspace(owner, 'Revoked YouTube Workspace');
  const authorizationId = crypto.randomUUID();
  const resourceId = crypto.randomUUID();
  const dataSourceId = crypto.randomUUID();
  const connectionId = crypto.randomUUID();
  const access = encryptSecret('expired-youtube-access');
  const refresh = encryptSecret('externally-revoked-refresh');

  await db.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, provider_subject, display_name, status, granted_at)
     VALUES (?, ?, 'youtube', ?, 'revoked-channel', 'Revoked Channel', 'active', UTC_TIMESTAMP(3))`,
    [authorizationId, workspace.id, owner.user.id]
  );
  await db.query(
    `INSERT INTO provider_authorization_credentials
      (id, provider_authorization_id, access_token_ciphertext, access_token_iv, access_token_tag,
       refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version,
       access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 MINUTE))`,
    [
      crypto.randomUUID(), authorizationId,
      access.ciphertext, access.iv, access.tag,
      refresh.ciphertext, refresh.iv, refresh.tag, access.keyVersion
    ]
  );
  for (const scope of YOUTUBE_SCOPES) {
    await db.query(
      `INSERT INTO provider_authorization_scopes
        (provider_authorization_id, scope, status, granted_at, last_confirmed_at)
       VALUES (?, ?, 'granted', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))`,
      [authorizationId, scope]
    );
  }
  await db.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type,
       provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'youtube', 'youtube_channel', 'revoked-channel', 'Revoked Channel', JSON_OBJECT())`,
    [resourceId, authorizationId, workspace.id]
  );
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status, next_sync_at)
     VALUES (?, ?, 'youtube', 'active', UTC_TIMESTAMP(3))`,
    [dataSourceId, workspace.id]
  );
  await db.query(
    `INSERT INTO workspace_provider_connections
      (id, workspace_id, provider_resource_id, data_source_id, provider, status, next_sync_at)
     VALUES (?, ?, ?, ?, 'youtube', 'active', UTC_TIMESTAMP(3))`,
    [connectionId, workspace.id, resourceId, dataSourceId]
  );
  await db.query(
    `INSERT INTO sync_jobs (id, data_source_id, run_after, status)
     VALUES (?, ?, UTC_TIMESTAMP(3), 'due')`,
    [crypto.randomUUID(), dataSourceId]
  );

  const handlers = installYouTubeQueue([
    (url, options) => {
      assert.equal(url, 'https://oauth2.googleapis.com/token');
      assert.equal(new URLSearchParams(options.body).get('refresh_token'), 'externally-revoked-refresh');
      return jsonResponse(400, { error: 'invalid_grant' });
    }
  ]);
  const result = await runDueSyncs({ timeBudgetSeconds: 5, leaseOwner: 'youtube-invalid-grant-worker' });
  assert.equal(result.processed, 1);
  assert.equal(result.results[0].status, 'failed');
  assert.equal(result.results[0].error.category, 'authentication');
  assert.equal(result.results[0].error.provider_code, 'invalid_grant');
  assert.equal(handlers.length, 0);

  const authorizationRows = await db.query(
    `SELECT pa.status, pa.actor_user_id, pa.provider_subject,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac WHERE pac.provider_authorization_id = pa.id) AS credential_count,
            (SELECT COUNT(*) FROM provider_resources pr WHERE pr.provider_authorization_id = pa.id) AS resource_count,
            (SELECT COUNT(*) FROM provider_revocation_events pre
             WHERE pre.provider_authorization_id = pa.id
               AND pre.failure_category = 'invalid_grant_external_revocation') AS revocation_count
     FROM provider_authorizations pa WHERE pa.id = ?`,
    [authorizationId]
  );
  assert.equal(authorizationRows[0].status, 'revoked');
  assert.equal(authorizationRows[0].actor_user_id, null);
  assert.equal(authorizationRows[0].provider_subject, null);
  assert.equal(Number(authorizationRows[0].credential_count), 0);
  assert.equal(Number(authorizationRows[0].resource_count), 0);
  assert.equal(Number(authorizationRows[0].revocation_count), 1);
  const sourceRows = await db.query('SELECT COUNT(*) AS count FROM data_sources WHERE id = ?', [dataSourceId]);
  assert.equal(Number(sourceRows[0].count), 0);
});

test('GA4 uses explicit property selection, encrypted credentials, worker-only reports, stored dashboards, and purge', async () => {
  await clearDatabase();
  const owner = await signIn('ga4-owner@example.com');
  const workspace = await createWorkspace(owner, 'GA4 Workspace');
  const windows = buildDateWindows('UTC', 180);
  const priorDailyDate = new Date(`${windows.endDate}T00:00:00.000Z`);
  priorDailyDate.setUTCDate(priorDailyDate.getUTCDate() - 1);
  const priorDailyKey = priorDailyDate.toISOString().slice(0, 10).replaceAll('-', '');
  const breakdownDimensions = [...new Set(GA4_BREAKDOWNS.flatMap(item => item.dimensions))];
  const reportMetricValue = (metric, context) => {
    const defaults = {
      activeUsers: 8,
      newUsers: 3,
      sessions: 12,
      screenPageViews: 20,
      engagementRate: 0.6,
      bounceRate: 0.4,
      averageSessionDuration: 42.5,
      sessionsPerUser: 1.5,
      screenPageViewsPerUser: 2.5
    };
    if (context === 'current-30') {
      return {
        ...defaults,
        activeUsers: 30,
        newUsers: 11,
        sessions: 45,
        screenPageViews: 90
      }[metric];
    }
    if (context === 'previous-30') {
      return {
        ...defaults,
        activeUsers: 20,
        newUsers: 7,
        sessions: 30,
        screenPageViews: 60
      }[metric];
    }
    return defaults[metric];
  };
  const propertyBody = {
    name: 'properties/123',
    account: 'accounts/456',
    displayName: 'Read-only Web Property',
    timeZone: 'UTC',
    currencyCode: 'USD',
    propertyType: 'PROPERTY_TYPE_ORDINARY',
    serviceLevel: 'GOOGLE_ANALYTICS_STANDARD'
  };
  const calls = installGoogleAnalyticsMock(call => {
    const pathName = call.url.pathname;
    if (call.url.hostname === 'oauth2.googleapis.com' && pathName === '/token') {
      const body = new URLSearchParams(call.options.body);
      assert.equal(body.get('client_id'), process.env.GA4_CLIENT_ID);
      assert.equal(body.get('client_secret'), process.env.GA4_CLIENT_SECRET);
      assert.equal(body.get('redirect_uri'), process.env.GA4_REDIRECT_URI);
      assert.equal(body.get('grant_type'), 'authorization_code');
      assert.ok(body.get('code_verifier'));
      return jsonResponse(200, {
        access_token: 'ga4-access-token',
        refresh_token: 'ga4-refresh-token',
        expires_in: 3600,
        token_type: 'Bearer',
        scope: GA4_SCOPES.join(' ')
      });
    }
    if (call.url.hostname === 'analyticsadmin.googleapis.com' && pathName === '/v1beta/accountSummaries') {
      assert.equal(call.options.headers.Authorization, 'Bearer ga4-access-token');
      assert.equal(call.url.searchParams.get('pageSize'), '200');
      return jsonResponse(200, {
        accountSummaries: [{
          account: 'accounts/456',
          displayName: 'Analytics Account',
          propertySummaries: [{ property: 'properties/123', displayName: 'Read-only Web Property' }]
        }]
      });
    }
    if (call.url.hostname === 'analyticsadmin.googleapis.com' && pathName === '/v1beta/properties/123') {
      assert.equal(call.options.headers.Authorization, 'Bearer ga4-access-token');
      return jsonResponse(200, propertyBody);
    }
    if (call.url.hostname === 'analyticsdata.googleapis.com' && pathName === '/v1beta/properties/123/metadata') {
      assert.equal(call.options.headers.Authorization, 'Bearer ga4-access-token');
      return jsonResponse(200, {
        dimensions: ['date', ...breakdownDimensions].map(apiName => ({ apiName, uiName: apiName })),
        metrics: GA4_METRICS.map(apiName => ({ apiName, uiName: apiName, type: 'TYPE_FLOAT' }))
      });
    }
    if (call.url.hostname === 'analyticsdata.googleapis.com' && pathName === '/v1beta/properties/123:checkCompatibility') {
      assert.equal(call.options.method, 'POST');
      assert.equal(call.options.headers.Authorization, 'Bearer ga4-access-token');
      const request = JSON.parse(call.options.body);
      return jsonResponse(200, {
        ...(request.dimensions.length > 0 ? {
          dimensionCompatibilities: request.dimensions.map(item => ({
            dimensionMetadata: { apiName: item.name },
            compatibility: 'COMPATIBLE'
          }))
        } : {}),
        metricCompatibilities: request.metrics.map(item => ({
          metricMetadata: { apiName: item.name },
          compatibility: 'COMPATIBLE'
        }))
      });
    }
    if (call.url.hostname === 'analyticsdata.googleapis.com' && pathName === '/v1beta/properties/123:runReport') {
      assert.equal(call.options.method, 'POST');
      assert.equal(call.options.headers.Authorization, 'Bearer ga4-access-token');
      const request = JSON.parse(call.options.body);
      assert.equal(request.returnPropertyQuota, true);
      const dimensions = request.dimensions.map(item => item.name);
      const metrics = request.metrics.map(item => item.name);
      const range = request.dateRanges[0];
      let context = 'default';
      const current30 = windows.ranges.find(item => item.key === '30d');
      const previous30 = windows.ranges.find(item => item.key === '30d_previous');
      if (range.startDate === current30.startDate && range.endDate === current30.endDate) context = 'current-30';
      if (range.startDate === previous30.startDate && range.endDate === previous30.endDate) context = 'previous-30';
      const dimensionValue = name => ({
        sessionSource: 'google',
        sessionMedium: 'organic',
        pagePath: '/home',
        pageTitle: 'Home',
        landingPagePlusQueryString: '/home?source=organic',
        deviceCategory: 'desktop',
        country: 'Pakistan',
        city: 'Lahore'
      })[name] || '(not set)';
      const rows = dimensions.includes('date')
        ? [
            {
              dimensionValues: [{ value: windows.endDate.replaceAll('-', '') }],
              metricValues: metrics.map(metric => ({ value: String(reportMetricValue(metric, 'default')) }))
            },
            {
              dimensionValues: [{ value: priorDailyKey }],
              metricValues: metrics.map(metric => ({ value: String(metric === 'activeUsers' ? 10 : reportMetricValue(metric, 'default')) }))
            }
          ]
        : [{
            dimensionValues: dimensions.map(name => ({ value: dimensionValue(name) })),
            metricValues: metrics.map(metric => ({ value: String(reportMetricValue(metric, context)) }))
          }];
      return jsonResponse(200, {
        dimensionHeaders: dimensions.map(name => ({ name })),
        metricHeaders: metrics.map(name => ({ name, type: 'TYPE_FLOAT' })),
        rows,
        rowCount: rows.length,
        metadata: {
          subjectToThresholding: dimensions.includes('city'),
          dataLossFromOtherRow: false
        },
        propertyQuota: {
          tokensPerDay: { consumed: 1, remaining: 9999 },
          potentiallyThresholdedRequestsPerHour: { consumed: dimensions.includes('city') ? 1 : 0, remaining: 119 }
        }
      });
    }
    if (call.url.hostname === 'oauth2.googleapis.com' && pathName === '/revoke') {
      const body = new URLSearchParams(call.options.body);
      assert.equal(body.get('token'), 'ga4-refresh-token');
      return jsonResponse(200, {});
    }
    throw new Error(`unexpected Google Analytics mock call: ${call.url.toString()}`);
  });

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/google-analytics/start`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { return_path: `/?workspace=${workspace.id}&view=connections&provider=google_analytics_4` }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  assert.equal(authorizationUrl.origin, 'https://accounts.google.com');
  assert.equal(authorizationUrl.searchParams.get('scope'), GA4_SCOPES.join(' '));
  assert.equal(authorizationUrl.searchParams.get('access_type'), 'offline');
  assert.equal(authorizationUrl.searchParams.get('prompt'), 'consent');
  assert.equal(authorizationUrl.searchParams.get('code_challenge_method'), 'S256');
  assert.equal(authorizationUrl.searchParams.get('client_id'), process.env.GA4_CLIENT_ID);
  const state = authorizationUrl.searchParams.get('state');
  const transactionRows = await db.query(
    `SELECT requested_scopes, redirect_uri, pkce_verifier_ciphertext,
            pkce_verifier_iv, pkce_verifier_tag, pkce_key_version
     FROM oauth_transactions WHERE state_hash = ? AND provider = 'google_analytics_4'`,
    [hashSecret(state)]
  );
  assert.deepEqual(
    typeof transactionRows[0].requested_scopes === 'string'
      ? JSON.parse(transactionRows[0].requested_scopes)
      : transactionRows[0].requested_scopes,
    GA4_SCOPES
  );
  assert.equal(transactionRows[0].redirect_uri, process.env.GA4_REDIRECT_URI);
  assert.ok(transactionRows[0].pkce_verifier_ciphertext);
  assert.notEqual(transactionRows[0].pkce_verifier_ciphertext, authorizationUrl.searchParams.get('code_challenge'));

  const callback = await requestApp(
    `/api/integrations/google-analytics/callback?code=ga4-provider-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(callback.statusCode, 303);
  assert.equal(new URL(callback.headers.location, 'http://localhost').searchParams.get('analytics'), 'selection_required');
  assert.equal(calls.length, 3);

  const replay = await requestApp(
    `/api/integrations/google-analytics/callback?code=ga4-provider-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(replay.statusCode, 303);
  assert.equal(new URL(replay.headers.location, 'http://localhost').searchParams.get('analytics'), 'failed');
  assert.equal(calls.length, 3, 'replayed state must fail before another Google call');

  const beforeSelection = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM data_sources WHERE workspace_id = ? AND provider = 'google_analytics_4') AS sources,
       (SELECT COUNT(*) FROM provider_resources WHERE workspace_id = ? AND provider = 'google_analytics_4') AS resources`,
    [workspace.id, workspace.id]
  );
  assert.equal(Number(beforeSelection[0].sources), 0);
  assert.equal(Number(beforeSelection[0].resources), 1);
  const catalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(catalog.statusCode, 200);
  const analyticsProvider = catalog.json().providers.find(provider => provider.id === 'google_analytics_4');
  assert.equal(analyticsProvider.status, 'selection_required');
  assert.equal(analyticsProvider.resources[0].selected, false);
  assert.equal(analyticsProvider.resources[0].available, true);
  assert.equal(analyticsProvider.resources[0].timezone, 'UTC');
  assert.equal(analyticsProvider.resources[0].currency, 'USD');

  const otherWorkspace = await createWorkspace(owner, 'Other GA4 Workspace');
  const crossWorkspaceSelection = await requestApp(
    `/api/workspaces/${otherWorkspace.id}/connections/google-analytics/select`,
    {
      method: 'POST',
      headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
      body: { resource_id: analyticsProvider.resources[0].id }
    }
  );
  assert.equal(crossWorkspaceSelection.statusCode, 404);

  const selection = await requestApp(`/api/workspaces/${workspace.id}/connections/google-analytics/select`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { resource_id: analyticsProvider.resources[0].id }
  });
  assert.equal(selection.statusCode, 201);
  const selectedConnection = selection.json().connection;
  assert.equal(selectedConnection.account.id, 'properties/123');
  const duplicateSelection = await requestApp(`/api/workspaces/${workspace.id}/connections/google-analytics/select`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { resource_id: analyticsProvider.resources[0].id }
  });
  assert.equal(duplicateSelection.statusCode, 409);

  const credentials = await db.query(
    `SELECT access_token_ciphertext, access_token_iv, access_token_tag,
            refresh_token_ciphertext, refresh_token_iv, refresh_token_tag, key_version
     FROM provider_authorization_credentials pac
     JOIN provider_authorizations pauth ON pauth.id = pac.provider_authorization_id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'google_analytics_4'`,
    [workspace.id]
  );
  assert.notEqual(credentials[0].access_token_ciphertext, 'ga4-access-token');
  assert.notEqual(credentials[0].refresh_token_ciphertext, 'ga4-refresh-token');
  assert.equal(decryptSecret({
    ciphertext: credentials[0].refresh_token_ciphertext,
    iv: credentials[0].refresh_token_iv,
    tag: credentials[0].refresh_token_tag,
    keyVersion: credentials[0].key_version
  }), 'ga4-refresh-token');

  const queued = await requestApp(`/api/workspaces/${workspace.id}/providers/google_analytics_4/sync-runs`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(queued.statusCode, 202);
  assert.equal(queued.json().status, 'queued');
  assert.equal(calls.length, 3, 'manual sync API must queue work without calling Google');

  const worker = await runDueSyncs({ timeBudgetSeconds: 20, leaseOwner: 'ga4-worker' });
  assert.equal(worker.processed, 1);
  assert.equal(worker.results[0].status, 'success');
  assert.ok(worker.results[0].metric_observation_count > 0);
  assert.ok(worker.results[0].dimension_observation_count > 0);
  const callsAfterWorker = calls.length;

  const dashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/google_analytics_4/dashboard?range=30d&connection_id=${selectedConnection.id}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(dashboard.statusCode, 200);
  const dashboardBody = dashboard.json();
  assert.equal(dashboardBody.connection.status, 'active');
  assert.equal(dashboardBody.property.id, 'properties/123');
  assert.equal(dashboardBody.property.timezone, 'UTC');
  assert.equal(dashboardBody.property.currency, 'USD');
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'ga4.active_users').value, 30);
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'ga4.active_users').baseline, 20);
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'ga4.sessions').value, 45);
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'ga4.screen_page_views').value, 90);
  assert.equal(dashboardBody.trend.length, 2);
  assert.equal(dashboardBody.breakdowns.length, GA4_BREAKDOWNS.length);
  assert.equal(dashboardBody.breakdowns.find(item => item.key === 'ga4.city').subject_to_thresholding, true);
  assert.equal(dashboardBody.availability.state, 'thresholded');
  assert.equal(calls.length, callsAfterWorker, 'dashboard must read stored observations only');

  const stored = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM provider_resource_observations WHERE workspace_id = ? AND provider = 'google_analytics_4') AS resources,
       (SELECT COUNT(*) FROM provider_metric_observations WHERE workspace_id = ? AND provider = 'google_analytics_4') AS metrics,
       (SELECT COUNT(*) FROM provider_dimension_observations WHERE workspace_id = ? AND provider = 'google_analytics_4') AS dimensions,
       (SELECT COUNT(*) FROM provider_request_events WHERE workspace_id = ? AND provider = 'google_analytics_4') AS requests`,
    [workspace.id, workspace.id, workspace.id, workspace.id]
  );
  assert.equal(Number(stored[0].resources), 1);
  assert.ok(Number(stored[0].metrics) >= GA4_METRICS.length * 8);
  assert.ok(Number(stored[0].dimensions) >= GA4_BREAKDOWNS.length * 3);
  assert.ok(Number(stored[0].requests) > 0);
  const requestEvents = await db.query(
    `SELECT request_category, method_name, status, failure_category
     FROM provider_request_events WHERE workspace_id = ? AND provider = 'google_analytics_4'`,
    [workspace.id]
  );
  assert.equal(JSON.stringify(requestEvents).includes('ga4-access-token'), false);
  assert.equal(JSON.stringify(requestEvents).includes('ga4-refresh-token'), false);

  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/google-analytics`, {
    method: 'DELETE',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(disconnect.statusCode, 200);
  assert.equal(disconnect.json().provider_revoke.success, true);
  assert.equal(calls.length, callsAfterWorker + 1);
  const purged = await db.query(
    `SELECT pauth.status, pauth.provider_subject,
            (SELECT COUNT(*) FROM provider_resources WHERE provider_authorization_id = pauth.id) AS resources,
            (SELECT COUNT(*) FROM provider_authorization_credentials WHERE provider_authorization_id = pauth.id) AS credentials,
            (SELECT COUNT(*) FROM data_sources WHERE workspace_id = pauth.workspace_id AND provider = 'google_analytics_4') AS sources,
            (SELECT COUNT(*) FROM provider_metric_observations WHERE workspace_id = pauth.workspace_id AND provider = 'google_analytics_4') AS metrics,
            (SELECT COUNT(*) FROM provider_dimension_observations WHERE workspace_id = pauth.workspace_id AND provider = 'google_analytics_4') AS dimensions
     FROM provider_authorizations pauth
     WHERE pauth.workspace_id = ? AND pauth.provider = 'google_analytics_4'`,
    [workspace.id]
  );
  assert.equal(purged[0].status, 'revoked');
  assert.equal(purged[0].provider_subject, null);
  assert.equal(Number(purged[0].resources), 0);
  assert.equal(Number(purged[0].credentials), 0);
  assert.equal(Number(purged[0].sources), 0);
  assert.equal(Number(purged[0].metrics), 0);
  assert.equal(Number(purged[0].dimensions), 0);
});

test('Facebook Pages uses explicit selection, encrypted Page tokens, worker-only insights, dashboards, and purge', async () => {
  await clearDatabase();
  const owner = await signIn('meta-facebook-owner@example.com');
  const workspace = await createWorkspace(owner, 'Meta Facebook Workspace');
  const metricValues = {
    page_follows: 101,
    page_daily_follows_unique: 3,
    page_daily_unfollows_unique: 1,
    page_post_engagements: 22,
    page_media_view: 70,
    page_total_media_view_unique: 55
  };
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  let discoveryCount = 0;
  const calls = installMetaMock(call => {
    const pathName = call.url.pathname;
    if (pathName === '/v25.0/oauth/access_token') {
      if (call.url.searchParams.get('grant_type') === 'fb_exchange_token') {
        assert.equal(call.url.searchParams.get('fb_exchange_token'), 'meta-short-token');
        return jsonResponse(200, { access_token: 'meta-long-token', token_type: 'bearer', expires_in: 60 * 24 * 60 * 60 });
      }
      assert.equal(call.url.searchParams.get('code'), 'meta-provider-code');
      assert.equal(call.url.searchParams.get('redirect_uri'), process.env.FACEBOOK_REDIRECT_URI);
      return jsonResponse(200, { access_token: 'meta-short-token', token_type: 'bearer' });
    }
    if (pathName === '/v25.0/debug_token') {
      assert.equal(call.url.searchParams.get('input_token'), 'meta-long-token');
      return jsonResponse(200, {
        data: {
          app_id: process.env.META_APP_ID,
          user_id: 'meta-user-1',
          is_valid: true,
          expires_at: Math.floor(Date.now() / 1000) + 60 * 24 * 60 * 60
        }
      });
    }
    if (pathName === '/v25.0/me/permissions') {
      if (call.options.method === 'DELETE') return jsonResponse(200, { success: true });
      assert.equal(call.options.headers.Authorization, 'Bearer meta-long-token');
      return jsonResponse(200, {
        data: [
          { permission: 'pages_show_list', status: 'granted' },
          { permission: 'pages_read_engagement', status: 'granted' },
          { permission: 'read_insights', status: 'granted' },
          { permission: 'public_profile', status: 'granted' }
        ]
      });
    }
    if (pathName === '/v25.0/me' && call.url.searchParams.get('fields') === 'id,name') {
      return jsonResponse(200, { id: 'meta-user-1', name: 'Meta Test User' });
    }
    if (pathName === '/v25.0/me/accounts') {
      assert.equal(call.options.headers.Authorization, 'Bearer meta-long-token');
      discoveryCount += 1;
      if (discoveryCount > 1) {
        return jsonResponse(200, {
          data: [{
            id: 'page-2',
            name: 'Different Read Only Page',
            tasks: ['ANALYZE'],
            access_token: 'meta-page-token-2'
          }]
        });
      }
      return jsonResponse(200, {
        data: [{
          id: 'page-1',
          name: 'Read Only Page',
          tasks: ['ANALYZE'],
          access_token: 'meta-page-token',
          picture: { data: { url: 'https://img.example/page-1.jpg' } }
        }]
      });
    }
    assert.equal(call.options.headers.Authorization, 'Bearer meta-page-token');
    assert.ok(call.url.searchParams.get('appsecret_proof'));
    if (pathName === '/v25.0/page-1' && call.url.searchParams.get('fields')) {
      return jsonResponse(200, {
        id: 'page-1', name: 'Read Only Page', picture: { data: { url: 'https://img.example/page-1.jpg' } }
      });
    }
    if (pathName === '/v25.0/page-1/insights') {
      const metric = call.url.searchParams.get('metric');
      assert.ok(Object.hasOwn(metricValues, metric));
      return jsonResponse(200, {
        data: [{ name: metric, period: 'day', values: [{ value: metricValues[metric], end_time: tomorrow }] }]
      });
    }
    if (pathName === '/v25.0/page-1/posts') {
      return jsonResponse(200, {
        data: [{
          id: 'page-1_post-1',
          message: 'A read-only Page post',
          created_time: `${yesterday}T10:00:00+0000`,
          permalink_url: 'https://facebook.example/page-1/posts/1',
          reactions: { summary: { total_count: 9 } },
          comments: { summary: { total_count: 4 } },
          shares: { count: 2 }
        }]
      });
    }
    if (pathName === '/v25.0/page-1_post-1/insights') {
      return jsonResponse(200, {
        data: [
          { name: 'post_media_view', values: [{ value: 88 }] },
          { name: 'post_total_media_view_unique', values: [{ value: 66 }] }
        ]
      });
    }
    throw new Error(`unexpected Meta mock call: ${pathName}`);
  });

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook/start`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { return_path: `/?workspace=${workspace.id}&view=connections&provider=facebook_pages` }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  assert.equal(authorizationUrl.pathname, '/v25.0/dialog/oauth');
  assert.equal(
    authorizationUrl.searchParams.get('config_id'),
    process.env.META_FACEBOOK_LOGIN_CONFIG_ID
  );
  assert.equal(authorizationUrl.searchParams.has('scope'), false);
  const state = authorizationUrl.searchParams.get('state');
  const transactionRows = await db.query(
    `SELECT provider_config_id FROM oauth_transactions
     WHERE state_hash = ? AND provider = 'facebook_pages'`,
    [hashSecret(state)]
  );
  assert.equal(transactionRows[0].provider_config_id, process.env.META_FACEBOOK_LOGIN_CONFIG_ID);

  const originalFacebookConfigId = process.env.META_FACEBOOK_LOGIN_CONFIG_ID;
  let mismatchedCallback;
  try {
    process.env.META_FACEBOOK_LOGIN_CONFIG_ID = 'changed-facebook-login-config-id';
    mismatchedCallback = await requestApp(
      `/api/integrations/facebook/callback?code=meta-provider-code&state=${encodeURIComponent(state)}`,
      { headers: { cookie: cookieHeader(owner.cookies) } }
    );
  } finally {
    process.env.META_FACEBOOK_LOGIN_CONFIG_ID = originalFacebookConfigId;
  }
  assert.equal(mismatchedCallback.statusCode, 303);
  assert.equal(
    new URL(mismatchedCallback.headers.location, 'http://localhost').searchParams.get('facebook'),
    'configuration_error'
  );
  assert.equal(calls.length, 0, 'config-bound state must fail before any Meta token exchange');

  const callback = await requestApp(
    `/api/integrations/facebook/callback?code=meta-provider-code&state=${encodeURIComponent(state)}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(callback.statusCode, 303);
  assert.equal(new URL(callback.headers.location, 'http://localhost').searchParams.get('facebook'), 'selection_required');
  assert.equal(calls.length, 6);

  const beforeSelection = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM data_sources WHERE workspace_id = ? AND provider = 'facebook_pages') AS sources,
       (SELECT COUNT(*) FROM provider_resources WHERE workspace_id = ? AND provider = 'facebook_pages') AS resources`,
    [workspace.id, workspace.id]
  );
  assert.equal(Number(beforeSelection[0].sources), 0);
  assert.equal(Number(beforeSelection[0].resources), 1);

  const catalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(catalog.statusCode, 200);
  const facebook = catalog.json().providers.find(provider => provider.id === 'facebook_pages');
  assert.equal(facebook.status, 'selection_required');
  assert.equal(facebook.resources[0].selected, false);
  assert.equal(facebook.resources[0].available, true);

  const selection = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook/select`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { resource_id: facebook.resources[0].id }
  });
  assert.equal(selection.statusCode, 201);
  const selectedConnection = selection.json().connection;
  const duplicateSelection = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook/select`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { resource_id: facebook.resources[0].id }
  });
  assert.equal(duplicateSelection.statusCode, 409);

  const credentialRows = await db.query(
    `SELECT pac.access_token_ciphertext AS user_ciphertext,
            prc.access_token_ciphertext AS resource_ciphertext,
            prc.access_token_iv, prc.access_token_tag, prc.key_version
     FROM provider_authorizations pauth
     JOIN provider_authorization_credentials pac ON pac.provider_authorization_id = pauth.id
     JOIN provider_resources pr ON pr.provider_authorization_id = pauth.id
     JOIN provider_resource_credentials prc ON prc.provider_resource_id = pr.id
     WHERE pauth.workspace_id = ? AND pauth.provider = 'facebook_pages'`,
    [workspace.id]
  );
  assert.notEqual(credentialRows[0].user_ciphertext, 'meta-long-token');
  assert.notEqual(credentialRows[0].resource_ciphertext, 'meta-page-token');
  assert.equal(decryptSecret({
    ciphertext: credentialRows[0].resource_ciphertext,
    iv: credentialRows[0].access_token_iv,
    tag: credentialRows[0].access_token_tag,
    keyVersion: credentialRows[0].key_version
  }), 'meta-page-token');

  const queued = await requestApp(`/api/workspaces/${workspace.id}/providers/facebook_pages/sync-runs`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(queued.statusCode, 202);
  assert.equal(queued.json().status, 'queued');
  assert.equal(calls.length, 6, 'manual API request must not call the Graph data API');

  const workerResult = await runDueSyncs({ timeBudgetSeconds: 5, leaseOwner: 'meta-facebook-worker' });
  assert.equal(workerResult.processed, 1);
  assert.equal(workerResult.results[0].status, 'success');
  assert.equal(workerResult.results[0].counts.content_seen_count, 1);
  assert.equal(calls.length, 15);

  const dashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/facebook_pages/dashboard?range=7d`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(dashboard.statusCode, 200);
  const dashboardBody = dashboard.json();
  assert.equal(dashboardBody.connection.status, 'active');
  assert.equal(dashboardBody.account.display_name, 'Read Only Page');
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'page_follows').value, 101);
  assert.equal(dashboardBody.content[0].view_count, 88);
  assert.equal(dashboardBody.content[0].like_count, 9);
  assert.equal(calls.length, 15, 'dashboard API must read stored snapshots only');

  const snapshotRows = await db.query(
    `SELECT
       (SELECT COUNT(*) FROM meta_account_insight_snapshots WHERE workspace_id = ?) AS account_snapshots,
       (SELECT COUNT(*) FROM content_items WHERE workspace_id = ?) AS content_items,
       (SELECT COUNT(*) FROM content_metric_snapshots WHERE workspace_id = ?) AS content_snapshots`,
    [workspace.id, workspace.id, workspace.id]
  );
  assert.equal(Number(snapshotRows[0].account_snapshots), 2);
  assert.equal(Number(snapshotRows[0].content_items), 1);
  assert.equal(Number(snapshotRows[0].content_snapshots), 1);

  const reauthorize = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook/start`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: {
      return_path: `/?workspace=${workspace.id}&view=connections&provider=facebook_pages`,
      connection_id: selectedConnection.id
    }
  });
  assert.equal(reauthorize.statusCode, 200);
  const reauthorizeUrl = new URL(reauthorize.json().authorization_url);
  const reauthorizeCallback = await requestApp(
    `/api/integrations/facebook/callback?code=meta-provider-code&state=${encodeURIComponent(reauthorizeUrl.searchParams.get('state'))}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(reauthorizeCallback.statusCode, 303);
  assert.equal(
    new URL(reauthorizeCallback.headers.location, 'http://localhost').searchParams.get('facebook'),
    'selected_resource_unavailable'
  );
  assert.equal(calls.length, 21);
  const afterReauthorizationCatalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(afterReauthorizationCatalog.statusCode, 200);
  const reauthorizedFacebook = afterReauthorizationCatalog.json().providers.find(
    provider => provider.id === 'facebook_pages'
  );
  assert.equal(reauthorizedFacebook.connection.id, selectedConnection.id);
  assert.equal(reauthorizedFacebook.connection.status, 'reconnect_required');
  assert.equal(reauthorizedFacebook.connection.account.id, 'page-1');
  assert.equal(
    reauthorizedFacebook.resources.find(resource => resource.provider_resource_id === 'page-1').available,
    false
  );
  assert.equal(
    reauthorizedFacebook.resources.find(resource => resource.provider_resource_id === 'page-2').selected,
    false
  );
  const sourceCountAfterReauthorization = await db.query(
    `SELECT COUNT(*) AS count FROM data_sources
     WHERE workspace_id = ? AND provider = 'facebook_pages'`,
    [workspace.id]
  );
  assert.equal(Number(sourceCountAfterReauthorization[0].count), 1);
  assert.equal(calls.length, 21, 'catalog reads must not call Meta or select a replacement');

  const disconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook`, {
    method: 'DELETE',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: selectedConnection.id }
  });
  assert.equal(disconnect.statusCode, 200);
  assert.equal(disconnect.json().provider_revoke.success, true);
  assert.equal(calls.length, 22);
  const afterDisconnect = await db.query(
    `SELECT pauth.status, pauth.provider_subject,
            (SELECT COUNT(*) FROM provider_resources pr WHERE pr.provider_authorization_id = pauth.id) AS resources,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac WHERE pac.provider_authorization_id = pauth.id) AS credentials,
            (SELECT COUNT(*) FROM data_sources ds WHERE ds.workspace_id = pauth.workspace_id AND ds.provider = 'facebook_pages') AS sources
     FROM provider_authorizations pauth
     WHERE pauth.workspace_id = ? AND pauth.provider = 'facebook_pages'`,
    [workspace.id]
  );
  assert.equal(afterDisconnect[0].status, 'revoked');
  assert.equal(afterDisconnect[0].provider_subject, null);
  assert.equal(Number(afterDisconnect[0].resources), 0);
  assert.equal(Number(afterDisconnect[0].credentials), 0);
  assert.equal(Number(afterDisconnect[0].sources), 0);
});

test('Instagram Facebook Login accepts only the exact read-only set and excludes Stories from worker history', async () => {
  await clearDatabase();
  const owner = await signIn('meta-instagram-owner@example.com');
  const workspace = await createWorkspace(owner, 'Meta Instagram Workspace');
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const accountMetricValues = {
    views: 120,
    reach: 80,
    accounts_engaged: 30,
    total_interactions: 45,
    likes: 20,
    comments: 5,
    saves: 6,
    shares: 3
  };
  const calls = installMetaMock(call => {
    const pathName = call.url.pathname;
    if (pathName === '/v25.0/oauth/access_token') {
      return call.url.searchParams.get('grant_type') === 'fb_exchange_token'
        ? jsonResponse(200, { access_token: 'ig-long-token', expires_in: 60 * 24 * 60 * 60 })
        : jsonResponse(200, { access_token: 'ig-short-token' });
    }
    if (pathName === '/v25.0/debug_token') {
      return jsonResponse(200, {
        data: {
          app_id: process.env.META_APP_ID,
          user_id: 'ig-meta-user',
          is_valid: true,
          expires_at: Math.floor(Date.now() / 1000) + 60 * 24 * 60 * 60
        }
      });
    }
    if (pathName === '/v25.0/me/permissions') {
      return jsonResponse(200, {
        data: [
          { permission: 'instagram_basic', status: 'granted' },
          { permission: 'instagram_manage_insights', status: 'granted' },
          { permission: 'pages_show_list', status: 'granted' },
          { permission: 'pages_read_engagement', status: 'granted' },
          { permission: 'public_profile', status: 'granted' }
        ]
      });
    }
    if (pathName === '/v25.0/me' && call.url.searchParams.get('fields') === 'id,name') {
      return jsonResponse(200, { id: 'ig-meta-user', name: 'Instagram Meta User' });
    }
    if (pathName === '/v25.0/me/accounts') {
      return jsonResponse(200, {
        data: [{
          id: 'ig-source-page',
          name: 'Instagram Source Page',
          tasks: ['ANALYZE'],
          access_token: 'ig-page-token',
          instagram_business_account: {
            id: 'ig-account-1',
            username: 'read_only_studio',
            name: 'Read Only Studio',
            profile_picture_url: 'https://img.example/ig.jpg',
            followers_count: 250,
            media_count: 12
          }
        }]
      });
    }
    assert.equal(call.options.headers.Authorization, 'Bearer ig-page-token');
    if (pathName === '/v25.0/ig-account-1' && call.url.searchParams.get('fields')) {
      return jsonResponse(200, {
        id: 'ig-account-1',
        username: 'read_only_studio',
        name: 'Read Only Studio',
        profile_picture_url: 'https://img.example/ig.jpg',
        followers_count: 250,
        media_count: 12
      });
    }
    if (pathName === '/v25.0/ig-account-1/insights') {
      const metric = call.url.searchParams.get('metric');
      assert.ok(Object.hasOwn(accountMetricValues, metric));
      assert.equal(call.url.searchParams.get('metric_type'), 'total_value');
      return jsonResponse(200, {
        data: [{ name: metric, period: 'day', total_value: { value: accountMetricValues[metric] } }]
      });
    }
    if (pathName === '/v25.0/ig-account-1/media') {
      return jsonResponse(200, {
        data: [{
          id: 'ig-story-1', media_type: 'IMAGE', media_product_type: 'STORY', timestamp: yesterday
        }, {
          id: 'ig-feed-1', caption: 'Read-only feed media', media_type: 'IMAGE',
          media_product_type: 'FEED', permalink: 'https://instagram.example/p/feed-1',
          timestamp: yesterday, like_count: 11, comments_count: 2
        }]
      });
    }
    if (pathName === '/v25.0/ig-feed-1/insights') {
      const requested = call.url.searchParams.get('metric').split(',').sort();
      assert.deepEqual(requested, ['reach', 'saved', 'shares', 'views']);
      return jsonResponse(200, {
        data: [
          { name: 'views', values: [{ value: 140 }] },
          { name: 'reach', values: [{ value: 95 }] },
          { name: 'shares', values: [{ value: 7 }] },
          { name: 'saved', values: [{ value: 8 }] }
        ]
      });
    }
    throw new Error(`unexpected Instagram mock call: ${pathName}`);
  });

  const start = await requestApp(`/api/workspaces/${workspace.id}/connections/instagram/start`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { return_path: `/?workspace=${workspace.id}&view=connections&provider=instagram` }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  assert.equal(
    authorizationUrl.searchParams.get('config_id'),
    process.env.META_INSTAGRAM_LOGIN_CONFIG_ID
  );
  assert.equal(authorizationUrl.searchParams.has('scope'), false);
  const callback = await requestApp(
    `/api/integrations/instagram/callback?code=ig-provider-code&state=${encodeURIComponent(authorizationUrl.searchParams.get('state'))}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(callback.statusCode, 303);
  assert.equal(new URL(callback.headers.location, 'http://localhost').searchParams.get('instagram'), 'selection_required');
  assert.equal(calls.length, 6);

  const catalog = await requestApp(`/api/workspaces/${workspace.id}/provider-catalog`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  const instagram = catalog.json().providers.find(provider => provider.id === 'instagram');
  assert.equal(instagram.resources[0].display_name, 'Read Only Studio');
  assert.equal(instagram.resources[0].source_page_name, 'Instagram Source Page');
  const selection = await requestApp(`/api/workspaces/${workspace.id}/connections/instagram/select`, {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { resource_id: instagram.resources[0].id }
  });
  assert.equal(selection.statusCode, 201);
  const workerResult = await runDueSyncs({ timeBudgetSeconds: 5, leaseOwner: 'meta-instagram-worker' });
  assert.equal(workerResult.processed, 1);
  assert.equal(workerResult.results[0].status, 'success');
  assert.equal(workerResult.results[0].excluded_instagram_stories, 1);
  assert.equal(workerResult.results[0].counts.content_seen_count, 1);
  assert.equal(calls.length, 33);

  const dashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/instagram/dashboard?range=7d`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(dashboard.statusCode, 200);
  const dashboardBody = dashboard.json();
  assert.equal(dashboardBody.account.username, 'read_only_studio');
  assert.equal(dashboardBody.range.provider_period_days, 7);
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'followers').value, 250);
  assert.equal(dashboardBody.metrics.find(metric => metric.key === 'views').value, 120);
  assert.equal(
    dashboardBody.metrics.find(metric => metric.key === 'views').semantics,
    'provider_total_over_7_days'
  );
  assert.equal(dashboardBody.content.length, 1);
  assert.equal(dashboardBody.content[0].provider_content_id, 'ig-feed-1');
  assert.equal(dashboardBody.content[0].view_count, 140);
  assert.match(dashboardBody.availability.note, /Stories are excluded/);
  const customFrom = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const customTo = new Date().toISOString().slice(0, 10);
  const customDashboard = await requestApp(
    `/api/workspaces/${workspace.id}/providers/instagram/dashboard?range=custom&from=${customFrom}&to=${customTo}`,
    { headers: { cookie: cookieHeader(owner.cookies) } }
  );
  assert.equal(customDashboard.statusCode, 200);
  assert.equal(customDashboard.json().range.provider_period_days, null);
  assert.equal(customDashboard.json().metrics.find(metric => metric.key === 'views').value, null);
  assert.equal(
    customDashboard.json().metrics.find(metric => metric.key === 'views').semantics,
    'unsupported_custom_period'
  );
  assert.match(customDashboard.json().availability.note, /unavailable for custom ranges/);
  assert.equal(calls.length, 33, 'custom dashboards must read stored data without synthesizing totals');
  const storyRows = await db.query(
    `SELECT COUNT(*) AS count FROM content_items
     WHERE workspace_id = ? AND provider_content_id = 'ig-story-1'`,
    [workspace.id]
  );
  assert.equal(Number(storyRows[0].count), 0);
  const periodRows = await db.query(
    `SELECT range_days, range_start_date, range_end_date
     FROM meta_account_insight_snapshots
     WHERE workspace_id = ? AND provider = 'instagram' AND snapshot_kind = 'period'
     ORDER BY range_days`,
    [workspace.id]
  );
  assert.deepEqual(periodRows.map(row => Number(row.range_days)), [7, 30, 90]);
  assert.equal(periodRows.every(row => row.range_start_date && row.range_end_date), true);
});

test('Meta disconnect preserves a selected sibling grant and purges unselected sibling credentials after final revoke', async () => {
  await clearDatabase();
  const owner = await signIn('meta-shared-grant-owner@example.com');
  const workspace = await createWorkspace(owner, 'Meta Shared Grant Workspace');
  const identifiers = {};

  for (const provider of ['facebook_pages', 'instagram']) {
    const authorizationId = crypto.randomUUID();
    const resourceId = crypto.randomUUID();
    const dataSourceId = crypto.randomUUID();
    const connectionId = crypto.randomUUID();
    const encrypted = encryptSecret(`${provider}-shared-user-token`);
    identifiers[provider] = { authorizationId, resourceId, dataSourceId, connectionId };
    await db.query(
      `INSERT INTO provider_authorizations
        (id, workspace_id, provider, actor_user_id, provider_subject, display_name, status, granted_at)
       VALUES (?, ?, ?, ?, 'shared-meta-subject', 'Shared Meta User', 'active', UTC_TIMESTAMP(3))`,
      [authorizationId, workspace.id, provider, owner.user.id]
    );
    await db.query(
      `INSERT INTO provider_authorization_credentials
        (id, provider_authorization_id, access_token_ciphertext, access_token_iv,
         access_token_tag, key_version, token_type, access_expires_at)
       VALUES (?, ?, ?, ?, ?, ?, 'Bearer', DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 DAY))`,
      [
        crypto.randomUUID(), authorizationId, encrypted.ciphertext, encrypted.iv,
        encrypted.tag, encrypted.keyVersion
      ]
    );
    await db.query(
      `INSERT INTO provider_resources
        (id, provider_authorization_id, workspace_id, provider, resource_type,
         provider_resource_id, display_name, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?, JSON_OBJECT('selectable', TRUE, 'discoveryStatus', 'available'))`,
      [
        resourceId, authorizationId, workspace.id, provider,
        provider === 'facebook_pages' ? 'facebook_page' : 'instagram_account',
        `${provider}-shared-resource`, `Shared ${provider} resource`
      ]
    );
    await db.query(
      `INSERT INTO data_sources (id, workspace_id, provider, status, next_sync_at)
       VALUES (?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
      [dataSourceId, workspace.id, provider]
    );
    await db.query(
      `INSERT INTO workspace_provider_connections
        (id, workspace_id, provider_resource_id, data_source_id, provider, status, next_sync_at)
       VALUES (?, ?, ?, ?, ?, 'active', UTC_TIMESTAMP(3))`,
      [connectionId, workspace.id, resourceId, dataSourceId, provider]
    );
  }

  const calls = installMetaMock(call => {
    assert.equal(call.url.pathname, '/v25.0/me/permissions');
    assert.equal(call.options.method, 'DELETE');
    assert.equal(call.options.headers.Authorization, 'Bearer instagram-shared-user-token');
    return jsonResponse(200, { success: true });
  });

  const facebookDisconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/facebook`, {
    method: 'DELETE',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: identifiers.facebook_pages.connectionId }
  });
  assert.equal(facebookDisconnect.statusCode, 200);
  assert.equal(facebookDisconnect.json().provider_grant_preserved, true);
  assert.equal(facebookDisconnect.json().provider_revoke.attempted, false);
  assert.equal(facebookDisconnect.json().provider_revoke.outcome_category, 'sibling_meta_grant_preserved');
  assert.equal(calls.length, 0);
  const afterFacebook = await db.query(
    `SELECT
       (SELECT status FROM provider_authorizations WHERE id = ?) AS facebook_status,
       (SELECT status FROM provider_authorizations WHERE id = ?) AS instagram_status,
       (SELECT COUNT(*) FROM workspace_provider_connections WHERE id = ?) AS instagram_connections`,
    [
      identifiers.facebook_pages.authorizationId,
      identifiers.instagram.authorizationId,
      identifiers.instagram.connectionId
    ]
  );
  assert.equal(afterFacebook[0].facebook_status, 'revoked');
  assert.equal(afterFacebook[0].instagram_status, 'active');
  assert.equal(Number(afterFacebook[0].instagram_connections), 1);

  const unselectedAuthorizationId = crypto.randomUUID();
  const unselectedResourceId = crypto.randomUUID();
  const unselectedUserToken = encryptSecret('unselected-shared-user-token');
  const unselectedPageToken = encryptSecret('unselected-shared-page-token');
  await db.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, provider_subject, display_name, status, granted_at)
     VALUES (?, ?, 'facebook_pages', ?, 'shared-meta-subject', 'Unselected Shared User',
             'active', UTC_TIMESTAMP(3))`,
    [unselectedAuthorizationId, workspace.id, owner.user.id]
  );
  await db.query(
    `INSERT INTO provider_authorization_credentials
      (id, provider_authorization_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, token_type, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, 'Bearer', DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 DAY))`,
    [
      crypto.randomUUID(), unselectedAuthorizationId, unselectedUserToken.ciphertext,
      unselectedUserToken.iv, unselectedUserToken.tag, unselectedUserToken.keyVersion
    ]
  );
  await db.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type,
       provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'facebook_pages', 'facebook_page', 'unselected-shared-page',
             'Unselected shared Page', JSON_OBJECT('selectable', TRUE))`,
    [unselectedResourceId, unselectedAuthorizationId, workspace.id]
  );
  await db.query(
    `INSERT INTO provider_resource_credentials
      (id, provider_resource_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, token_type, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, 'Bearer', DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 DAY))`,
    [
      crypto.randomUUID(), unselectedResourceId, unselectedPageToken.ciphertext,
      unselectedPageToken.iv, unselectedPageToken.tag, unselectedPageToken.keyVersion
    ]
  );

  const instagramDisconnect = await requestApp(`/api/workspaces/${workspace.id}/connections/instagram`, {
    method: 'DELETE',
    headers: { cookie: cookieHeader(owner.cookies), 'x-csrf-token': owner.csrf },
    body: { connection_id: identifiers.instagram.connectionId }
  });
  assert.equal(instagramDisconnect.statusCode, 200);
  assert.equal(instagramDisconnect.json().provider_grant_preserved, false);
  assert.equal(instagramDisconnect.json().provider_revoke.success, true);
  assert.equal(calls.length, 1);
  const afterFinalDisconnect = await db.query(
    `SELECT status, provider_subject,
            (SELECT COUNT(*) FROM provider_authorization_credentials
             WHERE provider_authorization_id = ?) AS credentials,
            (SELECT COUNT(*) FROM provider_resources
             WHERE provider_authorization_id = ?) AS resources
     FROM provider_authorizations WHERE id = ?`,
    [unselectedAuthorizationId, unselectedAuthorizationId, unselectedAuthorizationId]
  );
  assert.equal(afterFinalDisconnect[0].status, 'revoked');
  assert.equal(afterFinalDisconnect[0].provider_subject, null);
  assert.equal(Number(afterFinalDisconnect[0].credentials), 0);
  assert.equal(Number(afterFinalDisconnect[0].resources), 0);
});

test('Meta worker purges expired unselected authorizations after the retention deadline', async () => {
  await clearDatabase();
  const owner = await signIn('meta-expiry-owner@example.com');
  const workspace = await createWorkspace(owner, 'Meta Expiry Workspace');
  const authorizationId = crypto.randomUUID();
  const resourceId = crypto.randomUUID();
  const userToken = encryptSecret('expired-meta-user-token');
  const pageToken = encryptSecret('expired-meta-page-token');
  await db.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, provider_subject, display_name,
       status, granted_at, deletion_due_at)
     VALUES (?, ?, 'facebook_pages', ?, 'expired-meta-subject', 'Expired Meta User',
             'reconnect_required', DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 91 DAY),
             DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 1 SECOND))`,
    [authorizationId, workspace.id, owner.user.id]
  );
  await db.query(
    `INSERT INTO provider_authorization_credentials
      (id, provider_authorization_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 31 DAY))`,
    [crypto.randomUUID(), authorizationId, userToken.ciphertext, userToken.iv, userToken.tag, userToken.keyVersion]
  );
  await db.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type,
       provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'facebook_pages', 'facebook_page', 'expired-unselected-page',
             'Expired unselected Page', JSON_OBJECT('selectable', FALSE))`,
    [resourceId, authorizationId, workspace.id]
  );
  await db.query(
    `INSERT INTO provider_resource_credentials
      (id, provider_resource_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, DATE_SUB(UTC_TIMESTAMP(3), INTERVAL 31 DAY))`,
    [crypto.randomUUID(), resourceId, pageToken.ciphertext, pageToken.iv, pageToken.tag, pageToken.keyVersion]
  );

  const worker = await runDueSyncs({ timeBudgetSeconds: 1, leaseOwner: 'meta-expiry-worker' });
  assert.equal(worker.reconciled_meta_authorizations, 1);
  assert.equal(worker.processed, 0);
  const rows = await db.query(
    `SELECT status, provider_subject, deletion_due_at,
            (SELECT COUNT(*) FROM provider_authorization_credentials
             WHERE provider_authorization_id = ?) AS credentials,
            (SELECT COUNT(*) FROM provider_resources
             WHERE provider_authorization_id = ?) AS resources
     FROM provider_authorizations WHERE id = ?`,
    [authorizationId, authorizationId, authorizationId]
  );
  assert.equal(rows[0].status, 'revoked');
  assert.equal(rows[0].provider_subject, null);
  assert.equal(rows[0].deletion_due_at, null);
  assert.equal(Number(rows[0].credentials), 0);
  assert.equal(Number(rows[0].resources), 0);
});

test('Meta signed data-deletion callback is authenticated, replay-safe, and purges both credentials and snapshots', async () => {
  await clearDatabase();
  const owner = await signIn('meta-deletion-owner@example.com');
  const workspace = await createWorkspace(owner, 'Meta Deletion Workspace');
  const authorizationId = crypto.randomUUID();
  const resourceId = crypto.randomUUID();
  const dataSourceId = crypto.randomUUID();
  const connectionId = crypto.randomUUID();
  const runId = crypto.randomUUID();
  const userToken = encryptSecret('meta-deletion-user-token');
  const pageToken = encryptSecret('meta-deletion-page-token');
  await db.query(
    `INSERT INTO provider_authorizations
      (id, workspace_id, provider, actor_user_id, provider_subject, display_name,
       status, auth_product, api_version, granted_at)
     VALUES (?, ?, 'facebook_pages', ?, 'meta-delete-user', 'Delete User',
             'active', 'analytics', 'v25.0', UTC_TIMESTAMP(3))`,
    [authorizationId, workspace.id, owner.user.id]
  );
  await db.query(
    `INSERT INTO provider_authorization_credentials
      (id, provider_authorization_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 DAY))`,
    [crypto.randomUUID(), authorizationId, userToken.ciphertext, userToken.iv, userToken.tag, userToken.keyVersion]
  );
  await db.query(
    `INSERT INTO provider_resources
      (id, provider_authorization_id, workspace_id, provider, resource_type,
       provider_resource_id, display_name, metadata)
     VALUES (?, ?, ?, 'facebook_pages', 'facebook_page', 'delete-page', 'Delete Page', JSON_OBJECT())`,
    [resourceId, authorizationId, workspace.id]
  );
  await db.query(
    `INSERT INTO provider_resource_credentials
      (id, provider_resource_id, access_token_ciphertext, access_token_iv,
       access_token_tag, key_version, access_expires_at)
     VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL 30 DAY))`,
    [crypto.randomUUID(), resourceId, pageToken.ciphertext, pageToken.iv, pageToken.tag, pageToken.keyVersion]
  );
  await db.query(
    `INSERT INTO data_sources (id, workspace_id, provider, status)
     VALUES (?, ?, 'facebook_pages', 'active')`,
    [dataSourceId, workspace.id]
  );
  await db.query(
    `INSERT INTO workspace_provider_connections
      (id, workspace_id, provider_resource_id, data_source_id, provider, status)
     VALUES (?, ?, ?, ?, 'facebook_pages', 'active')`,
    [connectionId, workspace.id, resourceId, dataSourceId]
  );
  await db.query(
    `INSERT INTO sync_runs
      (id, workspace_id, data_source_id, trigger_type, status, finished_at)
     VALUES (?, ?, ?, 'scheduled', 'success', UTC_TIMESTAMP(3))`,
    [runId, workspace.id, dataSourceId]
  );
  await db.query(
    `INSERT INTO meta_account_insight_snapshots
      (id, workspace_id, data_source_id, workspace_provider_connection_id,
       sync_run_id, provider, snapshot_kind, report_date, observed_at,
       metric_values, availability)
     VALUES (?, ?, ?, ?, ?, 'facebook_pages', 'profile', UTC_DATE(), UTC_TIMESTAMP(3),
             JSON_OBJECT('followers', 10), JSON_OBJECT('followers', 'available'))`,
    [crypto.randomUUID(), workspace.id, dataSourceId, connectionId, runId]
  );

  const signedRequest = metaSignedRequest('meta-delete-user');
  const body = new URLSearchParams({ signed_request: signedRequest }).toString();
  const deletion = await requestApp('/api/integrations/meta/data-deletion', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body
  });
  assert.equal(deletion.statusCode, 200);
  const deletionBody = deletion.json();
  assert.ok(deletionBody.confirmation_code);
  assert.equal(
    deletionBody.url,
    `${process.env.BASE_URL}/api/integrations/meta/deletion-status/${encodeURIComponent(deletionBody.confirmation_code)}`
  );

  const replay = await requestApp('/api/integrations/meta/data-deletion', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body
  });
  assert.equal(replay.statusCode, 200);
  assert.equal(replay.json().confirmation_code, deletionBody.confirmation_code);
  const status = await requestApp(
    `/api/integrations/meta/deletion-status/${encodeURIComponent(deletionBody.confirmation_code)}`
  );
  assert.equal(status.statusCode, 200);
  assert.equal(status.json().status, 'completed');
  assert.equal(status.json().authorization_count, 1);

  const purged = await db.query(
    `SELECT pauth.status, pauth.provider_subject,
            (SELECT COUNT(*) FROM provider_authorization_credentials pac WHERE pac.provider_authorization_id = pauth.id) AS auth_credentials,
            (SELECT COUNT(*) FROM provider_resources pr WHERE pr.provider_authorization_id = pauth.id) AS resources,
            (SELECT COUNT(*) FROM data_sources ds WHERE ds.workspace_id = pauth.workspace_id AND ds.provider = 'facebook_pages') AS sources,
            (SELECT COUNT(*) FROM meta_account_insight_snapshots mais WHERE mais.workspace_id = pauth.workspace_id) AS snapshots,
            (SELECT COUNT(*) FROM meta_callback_events mce WHERE mce.provider_subject_hash = SHA2('meta-delete-user', 256)) AS callback_events
     FROM provider_authorizations pauth WHERE pauth.id = ?`,
    [authorizationId]
  );
  assert.equal(purged[0].status, 'revoked');
  assert.equal(purged[0].provider_subject, null);
  assert.equal(Number(purged[0].auth_credentials), 0);
  assert.equal(Number(purged[0].resources), 0);
  assert.equal(Number(purged[0].sources), 0);
  assert.equal(Number(purged[0].snapshots), 0);
  assert.equal(Number(purged[0].callback_events), 1);

  const tampered = await requestApp('/api/integrations/meta/data-deletion', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ signed_request: `${signedRequest}tampered` }).toString()
  });
  assert.equal(tampered.statusCode, 400);
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
