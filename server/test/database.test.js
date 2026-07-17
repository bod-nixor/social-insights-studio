const { after, before, test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
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
const { compareMetric, engagementRate } = require('../platform/analytics');
const { hasCapability, canAssignRole } = require('../platform/rbac');
const { runDueSyncs } = require('../platform/sync-service');
const { safeCsvCell } = require('../platform/export-service');
const { decryptSecret, encryptSecret } = require('../platform/secret-envelope');
const { assertNotProductionCommand } = require('../scripts/database-env');

let db;

before(async () => {
  db = await mariadb.createConnection(process.env.DATABASE_TEST_URL);
});

after(async () => {
  setGoogleOidcFetchImplementation(null);
  setTikTokFetchImplementation(null);
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

test('migrations are applied to the real MariaDB test database', async () => {
  const rows = await db.query('SELECT version FROM schema_migrations ORDER BY version');
  assert.deepEqual(rows.map(row => row.version), ['001_phase1_foundation', '002_session_csrf']);

  const tableRows = await db.query(
    `SELECT TABLE_NAME AS table_name FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('users', 'workspaces', 'oauth_credentials', 'profile_snapshots')`
  );
  assert.equal(tableRows.length, 4);
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
  const owner = await signIn('owner@example.com');

  const noCsrf = await requestApp('/api/workspaces', {
    method: 'POST',
    headers: { cookie: cookieHeader(owner.cookies) },
    body: { name: 'Owner Workspace' }
  });
  assert.equal(noCsrf.statusCode, 403);
  assert.equal(noCsrf.json().error, 'csrf_required');

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
});

test('Google OIDC route fails closed when credentials are unavailable', async () => {
  const response = await requestApp('/api/auth/google', {
    method: 'POST',
    body: { id_token: 'fake', state: 'state', nonce: 'nonce' }
  });
  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error, 'google_oidc_not_configured');
});

test('production-only safety guards refuse development paths', async () => {
  const previousNodeEnv = process.env.NODE_ENV;
  const previousMailAdapter = process.env.MAIL_ADAPTER;
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
  } finally {
    process.env.NODE_ENV = previousNodeEnv;
    if (previousMailAdapter === undefined) {
      delete process.env.MAIL_ADAPTER;
    } else {
      process.env.MAIL_ADAPTER = previousMailAdapter;
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
  } finally {
    delete process.env.GOOGLE_OIDC_CLIENT_ID;
    setGoogleOidcFetchImplementation(null);
  }
});

test('TikTok connection lifecycle is workspace-bound, encrypted, replay-safe, and auditable', async () => {
  await clearDatabase();
  const owner = await signIn('owner-tiktok@example.com');
  const viewer = await signIn('viewer-tiktok@example.com');
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

  const viewerStart = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(viewer.cookies),
      'x-csrf-token': viewer.csrf
    },
    body: { return_path: '/app' }
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
    body: { return_path: '/app?tab=connections' }
  });
  assert.equal(start.statusCode, 200);
  const authorizationUrl = new URL(start.json().authorization_url);
  const state = authorizationUrl.searchParams.get('state');
  assert.ok(state);

  const transactionRows = await db.query('SELECT state_hash, return_path FROM oauth_transactions WHERE workspace_id = ?', [workspace.id]);
  assert.equal(transactionRows.length, 1);
  assert.notEqual(transactionRows[0].state_hash, state);
  assert.equal(transactionRows[0].return_path, '/app?tab=connections');

  const callback = await requestApp(`/api/integrations/tiktok/callback?code=valid-code&state=${encodeURIComponent(state)}`);
  assert.equal(callback.statusCode, 200);

  const replay = await requestApp(`/api/integrations/tiktok/callback?code=valid-code&state=${encodeURIComponent(state)}`);
  assert.equal(replay.statusCode, 400);

  const credentialRows = await db.query(
    `SELECT oc.access_token_ciphertext, oc.revoked_at, ds.status
     FROM oauth_credentials oc
     JOIN data_sources ds ON ds.id = oc.data_source_id
     WHERE ds.workspace_id = ?`,
    [workspace.id]
  );
  assert.equal(credentialRows.length, 1);
  assert.notEqual(credentialRows[0].access_token_ciphertext, 'access-token-one');
  assert.equal(credentialRows[0].status, 'active');
  assert.equal(credentialRows[0].revoked_at, null);

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

  const missingStart = await requestApp(`/api/workspaces/${workspace.id}/connections/tiktok/start`, {
    method: 'POST',
    headers: {
      cookie: cookieHeader(owner.cookies),
      'x-csrf-token': owner.csrf
    },
    body: { return_path: '/app' }
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
    body: { return_path: '/app' }
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

  const contentDetail = await requestApp(`/api/workspaces/${workspace.id}/content/${contentBody.rows[0].id}`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(contentDetail.statusCode, 200);
  assert.equal(contentDetail.json().history[0].engagement_rate, 13);

  const missingContent = await requestApp(`/api/workspaces/${workspace.id}/content/00000000-0000-4000-8000-000000000999`, {
    headers: { cookie: cookieHeader(owner.cookies) }
  });
  assert.equal(missingContent.statusCode, 404);
  assert.equal(missingContent.json().error, 'content_not_found');

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
});
