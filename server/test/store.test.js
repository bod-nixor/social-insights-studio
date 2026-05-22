const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { FileStateStore, FileTokenStore } = require('../store');

const tempDir = path.join(os.tmpdir(), `social-insights-store-test-${process.pid}`);
const encryptionKey = '1'.repeat(64);

after(async () => {
  await fs.rm(tempDir, { recursive: true, force: true });
});

test('FileTokenStore rejects incomplete token records', async () => {
  const store = new FileTokenStore({
    filePath: path.join(tempDir, 'tokens.json'),
    lockPath: path.join(tempDir, 'tokens.json.lock'),
    encryptionKey
  });

  await assert.rejects(
    () => store.saveConnectorToken('subject', { accessToken: 'access-token' }),
    /incomplete connector token/
  );
});

test('FileTokenStore round-trips encrypted connector tokens', async () => {
  const store = new FileTokenStore({
    filePath: path.join(tempDir, 'tokens.json'),
    lockPath: path.join(tempDir, 'tokens.json.lock'),
    encryptionKey
  });
  const expiresAt = Date.now() + 60 * 60 * 1000;
  const refreshExpiresAt = Date.now() + 24 * 60 * 60 * 1000;

  await store.saveConnectorToken('subject', {
    accessToken: 'access-token',
    refreshToken: 'refresh-token',
    expiresAt,
    refreshExpiresAt,
    scopes: 'user.info.basic',
    openId: 'subject'
  });

  const token = await store.getConnectorToken('subject');

  assert.equal(token.accessToken, 'access-token');
  assert.equal(token.refreshToken, 'refresh-token');
  assert.equal(token.openId, 'subject');
  assert.equal(token.scopes, 'user.info.basic');
});

test('FileStateStore saves and consumes state successfully', async () => {
  const store = new FileStateStore({
    filePath: path.join(tempDir, 'states-save-consume.json'),
    lockPath: path.join(tempDir, 'states-save-consume.json.lock'),
    namespace: 'oauth',
    cleanupIntervalMs: 0
  });

  await store.save('state-1', { flow: 'oauth', redirectUri: 'https://script.google.com/macros/d/id/usercallback' });
  const result = await store.consumeWithResult('state-1');

  assert.equal(result.status, 'consumed');
  assert.equal(result.entry.flow, 'oauth');
});

test('FileStateStore consumes state only once', async () => {
  const store = new FileStateStore({
    filePath: path.join(tempDir, 'states-once.json'),
    lockPath: path.join(tempDir, 'states-once.json.lock'),
    namespace: 'oauth',
    cleanupIntervalMs: 0
  });

  await store.save('state-1', { flow: 'oauth' });

  assert.equal((await store.consumeWithResult('state-1')).status, 'consumed');
  assert.equal((await store.consumeWithResult('state-1')).status, 'missing');
});

test('FileStateStore rejects expired state', async () => {
  const store = new FileStateStore({
    filePath: path.join(tempDir, 'states-expired.json'),
    lockPath: path.join(tempDir, 'states-expired.json.lock'),
    namespace: 'oauth',
    ttlMs: 5,
    cleanupIntervalMs: 0
  });

  await store.save('state-1', { flow: 'oauth' });
  await new Promise(resolve => setTimeout(resolve, 20));
  const result = await store.consumeWithResult('state-1');

  assert.equal(result.status, 'expired');
});

test('FileStateStore state survives a new store instance', async () => {
  const filePath = path.join(tempDir, 'states-persistent.json');
  const lockPath = path.join(tempDir, 'states-persistent.json.lock');
  const firstStore = new FileStateStore({ filePath, lockPath, namespace: 'oauth', cleanupIntervalMs: 0 });
  const secondStore = new FileStateStore({ filePath, lockPath, namespace: 'oauth', cleanupIntervalMs: 0 });

  await firstStore.save('state-1', { flow: 'oauth', lookerState: 'looker-state' });
  const result = await secondStore.consumeWithResult('state-1');

  assert.equal(result.status, 'consumed');
  assert.equal(result.entry.lookerState, 'looker-state');
});

test('FileStateStore consumes concurrent state races once', async () => {
  const store = new FileStateStore({
    filePath: path.join(tempDir, 'states-race.json'),
    lockPath: path.join(tempDir, 'states-race.json.lock'),
    namespace: 'oauth',
    cleanupIntervalMs: 0
  });

  await store.save('state-1', { flow: 'oauth' });
  const results = await Promise.all(
    Array.from({ length: 5 }, () => store.consumeWithResult('state-1'))
  );
  const consumed = results.filter(result => result.status === 'consumed');
  const missing = results.filter(result => result.status === 'missing');

  assert.equal(consumed.length, 1);
  assert.equal(missing.length, 4);
});
