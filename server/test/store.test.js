const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { FileTokenStore } = require('../store');

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
