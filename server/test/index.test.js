const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const os = require('node:os');
const path = require('node:path');

const tempDir = path.join(os.tmpdir(), `social-insights-studio-test-${process.pid}`);

process.env.BASE_URL = 'http://localhost:3001';
process.env.NODE_ENV = 'test';
process.env.TIKTOK_CLIENT_KEY = 'test-client-key';
process.env.TIKTOK_CLIENT_SECRET = 'test-client-secret';
process.env.BACKEND_JWT_SECRET = 'a'.repeat(64);
process.env.ENCRYPTION_KEY = '0'.repeat(64);
process.env.TOKEN_STORE_PATH = path.join(tempDir, 'tokens.json');
process.env.TOKEN_LOCK_PATH = path.join(tempDir, 'tokens.json.lock');

const {
  escapeHtml,
  isAllowedRedirect,
  parseFieldsParam,
  readJsonResponse,
  stopStores
} = require('../index');

after(() => {
  stopStores();
});

test('isAllowedRedirect allows Apps Script OAuth2 callback URLs', () => {
  assert.equal(
    isAllowedRedirect('https://script.google.com/macros/d/abc123_user-script-id/usercallback'),
    true
  );
});

test('isAllowedRedirect allows Apps Script web app deployment URLs', () => {
  assert.equal(
    isAllowedRedirect('https://script.google.com/macros/s/deployment-id/exec'),
    true
  );
});

test('isAllowedRedirect rejects lookalike or malformed redirect URLs', () => {
  assert.equal(isAllowedRedirect('http://script.google.com/macros/d/id/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com.evil.test/macros/d/id/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/id/usercallback/extra'), false);
});

test('escapeHtml escapes dynamic callback error content', () => {
  assert.equal(
    escapeHtml('<script>"x"&\'</script>'),
    '&lt;script&gt;&quot;x&quot;&amp;&#39;&lt;/script&gt;'
  );
});

test('parseFieldsParam accepts only allowlisted fields', () => {
  const allowedFields = ['id', 'create_time'];

  assert.deepEqual(parseFieldsParam('id, create_time', allowedFields), ['id', 'create_time']);
  assert.deepEqual(parseFieldsParam(undefined, allowedFields), allowedFields);
  assert.equal(parseFieldsParam('id,unknown', allowedFields), null);
  assert.equal(parseFieldsParam(',', allowedFields), null);
});

test('readJsonResponse parses JSON and flags malformed upstream responses', async () => {
  assert.deepEqual(
    await readJsonResponse({ text: async () => '{"ok":true}' }),
    { ok: true, data: { ok: true } }
  );
  assert.deepEqual(
    await readJsonResponse({ text: async () => 'not json' }),
    { ok: false, data: { error: 'invalid_json_response' } }
  );
});
