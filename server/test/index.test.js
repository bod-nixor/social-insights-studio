const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const http = require('node:http');
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
process.env.STATE_STORE_PATH = path.join(tempDir, 'oauth-state.json');
process.env.STATE_LOCK_PATH = path.join(tempDir, 'oauth-state.json.lock');

const {
  app,
  escapeHtml,
  getTrustProxySetting,
  isAllowedRedirect,
  parseTrustProxyValue,
  parseFieldsParam,
  readJsonResponse,
  stopStores
} = require('../index');

after(async () => {
  stopStores();
  await fs.rm(tempDir, { recursive: true, force: true });
});

function requestApp(pathname) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      const req = http.get({ hostname: '127.0.0.1', port, path: pathname }, res => {
        res.resume();
        res.on('end', () => {
          server.close(() => resolve({ statusCode: res.statusCode, headers: res.headers }));
        });
      });
      req.on('error', error => {
        server.close(() => reject(error));
      });
    });
  });
}

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

test('parseTrustProxyValue uses safe proxy hop counts', () => {
  assert.equal(parseTrustProxyValue('1'), 1);
  assert.equal(parseTrustProxyValue('true'), 1);
  assert.equal(parseTrustProxyValue('false'), false);
  assert.equal(parseTrustProxyValue('loopback'), 'loopback');
});

test('getTrustProxySetting defaults to one hop in Passenger', () => {
  const originalPassengerEnv = process.env.PASSENGER_APP_ENV;
  const originalTrustProxy = process.env.TRUST_PROXY;
  try {
    delete process.env.TRUST_PROXY;
    process.env.PASSENGER_APP_ENV = 'production';

    assert.equal(getTrustProxySetting(), 1);
  } finally {
    if (originalPassengerEnv === undefined) {
      delete process.env.PASSENGER_APP_ENV;
    } else {
      process.env.PASSENGER_APP_ENV = originalPassengerEnv;
    }
    if (originalTrustProxy === undefined) {
      delete process.env.TRUST_PROXY;
    } else {
      process.env.TRUST_PROXY = originalTrustProxy;
    }
  }
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

test('/oauth/authorize stores state and redirects to TikTok', async () => {
  const response = await requestApp(
    '/oauth/authorize?client_id=looker-studio-connector&response_type=code&redirect_uri=https%3A%2F%2Fscript.google.com%2Fmacros%2Fd%2Fabc123%2Fusercallback&state=test-state'
  );
  const location = new URL(response.headers.location);

  assert.equal(response.statusCode, 302);
  assert.equal(`${location.origin}${location.pathname}`, 'https://www.tiktok.com/v2/auth/authorize/');
  assert.equal(location.searchParams.get('client_key'), 'test-client-key');
  assert.equal(location.searchParams.get('response_type'), 'code');
  assert.ok(location.searchParams.get('state'));
});
