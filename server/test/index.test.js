const { after, test } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');

const tempDir = path.join(os.tmpdir(), `social-insights-studio-test-${process.pid}`);
const allowedRedirect = 'https://script.google.com/macros/d/abc123/usercallback';
const allowedWebAppRedirect = 'https://script.google.com/macros/s/deployment-id/exec';

process.env.BASE_URL = 'http://localhost:3001';
process.env.NODE_ENV = 'test';
process.env.TIKTOK_CLIENT_KEY = 'test-client-key';
process.env.TIKTOK_CLIENT_SECRET = 'test-client-secret';
process.env.BACKEND_JWT_SECRET = 'a'.repeat(64);
process.env.ENCRYPTION_KEY = '0'.repeat(64);
process.env.LOOKER_CLIENT_ID = 'looker-studio-connector';
delete process.env.LOOKER_CLIENT_SECRET;
process.env.LOOKER_REDIRECT_URIS = `${allowedRedirect},${allowedWebAppRedirect}`;
process.env.PROVIDER_HTTP_TIMEOUT_MS = '50';
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
  setFetchImplementation,
  stopStores
} = require('../index');
const {
  buildAuthorizationUrl,
  categorizeProviderFailure,
  exchangeCode,
  fetchProfile,
  fetchVideosPage,
  missingScopes,
  refreshAccessToken,
  revokeAccess,
  setTikTokFetchImplementation
} = require('../integrations/tiktok');
const { closePool } = require('../database');
const { normalizeEmail, parseCookies, serializeCookie } = require('../platform/security');

after(async () => {
  setFetchImplementation(null);
  setTikTokFetchImplementation(null);
  stopStores();
  await closePool();
  await fs.rm(tempDir, { recursive: true, force: true });
});

let subjectCounter = 0;

function nextSubject() {
  subjectCounter += 1;
  return `subject-${subjectCounter}`;
}

function requestApp(pathname, options = {}) {
  const method = options.method || 'GET';
  const headers = options.headers || {};
  const body = options.body || null;

  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      const req = http.request(
        {
          hostname: '127.0.0.1',
          port,
          path: pathname,
          method,
          headers: body
            ? {
                'content-length': Buffer.byteLength(body),
                ...headers
              }
            : headers
        },
        res => {
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
        }
      );
      req.on('error', error => {
        server.close(() => reject(error));
      });
      if (body) {
        req.write(body);
      }
      req.end();
    });
  });
}

function formBody(values) {
  return new URLSearchParams(values).toString();
}

function jsonResponse(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body)
  };
}

function emptyResponse(status) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => ''
  };
}

function tiktokTokenBody(subject, overrides = {}) {
  return {
    data: {
      access_token: overrides.accessToken || `tt-access-${subject}`,
      refresh_token: overrides.refreshToken || `tt-refresh-${subject}`,
      open_id: subject,
      expires_in: overrides.expiresIn === undefined ? 3600 : overrides.expiresIn,
      refresh_expires_in: overrides.refreshExpiresIn === undefined ? 86400 : overrides.refreshExpiresIn,
      scope: overrides.scope || 'user.info.basic,user.info.profile,user.info.stats,video.list',
      token_type: 'Bearer'
    }
  };
}

function installDefaultTikTokMock(subject, overrides = {}) {
  const calls = [];
  setFetchImplementation(async (url, fetchOptions = {}) => {
    const call = {
      url: String(url),
      method: fetchOptions.method || 'GET',
      headers: fetchOptions.headers || {},
      body: String(fetchOptions.body || '')
    };
    calls.push(call);

    if (call.url === 'https://open.tiktokapis.com/v2/oauth/token/' && call.body.includes('grant_type=authorization_code')) {
      return jsonResponse(overrides.exchangeStatus || 200, overrides.exchangeBody || tiktokTokenBody(subject, overrides.token || {}));
    }

    if (call.url === 'https://open.tiktokapis.com/v2/oauth/token/' && call.body.includes('grant_type=refresh_token')) {
      return jsonResponse(200, tiktokTokenBody(subject, {
        accessToken: overrides.refreshedAccessToken || `tt-refreshed-${subject}`,
        refreshToken: overrides.refreshedRefreshToken || `tt-refreshed-refresh-${subject}`
      }));
    }

    if (call.url.startsWith('https://open.tiktokapis.com/v2/user/info/')) {
      return jsonResponse(200, {
        data: {
          user: {
            open_id: subject,
            username: 'creator',
            follower_count: 10
          }
        },
        error: { code: 'ok' }
      });
    }

    if (call.url.startsWith('https://open.tiktokapis.com/v2/video/list/')) {
      return jsonResponse(200, {
        data: {
          videos: [
            {
              id: 'video-1',
              view_count: 100,
              like_count: 12
            }
          ],
          has_more: false,
          cursor: 0
        },
        error: { code: 'ok' }
      });
    }

    if (call.url === 'https://open.tiktokapis.com/v2/oauth/revoke/') {
      return emptyResponse(overrides.revokeStatus || 200);
    }

    return jsonResponse(500, { error: 'unexpected_mock_call' });
  });
  return calls;
}

async function startLegacyOAuth(redirectUri = allowedRedirect) {
  const response = await requestApp(
    `/oauth/authorize?client_id=looker-studio-connector&response_type=code&redirect_uri=${encodeURIComponent(redirectUri)}&state=looker-state`
  );
  assert.equal(response.statusCode, 302);
  const location = new URL(response.headers.location);
  assert.equal(`${location.origin}${location.pathname}`, 'https://www.tiktok.com/v2/auth/authorize/');
  assert.equal(location.searchParams.get('client_key'), 'test-client-key');
  assert.equal(location.searchParams.get('response_type'), 'code');
  return location.searchParams.get('state');
}

async function finishProviderCallback(tiktokState, providerCode = 'provider-code') {
  const response = await requestApp(
    `/auth/tiktok/callback?code=${encodeURIComponent(providerCode)}&state=${encodeURIComponent(tiktokState)}`
  );
  assert.equal(response.statusCode, 302);
  const location = new URL(response.headers.location);
  assert.equal(`${location.origin}${location.pathname}`, allowedRedirect);
  assert.equal(location.searchParams.get('state'), 'looker-state');
  assert.ok(location.searchParams.get('code'));
  return location.searchParams.get('code');
}

async function redeemInternalCode(code, redirectUri = allowedRedirect, clientId = 'looker-studio-connector') {
  return requestApp('/oauth/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: formBody({
      grant_type: 'authorization_code',
      client_id: clientId,
      code,
      redirect_uri: redirectUri
    })
  });
}

async function completeOAuthFlow(subject = nextSubject(), overrides = {}) {
  const calls = installDefaultTikTokMock(subject, overrides);
  const tiktokState = await startLegacyOAuth();
  const internalCode = await finishProviderCallback(tiktokState);
  const tokenResponse = await redeemInternalCode(internalCode);
  assert.equal(tokenResponse.statusCode, 200);
  const tokens = tokenResponse.json();
  assert.ok(tokens.access_token);
  return { calls, internalCode, subject, tokens, tiktokState };
}

function hashStoreKey(namespace, state) {
  return crypto
    .createHash('sha256')
    .update(`${namespace}:${String(state)}`)
    .digest('hex');
}

async function writeExpiredOAuthState(state) {
  await fs.mkdir(tempDir, { recursive: true });
  const key = hashStoreKey('tiktok_oauth_state', state);
  await fs.writeFile(
    process.env.STATE_STORE_PATH,
    JSON.stringify({
      version: 1,
      states: {
        [key]: {
          namespace: 'tiktok_oauth_state',
          data: {
            flow: 'oauth',
            clientId: 'looker-studio-connector',
            redirectUri: allowedRedirect,
            lookerState: 'looker-state'
          },
          created_at: Date.now() - 10000,
          expires_at: Date.now() - 1000
        }
      }
    })
  );
}

test('isAllowedRedirect only allows exact configured Apps Script callback URLs', () => {
  assert.equal(isAllowedRedirect(allowedRedirect), true);
  assert.equal(isAllowedRedirect(allowedWebAppRedirect), true);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/other/usercallback'), false);
  assert.equal(isAllowedRedirect(`${allowedRedirect}?next=https://evil.test`), false);
  assert.equal(isAllowedRedirect(`${allowedRedirect}#fragment`), false);
  assert.equal(isAllowedRedirect('https://user:pass@script.google.com/macros/d/abc123/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com:443/macros/d/abc123/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com:444/macros/d/abc123/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/abc123/%75sercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/abc123%2Fusercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/abc123/../abc123/usercallback'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/abc123/usercallback%2Fextra'), false);
  assert.equal(isAllowedRedirect('https://script.google.com/macros/d/abc123/usercallback/extra'), false);
  assert.equal(isAllowedRedirect('https://script.google.com.evil.test/macros/d/abc123/usercallback'), false);
  assert.equal(isAllowedRedirect(allowedRedirect, 'unexpected-client'), false);
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
  assert.equal(parseTrustProxyValue('0'), false);
  assert.equal(parseTrustProxyValue('-1'), '-1');
  assert.equal(parseTrustProxyValue('unknown'), 'unknown');
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

test('security helpers normalize email and serialize cookies with expected protections', () => {
  assert.equal(normalizeEmail('  User@Example.COM  '), 'user@example.com');
  assert.deepEqual(parseCookies('sis_session=abc%20123; empty=; malformed; theme=light'), {
    sis_session: 'abc 123',
    empty: '',
    theme: 'light'
  });
  assert.equal(
    serializeCookie('sis_session', 'abc 123', {
      maxAge: 60,
      path: '/',
      httpOnly: true,
      secure: true,
      sameSite: 'Lax'
    }),
    'sis_session=abc%20123; Max-Age=60; Path=/; HttpOnly; Secure; SameSite=Lax'
  );
});

test('TikTok provider failures are categorized for retry and reconnect decisions', async () => {
  assert.deepEqual(categorizeProviderFailure(401, { error: { code: 'access_token_invalid' } }), {
    category: 'authentication',
    retryable: false,
    provider_code: 'access_token_invalid'
  });
  assert.deepEqual(categorizeProviderFailure(429, { error: { code: 'rate_limit_exceeded' } }), {
    category: 'rate_limit',
    retryable: true,
    provider_code: 'rate_limit_exceeded'
  });
  assert.deepEqual(categorizeProviderFailure(400, { error: { message: 'missing scope' } }), {
    category: 'scope',
    retryable: false,
    provider_code: 'missing scope'
  });
  assert.deepEqual(categorizeProviderFailure(400, { error: 'bad_request' }), {
    category: 'provider',
    retryable: false,
    provider_code: 'bad_request'
  });
  assert.deepEqual(categorizeProviderFailure(503, { code: 'provider_down' }), {
    category: 'provider',
    retryable: true,
    provider_code: 'provider_down'
  });
  assert.deepEqual(categorizeProviderFailure(200, {}), {
    category: 'malformed_response',
    retryable: false,
    provider_code: null
  });
  assert.deepEqual(missingScopes('user.info.basic video.list'), ['user.info.profile', 'user.info.stats']);

  const previousRedirectUri = process.env.TIKTOK_REDIRECT_URI;
  process.env.TIKTOK_REDIRECT_URI = 'https://local.example/tiktok/callback';
  assert.equal(
    new URL(buildAuthorizationUrl('state-value')).searchParams.get('redirect_uri'),
    'https://local.example/tiktok/callback'
  );
  if (previousRedirectUri === undefined) {
    delete process.env.TIKTOK_REDIRECT_URI;
  } else {
    process.env.TIKTOK_REDIRECT_URI = previousRedirectUri;
  }

  setTikTokFetchImplementation(async () => ({
    ok: true,
    status: 200,
    text: async () => 'not json'
  }));
  const malformedProfile = await fetchProfile('access-token');
  assert.equal(malformedProfile.ok, false);
  assert.equal(malformedProfile.error.category, 'malformed_response');
  assert.equal(malformedProfile.error.retryable, false);

  setTikTokFetchImplementation(async () => {
    const error = new Error('aborted');
    error.name = 'AbortError';
    throw error;
  });
  const timeoutExchange = await exchangeCode('provider-code');
  assert.equal(timeoutExchange.ok, false);
  assert.equal(timeoutExchange.error.category, 'timeout');
  assert.equal(timeoutExchange.error.retryable, true);

  setTikTokFetchImplementation(async () => {
    throw new Error('socket closed');
  });
  const networkRefresh = await refreshAccessToken('refresh-token');
  assert.equal(networkRefresh.ok, false);
  assert.equal(networkRefresh.error.category, 'network');
  assert.equal(networkRefresh.error.retryable, true);

  setTikTokFetchImplementation(async () => ({
    ok: true,
    status: 204,
    text: async () => ''
  }));
  const emptyProfile = await fetchProfile('access-token');
  assert.equal(emptyProfile.ok, true);
  assert.equal(emptyProfile.user, undefined);

  setTikTokFetchImplementation(async () => jsonResponse(200, {
    access_token: 'body-access',
    refresh_token: 'body-refresh',
    open_id: 'body-open-id',
    expires_in: 3600,
    refresh_expires_in: 86400
  }));
  const bodyToken = await exchangeCode('provider-code');
  assert.equal(bodyToken.ok, true);
  assert.equal(bodyToken.payload.access_token, 'body-access');

  setTikTokFetchImplementation(async () => jsonResponse(200, { data: { has_more: true, cursor: 42 } }));
  const page = await fetchVideosPage('access-token', 12);
  assert.equal(page.ok, true);
  assert.deepEqual(page.videos, []);
  assert.equal(page.cursor, 42);
  assert.equal(page.has_more, true);

  setTikTokFetchImplementation(async () => jsonResponse(200, { data: { videos: [{ id: 'video-1' }] } }));
  const defaultPage = await fetchVideosPage('access-token');
  assert.deepEqual(defaultPage.videos, [{ id: 'video-1' }]);
  assert.equal(defaultPage.cursor, 0);
  assert.equal(defaultPage.has_more, false);

  setTikTokFetchImplementation(async () => jsonResponse(403, { error: { code: 'permission_denied' } }));
  const revoke = await revokeAccess('access-token');
  assert.equal(revoke.attempted, true);
  assert.equal(revoke.success, false);
  assert.equal(revoke.error.category, 'authentication');
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

test('/health endpoints are non-sensitive', async () => {
  const live = await requestApp('/health/live');
  const ready = await requestApp('/health/ready');

  assert.equal(live.statusCode, 200);
  assert.deepEqual(live.json(), { status: 'live' });
  assert.equal(ready.statusCode, 200);
  assert.equal(ready.json().status, 'ready');
});

test('/oauth/authorize rejects unconfigured redirects and requires the expected client', async () => {
  assert.equal(
    (await requestApp(
      `/oauth/authorize?client_id=looker-studio-connector&response_type=code&redirect_uri=${encodeURIComponent('https://script.google.com/macros/d/other/usercallback')}&state=looker-state`
    )).statusCode,
    400
  );
  assert.equal(
    (await requestApp(
      `/oauth/authorize?client_id=evil-client&response_type=code&redirect_uri=${encodeURIComponent(allowedRedirect)}&state=looker-state`
    )).statusCode,
    400
  );
});

test('TikTok callback reports provider errors safely', async () => {
  const response = await requestApp('/auth/tiktok/callback?error=access_denied&error_description=%3Cscript%3Ebad%3C%2Fscript%3E');

  assert.equal(response.statusCode, 400);
  assert.match(response.body, /Authorization Error/);
  assert.match(response.body, /&lt;script&gt;bad&lt;\/script&gt;/);
});

test('TikTok callback rejects expired, mismatched, and replayed state', async () => {
  installDefaultTikTokMock(nextSubject());
  const expiredState = 'expired-state';
  await writeExpiredOAuthState(expiredState);

  assert.equal(
    (await requestApp(`/auth/tiktok/callback?code=provider-code&state=${expiredState}`)).statusCode,
    400
  );
  assert.equal(
    (await requestApp('/auth/tiktok/callback?code=provider-code&state=random-state')).statusCode,
    400
  );

  const tiktokState = await startLegacyOAuth();
  await finishProviderCallback(tiktokState);
  assert.equal(
    (await requestApp(`/auth/tiktok/callback?code=provider-code&state=${encodeURIComponent(tiktokState)}`)).statusCode,
    400
  );
});

test('legacy OAuth success issues a one-time client and redirect-bound backend code', async () => {
  const subject = nextSubject();
  installDefaultTikTokMock(subject);
  const tiktokState = await startLegacyOAuth();
  const internalCode = await finishProviderCallback(tiktokState);

  const wrongRedirect = await redeemInternalCode(internalCode, allowedWebAppRedirect);
  assert.equal(wrongRedirect.statusCode, 400);
  assert.equal(wrongRedirect.json().error, 'invalid_grant');

  const nextState = await startLegacyOAuth();
  const nextCode = await finishProviderCallback(nextState);
  const wrongClient = await redeemInternalCode(nextCode, allowedRedirect, 'wrong-client');
  assert.equal(wrongClient.statusCode, 400);
  assert.equal(wrongClient.json().error, 'invalid_client');

  const finalState = await startLegacyOAuth();
  const finalCode = await finishProviderCallback(finalState);
  const tokenResponse = await redeemInternalCode(finalCode);
  assert.equal(tokenResponse.statusCode, 200);
  assert.ok(tokenResponse.json().access_token);

  const replay = await redeemInternalCode(finalCode);
  assert.equal(replay.statusCode, 400);
  assert.equal(replay.json().error, 'invalid_grant');
});

test('/oauth/token requires client_id and does not accept public-client secrets', async () => {
  const noClient = await requestApp('/oauth/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: formBody({
      grant_type: 'authorization_code',
      code: 'anything',
      redirect_uri: allowedRedirect
    })
  });
  const unexpectedSecret = await requestApp('/oauth/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: formBody({
      grant_type: 'authorization_code',
      client_id: 'looker-studio-connector',
      client_secret: 'unused',
      code: 'anything',
      redirect_uri: allowedRedirect
    })
  });

  assert.equal(noClient.statusCode, 400);
  assert.equal(noClient.json().error, 'invalid_client');
  assert.equal(unexpectedSecret.statusCode, 400);
  assert.equal(unexpectedSecret.json().error, 'invalid_client');
});

test('authenticated TikTok user and video proxy routes return provider data', async () => {
  const { tokens } = await completeOAuthFlow();

  const user = await requestApp('/api/tiktok/user?fields=open_id,username,follower_count', {
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });
  const videos = await requestApp('/api/tiktok/videos?fields=id,view_count&max_count=5', {
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });

  assert.equal(user.statusCode, 200);
  assert.equal(user.json().data.user.username, 'creator');
  assert.equal(videos.statusCode, 200);
  assert.equal(videos.json().data.videos[0].id, 'video-1');
});

test('backend refreshes expired TikTok access tokens before proxying', async () => {
  const subject = nextSubject();
  const { tokens } = await completeOAuthFlow(subject, {
    token: {
      expiresIn: 1,
      accessToken: 'expired-access',
      refreshToken: 'refresh-me'
    },
    refreshedAccessToken: 'refreshed-access'
  });
  const calls = installDefaultTikTokMock(subject, { refreshedAccessToken: 'refreshed-access' });

  const user = await requestApp('/api/tiktok/user?fields=open_id,username', {
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });

  assert.equal(user.statusCode, 200);
  assert.ok(calls.some(call => call.body.includes('grant_type=refresh_token')));
  assert.ok(calls.some(call => call.url.startsWith('https://open.tiktokapis.com/v2/user/info/') && call.headers.Authorization === 'Bearer refreshed-access'));
});

test('provider timeout is returned as an explicit retryable error state', async () => {
  const { tokens } = await completeOAuthFlow();
  setFetchImplementation(async () => {
    const error = new Error('aborted');
    error.name = 'AbortError';
    throw error;
  });

  const response = await requestApp('/api/tiktok/user?fields=open_id', {
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });

  assert.equal(response.statusCode, 504);
  assert.deepEqual(response.json(), {
    error: 'tiktok_request_failed',
    category: 'timeout',
    retryable: true
  });
});

test('disconnect attempts TikTok revoke before deleting local credentials and is idempotent', async () => {
  const subject = nextSubject();
  const { calls, tokens } = await completeOAuthFlow(subject, {
    token: {
      accessToken: 'revoke-this-token'
    }
  });

  const first = await requestApp('/api/connector/revoke', {
    method: 'POST',
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });
  const second = await requestApp('/api/connector/revoke', {
    method: 'POST',
    headers: { authorization: `Bearer ${tokens.access_token}` }
  });

  assert.equal(first.statusCode, 200);
  assert.equal(first.json().revoked, true);
  assert.equal(first.json().provider_revoke.success, true);
  assert.ok(calls.some(call => (
    call.url === 'https://open.tiktokapis.com/v2/oauth/revoke/'
    && call.body.includes('token=revoke-this-token')
  )));
  assert.equal(second.statusCode, 200);
  assert.deepEqual(second.json(), {
    revoked: false,
    provider_revoke: {
      attempted: false,
      reason: 'credential_not_found'
    }
  });
});
