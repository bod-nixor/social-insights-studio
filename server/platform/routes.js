const express = require('express');
const {
  CSRF_COOKIE,
  OIDC_NONCE_COOKIE,
  OIDC_STATE_COOKIE,
  SESSION_COOKIE,
  SESSION_TTL_SECONDS,
  authenticate,
  createWorkspaceForUser,
  hashSecret,
  inviteMember,
  listWorkspaceMembers,
  listWorkspaces,
  removeMember,
  requestMagicLink,
  signOut,
  updateMemberRole,
  verifyGoogleOidc,
  verifyMagicLink
} = require('./services');
const {
  completeTikTokConnection,
  disconnectTikTok,
  startTikTokConnection
} = require('./connection-service');
const {
  getContent,
  getContentDetail,
  getDashboard,
  getSyncHistory
} = require('./dashboard-service');
const { createContentCsvExport } = require('./export-service');
const {
  getPublicProviderCatalog,
  listWorkspaceProviderCatalog
} = require('./provider-registry');
const { requestManualSync } = require('./sync-service');
const { hashSecret: hashValue, parseCookies, randomToken, serializeCookie } = require('./security');

function isSecureCookie(req) {
  return process.env.NODE_ENV === 'production' || req.secure || req.get('x-forwarded-proto') === 'https';
}

function setAuthCookies(req, res, sessionToken, csrfToken) {
  const secure = isSecureCookie(req);
  res.append('set-cookie', serializeCookie(SESSION_COOKIE, sessionToken, {
    httpOnly: true,
    secure,
    sameSite: 'Lax',
    path: '/',
    maxAge: SESSION_TTL_SECONDS
  }));
  res.append('set-cookie', serializeCookie(CSRF_COOKIE, csrfToken, {
    httpOnly: false,
    secure,
    sameSite: 'Lax',
    path: '/',
    maxAge: SESSION_TTL_SECONDS
  }));
}

function clearAuthCookies(req, res) {
  const secure = isSecureCookie(req);
  for (const name of [SESSION_COOKIE, CSRF_COOKIE]) {
    res.append('set-cookie', serializeCookie(name, '', {
      httpOnly: name === SESSION_COOKIE,
      secure,
      sameSite: 'Lax',
      path: '/',
      maxAge: 0
    }));
  }
}

function setOidcCookie(req, res, name, value) {
  res.append('set-cookie', serializeCookie(name, value, {
    httpOnly: true,
    secure: isSecureCookie(req),
    sameSite: 'Lax',
    path: '/',
    maxAge: 600
  }));
}

function clearOidcCookies(req, res) {
  for (const name of [OIDC_STATE_COOKIE, OIDC_NONCE_COOKIE]) {
    res.append('set-cookie', serializeCookie(name, '', {
      httpOnly: true,
      secure: isSecureCookie(req),
      sameSite: 'Lax',
      path: '/',
      maxAge: 0
    }));
  }
}

function sendError(res, error) {
  const status = error.status || 500;
  return res.status(status).json({
    error: error.code || (status === 500 ? 'server_error' : error.message)
  });
}

async function requireSession(req, res, next) {
  const cookies = parseCookies(req.get('cookie'));
  try {
    const session = await authenticate(cookies[SESSION_COOKIE]);
    if (!session) {
      return res.status(401).json({ error: 'authentication_required' });
    }
    req.session = session;
    req.csrfCookie = cookies[CSRF_COOKIE] || null;
    return next();
  } catch (error) {
    return sendError(res, error);
  }
}

function requireCsrf(req, res, next) {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) {
    return next();
  }
  const headerToken = req.get('x-csrf-token');
  if (!headerToken || !req.csrfCookie || headerToken !== req.csrfCookie) {
    return res.status(403).json({ error: 'csrf_required' });
  }
  if (hashValue(headerToken) !== req.session.csrfTokenHash) {
    return res.status(403).json({ error: 'csrf_invalid' });
  }
  return next();
}

function createPlatformRouter() {
  const router = express.Router();

  router.get('/providers/catalog', async (req, res) => {
    return res.json({ providers: getPublicProviderCatalog() });
  });

  router.post('/auth/magic-link/request', async (req, res) => {
    try {
      const ipHash = req.ip ? hashSecret(req.ip) : null;
      const result = await requestMagicLink({ email: req.body.email, ipHash });
      return res.json(result);
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/auth/magic-link/verify', async (req, res) => {
    try {
      const userAgentHash = req.get('user-agent') ? hashSecret(req.get('user-agent')) : null;
      const result = await verifyMagicLink(req.body.token, userAgentHash);
      setAuthCookies(req, res, result.sessionToken, result.csrfToken);
      return res.json({
        user: result.user,
        csrf_token: result.csrfToken
      });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/auth/google/state', async (req, res) => {
    if (!process.env.GOOGLE_OIDC_CLIENT_ID) {
      return res.status(503).json({ error: 'google_oidc_not_configured' });
    }
    const state = randomToken(32);
    const nonce = randomToken(32);
    setOidcCookie(req, res, OIDC_STATE_COOKIE, hashSecret(state));
    setOidcCookie(req, res, OIDC_NONCE_COOKIE, hashSecret(nonce));
    return res.json({
      client_id: process.env.GOOGLE_OIDC_CLIENT_ID,
      state,
      nonce
    });
  });

  router.post('/auth/google', async (req, res) => {
    if (!process.env.GOOGLE_OIDC_CLIENT_ID) {
      return res.status(503).json({ error: 'google_oidc_not_configured' });
    }
    if (!req.body.id_token || !req.body.state || !req.body.nonce) {
      return res.status(400).json({ error: 'invalid_oidc_request' });
    }
    const cookies = parseCookies(req.get('cookie'));
    if (
      cookies[OIDC_STATE_COOKIE] !== hashSecret(req.body.state) ||
      cookies[OIDC_NONCE_COOKIE] !== hashSecret(req.body.nonce)
    ) {
      return res.status(403).json({ error: 'oidc_state_invalid' });
    }
    try {
      const userAgentHash = req.get('user-agent') ? hashSecret(req.get('user-agent')) : null;
      const result = await verifyGoogleOidc({
        idToken: req.body.id_token,
        nonce: req.body.nonce,
        userAgentHash
      });
      setAuthCookies(req, res, result.sessionToken, result.csrfToken);
      clearOidcCookies(req, res);
      return res.json({
        user: result.user,
        csrf_token: result.csrfToken
      });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/session', requireSession, async (req, res) => {
    return res.json({
      user: req.session.user,
      csrf_token_available: Boolean(req.csrfCookie)
    });
  });

  router.post('/sign-out', requireSession, requireCsrf, async (req, res) => {
    try {
      await signOut(req.session.id);
      clearAuthCookies(req, res);
      return res.json({ signed_out: true });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces', requireSession, async (req, res) => {
    try {
      return res.json({ workspaces: await listWorkspaces(req.session.user.id) });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(201).json({
        workspace: await createWorkspaceForUser(req.session.user.id, req.body.name)
      });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/members', requireSession, async (req, res) => {
    try {
      return res.json(await listWorkspaceMembers(req.session.user.id, req.params.workspaceId));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/invitations', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(201).json(await inviteMember(
        req.session.user.id,
        req.params.workspaceId,
        req.body.email,
        req.body.role
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.patch('/workspaces/:workspaceId/members/:userId', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await updateMemberRole(
        req.session.user.id,
        req.params.workspaceId,
        req.params.userId,
        req.body.role
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/workspaces/:workspaceId/members/:userId', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await removeMember(
        req.session.user.id,
        req.params.workspaceId,
        req.params.userId
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/connections/tiktok/start', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await startTikTokConnection(
        req.session.user.id,
        req.params.workspaceId,
        req.body.return_path || '/'
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/workspaces/:workspaceId/connections/tiktok', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await disconnectTikTok(req.session.user.id, req.params.workspaceId));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getDashboard(req.session.user.id, req.params.workspaceId, req.query));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/provider-catalog', requireSession, async (req, res) => {
    try {
      return res.json({
        providers: await listWorkspaceProviderCatalog(req.session.user.id, req.params.workspaceId)
      });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/content', requireSession, async (req, res) => {
    try {
      return res.json(await getContent(req.session.user.id, req.params.workspaceId, req.query));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/content/:contentItemId', requireSession, async (req, res) => {
    try {
      return res.json(await getContentDetail(
        req.session.user.id,
        req.params.workspaceId,
        req.params.contentItemId
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/sync-runs', requireSession, async (req, res) => {
    try {
      return res.json(await getSyncHistory(req.session.user.id, req.params.workspaceId, req.query));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/sync-runs', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(202).json(await requestManualSync(req.session.user.id, req.params.workspaceId));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/exports/content.csv', requireSession, async (req, res) => {
    try {
      const result = await createContentCsvExport(req.session.user.id, req.params.workspaceId, req.query);
      res.setHeader('content-type', result.contentType);
      res.setHeader('content-disposition', `attachment; filename="${result.filename}"`);
      return res.status(200).send(result.body);
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/integrations/tiktok/callback', async (req, res) => {
    try {
      const result = await completeTikTokConnection({
        code: req.query.code,
        state: req.query.state
      });
      return res.status(200).send(`<!doctype html><title>TikTok connected</title><p>TikTok connected. Return to Social Insights Studio.</p><script>window.location.href=${JSON.stringify(result.return_path || '/')};</script>`);
    } catch (error) {
      return res.status(error.status || 500).send('<!doctype html><title>Connection failed</title><p>TikTok connection failed. Return to Social Insights Studio and try again.</p>');
    }
  });

  return router;
}

module.exports = {
  createPlatformRouter
};
