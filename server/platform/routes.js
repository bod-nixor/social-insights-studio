const express = require('express');
const {
  CSRF_COOKIE,
  OIDC_NONCE_COOKIE,
  OIDC_STATE_COOKIE,
  SESSION_COOKIE,
  SESSION_TTL_SECONDS,
  acceptWorkspaceInvitation,
  authenticate,
  createWorkspaceForUser,
  describeUserAgent,
  getAccount,
  hashSecret,
  inviteMember,
  listWorkspaceMembers,
  listWorkspaces,
  removeMember,
  requestAccountDeletion,
  requestMagicLink,
  requestWorkspaceDeletion,
  resendInvitation,
  revokeAccountSession,
  revokeAllAccountSessions,
  revokeInvitation,
  revokeOtherAccountSessions,
  signOut,
  updateAccountProfile,
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
  completeYouTubeConnection,
  disconnectYouTube,
  selectYouTubeResource,
  startYouTubeConnection
} = require('./youtube-connection-service');
const { getYouTubeDashboard } = require('./youtube-dashboard-service');
const {
  completeMetaConnection,
  disconnectMeta,
  getMetaDeletionStatus,
  processMetaSignedCallback,
  selectMetaResource,
  startMetaConnection
} = require('./meta-connection-service');
const { getMetaDashboard } = require('./meta-dashboard-service');
const {
  completeGoogleAnalyticsConnection,
  disconnectGoogleAnalytics,
  selectGoogleAnalyticsResource,
  startGoogleAnalyticsConnection
} = require('./google-analytics-connection-service');
const { getGoogleAnalyticsDashboard } = require('./google-analytics-dashboard-service');
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

function youtubeCallbackOutcome(error) {
  const code = error && (error.code || error.message);
  if (code === 'youtube_authorization_denied') return 'denied';
  if (code === 'youtube_required_scopes_missing') return 'missing_scopes';
  if (code === 'youtube_oauth_redirect_mismatch' || code === 'youtube_not_configured') {
    return 'configuration_error';
  }
  if (
    code === 'youtube_authorization_failed' ||
    code === 'youtube_token_exchange_failed' ||
    code === 'youtube_channel_discovery_failed' ||
    code === 'youtube_channels_response_malformed'
  ) {
    return 'provider_error';
  }
  return 'failed';
}

function metaCallbackOutcome(provider, error) {
  const code = error && (error.code || error.message);
  if (code === `${provider}_authorization_denied`) return 'denied';
  if (code === `${provider}_provider_subject_mismatch`) return 'account_mismatch';
  if (code === `${provider}_required_scopes_missing`) return 'missing_scopes';
  if (
    code === `${provider}_oauth_redirect_mismatch` ||
    code === `${provider}_oauth_config_mismatch` ||
    code === `${provider}_not_configured`
  ) {
    return 'configuration_error';
  }
  if (
    code === `${provider}_authorization_failed` ||
    code === `${provider}_token_exchange_failed` ||
    code === `${provider}_long_lived_token_failed` ||
    code === `${provider}_resource_discovery_failed`
  ) return 'provider_error';
  return 'failed';
}

function googleAnalyticsCallbackOutcome(error) {
  const code = error && (error.code || error.message);
  if (code === 'ga4_authorization_denied') return 'denied';
  if (code === 'ga4_required_scopes_missing') return 'missing_scopes';
  if (code === 'ga4_oauth_redirect_mismatch' || code === 'ga4_not_configured') {
    return 'configuration_error';
  }
  if (
    code === 'ga4_authorization_failed' ||
    code === 'ga4_token_exchange_failed' ||
    code === 'ga4_property_discovery_failed' ||
    code === 'ga4_property_discovery_incomplete'
  ) return 'provider_error';
  return 'failed';
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
      const userAgent = req.get('user-agent') || '';
      const userAgentHash = userAgent ? hashSecret(userAgent) : null;
      const result = await verifyMagicLink(req.body.token, userAgentHash, describeUserAgent(userAgent));
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
      const userAgent = req.get('user-agent') || '';
      const userAgentHash = userAgent ? hashSecret(userAgent) : null;
      const result = await verifyGoogleOidc({
        idToken: req.body.id_token,
        nonce: req.body.nonce,
        userAgentHash,
        deviceLabel: describeUserAgent(userAgent)
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

  router.get('/account', requireSession, async (req, res) => {
    try {
      return res.json(await getAccount(req.session.user.id, req.session.id));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.patch('/account/profile', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await updateAccountProfile(req.session.user.id, req.body && req.body.display_name));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/account/sessions/:sessionId', requireSession, requireCsrf, async (req, res) => {
    try {
      const result = await revokeAccountSession(req.session.user.id, req.params.sessionId);
      if (req.params.sessionId === req.session.id) clearAuthCookies(req, res);
      return res.json({ ...result, signed_out: req.params.sessionId === req.session.id });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/account/sessions/revoke-others', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await revokeOtherAccountSessions(req.session.user.id, req.session.id));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/account/sessions/revoke-all', requireSession, requireCsrf, async (req, res) => {
    try {
      const result = await revokeAllAccountSessions(req.session.user.id);
      clearAuthCookies(req, res);
      return res.json({ ...result, signed_out: true });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/account/deletion-requests', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(202).json(await requestAccountDeletion(
        req.session.user.id,
        req.body && req.body.confirmation
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/invitations/accept', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await acceptWorkspaceInvitation(req.session.user.id, req.body && req.body.token));
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

  router.post('/workspaces/:workspaceId/invitations/:invitationId/resend', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await resendInvitation(
        req.session.user.id,
        req.params.workspaceId,
        req.params.invitationId
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/workspaces/:workspaceId/invitations/:invitationId', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await revokeInvitation(
        req.session.user.id,
        req.params.workspaceId,
        req.params.invitationId
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/deletion-requests', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(202).json(await requestWorkspaceDeletion(
        req.session.user.id,
        req.params.workspaceId,
        req.body && req.body.confirmation
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

  router.post('/workspaces/:workspaceId/connections/youtube/start', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await startYouTubeConnection({
        userId: req.session.user.id,
        sessionId: req.session.id,
        workspaceId: req.params.workspaceId,
        returnPath: (req.body && req.body.return_path) || '/',
        targetConnectionId: (req.body && req.body.connection_id) || null
      }));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/connections/youtube/select', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(201).json(await selectYouTubeResource(
        req.session.user.id,
        req.params.workspaceId,
        req.body && req.body.resource_id
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/workspaces/:workspaceId/connections/youtube', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await disconnectYouTube(
        req.session.user.id,
        req.params.workspaceId,
        req.body && req.body.connection_id ? req.body.connection_id : null
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/connections/google-analytics/start', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await startGoogleAnalyticsConnection({
        userId: req.session.user.id,
        sessionId: req.session.id,
        workspaceId: req.params.workspaceId,
        returnPath: (req.body && req.body.return_path) || '/',
        targetConnectionId: (req.body && req.body.connection_id) || null
      }));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/workspaces/:workspaceId/connections/google-analytics/select', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(201).json(await selectGoogleAnalyticsResource(
        req.session.user.id,
        req.params.workspaceId,
        req.body && req.body.resource_id
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.delete('/workspaces/:workspaceId/connections/google-analytics', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.json(await disconnectGoogleAnalytics(
        req.session.user.id,
        req.params.workspaceId,
        req.body && req.body.connection_id ? req.body.connection_id : null
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  for (const route of [
    { path: 'facebook', provider: 'facebook_pages' },
    { path: 'instagram', provider: 'instagram' }
  ]) {
    router.post(`/workspaces/:workspaceId/connections/${route.path}/start`, requireSession, requireCsrf, async (req, res) => {
      try {
        return res.json(await startMetaConnection({
          provider: route.provider,
          userId: req.session.user.id,
          sessionId: req.session.id,
          workspaceId: req.params.workspaceId,
          returnPath: (req.body && req.body.return_path) || '/',
          targetConnectionId: (req.body && req.body.connection_id) || null
        }));
      } catch (error) {
        return sendError(res, error);
      }
    });

    router.post(`/workspaces/:workspaceId/connections/${route.path}/select`, requireSession, requireCsrf, async (req, res) => {
      try {
        return res.status(201).json(await selectMetaResource(
          req.session.user.id,
          req.params.workspaceId,
          route.provider,
          req.body && req.body.resource_id
        ));
      } catch (error) {
        return sendError(res, error);
      }
    });

    router.delete(`/workspaces/:workspaceId/connections/${route.path}`, requireSession, requireCsrf, async (req, res) => {
      try {
        return res.json(await disconnectMeta(
          req.session.user.id,
          req.params.workspaceId,
          route.provider,
          req.body && req.body.connection_id ? req.body.connection_id : null
        ));
      } catch (error) {
        return sendError(res, error);
      }
    });
  }

  router.get('/workspaces/:workspaceId/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getDashboard(req.session.user.id, req.params.workspaceId, req.query));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/providers/youtube/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getYouTubeDashboard(req.session.user.id, req.params.workspaceId, req.query));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/providers/facebook_pages/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getMetaDashboard(
        req.session.user.id,
        req.params.workspaceId,
        'facebook_pages',
        req.query
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/providers/instagram/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getMetaDashboard(
        req.session.user.id,
        req.params.workspaceId,
        'instagram',
        req.query
      ));
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/workspaces/:workspaceId/providers/google_analytics_4/dashboard', requireSession, async (req, res) => {
    try {
      return res.json(await getGoogleAnalyticsDashboard(
        req.session.user.id,
        req.params.workspaceId,
        req.query
      ));
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

  router.post('/workspaces/:workspaceId/providers/youtube/sync-runs', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(202).json(await requestManualSync(req.session.user.id, req.params.workspaceId, {
        provider: 'youtube',
        connectionId: (req.body && req.body.connection_id) || null
      }));
    } catch (error) {
      return sendError(res, error);
    }
  });

  for (const provider of ['facebook_pages', 'instagram']) {
    router.post(`/workspaces/:workspaceId/providers/${provider}/sync-runs`, requireSession, requireCsrf, async (req, res) => {
      try {
        return res.status(202).json(await requestManualSync(req.session.user.id, req.params.workspaceId, {
          provider,
          connectionId: (req.body && req.body.connection_id) || null
        }));
      } catch (error) {
        return sendError(res, error);
      }
    });
  }

  router.post('/workspaces/:workspaceId/providers/google_analytics_4/sync-runs', requireSession, requireCsrf, async (req, res) => {
    try {
      return res.status(202).json(await requestManualSync(req.session.user.id, req.params.workspaceId, {
        provider: 'google_analytics_4',
        connectionId: (req.body && req.body.connection_id) || null
      }));
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

  router.get('/integrations/youtube/callback', async (req, res) => {
    try {
      const cookies = parseCookies(req.get('cookie'));
      const session = await authenticate(cookies[SESSION_COOKIE]);
      if (!session) return res.redirect(303, '/?view=connections&youtube=failed');
      const result = await completeYouTubeConnection({
        code: req.query.code,
        state: req.query.state,
        providerError: req.query.error,
        sessionId: session.id,
        userId: session.user.id
      });
      const destination = new URL(result.return_path || '/', 'https://social-insights.local');
      destination.searchParams.set('youtube', result.outcome);
      return res.redirect(303, `${destination.pathname}${destination.search}${destination.hash}`);
    } catch (error) {
      return res.redirect(303, `/?view=connections&youtube=${youtubeCallbackOutcome(error)}`);
    }
  });

  router.get('/integrations/google-analytics/callback', async (req, res) => {
    try {
      const cookies = parseCookies(req.get('cookie'));
      const session = await authenticate(cookies[SESSION_COOKIE]);
      if (!session) return res.redirect(303, '/?view=connections&analytics=failed');
      const result = await completeGoogleAnalyticsConnection({
        code: req.query.code,
        state: req.query.state,
        providerError: req.query.error,
        sessionId: session.id,
        userId: session.user.id
      });
      const destination = new URL(result.return_path || '/', 'https://social-insights.local');
      destination.searchParams.set('analytics', result.outcome);
      return res.redirect(303, `${destination.pathname}${destination.search}${destination.hash}`);
    } catch (error) {
      return res.redirect(303, `/?view=connections&analytics=${googleAnalyticsCallbackOutcome(error)}`);
    }
  });

  for (const callback of [
    { path: 'facebook', provider: 'facebook_pages', queryKey: 'facebook' },
    { path: 'instagram', provider: 'instagram', queryKey: 'instagram' }
  ]) {
    router.get(`/integrations/${callback.path}/callback`, async (req, res) => {
      try {
        const cookies = parseCookies(req.get('cookie'));
        const session = await authenticate(cookies[SESSION_COOKIE]);
        if (!session) return res.redirect(303, `/?view=connections&${callback.queryKey}=failed`);
        const result = await completeMetaConnection({
          provider: callback.provider,
          code: req.query.code,
          state: req.query.state,
          providerError: req.query.error,
          sessionId: session.id,
          userId: session.user.id
        });
        const destination = new URL(result.return_path || '/', 'https://social-insights.local');
        destination.searchParams.set(callback.queryKey, result.outcome);
        return res.redirect(303, `${destination.pathname}${destination.search}${destination.hash}`);
      } catch (error) {
        return res.redirect(
          303,
          `/?view=connections&${callback.queryKey}=${metaCallbackOutcome(callback.provider, error)}`
        );
      }
    });
  }

  router.post('/integrations/meta/data-deletion', async (req, res) => {
    try {
      const result = await processMetaSignedCallback('data_deletion', req.body && req.body.signed_request);
      const base = String(process.env.BASE_URL || '').replace(/\/+$/, '');
      return res.json({
        url: `${base}/api/integrations/meta/deletion-status/${encodeURIComponent(result.confirmation_code)}`,
        confirmation_code: result.confirmation_code
      });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.post('/integrations/meta/deauthorize', async (req, res) => {
    try {
      const result = await processMetaSignedCallback('deauthorization', req.body && req.body.signed_request);
      return res.json({ success: result.status === 'completed' });
    } catch (error) {
      return sendError(res, error);
    }
  });

  router.get('/integrations/meta/deletion-status/:confirmationCode', async (req, res) => {
    try {
      return res.json(await getMetaDeletionStatus(req.params.confirmationCode));
    } catch (error) {
      return sendError(res, error);
    }
  });

  return router;
}

module.exports = {
  createPlatformRouter
};
