const { getConnection } = require('../database');
const repositories = require('./repositories');
const { verifyGoogleIdToken } = require('./google-oidc');
const { sendInvitationEmail, sendMagicLinkEmail, validateMailConfiguration } = require('./mail');
const { assertCapability, canAssignRole } = require('./rbac');
const {
  createId,
  hashSecret,
  normalizeEmail,
  randomToken
} = require('./security');

const SESSION_COOKIE = 'sis_session';
const CSRF_COOKIE = 'sis_csrf';
const OIDC_STATE_COOKIE = 'sis_oidc_state';
const OIDC_NONCE_COOKIE = 'sis_oidc_nonce';
const SESSION_TTL_SECONDS = Number(process.env.SESSION_TTL_SECONDS || 60 * 60 * 24 * 30);
const SESSION_IDLE_SECONDS = Number(process.env.SESSION_IDLE_SECONDS || 60 * 60 * 24 * 7);
const MAGIC_LINK_TTL_SECONDS = 15 * 60;
const MAGIC_LINK_WINDOW_SECONDS = 15 * 60;
const MAGIC_LINK_MAX_PER_WINDOW = 5;
const INVITATION_TTL_SECONDS = 60 * 60 * 24 * 7;
const INVITATION_RESEND_COOLDOWN_SECONDS = 60;
const INVITATION_MAX_SENDS = 5;

function exposeDevelopmentAuthToken() {
  return process.env.NODE_ENV !== 'production' && process.env.AUTH_DEV_MAGIC_LINKS === 'true';
}

function createHttpError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  return error;
}

function describeUserAgent(value) {
  const userAgent = String(value || '');
  let browser = 'Browser';
  let platform = 'unknown device';

  if (/Edg\//i.test(userAgent)) browser = 'Microsoft Edge';
  else if (/OPR\//i.test(userAgent)) browser = 'Opera';
  else if (/Firefox\//i.test(userAgent)) browser = 'Firefox';
  else if (/Chrome\//i.test(userAgent)) browser = 'Chrome';
  else if (/Safari\//i.test(userAgent) && /Version\//i.test(userAgent)) browser = 'Safari';

  if (/iPad/i.test(userAgent)) platform = 'iPad';
  else if (/iPhone/i.test(userAgent)) platform = 'iPhone';
  else if (/Android/i.test(userAgent)) platform = 'Android';
  else if (/Windows/i.test(userAgent)) platform = 'Windows';
  else if (/Macintosh|Mac OS X/i.test(userAgent)) platform = 'macOS';
  else if (/Linux/i.test(userAgent)) platform = 'Linux';

  return `${browser} on ${platform}`.slice(0, 160);
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || `workspace-${Date.now()}`;
}

async function withConnection(fn) {
  const connection = await getConnection();
  if (!connection) {
    const error = new Error('database_not_configured');
    error.status = 503;
    error.code = 'database_not_configured';
    throw error;
  }
  try {
    return await fn(connection);
  } finally {
    await connection.release();
  }
}

async function requestMagicLink({ email, ipHash }) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || !normalizedEmail.includes('@')) {
    const error = new Error('invalid_email');
    error.status = 400;
    error.code = 'invalid_email';
    throw error;
  }
  validateMailConfiguration();
  const token = randomToken(32);
  const devMode = process.env.NODE_ENV !== 'production' && process.env.AUTH_DEV_MAGIC_LINKS === 'true';

  return withConnection(async connection => {
    const recentCount = await repositories.countRecentMagicLinks(connection, normalizedEmail, MAGIC_LINK_WINDOW_SECONDS);
    if (recentCount >= MAGIC_LINK_MAX_PER_WINDOW) {
      const error = new Error('rate_limited');
      error.status = 429;
      error.code = 'rate_limited';
      throw error;
    }

    await repositories.saveMagicLinkToken(connection, {
      id: createId(),
      email: normalizedEmail,
      tokenHash: hashSecret(token),
      requestedIpHash: ipHash,
      ttlSeconds: MAGIC_LINK_TTL_SECONDS
    });
  }).then(async () => {
    await sendMagicLinkEmail({ email: normalizedEmail, token });

    return {
      sent: true,
      dev_token: devMode ? token : undefined
    };
  });
}

async function verifyMagicLink(token, userAgentHash, deviceLabel = null) {
  if (!token) {
    const error = new Error('invalid_token');
    error.status = 400;
    error.code = 'invalid_token';
    throw error;
  }

  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const magicLink = await repositories.consumeMagicLinkToken(connection, hashSecret(token));
      if (!magicLink) {
        const error = new Error('invalid_or_expired_token');
        error.status = 400;
        error.code = 'invalid_or_expired_token';
        throw error;
      }
      const user = await repositories.findOrCreateUserByEmail(connection, magicLink.email);
      const sessionToken = randomToken(32);
      const csrfToken = randomToken(32);
      await repositories.createSession(connection, {
        userId: user.id,
        tokenHash: hashSecret(sessionToken),
        csrfTokenHash: hashSecret(csrfToken),
        absoluteTtlSeconds: SESSION_TTL_SECONDS,
        idleTtlSeconds: SESSION_IDLE_SECONDS,
        userAgentHash,
        deviceLabel
      });
      await connection.commit();
      return { user, sessionToken, csrfToken };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function verifyGoogleOidc({ idToken, nonce, userAgentHash, deviceLabel = null }) {
  const profile = await verifyGoogleIdToken(idToken, {
    audience: process.env.GOOGLE_OIDC_CLIENT_ID,
    nonce
  });
  const normalizedEmail = normalizeEmail(profile.email);

  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const identityRows = await connection.query(
        `SELECT u.*
         FROM user_identities i
         JOIN users u ON u.id = i.user_id
         WHERE i.provider = 'google' AND i.provider_subject = ?
           AND u.deleted_at IS NULL AND u.status = 'active'
         LIMIT 1`,
        [profile.subject]
      );
      let user = identityRows[0] || null;
      if (!user) {
        user = await repositories.findOrCreateUserByEmail(connection, normalizedEmail);
        await connection.query(
          `INSERT INTO user_identities (id, user_id, provider, provider_subject, email)
           VALUES (?, ?, 'google', ?, ?)
           ON DUPLICATE KEY UPDATE email = VALUES(email)`,
          [createId(), user.id, profile.subject, normalizedEmail]
        );
      }
      await connection.query(
        `UPDATE users SET display_name = COALESCE(display_name, ?), last_login_at = UTC_TIMESTAMP(3)
         WHERE id = ?`,
        [profile.displayName, user.id]
      );
      const sessionToken = randomToken(32);
      const csrfToken = randomToken(32);
      await repositories.createSession(connection, {
        userId: user.id,
        tokenHash: hashSecret(sessionToken),
        csrfTokenHash: hashSecret(csrfToken),
        absoluteTtlSeconds: SESSION_TTL_SECONDS,
        idleTtlSeconds: SESSION_IDLE_SECONDS,
        userAgentHash,
        deviceLabel
      });
      await connection.commit();
      return {
        user: {
          id: user.id,
          email: normalizedEmail,
          display_name: user.display_name || profile.displayName
        },
        sessionToken,
        csrfToken
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function authenticate(sessionToken) {
  if (!sessionToken) return null;
  return withConnection(async connection => {
    const session = await repositories.findSessionByTokenHash(connection, hashSecret(sessionToken));
    if (!session) return null;
    await repositories.touchSession(connection, session.id, SESSION_IDLE_SECONDS);
    return {
      id: session.id,
      user: {
        id: session.user_id,
        email: session.email,
        display_name: session.display_name
      },
      csrfTokenHash: session.csrf_token_hash
    };
  });
}

async function signOut(sessionId) {
  return withConnection(connection => repositories.revokeSession(connection, sessionId));
}

async function getAccount(userId, currentSessionId) {
  return withConnection(async connection => {
    const [profile, identities, sessions, deletionRequests] = await Promise.all([
      repositories.getAccountProfile(connection, userId),
      repositories.listUserIdentities(connection, userId),
      repositories.listActiveSessions(connection, userId),
      repositories.listDeletionRequests(connection, userId)
    ]);
    if (!profile) throw createHttpError(404, 'account_not_found');
    return {
      profile,
      authentication_methods: identities.map(identity => ({
        provider: identity.provider,
        email: identity.email,
        connected_at: identity.created_at
      })),
      sessions: sessions.map(session => ({
        ...session,
        device_label: session.device_label || 'Unrecognized device',
        current: session.id === currentSessionId
      })),
      deletion_requests: deletionRequests
    };
  });
}

async function updateAccountProfile(userId, displayName) {
  const normalized = String(displayName || '').trim();
  if (normalized.length > 100) throw createHttpError(400, 'display_name_too_long');
  return withConnection(async connection => {
    await repositories.updateUserDisplayName(connection, userId, normalized || null);
    const profile = await repositories.getAccountProfile(connection, userId);
    return { profile };
  });
}

async function revokeAccountSession(userId, sessionId) {
  return withConnection(async connection => {
    const revoked = await repositories.revokeUserSession(connection, userId, sessionId);
    if (!revoked) throw createHttpError(404, 'session_not_found');
    return { revoked: true };
  });
}

async function revokeOtherAccountSessions(userId, currentSessionId) {
  return withConnection(async connection => ({
    revoked: await repositories.revokeOtherUserSessions(connection, userId, currentSessionId)
  }));
}

async function revokeAllAccountSessions(userId) {
  return withConnection(async connection => ({
    revoked: await repositories.revokeAllUserSessions(connection, userId)
  }));
}

async function requestAccountDeletion(userId, confirmation) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const profile = await repositories.getAccountProfile(connection, userId);
      if (!profile) throw createHttpError(404, 'account_not_found');
      if (normalizeEmail(confirmation) !== profile.email) {
        throw createHttpError(400, 'account_deletion_confirmation_invalid');
      }
      const existing = await repositories.findOpenDeletionRequest(connection, {
        userId,
        workspaceId: null,
        scope: 'user'
      });
      if (existing) {
        await connection.commit();
        return { ...existing, existing: true };
      }
      const request = await repositories.createDeletionRequest(connection, {
        userId,
        email: profile.email,
        scope: 'user'
      });
      await repositories.createAuditLog(connection, {
        actorUserId: userId,
        action: 'account_deletion_requested',
        targetType: 'user',
        targetId: userId
      });
      await connection.commit();
      return request;
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function requestWorkspaceDeletion(userId, workspaceId, confirmation) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await requireMembership(connection, workspaceId, userId, 'deleteWorkspace');
      const [workspace, profile] = await Promise.all([
        repositories.getWorkspace(connection, workspaceId),
        repositories.getAccountProfile(connection, userId)
      ]);
      if (!workspace) throw createHttpError(404, 'workspace_not_found');
      if (String(confirmation || '').trim() !== workspace.name) {
        throw createHttpError(400, 'workspace_deletion_confirmation_invalid');
      }
      const existing = await repositories.findOpenDeletionRequest(connection, {
        userId,
        workspaceId,
        scope: 'workspace'
      });
      if (existing) {
        await connection.commit();
        return { ...existing, existing: true };
      }
      const request = await repositories.createDeletionRequest(connection, {
        userId,
        workspaceId,
        email: profile && profile.email,
        scope: 'workspace'
      });
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'workspace_deletion_requested',
        targetType: 'workspace',
        targetId: workspaceId
      });
      await connection.commit();
      return request;
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function createWorkspaceForUser(userId, name) {
  const cleanName = String(name || '').trim();
  if (!cleanName) {
    const error = new Error('invalid_workspace_name');
    error.status = 400;
    error.code = 'invalid_workspace_name';
    throw error;
  }
  return withConnection(connection => repositories.createWorkspace(connection, {
    name: cleanName,
    slug: `${slugify(cleanName)}-${randomToken(4).toLowerCase()}`,
    createdBy: userId
  }));
}

async function listWorkspaces(userId) {
  return withConnection(connection => repositories.listWorkspacesForUser(connection, userId));
}

async function requireMembership(connection, workspaceId, userId, capability) {
  const membership = await repositories.getMembership(connection, workspaceId, userId);
  if (!membership) {
    const error = new Error('workspace_not_found');
    error.status = 404;
    error.code = 'workspace_not_found';
    throw error;
  }
  if (capability) {
    assertCapability(membership.role, capability);
  }
  return membership;
}

async function listWorkspaceMembers(userId, workspaceId) {
  return withConnection(async connection => {
    await requireMembership(connection, workspaceId, userId, 'manageMembers');
    const [members, invitations] = await Promise.all([
      repositories.listMembers(connection, workspaceId),
      repositories.listInvitations(connection, workspaceId)
    ]);
    return { members, invitations };
  });
}

async function inviteMember(userId, workspaceId, email, role) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || !normalizedEmail.includes('@')) throw createHttpError(400, 'invalid_email');
  validateMailConfiguration();
  const token = randomToken(32);
  const result = await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const actor = await requireMembership(connection, workspaceId, userId, 'manageMembers');
      if (role === 'owner' || !canAssignRole(actor.role, role)) {
        throw createHttpError(403, 'invalid_role_assignment');
      }
      if (await repositories.getMembership(connection, workspaceId, userId)) {
        const actorProfile = await repositories.getAccountProfile(connection, userId);
        if (actorProfile && actorProfile.email === normalizedEmail) {
          throw createHttpError(409, 'already_a_member');
        }
      }
      const existingUser = await repositories.findUserByEmail(connection, normalizedEmail);
      if (existingUser && await repositories.getMembership(connection, workspaceId, existingUser.id)) {
        throw createHttpError(409, 'already_a_member');
      }
      if (await repositories.findPendingInvitationByEmail(connection, workspaceId, normalizedEmail)) {
        throw createHttpError(409, 'invitation_pending');
      }
      const [workspace, inviter] = await Promise.all([
        repositories.getWorkspace(connection, workspaceId),
        repositories.getAccountProfile(connection, userId)
      ]);
      if (!workspace || !inviter) throw createHttpError(404, 'workspace_not_found');
      const invitationId = createId();
      await repositories.createInvitation(connection, {
        id: invitationId,
        workspaceId,
        email: normalizedEmail,
        role,
        tokenHash: hashSecret(token),
        invitedBy: userId,
        ttlSeconds: INVITATION_TTL_SECONDS
      });
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'member_invited',
        targetType: 'invitation',
        targetId: invitationId,
        metadata: { email: normalizedEmail, role }
      });
      await connection.commit();
      return { workspace, inviter };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
  await sendInvitationEmail({
    email: normalizedEmail,
    token,
    workspaceName: result.workspace.name,
    inviterEmail: result.inviter.email
  });
  return { invited: true, dev_token: exposeDevelopmentAuthToken() ? token : undefined };
}

async function resendInvitation(userId, workspaceId, invitationId) {
  validateMailConfiguration();
  const token = randomToken(32);
  const result = await withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await requireMembership(connection, workspaceId, userId, 'manageMembers');
      const invitation = await repositories.findInvitationForUpdate(connection, workspaceId, invitationId);
      if (!invitation) throw createHttpError(404, 'invitation_not_found');
      if (invitation.accepted_at) throw createHttpError(409, 'invitation_already_accepted');
      if (invitation.revoked_at) throw createHttpError(409, 'invitation_revoked');
      if (Number(invitation.send_count || 0) >= INVITATION_MAX_SENDS) {
        throw createHttpError(429, 'invitation_send_limit_reached');
      }
      if (Number(invitation.seconds_since_last_send || 0) < INVITATION_RESEND_COOLDOWN_SECONDS) {
        throw createHttpError(429, 'invitation_resend_cooldown');
      }
      const inviter = await repositories.getAccountProfile(connection, userId);
      await repositories.rotateInvitation(connection, invitation.id, hashSecret(token), INVITATION_TTL_SECONDS);
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'invitation_resent',
        targetType: 'invitation',
        targetId: invitation.id,
        metadata: { email: invitation.email }
      });
      await connection.commit();
      return { invitation, inviter };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
  await sendInvitationEmail({
    email: result.invitation.email,
    token,
    workspaceName: result.invitation.workspace_name,
    inviterEmail: result.inviter && result.inviter.email
  });
  return { resent: true, dev_token: exposeDevelopmentAuthToken() ? token : undefined };
}

async function revokeInvitation(userId, workspaceId, invitationId) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      await requireMembership(connection, workspaceId, userId, 'manageMembers');
      const invitation = await repositories.findInvitationForUpdate(connection, workspaceId, invitationId);
      if (!invitation) throw createHttpError(404, 'invitation_not_found');
      if (invitation.accepted_at) throw createHttpError(409, 'invitation_already_accepted');
      if (invitation.revoked_at) return { revoked: true, existing: true };
      await repositories.revokeInvitation(connection, invitation.id);
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'invitation_revoked',
        targetType: 'invitation',
        targetId: invitation.id,
        metadata: { email: invitation.email }
      });
      await connection.commit();
      return { revoked: true };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function acceptWorkspaceInvitation(userId, token) {
  if (!token) throw createHttpError(400, 'invitation_token_required');
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const [invitation, profile] = await Promise.all([
        repositories.findInvitationByTokenForUpdate(connection, hashSecret(token)),
        repositories.getAccountProfile(connection, userId)
      ]);
      if (!invitation || invitation.revoked_at || invitation.accepted_at || Number(invitation.is_expired)) {
        throw createHttpError(400, 'invitation_invalid_or_expired');
      }
      if (!profile || normalizeEmail(profile.email) !== normalizeEmail(invitation.email)) {
        throw createHttpError(403, 'invitation_email_mismatch');
      }
      await repositories.acceptInvitation(connection, invitation, userId);
      await repositories.createAuditLog(connection, {
        workspaceId: invitation.workspace_id,
        actorUserId: userId,
        action: 'invitation_accepted',
        targetType: 'invitation',
        targetId: invitation.id,
        metadata: { role: invitation.role }
      });
      await connection.commit();
      return {
        accepted: true,
        workspace: {
          id: invitation.workspace_id,
          name: invitation.workspace_name,
          role: invitation.role
        }
      };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function updateMemberRole(userId, workspaceId, memberUserId, role) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const actor = await requireMembership(connection, workspaceId, userId, 'manageMembers');
      if (!canAssignRole(actor.role, role)) {
        const error = new Error('invalid_role_assignment');
        error.status = 403;
        error.code = 'invalid_role_assignment';
        throw error;
      }
      const current = await repositories.getMembership(connection, workspaceId, memberUserId);
      if (!current) {
        const error = new Error('member_not_found');
        error.status = 404;
        error.code = 'member_not_found';
        throw error;
      }
      if (current.role === 'owner' && actor.role !== 'owner') {
        throw createHttpError(403, 'owner_management_requires_owner');
      }
      if (current.role === 'owner' && role !== 'owner' && await repositories.countOwners(connection, workspaceId) <= 1) {
        const error = new Error('last_owner_required');
        error.status = 400;
        error.code = 'last_owner_required';
        throw error;
      }
      await repositories.updateMemberRole(connection, workspaceId, memberUserId, role);
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'member_role_updated',
        targetType: 'user',
        targetId: memberUserId,
        metadata: { previous_role: current.role, role }
      });
      await connection.commit();
      return { updated: true };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function removeMember(userId, workspaceId, memberUserId) {
  return withConnection(async connection => {
    await connection.beginTransaction();
    try {
      const actor = await requireMembership(connection, workspaceId, userId, 'manageMembers');
      const current = await repositories.getMembership(connection, workspaceId, memberUserId);
      if (!current) {
        const error = new Error('member_not_found');
        error.status = 404;
        error.code = 'member_not_found';
        throw error;
      }
      if (current.role === 'owner' && actor.role !== 'owner') {
        throw createHttpError(403, 'owner_management_requires_owner');
      }
      if (current.role === 'owner' && await repositories.countOwners(connection, workspaceId) <= 1) {
        const error = new Error('last_owner_required');
        error.status = 400;
        error.code = 'last_owner_required';
        throw error;
      }
      await repositories.removeMember(connection, workspaceId, memberUserId);
      await repositories.createAuditLog(connection, {
        workspaceId,
        actorUserId: userId,
        action: 'member_removed',
        targetType: 'user',
        targetId: memberUserId,
        metadata: { role: current.role }
      });
      await connection.commit();
      return { removed: true };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

module.exports = {
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
};
