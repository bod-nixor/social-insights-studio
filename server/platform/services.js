const { getConnection } = require('../database');
const repositories = require('./repositories');
const { verifyGoogleIdToken } = require('./google-oidc');
const { sendMagicLinkEmail, validateMailConfiguration } = require('./mail');
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

async function verifyMagicLink(token, userAgentHash) {
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
        userAgentHash
      });
      await connection.commit();
      return { user, sessionToken, csrfToken };
    } catch (error) {
      await connection.rollback();
      throw error;
    }
  });
}

async function verifyGoogleOidc({ idToken, nonce, userAgentHash }) {
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
        userAgentHash
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
  return withConnection(async connection => {
    const actor = await requireMembership(connection, workspaceId, userId, 'manageMembers');
    if (!canAssignRole(actor.role, role)) {
      const error = new Error('invalid_role_assignment');
      error.status = 403;
      error.code = 'invalid_role_assignment';
      throw error;
    }
    const token = randomToken(32);
    await repositories.createInvitation(connection, {
      id: createId(),
      workspaceId,
      email: normalizedEmail,
      role,
      tokenHash: hashSecret(token),
      invitedBy: userId,
      ttlSeconds: 60 * 60 * 24 * 7
    });
    return { invited: true, dev_token: process.env.NODE_ENV !== 'production' ? token : undefined };
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
      if (current.role === 'owner' && role !== 'owner' && await repositories.countOwners(connection, workspaceId) <= 1) {
        const error = new Error('last_owner_required');
        error.status = 400;
        error.code = 'last_owner_required';
        throw error;
      }
      await repositories.updateMemberRole(connection, workspaceId, memberUserId, role);
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
      await requireMembership(connection, workspaceId, userId, 'manageMembers');
      const current = await repositories.getMembership(connection, workspaceId, memberUserId);
      if (!current) {
        const error = new Error('member_not_found');
        error.status = 404;
        error.code = 'member_not_found';
        throw error;
      }
      if (current.role === 'owner' && await repositories.countOwners(connection, workspaceId) <= 1) {
        const error = new Error('last_owner_required');
        error.status = 400;
        error.code = 'last_owner_required';
        throw error;
      }
      await repositories.removeMember(connection, workspaceId, memberUserId);
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
};
