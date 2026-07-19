const { createId } = require('./security');

async function findUserByEmail(connection, email) {
  const rows = await connection.query('SELECT * FROM users WHERE email = ? AND deleted_at IS NULL LIMIT 1', [email]);
  return rows[0] || null;
}

async function createUser(connection, { email, displayName = null }) {
  const id = createId();
  await connection.query(
    'INSERT INTO users (id, email, display_name) VALUES (?, ?, ?)',
    [id, email, displayName]
  );
  await connection.query(
    'INSERT INTO user_identities (id, user_id, provider, provider_subject, email) VALUES (?, ?, ?, ?, ?)',
    [createId(), id, 'email', email, email]
  );
  return { id, email, display_name: displayName, status: 'active' };
}

async function findOrCreateUserByEmail(connection, email) {
  const existing = await findUserByEmail(connection, email);
  if (existing) return existing;
  return createUser(connection, { email });
}

async function saveMagicLinkToken(connection, record) {
  await connection.query(
    `INSERT INTO magic_link_tokens
      (id, email, user_id, token_hash, requested_ip_hash, expires_at)
     VALUES (?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
    [
      record.id,
      record.email,
      record.userId || null,
      record.tokenHash,
      record.requestedIpHash || null,
      record.ttlSeconds
    ]
  );
}

async function consumeMagicLinkToken(connection, tokenHash) {
  const rows = await connection.query(
    `SELECT * FROM magic_link_tokens
     WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > UTC_TIMESTAMP(3)
     LIMIT 1`,
    [tokenHash]
  );
  const token = rows[0] || null;
  if (!token) return null;
  await connection.query('UPDATE magic_link_tokens SET consumed_at = UTC_TIMESTAMP(3) WHERE id = ?', [token.id]);
  return token;
}

async function countRecentMagicLinks(connection, email, windowSeconds) {
  const rows = await connection.query(
    `SELECT COUNT(*) AS count FROM magic_link_tokens
     WHERE email = ? AND created_at > DATE_SUB(UTC_TIMESTAMP(3), INTERVAL ? SECOND)`,
    [email, windowSeconds]
  );
  return Number(rows[0].count || 0);
}

async function createSession(connection, record) {
  const id = createId();
  await connection.query(
    `INSERT INTO user_sessions
      (id, user_id, token_hash, csrf_token_hash, expires_at, idle_expires_at, user_agent_hash, device_label)
     VALUES (?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), ?, ?)`,
    [
      id,
      record.userId,
      record.tokenHash,
      record.csrfTokenHash,
      record.absoluteTtlSeconds,
      record.idleTtlSeconds,
      record.userAgentHash || null,
      record.deviceLabel || null
    ]
  );
  return { id };
}

async function findSessionByTokenHash(connection, tokenHash) {
  const rows = await connection.query(
    `SELECT s.*, u.email, u.display_name
     FROM user_sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token_hash = ?
       AND s.revoked_at IS NULL
       AND s.expires_at > UTC_TIMESTAMP(3)
       AND s.idle_expires_at > UTC_TIMESTAMP(3)
       AND u.deleted_at IS NULL
       AND u.status = 'active'
     LIMIT 1`,
    [tokenHash]
  );
  return rows[0] || null;
}

async function touchSession(connection, sessionId, idleTtlSeconds) {
  await connection.query(
    `UPDATE user_sessions
     SET last_seen_at = UTC_TIMESTAMP(3),
         idle_expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
     WHERE id = ?`,
    [idleTtlSeconds, sessionId]
  );
}

async function revokeSession(connection, sessionId) {
  await connection.query('UPDATE user_sessions SET revoked_at = UTC_TIMESTAMP(3) WHERE id = ?', [sessionId]);
}

async function listActiveSessions(connection, userId) {
  return connection.query(
    `SELECT id, device_label, created_at, last_seen_at, expires_at, idle_expires_at
     FROM user_sessions
     WHERE user_id = ?
       AND revoked_at IS NULL
       AND expires_at > UTC_TIMESTAMP(3)
       AND idle_expires_at > UTC_TIMESTAMP(3)
     ORDER BY last_seen_at DESC, created_at DESC`,
    [userId]
  );
}

async function revokeUserSession(connection, userId, sessionId) {
  const result = await connection.query(
    `UPDATE user_sessions SET revoked_at = UTC_TIMESTAMP(3)
     WHERE id = ? AND user_id = ? AND revoked_at IS NULL`,
    [sessionId, userId]
  );
  return Number(result.affectedRows || 0);
}

async function revokeOtherUserSessions(connection, userId, currentSessionId) {
  const result = await connection.query(
    `UPDATE user_sessions SET revoked_at = UTC_TIMESTAMP(3)
     WHERE user_id = ? AND id <> ? AND revoked_at IS NULL`,
    [userId, currentSessionId]
  );
  return Number(result.affectedRows || 0);
}

async function revokeAllUserSessions(connection, userId) {
  const result = await connection.query(
    `UPDATE user_sessions SET revoked_at = UTC_TIMESTAMP(3)
     WHERE user_id = ? AND revoked_at IS NULL`,
    [userId]
  );
  return Number(result.affectedRows || 0);
}

async function getAccountProfile(connection, userId) {
  const rows = await connection.query(
    `SELECT id, email, display_name, created_at, last_login_at
     FROM users WHERE id = ? AND deleted_at IS NULL LIMIT 1`,
    [userId]
  );
  return rows[0] || null;
}

async function listUserIdentities(connection, userId) {
  return connection.query(
    `SELECT provider, email, created_at
     FROM user_identities WHERE user_id = ? ORDER BY created_at ASC`,
    [userId]
  );
}

async function updateUserDisplayName(connection, userId, displayName) {
  await connection.query('UPDATE users SET display_name = ? WHERE id = ? AND deleted_at IS NULL', [
    displayName,
    userId
  ]);
}

async function createWorkspace(connection, { name, slug, createdBy }) {
  const id = createId();
  await connection.beginTransaction();
  try {
    await connection.query(
      'INSERT INTO workspaces (id, name, slug, created_by) VALUES (?, ?, ?, ?)',
      [id, name, slug, createdBy]
    );
    await connection.query(
      `INSERT INTO workspace_memberships (workspace_id, user_id, role, status)
       VALUES (?, ?, 'owner', 'active')`,
      [id, createdBy]
    );
    await connection.commit();
    return { id, name, slug, role: 'owner' };
  } catch (error) {
    await connection.rollback();
    throw error;
  }
}

async function listWorkspacesForUser(connection, userId) {
  return connection.query(
    `SELECT w.id, w.name, w.slug, m.role
     FROM workspaces w
     JOIN workspace_memberships m ON m.workspace_id = w.id
     WHERE m.user_id = ? AND m.status = 'active' AND w.deleted_at IS NULL
     ORDER BY w.created_at ASC`,
    [userId]
  );
}

async function getMembership(connection, workspaceId, userId) {
  const rows = await connection.query(
    `SELECT role, status FROM workspace_memberships
     WHERE workspace_id = ? AND user_id = ? AND status = 'active'
     LIMIT 1`,
    [workspaceId, userId]
  );
  return rows[0] || null;
}

async function listMembers(connection, workspaceId) {
  return connection.query(
    `SELECT u.id AS user_id, u.email, u.display_name, m.role, m.status, m.joined_at
     FROM workspace_memberships m
     JOIN users u ON u.id = m.user_id
     WHERE m.workspace_id = ? AND m.status = 'active'
     ORDER BY FIELD(m.role, 'owner', 'admin', 'analyst', 'viewer'), u.email`,
    [workspaceId]
  );
}

async function listInvitations(connection, workspaceId) {
  return connection.query(
    `SELECT i.id, i.email, i.role, i.created_at, i.last_sent_at, i.send_count,
            i.expires_at, i.accepted_at, i.revoked_at,
            inviter.email AS invited_by_email
     FROM workspace_invitations i
     JOIN users inviter ON inviter.id = i.invited_by
     WHERE i.workspace_id = ?
     ORDER BY i.created_at DESC, i.email`,
    [workspaceId]
  );
}

async function countOwners(connection, workspaceId) {
  const rows = await connection.query(
    `SELECT COUNT(*) AS count FROM workspace_memberships
     WHERE workspace_id = ? AND role = 'owner' AND status = 'active'`,
    [workspaceId]
  );
  return Number(rows[0].count || 0);
}

async function updateMemberRole(connection, workspaceId, userId, role) {
  await connection.query(
    `UPDATE workspace_memberships SET role = ?
     WHERE workspace_id = ? AND user_id = ? AND status = 'active'`,
    [role, workspaceId, userId]
  );
}

async function removeMember(connection, workspaceId, userId) {
  await connection.query(
    `UPDATE workspace_memberships SET status = 'removed'
     WHERE workspace_id = ? AND user_id = ? AND status = 'active'`,
    [workspaceId, userId]
  );
}

async function createInvitation(connection, record) {
  await connection.query(
    `INSERT INTO workspace_invitations
      (id, workspace_id, email, role, token_hash, invited_by, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND))`,
    [
      record.id,
      record.workspaceId,
      record.email,
      record.role,
      record.tokenHash,
      record.invitedBy,
      record.ttlSeconds
    ]
  );
}

async function findPendingInvitationByEmail(connection, workspaceId, email) {
  const rows = await connection.query(
    `SELECT id FROM workspace_invitations
     WHERE workspace_id = ? AND email = ?
       AND accepted_at IS NULL AND revoked_at IS NULL AND expires_at > UTC_TIMESTAMP(3)
     LIMIT 1`,
    [workspaceId, email]
  );
  return rows[0] || null;
}

async function findInvitationForUpdate(connection, workspaceId, invitationId) {
  const rows = await connection.query(
    `SELECT i.*, w.name AS workspace_name,
            TIMESTAMPDIFF(SECOND, i.last_sent_at, UTC_TIMESTAMP(3)) AS seconds_since_last_send,
            i.expires_at <= UTC_TIMESTAMP(3) AS is_expired
     FROM workspace_invitations i
     JOIN workspaces w ON w.id = i.workspace_id
     WHERE i.workspace_id = ? AND i.id = ? AND w.deleted_at IS NULL
     LIMIT 1 FOR UPDATE`,
    [workspaceId, invitationId]
  );
  return rows[0] || null;
}

async function findInvitationByTokenForUpdate(connection, tokenHash) {
  const rows = await connection.query(
    `SELECT i.*, w.name AS workspace_name,
            TIMESTAMPDIFF(SECOND, i.last_sent_at, UTC_TIMESTAMP(3)) AS seconds_since_last_send,
            i.expires_at <= UTC_TIMESTAMP(3) AS is_expired
     FROM workspace_invitations i
     JOIN workspaces w ON w.id = i.workspace_id
     WHERE i.token_hash = ? AND w.deleted_at IS NULL
     LIMIT 1 FOR UPDATE`,
    [tokenHash]
  );
  return rows[0] || null;
}

async function rotateInvitation(connection, invitationId, tokenHash, ttlSeconds) {
  await connection.query(
    `UPDATE workspace_invitations
     SET token_hash = ?, last_sent_at = UTC_TIMESTAMP(3), send_count = send_count + 1,
         expires_at = DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND)
     WHERE id = ?`,
    [tokenHash, ttlSeconds, invitationId]
  );
}

async function revokeInvitation(connection, invitationId) {
  await connection.query(
    `UPDATE workspace_invitations SET revoked_at = UTC_TIMESTAMP(3)
     WHERE id = ? AND accepted_at IS NULL AND revoked_at IS NULL`,
    [invitationId]
  );
}

async function acceptInvitation(connection, invitation, userId) {
  await connection.query(
    `INSERT INTO workspace_memberships
      (workspace_id, user_id, role, status, invited_by, joined_at)
     VALUES (?, ?, ?, 'active', ?, UTC_TIMESTAMP(3))
     ON DUPLICATE KEY UPDATE role = VALUES(role), status = 'active',
       invited_by = VALUES(invited_by), joined_at = UTC_TIMESTAMP(3)`,
    [invitation.workspace_id, userId, invitation.role, invitation.invited_by]
  );
  await connection.query(
    `UPDATE workspace_invitations
     SET accepted_at = UTC_TIMESTAMP(3), accepted_by_user_id = ?
     WHERE id = ? AND accepted_at IS NULL AND revoked_at IS NULL`,
    [userId, invitation.id]
  );
}

async function getWorkspace(connection, workspaceId) {
  const rows = await connection.query(
    'SELECT id, name, slug FROM workspaces WHERE id = ? AND deleted_at IS NULL LIMIT 1',
    [workspaceId]
  );
  return rows[0] || null;
}

async function createAuditLog(connection, record) {
  await connection.query(
    `INSERT INTO audit_logs
      (id, workspace_id, actor_user_id, action, target_type, target_id, metadata)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      createId(),
      record.workspaceId || null,
      record.actorUserId || null,
      record.action,
      record.targetType || null,
      record.targetId || null,
      record.metadata ? JSON.stringify(record.metadata) : null
    ]
  );
}

async function listDeletionRequests(connection, userId) {
  return connection.query(
    `SELECT d.id, d.workspace_id, d.scope, d.status, d.requested_at, d.completed_at,
            w.name AS workspace_name
     FROM data_deletion_requests d
     LEFT JOIN workspaces w ON w.id = d.workspace_id
     WHERE d.requester_user_id = ?
     ORDER BY d.requested_at DESC`,
    [userId]
  );
}

async function findOpenDeletionRequest(connection, { userId, workspaceId, scope }) {
  const rows = await connection.query(
    `SELECT id, status FROM data_deletion_requests
     WHERE requester_user_id = ? AND scope = ?
       AND ((? IS NULL AND workspace_id IS NULL) OR workspace_id = ?)
       AND status IN ('requested', 'verified', 'processing')
     LIMIT 1`,
    [userId, scope, workspaceId || null, workspaceId || null]
  );
  return rows[0] || null;
}

async function createDeletionRequest(connection, record) {
  const id = createId();
  await connection.query(
    `INSERT INTO data_deletion_requests
      (id, workspace_id, requester_user_id, requester_email, scope, status)
     VALUES (?, ?, ?, ?, ?, 'verified')`,
    [id, record.workspaceId || null, record.userId, record.email, record.scope]
  );
  return { id, status: 'verified' };
}

module.exports = {
  acceptInvitation,
  consumeMagicLinkToken,
  countOwners,
  countRecentMagicLinks,
  createAuditLog,
  createDeletionRequest,
  createInvitation,
  createSession,
  createWorkspace,
  findInvitationByTokenForUpdate,
  findInvitationForUpdate,
  findOpenDeletionRequest,
  findOrCreateUserByEmail,
  findPendingInvitationByEmail,
  findSessionByTokenHash,
  findUserByEmail,
  getAccountProfile,
  getMembership,
  getWorkspace,
  listActiveSessions,
  listDeletionRequests,
  listInvitations,
  listMembers,
  listUserIdentities,
  listWorkspacesForUser,
  removeMember,
  revokeAllUserSessions,
  revokeInvitation,
  revokeOtherUserSessions,
  revokeSession,
  revokeUserSession,
  rotateInvitation,
  saveMagicLinkToken,
  touchSession,
  updateUserDisplayName,
  updateMemberRole
};
