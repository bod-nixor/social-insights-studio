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
      (id, user_id, token_hash, csrf_token_hash, expires_at, idle_expires_at, user_agent_hash)
     VALUES (?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? SECOND), ?)`,
    [
      id,
      record.userId,
      record.tokenHash,
      record.csrfTokenHash,
      record.absoluteTtlSeconds,
      record.idleTtlSeconds,
      record.userAgentHash || null
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

module.exports = {
  consumeMagicLinkToken,
  countOwners,
  countRecentMagicLinks,
  createInvitation,
  createSession,
  createWorkspace,
  findOrCreateUserByEmail,
  findSessionByTokenHash,
  getMembership,
  listMembers,
  listWorkspacesForUser,
  removeMember,
  revokeSession,
  saveMagicLinkToken,
  touchSession,
  updateMemberRole
};
