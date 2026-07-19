const ROLES = ['owner', 'admin', 'analyst', 'viewer'];

const CAPABILITIES = {
  viewDashboard: new Set(['owner', 'admin', 'analyst', 'viewer']),
  triggerManualSync: new Set(['owner', 'admin', 'analyst']),
  exportCsv: new Set(['owner', 'admin', 'analyst']),
  manageReports: new Set(['owner', 'admin', 'analyst']),
  manageConnection: new Set(['owner', 'admin']),
  manageMembers: new Set(['owner', 'admin']),
  deleteWorkspace: new Set(['owner'])
};

function hasCapability(role, capability) {
  return Boolean(CAPABILITIES[capability] && CAPABILITIES[capability].has(role));
}

function assertCapability(role, capability) {
  if (!hasCapability(role, capability)) {
    const error = new Error('permission_denied');
    error.status = 403;
    error.code = 'permission_denied';
    throw error;
  }
}

function canAssignRole(actorRole, targetRole) {
  if (!ROLES.includes(targetRole)) {
    return false;
  }
  if (actorRole === 'owner') {
    return true;
  }
  return actorRole === 'admin' && targetRole !== 'owner';
}

module.exports = {
  ROLES,
  assertCapability,
  canAssignRole,
  hasCapability
};
