const REQUIRED_OPERATION_GROUPS = Object.freeze({
  authorization: Object.freeze([
    'buildAuthorizationUrl',
    'completeAuthorization',
    'refreshAuthorization',
    'inspectScopes',
    'revokeAuthorization'
  ]),
  resources: Object.freeze(['discoverResources', 'selectResource']),
  sync: Object.freeze(['synchronize']),
  deletion: Object.freeze(['deleteConnectionData'])
});

function contractError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

function assertStringArray(value, field) {
  if (!Array.isArray(value) || value.length === 0 || value.some(item => !String(item || '').trim())) {
    throw contractError(`invalid_adapter_${field}`);
  }
}

/**
 * Validate the executable provider boundary used by new integrations.
 * Existing TikTok, YouTube, and Meta services remain compatibility adapters until
 * they can move behind this interface without changing their tested behavior.
 *
 * @param {object} adapter
 * @returns {true}
 */
function assertProviderAdapterContract(adapter) {
  if (!adapter || typeof adapter !== 'object') throw contractError('invalid_provider_adapter');
  if (!String(adapter.provider || '').trim()) throw contractError('invalid_adapter_provider');
  if (adapter.contractVersion !== 1) throw contractError('unsupported_adapter_contract_version');
  assertStringArray(adapter.requiredScopes, 'required_scopes');
  assertStringArray(adapter.resourceTypes, 'resource_types');
  assertStringArray(adapter.capabilities, 'capabilities');

  for (const [groupName, methodNames] of Object.entries(REQUIRED_OPERATION_GROUPS)) {
    const group = adapter[groupName];
    if (!group || typeof group !== 'object') throw contractError(`missing_adapter_group:${groupName}`);
    for (const methodName of methodNames) {
      if (typeof group[methodName] !== 'function') {
        throw contractError(`missing_adapter_operation:${groupName}.${methodName}`);
      }
    }
  }
  return true;
}

function freezeOperationGroup(group) {
  return Object.freeze({ ...group });
}

function defineProviderAdapter(adapter) {
  assertProviderAdapterContract(adapter);
  return Object.freeze({
    ...adapter,
    requiredScopes: Object.freeze([...adapter.requiredScopes]),
    resourceTypes: Object.freeze([...adapter.resourceTypes]),
    capabilities: Object.freeze([...adapter.capabilities]),
    authorization: freezeOperationGroup(adapter.authorization),
    resources: freezeOperationGroup(adapter.resources),
    sync: freezeOperationGroup(adapter.sync),
    deletion: freezeOperationGroup(adapter.deletion)
  });
}

module.exports = {
  REQUIRED_OPERATION_GROUPS,
  assertProviderAdapterContract,
  defineProviderAdapter
};
