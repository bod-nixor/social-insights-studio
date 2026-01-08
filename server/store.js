const TOKEN_STATE_TTL_MS = 10 * 60 * 1000;
const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;

class InMemoryTokenStore {
  constructor() {
    this.stateStore = new Map();
    this.connectorStore = new Map();
  }

  saveState(state, data) {
    this.stateStore.set(state, { ...data, expiresAt: Date.now() + TOKEN_STATE_TTL_MS });
  }

  consumeState(state) {
    const entry = this.stateStore.get(state);
    if (!entry) {
      return null;
    }
    this.stateStore.delete(state);
    if (entry.expiresAt <= Date.now()) {
      return null;
    }
    return entry;
  }

  saveConnectorToken(connectorToken, tokenData) {
    this.connectorStore.set(connectorToken, { ...tokenData, updatedAt: Date.now() });
  }

  getConnectorToken(connectorToken) {
    return this.connectorStore.get(connectorToken) || null;
  }

  revokeConnectorToken(connectorToken) {
    return this.connectorStore.delete(connectorToken);
  }

  isAccessTokenValid(tokenData) {
    return tokenData.expiresAt && Date.now() < tokenData.expiresAt - TOKEN_REFRESH_BUFFER_MS;
  }

  isRefreshTokenValid(tokenData) {
    return tokenData.refreshExpiresAt && Date.now() < tokenData.refreshExpiresAt - TOKEN_REFRESH_BUFFER_MS;
  }
}

module.exports = {
  InMemoryTokenStore,
  TOKEN_REFRESH_BUFFER_MS
};
