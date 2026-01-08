const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const TOKEN_REFRESH_BUFFER_MS = 5 * 60 * 1000;
const TOKEN_STATE_TTL_MS = 10 * 60 * 1000;
const DEFAULT_PRUNE_DAYS = 30;
const LOCK_RETRY_DELAY_MS = 50;
const LOCK_TIMEOUT_MS = 5000;

function parseEncryptionKey(rawKey) {
  if (!rawKey) {
    throw new Error('Missing ENCRYPTION_KEY for token encryption.');
  }

  let keyBuffer = null;
  if (/^[0-9a-fA-F]{64}$/.test(rawKey)) {
    keyBuffer = Buffer.from(rawKey, 'hex');
  } else {
    keyBuffer = Buffer.from(rawKey, 'base64');
  }

  if (keyBuffer.length !== 32) {
    throw new Error('ENCRYPTION_KEY must be 32 bytes (base64 or hex).');
  }

  return keyBuffer;
}

function encryptValue(value, key) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const data = Buffer.concat([cipher.update(String(value), 'utf8'), cipher.final()]).toString('base64');
  const tag = cipher.getAuthTag().toString('base64');

  return {
    iv: iv.toString('base64'),
    tag,
    data
  };
}

function decryptValue(payload, key) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  try {
    const iv = Buffer.from(payload.iv, 'base64');
    const tag = Buffer.from(payload.tag, 'base64');
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const data = Buffer.concat([
      decipher.update(Buffer.from(payload.data, 'base64')),
      decipher.final()
    ]).toString('utf8');

    return data;
  } catch (error) {
    console.error('decryptValue failed to decrypt payload');
    return null;
  }
}

function safeTokenLabel(token) {
  if (!token) return 'unknown';
  return `${token.slice(0, 6)}...(${token.length})`;
}

async function writeFileAtomic(filePath, contents) {
  const dir = path.dirname(filePath);
  await fs.promises.mkdir(dir, { recursive: true });
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  const handle = await fs.promises.open(tempPath, 'w', 0o600);
  try {
    await handle.writeFile(contents, 'utf8');
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fs.promises.rename(tempPath, filePath);
  } catch (error) {
    await fs.promises.unlink(tempPath).catch(() => {});
    throw error;
  }
}

class FileTokenStore {
  constructor(options = {}) {
    this.filePath = options.filePath || path.join(__dirname, 'data', 'tokens.json');
    this.lockPath = options.lockPath || `${this.filePath}.lock`;
    this.pruneAfterDays = options.pruneAfterDays || DEFAULT_PRUNE_DAYS;
    this.encryptionKey = parseEncryptionKey(options.encryptionKey);
  }

  async withLock(fn) {
    const start = Date.now();
    let fd = null;
    while (!fd) {
      try {
        fd = fs.openSync(this.lockPath, 'wx', 0o600);
        fs.writeSync(fd, `${process.pid}\n`);
        fs.fsyncSync(fd);
      } catch (error) {
        if (error.code !== 'EEXIST') {
          throw error;
        }
        const staleCleared = this.tryClearStaleLock();
        if (staleCleared) {
          continue;
        }
        if (Date.now() - start > LOCK_TIMEOUT_MS) {
          throw new Error('Token store lock timeout.');
        }
        await new Promise(resolve => setTimeout(resolve, LOCK_RETRY_DELAY_MS));
      }
    }

    try {
      return await fn();
    } finally {
      try {
        fs.closeSync(fd);
      } catch (error) {
        // ignore
      }
      try {
        fs.unlinkSync(this.lockPath);
      } catch (error) {
        // ignore
      }
    }
  }

  tryClearStaleLock() {
    try {
      const pidText = fs.readFileSync(this.lockPath, 'utf8').trim();
      const pid = Number.parseInt(pidText, 10);
      if (!Number.isNaN(pid) && this.isProcessAlive(pid)) {
        return false;
      }
    } catch (error) {
      // ignore and attempt cleanup
    }
    try {
      fs.unlinkSync(this.lockPath);
      return true;
    } catch (error) {
      return false;
    }
  }

  isProcessAlive(pid) {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error) {
      return error.code === 'EPERM';
    }
  }

  async readData() {
    try {
      const raw = await fs.promises.readFile(this.filePath, 'utf8');
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : { version: 1, tokens: {} };
    } catch (error) {
      if (error.code === 'ENOENT') {
        return { version: 1, tokens: {} };
      }
      throw error;
    }
  }

  async writeData(data) {
    await writeFileAtomic(this.filePath, JSON.stringify(data, null, 2));
  }

  needsMigration(data) {
    if (!data || !data.tokens) {
      return false;
    }
    return Object.values(data.tokens).some(entry => entry && (entry.accessToken || entry.refreshToken));
  }

  migrateData(data) {
    const migrated = {
      version: 1,
      tokens: {}
    };
    Object.entries(data.tokens || {}).forEach(([token, entry]) => {
      if (!entry) return;
      const accessToken = entry.accessToken || entry.access_token;
      const refreshToken = entry.refreshToken || entry.refresh_token;
      if (!accessToken || !refreshToken) {
        return;
      }
      const now = Date.now();
      migrated.tokens[token] = {
        access_token: encryptValue(accessToken, this.encryptionKey),
        refresh_token: encryptValue(refreshToken, this.encryptionKey),
        expires_at: entry.expiresAt || entry.expires_at || now,
        refresh_expires_at: entry.refreshExpiresAt || entry.refresh_expires_at || now,
        scopes: entry.scope || entry.scopes || null,
        created_at: entry.createdAt || entry.created_at || now,
        updated_at: entry.updatedAt || entry.updated_at || now,
        open_id: entry.openId || entry.open_id || null
      };
    });
    return migrated;
  }

  pruneTokens(data) {
    const now = Date.now();
    const cutoff = now - this.pruneAfterDays * 24 * 60 * 60 * 1000;
    const tokens = data.tokens || {};
    const pruned = {};
    Object.entries(tokens).forEach(([token, entry]) => {
      if (!entry) return;
      const refreshExpired = entry.refresh_expires_at && entry.refresh_expires_at < now;
      const tooOld = entry.updated_at && entry.updated_at < cutoff;
      if (refreshExpired && tooOld) {
        return;
      }
      pruned[token] = entry;
    });
    return { ...data, tokens: pruned };
  }

  async saveConnectorToken(connectorToken, tokenData) {
    return this.withLock(async () => {
      let data = await this.readData();
      if (this.needsMigration(data)) {
        data = this.migrateData(data);
      }
      const now = Date.now();
      const existing = data.tokens[connectorToken];
      const createdAt = existing ? existing.created_at : now;
      data.tokens[connectorToken] = {
        access_token: encryptValue(tokenData.accessToken, this.encryptionKey),
        refresh_token: encryptValue(tokenData.refreshToken, this.encryptionKey),
        expires_at: tokenData.expiresAt || null,
        refresh_expires_at: tokenData.refreshExpiresAt || null,
        scopes: tokenData.scopes || null,
        created_at: createdAt,
        updated_at: now,
        open_id: tokenData.openId || null
      };
      data = this.pruneTokens(data);
      await this.writeData(data);
    });
  }

  async getConnectorToken(connectorToken) {
    return this.withLock(async () => {
      let data = await this.readData();
      if (this.needsMigration(data)) {
        data = this.migrateData(data);
        await this.writeData(data);
      }
      const entry = data.tokens[connectorToken];
      if (!entry) {
        return null;
      }
      const accessToken = decryptValue(entry.access_token, this.encryptionKey);
      const refreshToken = decryptValue(entry.refresh_token, this.encryptionKey);
      if (!accessToken || !refreshToken) {
        return null;
      }
      return {
        accessToken,
        refreshToken,
        expiresAt: entry.expires_at,
        refreshExpiresAt: entry.refresh_expires_at,
        scopes: entry.scopes,
        createdAt: entry.created_at,
        updatedAt: entry.updated_at,
        openId: entry.open_id || null
      };
    });
  }

  async revokeConnectorToken(connectorToken) {
    return this.withLock(async () => {
      let data = await this.readData();
      if (this.needsMigration(data)) {
        data = this.migrateData(data);
        await this.writeData(data);
      }
      const existed = Boolean(data.tokens[connectorToken]);
      if (existed) {
        delete data.tokens[connectorToken];
        await this.writeData(data);
      }
      return existed;
    });
  }

  async listTokensSafe() {
    return this.withLock(async () => {
      const data = await this.readData();
      return Object.entries(data.tokens || {}).map(([token, entry]) => ({
        token_prefix: safeTokenLabel(token),
        open_id: entry.open_id || null,
        scopes: entry.scopes || null,
        expires_at: entry.expires_at || null,
        refresh_expires_at: entry.refresh_expires_at || null,
        created_at: entry.created_at || null,
        updated_at: entry.updated_at || null
      }));
    });
  }

  isAccessTokenValid(tokenData) {
    return tokenData.expiresAt && Date.now() < tokenData.expiresAt - TOKEN_REFRESH_BUFFER_MS;
  }

  isRefreshTokenValid(tokenData) {
    return tokenData.refreshExpiresAt && Date.now() < tokenData.refreshExpiresAt - TOKEN_REFRESH_BUFFER_MS;
  }
}

class StateStore {
  constructor(ttlMs = TOKEN_STATE_TTL_MS, cleanupIntervalMs) {
    this.ttlMs = ttlMs;
    this.store = new Map();
    const interval = cleanupIntervalMs || Math.max(Math.floor(ttlMs / 2), 60 * 1000);
    this.intervalId = setInterval(() => this.pruneExpiredEntries(), interval);
  }

  save(state, data) {
    this.store.set(state, { ...data, expiresAt: Date.now() + this.ttlMs });
  }

  consume(state) {
    const entry = this.store.get(state);
    if (!entry) {
      return null;
    }
    this.store.delete(state);
    if (entry.expiresAt <= Date.now()) {
      return null;
    }
    return entry;
  }

  pruneExpiredEntries() {
    const now = Date.now();
    for (const [key, entry] of this.store.entries()) {
      if (!entry || entry.expiresAt <= now) {
        this.store.delete(key);
      }
    }
  }

  stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }
}

module.exports = {
  FileTokenStore,
  StateStore,
  TOKEN_REFRESH_BUFFER_MS
};
