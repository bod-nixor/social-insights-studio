const crypto = require('crypto');

function parseEncryptionKey(rawKey) {
  if (!rawKey) {
    throw new Error('Missing ENCRYPTION_KEY.');
  }
  if (/^[0-9a-fA-F]{64}$/.test(rawKey)) {
    return Buffer.from(rawKey, 'hex');
  }
  const decoded = Buffer.from(rawKey, 'base64');
  if (decoded.length === 32) {
    return decoded;
  }
  throw new Error('ENCRYPTION_KEY must be 32 bytes as hex or base64.');
}

function getCurrentKeyVersion() {
  return process.env.ENCRYPTION_KEY_VERSION || 'local-v1';
}

function parsePreviousKeys() {
  const entries = String(process.env.ENCRYPTION_PREVIOUS_KEYS || '')
    .split(',')
    .map(entry => entry.trim())
    .filter(Boolean);
  const keys = new Map();
  for (const entry of entries) {
    const separatorIndex = entry.indexOf(':');
    if (separatorIndex <= 0) {
      throw new Error('ENCRYPTION_PREVIOUS_KEYS entries must use version:key format.');
    }
    const version = entry.slice(0, separatorIndex);
    const rawKey = entry.slice(separatorIndex + 1);
    keys.set(version, parseEncryptionKey(rawKey));
  }
  return keys;
}

function getKeyForVersion(version) {
  const currentVersion = getCurrentKeyVersion();
  if (!version || version === currentVersion) {
    return parseEncryptionKey(process.env.ENCRYPTION_KEY);
  }
  const previous = parsePreviousKeys();
  const key = previous.get(version);
  if (!key) {
    throw new Error('Encryption key version is not available.');
  }
  return key;
}

function encryptSecret(value) {
  const key = parseEncryptionKey(process.env.ENCRYPTION_KEY);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(String(value), 'utf8'), cipher.final()]);
  return {
    ciphertext: ciphertext.toString('base64'),
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
    keyVersion: getCurrentKeyVersion()
  };
}

function decryptSecret(envelope) {
  const key = getKeyForVersion(envelope && envelope.keyVersion);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(envelope.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
  return Buffer.concat([
    decipher.update(Buffer.from(envelope.ciphertext, 'base64')),
    decipher.final()
  ]).toString('utf8');
}

module.exports = {
  decryptSecret,
  encryptSecret,
  parsePreviousKeys
};
