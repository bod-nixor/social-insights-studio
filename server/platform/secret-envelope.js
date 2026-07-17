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

function encryptSecret(value) {
  const key = parseEncryptionKey(process.env.ENCRYPTION_KEY);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(String(value), 'utf8'), cipher.final()]);
  return {
    ciphertext: ciphertext.toString('base64'),
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
    keyVersion: process.env.ENCRYPTION_KEY_VERSION || 'local-v1'
  };
}

function decryptSecret(envelope) {
  const key = parseEncryptionKey(process.env.ENCRYPTION_KEY);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(envelope.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
  return Buffer.concat([
    decipher.update(Buffer.from(envelope.ciphertext, 'base64')),
    decipher.final()
  ]).toString('utf8');
}

module.exports = {
  decryptSecret,
  encryptSecret
};
