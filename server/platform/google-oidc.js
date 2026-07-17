const crypto = require('crypto');
const jwt = require('jsonwebtoken');
let fetchImpl = require('node-fetch');

const GOOGLE_JWKS_URL = 'https://www.googleapis.com/oauth2/v3/certs';
const GOOGLE_ISSUERS = ['https://accounts.google.com', 'accounts.google.com'];

let cachedKeys = null;
let cachedUntil = 0;

function setGoogleOidcFetchImplementation(nextFetch) {
  fetchImpl = nextFetch || require('node-fetch');
  cachedKeys = null;
  cachedUntil = 0;
}

async function readJson(response) {
  const text = await response.text();
  return text ? JSON.parse(text) : {};
}

async function fetchJwks() {
  if (cachedKeys && cachedUntil > Date.now()) return cachedKeys;
  const response = await fetchImpl(GOOGLE_JWKS_URL);
  if (!response.ok) {
    const error = new Error('google_jwks_unavailable');
    error.status = 503;
    error.code = 'google_jwks_unavailable';
    throw error;
  }
  const body = await readJson(response);
  cachedKeys = Array.isArray(body.keys) ? body.keys : [];
  cachedUntil = Date.now() + 60 * 60 * 1000;
  return cachedKeys;
}

async function getPublicKey(kid) {
  const keys = await fetchJwks();
  const jwk = keys.find(key => key.kid === kid && key.kty === 'RSA');
  if (!jwk) {
    const error = new Error('google_key_not_found');
    error.status = 401;
    error.code = 'invalid_google_token';
    throw error;
  }
  return crypto.createPublicKey({ key: jwk, format: 'jwk' }).export({ type: 'spki', format: 'pem' });
}

async function verifyGoogleIdToken(idToken, { audience, nonce }) {
  if (!idToken || !audience || !nonce) {
    const error = new Error('invalid_oidc_request');
    error.status = 400;
    error.code = 'invalid_oidc_request';
    throw error;
  }
  const decoded = jwt.decode(idToken, { complete: true });
  if (!decoded || decoded.header.alg !== 'RS256' || !decoded.header.kid) {
    const error = new Error('invalid_google_token');
    error.status = 401;
    error.code = 'invalid_google_token';
    throw error;
  }
  const publicKey = await getPublicKey(decoded.header.kid);
  let claims;
  try {
    claims = jwt.verify(idToken, publicKey, {
      algorithms: ['RS256'],
      audience,
      issuer: GOOGLE_ISSUERS
    });
  } catch (error) {
    const invalid = new Error('invalid_google_token');
    invalid.status = 401;
    invalid.code = 'invalid_google_token';
    throw invalid;
  }
  if (claims.nonce !== nonce || !claims.sub || !claims.email || claims.email_verified !== true) {
    const error = new Error('invalid_google_claims');
    error.status = 401;
    error.code = 'invalid_google_claims';
    throw error;
  }
  return {
    subject: claims.sub,
    email: claims.email,
    displayName: claims.name || claims.email
  };
}

module.exports = {
  setGoogleOidcFetchImplementation,
  verifyGoogleIdToken
};
