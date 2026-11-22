/**
 * Pipvane v3 - Cloud Control Plane
 * Cryptographic RS256 JWT Utility Module
 *
 * Implements RFC 7519 (JSON Web Token) and RFC 7515 (JSON Web Signature)
 * using Node.js native crypto module (Zero external dependencies).
 */

const crypto = require('node:crypto');

// Strict cryptographic constants (FR-8.1 & FR-8.4)
const THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60; // 2,592,000 seconds
const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60;   // 604,800 seconds

/**
 * Base64URL encode string or Buffer (RFC 4648 §5)
 * @param {string|Buffer} input
 * @returns {string}
 */
function base64urlEncode(input) {
  const buf = Buffer.isBuffer(input) ? input : Buffer.from(input, 'utf8');
  return buf
    .toString('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');
}

/**
 * Base64URL decode to Buffer
 * @param {string} input
 * @returns {Buffer}
 */
function base64urlDecode(input) {
  let base64 = input.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4 !== 0) {
    base64 += '=';
  }
  return Buffer.from(base64, 'base64');
}

/**
 * Base64URL decode to UTF-8 JSON object
 * @param {string} input
 * @returns {object}
 */
function base64urlDecodeJSON(input) {
  const jsonStr = base64urlDecode(input).toString('utf8');
  return JSON.parse(jsonStr);
}

/**
 * Sign payload using RS256 (RSA-SHA256)
 *
 * @param {object} payload - Claims object to include in JWT
 * @param {string|Buffer} privateKeyPem - RSA private key in PEM format (PKCS#1 or PKCS#8)
 * @param {object} [headerOptions] - Optional header overrides
 * @returns {string} Compact serialized JWT (header.payload.signature)
 */
function signRS256(payload, privateKeyPem, headerOptions = {}) {
  const header = {
    alg: 'RS256',
    typ: 'JWT',
    ...headerOptions,
  };

  const encodedHeader = base64urlEncode(JSON.stringify(header));
  const encodedPayload = base64urlEncode(JSON.stringify(payload));
  const signingInput = `${encodedHeader}.${encodedPayload}`;

  const signer = crypto.createSign('RSA-SHA256');
  signer.update(signingInput);
  signer.end();

  const signature = signer.sign(privateKeyPem);
  const encodedSignature = base64urlEncode(signature);

  return `${signingInput}.${encodedSignature}`;
}

/**
 * Verify RS256 signed JWT
 *
 * @param {string} token - Compact JWT string
 * @param {string|Buffer} publicKeyPem - RSA public key in PEM format (SPKI or PKCS#1)
 * @param {object} [options] - Verification options
 * @param {number} [options.clockTolerance=0] - Clock skew tolerance in seconds
 * @param {boolean} [options.ignoreExpiration=false] - Whether to bypass exp check
 * @param {number} [options.currentEpoch] - Explicit current timestamp for deterministic tests
 * @returns {object} Decoded payload if valid
 * @throws {Error} Detailed error if invalid or expired
 */
function verifyRS256(token, publicKeyPem, options = {}) {
  if (typeof token !== 'string') {
    const err = new Error('Token must be a string');
    err.code = 'INVALID_TOKEN_TYPE';
    throw err;
  }

  const parts = token.split('.');
  if (parts.length !== 3) {
    const err = new Error('Invalid JWT format: token must contain 3 parts');
    err.code = 'INVALID_TOKEN_FORMAT';
    throw err;
  }

  const [encodedHeader, encodedPayload, encodedSignature] = parts;

  let header;
  let payload;
  try {
    header = base64urlDecodeJSON(encodedHeader);
    payload = base64urlDecodeJSON(encodedPayload);
  } catch (parseErr) {
    const err = new Error('Failed to parse JWT header or payload JSON');
    err.code = 'MALFORMED_JSON';
    throw err;
  }

  if (header.alg !== 'RS256') {
    const err = new Error(`Unsupported algorithm: expected RS256, got ${header.alg}`);
    err.code = 'INVALID_ALGORITHM';
    throw err;
  }

  // Cryptographic signature check
  const signingInput = `${encodedHeader}.${encodedPayload}`;
  const signatureBuffer = base64urlDecode(encodedSignature);

  const verifier = crypto.createVerify('RSA-SHA256');
  verifier.update(signingInput);
  verifier.end();

  const isValidSignature = verifier.verify(publicKeyPem, signatureBuffer);
  if (!isValidSignature) {
    const err = new Error('RS256 cryptographic signature verification failed');
    err.code = 'INVALID_SIGNATURE';
    throw err;
  }

  // Expiration check (FR-8.1 30-day strict expiry)
  const clockTolerance = options.clockTolerance || 0;
  const now = options.currentEpoch !== undefined ? options.currentEpoch : Math.floor(Date.now() / 1000);

  if (!options.ignoreExpiration && payload.exp !== undefined) {
    if (typeof payload.exp !== 'number') {
      const err = new Error('Invalid "exp" claim: must be a numeric epoch timestamp');
      err.code = 'INVALID_EXP_CLAIM';
      throw err;
    }

    if (now >= payload.exp + clockTolerance) {
      const err = new Error(`Token expired at epoch ${payload.exp} (current epoch: ${now})`);
      err.code = 'TOKEN_EXPIRED';
      err.expiredAt = payload.exp;
      err.currentEpoch = now;
      throw err;
    }
  }

  // Not-before check
  if (payload.nbf !== undefined && typeof payload.nbf === 'number') {
    if (now < payload.nbf - clockTolerance) {
      const err = new Error(`Token not valid before epoch ${payload.nbf} (current epoch: ${now})`);
      err.code = 'TOKEN_NOT_YET_VALID';
      throw err;
    }
  }

  return payload;
}

/**
 * Decode JWT without signature verification (useful for debugging / claims inspection)
 * @param {string} token
 * @returns {{ header: object, payload: object }}
 */
function decodeJWT(token) {
  if (typeof token !== 'string') return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;

  try {
    return {
      header: base64urlDecodeJSON(parts[0]),
      payload: base64urlDecodeJSON(parts[1]),
    };
  } catch {
    return null;
  }
}

module.exports = {
  THIRTY_DAYS_SECONDS,
  SEVEN_DAYS_SECONDS,
  base64urlEncode,
  base64urlDecode,
  base64urlDecodeJSON,
  signRS256,
  verifyRS256,
  decodeJWT,
};
