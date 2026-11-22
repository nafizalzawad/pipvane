/**
 * Pipvane v3 - Cloud Control Plane
 * Cryptographic RSA Key Management Module
 *
 * Manages 2048-bit and 4096-bit RSA keypairs for RS256 token signing and verification.
 */

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

let cachedKeyPair = null;

/**
 * Generate a new RSA Key Pair in PEM format
 * @param {number} [modulusLength=2048] 2048 or 4096 bits
 * @returns {{ privateKey: string, publicKey: string, kid: string }}
 */
function generateRSAKeyPair(modulusLength = 2048) {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength,
    publicKeyEncoding: {
      type: 'spki',
      format: 'pem',
    },
    privateKeyEncoding: {
      type: 'pkcs8',
      format: 'pem',
    },
  });

  // Derive unique Key ID from SHA-256 fingerprint of public key
  const kid = crypto
    .createHash('sha256')
    .update(publicKey)
    .digest('hex')
    .substring(0, 16);

  return { privateKey, publicKey, kid };
}

/**
 * Load or initialize the active RSA key pair
 * Checks:
 * 1. Process environment variables PIPVANE_RSA_PRIVATE_KEY / PIPVANE_RSA_PUBLIC_KEY
 * 2. Key files on disk (if PIPVANE_KEY_DIR is provided)
 * 3. Fallback to generating an in-memory key pair for local execution / testing
 *
 * @param {object} [options]
 * @param {boolean} [options.forceNew=false]
 * @param {number} [options.modulusLength=2048]
 * @returns {{ privateKey: string, publicKey: string, kid: string }}
 */
function getKeyPair(options = {}) {
  if (cachedKeyPair && !options.forceNew) {
    return cachedKeyPair;
  }

  // 1. Environment variables
  if (process.env.PIPVANE_RSA_PRIVATE_KEY && process.env.PIPVANE_RSA_PUBLIC_KEY) {
    const privateKey = process.env.PIPVANE_RSA_PRIVATE_KEY.replace(/\\n/g, '\n');
    const publicKey = process.env.PIPVANE_RSA_PUBLIC_KEY.replace(/\\n/g, '\n');
    const kid = process.env.PIPVANE_RSA_KID ||
      crypto.createHash('sha256').update(publicKey).digest('hex').substring(0, 16);

    cachedKeyPair = { privateKey, publicKey, kid };
    return cachedKeyPair;
  }

  // 2. Local Key Directory if specified
  if (process.env.PIPVANE_KEY_DIR) {
    const privPath = path.join(process.env.PIPVANE_KEY_DIR, 'private_key.pem');
    const pubPath = path.join(process.env.PIPVANE_KEY_DIR, 'public_key.pem');

    if (fs.existsSync(privPath) && fs.existsSync(pubPath)) {
      const privateKey = fs.readFileSync(privPath, 'utf8');
      const publicKey = fs.readFileSync(pubPath, 'utf8');
      const kid = crypto.createHash('sha256').update(publicKey).digest('hex').substring(0, 16);

      cachedKeyPair = { privateKey, publicKey, kid };
      return cachedKeyPair;
    }
  }

  // 3. In-memory generation
  const generated = generateRSAKeyPair(options.modulusLength || 2048);
  cachedKeyPair = generated;
  return cachedKeyPair;
}

/**
 * Reset cached key pair (primarily for testing key rotation)
 */
function resetKeyPairCache() {
  cachedKeyPair = null;
}

module.exports = {
  generateRSAKeyPair,
  getKeyPair,
  resetKeyPairCache,
};
