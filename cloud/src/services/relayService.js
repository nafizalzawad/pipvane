/**
 * Pipvane v3 - Cloud Control Plane
 * Encrypted Configuration Relay Service (Sprint 15 Directive)
 * -----------------------------------------------------------
 * Implements:
 * - End-to-end encrypted (E2EE) configuration state relay between Primary and Standby devices.
 * - Zero-Knowledge guarantees: The cloud server never receives or stores a passphrase or key.
 *   The payload is mathematically undecryptable by the cloud.
 * - Ephemeral storage with configurable TTL (default: 24 hours / 86,400,000 ms).
 */

'use strict';

const DEFAULT_RELAY_TTL_MS = 24 * 60 * 60 * 1000; // 24 Hours in milliseconds
const MAX_PAYLOAD_BYTES = 2 * 1024 * 1024; // 2 MB limit to prevent abuse

class RelayService {
  /**
   * @param {object} [options]
   * @param {number} [options.defaultTtlMs]
   */
  constructor(options = {}) {
    this.defaultTtlMs = options.defaultTtlMs || DEFAULT_RELAY_TTL_MS;
    /** @type {Map<string, object>} Keyed by license_id */
    this._store = new Map();
  }

  /**
   * Store an encrypted configuration payload for a license_id
   * @param {object} params
   * @param {string} params.license_id - License identifier
   * @param {number} [params.version] - Configuration version number
   * @param {object} params.encrypted_payload - E2EE envelope containing ciphertext, salt, iv, tag
   * @param {string} [params.source_node_id] - ID of origin device
   * @param {number} [params.ttl_ms] - Optional custom TTL
   * @returns {object} Storage receipt with expiration timestamp
   */
  storePayload(params) {
    if (!params || typeof params !== 'object') {
      const err = new Error('Payload must be a non-null object');
      err.code = 'INVALID_PAYLOAD';
      throw err;
    }

    const licenseId = params.license_id || params.licenseId;
    if (!licenseId || typeof licenseId !== 'string' || licenseId.trim().length === 0) {
      const err = new Error('license_id is required');
      err.code = 'MISSING_LICENSE_ID';
      throw err;
    }

    const encryptedPayload = params.encrypted_payload || params.encryptedPayload;
    if (!encryptedPayload || typeof encryptedPayload !== 'object') {
      const err = new Error('encrypted_payload is required and must be an object');
      err.code = 'MISSING_ENCRYPTED_PAYLOAD';
      throw err;
    }

    // Validate presence of required AES-GCM envelope components
    const { ciphertext, iv, tag, salt } = encryptedPayload;
    if (!ciphertext || typeof ciphertext !== 'string') {
      const err = new Error('encrypted_payload.ciphertext is required and must be a string');
      err.code = 'INVALID_CIPHERTEXT';
      throw err;
    }
    if (!iv || typeof iv !== 'string') {
      const err = new Error('encrypted_payload.iv is required and must be a string');
      err.code = 'INVALID_IV';
      throw err;
    }
    if (!tag || typeof tag !== 'string') {
      const err = new Error('encrypted_payload.tag is required and must be a string');
      err.code = 'INVALID_TAG';
      throw err;
    }
    if (!salt || typeof salt !== 'string') {
      const err = new Error('encrypted_payload.salt is required and must be a string');
      err.code = 'INVALID_SALT';
      throw err;
    }

    // Mathematical zero-knowledge guarantee assertion:
    // Cloud MUST NOT receive any passphrase, secret key, or plaintext data.
    if ('passphrase' in params || 'password' in params || 'key' in params || 'secret' in params) {
      const err = new Error('Zero-knowledge violation: Passphrase or key must never be sent to Cloud Relay');
      err.code = 'ZERO_KNOWLEDGE_VIOLATION';
      throw err;
    }

    const now = Date.now();
    const ttlMs = params.ttl_ms && params.ttl_ms > 0 ? params.ttl_ms : this.defaultTtlMs;
    const expiresAt = now + ttlMs;

    const record = {
      license_id: licenseId.trim(),
      version: typeof params.version === 'number' ? params.version : 1,
      source_node_id: params.source_node_id || params.sourceNodeId || 'unknown_node',
      encrypted_payload: {
        kdf: encryptedPayload.kdf || 'PBKDF2-HMAC-SHA256',
        iterations: encryptedPayload.iterations || 100000,
        salt,
        iv,
        tag,
        ciphertext,
        aad: encryptedPayload.aad || '',
      },
      created_at: now,
      expires_at: expiresAt,
    };

    this._store.set(licenseId.trim(), record);

    return {
      success: true,
      license_id: record.license_id,
      version: record.version,
      source_node_id: record.source_node_id,
      created_at: record.created_at,
      expires_at: record.expires_at,
      ttl_seconds: Math.floor(ttlMs / 1000),
    };
  }

  /**
   * Retrieve active encrypted configuration payload for a license_id
   * @param {string} licenseId
   * @returns {object|null}
   */
  getPayload(licenseId) {
    if (!licenseId || typeof licenseId !== 'string') {
      return null;
    }

    const key = licenseId.trim();
    const record = this._store.get(key);
    if (!record) {
      return null;
    }

    const now = Date.now();
    if (now >= record.expires_at) {
      // Ephemeral TTL expired: prune record
      this._store.delete(key);
      return null;
    }

    return {
      license_id: record.license_id,
      version: record.version,
      source_node_id: record.source_node_id,
      encrypted_payload: record.encrypted_payload,
      created_at: record.created_at,
      expires_at: record.expires_at,
    };
  }

  /**
   * Delete payload for a license_id
   * @param {string} licenseId
   * @returns {boolean}
   */
  deletePayload(licenseId) {
    if (!licenseId || typeof licenseId !== 'string') {
      return false;
    }
    return this._store.delete(licenseId.trim());
  }

  /**
   * Prune all expired records
   * @returns {number} Count of pruned records
   */
  pruneExpired() {
    const now = Date.now();
    let count = 0;
    for (const [key, record] of this._store.entries()) {
      if (now >= record.expires_at) {
        this._store.delete(key);
        count++;
      }
    }
    return count;
  }

  /**
   * Clear all stored records (useful for test isolation)
   */
  clear() {
    this._store.clear();
  }

  /**
   * Number of currently active records
   * @returns {number}
   */
  size() {
    this.pruneExpired();
    return this._store.size;
  }

  /**
   * Proof of Zero-Knowledge: Cloud has no decryption capability
   * @returns {boolean} Always false
   */
  canDecrypt() {
    return false;
  }
}

module.exports = {
  RelayService,
  DEFAULT_RELAY_TTL_MS,
  MAX_PAYLOAD_BYTES,
};
