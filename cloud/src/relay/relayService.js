/**
 * Pipvane v3 - Cloud Control Plane
 * Encrypted Command Relay Service (Zero-Knowledge Message Broker)
 *
 * Implements Sprint 17:
 * - Directional Inbox/Outbox queues tied to license_id
 * - Strict Zero-Knowledge payload validation: separates routing metadata from ciphertext
 * - Strictly forbids unencrypted trade data, credentials, or signal parameters in the cloud
 * - FIFO ordering and asynchronous long-polling delivery
 */

const crypto = require('node:crypto');
const { EventEmitter } = require('node:events');
const { MemoryRelayQueue } = require('./relayQueue');

// Forbidden plain-text trade / credential keys that violate Zero-Knowledge
const FORBIDDEN_PLAINTEXT_KEYS = [
  'symbol',
  'lot',
  'lots',
  'volume',
  'order_type',
  'stop_loss',
  'sl',
  'take_profit',
  'tp',
  'price',
  'ticket',
  'broker',
  'login',
  'password',
  'investor_password',
  'signal_text',
  'command_name',
  'trade',
  'action_type',
  'account_number',
  'magic_number',
];

// Maximum allowed ciphertext payload size: 64 KB
const MAX_CIPHERTEXT_BYTES = 64 * 1024;

class ZeroKnowledgeViolationError extends Error {
  constructor(message, forbiddenKey) {
    super(message);
    this.name = 'ZeroKnowledgeViolationError';
    this.code = 'ZERO_KNOWLEDGE_VIOLATION';
    this.forbiddenKey = forbiddenKey;
  }
}

class RelayService extends EventEmitter {
  /**
   * @param {object} [options]
   * @param {object} [options.queue] - RelayQueue implementation instance
   * @param {number} [options.defaultTtlSeconds=3600] - Default envelope TTL (1 hour)
   */
  constructor(options = {}) {
    super();
    this.queue = options.queue || new MemoryRelayQueue();
    this.defaultTtlSeconds = options.defaultTtlSeconds || 3600;
  }

  /**
   * Deeply inspects an object to ensure no forbidden plaintext keys are present
   * @param {object} obj
   * @param {string} [path='']
   */
  _assertZeroKnowledgeCompliance(obj, path = '') {
    if (!obj || typeof obj !== 'object') return;

    for (const key of Object.keys(obj)) {
      const lowerKey = key.toLowerCase();
      if (FORBIDDEN_PLAINTEXT_KEYS.includes(lowerKey)) {
        throw new ZeroKnowledgeViolationError(
          `Zero-Knowledge Violation: Plain-text key "${key}" detected at "${path}${key}". All trading commands and credentials must be encapsulated strictly within the encrypted "ciphertext" field.`,
          key
        );
      }

      if (typeof obj[key] === 'object' && obj[key] !== null) {
        this._assertZeroKnowledgeCompliance(obj[key], `${path}${key}.`);
      }
    }
  }

  /**
   * Validate and sanitize an incoming relay envelope
   * Enforces:
   * 1. Top-level separation: { metadata, ciphertext }
   * 2. Non-empty opaque ciphertext string <= 64KB
   * 3. metadata.license_id required
   * 4. Zero-knowledge compliance: no plain-text trading signals / credentials
   *
   * @param {object} raw
   * @returns {object} Validated RelayEnvelope
   */
  validateEnvelope(raw) {
    if (!raw || typeof raw !== 'object') {
      const err = new Error('Relay envelope must be a valid JSON object');
      err.code = 'INVALID_ENVELOPE';
      throw err;
    }

    // 1. Zero-Knowledge compliance check on root object
    this._assertZeroKnowledgeCompliance(raw);

    // 2. Validate ciphertext field
    if (!raw.ciphertext || typeof raw.ciphertext !== 'string' || raw.ciphertext.trim().length === 0) {
      const err = new Error('Field "ciphertext" is required and must be a non-empty opaque string');
      err.code = 'MISSING_CIPHERTEXT';
      throw err;
    }

    const cipherLength = Buffer.byteLength(raw.ciphertext, 'utf8');
    if (cipherLength > MAX_CIPHERTEXT_BYTES) {
      const err = new Error(`Ciphertext exceeds maximum allowed size of ${MAX_CIPHERTEXT_BYTES} bytes (actual: ${cipherLength} bytes)`);
      err.code = 'PAYLOAD_TOO_LARGE';
      throw err;
    }

    // 3. Validate metadata
    const metadata = raw.metadata;
    if (!metadata || typeof metadata !== 'object') {
      const err = new Error('Field "metadata" is required and must be an object');
      err.code = 'MISSING_METADATA';
      throw err;
    }

    const licenseId = (metadata.license_id || metadata.licenseId || '').trim();
    if (!licenseId) {
      const err = new Error('metadata.license_id is required');
      err.code = 'MISSING_LICENSE_ID';
      throw err;
    }

    // Direction normalization
    const direction = (metadata.direction || 'mobile_to_engine').toLowerCase();
    if (direction !== 'mobile_to_engine' && direction !== 'engine_to_mobile') {
      const err = new Error(`Invalid direction "${metadata.direction}": must be "mobile_to_engine" or "engine_to_mobile"`);
      err.code = 'INVALID_DIRECTION';
      throw err;
    }

    const messageId = metadata.message_id || metadata.messageId || crypto.randomUUID();
    const timestamp = typeof metadata.timestamp === 'number' ? metadata.timestamp : Date.now();
    const ttlSeconds = typeof metadata.ttl_seconds === 'number' && metadata.ttl_seconds > 0
      ? Math.min(metadata.ttl_seconds, 86400) // 24hr max
      : this.defaultTtlSeconds;

    return {
      metadata: {
        message_id: messageId,
        license_id: licenseId,
        direction,
        sender_type: metadata.sender_type || (direction === 'mobile_to_engine' ? 'mobile_app' : 'local_engine'),
        sender_id: metadata.sender_id || null,
        recipient_id: metadata.recipient_id || (direction === 'mobile_to_engine' ? 'local_engine' : 'mobile_app'),
        timestamp,
        ttl_seconds: ttlSeconds,
        algorithm: metadata.algorithm || 'XChaCha20-Poly1305',
        iv: metadata.iv || null,
        tag: metadata.tag || null,
        salt: metadata.salt || null,
        aad: metadata.aad || null,
        key_id: metadata.key_id || null,
      },
      ciphertext: raw.ciphertext.trim(),
    };
  }

  /**
   * Push an encrypted command into the queue
   *
   * @param {object} params
   * @param {string} [params.licenseId] - Explicit override or extracted from envelope
   * @param {'inbox'|'outbox'} [params.queueType] - Defaults to 'inbox' for mobile_to_engine
   * @param {object} params.envelope - The relay envelope { metadata, ciphertext }
   * @returns {Promise<object>} Result
   */
  async pushCommand({ licenseId, queueType, envelope }) {
    const validated = this.validateEnvelope(envelope);
    const targetLicense = licenseId ? licenseId.trim() : validated.metadata.license_id;

    if (licenseId && validated.metadata.license_id !== targetLicense) {
      const err = new Error('license_id in path/header does not match envelope metadata.license_id');
      err.code = 'LICENSE_ID_MISMATCH';
      throw err;
    }

    // Determine target queue: inbox (mobile -> engine) or outbox (engine -> mobile)
    const targetQueue = queueType
      ? queueType.toLowerCase()
      : (validated.metadata.direction === 'mobile_to_engine' ? 'inbox' : 'outbox');

    const result = await this.queue.push(targetLicense, targetQueue, validated);

    // Emit event for real-time WebSocket dispatch
    this.emit('command_pushed', {
      licenseId: targetLicense,
      queueType: targetQueue,
      envelope: validated,
    });

    return {
      success: true,
      message_id: validated.metadata.message_id,
      license_id: targetLicense,
      queue: targetQueue,
      direction: validated.metadata.direction,
      timestamp: validated.metadata.timestamp,
    };
  }

  /**
   * Pop an encrypted command from the queue (FIFO)
   *
   * @param {object} params
   * @param {string} params.licenseId
   * @param {'inbox'|'outbox'} [params.queueType='inbox']
   * @param {number} [params.waitMs=0] - Long-polling wait duration in ms
   * @returns {Promise<object|null>} The popped envelope or null if empty
   */
  async popCommand({ licenseId, queueType = 'inbox', waitMs = 0 }) {
    if (!licenseId) {
      const err = new Error('licenseId is required for popCommand');
      err.code = 'MISSING_LICENSE_ID';
      throw err;
    }

    const envelope = await this.queue.pop(licenseId.trim(), queueType.toLowerCase(), waitMs);
    return envelope;
  }

  /**
   * Peek at pending messages without popping
   * @param {object} params
   * @param {string} params.licenseId
   * @param {'inbox'|'outbox'} [params.queueType='inbox']
   * @param {number} [params.limit=10]
   * @returns {Promise<Array<object>>}
   */
  async peekCommands({ licenseId, queueType = 'inbox', limit = 10 }) {
    if (!licenseId) {
      const err = new Error('licenseId is required for peekCommands');
      err.code = 'MISSING_LICENSE_ID';
      throw err;
    }
    return await this.queue.peek(licenseId.trim(), queueType.toLowerCase(), limit);
  }

  /**
   * Acknowledge and remove a processed message
   * @param {object} params
   * @param {string} params.licenseId
   * @param {'inbox'|'outbox'} [params.queueType='inbox']
   * @param {string} params.messageId
   * @returns {Promise<boolean>}
   */
  async ackCommand({ licenseId, queueType = 'inbox', messageId }) {
    if (!licenseId || !messageId) {
      const err = new Error('licenseId and messageId are required for ackCommand');
      err.code = 'INVALID_ACK_PARAMS';
      throw err;
    }
    return await this.queue.ack(licenseId.trim(), queueType.toLowerCase(), messageId);
  }

  /**
   * Get queue statistics for a license
   * @param {string} licenseId
   * @returns {Promise<object>}
   */
  async getStats(licenseId) {
    if (!licenseId) {
      const err = new Error('licenseId is required');
      err.code = 'MISSING_LICENSE_ID';
      throw err;
    }
    return await this.queue.getStats(licenseId.trim());
  }

  async clear(licenseId) {
    await this.queue.clear(licenseId);
  }
}

module.exports = {
  RelayService,
  ZeroKnowledgeViolationError,
  FORBIDDEN_PLAINTEXT_KEYS,
  MAX_CIPHERTEXT_BYTES,
};
