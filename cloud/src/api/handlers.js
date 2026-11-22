/**
 * Pipvane v3 - Cloud Control Plane
 * HTTP Request Handlers for Licensing & Token Lifecycle
 */

const { LicenseService } = require('../services/licenseService');
const { RelayService } = require('../services/relayService');
const { RelayService: CommandRelayService } = require('../relay/relayService');

/**
 * Helper to parse JSON body from incoming request
 * @param {object} req
 * @returns {Promise<object>}
 */
async function parseJsonBody(req) {
  if (req.body && typeof req.body === 'object') {
    return req.body;
  }

  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', chunk => {
      raw += chunk;
      if (raw.length > 1024 * 1024) { // 1MB payload limit
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      if (!raw || raw.trim().length === 0) {
        resolve({});
        return;
      }
      try {
        const parsed = JSON.parse(raw);
        resolve(parsed);
      } catch (err) {
        const error = new Error('Invalid JSON payload');
        error.code = 'INVALID_JSON';
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

/**
 * Standard JSON response helper
 * @param {object} res
 * @param {number} statusCode
 * @param {object} data
 */
function sendJSON(res, statusCode, data) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store, no-cache, must-revalidate',
    'X-Content-Type-Options': 'nosniff',
  });
  res.end(JSON.stringify(data));
}

/**
 * Create API router / dispatcher with an injected LicenseService instance
 * @param {LicenseService} [service]
 * @returns {object} API controller functions
 */
function createLicenseApi(service, relay, commandRelay) {
  const licenseService = service || new LicenseService();
  const relayService = relay || new RelayService();
  const commandRelayService = commandRelay || new CommandRelayService();

  /**
   * POST /api/license/issue
   */
  async function handleIssue(req, res) {
    try {
      const body = await parseJsonBody(req);
      const {
        license_id,
        licenseId,
        device_fingerprint,
        deviceFingerprint,
        tier,
        account_limit,
        accountLimit,
        edition,
        current_epoch,
        currentEpoch,
        custom_claims,
        customClaims,
      } = body;

      const targetLicenseId = license_id || licenseId;
      const targetFingerprint = device_fingerprint || deviceFingerprint;
      const targetLimit = account_limit !== undefined ? account_limit : accountLimit;
      const targetEpoch = current_epoch !== undefined ? current_epoch : currentEpoch;
      const targetEdition = edition || (custom_claims && custom_claims.edition) || (customClaims && customClaims.edition);

      if (!targetLicenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id is required',
          code: 'MISSING_LICENSE_ID',
        });
      }

      if (!targetFingerprint) {
        return sendJSON(res, 400, {
          success: false,
          error: 'device_fingerprint is required',
          code: 'MISSING_DEVICE_FINGERPRINT',
        });
      }

      const result = await licenseService.issueLicense({
        licenseId: targetLicenseId,
        deviceFingerprint: targetFingerprint,
        tier: tier || 'Standard',
        accountLimit: targetLimit,
        edition: targetEdition,
        currentEpoch: targetEpoch,
        customClaims: custom_claims || customClaims || {},
      });

      return sendJSON(res, 201, result);
    } catch (err) {
      const statusCode = err.code && (
        err.code.startsWith('INVALID_') ||
        err.code === 'ACCOUNT_LIMIT_EXCEEDED'
      ) ? 400 : 500;

      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'INTERNAL_ERROR',
      });
    }
  }

  /**
   * POST /api/license/refresh
   * Enforces 7-day silent refresh loop, single-use token rotation, anti-replay
   */
  async function handleRefresh(req, res) {
    try {
      const body = await parseJsonBody(req);

      // Extract token from body or Authorization: Bearer <token>
      let token = body.token;
      if (!token && req.headers && req.headers.authorization) {
        const authHeader = req.headers.authorization.trim();
        if (authHeader.startsWith('Bearer ')) {
          token = authHeader.substring(7).trim();
        }
      }

      const deviceFingerprint = body.device_fingerprint || body.deviceFingerprint;
      const currentEpoch = body.current_epoch !== undefined ? body.current_epoch : body.currentEpoch;

      if (!token) {
        return sendJSON(res, 400, {
          success: false,
          error: 'token is required for refresh',
          code: 'MISSING_TOKEN',
        });
      }

      if (!deviceFingerprint) {
        return sendJSON(res, 400, {
          success: false,
          error: 'device_fingerprint is required for refresh verification',
          code: 'MISSING_DEVICE_FINGERPRINT',
        });
      }

      const result = await licenseService.refreshLicense({
        token,
        deviceFingerprint,
        currentEpoch,
      });

      return sendJSON(res, 200, result);
    } catch (err) {
      let statusCode = 400;
      if (err.code === 'TOKEN_EXPIRED' || err.code === 'INVALID_SIGNATURE' || err.code === 'INVALID_TOKEN_FORMAT') {
        statusCode = 401; // Unauthorized due to invalid/expired cryptographic token
      } else if (err.code === 'DEVICE_FINGERPRINT_MISMATCH' || err.code === 'TOKEN_REVOKED') {
        statusCode = 403; // Forbidden due to hardware violation or replay attempt
      }

      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'REFRESH_FAILED',
        details: {
          expected: err.expected,
          actual: err.actual,
          jti: err.jti,
        },
      });
    }
  }

  /**
   * POST /api/license/validate
   */
  async function handleValidate(req, res) {
    try {
      const body = await parseJsonBody(req);
      const token = body.token || (req.headers.authorization && req.headers.authorization.replace('Bearer ', ''));
      const deviceFingerprint = body.device_fingerprint || body.deviceFingerprint;
      const currentEpoch = body.current_epoch !== undefined ? body.current_epoch : body.currentEpoch;

      if (!token) {
        return sendJSON(res, 400, {
          valid: false,
          error: 'token is required',
          code: 'MISSING_TOKEN',
        });
      }

      const result = await licenseService.validateLicense({
        token,
        deviceFingerprint,
        currentEpoch,
      });

      return sendJSON(res, result.valid ? 200 : 401, result);
    } catch (err) {
      return sendJSON(res, 500, {
        valid: false,
        error: err.message,
        code: 'VALIDATION_ERROR',
      });
    }
  }

  /**
   * POST /api/license/revoke
   */
  async function handleRevoke(req, res) {
    try {
      const body = await parseJsonBody(req);
      const jti = body.jti;

      if (!jti) {
        return sendJSON(res, 400, {
          success: false,
          error: 'jti is required',
          code: 'MISSING_JTI',
        });
      }

      const result = await licenseService.revokeToken(jti, body.exp);
      return sendJSON(res, 200, result);
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  /**
   * GET /api/license/public-key
   * Exposes public key for offline agent verification
   */
  function handlePublicKey(req, res) {
    return sendJSON(res, 200, {
      algorithm: 'RS256',
      kid: licenseService.keyPair.kid,
      publicKey: licenseService.keyPair.publicKey,
    });
  }

  /**
   * GET /api/health
   */
  function handleHealth(req, res) {
    return sendJSON(res, 200, {
      status: 'healthy',
      service: 'pipvane-cloud-control-plane',
      version: '3.0.0',
      timestamp: Math.floor(Date.now() / 1000),
      features: {
        offline_grace_period_days: 30,
        silent_refresh_window_days: 7,
        algorithm: 'RS256',
      },
    });
  }

  /**
   * POST /relay/sync or /api/relay/sync
   * Stores E2EE encrypted configuration payload against license_id with 24h TTL
   */
  async function handleRelayStore(req, res) {
    try {
      const body = await parseJsonBody(req);
      const receipt = relayService.storePayload(body);
      return sendJSON(res, 200, receipt);
    } catch (err) {
      const statusCode = err.code === 'MISSING_LICENSE_ID' ||
        err.code === 'MISSING_ENCRYPTED_PAYLOAD' ||
        err.code === 'INVALID_CIPHERTEXT' ||
        err.code === 'INVALID_IV' ||
        err.code === 'INVALID_TAG' ||
        err.code === 'INVALID_SALT' ||
        err.code === 'ZERO_KNOWLEDGE_VIOLATION'
        ? 400
        : 500;
      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'RELAY_STORE_ERROR',
      });
    }
  }

  /**
   * GET /relay/sync or /api/relay/sync
   * Retrieves active E2EE encrypted configuration payload for license_id
   */
  function handleRelayFetch(req, res, searchParams) {
    try {
      let licenseId = searchParams ? searchParams.get('license_id') || searchParams.get('licenseId') : null;
      if (!licenseId && req.headers['x-license-id']) {
        licenseId = req.headers['x-license-id'];
      }

      if (!licenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id query parameter or X-License-ID header is required',
          code: 'MISSING_LICENSE_ID',
        });
      }

      const payload = relayService.getPayload(licenseId);
      if (!payload) {
        return sendJSON(res, 404, {
          success: false,
          error: 'No relay payload found for license or payload has expired (24-hour TTL)',
          code: 'RELAY_NOT_FOUND',
        });
      }

      return sendJSON(res, 200, {
        success: true,
        payload,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
        code: 'RELAY_FETCH_ERROR',
      });
    }
  }

  /**
   * DELETE /relay/sync or /api/relay/sync
   * Manually invalidates/deletes payload for license_id
   */
  function handleRelayDelete(req, res, searchParams) {
    try {
      let licenseId = searchParams ? searchParams.get('license_id') || searchParams.get('licenseId') : null;
      if (!licenseId && req.headers['x-license-id']) {
        licenseId = req.headers['x-license-id'];
      }

      if (!licenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id is required',
          code: 'MISSING_LICENSE_ID',
        });
      }

      const deleted = relayService.deletePayload(licenseId);
      return sendJSON(res, 200, {
        success: true,
        deleted,
        license_id: licenseId,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  /**
   * POST /relay/commands
   * Enqueues an encrypted command from mobile app targeting local engine
   */
  async function handlePushCommand(req, res) {
    try {
      const body = await parseJsonBody(req);
      const envelope = body.envelope || body;
      const licenseId = body.license_id || body.licenseId || (envelope.metadata && (envelope.metadata.license_id || envelope.metadata.licenseId));
      const queueType = body.queue || 'outbox';

      const result = await commandRelayService.pushCommand({
        licenseId,
        queueType,
        envelope,
      });
      return sendJSON(res, 201, result);
    } catch (err) {
      const statusCode = err.code === 'MISSING_LICENSE_ID' ||
        err.code === 'MISSING_CIPHERTEXT' ||
        err.code === 'ZERO_KNOWLEDGE_VIOLATION' ||
        err.code === 'INVALID_ENVELOPE'
        ? 400
        : 500;
      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'COMMAND_PUSH_ERROR',
      });
    }
  }

  /**
   * GET /relay/commands/outbox or /relay/outbox
   * Retrieves pending encrypted command(s) for the local engine
   */
  async function handleFetchOutboxCommands(req, res, searchParams) {
    try {
      let licenseId = searchParams ? searchParams.get('license_id') || searchParams.get('licenseId') : null;
      if (!licenseId && req.headers['x-license-id']) {
        licenseId = req.headers['x-license-id'];
      }
      if (!licenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id query parameter or X-License-ID header is required',
          code: 'MISSING_LICENSE_ID',
        });
      }

      const waitMs = searchParams && searchParams.get('wait_ms') ? parseInt(searchParams.get('wait_ms'), 10) : 0;
      const peek = searchParams && searchParams.get('peek') === 'true';
      const queueType = (searchParams && searchParams.get('queue')) || 'outbox';

      if (peek) {
        let commands = await commandRelayService.peekCommands({ licenseId, queueType, limit: 20 });
        if (commands.length === 0 && queueType === 'outbox') {
          // Fallback to inbox if mobile pushed with inbox
          commands = await commandRelayService.peekCommands({ licenseId, queueType: 'inbox', limit: 20 });
        }
        return sendJSON(res, 200, { success: true, count: commands.length, commands });
      }

      // Pop the next command
      let envelope = await commandRelayService.popCommand({ licenseId, queueType, waitMs });
      if (!envelope && queueType === 'outbox') {
        envelope = await commandRelayService.popCommand({ licenseId, queueType: 'inbox', waitMs: 0 });
      }

      if (!envelope) {
        return sendJSON(res, 200, {
          success: true,
          count: 0,
          commands: [],
        });
      }

      return sendJSON(res, 200, {
        success: true,
        count: 1,
        commands: [envelope],
        envelope,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
        code: 'COMMAND_FETCH_ERROR',
      });
    }
  }

  /**
   * POST /relay/commands/ack
   * Acknowledges that a command has been processed
   */
  async function handleAckCommand(req, res) {
    try {
      const body = await parseJsonBody(req);
      const licenseId = body.license_id || body.licenseId;
      const messageId = body.message_id || body.messageId || body.command_id;
      const queueType = body.queue || 'outbox';

      if (!licenseId || !messageId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id and message_id are required for acknowledgment',
          code: 'MISSING_ACK_PARAMS',
        });
      }

      let acked = await commandRelayService.ackCommand({ licenseId, queueType, messageId });
      if (!acked && queueType === 'outbox') {
        acked = await commandRelayService.ackCommand({ licenseId, queueType: 'inbox', messageId });
      }

      return sendJSON(res, 200, {
        success: true,
        acknowledged: acked,
        message_id: messageId,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  /**
   * DELETE /relay/commands/outbox
   */
  async function handleDeleteOutboxCommand(req, res, searchParams) {
    try {
      const licenseId = searchParams ? searchParams.get('license_id') || searchParams.get('licenseId') : null;
      const messageId = searchParams ? searchParams.get('message_id') || searchParams.get('messageId') : null;
      if (!licenseId || !messageId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id and message_id query parameters are required',
        });
      }
      const acked = await commandRelayService.ackCommand({ licenseId, queueType: 'outbox', messageId })
        || await commandRelayService.ackCommand({ licenseId, queueType: 'inbox', messageId });
      return sendJSON(res, 200, { success: true, deleted: acked });
    } catch (err) {
      return sendJSON(res, 500, { success: false, error: err.message });
    }
  }

  /**
   * GET /relay/commands/stats
   */
  async function handleCommandStats(req, res, searchParams) {
    try {
      const licenseId = searchParams ? searchParams.get('license_id') || searchParams.get('licenseId') : null;
      if (!licenseId) {
        return sendJSON(res, 400, { success: false, error: 'license_id is required' });
      }
      const stats = await commandRelayService.getStats(licenseId);
      return sendJSON(res, 200, { success: true, stats });
    } catch (err) {
      return sendJSON(res, 500, { success: false, error: err.message });
    }
  }

  return {
    licenseService,
    relayService,
    commandRelayService,
    handleIssue,
    handleRefresh,
    handleValidate,
    handleRevoke,
    handlePublicKey,
    handleHealth,
    handleRelayStore,
    handleRelayFetch,
    handleRelayDelete,
    handlePushCommand,
    handleFetchOutboxCommands,
    handleAckCommand,
    handleDeleteOutboxCommand,
    handleCommandStats,
  };
}

module.exports = {
  createLicenseApi,
  parseJsonBody,
  sendJSON,
};
