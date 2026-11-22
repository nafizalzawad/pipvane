/**
 * Pipvane v3 - Cloud Control Plane
 * HTTP / REST Handlers for Encrypted Command Relay (/relay/command)
 */

const { RelayService, ZeroKnowledgeViolationError } = require('./relayService');
const { parseJsonBody, sendJSON } = require('../api/handlers');

/**
 * Create relay request dispatcher
 * @param {RelayService} [service]
 * @returns {object} Route handlers
 */
function createRelayApi(service) {
  const relayService = service || new RelayService();

  /**
   * POST /relay/command
   * Push encrypted envelope into queue (Inbox or Outbox)
   */
  async function handlePush(req, res) {
    try {
      const body = await parseJsonBody(req);

      // Support explicit license_id via header or query if not in body
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      const headerLicense = req.headers['x-license-id'] || url.searchParams.get('license_id');
      const queueType = url.searchParams.get('queue') || (body.metadata && body.metadata.queue);

      const result = await relayService.pushCommand({
        licenseId: headerLicense,
        queueType,
        envelope: body,
      });

      return sendJSON(res, 201, result);
    } catch (err) {
      if (err instanceof ZeroKnowledgeViolationError || err.code === 'ZERO_KNOWLEDGE_VIOLATION') {
        return sendJSON(res, 400, {
          success: false,
          error: err.message,
          code: 'ZERO_KNOWLEDGE_VIOLATION',
          forbiddenKey: err.forbiddenKey,
        });
      }

      const statusCode = err.code && (
        err.code.startsWith('INVALID_') ||
        err.code.startsWith('MISSING_') ||
        err.code === 'PAYLOAD_TOO_LARGE'
      ) ? 400 : 500;

      return sendJSON(res, statusCode, {
        success: false,
        error: err.message,
        code: err.code || 'RELAY_ERROR',
      });
    }
  }

  /**
   * GET /relay/command
   * Long-poll / pop an encrypted envelope from queue
   */
  async function handlePop(req, res) {
    try {
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      const licenseId = url.searchParams.get('license_id') || req.headers['x-license-id'];
      const queueType = url.searchParams.get('queue') || 'inbox';
      const waitMsStr = url.searchParams.get('wait_ms');
      const waitMs = waitMsStr ? Math.min(Math.max(0, parseInt(waitMsStr, 10)), 30000) : 0;
      const mode = url.searchParams.get('mode') || 'pop';

      if (!licenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id is required via query param or X-License-Id header',
          code: 'MISSING_LICENSE_ID',
        });
      }

      if (mode === 'peek') {
        const limitStr = url.searchParams.get('limit') || '10';
        const limit = parseInt(limitStr, 10) || 10;
        const messages = await relayService.peekCommands({
          licenseId,
          queueType,
          limit,
        });
        return sendJSON(res, 200, {
          success: true,
          count: messages.length,
          messages,
        });
      }

      const envelope = await relayService.popCommand({
        licenseId,
        queueType,
        waitMs,
      });

      if (!envelope) {
        // Return 204 No Content or 200 with empty payload
        return sendJSON(res, 200, {
          success: true,
          message: null,
          queue: queueType,
        });
      }

      return sendJSON(res, 200, {
        success: true,
        message: envelope,
        queue: queueType,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
        code: err.code || 'POP_ERROR',
      });
    }
  }

  /**
   * POST /relay/command/ack
   * Acknowledge processed message
   */
  async function handleAck(req, res) {
    try {
      const body = await parseJsonBody(req);
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      const licenseId = body.license_id || body.licenseId || url.searchParams.get('license_id');
      const messageId = body.message_id || body.messageId || url.searchParams.get('message_id');
      const queueType = body.queue || url.searchParams.get('queue') || 'inbox';

      if (!licenseId || !messageId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id and message_id are required for acknowledgment',
          code: 'MISSING_ACK_PARAMS',
        });
      }

      const acked = await relayService.ackCommand({
        licenseId,
        queueType,
        messageId,
      });

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
   * GET /relay/command/stats
   */
  async function handleStats(req, res) {
    try {
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      const licenseId = url.searchParams.get('license_id') || req.headers['x-license-id'];

      if (!licenseId) {
        return sendJSON(res, 400, {
          success: false,
          error: 'license_id is required',
          code: 'MISSING_LICENSE_ID',
        });
      }

      const stats = await relayService.getStats(licenseId);
      return sendJSON(res, 200, {
        success: true,
        stats,
      });
    } catch (err) {
      return sendJSON(res, 500, {
        success: false,
        error: err.message,
      });
    }
  }

  return {
    relayService,
    handlePush,
    handlePop,
    handleAck,
    handleStats,
  };
}

module.exports = {
  createRelayApi,
};
