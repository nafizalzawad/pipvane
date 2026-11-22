/**
 * Pipvane v3 - Cloud Control Plane
 * WebSocket Real-Time Encrypted Command Relay
 *
 * Provides real-time bi-directional streaming of Zero-Knowledge encrypted commands
 * between the Mobile Companion App and the Pipvane Local Engine.
 */

const { WebSocket } = require('ws');
const { RelayService } = require('./relayService');

class WebSocketRelayHandler {
  /**
   * @param {object} options
   * @param {RelayService} options.relayService
   */
  constructor(options = {}) {
    this.relayService = options.relayService || new RelayService();
    // Index: Map<license_id, Set<{ ws, role: 'mobile_app'|'local_engine' }>>
    this.licenseSessions = new Map();

    // Listen to relayService events
    this.relayService.on('command_pushed', (event) => {
      this._deliverToTarget(event);
    });
  }

  /**
   * Handle newly authenticated WebSocket connection for relay
   * @param {WebSocket} ws
   * @param {object} sessionInfo - { licenseId, role: 'mobile_app'|'local_engine', clientId }
   */
  registerConnection(ws, sessionInfo) {
    const { licenseId, role = 'local_engine', clientId } = sessionInfo;
    if (!licenseId) return;

    ws.licenseId = licenseId;
    ws.role = role.toLowerCase();
    ws.clientId = clientId;

    let sessions = this.licenseSessions.get(licenseId);
    if (!sessions) {
      sessions = new Set();
      this.licenseSessions.set(licenseId, sessions);
    }
    sessions.add(ws);

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        await this._handleMessage(ws, msg);
      } catch (err) {
        this._send(ws, {
          event: 'ERROR',
          code: err.code || 'BAD_REQUEST',
          message: err.message,
        });
      }
    });

    ws.on('close', () => {
      this._handleDisconnect(ws);
    });

    // Send connection ack with queue stats
    this.relayService.getStats(licenseId).then((stats) => {
      this._send(ws, {
        event: 'RELAY_CONNECTED',
        licenseId,
        role: ws.role,
        stats,
        timestamp: Date.now(),
      });
    }).catch(() => {});
  }

  async _handleMessage(ws, msg) {
    const action = msg.action;

    switch (action) {
      case 'RELAY_PUSH':
      case 'PUSH_COMMAND': {
        const envelope = msg.envelope || msg;
        const result = await this.relayService.pushCommand({
          licenseId: ws.licenseId,
          queueType: msg.queue,
          envelope,
        });

        this._send(ws, {
          event: 'COMMAND_ACK',
          status: 'ENQUEUED',
          message_id: result.message_id,
          queue: result.queue,
        });
        break;
      }

      case 'RELAY_POP':
      case 'POP_COMMAND': {
        const queueType = msg.queue || (ws.role === 'local_engine' ? 'inbox' : 'outbox');
        const envelope = await this.relayService.popCommand({
          licenseId: ws.licenseId,
          queueType,
          waitMs: msg.wait_ms || 0,
        });

        this._send(ws, {
          event: 'COMMAND_DELIVERED',
          queue: queueType,
          message: envelope,
        });
        break;
      }

      case 'RELAY_ACK':
      case 'ACK_COMMAND': {
        const queueType = msg.queue || 'inbox';
        const acked = await this.relayService.ackCommand({
          licenseId: ws.licenseId,
          queueType,
          messageId: msg.message_id || msg.messageId,
        });

        this._send(ws, {
          event: 'MESSAGE_ACKED',
          message_id: msg.message_id || msg.messageId,
          acknowledged: acked,
        });
        break;
      }

      case 'RELAY_STATS': {
        const stats = await this.relayService.getStats(ws.licenseId);
        this._send(ws, {
          event: 'RELAY_STATS',
          stats,
        });
        break;
      }

      default:
        // Pass through or ignore
        break;
    }
  }

  /**
   * Deliver newly pushed envelope in real-time to connected counterpart
   */
  _deliverToTarget(event) {
    const { licenseId, queueType, envelope } = event;
    const sessions = this.licenseSessions.get(licenseId);
    if (!sessions || sessions.size === 0) return;

    // Target role:
    // inbox (mobile -> engine): target is local_engine
    // outbox (engine -> mobile): target is mobile_app
    const targetRole = queueType === 'inbox' ? 'local_engine' : 'mobile_app';

    const payload = JSON.stringify({
      event: 'INCOMING_COMMAND',
      queue: queueType,
      envelope,
      timestamp: Date.now(),
    });

    for (const ws of sessions) {
      if (ws.readyState === WebSocket.OPEN && (ws.role === targetRole || !ws.role)) {
        ws.send(payload);
      }
    }
  }

  _handleDisconnect(ws) {
    if (ws.licenseId) {
      const sessions = this.licenseSessions.get(ws.licenseId);
      if (sessions) {
        sessions.delete(ws);
        if (sessions.size === 0) {
          this.licenseSessions.delete(ws.licenseId);
        }
      }
    }
  }

  _send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  }
}

module.exports = {
  WebSocketRelayHandler,
};
