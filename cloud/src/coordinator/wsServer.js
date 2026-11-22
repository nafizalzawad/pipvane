/**
 * Pipvane v3 - Cloud Control Plane
 * High-Concurrency WebSocket Server with mTLS & JWT Authentication
 *
 * Implements:
 * - Mutual TLS (mTLS 1.3) client certificate authentication
 * - RS256 JWT bearer authentication
 * - High-concurrency WebSocket connection pool
 * - Real-time lease management & heartbeat protocol
 * - Active/Standby state coordination & automatic failover signaling
 */

const https = require('node:https');
const http = require('node:http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('node:crypto');
const { LeaseCoordinator } = require('./leaseCoordinator');
const { verifyRS256 } = require('../crypto/jwt');
const { getKeyPair } = require('../crypto/keys');

class LeaseWebSocketServer {
  /**
   * @param {object} [options]
   * @param {LeaseCoordinator} [options.coordinator] - LeaseCoordinator instance
   * @param {'mtls'|'jwt'|'hybrid'} [options.authMode='hybrid'] - Authentication mode
   * @param {object} [options.tlsOptions] - TLS configuration ({ key, cert, ca })
   * @param {string} [options.jwtPublicKey] - RSA public key for JWT validation
   * @param {number} [options.pingIntervalMs=15000] - Dead connection sweep interval
   */
  constructor(options = {}) {
    this.coordinator = options.coordinator || new LeaseCoordinator();
    this.authMode = options.authMode || 'hybrid';
    this.tlsOptions = options.tlsOptions || null;
    this.jwtPublicKey = options.jwtPublicKey || getKeyPair().publicKey;
    this.pingIntervalMs = options.pingIntervalMs || 15000;

    // Connected clients tracking: Map<clientId, { ws, clientInfo }>
    this.clients = new Map();
    // License index: Map<licenseId, Set<WebSocket>>
    this.licenseClients = new Map();

    this.httpServer = null;
    this.wss = null;
    this.keepAliveTimer = null;

    // Listen to coordinator events for broadcast
    this._bindCoordinatorEvents();
  }

  /**
   * Bind coordinator event broadcasts to connected clients
   */
  _bindCoordinatorEvents() {
    this.coordinator.on('lease_vacant', (data) => {
      this.broadcastToLicense(data.licenseId, {
        event: 'LEASE_VACANT',
        licenseId: data.licenseId,
        previousHolder: data.expiredNodeId,
        message: 'Active lease is now vacant. Standby promotion available.',
        timestamp: Date.now(),
      });
    });

    this.coordinator.on('lease_released', (data) => {
      this.broadcastToLicense(data.licenseId, {
        event: 'LEASE_VACANT',
        licenseId: data.licenseId,
        previousHolder: data.nodeId,
        message: 'Active lease was released cleanly by holder.',
        timestamp: Date.now(),
      });
    });
  }

  /**
   * Start listening on specified port and host
   * @param {number} [port=0] 0 for ephemeral port
   * @param {string} [host='127.0.0.1']
   * @returns {Promise<{ port: number, address: string }>}
   */
  async start(port = 0, host = '127.0.0.1') {
    // 1. Create underlying HTTPS or HTTP server
    if (this.tlsOptions) {
      const serverConfig = {
        key: this.tlsOptions.key,
        cert: this.tlsOptions.cert,
        ca: this.tlsOptions.ca,
        minVersion: 'TLSv1.3',
        // In strict mTLS mode, require client certificate
        requestCert: this.authMode === 'mtls' || this.authMode === 'hybrid',
        rejectUnauthorized: this.authMode === 'mtls',
      };
      this.httpServer = https.createServer(serverConfig);
    } else {
      this.httpServer = http.createServer();
    }

    // 2. Initialize WebSocket server with connection verifier
    this.wss = new WebSocketServer({
      server: this.httpServer,
      verifyClient: (info, callback) => this._verifyClient(info, callback),
    });

    this.wss.on('connection', (ws, req) => this._handleConnection(ws, req));

    // 3. Heartbeat ping sweep for dead TCP sockets
    this.keepAliveTimer = setInterval(() => this._sweepDeadConnections(), this.pingIntervalMs);
    if (this.keepAliveTimer.unref) {
      this.keepAliveTimer.unref();
    }

    // 4. Start HTTP/HTTPS server
    return new Promise((resolve, reject) => {
      this.httpServer.on('error', reject);
      this.httpServer.listen(port, host, () => {
        const address = this.httpServer.address();
        resolve({
          port: address.port,
          address: address.address,
        });
      });
    });
  }

  /**
   * Verify client authentication during WebSocket HTTP upgrade handshake
   */
  _verifyClient(info, callback) {
    const { req } = info;
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

    // Mode 1: Check mTLS peer certificate
    let hasValidMtls = false;
    let peerCert = null;
    if (req.socket && typeof req.socket.getPeerCertificate === 'function') {
      peerCert = req.socket.getPeerCertificate();
      if (peerCert && Object.keys(peerCert).length > 0) {
        hasValidMtls = req.client && req.client.authorized !== false;
      }
    }

    // Mode 2: Check JWT Bearer token (via query param ?token=... or Authorization header)
    let hasValidJwt = false;
    let jwtPayload = null;
    let token = url.searchParams.get('token');
    if (!token && req.headers.authorization && req.headers.authorization.startsWith('Bearer ')) {
      token = req.headers.authorization.substring(7).trim();
    }

    if (token) {
      try {
        jwtPayload = verifyRS256(token, this.jwtPublicKey);
        hasValidJwt = true;
      } catch {
        hasValidJwt = false;
      }
    }

    // Evaluate authentication against authMode
    if (this.authMode === 'mtls') {
      if (!hasValidMtls) {
        return callback(false, 401, 'Unauthorized: Mutual TLS (mTLS) certificate required and must be trusted');
      }
    } else if (this.authMode === 'jwt') {
      if (!hasValidJwt) {
        return callback(false, 401, 'Unauthorized: Valid RS256 JWT required');
      }
    } else {
      // Hybrid mode: either valid mTLS OR valid JWT accepted
      if (!hasValidMtls && !hasValidJwt) {
        return callback(false, 401, 'Unauthorized: Must provide valid mTLS client certificate or valid RS256 JWT');
      }
    }

    // Attach auth metadata to req for connection handler
    req.authContext = {
      method: hasValidMtls ? 'mTLS' : 'JWT',
      peerCert,
      jwtPayload,
      nodeId: (peerCert && peerCert.subject && peerCert.subject.CN) || (jwtPayload && jwtPayload.device_fingerprint),
      licenseId: (jwtPayload && jwtPayload.license_id) || url.searchParams.get('license_id'),
    };

    return callback(true);
  }

  /**
   * Handle established WebSocket connection
   */
  _handleConnection(ws, req) {
    const clientId = crypto.randomUUID();
    const authContext = req.authContext || {};

    ws.isAlive = true;
    ws.clientId = clientId;
    ws.authContext = authContext;
    ws.licenseId = authContext.licenseId || null;
    ws.nodeId = authContext.nodeId || null;

    this.clients.set(clientId, { ws, authContext });
    if (ws.licenseId) {
      this._indexClientLicense(ws.licenseId, ws);
    }

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', async (raw) => {
      try {
        const message = JSON.parse(raw.toString());
        await this._processMessage(ws, message);
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

    ws.on('error', (err) => {
      console.error(`[WS Server] Client ${clientId} error:`, err.message);
    });

    // Send connection established ACK
    this._send(ws, {
      event: 'CONNECTED',
      clientId,
      authMethod: authContext.method,
      licenseId: ws.licenseId,
      nodeId: ws.nodeId,
      timestamp: Date.now(),
    });
  }

  /**
   * Process incoming client command
   */
  async _processMessage(ws, msg) {
    const action = msg.action;

    switch (action) {
      case 'ACQUIRE_LEASE': {
        const licenseId = msg.license_id || msg.licenseId || ws.licenseId;
        const nodeId = msg.node_id || msg.nodeId || ws.nodeId;
        const nodeType = (msg.node_type || msg.nodeType || 'PRIMARY').toUpperCase();

        if (!licenseId || !nodeId) {
          return this._send(ws, {
            event: 'ERROR',
            code: 'MISSING_PARAMS',
            message: 'license_id and node_id are required for ACQUIRE_LEASE',
          });
        }

        // Update ws metadata and index
        ws.licenseId = licenseId;
        ws.nodeId = nodeId;
        this._indexClientLicense(licenseId, ws);

        const decision = await this.coordinator.acquireLease({
          licenseId,
          nodeId,
          nodeType,
          currentEpochMs: msg.current_epoch_ms || msg.currentEpochMs,
        });

        if (decision.status === 'GRANTED' || decision.status === 'RENEWED') {
          ws.activeLeaseToken = decision.leaseToken;
          this._send(ws, {
            event: 'LEASE_GRANTED',
            ...decision,
          });

          // Broadcast to other clients for this license that active lease is taken
          this.broadcastToLicense(licenseId, {
            event: 'STANDBY_ARMED',
            licenseId,
            activeHolder: nodeId,
            activeRole: decision.role,
            epochVersion: decision.epochVersion,
            expiresAt: decision.expiresAt,
          }, [ws]);
        } else {
          // Denied
          this._send(ws, {
            event: 'LEASE_DENIED',
            ...decision,
          });
        }
        break;
      }

      case 'HEARTBEAT': {
        const licenseId = msg.license_id || msg.licenseId || ws.licenseId;
        const nodeId = msg.node_id || msg.nodeId || ws.nodeId;
        const leaseToken = msg.lease_token || msg.leaseToken || ws.activeLeaseToken;

        const res = await this.coordinator.renewHeartbeat({
          licenseId,
          nodeId,
          leaseToken,
          currentEpochMs: msg.current_epoch_ms || msg.currentEpochMs,
        });

        if (res.status === 'RENEWED') {
          this._send(ws, {
            event: 'HEARTBEAT_ACK',
            ...res,
          });
        } else {
          // Fenced or Expired
          this._send(ws, {
            event: res.status === 'EXPIRED' ? 'LEASE_EXPIRED' : 'LEASE_FENCED',
            ...res,
          });
        }
        break;
      }

      case 'RELEASE_LEASE': {
        const licenseId = msg.license_id || msg.licenseId || ws.licenseId;
        const nodeId = msg.node_id || msg.nodeId || ws.nodeId;
        const leaseToken = msg.lease_token || msg.leaseToken || ws.activeLeaseToken;

        const res = await this.coordinator.releaseLease({
          licenseId,
          nodeId,
          leaseToken,
        });

        ws.activeLeaseToken = null;
        this._send(ws, {
          event: 'LEASE_RELEASED',
          ...res,
        });
        break;
      }

      case 'GET_STATUS': {
        const licenseId = msg.license_id || msg.licenseId || ws.licenseId;
        const status = this.coordinator.getLeaseStatus(licenseId);
        this._send(ws, {
          event: 'LEASE_STATUS',
          status,
        });
        break;
      }

      case 'PING': {
        this._send(ws, {
          event: 'PONG',
          timestamp: Date.now(),
        });
        break;
      }

      default: {
        this._send(ws, {
          event: 'ERROR',
          code: 'UNKNOWN_ACTION',
          message: `Unknown action: ${action}`,
        });
      }
    }
  }

  _handleDisconnect(ws) {
    this.clients.delete(ws.clientId);

    if (ws.licenseId) {
      const set = this.licenseClients.get(ws.licenseId);
      if (set) {
        set.delete(ws);
        if (set.size === 0) {
          this.licenseClients.delete(ws.licenseId);
        }
      }
    }
  }

  _indexClientLicense(licenseId, ws) {
    let set = this.licenseClients.get(licenseId);
    if (!set) {
      set = new Set();
      this.licenseClients.set(licenseId, set);
    }
    set.add(ws);
  }

  broadcastToLicense(licenseId, payload, excludeSockets = []) {
    const sockets = this.licenseClients.get(licenseId);
    if (!sockets) return;

    const data = JSON.stringify(payload);
    for (const ws of sockets) {
      if (ws.readyState === WebSocket.OPEN && !excludeSockets.includes(ws)) {
        ws.send(data);
      }
    }
  }

  _send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  }

  _sweepDeadConnections() {
    for (const [clientId, { ws }] of this.clients.entries()) {
      if (!ws.isAlive) {
        ws.terminate();
        this._handleDisconnect(ws);
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }

  async close() {
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }

    this.coordinator.close();

    // Terminate all connected clients
    for (const [, { ws }] of this.clients.entries()) {
      ws.terminate();
    }
    this.clients.clear();
    this.licenseClients.clear();

    if (this.wss) {
      await new Promise((resolve) => this.wss.close(resolve));
      this.wss = null;
    }

    if (this.httpServer) {
      await new Promise((resolve) => this.httpServer.close(resolve));
      this.httpServer = null;
    }
  }
}

module.exports = {
  LeaseWebSocketServer,
};
