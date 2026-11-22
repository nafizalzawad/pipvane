/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * WebSocket Server End-to-End Tests with mTLS & JWT Authentication
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const { WebSocket } = require('ws');
const crypto = require('node:crypto');

const { LeaseWebSocketServer } = require('../src/coordinator/wsServer');
const { LeaseCoordinator } = require('../src/coordinator/leaseCoordinator');
const { generateMtlsCertificates } = require('../src/coordinator/mtls');
const { LicenseService } = require('../src/services/licenseService');
const { getKeyPair } = require('../src/crypto/keys');

function createWsClient(url, options = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, {
      rejectUnauthorized: options.rejectUnauthorized !== false,
      key: options.key,
      cert: options.cert,
      ca: options.ca,
      headers: options.headers,
    });

    const messages = [];
    ws.on('message', (data) => {
      try {
        messages.push(JSON.parse(data.toString()));
      } catch {
        messages.push(data.toString());
      }
    });

    ws.on('open', () => {
      resolve({
        ws,
        messages,
        send(obj) {
          ws.send(JSON.stringify(obj));
        },
        async waitForMessage(predicate, timeoutMs = 5000) {
          const start = Date.now();
          while (Date.now() - start < timeoutMs) {
            const found = messages.find(predicate);
            if (found) return found;
            await new Promise((r) => setTimeout(r, 20));
          }
          throw new Error(`Timeout waiting for message matching predicate. Received: ${JSON.stringify(messages)}`);
        },
        close() {
          return new Promise((r) => {
            ws.once('close', r);
            ws.close();
          });
        },
      });
    });

    ws.on('error', (err) => {
      reject(err);
    });
  });
}

describe('LeaseWebSocketServer with mTLS & JWT Auth', () => {
  let certs;
  let serverMtls;
  let serverPortMtls;
  let coordinatorMtls;

  let serverJwt;
  let serverPortJwt;
  let coordinatorJwt;
  let licenseService;
  let jwtTokenPrimary;
  let jwtTokenStandby;

  const validFingerprintPrimary = crypto.createHash('sha256').update('PRIMARY_NODE_HARDWARE').digest('hex');
  const validFingerprintStandby = crypto.createHash('sha256').update('STANDBY_NODE_HARDWARE').digest('hex');

  before(async () => {
    // 1. Generate mTLS test certs
    certs = await generateMtlsCertificates({
      serverHost: 'localhost',
      clientCN: 'pipvane-primary-node',
    });

    // 2. Setup mTLS-mode server
    coordinatorMtls = new LeaseCoordinator({
      leaseTtlMs: 30000,
      heartbeatIntervalMs: 10000,
      autoSweep: false,
    });

    serverMtls = new LeaseWebSocketServer({
      coordinator: coordinatorMtls,
      authMode: 'mtls',
      tlsOptions: {
        key: certs.server.key,
        cert: certs.server.cert,
        ca: [certs.client.cert], // Trust client cert
      },
    });

    const mtlsAddr = await serverMtls.start(0, '127.0.0.1');
    serverPortMtls = mtlsAddr.port;

    // 3. Setup JWT-mode server
    const keyPair = getKeyPair();
    licenseService = new LicenseService({ keyPair });
    coordinatorJwt = new LeaseCoordinator({
      leaseTtlMs: 30000,
      heartbeatIntervalMs: 10000,
      autoSweep: false,
    });

    serverJwt = new LeaseWebSocketServer({
      coordinator: coordinatorJwt,
      authMode: 'jwt',
      jwtPublicKey: keyPair.publicKey,
    });

    const jwtAddr = await serverJwt.start(0, '127.0.0.1');
    serverPortJwt = jwtAddr.port;

    // Issue valid JWT tokens for Primary and Standby
    const primaryLic = await licenseService.issueLicense({
      licenseId: 'lic_jwt_failover',
      deviceFingerprint: validFingerprintPrimary,
      tier: 'Pro',
    });
    jwtTokenPrimary = primaryLic.token;

    const standbyLic = await licenseService.issueLicense({
      licenseId: 'lic_jwt_failover',
      deviceFingerprint: validFingerprintStandby,
      tier: 'Pro',
    });
    jwtTokenStandby = standbyLic.token;
  });

  after(async () => {
    if (serverMtls) await serverMtls.close();
    if (serverJwt) await serverJwt.close();
  });

  // =========================================================================
  // 1. mTLS Authentication Gate
  // =========================================================================
  describe('Mutual TLS (mTLS) Authentication Gate', () => {
    it('rejects connection if client does not present a client certificate', async () => {
      await assert.rejects(
        async () => {
          await createWsClient(`wss://127.0.0.1:${serverPortMtls}`, {
            ca: certs.server.cert,
            rejectUnauthorized: true,
            // No client key or cert provided!
          });
        },
        (err) => {
          assert.ok(
            err.code === 'ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED' ||
            err.message.includes('certificate required') ||
            err.message.includes('SSL') ||
            err.code === 'ECONNRESET' ||
            err.message.includes('socket hang up')
          );
          return true;
        }
      );
    });

    it('rejects connection if client presents an untrusted / unauthorized certificate', async () => {
      await assert.rejects(
        async () => {
          await createWsClient(`wss://127.0.0.1:${serverPortMtls}`, {
            ca: certs.server.cert,
            key: certs.untrustedClient.key,
            cert: certs.untrustedClient.cert,
            rejectUnauthorized: true,
          });
        },
        (err) => {
          assert.ok(
            err.code === 'ECONNRESET' ||
            err.message.includes('socket hang up') ||
            err.message.includes('SSL') ||
            err.message.includes('alert')
          );
          return true;
        }
      );
    });

    it('successfully connects with trusted client certificate', async () => {
      const client = await createWsClient(`wss://127.0.0.1:${serverPortMtls}`, {
        ca: certs.server.cert,
        key: certs.client.key,
        cert: certs.client.cert,
        rejectUnauthorized: true,
      });

      const connectAck = await client.waitForMessage((m) => m.event === 'CONNECTED');
      assert.strictEqual(connectAck.event, 'CONNECTED');
      assert.strictEqual(connectAck.authMethod, 'mTLS');

      await client.close();
    });
  });

  // =========================================================================
  // 2. JWT Authentication Gate
  // =========================================================================
  describe('JWT Authentication Gate', () => {
    it('rejects connection without JWT token', async () => {
      await assert.rejects(
        async () => {
          await createWsClient(`ws://127.0.0.1:${serverPortJwt}`);
        },
        (err) => {
          assert.ok(err.message.includes('401') || err.message.includes('Unexpected server response'));
          return true;
        }
      );
    });

    it('rejects connection with invalid or forged JWT token', async () => {
      await assert.rejects(
        async () => {
          await createWsClient(`ws://127.0.0.1:${serverPortJwt}?token=invalid.jwt.token`);
        },
        (err) => {
          assert.ok(err.message.includes('401') || err.message.includes('Unexpected server response'));
          return true;
        }
      );
    });

    it('successfully connects with valid RS256 JWT token', async () => {
      const client = await createWsClient(`ws://127.0.0.1:${serverPortJwt}?token=${jwtTokenPrimary}`);
      const connectAck = await client.waitForMessage((m) => m.event === 'CONNECTED');
      assert.strictEqual(connectAck.event, 'CONNECTED');
      assert.strictEqual(connectAck.authMethod, 'JWT');
      assert.strictEqual(connectAck.licenseId, 'lic_jwt_failover');

      await client.close();
    });
  });

  // =========================================================================
  // 3. Lease Coordination & Heartbeat Protocol Over WebSocket
  // =========================================================================
  describe('WebSocket Lease Lifecycle & Heartbeat Protocol', () => {
    it('handles ACQUIRE_LEASE, periodic HEARTBEAT, and prevents split-brain between Primary & Standby', async () => {
      // 1. Connect Primary client
      const primaryClient = await createWsClient(`ws://127.0.0.1:${serverPortJwt}?token=${jwtTokenPrimary}`);
      await primaryClient.waitForMessage((m) => m.event === 'CONNECTED');

      // 2. Connect Standby client
      const standbyClient = await createWsClient(`ws://127.0.0.1:${serverPortJwt}?token=${jwtTokenStandby}`);
      await standbyClient.waitForMessage((m) => m.event === 'CONNECTED');

      // 3. Primary requests ACQUIRE_LEASE
      primaryClient.send({
        action: 'ACQUIRE_LEASE',
        license_id: 'lic_jwt_failover',
        node_id: 'primary_agent_01',
        node_type: 'PRIMARY',
      });

      const primaryGranted = await primaryClient.waitForMessage((m) => m.event === 'LEASE_GRANTED');
      assert.strictEqual(primaryGranted.status, 'GRANTED');
      assert.strictEqual(primaryGranted.active, true);
      assert.strictEqual(primaryGranted.ttlMs, 30000);
      assert.ok(primaryGranted.leaseToken);
      const activeLeaseToken = primaryGranted.leaseToken;

      // Standby receives broadcast notification that Primary is active
      const standbyArmedBroadcast = await standbyClient.waitForMessage((m) => m.event === 'STANDBY_ARMED');
      assert.strictEqual(standbyArmedBroadcast.activeHolder, 'primary_agent_01');

      // 4. Standby attempts to claim ACQUIRE_LEASE while Primary is active
      standbyClient.send({
        action: 'ACQUIRE_LEASE',
        license_id: 'lic_jwt_failover',
        node_id: 'standby_agent_02',
        node_type: 'STANDBY',
      });

      const standbyDenied = await standbyClient.waitForMessage((m) => m.event === 'LEASE_DENIED');
      assert.strictEqual(standbyDenied.status, 'DENIED');
      assert.strictEqual(standbyDenied.active, false);
      assert.strictEqual(standbyDenied.role, 'STANDBY');
      assert.strictEqual(standbyDenied.currentHolder, 'primary_agent_01');

      // 5. Primary sends periodic HEARTBEAT
      primaryClient.send({
        action: 'HEARTBEAT',
        license_id: 'lic_jwt_failover',
        node_id: 'primary_agent_01',
        lease_token: activeLeaseToken,
      });

      const hbAck = await primaryClient.waitForMessage((m) => m.event === 'HEARTBEAT_ACK');
      assert.strictEqual(hbAck.status, 'RENEWED');
      assert.strictEqual(hbAck.active, true);

      // 6. Clean release by Primary
      primaryClient.send({
        action: 'RELEASE_LEASE',
        license_id: 'lic_jwt_failover',
        node_id: 'primary_agent_01',
        lease_token: activeLeaseToken,
      });

      await primaryClient.waitForMessage((m) => m.event === 'LEASE_RELEASED');

      // Standby receives broadcast that lease is vacant
      await standbyClient.waitForMessage((m) => m.event === 'LEASE_VACANT');

      // 7. Standby now acquires active lease!
      standbyClient.send({
        action: 'ACQUIRE_LEASE',
        license_id: 'lic_jwt_failover',
        node_id: 'standby_agent_02',
        node_type: 'STANDBY',
      });

      const standbyGranted = await standbyClient.waitForMessage((m) => m.event === 'LEASE_GRANTED');
      assert.strictEqual(standbyGranted.status, 'GRANTED');
      assert.strictEqual(standbyGranted.nodeId, 'standby_agent_02');
      assert.strictEqual(standbyGranted.active, true);

      await primaryClient.close();
      await standbyClient.close();
    });
  });
});
