/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * Sprint 17: Encrypted Command Relay & Zero-Knowledge Enforcement
 *
 * Verifies:
 * 1. Zero-Knowledge Enforcement:
 *    - Strict separation of routing metadata from opaque ciphertext
 *    - Strict rejection of unencrypted trading/credential fields (ZERO_KNOWLEDGE_VIOLATION)
 * 2. Directional Inbox/Outbox Queues:
 *    - Mobile App pushes encrypted payload to Inbox queue
 *    - Local Engine pops payload in FIFO order
 *    - Local Engine pushes response to Outbox queue; Mobile App pops it
 * 3. Asynchronous Long-Polling:
 *    - Pop waiter suspends and resolves immediately upon command arrival
 * 4. Multi-Tenant Queue Isolation:
 *    - License A cannot access License B's messages
 * 5. REST API Endpoints (/relay/command):
 *    - POST /relay/command (push)
 *    - GET /relay/command (pop & peek)
 *    - POST /relay/command/ack (acknowledgment)
 *    - GET /relay/command/stats (queue lengths)
 * 6. WebSocket Real-Time Command Streaming:
 *    - Direct push-to-delivery between Mobile App and Local Engine
 */

const { describe, it, before, after, beforeEach } = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const crypto = require('node:crypto');
const { WebSocket } = require('ws');

const {
  RelayService,
  ZeroKnowledgeViolationError,
  MAX_CIPHERTEXT_BYTES,
} = require('../src/relay/relayService');
const { MemoryRelayQueue } = require('../src/relay/relayQueue');
const { createServer } = require('../src/server');
const { WebSocketRelayHandler } = require('../src/relay/wsRelay');

function makeRequest(options, postData) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => {
        body += chunk;
      });
      res.on('end', () => {
        let json;
        try {
          json = JSON.parse(body);
        } catch {
          json = body;
        }
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          data: json,
        });
      });
    });

    req.on('error', reject);

    if (postData) {
      const dataStr = typeof postData === 'string' ? postData : JSON.stringify(postData);
      req.write(dataStr);
    }
    req.end();
  });
}

describe('Sprint 17 - Encrypted Command Relay', () => {
  let relayService;
  let server;
  let port;

  const testLicenseA = 'lic_relay_test_alpha';
  const testLicenseB = 'lic_relay_test_beta';

  // Sample valid encrypted payload (XChaCha20-Poly1305 ciphertext)
  const sampleCiphertext = Buffer.from(crypto.randomBytes(64)).toString('base64');
  const sampleIv = Buffer.from(crypto.randomBytes(24)).toString('base64');
  const sampleTag = Buffer.from(crypto.randomBytes(16)).toString('base64');

  beforeEach(() => {
    relayService = new RelayService({
      queue: new MemoryRelayQueue(),
    });
  });

  before(async () => {
    const testService = new RelayService({ queue: new MemoryRelayQueue() });
    const instance = createServer(undefined, undefined, testService);
    server = instance.server;

    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        port = server.address().port;
        resolve();
      });
    });
  });

  after(async () => {
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
  });

  // =========================================================================
  // 1. Zero-Knowledge Enforcement
  // =========================================================================
  describe('Zero-Knowledge Enforcement & Separation of Concerns', () => {
    it('accepts valid envelope separating metadata from opaque ciphertext', () => {
      const validEnvelope = {
        metadata: {
          license_id: testLicenseA,
          direction: 'mobile_to_engine',
          sender_type: 'mobile_app',
          algorithm: 'XChaCha20-Poly1305',
          iv: sampleIv,
          tag: sampleTag,
        },
        ciphertext: sampleCiphertext,
      };

      const validated = relayService.validateEnvelope(validEnvelope);
      assert.strictEqual(validated.metadata.license_id, testLicenseA);
      assert.strictEqual(validated.metadata.direction, 'mobile_to_engine');
      assert.strictEqual(validated.ciphertext, sampleCiphertext);
      assert.ok(validated.metadata.message_id);
      assert.ok(validated.metadata.timestamp);
    });

    it('strictly rejects envelope containing plaintext trade parameters (symbol, price, lot)', () => {
      const leakyEnvelope = {
        metadata: {
          license_id: testLicenseA,
          direction: 'mobile_to_engine',
          symbol: 'EURUSD', // FORBIDDEN plaintext leak!
        },
        ciphertext: sampleCiphertext,
      };

      assert.throws(
        () => relayService.validateEnvelope(leakyEnvelope),
        (err) => {
          assert.strictEqual(err.code, 'ZERO_KNOWLEDGE_VIOLATION');
          assert.strictEqual(err.forbiddenKey, 'symbol');
          return true;
        }
      );
    });

    it('strictly rejects envelope containing credentials (password, login, broker)', () => {
      const credentialLeak = {
        metadata: {
          license_id: testLicenseA,
        },
        password: 'investor_secret_123', // FORBIDDEN plaintext leak!
        ciphertext: sampleCiphertext,
      };

      assert.throws(
        () => relayService.validateEnvelope(credentialLeak),
        (err) => {
          assert.strictEqual(err.code, 'ZERO_KNOWLEDGE_VIOLATION');
          assert.strictEqual(err.forbiddenKey, 'password');
          return true;
        }
      );
    });

    it('rejects envelope without ciphertext or with empty ciphertext', () => {
      assert.throws(
        () =>
          relayService.validateEnvelope({
            metadata: { license_id: testLicenseA },
            ciphertext: '',
          }),
        (err) => {
          assert.strictEqual(err.code, 'MISSING_CIPHERTEXT');
          return true;
        }
      );
    });

    it('rejects payload exceeding maximum ciphertext size limit (64KB)', () => {
      const hugeCiphertext = 'A'.repeat(MAX_CIPHERTEXT_BYTES + 10);
      assert.throws(
        () =>
          relayService.validateEnvelope({
            metadata: { license_id: testLicenseA },
            ciphertext: hugeCiphertext,
          }),
        (err) => {
          assert.strictEqual(err.code, 'PAYLOAD_TOO_LARGE');
          return true;
        }
      );
    });
  });

  // =========================================================================
  // 2. Inbox / Outbox Directional Queues & FIFO Ordering
  // =========================================================================
  describe('Inbox/Outbox Queue FIFO Operations', () => {
    it('pushes from mobile to Inbox, pops by local engine in exact FIFO order', async () => {
      // Mobile app pushes command 1
      const p1 = await relayService.pushCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        envelope: {
          metadata: { license_id: testLicenseA, direction: 'mobile_to_engine', message_id: 'cmd_001' },
          ciphertext: 'ENCRYPTED_CMD_1',
        },
      });
      assert.strictEqual(p1.success, true);
      assert.strictEqual(p1.queue, 'inbox');

      // Mobile app pushes command 2
      const p2 = await relayService.pushCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        envelope: {
          metadata: { license_id: testLicenseA, direction: 'mobile_to_engine', message_id: 'cmd_002' },
          ciphertext: 'ENCRYPTED_CMD_2',
        },
      });
      assert.strictEqual(p2.success, true);

      // Local engine pops message 1
      const popped1 = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.ok(popped1);
      assert.strictEqual(popped1.metadata.message_id, 'cmd_001');
      assert.strictEqual(popped1.ciphertext, 'ENCRYPTED_CMD_1');

      // Local engine pops message 2
      const popped2 = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.ok(popped2);
      assert.strictEqual(popped2.metadata.message_id, 'cmd_002');
      assert.strictEqual(popped2.ciphertext, 'ENCRYPTED_CMD_2');

      // Third pop is empty
      const popped3 = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.strictEqual(popped3, null);
    });

    it('pushes from engine to Outbox, pops by mobile app', async () => {
      // Local engine pushes execution status event
      await relayService.pushCommand({
        licenseId: testLicenseA,
        queueType: 'outbox',
        envelope: {
          metadata: { license_id: testLicenseA, direction: 'engine_to_mobile', message_id: 'status_001' },
          ciphertext: 'ENCRYPTED_STATUS_ACK',
        },
      });

      // Mobile app pops from outbox
      const res = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'outbox',
      });
      assert.ok(res);
      assert.strictEqual(res.metadata.message_id, 'status_001');
      assert.strictEqual(res.ciphertext, 'ENCRYPTED_STATUS_ACK');
    });

    it('peeks at pending messages without consuming them', async () => {
      await relayService.pushCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        envelope: {
          metadata: { license_id: testLicenseA, message_id: 'peek_001' },
          ciphertext: 'ENCRYPTED_PAYLOAD_PEEK',
        },
      });

      // Peek
      const peeked = await relayService.peekCommands({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.strictEqual(peeked.length, 1);
      assert.strictEqual(peeked[0].metadata.message_id, 'peek_001');

      // Pop still retrieves the item
      const popped = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.strictEqual(popped.metadata.message_id, 'peek_001');
    });

    it('acknowledges and removes a specific message', async () => {
      await relayService.pushCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        envelope: {
          metadata: { license_id: testLicenseA, message_id: 'ack_target_1' },
          ciphertext: 'CIPHER_ACK_1',
        },
      });

      const acked = await relayService.ackCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        messageId: 'ack_target_1',
      });
      assert.strictEqual(acked, true);

      // Subsequent pop returns null
      const popped = await relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
      });
      assert.strictEqual(popped, null);
    });
  });

  // =========================================================================
  // 3. Multi-Tenant Queue Isolation
  // =========================================================================
  describe('Multi-Tenant Queue Isolation', () => {
    it('isolates messages between distinct license IDs', async () => {
      await relayService.pushCommand({
        licenseId: testLicenseA,
        envelope: {
          metadata: { license_id: testLicenseA, message_id: 'msg_alpha' },
          ciphertext: 'CIPHER_ALPHA',
        },
      });

      await relayService.pushCommand({
        licenseId: testLicenseB,
        envelope: {
          metadata: { license_id: testLicenseB, message_id: 'msg_beta' },
          ciphertext: 'CIPHER_BETA',
        },
      });

      // License B cannot pop License A's message
      const poppedB = await relayService.popCommand({ licenseId: testLicenseB });
      assert.strictEqual(poppedB.metadata.message_id, 'msg_beta');

      const poppedA = await relayService.popCommand({ licenseId: testLicenseA });
      assert.strictEqual(poppedA.metadata.message_id, 'msg_alpha');
    });
  });

  // =========================================================================
  // 4. Asynchronous Long-Polling Delivery
  // =========================================================================
  describe('Asynchronous Long-Polling', () => {
    it('resolves long-polling waiter immediately when a message is pushed', async () => {
      // Start pop with 3000ms wait
      const popPromise = relayService.popCommand({
        licenseId: testLicenseA,
        queueType: 'inbox',
        waitMs: 3000,
      });

      // 50ms later, mobile app pushes a message
      setTimeout(async () => {
        await relayService.pushCommand({
          licenseId: testLicenseA,
          queueType: 'inbox',
          envelope: {
            metadata: { license_id: testLicenseA, message_id: 'async_delivered_msg' },
            ciphertext: 'CIPHER_ASYNC',
          },
        });
      }, 50);

      const result = await popPromise;
      assert.ok(result);
      assert.strictEqual(result.metadata.message_id, 'async_delivered_msg');
      assert.strictEqual(result.ciphertext, 'CIPHER_ASYNC');
    });

    it('returns null when long-polling wait expires without incoming message', async () => {
      const result = await relayService.popCommand({
        licenseId: 'lic_timeout_test',
        queueType: 'inbox',
        waitMs: 50,
      });
      assert.strictEqual(result, null);
    });
  });

  // =========================================================================
  // 5. REST API Endpoints (/relay/command)
  // =========================================================================
  describe('REST API Endpoints (/relay/command)', () => {
    it('POST /relay/command enqueues valid zero-knowledge encrypted envelope', async () => {
      const res = await makeRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/relay/command',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
        {
          metadata: {
            license_id: 'lic_http_relay_100',
            direction: 'mobile_to_engine',
            message_id: 'http_cmd_001',
            algorithm: 'XChaCha20-Poly1305',
          },
          ciphertext: 'ENCRYPTED_HTTP_COMMAND_PAYLOAD',
        }
      );

      assert.strictEqual(res.statusCode, 201);
      assert.strictEqual(res.data.success, true);
      assert.strictEqual(res.data.message_id, 'http_cmd_001');
      assert.strictEqual(res.data.queue, 'inbox');
    });

    it('POST /relay/command returns 400 ZERO_KNOWLEDGE_VIOLATION when plain-text trade data is leaked', async () => {
      const res = await makeRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/relay/command',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
        {
          metadata: {
            license_id: 'lic_http_relay_100',
            symbol: 'XAUUSD', // FORBIDDEN!
          },
          ciphertext: 'ENCRYPTED_BLOB',
        }
      );

      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(res.data.code, 'ZERO_KNOWLEDGE_VIOLATION');
      assert.strictEqual(res.data.forbiddenKey, 'symbol');
    });

    it('GET /relay/command pops enqueued message', async () => {
      const res = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/relay/command?license_id=lic_http_relay_100&queue=inbox',
        method: 'GET',
      });

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.data.success, true);
      assert.ok(res.data.message);
      assert.strictEqual(res.data.message.metadata.message_id, 'http_cmd_001');
      assert.strictEqual(res.data.message.ciphertext, 'ENCRYPTED_HTTP_COMMAND_PAYLOAD');
    });

    it('GET /relay/command returns null message when queue is empty', async () => {
      const res = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/relay/command?license_id=lic_http_relay_100&queue=inbox',
        method: 'GET',
      });

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.data.success, true);
      assert.strictEqual(res.data.message, null);
    });

    it('POST /relay/command/ack acknowledges message', async () => {
      const res = await makeRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/relay/command/ack',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
        {
          license_id: 'lic_http_relay_100',
          message_id: 'http_cmd_001',
          queue: 'inbox',
        }
      );

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.data.success, true);
    });

    it('GET /relay/command/stats returns queue depths', async () => {
      const res = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/relay/command/stats?license_id=lic_http_relay_100',
        method: 'GET',
      });

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.data.success, true);
      assert.strictEqual(typeof res.data.stats.inboxLength, 'number');
      assert.strictEqual(typeof res.data.stats.outboxLength, 'number');
    });
  });

  // =========================================================================
  // 6. WebSocket Real-Time Command Streaming
  // =========================================================================
  describe('WebSocket Real-Time Command Streaming', () => {
    let wsServer;
    let wsPort;
    let wsRelayHandler;

    before(async () => {
      wsRelayHandler = new WebSocketRelayHandler({ relayService });
      const serverHttp = http.createServer();
      const wss = new WebSocket.Server({ server: serverHttp });

      wss.on('connection', (ws, req) => {
        const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
        const licenseId = url.searchParams.get('license_id');
        const role = url.searchParams.get('role');
        wsRelayHandler.registerConnection(ws, { licenseId, role });
      });

      await new Promise((resolve) => {
        serverHttp.listen(0, '127.0.0.1', () => {
          wsPort = serverHttp.address().port;
          wsServer = serverHttp;
          resolve();
        });
      });
    });

    after(async () => {
      if (wsServer) {
        await new Promise((resolve) => wsServer.close(resolve));
      }
    });

    it('streams pushed command directly to connected local engine in real-time', async () => {
      const licenseId = 'lic_ws_stream_1';

      // 1. Connect Local Engine client
      const engineWs = new WebSocket(`ws://127.0.0.1:${wsPort}?license_id=${licenseId}&role=local_engine`);
      const engineMessages = [];
      engineWs.on('message', (d) => engineMessages.push(JSON.parse(d.toString())));
      await new Promise((r) => engineWs.on('open', r));

      // Wait for connect ack
      await new Promise((r) => setTimeout(r, 30));

      // 2. Connect Mobile App client
      const mobileWs = new WebSocket(`ws://127.0.0.1:${wsPort}?license_id=${licenseId}&role=mobile_app`);
      await new Promise((r) => mobileWs.on('open', r));

      // 3. Mobile App pushes command via WebSocket
      mobileWs.send(
        JSON.stringify({
          action: 'RELAY_PUSH',
          queue: 'inbox',
          envelope: {
            metadata: {
              license_id: licenseId,
              direction: 'mobile_to_engine',
              message_id: 'realtime_cmd_999',
            },
            ciphertext: 'ENCRYPTED_REALTIME_STOP_COMMAND',
          },
        })
      );

      // 4. Assert Local Engine received the command in real-time
      let received = null;
      const start = Date.now();
      while (Date.now() - start < 3000) {
        received = engineMessages.find((m) => m.event === 'INCOMING_COMMAND');
        if (received) break;
        await new Promise((r) => setTimeout(r, 20));
      }

      assert.ok(received, 'Engine must receive INCOMING_COMMAND event');
      assert.strictEqual(received.queue, 'inbox');
      assert.strictEqual(received.envelope.metadata.message_id, 'realtime_cmd_999');
      assert.strictEqual(received.envelope.ciphertext, 'ENCRYPTED_REALTIME_STOP_COMMAND');

      engineWs.close();
      mobileWs.close();
    });
  });
});
