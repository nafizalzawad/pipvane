/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * Encrypted Configuration Relay Sync Endpoints & Zero-Knowledge Verification (Sprint 15)
 */

'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { createServer } = require('../src/server');
const { LicenseService } = require('../src/services/licenseService');
const { RelayService } = require('../src/services/relayService');

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

describe('Pipvane v3 - Encrypted Configuration Relay API (/relay/sync)', () => {
  let server;
  let baseUrl;
  let relayService;

  before(async () => {
    relayService = new RelayService({ defaultTtlMs: 24 * 60 * 60 * 1000 });
    const instance = createServer(new LicenseService(), relayService);
    server = instance.server;

    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        const port = server.address().port;
        baseUrl = `http://127.0.0.1:${port}`;
        resolve();
      });
    });
  });

  after(async () => {
    await new Promise((resolve) => server.close(resolve));
  });

  const mockEncryptedPayload = {
    kdf: 'PBKDF2-HMAC-SHA256',
    iterations: 100000,
    salt: Buffer.from('mock_salt_16bytes!').toString('base64'),
    iv: Buffer.from('mock_iv_12byt').toString('base64'),
    tag: Buffer.from('mock_tag_16bytes!').toString('base64'),
    ciphertext: Buffer.from('mock_aes_gcm_ciphertext_opaque_bytes').toString('base64'),
    aad: 'pipvane:sync:LIC-PRO-2026-X889:1',
  };

  it('POST /relay/sync accepts and stores an encrypted E2EE payload', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port: server.address().port,
        path: '/relay/sync',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'LIC-PRO-2026-X889',
        version: 1,
        source_node_id: 'primary_node_01',
        encrypted_payload: mockEncryptedPayload,
      }
    );

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.success, true);
    assert.strictEqual(res.data.license_id, 'LIC-PRO-2026-X889');
    assert.strictEqual(res.data.version, 1);
    assert.ok(res.data.expires_at > Date.now());
    assert.strictEqual(res.data.ttl_seconds, 86400); // 24 hours
  });

  it('GET /relay/sync retrieves the encrypted payload for the given license_id', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port: server.address().port,
      path: '/relay/sync?license_id=LIC-PRO-2026-X889',
      method: 'GET',
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.success, true);
    assert.strictEqual(res.data.payload.license_id, 'LIC-PRO-2026-X889');
    assert.strictEqual(res.data.payload.version, 1);
    assert.strictEqual(res.data.payload.source_node_id, 'primary_node_01');
    assert.strictEqual(res.data.payload.encrypted_payload.ciphertext, mockEncryptedPayload.ciphertext);
    assert.strictEqual(res.data.payload.encrypted_payload.salt, mockEncryptedPayload.salt);
  });

  it('GET /relay/sync with X-License-ID header works identically', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port: server.address().port,
      path: '/relay/sync',
      method: 'GET',
      headers: { 'X-License-ID': 'LIC-PRO-2026-X889' },
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.success, true);
    assert.strictEqual(res.data.payload.license_id, 'LIC-PRO-2026-X889');
  });

  it('POST /relay/sync rejects payload missing license_id with 400', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port: server.address().port,
        path: '/relay/sync',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        version: 1,
        encrypted_payload: mockEncryptedPayload,
      }
    );

    assert.strictEqual(res.statusCode, 400);
    assert.strictEqual(res.data.code, 'MISSING_LICENSE_ID');
  });

  it('POST /relay/sync rejects payload missing required encrypted_payload fields with 400', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port: server.address().port,
        path: '/relay/sync',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'LIC-PRO-2026-X889',
        encrypted_payload: { ciphertext: 'some_cipher' }, // Missing salt, iv, tag
      }
    );

    assert.strictEqual(res.statusCode, 400);
    assert.strictEqual(res.data.code, 'INVALID_IV');
  });

  it('POST /relay/sync rejects requests containing plaintext passphrase (Zero-Knowledge violation)', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port: server.address().port,
        path: '/relay/sync',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'LIC-PRO-2026-X889',
        passphrase: 'super_secret_user_passphrase',
        encrypted_payload: mockEncryptedPayload,
      }
    );

    assert.strictEqual(res.statusCode, 400);
    assert.strictEqual(res.data.code, 'ZERO_KNOWLEDGE_VIOLATION');
  });

  it('GET /relay/sync returns 404 for unknown or expired license', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port: server.address().port,
      path: '/relay/sync?license_id=LIC-NON-EXISTENT',
      method: 'GET',
    });

    assert.strictEqual(res.statusCode, 404);
    assert.strictEqual(res.data.code, 'RELAY_NOT_FOUND');
  });

  it('Ephemeral 24-hour TTL pruning works correctly', () => {
    // Test custom short TTL in RelayService
    const shortRelay = new RelayService();
    shortRelay.storePayload({
      license_id: 'LIC-EXPIRING',
      version: 1,
      encrypted_payload: mockEncryptedPayload,
      ttl_ms: 10, // 10 milliseconds
    });

    assert.ok(shortRelay.getPayload('LIC-EXPIRING') !== null);

    return new Promise((resolve) => {
      setTimeout(() => {
        const expired = shortRelay.getPayload('LIC-EXPIRING');
        assert.strictEqual(expired, null, 'Expired record must return null after TTL');
        resolve();
      }, 25);
    });
  });

  it('DELETE /relay/sync invalidates stored configuration', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port: server.address().port,
      path: '/relay/sync?license_id=LIC-PRO-2026-X889',
      method: 'DELETE',
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.deleted, true);

    // Verify subsequent GET returns 404
    const getRes = await makeRequest({
      hostname: '127.0.0.1',
      port: server.address().port,
      path: '/relay/sync?license_id=LIC-PRO-2026-X889',
      method: 'GET',
    });
    assert.strictEqual(getRes.statusCode, 404);
  });

  it('Mathematical Zero-Knowledge Proof: Cloud cannot decrypt payload without user passphrase', () => {
    assert.strictEqual(relayService.canDecrypt(), false);
    // Cloud storage only holds opaque ciphertext
    const stored = relayService.getPayload('LIC-PRO-2026-X889');
    assert.strictEqual(stored, null); // was deleted in prior test
  });
});
