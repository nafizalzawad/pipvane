/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * HTTP Server API End-to-End Tests
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const crypto = require('node:crypto');

const { createServer } = require('../src/server');
const { LicenseService } = require('../src/services/licenseService');
const { generateRSAKeyPair } = require('../src/crypto/keys');
const { MemoryRevocationStore } = require('../src/services/revocationStore');

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

describe('Pipvane v3 - HTTP Server API Endpoints', () => {
  let server;
  let baseUrl;
  let port;
  let licenseService;
  let revocationStore;

  const validFingerprint = crypto.createHash('sha256').update('TEST_AGENT_HW_ID_007').digest('hex');
  const otherFingerprint = crypto.createHash('sha256').update('TEST_AGENT_HW_ID_OTHER').digest('hex');

  before(async () => {
    const keyPair = generateRSAKeyPair(2048);
    revocationStore = new MemoryRevocationStore({ autoPrune: false });
    licenseService = new LicenseService({ keyPair, revocationStore });

    const instance = createServer(licenseService);
    server = instance.server;

    await new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        port = server.address().port;
        baseUrl = `http://127.0.0.1:${port}`;
        resolve();
      });
    });
  });

  after(async () => {
    await new Promise((resolve) => server.close(resolve));
    revocationStore.close();
  });

  it('GET /api/health returns 200 and healthy status', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port,
      path: '/api/health',
      method: 'GET',
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.status, 'healthy');
    assert.strictEqual(res.data.features.offline_grace_period_days, 30);
    assert.strictEqual(res.data.features.silent_refresh_window_days, 7);
  });

  it('GET /api/license/public-key returns active RS256 public key', async () => {
    const res = await makeRequest({
      hostname: '127.0.0.1',
      port,
      path: '/api/license/public-key',
      method: 'GET',
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.data.algorithm, 'RS256');
    assert.ok(res.data.publicKey.includes('BEGIN PUBLIC KEY'));
    assert.ok(res.data.kid);
  });

  it('POST /api/license/issue issues fresh 30-day JWT (FR-8.1)', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'lic_http_std_100',
        device_fingerprint: validFingerprint,
        tier: 'Standard',
      }
    );

    assert.strictEqual(res.statusCode, 201);
    assert.strictEqual(res.data.success, true);
    assert.strictEqual(res.data.valid_for_seconds, 2592000);
    assert.strictEqual(res.data.payload.tier, 'Standard');
    assert.strictEqual(res.data.payload.account_limit, 4);
    assert.strictEqual(res.data.payload.exp - res.data.payload.iat, 2592000);
    assert.ok(res.data.token);
  });

  it('POST /api/license/issue returns 400 when missing license_id', async () => {
    const res = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        device_fingerprint: validFingerprint,
      }
    );

    assert.strictEqual(res.statusCode, 400);
    assert.strictEqual(res.data.code, 'MISSING_LICENSE_ID');
  });

  it('POST /api/license/refresh executes silent refresh & token rotation (FR-8.4)', async () => {
    // 1. Issue initial token
    const issueRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'lic_http_refresh_200',
        device_fingerprint: validFingerprint,
        tier: 'Pro',
      }
    );
    assert.strictEqual(issueRes.statusCode, 201);
    const initialToken = issueRes.data.token;
    const oldJti = issueRes.data.payload.jti;

    // 2. Refresh token
    const refreshRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/refresh',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${initialToken}`,
        },
      },
      {
        device_fingerprint: validFingerprint,
      }
    );

    assert.strictEqual(refreshRes.statusCode, 200);
    assert.strictEqual(refreshRes.data.success, true);
    assert.strictEqual(refreshRes.data.token_version, 2);
    assert.strictEqual(refreshRes.data.previous_jti, oldJti);
    assert.strictEqual(refreshRes.data.payload.tier, 'Pro');
    assert.strictEqual(refreshRes.data.payload.account_limit, 7);
    assert.strictEqual(refreshRes.data.valid_for_seconds, 2592000);

    // 3. Replay attack: attempting to refresh again with initialToken must return 403 Forbidden!
    const replayRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/refresh',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${initialToken}`,
        },
      },
      {
        device_fingerprint: validFingerprint,
      }
    );

    assert.strictEqual(replayRes.statusCode, 403);
    assert.strictEqual(replayRes.data.code, 'TOKEN_REVOKED');
  });

  it('POST /api/license/refresh rejects mismatched device fingerprint with 403 Forbidden', async () => {
    const issueRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'lic_http_fp_test',
        device_fingerprint: validFingerprint,
      }
    );

    const refreshRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/refresh',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        token: issueRes.data.token,
        device_fingerprint: otherFingerprint, // Different hardware
      }
    );

    assert.strictEqual(refreshRes.statusCode, 403);
    assert.strictEqual(refreshRes.data.code, 'DEVICE_FINGERPRINT_MISMATCH');
  });

  it('POST /api/license/validate returns valid true for active token', async () => {
    const issueRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        license_id: 'lic_http_validate_ok',
        device_fingerprint: validFingerprint,
      }
    );

    const valRes = await makeRequest(
      {
        hostname: '127.0.0.1',
        port,
        path: '/api/license/validate',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      },
      {
        token: issueRes.data.token,
        device_fingerprint: validFingerprint,
      }
    );

    assert.strictEqual(valRes.statusCode, 200);
    assert.strictEqual(valRes.data.valid, true);
    assert.strictEqual(valRes.data.payload.license_id, 'lic_http_validate_ok');
  });
});
