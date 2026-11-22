/**
 * Pipvane v3 - Cloud Control Plane
 * Automated Test Suite: Sprint 26 - Founder Cohort Provisioning & Support Prioritization
 */

const { describe, it, beforeEach, before, after } = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const http = require('node:http');
const { LicenseService } = require('../src/services/licenseService');
const { TelemetryService } = require('../src/telemetry/telemetryService');
const { TelemetryScrubber } = require('../src/telemetry/telemetryScrubber');
const { MemoryTelemetryStore } = require('../src/telemetry/telemetryStore');
const { verifyRS256 } = require('../src/crypto/jwt');
const { createServer } = require('../src/server');

function makeRequest(options, postData) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
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

describe('Sprint 26 - Founder Provisioning & Support Prioritization', () => {
  let licenseService;
  let telemetryService;
  const mockFingerprint = crypto.createHash('sha256').update('founder-hardware-device-01').digest('hex');

  beforeEach(() => {
    licenseService = new LicenseService();
    telemetryService = new TelemetryService({ store: new MemoryTelemetryStore() });
  });

  describe('Founder Entitlement & Permanent Account Limits', () => {
    it('sets permanent account_limit to 4 for Founder cohort on Standard tier', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'founder-user-001',
        deviceFingerprint: mockFingerprint,
        edition: 'Founder',
        tier: 'Standard',
      });

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.payload.edition, 'Founder');
      assert.strictEqual(result.payload.account_limit, 4);
      assert.strictEqual(result.payload.priority_support, true);
      assert.strictEqual(result.payload.tier, 'Standard');

      // Verify RS256 signature integrity
      const decoded = verifyRS256(result.token, licenseService.keyPair.publicKey);
      assert.strictEqual(decoded.edition, 'Founder');
      assert.strictEqual(decoded.account_limit, 4);
      assert.strictEqual(decoded.priority_support, true);
    });

    it('sets permanent account_limit to 7 for Founder cohort when Pro tier or pack is purchased', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'founder-user-pro-002',
        deviceFingerprint: mockFingerprint,
        edition: 'Founder',
        tier: 'Pro',
      });

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.payload.edition, 'Founder');
      assert.strictEqual(result.payload.account_limit, 7);
      assert.strictEqual(result.payload.priority_support, true);
      assert.strictEqual(result.payload.tier, 'Pro');
    });

    it('sets account_limit to 7 if Founder purchased pack with accountLimit: 7', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'founder-user-pack-003',
        deviceFingerprint: mockFingerprint,
        edition: 'Founder',
        accountLimit: 7,
      });

      assert.strictEqual(result.payload.account_limit, 7);
      assert.strictEqual(result.payload.tier, 'Pro');
      assert.strictEqual(result.payload.edition, 'Founder');
      assert.strictEqual(result.payload.priority_support, true);
    });

    it('maintains Standard user defaults (4 terminals, priority_support: false) when not Founder', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'standard-user-001',
        deviceFingerprint: mockFingerprint,
        edition: 'Standard',
        tier: 'Standard',
      });

      assert.strictEqual(result.payload.edition, 'Standard');
      assert.strictEqual(result.payload.account_limit, 4);
      assert.strictEqual(result.payload.priority_support, false);
    });
  });

  describe('Founder Token Rotation & 7-Day Silent Refresh', () => {
    it('preserves Founder claim, priority_support, and account_limit during token rotation', async () => {
      const initialEpoch = 1700000000;
      const issued = await licenseService.issueLicense({
        licenseId: 'founder-rotation-user',
        deviceFingerprint: mockFingerprint,
        edition: 'Founder',
        tier: 'Standard',
        currentEpoch: initialEpoch,
      });

      assert.strictEqual(issued.payload.edition, 'Founder');
      assert.strictEqual(issued.payload.account_limit, 4);

      // Perform 7-day silent refresh 25 days later
      const refreshEpoch = initialEpoch + (25 * 86400);
      const refreshed = await licenseService.refreshLicense({
        token: issued.token,
        deviceFingerprint: mockFingerprint,
        currentEpoch: refreshEpoch,
      });

      assert.strictEqual(refreshed.success, true);
      assert.strictEqual(refreshed.payload.edition, 'Founder');
      assert.strictEqual(refreshed.payload.account_limit, 4);
      assert.strictEqual(refreshed.payload.priority_support, true);
      assert.strictEqual(refreshed.token_version, 2);
      assert.strictEqual(refreshed.previous_jti, issued.payload.jti);

      // Verify expiration is renewed for exactly 30 days
      assert.strictEqual(refreshed.payload.exp - refreshed.payload.iat, 2592000);

      // Verify old JTI cannot be reused
      await assert.rejects(
        licenseService.refreshLicense({
          token: issued.token,
          deviceFingerprint: mockFingerprint,
          currentEpoch: refreshEpoch + 10,
        }),
        (err) => err.code === 'TOKEN_REVOKED'
      );
    });
  });

  describe('Support Queue Telemetry Prioritization', () => {
    it('tags Founder telemetry with HIGH support_queue_priority', async () => {
      const founderReceipt = await telemetryService.ingest({
        license_id: 'founder-telemetry-01',
        edition: 'Founder',
        os_version: 'Windows 11 Pro 64-bit',
        latency_ms: 18.5,
        crash_code: null,
      });

      assert.strictEqual(founderReceipt.success, true);
      assert.strictEqual(founderReceipt.support_queue_priority, 'HIGH');

      const standardReceipt = await telemetryService.ingest({
        license_id: 'standard-telemetry-01',
        edition: 'Standard',
        os_version: 'Windows 10 64-bit',
        latency_ms: 22.0,
      });

      assert.strictEqual(standardReceipt.success, true);
      assert.strictEqual(standardReceipt.support_queue_priority, 'NORMAL');
    });

    it('recognizes pre-registered Founder license ID hashes in telemetry service', async () => {
      const founderLicense = 'founder-offline-key-xyz';
      telemetryService.registerFounder(founderLicense);

      // Engine sends anonymized telemetry without explicit edition claim
      const receipt = await telemetryService.ingest({
        license_id: founderLicense,
        os_version: 'Windows Server 2022',
        latency_ms: 12.4,
      });

      assert.strictEqual(receipt.support_queue_priority, 'HIGH');
    });

    it('strictly sorts Founder support cases to the top of the queue', async () => {
      // 1. Ingest standard samples first
      await telemetryService.ingest({
        license_id: 'std-user-1',
        edition: 'Standard',
        os_version: 'Windows 10',
        latency_ms: 30,
        crash_code: 'CRASH_MT5_DISCONNECT',
      });

      await telemetryService.ingest({
        license_id: 'std-user-2',
        edition: 'Standard',
        os_version: 'Windows 11',
        latency_ms: 25,
      });

      // 2. Ingest founder sample
      await telemetryService.ingest({
        license_id: 'founder-vip-1',
        edition: 'Founder',
        os_version: 'Windows Server 2019',
        latency_ms: 14,
        crash_code: 'CRASH_ORDER_SEND_TIMEOUT',
      });

      // 3. Ingest another standard sample later
      await telemetryService.ingest({
        license_id: 'std-user-3',
        edition: 'Standard',
        os_version: 'Windows 10',
        latency_ms: 45,
      });

      const queue = await telemetryService.getSupportQueue(10);
      assert.strictEqual(queue.length, 4);

      // Founder record MUST be the first item in the support queue
      assert.strictEqual(queue[0].support_queue_priority, 'HIGH');
      assert.strictEqual(queue[0].edition, 'Founder');
      assert.strictEqual(queue[0].crash_code, 'CRASH_ORDER_SEND_TIMEOUT');

      // Remaining items are standard priority
      assert.strictEqual(queue[1].support_queue_priority, 'NORMAL');
      assert.strictEqual(queue[2].support_queue_priority, 'NORMAL');
      assert.strictEqual(queue[3].support_queue_priority, 'NORMAL');
    });

    it('tracks founder sample count in aggregated metrics', async () => {
      await telemetryService.ingest({
        license_id: 'std-user-1',
        edition: 'Standard',
        os_version: 'Windows 10',
        latency_ms: 30,
      });

      await telemetryService.ingest({
        license_id: 'std-user-2',
        edition: 'Standard',
        os_version: 'Windows 11',
        latency_ms: 25,
      });

      await telemetryService.ingest({
        license_id: 'founder-vip-1',
        edition: 'Founder',
        os_version: 'Windows Server 2019',
        latency_ms: 14,
      });

      const metrics = await telemetryService.getMetrics();
      assert.strictEqual(metrics.sample_count, 3);
      assert.strictEqual(metrics.founder_sample_count, 1);
    });
  });

  describe('Auth0 Post-Login Action Claim Logic Simulation', () => {
    it('correctly maps Founder Edition role and app_metadata to token claims', async () => {
      // Simulation of the JS script deployed in Auth0 Action (cloud/terraform/auth0.tf)
      const simulateAuth0Action = (event) => {
        const customClaims = {};
        const namespace = 'https://pipvane.com/claims/';
        const api = {
          accessToken: {
            setCustomClaim: (key, val) => { customClaims[key] = val; },
          },
          idToken: {
            setCustomClaim: (key, val) => { customClaims[key] = val; },
          },
        };

        const roles = event.authorization && event.authorization.roles ? event.authorization.roles : [];
        const appMetadata = event.user.app_metadata || {};

        const isFounder = roles.includes('Pipvane Founder Edition') ||
          roles.includes('Founder') ||
          appMetadata.edition === 'Founder' ||
          appMetadata.is_founder === true;

        const edition = isFounder ? 'Founder' : (appMetadata.edition || 'Standard');
        const isProPack = roles.includes('Pipvane Pro License') || appMetadata.tier === 'Pro' || appMetadata.pack === 'pro_7';

        let tier = isProPack ? 'Pro' : 'Standard';
        let accountLimit = isProPack ? 7 : 4;
        const licenseId = appMetadata.license_id ? appMetadata.license_id : event.user.user_id;

        api.accessToken.setCustomClaim(namespace + 'edition', edition);
        api.accessToken.setCustomClaim(namespace + 'tier', tier);
        api.accessToken.setCustomClaim(namespace + 'account_limit', accountLimit);
        api.accessToken.setCustomClaim(namespace + 'license_id', licenseId);
        api.accessToken.setCustomClaim(namespace + 'priority_support', isFounder);

        return customClaims;
      };

      // Test 1: Founder with Standard Edition
      const founderClaims = simulateAuth0Action({
        user: { user_id: 'auth0|12345', app_metadata: { license_id: 'lic-founder-99' } },
        authorization: { roles: ['Pipvane Founder Edition'] },
      });

      assert.strictEqual(founderClaims['https://pipvane.com/claims/edition'], 'Founder');
      assert.strictEqual(founderClaims['https://pipvane.com/claims/account_limit'], 4);
      assert.strictEqual(founderClaims['https://pipvane.com/claims/priority_support'], true);
      assert.strictEqual(founderClaims['https://pipvane.com/claims/tier'], 'Standard');

      // Test 2: Founder with Pro Pack (7 terminals)
      const founderProClaims = simulateAuth0Action({
        user: { user_id: 'auth0|67890', app_metadata: { edition: 'Founder', pack: 'pro_7' } },
        authorization: { roles: ['Pipvane Founder Edition', 'Pipvane Pro License'] },
      });

      assert.strictEqual(founderProClaims['https://pipvane.com/claims/edition'], 'Founder');
      assert.strictEqual(founderProClaims['https://pipvane.com/claims/account_limit'], 7);
      assert.strictEqual(founderProClaims['https://pipvane.com/claims/priority_support'], true);
      assert.strictEqual(founderProClaims['https://pipvane.com/claims/tier'], 'Pro');

      // Test 3: Standard Non-Founder user
      const standardClaims = simulateAuth0Action({
        user: { user_id: 'auth0|11111', app_metadata: {} },
        authorization: { roles: ['Pipvane Standard License'] },
      });

      assert.strictEqual(standardClaims['https://pipvane.com/claims/edition'], 'Standard');
      assert.strictEqual(standardClaims['https://pipvane.com/claims/account_limit'], 4);
      assert.strictEqual(standardClaims['https://pipvane.com/claims/priority_support'], false);
    });
  });

  describe('HTTP REST API Founder Endpoints', () => {
    let server;
    let port;
    let baseUrl;
    let httpLicenseService;
    let httpTelemetryService;

    before(async () => {
      httpLicenseService = new LicenseService();
      httpTelemetryService = new TelemetryService({ store: new MemoryTelemetryStore() });
      const instance = createServer(httpLicenseService, undefined, undefined, httpTelemetryService);
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
    });

    it('POST /api/license/issue issues token with Edition: Founder and account_limit 4', async () => {
      const res = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/api/license/issue',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      }, {
        license_id: 'http-founder-user-1',
        device_fingerprint: mockFingerprint,
        edition: 'Founder',
        tier: 'Standard',
      });

      assert.strictEqual(res.statusCode, 201);
      assert.strictEqual(res.data.success, true);
      assert.strictEqual(res.data.payload.edition, 'Founder');
      assert.strictEqual(res.data.payload.account_limit, 4);
      assert.strictEqual(res.data.payload.priority_support, true);
    });

    it('POST /telemetry and GET /telemetry/support-queue prioritize Founder records', async () => {
      // Ingest standard telemetry
      await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/telemetry',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      }, {
        license_id: 'http-standard-user',
        os_version: 'Windows 11',
        latency_ms: 32.5,
      });

      // Ingest founder telemetry
      const founderRes = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/telemetry',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      }, {
        license_id: 'http-founder-user-1',
        edition: 'Founder',
        os_version: 'Windows Server 2022',
        latency_ms: 12.1,
        crash_code: 'CRASH_NETWORK_TIMEOUT',
      });

      assert.strictEqual(founderRes.statusCode, 201);
      assert.strictEqual(founderRes.data.support_queue_priority, 'HIGH');

      // Query GET /telemetry/support-queue
      const queueRes = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/telemetry/support-queue',
        method: 'GET',
      });

      assert.strictEqual(queueRes.statusCode, 200);
      assert.strictEqual(queueRes.data.success, true);
      assert.strictEqual(queueRes.data.count, 2);
      // Founder item is first in queue
      assert.strictEqual(queueRes.data.support_queue_queue || queueRes.data.support_queue[0].support_queue_priority, 'HIGH');
      assert.strictEqual(queueRes.data.support_queue[0].edition, 'Founder');
      assert.strictEqual(queueRes.data.support_queue[0].crash_code, 'CRASH_NETWORK_TIMEOUT');
    });
  });
});
