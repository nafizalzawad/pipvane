/**
 * Pipvane v3 - Cloud Control Plane
 * Cloudflare Workers Edge Handler (Fetch API Standard)
 */

const { LicenseService } = require('./services/licenseService');
const { RelayService } = require('./relay/relayService');
const { TelemetryService } = require('./telemetry/telemetryService');

let licenseServiceInstance = null;
let relayServiceInstance = null;
let telemetryServiceInstance = null;

function getService(env) {
  if (!licenseServiceInstance) {
    licenseServiceInstance = new LicenseService();
  }
  return licenseServiceInstance;
}

function getRelayService(env) {
  if (!relayServiceInstance) {
    relayServiceInstance = new RelayService();
  }
  return relayServiceInstance;
}

function getTelemetryService(env) {
  if (!telemetryServiceInstance) {
    telemetryServiceInstance = new TelemetryService();
  }
  return telemetryServiceInstance;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Device-Fingerprint',
    },
  });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Device-Fingerprint',
        },
      });
    }

    const url = new URL(request.url);
    const service = getService(env);

    try {
      if (request.method === 'POST' && url.pathname === '/api/license/issue') {
        const body = await request.json();
        const result = await service.issueLicense({
          licenseId: body.license_id || body.licenseId,
          deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
          tier: body.tier || 'Standard',
          accountLimit: body.account_limit !== undefined ? body.account_limit : body.accountLimit,
          edition: body.edition,
          customClaims: body.custom_claims || body.customClaims || {},
        });
        return jsonResponse(result, 201);
      }

      if (request.method === 'POST' && url.pathname === '/api/license/refresh') {
        const body = await request.json();
        let token = body.token;
        const authHeader = request.headers.get('Authorization');
        if (!token && authHeader && authHeader.startsWith('Bearer ')) {
          token = authHeader.substring(7).trim();
        }

        const result = await service.refreshLicense({
          token,
          deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
        });
        return jsonResponse(result, 200);
      }

      if (request.method === 'POST' && url.pathname === '/api/license/validate') {
        const body = await request.json();
        const result = await service.validateLicense({
          token: body.token,
          deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
        });
        return jsonResponse(result, result.valid ? 200 : 401);
      }

      if (request.method === 'GET' && url.pathname === '/api/license/public-key') {
        return jsonResponse({
          algorithm: 'RS256',
          kid: service.keyPair.kid,
          publicKey: service.keyPair.publicKey,
        });
      }

      if (request.method === 'GET' && (url.pathname === '/api/health' || url.pathname === '/health')) {
        return jsonResponse({
          status: 'healthy',
          runtime: 'cloudflare-workers',
          version: '3.0.0',
          timestamp: Math.floor(Date.now() / 1000),
        });
      }

      // Encrypted Command Relay Endpoints (Sprint 17)
      const relayService = getRelayService(env);

      if (request.method === 'POST' && (url.pathname === '/relay/command' || url.pathname === '/api/relay/command')) {
        const body = await request.json();
        const headerLicense = request.headers.get('X-License-Id') || url.searchParams.get('license_id');
        const queueType = url.searchParams.get('queue');
        const result = await relayService.pushCommand({
          licenseId: headerLicense,
          queueType,
          envelope: body,
        });
        return jsonResponse(result, 201);
      }

      if (request.method === 'GET' && (url.pathname === '/relay/command' || url.pathname === '/api/relay/command')) {
        const licenseId = url.searchParams.get('license_id') || request.headers.get('X-License-Id');
        const queueType = url.searchParams.get('queue') || 'inbox';
        const waitMs = url.searchParams.get('wait_ms') ? parseInt(url.searchParams.get('wait_ms'), 10) : 0;
        const mode = url.searchParams.get('mode') || 'pop';

        if (!licenseId) {
          return jsonResponse({ error: 'license_id is required' }, 400);
        }

        if (mode === 'peek') {
          const messages = await relayService.peekCommands({
            licenseId,
            queueType,
            limit: url.searchParams.get('limit') ? parseInt(url.searchParams.get('limit'), 10) : 10,
          });
          return jsonResponse({ success: true, count: messages.length, messages });
        }

        const envelope = await relayService.popCommand({
          licenseId,
          queueType,
          waitMs,
        });
        return jsonResponse({ success: true, message: envelope, queue: queueType });
      }

      if (request.method === 'POST' && (url.pathname === '/relay/command/ack' || url.pathname === '/api/relay/command/ack')) {
        const body = await request.json();
        const licenseId = body.license_id || body.licenseId || url.searchParams.get('license_id');
        const messageId = body.message_id || body.messageId || url.searchParams.get('message_id');
        const queueType = body.queue || url.searchParams.get('queue') || 'inbox';

        const acked = await relayService.ackCommand({
          licenseId,
          queueType,
          messageId,
        });
        return jsonResponse({ success: true, acknowledged: acked, message_id: messageId });
      }

      if (request.method === 'GET' && (url.pathname === '/relay/command/stats' || url.pathname === '/api/relay/command/stats')) {
        const licenseId = url.searchParams.get('license_id') || request.headers.get('X-License-Id');
        const stats = await relayService.getStats(licenseId);
        return jsonResponse({ success: true, stats });
      }

      // Anonymous Telemetry Ingestion (Sprint 21)
      const telemetryService = getTelemetryService(env);

      if (request.method === 'POST' && (url.pathname === '/telemetry' || url.pathname === '/api/telemetry')) {
        const body = await request.json();
        const result = await telemetryService.ingest(body);
        return jsonResponse(result, 201);
      }

      if (request.method === 'GET' && (
        url.pathname === '/telemetry' ||
        url.pathname === '/api/telemetry' ||
        url.pathname === '/telemetry/metrics' ||
        url.pathname === '/api/telemetry/metrics'
      )) {
        const metrics = await telemetryService.getMetrics();
        return jsonResponse({ success: true, metrics });
      }

      if (request.method === 'GET' && (
        url.pathname === '/telemetry/support-queue' ||
        url.pathname === '/api/telemetry/support-queue'
      )) {
        const limit = url.searchParams.get('limit') ? parseInt(url.searchParams.get('limit'), 10) : 100;
        const queue = await telemetryService.getSupportQueue(limit);
        return jsonResponse({ success: true, count: queue.length, support_queue: queue });
      }

      return jsonResponse({ error: 'Endpoint not found', path: url.pathname }, 404);
    } catch (err) {
      let status = 400;
      if (err.code === 'TOKEN_EXPIRED' || err.code === 'INVALID_SIGNATURE' || err.code === 'INVALID_TOKEN_FORMAT') {
        status = 401;
      } else if (err.code === 'DEVICE_FINGERPRINT_MISMATCH' || err.code === 'TOKEN_REVOKED') {
        status = 403;
      }

      return jsonResponse({
        success: false,
        error: err.message,
        code: err.code || 'REQUEST_FAILED',
      }, status);
    }
  },
};
