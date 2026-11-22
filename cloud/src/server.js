/**
 * Pipvane v3 - Cloud Control Plane
 * HTTP Server for Licensing & Token Lifecycle API
 */

const http = require('node:http');
const { createLicenseApi, sendJSON } = require('./api/handlers');
const { LicenseService } = require('./services/licenseService');
const { RelayService } = require('./services/relayService');
const { createRelayApi } = require('./relay/handlers');
const { RelayService: CommandRelayService } = require('./relay/relayService');
const { createTelemetryApi } = require('./telemetry/handlers');
const { TelemetryService } = require('./telemetry/telemetryService');

function createServer(
  licenseService = new LicenseService(),
  relayService = new RelayService(),
  commandRelayService = new CommandRelayService(),
  telemetryService = new TelemetryService()
) {
  const api = createLicenseApi(licenseService, relayService);
  const commandApi = createRelayApi(commandRelayService);
  const telemetryApi = createTelemetryApi(telemetryService);

  const server = http.createServer(async (req, res) => {
    // Enable CORS for web and dashboard clients
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Device-Fingerprint, X-License-Id');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = url.pathname;

    try {
      if (req.method === 'POST' && pathname === '/api/license/issue') {
        return await api.handleIssue(req, res);
      }

      if (req.method === 'POST' && pathname === '/api/license/refresh') {
        return await api.handleRefresh(req, res);
      }

      if (req.method === 'POST' && pathname === '/api/license/validate') {
        return await api.handleValidate(req, res);
      }

      if (req.method === 'POST' && pathname === '/api/license/revoke') {
        return await api.handleRevoke(req, res);
      }

      if (req.method === 'GET' && pathname === '/api/license/public-key') {
        return api.handlePublicKey(req, res);
      }

      if (req.method === 'GET' && (pathname === '/api/health' || pathname === '/health')) {
        return api.handleHealth(req, res);
      }

      // E2EE Configuration Relay Sync Endpoints (Sprint 15)
      if (req.method === 'POST' && (pathname === '/relay/sync' || pathname === '/api/relay/sync')) {
        return await api.handleRelayStore(req, res);
      }

      if (req.method === 'GET' && (pathname === '/relay/sync' || pathname === '/api/relay/sync')) {
        return api.handleRelayFetch(req, res, url.searchParams);
      }

      if (req.method === 'DELETE' && (pathname === '/relay/sync' || pathname === '/api/relay/sync')) {
        return api.handleRelayDelete(req, res, url.searchParams);
      }

      // E2EE Command Relay Inbox/Outbox Endpoints (Sprint 17)
      if (req.method === 'POST' && (
        pathname === '/relay/command' ||
        pathname === '/api/relay/command' ||
        pathname === '/relay/commands' ||
        pathname === '/api/relay/commands'
      )) {
        return await commandApi.handlePush(req, res);
      }

      if (req.method === 'GET' && (
        pathname === '/relay/command' ||
        pathname === '/api/relay/command' ||
        pathname === '/relay/commands' ||
        pathname === '/api/relay/commands' ||
        pathname === '/relay/commands/outbox' ||
        pathname === '/api/relay/commands/outbox' ||
        pathname === '/relay/outbox' ||
        pathname === '/api/relay/outbox'
      )) {
        if (pathname.includes('outbox') && !url.searchParams.has('queue')) {
          url.searchParams.set('queue', 'outbox');
        }
        return await commandApi.handlePop(req, res);
      }

      if (req.method === 'POST' && (
        pathname === '/relay/command/ack' ||
        pathname === '/api/relay/command/ack' ||
        pathname === '/relay/commands/ack' ||
        pathname === '/api/relay/commands/ack'
      )) {
        return await commandApi.handleAck(req, res);
      }

      if (req.method === 'GET' && (
        pathname === '/relay/command/stats' ||
        pathname === '/api/relay/command/stats' ||
        pathname === '/relay/commands/stats' ||
        pathname === '/api/relay/commands/stats'
      )) {
        return await commandApi.handleStats(req, res);
      }

      // Anonymous Telemetry Ingestion Endpoints (Sprint 21)
      if (req.method === 'POST' && (pathname === '/telemetry' || pathname === '/api/telemetry')) {
        return await telemetryApi.handleIngest(req, res);
      }

      if (req.method === 'GET' && (
        pathname === '/telemetry' ||
        pathname === '/api/telemetry' ||
        pathname === '/telemetry/metrics' ||
        pathname === '/api/telemetry/metrics'
      )) {
        return await telemetryApi.handleMetrics(req, res);
      }

      // Prioritized Founder Support Queue Endpoint (Sprint 26)
      if (req.method === 'GET' && (
        pathname === '/telemetry/support-queue' ||
        pathname === '/api/telemetry/support-queue' ||
        pathname === '/telemetry/queue' ||
        pathname === '/api/telemetry/queue'
      )) {
        return await telemetryApi.handleSupportQueue(req, res, url.searchParams);
      }

      sendJSON(res, 404, {
        error: 'Not Found',
        path: pathname,
      });
    } catch (err) {
      console.error('[Cloud Control Plane] Unhandled error:', err);
      sendJSON(res, 500, {
        error: 'Internal Server Error',
        message: err.message,
      });
    }
  });

  return { server, api, commandApi, telemetryApi, licenseService, commandRelayService, telemetryService };
}

// Start standalone server if run directly
if (require.main === module) {
  const PORT = process.env.PORT || 8080;
  const HOST = process.env.HOST || '0.0.0.0';
  const { server } = createServer();

  server.listen(PORT, HOST, () => {
    console.log(`[Pipvane Cloud Control Plane] Listening on http://${HOST}:${PORT}`);
  });
}

module.exports = {
  createServer,
};
