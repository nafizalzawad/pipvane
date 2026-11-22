/**
 * Pipvane v3 - Mock Engine API Server for E2E Testing
 *
 * Implements the local Engine REST endpoints on 127.0.0.1:8765
 * with CORS headers for static Next.js frontend integration:
 * - POST /api/v1/license/validate
 * - GET  /api/v1/status
 * - POST /api/v1/telegram/connect
 * - POST /api/v1/broker/terminal
 * - GET  /health
 */

const http = require('http');

const PORT = process.env.ENGINE_PORT || 8765;
const HOST = '127.0.0.1';

const server = http.createServer((req, res) => {
  // Set CORS headers for all responses
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-pipvane-session-token');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${HOST}:${PORT}`);

  // Health probe
  if (url.pathname === '/health' || url.pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'OK', service: 'Pipvane-Mock-Engine-API', uptime: process.uptime() }));
    return;
  }

  // Engine status endpoint
  if (url.pathname === '/api/v1/status' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'RUNNING',
      version: '3.0.0',
      uptimeSeconds: Math.floor(process.uptime()) + 3600,
      ipcLatencyUs: 780,
      activeAccounts: 1,
      cloudSync: {
        status: 'ACTIVE',
        leaseValid: true,
        remainingDays: 30,
      },
    }));
    return;
  }

  // License validation endpoint
  if (url.pathname === '/api/v1/license/validate' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
    });

    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const key = (payload.license_key || '').trim();

        if (!key) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            success: false,
            valid: false,
            error: 'EMPTY_LICENSE_KEY',
            message: 'Please enter a valid Pipvane license key or 30-day offline lease token.',
          }));
          return;
        }

        // Failure simulation: Invalid key pattern
        if (
          key.toUpperCase().startsWith('INVALID') ||
          key.toUpperCase().includes('FAIL') ||
          key.length < 15
        ) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            success: false,
            valid: false,
            error: 'INVALID_SIGNATURE',
            message: 'Cryptographic signature verification failed: Token is invalid, untrusted, or has expired.',
          }));
          return;
        }

        // Happy path: Valid license
        let tier = 'Pro Multi-Terminal';
        let maxAccounts = 7;

        if (key.includes('ENT') || key.includes('VIP')) {
          tier = 'Enterprise';
          maxAccounts = 25;
        } else if (key.includes('START')) {
          tier = 'Starter';
          maxAccounts = 3;
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          success: true,
          valid: true,
          tier,
          daysRemaining: 30,
          maxAccounts,
          licenseId: 'LIC-PV3-PRO-8921',
          message: 'Cryptographic license token verified. 30-day offline grace lease granted.',
        }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          success: false,
          valid: false,
          error: 'BAD_REQUEST',
          message: 'Unable to parse request payload.',
        }));
      }
    });
    return;
  }

  // Telegram dummy connect endpoint
  if (url.pathname === '/api/v1/telegram/connect' && req.method === 'POST') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      success: true,
      status: 'CONNECTED',
      phone: '+1 (555) 0199',
      safety_mode: true,
    }));
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'NOT_FOUND', message: `Endpoint ${url.pathname} not found` }));
});

server.listen(PORT, HOST, () => {
  console.log(`[MockEngineServer] Pipvane Mock Engine API running on http://${HOST}:${PORT}`);
});

process.on('SIGINT', () => {
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
});
