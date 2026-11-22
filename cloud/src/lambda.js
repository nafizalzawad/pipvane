/**
 * Pipvane v3 - Cloud Control Plane
 * AWS Lambda Serverless Handler
 *
 * Compatible with API Gateway HTTP API (v2) and REST API (v1).
 */

const { LicenseService } = require('./services/licenseService');
const { DynamoDBRevocationStore } = require('./services/revocationStore');
const { RelayService } = require('./relay/relayService');
const { DynamoDBRelayQueue } = require('./relay/relayQueue');
const { TelemetryService } = require('./telemetry/telemetryService');
const { DynamoDBTelemetryStore } = require('./telemetry/telemetryStore');

// Optional AWS SDK DynamoDB DocumentClient initialization if deployed to AWS
let revocationStore;
let relayQueue;
let telemetryStore;
if (process.env.AWS_EXECUTION_ENV) {
  try {
    const AWS = require('aws-sdk');
    const docClient = new AWS.DynamoDB.DocumentClient();
    if (process.env.PIPVANE_DYNAMO_TABLE) {
      revocationStore = new DynamoDBRevocationStore({
        dynamoClient: docClient,
        tableName: process.env.PIPVANE_DYNAMO_TABLE,
      });
    }
    if (process.env.PIPVANE_RELAY_TABLE) {
      relayQueue = new DynamoDBRelayQueue({
        dynamoClient: docClient,
        tableName: process.env.PIPVANE_RELAY_TABLE,
      });
    }
    if (process.env.PIPVANE_TELEMETRY_TABLE) {
      telemetryStore = new DynamoDBTelemetryStore({
        dynamoClient: docClient,
        tableName: process.env.PIPVANE_TELEMETRY_TABLE,
      });
    }
  } catch {
    // Fallback to in-memory if AWS SDK is not bundled
  }
}

const defaultLicenseService = new LicenseService({ revocationStore });
const defaultRelayService = new RelayService({ queue: relayQueue });
const defaultTelemetryService = new TelemetryService({ store: telemetryStore });

/**
 * AWS Lambda proxy handler
 * @param {object} event - API Gateway event
 * @param {object} context - Lambda execution context
 */
exports.handler = async (event, context) => {
  const method = (event.httpMethod || (event.requestContext && event.requestContext.http && event.requestContext.http.method) || 'GET').toUpperCase();
  const rawPath = event.path || (event.requestContext && event.requestContext.http && event.requestContext.http.path) || '/';

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Device-Fingerprint',
  };

  if (method === 'OPTIONS') {
    return {
      statusCode: 204,
      headers,
      body: '',
    };
  }

  let body = {};
  if (event.body) {
    try {
      const rawBody = event.isBase64Encoded
        ? Buffer.from(event.body, 'base64').toString('utf8')
        : event.body;
      body = JSON.parse(rawBody);
    } catch {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'Invalid JSON body' }),
      };
    }
  }

  try {
    if (method === 'POST' && rawPath.endsWith('/api/license/issue')) {
      const result = await defaultLicenseService.issueLicense({
        licenseId: body.license_id || body.licenseId,
        deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
        tier: body.tier || 'Standard',
        accountLimit: body.account_limit !== undefined ? body.account_limit : body.accountLimit,
        edition: body.edition,
        customClaims: body.custom_claims || body.customClaims || {},
      });

      return {
        statusCode: 201,
        headers,
        body: JSON.stringify(result),
      };
    }

    if (method === 'POST' && rawPath.endsWith('/api/license/refresh')) {
      let token = body.token;
      const authHeader = (event.headers && (event.headers.Authorization || event.headers.authorization));
      if (!token && authHeader && authHeader.startsWith('Bearer ')) {
        token = authHeader.substring(7).trim();
      }

      const result = await defaultLicenseService.refreshLicense({
        token,
        deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
      });

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify(result),
      };
    }

    if (method === 'POST' && rawPath.endsWith('/api/license/validate')) {
      const result = await defaultLicenseService.validateLicense({
        token: body.token,
        deviceFingerprint: body.device_fingerprint || body.deviceFingerprint,
      });

      return {
        statusCode: result.valid ? 200 : 401,
        headers,
        body: JSON.stringify(result),
      };
    }

    if (method === 'GET' && rawPath.endsWith('/api/license/public-key')) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          algorithm: 'RS256',
          kid: defaultLicenseService.keyPair.kid,
          publicKey: defaultLicenseService.keyPair.publicKey,
        }),
      };
    }

    if (method === 'GET' && (rawPath.endsWith('/api/health') || rawPath.endsWith('/health'))) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          status: 'healthy',
          service: 'pipvane-cloud-control-plane-lambda',
          version: '3.0.0',
          timestamp: Math.floor(Date.now() / 1000),
        }),
      };
    }

    // Encrypted Command Relay Endpoints (Sprint 17)
    if (method === 'POST' && (rawPath.endsWith('/relay/command') || rawPath.endsWith('/api/relay/command'))) {
      const qParams = event.queryStringParameters || {};
      const result = await defaultRelayService.pushCommand({
        licenseId: (event.headers && (event.headers['x-license-id'] || event.headers['X-License-Id'])) || qParams.license_id,
        queueType: qParams.queue,
        envelope: body,
      });
      return {
        statusCode: 201,
        headers,
        body: JSON.stringify(result),
      };
    }

    if (method === 'GET' && (rawPath.endsWith('/relay/command') || rawPath.endsWith('/api/relay/command'))) {
      const qParams = event.queryStringParameters || {};
      const licenseId = qParams.license_id || (event.headers && (event.headers['x-license-id'] || event.headers['X-License-Id']));
      const queueType = qParams.queue || 'inbox';
      const waitMs = qParams.wait_ms ? parseInt(qParams.wait_ms, 10) : 0;
      const mode = qParams.mode || 'pop';

      if (!licenseId) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ error: 'license_id is required' }),
        };
      }

      if (mode === 'peek') {
        const messages = await defaultRelayService.peekCommands({
          licenseId,
          queueType,
          limit: qParams.limit ? parseInt(qParams.limit, 10) : 10,
        });
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({ success: true, count: messages.length, messages }),
        };
      }

      const envelope = await defaultRelayService.popCommand({
        licenseId,
        queueType,
        waitMs,
      });

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, message: envelope, queue: queueType }),
      };
    }

    if (method === 'POST' && (rawPath.endsWith('/relay/command/ack') || rawPath.endsWith('/api/relay/command/ack'))) {
      const qParams = event.queryStringParameters || {};
      const licenseId = body.license_id || body.licenseId || qParams.license_id;
      const messageId = body.message_id || body.messageId || qParams.message_id;
      const queueType = body.queue || qParams.queue || 'inbox';

      const acked = await defaultRelayService.ackCommand({
        licenseId,
        queueType,
        messageId,
      });

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, acknowledged: acked, message_id: messageId }),
      };
    }

    if (method === 'GET' && (rawPath.endsWith('/relay/command/stats') || rawPath.endsWith('/api/relay/command/stats'))) {
      const qParams = event.queryStringParameters || {};
      const licenseId = qParams.license_id || (event.headers && (event.headers['x-license-id'] || event.headers['X-License-Id']));
      const stats = await defaultRelayService.getStats(licenseId);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, stats }),
      };
    }

    // Anonymous Telemetry Ingestion (Sprint 21)
    if (method === 'POST' && (rawPath.endsWith('/telemetry') || rawPath.endsWith('/api/telemetry'))) {
      const result = await defaultTelemetryService.ingest(body);
      return {
        statusCode: 201,
        headers,
        body: JSON.stringify(result),
      };
    }

    if (method === 'GET' && (
      rawPath.endsWith('/telemetry') ||
      rawPath.endsWith('/api/telemetry') ||
      rawPath.endsWith('/telemetry/metrics') ||
      rawPath.endsWith('/api/telemetry/metrics')
    )) {
      const metrics = await defaultTelemetryService.getMetrics();
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, metrics }),
      };
    }

    if (method === 'GET' && (
      rawPath.endsWith('/telemetry/support-queue') ||
      rawPath.endsWith('/api/telemetry/support-queue')
    )) {
      const qParams = event.queryStringParameters || {};
      const limit = qParams.limit ? parseInt(qParams.limit, 10) : 100;
      const queue = await defaultTelemetryService.getSupportQueue(limit);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, count: queue.length, support_queue: queue }),
      };
    }

    return {
      statusCode: 404,
      headers,
      body: JSON.stringify({ error: 'Endpoint not found', path: rawPath }),
    };
  } catch (err) {
    let statusCode = 400;
    if (err.code === 'TOKEN_EXPIRED' || err.code === 'INVALID_SIGNATURE' || err.code === 'INVALID_TOKEN_FORMAT') {
      statusCode = 401;
    } else if (err.code === 'DEVICE_FINGERPRINT_MISMATCH' || err.code === 'TOKEN_REVOKED') {
      statusCode = 403;
    }

    return {
      statusCode,
      headers,
      body: JSON.stringify({
        success: false,
        error: err.message,
        code: err.code || 'REQUEST_FAILED',
      }),
    };
  }
};
