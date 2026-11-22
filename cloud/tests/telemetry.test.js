/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * Sprint 21: Anonymous Telemetry Ingestion & Data Scrubbing
 *
 * Verifies:
 * 1. Ingestion API:
 *    - Serverless endpoint (POST /telemetry) accepts predefined JSON payloads.
 * 2. Data Scrubbing:
 *    - Strips IP addresses, User-Agents, and any raw Telegram text before writing to database.
 *    - Stores ONLY: license_id_hash, os_version, latency_ms, crash_code, timestamp.
 * 3. Hashing:
 *    - license_id is irreversibly hashed to SHA-256 hex; plaintext is never stored.
 * 4. Latency & Crash Code Sanitization:
 *    - Numeric latency precision, bounds capping, and crash code formatting.
 * 5. Metrics Aggregation:
 *    - Calculates sample count, average, P50, P95, P99 latencies, and crash statistics.
 * 6. HTTP Server End-to-End Integration.
 */

const { describe, it, before, after, beforeEach } = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const crypto = require('node:crypto');

const { TelemetryScrubber } = require('../src/telemetry/telemetryScrubber');
const { TelemetryService } = require('../src/telemetry/telemetryService');
const { MemoryTelemetryStore } = require('../src/telemetry/telemetryStore');
const { createServer } = require('../src/server');

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

describe('Sprint 21 - Anonymous Telemetry Pipeline', () => {
  let telemetryStore;
  let telemetryService;
  let server;
  let port;

  const sampleLicenseId = 'lic_user_998877';
  const expectedHash = crypto.createHash('sha256').update(sampleLicenseId, 'utf8').digest('hex');

  beforeEach(() => {
    telemetryStore = new MemoryTelemetryStore({ maxCapacity: 1000 });
    telemetryService = new TelemetryService({ store: telemetryStore });
  });

  before(async () => {
    const testStore = new MemoryTelemetryStore({ maxCapacity: 1000 });
    const testService = new TelemetryService({ store: testStore });
    const instance = createServer(undefined, undefined, undefined, testService);
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
  // 1. Data Scrubbing & Zero-Knowledge Verification
  // =========================================================================
  describe('Data Scrubbing & Field Sanitization', () => {
    it('explicitly strips IP addresses, User-Agents, and raw Telegram text before writing to DB', async () => {
      const hostilePayload = {
        license_id: sampleLicenseId,
        os_version: 'Windows 11 Pro 23H2',
        latency_ms: 0.045,
        crash_code: 'CRASH_NONE',
        // Prohibited PII / network / signal leak fields that must be scrubbed:
        ip: '192.168.1.100',
        ip_address: '10.0.0.45',
        client_ip: '172.16.0.2',
        user_agent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) PipvaneAgent/3.0',
        raw_text: 'BUY EURUSD 1.0850 SL 1.0800 TP 1.0950',
        telegram_text: 'VIP Signals: Buy Gold @ 2035.50',
        signal_text: 'SELL GBPUSD now',
        password: 'super_secret_broker_password',
        login: '12345678',
        broker: 'ICMarkets-Live01',
      };

      const receipt = await telemetryService.ingest(hostilePayload);
      assert.strictEqual(receipt.success, true);
      assert.strictEqual(receipt.license_id_hash, expectedHash);

      // Inspect persisted record in database store
      const recent = await telemetryStore.getRecent(1);
      assert.strictEqual(recent.length, 1);
      const stored = recent[0];

      // 1. Assert permitted fields exist and are accurate
      assert.strictEqual(stored.license_id_hash, expectedHash);
      assert.strictEqual(stored.os_version, 'Windows 11 Pro 23H2');
      assert.strictEqual(stored.latency_ms, 0.045);
      assert.strictEqual(stored.crash_code, 'CRASH_NONE');
      assert.ok(stored.timestamp);
      assert.ok(stored.telemetry_id);

      // 2. Assert all PII, IP, User-Agent, and Telegram text fields are completely ABSENT
      assert.strictEqual(stored.ip, undefined);
      assert.strictEqual(stored.ip_address, undefined);
      assert.strictEqual(stored.client_ip, undefined);
      assert.strictEqual(stored.user_agent, undefined);
      assert.strictEqual(stored.raw_text, undefined);
      assert.strictEqual(stored.telegram_text, undefined);
      assert.strictEqual(stored.signal_text, undefined);
      assert.strictEqual(stored.password, undefined);
      assert.strictEqual(stored.login, undefined);
      assert.strictEqual(stored.broker, undefined);

      // 3. Plain-text license_id must NEVER be stored
      assert.strictEqual(stored.license_id, undefined);
      assert.strictEqual(JSON.stringify(stored).includes(sampleLicenseId), false);
    });

    it('hashes license_id with SHA-256 and preserves pre-hashed values idempotently', () => {
      const hashed1 = TelemetryScrubber.hashLicenseId('my_test_license_key');
      assert.strictEqual(hashed1.length, 64);
      assert.strictEqual(/^[a-f0-9]{64}$/.test(hashed1), true);

      // Pre-hashed value remains identical
      const hashed2 = TelemetryScrubber.hashLicenseId(hashed1);
      assert.strictEqual(hashed2, hashed1);
    });

    it('sanitizes latency_ms bounds and rounding', () => {
      assert.strictEqual(TelemetryScrubber.sanitizeLatency(0.04523), 0.045);
      assert.strictEqual(TelemetryScrubber.sanitizeLatency(-5.0), 0.0);
      assert.strictEqual(TelemetryScrubber.sanitizeLatency(120000.0), 60000.0); // Capped at 60s
      assert.strictEqual(TelemetryScrubber.sanitizeLatency('invalid'), 0.0);
    });

    it('sanitizes os_version and crash_code strings', () => {
      const cleanOs = TelemetryScrubber.sanitizeOsVersion('Windows 10 Pro <script>alert(1)</script>');
      assert.strictEqual(cleanOs.includes('<script>'), false);

      assert.strictEqual(TelemetryScrubber.sanitizeCrashCode('CRASH_IPC_TIMEOUT'), 'CRASH_IPC_TIMEOUT');
      assert.strictEqual(TelemetryScrubber.sanitizeCrashCode(null), null);
      assert.strictEqual(TelemetryScrubber.sanitizeCrashCode(''), null);
    });

    it('rejects payload missing license identifier with MISSING_LICENSE_IDENTIFIER', async () => {
      await assert.rejects(
        async () => {
          await telemetryService.ingest({
            os_version: 'Windows 11',
            latency_ms: 0.05,
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'MISSING_LICENSE_IDENTIFIER');
          return true;
        }
      );
    });
  });

  // =========================================================================
  // 2. Metrics & Statistical Aggregation
  // =========================================================================
  describe('Metrics & Latency Percentiles', () => {
    it('accurately calculates P50, P95, P99, average latency, and crash code counts', async () => {
      // Ingest 100 samples with known latencies: 1ms through 100ms
      for (let i = 1; i <= 100; i++) {
        await telemetryService.ingest({
          license_id: `lic_perf_${i}`,
          os_version: 'Windows Server 2022',
          latency_ms: i,
          crash_code: i === 13 ? 'ERR_DLL_ATTACH' : i === 77 ? 'ERR_PIPE_BROKEN' : 'CRASH_NONE',
        });
      }

      const metrics = await telemetryService.getMetrics();
      assert.strictEqual(metrics.sample_count, 100);
      assert.strictEqual(metrics.average_latency_ms, 50.5);
      assert.strictEqual(metrics.p50_latency_ms, 51);
      assert.strictEqual(metrics.p95_latency_ms, 96);
      assert.strictEqual(metrics.p99_latency_ms, 100);

      // Verify crash counts
      assert.strictEqual(metrics.crash_counts['ERR_DLL_ATTACH'], 1);
      assert.strictEqual(metrics.crash_counts['ERR_PIPE_BROKEN'], 1);
      assert.strictEqual(metrics.crash_counts['CRASH_NONE'], 98);
    });
  });

  // =========================================================================
  // 3. Ring Buffer Capacity & Eviction
  // =========================================================================
  describe('Ring Buffer Capacity', () => {
    it('evicts oldest records when capacity is exceeded', async () => {
      const smallStore = new MemoryTelemetryStore({ maxCapacity: 3 });
      const smallService = new TelemetryService({ store: smallStore });

      await smallService.ingest({ license_id: 'lic_1', latency_ms: 10 });
      await smallService.ingest({ license_id: 'lic_2', latency_ms: 20 });
      await smallService.ingest({ license_id: 'lic_3', latency_ms: 30 });
      assert.strictEqual(await smallStore.count(), 3);

      // 4th ingest evicts lic_1
      await smallService.ingest({ license_id: 'lic_4', latency_ms: 40 });
      assert.strictEqual(await smallStore.count(), 3);

      const recent = await smallStore.getRecent(10);
      const latencies = recent.map((r) => r.latency_ms);
      assert.deepStrictEqual(latencies, [40, 30, 20]);
    });
  });

  // =========================================================================
  // 4. REST API Ingestion Endpoints (POST /telemetry)
  // =========================================================================
  describe('HTTP End-to-End API (POST /telemetry)', () => {
    it('POST /telemetry successfully ingests valid payload and returns 201 Created', async () => {
      const res = await makeRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/telemetry',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'User-Agent': 'LocalEngine/3.0.0 (Windows NT 10.0; Win64; x64)',
            'X-Forwarded-For': '203.0.113.195',
          },
        },
        {
          license_id: 'lic_live_engine_100',
          os_version: 'Windows 10 Enterprise LTSC (build 19044)',
          latency_ms: 0.038,
          crash_code: 'CRASH_NONE',
        }
      );

      assert.strictEqual(res.statusCode, 201);
      assert.strictEqual(res.data.success, true);
      assert.ok(res.data.telemetry_id);
      assert.ok(res.data.license_id_hash);
      assert.ok(res.data.timestamp);
    });

    it('POST /telemetry returns 400 Bad Request when missing license_id', async () => {
      const res = await makeRequest(
        {
          hostname: '127.0.0.1',
          port,
          path: '/telemetry',
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
        {
          os_version: 'Windows 11',
          latency_ms: 0.05,
        }
      );

      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(res.data.code, 'MISSING_LICENSE_IDENTIFIER');
    });

    it('GET /telemetry/metrics returns aggregated metrics', async () => {
      const res = await makeRequest({
        hostname: '127.0.0.1',
        port,
        path: '/telemetry/metrics',
        method: 'GET',
      });

      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.data.success, true);
      assert.ok(res.data.metrics);
      assert.strictEqual(typeof res.data.metrics.sample_count, 'number');
    });
  });
});
