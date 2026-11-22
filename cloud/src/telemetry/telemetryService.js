/**
 * Pipvane v3 - Cloud Control Plane
 * Telemetry Service
 *
 * Coordinates anonymous telemetry ingestion, strict scrubbing, and persistence.
 */

const { TelemetryScrubber } = require('./telemetryScrubber');
const { MemoryTelemetryStore } = require('./telemetryStore');

class TelemetryService {
  /**
   * @param {object} [options]
   * @param {object} [options.store] - TelemetryStore instance
   */
  constructor(options = {}) {
    this.store = options.store || new MemoryTelemetryStore();
    // Registry of Founder license SHA-256 hashes for support queue prioritization
    this.founderRegistry = new Set();
  }

  /**
   * Register a license ID or hash as belonging to the Founder cohort (Sprint 26)
   * @param {string} licenseIdOrHash
   */
  registerFounder(licenseIdOrHash) {
    if (!licenseIdOrHash || typeof licenseIdOrHash !== 'string') return;
    const hash = TelemetryScrubber.hashLicenseId(licenseIdOrHash);
    this.founderRegistry.add(hash);
  }

  isFounder(licenseIdOrHash) {
    if (!licenseIdOrHash || typeof licenseIdOrHash !== 'string') return false;
    const hash = TelemetryScrubber.hashLicenseId(licenseIdOrHash);
    return this.founderRegistry.has(hash);
  }

  /**
   * Ingest and scrub anonymous telemetry
   * @param {object} rawPayload
   * @returns {Promise<object>} Receipt
   */
  async ingest(rawPayload) {
    // 1. Scrub payload of all PII, IP addresses, user-agents, and raw Telegram text
    // Automatically checks founderRegistry for support prioritization
    const scrubbed = TelemetryScrubber.scrub(rawPayload, this.founderRegistry);

    // 2. Persist in telemetry store
    await this.store.record(scrubbed);

    // 3. Return sanitized confirmation receipt
    return {
      success: true,
      telemetry_id: scrubbed.telemetry_id,
      license_id_hash: scrubbed.license_id_hash,
      support_queue_priority: scrubbed.support_queue_priority,
      timestamp: scrubbed.timestamp,
    };
  }

  /**
   * Get recent anonymized telemetry samples
   * @param {number} [limit=100]
   * @returns {Promise<Array<object>>}
   */
  async getRecent(limit = 100) {
    return await this.store.getRecent(limit);
  }

  /**
   * Get prioritized Support Queue with Founder cohort telemetry placed at the top (Sprint 26)
   * @param {number} [limit=100]
   * @returns {Promise<Array<object>>}
   */
  async getSupportQueue(limit = 100) {
    if (typeof this.store.getSupportQueue === 'function') {
      return await this.store.getSupportQueue(limit);
    }
    return await this.store.getRecent(limit);
  }

  /**
   * Get aggregated latency and crash metrics
   * @returns {Promise<object>}
   */
  async getMetrics() {
    return await this.store.getAggregates();
  }

  async clear() {
    await this.store.clear();
  }
}

module.exports = {
  TelemetryService,
};
