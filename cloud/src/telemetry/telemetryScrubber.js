/**
 * Pipvane v3 - Cloud Control Plane
 * Anonymous Telemetry Scrubber & Anonymizer
 *
 * Implements Sprint 21:
 * - Data Scrubbing: Explicitly strips IP addresses, User-Agents, and any raw Telegram text.
 * - Stores ONLY:
 *   * license_id_hash (SHA-256 hash of license_id)
 *   * os_version (sanitized OS string)
 *   * latency_ms (IPC / execution round-trip latency in ms)
 *   * crash_code (standardized error code or null)
 *   * timestamp (epoch seconds)
 */

const crypto = require('node:crypto');

// Forbidden fields that must be scrubbed / purged
const FORBIDDEN_TELEMETRY_FIELDS = [
  'ip',
  'ip_address',
  'client_ip',
  'remote_ip',
  'user_agent',
  'useragent',
  'raw_text',
  'telegram_text',
  'signal_text',
  'message_text',
  'message',
  'text',
  'caption',
  'password',
  'login',
  'broker',
  'account',
  'phone',
  'email',
];

class TelemetryScrubber {
  /**
   * Compute a one-way SHA-256 hash of a license identifier
   * @param {string} licenseId
   * @returns {string} 64-char lowercase hex string
   */
  static hashLicenseId(licenseId) {
    if (!licenseId || typeof licenseId !== 'string') {
      throw new Error('Valid license_id or license_id_hash is required');
    }

    const clean = licenseId.trim();
    // If it's already a 64-char hex hash, retain it
    if (/^[a-f0-9]{64}$/i.test(clean)) {
      return clean.toLowerCase();
    }

    return crypto.createHash('sha256').update(clean, 'utf8').digest('hex');
  }

  /**
   * Sanitize OS Version string
   * @param {string} osVersion
   * @returns {string}
   */
  static sanitizeOsVersion(osVersion) {
    if (!osVersion || typeof osVersion !== 'string') {
      return 'Unknown OS';
    }
    // Limit to 100 characters and strip non-printable or dangerous characters
    return osVersion.trim().replace(/[^\w\s.,\-+()\/]/g, '').substring(0, 100);
  }

  /**
   * Validate and sanitize latency in milliseconds
   * @param {number|string} latencyMs
   * @returns {number}
   */
  static sanitizeLatency(latencyMs) {
    const num = Number(latencyMs);
    if (isNaN(num) || num < 0) {
      return 0.0;
    }
    // Cap at reasonable maximum (e.g. 60,000ms = 60s) and round to 3 decimals
    const capped = Math.min(num, 60000.0);
    return Math.round(capped * 1000) / 1000;
  }

  /**
   * Sanitize crash code
   * @param {string|null} crashCode
   * @returns {string|null}
   */
  static sanitizeCrashCode(crashCode) {
    if (!crashCode || typeof crashCode !== 'string') {
      return null;
    }
    const clean = crashCode.trim().replace(/[^\w\-_]/g, '').substring(0, 50);
    return clean.length > 0 ? clean : null;
  }

  /**
   * Scrub an incoming request and produce an anonymized telemetry record
   *
   * @param {object} rawPayload - Incoming client JSON payload
   * @param {Set<string>|null} [founderRegistry] - Optional set of Founder license SHA-256 hashes
   * @returns {object} Anonymized record ready for persistence
   */
  static scrub(rawPayload, founderRegistry = null) {
    if (!rawPayload || typeof rawPayload !== 'object') {
      const err = new Error('Telemetry payload must be a non-null JSON object');
      err.code = 'INVALID_PAYLOAD';
      throw err;
    }

    // 1. Resolve and hash license identifier
    const rawLicense = rawPayload.license_id || rawPayload.licenseId || rawPayload.license_id_hash || rawPayload.licenseIdHash;
    if (!rawLicense || typeof rawLicense !== 'string' || rawLicense.trim().length === 0) {
      const err = new Error('license_id or license_id_hash is required');
      err.code = 'MISSING_LICENSE_IDENTIFIER';
      throw err;
    }

    const licenseIdHash = this.hashLicenseId(rawLicense);

    // 2. Sanitize OS Version
    const osVersion = this.sanitizeOsVersion(rawPayload.os_version || rawPayload.osVersion);

    // 3. Sanitize Latency
    const latencyMs = this.sanitizeLatency(rawPayload.latency_ms !== undefined ? rawPayload.latency_ms : rawPayload.latencyMs);

    // 4. Sanitize Crash Code
    const crashCode = this.sanitizeCrashCode(rawPayload.crash_code || rawPayload.crashCode);

    // 5. Check Founder priority status (Sprint 26)
    const isFounder = (
      (rawPayload.edition && rawPayload.edition.toString().trim().toLowerCase() === 'founder') ||
      rawPayload.is_founder === true ||
      rawPayload.priority_support === true ||
      (founderRegistry && typeof founderRegistry.has === 'function' && founderRegistry.has(licenseIdHash))
    );

    // 6. Construct scrubbed record containing ONLY permitted fields
    // Notice: Any raw_text, ip, user_agent, telegram_text from rawPayload is COMPLETELY EXCLUDED
    const record = {
      telemetry_id: crypto.randomUUID(),
      license_id_hash: licenseIdHash,
      os_version: osVersion,
      latency_ms: latencyMs,
      crash_code: crashCode,
      edition: isFounder ? 'Founder' : 'Standard',
      support_queue_priority: isFounder ? 'HIGH' : 'NORMAL',
      priority_support: Boolean(isFounder),
      timestamp: Math.floor(Date.now() / 1000),
    };

    return record;
  }
}

module.exports = {
  TelemetryScrubber,
  FORBIDDEN_TELEMETRY_FIELDS,
};
