/**
 * Pipvane v3 - Cloud Control Plane
 * License Management Service
 *
 * Implements:
 * - 30-Day Cryptographic Licensing (FR-8.1)
 * - 7-Day Silent Background Refresh & Token Rotation (FR-8.4)
 * - Hardware Fingerprint Enforcement
 * - Anti-Replay Revocation Blacklist
 */

const crypto = require('node:crypto');
const {
  THIRTY_DAYS_SECONDS,
  SEVEN_DAYS_SECONDS,
  signRS256,
  verifyRS256,
  decodeJWT,
} = require('../crypto/jwt');
const { getKeyPair } = require('../crypto/keys');
const { MemoryRevocationStore } = require('./revocationStore');

// Valid tier constants and terminal limits
const TIER_LIMITS = {
  Standard: 4,
  Pro: 7,
};

// SHA-256 hex string validator
const SHA256_HEX_REGEX = /^[a-f0-9]{64}$/i;

class LicenseService {
  /**
   * @param {object} [options]
   * @param {object} [options.keyPair] - RSA keypair { privateKey, publicKey, kid }
   * @param {object} [options.revocationStore] - RevocationStore instance
   * @param {number} [options.clockTolerance=0] - Clock tolerance in seconds
   */
  constructor(options = {}) {
    this.keyPair = options.keyPair || getKeyPair();
    this.revocationStore = options.revocationStore || new MemoryRevocationStore();
    this.clockTolerance = options.clockTolerance || 0;
  }

  /**
   * Normalize and validate hardware device fingerprint
   * @param {string} fingerprint
   * @returns {string} Lowercase SHA-256 hex string
   */
  validateFingerprint(fingerprint) {
    if (!fingerprint || typeof fingerprint !== 'string') {
      const err = new Error('Device fingerprint is required and must be a string');
      err.code = 'INVALID_FINGERPRINT';
      throw err;
    }
    const clean = fingerprint.trim().toLowerCase();
    if (!SHA256_HEX_REGEX.test(clean)) {
      const err = new Error('Device fingerprint must be a valid 64-character SHA-256 hexadecimal string');
      err.code = 'INVALID_FINGERPRINT_FORMAT';
      throw err;
    }
    return clean;
  }

  /**
   * Normalize tier, edition, and determine permanent account limit
   * (Sprint 26: Founder cohort permanent limit overrides)
   *
   * @param {string} tier - "Standard" or "Pro"
   * @param {number} [accountLimit] - Explicit override (optional)
   * @param {string} [edition='Standard'] - "Standard" or "Founder"
   * @returns {{ tier: string, accountLimit: number, edition: string, prioritySupport: boolean }}
   */
  resolveTierAndLimit(tier, accountLimit, edition = 'Standard') {
    const isFounder = edition && typeof edition === 'string' && edition.trim().toLowerCase() === 'founder';
    let normalizedTier = 'Standard';
    if (tier && typeof tier === 'string') {
      const lower = tier.trim().toLowerCase();
      if (lower === 'pro') {
        normalizedTier = 'Pro';
      } else if (lower === 'standard') {
        normalizedTier = 'Standard';
      } else {
        const err = new Error(`Invalid tier "${tier}": must be "Standard" or "Pro"`);
        err.code = 'INVALID_TIER';
        throw err;
      }
    }

    // Sprint 26: If the user holds the Founder claim, their account_limit is permanently
    // set to 4 (or 7 if they bought the pack)
    if (isFounder) {
      const isProPack = normalizedTier === 'Pro' || Number(accountLimit) === 7;
      return {
        tier: isProPack ? 'Pro' : 'Standard',
        accountLimit: isProPack ? 7 : 4,
        edition: 'Founder',
        prioritySupport: true,
      };
    }

    const defaultLimit = TIER_LIMITS[normalizedTier];
    let resolvedLimit = defaultLimit;

    if (accountLimit !== undefined && accountLimit !== null) {
      const limitNum = Number(accountLimit);
      if (!Number.isInteger(limitNum) || limitNum <= 0) {
        const err = new Error('account_limit must be a positive integer');
        err.code = 'INVALID_ACCOUNT_LIMIT';
        throw err;
      }
      if (limitNum > defaultLimit) {
        const err = new Error(`account_limit (${limitNum}) exceeds the maximum allowed for ${normalizedTier} tier (${defaultLimit})`);
        err.code = 'ACCOUNT_LIMIT_EXCEEDED';
        throw err;
      }
      resolvedLimit = limitNum;
    }

    return {
      tier: normalizedTier,
      accountLimit: resolvedLimit,
      edition: 'Standard',
      prioritySupport: false,
    };
  }

  /**
   * Issue a fresh 30-Day cryptographically signed JWT license
   *
   * @param {object} params
   * @param {string} params.licenseId - Unique user license ID
   * @param {string} params.deviceFingerprint - SHA-256 hardware hash
   * @param {string} [params.tier='Standard'] - "Standard" (4 terminals) or "Pro" (7 terminals)
   * @param {number} [params.accountLimit] - Terminals limit override
   * @param {string} [params.edition='Standard'] - "Standard" or "Founder" (Sprint 26)
   * @param {number} [params.currentEpoch] - Custom current epoch in seconds (for deterministic testing)
   * @param {object} [params.customClaims] - Optional additional claims
   * @returns {Promise<{ token: string, payload: object, expires_at: number, valid_for_seconds: number }>}
   */
  async issueLicense({
    licenseId,
    deviceFingerprint,
    tier = 'Standard',
    accountLimit,
    edition,
    currentEpoch,
    customClaims = {},
  }) {
    if (!licenseId || typeof licenseId !== 'string' || licenseId.trim().length === 0) {
      const err = new Error('license_id is required');
      err.code = 'INVALID_LICENSE_ID';
      throw err;
    }

    const cleanFingerprint = this.validateFingerprint(deviceFingerprint);
    const targetEdition = edition || customClaims.edition || (customClaims['https://pipvane.com/claims/edition']) || 'Standard';
    const { tier: resolvedTier, accountLimit: resolvedLimit, edition: resolvedEdition, prioritySupport } =
      this.resolveTierAndLimit(tier, accountLimit, targetEdition);

    const iat = currentEpoch !== undefined ? currentEpoch : Math.floor(Date.now() / 1000);
    // Strict 30-day cryptographic expiry: iat + 2,592,000 seconds (FR-8.1)
    const exp = iat + THIRTY_DAYS_SECONDS;
    const jti = crypto.randomUUID();
    const tokenVersion = 1;

    const payload = {
      iss: 'pipvane-cloud-control-plane',
      aud: 'https://api.pipvane.com/v3',
      sub: licenseId.trim(),
      license_id: licenseId.trim(),
      device_fingerprint: cleanFingerprint,
      account_limit: resolvedLimit,
      tier: resolvedTier,
      edition: resolvedEdition,
      priority_support: prioritySupport,
      jti,
      token_version: tokenVersion,
      iat,
      exp,
      ...customClaims,
    };

    const header = {
      kid: this.keyPair.kid,
    };

    const token = signRS256(payload, this.keyPair.privateKey, header);

    return {
      success: true,
      token,
      payload,
      expires_at: exp,
      valid_for_seconds: THIRTY_DAYS_SECONDS,
    };
  }

  /**
   * Refresh a license token in the 7-day silent refresh loop (FR-8.4)
   *
   * Validates:
   * 1. Cryptographic RS256 signature against public key
   * 2. Unexpired status (now < exp)
   * 3. Hardware device fingerprint match
   * 4. Single-use rotation: checks if previous JTI was already revoked/replayed
   *
   * Actions:
   * 1. Immediately invalidates / blacklists the incoming JTI
   * 2. Increments `token_version` monotonically
   * 3. Issues a fresh 30-day token with new JTI
   *
   * @param {object} params
   * @param {string} params.token - Current active license token
   * @param {string} params.deviceFingerprint - SHA-256 hardware hash from the calling engine
   * @param {number} [params.currentEpoch] - Custom current epoch in seconds (for testing)
   * @returns {Promise<object>}
   */
  async refreshLicense({
    token,
    deviceFingerprint,
    currentEpoch,
  }) {
    if (!token || typeof token !== 'string') {
      const err = new Error('token is required for refresh');
      err.code = 'MISSING_TOKEN';
      throw err;
    }

    const callerFingerprint = this.validateFingerprint(deviceFingerprint);
    const now = currentEpoch !== undefined ? currentEpoch : Math.floor(Date.now() / 1000);

    // 1. Verify cryptographic RS256 signature and token expiry
    let existingPayload;
    try {
      existingPayload = verifyRS256(token, this.keyPair.publicKey, {
        clockTolerance: this.clockTolerance,
        currentEpoch: now,
      });
    } catch (verifyErr) {
      // Re-throw with consistent error properties
      throw verifyErr;
    }

    // 2. Hardware fingerprint verification
    const tokenFingerprint = (existingPayload.device_fingerprint || '').toLowerCase();
    if (tokenFingerprint !== callerFingerprint) {
      const err = new Error('Device fingerprint mismatch: token is bound to a different hardware signature');
      err.code = 'DEVICE_FINGERPRINT_MISMATCH';
      err.expected = tokenFingerprint;
      err.actual = callerFingerprint;
      throw err;
    }

    // 3. Replay prevention: check if this JTI was already revoked
    const isRevoked = await this.revocationStore.isRevoked(existingPayload.jti, now);
    if (isRevoked) {
      const err = new Error(`Token with JTI ${existingPayload.jti} has already been refreshed or revoked (replay attack prevented)`);
      err.code = 'TOKEN_REVOKED';
      err.jti = existingPayload.jti;
      throw err;
    }

    // 4. Token rotation: immediately revoke the old JTI to prevent reuse
    await this.revocationStore.revoke(existingPayload.jti, existingPayload.exp);

    // 5. Silent refresh window calculation (FR-8.4: silent refresh runs within 7 days of expiry)
    const secondsRemaining = existingPayload.exp - now;
    const isSilentRefreshWindow = secondsRemaining <= SEVEN_DAYS_SECONDS;

    // 6. Monotonic token version increment
    const previousVersion = typeof existingPayload.token_version === 'number'
      ? existingPayload.token_version
      : 1;
    const nextVersion = previousVersion + 1;

    // 7. Issue fresh 30-day token
    const newIat = now;
    const newExp = newIat + THIRTY_DAYS_SECONDS;
    const newJti = crypto.randomUUID();

    const newPayload = {
      ...existingPayload,
      jti: newJti,
      token_version: nextVersion,
      iat: newIat,
      exp: newExp,
    };

    const header = {
      kid: this.keyPair.kid,
    };

    const newToken = signRS256(newPayload, this.keyPair.privateKey, header);

    return {
      success: true,
      token: newToken,
      payload: newPayload,
      previous_jti: existingPayload.jti,
      token_version: nextVersion,
      valid_for_seconds: THIRTY_DAYS_SECONDS,
      expires_at: newExp,
      silent_refresh_window_active: isSilentRefreshWindow,
      seconds_remaining_on_previous: secondsRemaining,
    };
  }

  /**
   * Validate a license token (offline or online check)
   *
   * @param {object} params
   * @param {string} params.token
   * @param {string} [params.deviceFingerprint]
   * @param {boolean} [params.checkRevocation=true]
   * @param {number} [params.currentEpoch]
   * @returns {Promise<{ valid: boolean, payload?: object, error?: string, code?: string }>}
   */
  async validateLicense({
    token,
    deviceFingerprint,
    checkRevocation = true,
    currentEpoch,
  }) {
    const now = currentEpoch !== undefined ? currentEpoch : Math.floor(Date.now() / 1000);

    let payload;
    try {
      payload = verifyRS256(token, this.keyPair.publicKey, {
        clockTolerance: this.clockTolerance,
        currentEpoch: now,
      });
    } catch (err) {
      return {
        valid: false,
        error: err.message,
        code: err.code || 'VERIFICATION_FAILED',
      };
    }

    if (deviceFingerprint) {
      const cleanFingerprint = deviceFingerprint.trim().toLowerCase();
      if (payload.device_fingerprint !== cleanFingerprint) {
        return {
          valid: false,
          error: 'Device fingerprint mismatch',
          code: 'DEVICE_FINGERPRINT_MISMATCH',
        };
      }
    }

    if (checkRevocation && this.revocationStore) {
      const isRevoked = await this.revocationStore.isRevoked(payload.jti, now);
      if (isRevoked) {
        return {
          valid: false,
          error: 'Token has been revoked',
          code: 'TOKEN_REVOKED',
          jti: payload.jti,
        };
      }
    }

    const secondsRemaining = payload.exp - now;
    const silentRefreshEligible = secondsRemaining <= SEVEN_DAYS_SECONDS;

    return {
      valid: true,
      payload,
      seconds_remaining: secondsRemaining,
      silent_refresh_eligible: silentRefreshEligible,
    };
  }

  /**
   * Explicitly revoke a token JTI
   * @param {string} jti
   * @param {number} [exp]
   */
  async revokeToken(jti, exp) {
    if (!jti) {
      const err = new Error('jti is required for revocation');
      err.code = 'MISSING_JTI';
      throw err;
    }
    await this.revocationStore.revoke(jti, exp);
    return { success: true, revoked_jti: jti };
  }
}

module.exports = {
  LicenseService,
  TIER_LIMITS,
  SHA256_HEX_REGEX,
  THIRTY_DAYS_SECONDS,
  SEVEN_DAYS_SECONDS,
};
