/**
 * Pipvane v3 - Cloud Control Plane Test Suite
 * Sprint 4: Identity & JWT Lifecycle
 *
 * Tests:
 * 1. Strict 30-day cryptographic expiry rule (exp - iat === 2,592,000s) (FR-8.1)
 * 2. RS256 cryptographic signature verification with public key
 * 3. 7-day silent refresh and token rotation (FR-8.4)
 * 4. Token revocation & anti-replay protection (rejecting re-use of old JTIs)
 * 5. Device fingerprint hardware binding & mismatch rejection
 * 6. Tampered payload / signature rejection
 * 7. Expired token rejection
 * 8. Tier entitlement & terminal limit mapping (Standard: 4, Pro: 7)
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');

const {
  THIRTY_DAYS_SECONDS,
  SEVEN_DAYS_SECONDS,
  signRS256,
  verifyRS256,
  decodeJWT,
} = require('../src/crypto/jwt');
const { generateRSAKeyPair } = require('../src/crypto/keys');
const { LicenseService } = require('../src/services/licenseService');
const { MemoryRevocationStore } = require('../src/services/revocationStore');

describe('Pipvane v3 - JWT Lifecycle & Cryptographic Licensing', () => {
  let testKeyPair;
  let revocationStore;
  let licenseService;

  // Consistent test hardware fingerprint (64-char SHA-256 hex)
  const validFingerprintA = crypto.createHash('sha256').update('WORKSTATION_PRIMARY_HW_UUID_101').digest('hex');
  const validFingerprintB = crypto.createHash('sha256').update('WORKSTATION_STANDBY_HW_UUID_202').digest('hex');

  beforeEach(() => {
    testKeyPair = generateRSAKeyPair(2048);
    revocationStore = new MemoryRevocationStore({ autoPrune: false });
    licenseService = new LicenseService({
      keyPair: testKeyPair,
      revocationStore,
    });
  });

  // =========================================================================
  // TASK 1: Strict 30-day Cryptographic Expiry Rule (FR-8.1)
  // =========================================================================
  describe('Strict 30-Day Expiry Enforcement', () => {
    it('asserts exp - iat is strictly 2,592,000 seconds (30 days) on issuance', async () => {
      const fixedEpoch = 1770000000;
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_std_001',
        deviceFingerprint: validFingerprintA,
        tier: 'Standard',
        currentEpoch: fixedEpoch,
      });

      assert.strictEqual(result.success, true);
      assert.strictEqual(typeof result.token, 'string');
      assert.strictEqual(result.valid_for_seconds, 2592000);
      assert.strictEqual(result.valid_for_seconds, THIRTY_DAYS_SECONDS);

      const payload = result.payload;
      assert.strictEqual(payload.iat, fixedEpoch);
      assert.strictEqual(payload.exp, fixedEpoch + 2592000);
      assert.strictEqual(payload.exp - payload.iat, 2592000);
      assert.strictEqual(result.expires_at, payload.exp);
    });

    it('asserts exp - iat is strictly 2,592,000 seconds upon 7-day silent refresh', async () => {
      const issueEpoch = 1770000000;
      const initial = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_std_001',
        deviceFingerprint: validFingerprintA,
        tier: 'Standard',
        currentEpoch: issueEpoch,
      });

      // Advance clock by 24 days (inside the 7-day silent refresh window: 30 - 24 = 6 days remaining)
      const refreshEpoch = issueEpoch + (24 * 86400);
      const refreshed = await licenseService.refreshLicense({
        token: initial.token,
        deviceFingerprint: validFingerprintA,
        currentEpoch: refreshEpoch,
      });

      assert.strictEqual(refreshed.success, true);
      assert.strictEqual(refreshed.valid_for_seconds, 2592000);
      assert.strictEqual(refreshed.payload.iat, refreshEpoch);
      assert.strictEqual(refreshed.payload.exp, refreshEpoch + 2592000);
      assert.strictEqual(refreshed.payload.exp - refreshed.payload.iat, 2592000);
      assert.strictEqual(refreshed.silent_refresh_window_active, true);
    });
  });

  // =========================================================================
  // TASK 2: RS256 Cryptographic Signature Verification
  // =========================================================================
  describe('RS256 Cryptographic Signature Verification', () => {
    it('successfully verifies a legitimately signed token against public key', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_pro_999',
        deviceFingerprint: validFingerprintA,
        tier: 'Pro',
      });

      const verified = verifyRS256(result.token, testKeyPair.publicKey);
      assert.strictEqual(verified.license_id, 'lic_pipvane_pro_999');
      assert.strictEqual(verified.tier, 'Pro');
      assert.strictEqual(verified.account_limit, 7);
      assert.strictEqual(verified.device_fingerprint, validFingerprintA.toLowerCase());
      assert.ok(verified.jti);
      assert.strictEqual(verified.token_version, 1);
    });

    it('rejects a token verified with a mismatched / different public key', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_std_002',
        deviceFingerprint: validFingerprintA,
      });

      const foreignKeyPair = generateRSAKeyPair(2048);
      assert.throws(
        () => verifyRS256(result.token, foreignKeyPair.publicKey),
        (err) => {
          assert.strictEqual(err.code, 'INVALID_SIGNATURE');
          return true;
        }
      );
    });

    it('rejects a token when the payload claims are tampered or altered', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_std_tamper',
        deviceFingerprint: validFingerprintA,
        tier: 'Standard',
      });

      // Split compact JWT into [header, payload, signature]
      const parts = result.token.split('.');
      const decodedPayload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));

      // Attacker tampers with tier (escalating from Standard to Pro) and account_limit (4 -> 7)
      decodedPayload.tier = 'Pro';
      decodedPayload.account_limit = 7;
      const forgedPayload = Buffer.from(JSON.stringify(decodedPayload), 'utf8').toString('base64url');

      const forgedToken = `${parts[0]}.${forgedPayload}.${parts[2]}`;

      assert.throws(
        () => verifyRS256(forgedToken, testKeyPair.publicKey),
        (err) => {
          assert.strictEqual(err.code, 'INVALID_SIGNATURE');
          return true;
        }
      );
    });

    it('rejects a token when the cryptographic signature is tampered or corrupted', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pipvane_std_003',
        deviceFingerprint: validFingerprintA,
      });

      const parts = result.token.split('.');
      // Corrupt the signature by flipping the first character
      const corruptedSig = (parts[2][0] === 'A' ? 'B' : 'A') + parts[2].substring(1);
      const corruptedToken = `${parts[0]}.${parts[1]}.${corruptedSig}`;

      assert.throws(
        () => verifyRS256(corruptedToken, testKeyPair.publicKey),
        (err) => {
          assert.strictEqual(err.code, 'INVALID_SIGNATURE');
          return true;
        }
      );
    });

    it('rejects a malformed token missing required JWT segments', () => {
      assert.throws(
        () => verifyRS256('not.a.valid.jwt.four.parts', testKeyPair.publicKey),
        (err) => {
          assert.strictEqual(err.code, 'INVALID_TOKEN_FORMAT');
          return true;
        }
      );
    });
  });

  // =========================================================================
  // TASK 3: Token Rotation & JTI Invalidation (Anti-Replay)
  // =========================================================================
  describe('Token Rotation & JTI Blacklist Revocation', () => {
    it('refreshes token and invalidates previous jti (rejecting reuse/replay)', async () => {
      const issueEpoch = Math.floor(Date.now() / 1000);
      const initial = await licenseService.issueLicense({
        licenseId: 'lic_replay_test_01',
        deviceFingerprint: validFingerprintA,
        currentEpoch: issueEpoch,
      });

      const oldJti = initial.payload.jti;
      assert.ok(oldJti);

      // Verify old token is initially NOT revoked
      const isInitiallyRevoked = await revocationStore.isRevoked(oldJti);
      assert.strictEqual(isInitiallyRevoked, false);

      // First refresh: valid
      const refreshEpoch = issueEpoch + (25 * 86400); // 5 days remaining
      const refreshed1 = await licenseService.refreshLicense({
        token: initial.token,
        deviceFingerprint: validFingerprintA,
        currentEpoch: refreshEpoch,
      });

      assert.strictEqual(refreshed1.success, true);
      assert.strictEqual(refreshed1.previous_jti, oldJti);
      assert.strictEqual(refreshed1.token_version, 2);
      assert.notStrictEqual(refreshed1.payload.jti, oldJti);

      // Verify that old JTI is now recorded in the revocation store
      const isOldJtiRevoked = await revocationStore.isRevoked(oldJti, refreshEpoch);
      assert.strictEqual(isOldJtiRevoked, true);

      // SECOND REFRESH ATTEMPT with the original token must be REJECTED!
      await assert.rejects(
        async () => {
          await licenseService.refreshLicense({
            token: initial.token,
            deviceFingerprint: validFingerprintA,
            currentEpoch: refreshEpoch + 60,
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'TOKEN_REVOKED');
          assert.strictEqual(err.jti, oldJti);
          return true;
        }
      );

      // Subsequent refresh with refreshed1 token succeeds and rotates JTI again
      const refreshEpoch2 = refreshEpoch + (10 * 86400);
      const refreshed2 = await licenseService.refreshLicense({
        token: refreshed1.token,
        deviceFingerprint: validFingerprintA,
        currentEpoch: refreshEpoch2,
      });

      assert.strictEqual(refreshed2.success, true);
      assert.strictEqual(refreshed2.previous_jti, refreshed1.payload.jti);
      assert.strictEqual(refreshed2.token_version, 3);

      // Both previous JTIs are now revoked
      assert.strictEqual(await revocationStore.isRevoked(oldJti, refreshEpoch2), true);
      assert.strictEqual(await revocationStore.isRevoked(refreshed1.payload.jti, refreshEpoch2), true);
      assert.strictEqual(await revocationStore.isRevoked(refreshed2.payload.jti, refreshEpoch2), false);
    });
  });

  // =========================================================================
  // TASK 4: Hardware Device Fingerprint Binding & Mismatch
  // =========================================================================
  describe('Hardware Device Fingerprint Binding', () => {
    it('rejects refresh when calling hardware fingerprint does not match token binding', async () => {
      const issueEpoch = 1770000000;
      const initial = await licenseService.issueLicense({
        licenseId: 'lic_hw_bound_01',
        deviceFingerprint: validFingerprintA,
        currentEpoch: issueEpoch,
      });

      // Try refreshing using a different hardware fingerprint (e.g. validFingerprintB)
      await assert.rejects(
        async () => {
          await licenseService.refreshLicense({
            token: initial.token,
            deviceFingerprint: validFingerprintB,
            currentEpoch: issueEpoch + (20 * 86400),
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'DEVICE_FINGERPRINT_MISMATCH');
          assert.strictEqual(err.expected, validFingerprintA.toLowerCase());
          assert.strictEqual(err.actual, validFingerprintB.toLowerCase());
          return true;
        }
      );

      // Verify that the original token was NOT revoked on failed fingerprint check
      assert.strictEqual(await revocationStore.isRevoked(initial.payload.jti), false);
    });

    it('rejects issuance when device fingerprint is not a valid 64-char SHA-256 hex string', async () => {
      await assert.rejects(
        async () => {
          await licenseService.issueLicense({
            licenseId: 'lic_bad_fp',
            deviceFingerprint: 'invalid_short_hash_123',
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'INVALID_FINGERPRINT_FORMAT');
          return true;
        }
      );
    });

    it('handles uppercase and mixed-case SHA-256 fingerprints idempotently', async () => {
      const upperFingerprint = validFingerprintA.toUpperCase();
      const result = await licenseService.issueLicense({
        licenseId: 'lic_case_fp',
        deviceFingerprint: upperFingerprint,
      });

      assert.strictEqual(result.payload.device_fingerprint, validFingerprintA.toLowerCase());

      // Refresh using lowercase
      const refreshed = await licenseService.refreshLicense({
        token: result.token,
        deviceFingerprint: validFingerprintA.toLowerCase(),
      });
      assert.strictEqual(refreshed.success, true);
    });
  });

  // =========================================================================
  // TASK 5: Expiration Enforcement
  // =========================================================================
  describe('Token Expiration Enforcement', () => {
    it('rejects verification and refresh for expired tokens (now >= exp)', async () => {
      const issueEpoch = 1770000000;
      const initial = await licenseService.issueLicense({
        licenseId: 'lic_expired_test',
        deviceFingerprint: validFingerprintA,
        currentEpoch: issueEpoch,
      });

      // Advance clock past 30 days (e.g. 30 days + 1 second)
      const expiredEpoch = issueEpoch + THIRTY_DAYS_SECONDS + 1;

      // verifyRS256 throws TOKEN_EXPIRED
      assert.throws(
        () => verifyRS256(initial.token, testKeyPair.publicKey, { currentEpoch: expiredEpoch }),
        (err) => {
          assert.strictEqual(err.code, 'TOKEN_EXPIRED');
          return true;
        }
      );

      // refreshLicense rejects with TOKEN_EXPIRED
      await assert.rejects(
        async () => {
          await licenseService.refreshLicense({
            token: initial.token,
            deviceFingerprint: validFingerprintA,
            currentEpoch: expiredEpoch,
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'TOKEN_EXPIRED');
          return true;
        }
      );
    });
  });

  // =========================================================================
  // TASK 6: Tier Mapping & Terminal Limits
  // =========================================================================
  describe('Tier Entitlement & Terminal Limits', () => {
    it('assigns 4 terminal limit for Standard tier', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_std_tier',
        deviceFingerprint: validFingerprintA,
        tier: 'Standard',
      });
      assert.strictEqual(result.payload.tier, 'Standard');
      assert.strictEqual(result.payload.account_limit, 4);
    });

    it('assigns 7 terminal limit for Pro tier', async () => {
      const result = await licenseService.issueLicense({
        licenseId: 'lic_pro_tier',
        deviceFingerprint: validFingerprintA,
        tier: 'Pro',
      });
      assert.strictEqual(result.payload.tier, 'Pro');
      assert.strictEqual(result.payload.account_limit, 7);
    });

    it('rejects account_limit exceeding tier ceiling', async () => {
      await assert.rejects(
        async () => {
          await licenseService.issueLicense({
            licenseId: 'lic_overlimit',
            deviceFingerprint: validFingerprintA,
            tier: 'Standard',
            accountLimit: 5, // Standard max is 4
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'ACCOUNT_LIMIT_EXCEEDED');
          return true;
        }
      );
    });

    it('rejects invalid tier identifiers', async () => {
      await assert.rejects(
        async () => {
          await licenseService.issueLicense({
            licenseId: 'lic_invalid_tier',
            deviceFingerprint: validFingerprintA,
            tier: 'EnterprisePlus',
          });
        },
        (err) => {
          assert.strictEqual(err.code, 'INVALID_TIER');
          return true;
        }
      );
    });
  });

  // =========================================================================
  // TASK 7: 7-Day Silent Refresh Window Logic (FR-8.4)
  // =========================================================================
  describe('7-Day Silent Refresh Window Calculations', () => {
    it('correctly marks silent_refresh_window_active when within last 7 days of 30-day lease', async () => {
      const issueEpoch = 1770000000;
      const initial = await licenseService.issueLicense({
        licenseId: 'lic_window_test',
        deviceFingerprint: validFingerprintA,
        currentEpoch: issueEpoch,
      });

      // Day 10 of 30: 20 days remaining -> NOT in silent refresh window
      const day10Epoch = issueEpoch + (10 * 86400);
      const validationDay10 = await licenseService.validateLicense({
        token: initial.token,
        deviceFingerprint: validFingerprintA,
        currentEpoch: day10Epoch,
      });
      assert.strictEqual(validationDay10.valid, true);
      assert.strictEqual(validationDay10.silent_refresh_eligible, false);

      // Day 24 of 30: 6 days remaining (<= 7 days) -> IN silent refresh window
      const day24Epoch = issueEpoch + (24 * 86400);
      const validationDay24 = await licenseService.validateLicense({
        token: initial.token,
        deviceFingerprint: validFingerprintA,
        currentEpoch: day24Epoch,
      });
      assert.strictEqual(validationDay24.valid, true);
      assert.strictEqual(validationDay24.silent_refresh_eligible, true);
    });
  });
});
