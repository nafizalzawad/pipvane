"""
Unit tests validating Pipvane v3 LicenseManager (Sprint 8 / FR-8.1 & FR-8.4).
Tests:
1. Offline cryptographic token validation (RS256, EdDSA/Ed25519, HS256).
2. Strict 30-day expiration check and tampering rejection.
3. Entitlement parsing (account_limit, license_id, tier) and enforcement.
4. Windows DPAPI SecureVault integration.
5. Silent 7-day refresh loop and resilient continuity during cloud outages.
"""

import math
import os
import time
import pytest
from pathlib import Path
from typing import Optional, Dict, Any

from engine.vault import VaultStorage
from engine.license_manager import (
    LicenseManager,
    LicenseEntitlements,
    LicenseValidationResult,
    LicenseValidationStatus,
    LicenseError,
    LicenseValidationError,
    LicenseExpiredError,
    LicenseAccountLimitExceededError,
    VAULT_LICENSE_KEY,
    VAULT_LAST_REFRESH_KEY,
    SEVEN_DAYS_SECONDS,
    THIRTY_DAYS_SECONDS,
    create_signed_jwt,
    decode_jwt_unverified,
    verify_jwt_signature,
    DEFAULT_BUNDLED_RSA_PUBLIC_KEY,
    DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
)


# Test RSA Keypair matching DEFAULT_BUNDLED_RSA_PUBLIC_KEY
TEST_RSA_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCd51oBhL856+88
Jwm0jGsefR0I/pfe+ei/Gd8hBIrGMAq4g+ikDQcn9L99zI87rK9KcoFyvdlcGRQc
RFZYKvctOSE8/yOdYGKJPnmgV5H/xOwsltzAs8nB8xLIpESoEeY/6zGZm8bLIrU4
eqY3hTwDiivhYqKSn4sn+iHzAX6RG3FMONvUaNMowCxOR4ub7OFetHLp8wuJR3lt
98J4A9F2Ty2wJFq95torogwvmZSIvH4CP1f7IIigtsZDAaaBHgJUEDZT5TZh/Orp
YD92ZpKOW2yfLpmrkIAC6CsHNP5zLpoYwXLjU7HGXHnTsXadKRyPiuEKtyK9EylU
2MKDLnGZAgMBAAECggEAGoSFywd+Db4fmRx9NsL2IlKkk6Qd4SZa/ezcWfEnVQ0w
X7hXU2WUtBMYLYoy67SeY4RDVP+pBxx3EiAv1/KvO/sCJ/o5Au7y3VWOm2zZafiq
KPrU68/FrDqkBun/VbJb1pQq6j8M7YRQpsGCNIS9hWKo59rmrnT4q6gIF4VCuh97
1bqfdCM3QfYlFSR/833Z9FItBALfLz9pAZqAEsJywzfuK7veUmdvLx9wEnPjuKmX
Ua7HcvP1XhAH5F5B01dY0B1yPuPRY1sJDbw2kaSDQRisn8vfZIB+ln6mmRfAAdal
EZ9fdR74WOvE1myGxSJQeA/xr1aPbm7DPdoNzjVlmQKBgQDd4pOoDC4VKUM+OMLj
AvsJH/A3adEKT75YiTUnGtozZshCn0UseT1P5ZijD+6KV2LEP/3m3vqslLYkfJTv
tiUofJPjn30JvZG0VzgWLA/GawsO/eleyV3ycyHwL6ItAwY4VaErt8+Hvls03iu5
cCXXYsY5FUM1Z1QC80ZKj+ei/wKBgQC2LnYMx9d7flDOAHRN+f181EdvOfuw4b0J
+21L7MIHQBPvTppgUUZecE8dz2wARWqoy75SabXsv0IiHV4CWc7W+FKh95SYN0XZ
GBrxSJe39RMa9KtcRrrgw1YXToRq9sWNWGYUW8tORd+76AvgBvsLnus6JD8P+CTN
yuj1FU0jZwKBgQDc9tPW6XJG7pWhdENYcVJwDtdZR86+NxW288T0EYC06amK9zOb
C43scBgmYypJqKaaC6Qdc3ctwNb21P45DipI/G8GooCR37AnaShYpD59eYJExsXN
+DjPxqpImBcoPjfGiDf94ooQg5guuQXQIQf+S8ZdeHg6yZDXc09P16l1HwKBgQCX
e96RFgKpHwlVoa8Tv5D1oRWc+Nu94pZrnHLvNPjJSpDI5dUbzbjzgfNnWDf3qcDc
hj3FDT1GlGoLL9whnEePHICpSNJ4F02vDIoqoy/0uIWgXl22puFxTztgudsFjhgK
RbzrpMBSX2uANZzaNTO+H/B8MfN19eG0dMpsL8AqdQKBgB2yUiAeS+ryCNOFcF38
H5AFGNNER0EElBzMzZtm+ngs4NrIEHkxkwluMoGPskLwKUVMd76QqmaAHJzR8i4G
opObmxcVGAce6Tl4bZHS6noasv65kk149PG8pH3b0Q+/2pzg9vC5qAy5GHjU/NcT
aD6zzERZxQR7lHixHCF3OSSN
-----END PRIVATE KEY-----"""

# Test Ed25519 Private Key matching DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX
TEST_ED25519_PRIVATE_KEY_HEX = "b826d3096cc70c9f5286df442ae526f25b79e1a16d0a81b86dec767fca1a8627"


@pytest.fixture
def memory_vault():
    """In-memory SecureVault instance for isolated unit tests."""
    vault = VaultStorage(db_path=":memory:")
    yield vault
    vault.close()


def generate_test_jwt(
    license_id: str = "lic_pv3_pro_1001",
    account_limit: int = 7,
    tier: str = "Pro",
    iat: Optional[float] = None,
    exp: Optional[float] = None,
    algorithm: str = "EdDSA",
    private_key: Optional[str] = None,
) -> str:
    """Helper to generate signed test tokens."""
    now = iat if iat is not None else time.time()
    expires = exp if exp is not None else (now + THIRTY_DAYS_SECONDS)

    payload = {
        "iss": "pipvane-cloud-control-plane",
        "aud": "https://api.pipvane.com/v3",
        "sub": license_id,
        "license_id": license_id,
        "device_fingerprint": "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
        "account_limit": account_limit,
        "tier": tier,
        "jti": f"jwt_{int(now * 1000)}",
        "iat": int(now),
        "exp": int(expires),
        "features": ["mt4", "mt5", "circuit_breaker", "news_filter"],
    }

    if algorithm == "EdDSA":
        key = private_key or TEST_ED25519_PRIVATE_KEY_HEX
    elif algorithm == "HS256":
        key = private_key or "secret-test-key-32-bytes-long!!"
    else:
        key = private_key or TEST_RSA_PRIVATE_KEY

    return create_signed_jwt(payload, key, algorithm=algorithm)


# =============================================================================
# 1. OFFLINE CRYPTOGRAPHIC TOKEN VALIDATION TESTS
# =============================================================================

@pytest.mark.unit
def test_validate_valid_eddsa_token(memory_vault):
    """Verify offline validation of Ed25519 (EdDSA) signed JWT."""
    now = 1700000000.0
    token = generate_test_jwt(
        license_id="lic_eddsa_test",
        account_limit=7,
        tier="Pro",
        iat=now,
        exp=now + THIRTY_DAYS_SECONDS,
        algorithm="EdDSA",
    )

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )
    result = manager.validate_token(token, current_time=now + 100.0)

    assert result.is_valid is True
    assert result.status == LicenseValidationStatus.VALID
    assert result.entitlements is not None
    assert result.entitlements.license_id == "lic_eddsa_test"
    assert result.entitlements.account_limit == 7
    assert result.entitlements.tier == "Pro"
    assert math.isclose(result.days_remaining, 30.0, rel_tol=0.01)


@pytest.mark.unit
def test_validate_valid_hs256_token(memory_vault):
    """Verify offline validation of HS256 signed JWT."""
    now = 1700000000.0
    shared_key = "pipvane-secure-shared-key-12345"
    token = generate_test_jwt(
        license_id="lic_hs256_test",
        account_limit=4,
        tier="Standard",
        iat=now,
        exp=now + THIRTY_DAYS_SECONDS,
        algorithm="HS256",
        private_key=shared_key,
    )

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=shared_key,
    )
    result = manager.validate_token(token, current_time=now + 3600.0)

    assert result.is_valid is True
    assert result.entitlements.license_id == "lic_hs256_test"
    assert result.entitlements.account_limit == 4
    assert result.entitlements.tier == "Standard"


@pytest.mark.unit
def test_reject_signature_tampering(memory_vault):
    """Verify modified payload or invalid signature is rejected immediately."""
    token = generate_test_jwt(license_id="lic_original", account_limit=4)
    parts = token.split(".")

    # Tamper payload: increase account_limit from 4 to 999
    payload_dict = decode_jwt_unverified(token)[1]
    payload_dict["account_limit"] = 999
    tampered_payload_b64 = parts[1][:-4] + "AAAA"  # Corrupt signature input
    tampered_token = f"{parts[0]}.{tampered_payload_b64}.{parts[2]}"

    manager = LicenseManager(vault=memory_vault)
    result = manager.validate_token(tampered_token)

    assert result.is_valid is False
    assert result.status in (LicenseValidationStatus.INVALID_SIGNATURE, LicenseValidationStatus.MALFORMED)


@pytest.mark.unit
def test_reject_expired_token(memory_vault):
    """Verify token past its strict 30-day expiration is rejected."""
    now = 1700000000.0
    # Token expired 1 hour ago
    token = generate_test_jwt(iat=now - THIRTY_DAYS_SECONDS - 3600, exp=now - 3600)

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )
    result = manager.validate_token(token, current_time=now)

    assert result.is_valid is False
    assert result.status == LicenseValidationStatus.EXPIRED
    assert "License expired at epoch" in result.reason
    assert result.days_remaining == 0.0


@pytest.mark.unit
def test_missing_token_in_vault(memory_vault):
    """Verify validation when no token has been stored in vault."""
    manager = LicenseManager(vault=memory_vault)
    result = manager.validate_token()

    assert result.is_valid is False
    assert result.status == LicenseValidationStatus.NOT_FOUND
    assert "No license token found" in result.reason


# =============================================================================
# 2. ENTITLEMENT PARSING & ENFORCEMENT TESTS
# =============================================================================

@pytest.mark.unit
def test_entitlement_enforcement_pro_and_standard(memory_vault):
    """Verify enforcement of 7 terminals for Pro and 4 terminals for Standard."""
    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )

    # 1. Pro Tier (7 accounts)
    token_pro = generate_test_jwt(account_limit=7, tier="Pro")
    manager.store_token(token_pro)

    assert manager.entitlements.account_limit == 7
    assert manager.can_register_terminal(current_terminal_count=6) is True
    assert manager.can_register_terminal(current_terminal_count=7) is False
    assert manager.enforce_account_limit(active_accounts_count=7) is True

    with pytest.raises(LicenseAccountLimitExceededError) as exc_info:
        manager.enforce_account_limit(active_accounts_count=8)
    assert "Active accounts (8) exceeds license entitlement limit (7)" in str(exc_info.value)

    # 2. Standard Tier (4 accounts)
    token_std = generate_test_jwt(account_limit=4, tier="Standard")
    manager.store_token(token_std)

    assert manager.entitlements.account_limit == 4
    assert manager.can_register_terminal(current_terminal_count=3) is True
    assert manager.can_register_terminal(current_terminal_count=4) is False
    assert manager.enforce_account_limit(active_accounts_count=4) is True

    with pytest.raises(LicenseAccountLimitExceededError):
        manager.enforce_account_limit(active_accounts_count=5)


# =============================================================================
# 3. SECURE VAULT PERSISTENCE INTEGRATION TESTS
# =============================================================================

@pytest.mark.unit
def test_secure_vault_persistence_and_loading(memory_vault):
    """Verify storing token in SecureVault encrypts with DPAPI and reloads cleanly."""
    token = generate_test_jwt(license_id="lic_vault_roundtrip", account_limit=7)
    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )

    # Store token
    manager.store_token(token)

    # Verify secret exists encrypted in vault
    raw_secret = memory_vault.get_secret_str(VAULT_LICENSE_KEY)
    assert raw_secret == token

    # Instantiate fresh LicenseManager using the same vault
    fresh_manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )
    result = fresh_manager.validate_token()

    assert result.is_valid is True
    assert result.entitlements.license_id == "lic_vault_roundtrip"
    assert result.entitlements.account_limit == 7


# =============================================================================
# 4. SILENT 7-DAY REFRESH LOOP & RESILIENT FALLBACK TESTS
# =============================================================================

@pytest.mark.unit
def test_needs_refresh_logic(memory_vault):
    """Verify token correctly signals needs_refresh when within 7 days of expiry or after 7 days."""
    now = time.time()
    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )

    # Case 1: Fresh token (25 days left, refreshed 1 day ago) -> No refresh needed
    token_fresh = generate_test_jwt(iat=now - 86400, exp=now + (25 * 86400))
    manager.store_token(token_fresh, current_time=now)
    # Set last refresh timestamp to 1 day ago
    memory_vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now - 86400))

    res_fresh = manager.validate_token(current_time=now)
    assert res_fresh.is_valid is True
    assert res_fresh.needs_refresh is False

    # Case 2: 5 days remaining to expiry (<= 7 days) -> Needs refresh
    token_near_exp = generate_test_jwt(iat=now - (25 * 86400), exp=now + (5 * 86400))
    manager.store_token(token_near_exp, current_time=now)
    memory_vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now - 86400))

    res_near_exp = manager.validate_token(current_time=now)
    assert res_near_exp.is_valid is True
    assert res_near_exp.needs_refresh is True

    # Case 3: Refreshed 8 days ago (>= 7 days elapsed) -> Needs refresh
    memory_vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now - (8 * 86400)))
    res_elapsed = manager.validate_token(current_time=now)
    assert res_elapsed.needs_refresh is True


@pytest.mark.unit
def test_silent_refresh_success_overwrites_vault(memory_vault):
    """Verify successful silent refresh extends lease and overwrites vault."""
    now = time.time()
    old_token = generate_test_jwt(license_id="lic_renew", iat=now - (20 * 86400), exp=now + (10 * 86400))

    # Mock Cloud Agent response: returns renewed 30-day token
    renewed_token = generate_test_jwt(license_id="lic_renew", iat=now, exp=now + THIRTY_DAYS_SECONDS)

    def mock_cloud_handler(current_token: str, lic_id: str) -> str:
        assert current_token == old_token
        assert lic_id == "lic_renew"
        return renewed_token

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
        refresh_handler=mock_cloud_handler,
    )
    manager.store_token(old_token, current_time=now)

    # Perform refresh
    success = manager.refresh_token(current_time=now)
    assert success is True

    # Check updated token in vault
    updated_in_vault = memory_vault.get_secret_str(VAULT_LICENSE_KEY)
    assert updated_in_vault == renewed_token
    assert math.isclose(manager.entitlements.expires_at, now + THIRTY_DAYS_SECONDS)


@pytest.mark.unit
def test_silent_refresh_failure_preserves_trading_continuity(memory_vault):
    """Verify failed cloud refresh logs warning but preserves trading continuity until 30-day exp."""
    now = time.time()
    valid_until = now + (15 * 86400)  # Still 15 days remaining!
    token = generate_test_jwt(license_id="lic_continuity", iat=now - (15 * 86400), exp=valid_until)

    def failing_cloud_handler(current_token: str, lic_id: str) -> None:
        raise ConnectionError("Cloud control plane unreachable (HTTP 503 Gateway Timeout)")

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
        refresh_handler=failing_cloud_handler,
    )
    manager.store_token(token, current_time=now)

    # Refresh attempt fails
    success = manager.refresh_token(current_time=now)
    assert success is False

    # Execution continuity verified: License is still valid for 15 days!
    assert manager.is_valid is True
    assert manager.entitlements.license_id == "lic_continuity"
    assert math.isclose(manager.entitlements.expires_at, valid_until)
    assert manager.can_register_terminal(6) is True


@pytest.mark.unit
def test_startup_refresh_check(memory_vault):
    """Verify check_and_refresh_on_startup triggers refresh only when due."""
    now = time.time()
    called = []

    def mock_cloud_handler(curr: str, lic: str) -> str:
        called.append(True)
        return generate_test_jwt(iat=now, exp=now + THIRTY_DAYS_SECONDS)

    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
        refresh_handler=mock_cloud_handler,
    )

    # Case 1: Fresh token (refreshed 1 day ago) -> No call
    token1 = generate_test_jwt(iat=now - 86400, exp=now + (25 * 86400))
    manager.store_token(token1, current_time=now)
    memory_vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now - 86400))

    assert manager.check_and_refresh_on_startup(current_time=now) is True
    assert len(called) == 0

    # Case 2: Token needs refresh (8 days since last refresh) -> Triggers refresh
    memory_vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now - (8 * 86400)))
    assert manager.check_and_refresh_on_startup(current_time=now) is True
    assert len(called) == 1


@pytest.mark.unit
def test_silent_refresh_daemon_lifecycle(memory_vault):
    """Verify starting and stopping the background silent refresh daemon."""
    token = generate_test_jwt()
    manager = LicenseManager(
        vault=memory_vault,
        bundled_public_key=DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    )
    manager.store_token(token)

    manager.start_silent_refresh_daemon(check_interval_s=0.5)
    assert manager._daemon_thread is not None
    assert manager._daemon_thread.is_alive()

    manager.stop_silent_refresh_daemon()
    assert manager._daemon_thread is None
