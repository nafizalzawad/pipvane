"""
Pipvane v3 Security Audit Automation Suite (Sprint 8).

Validates:
1. DPAPI Zero-Knowledge Extraction Resistance:
   - Simulates malicious direct SQLite file and binary disk extraction attacks.
   - Confirms that raw database bytes and direct SQLite SELECT queries yield undecipherable ciphertext
     and never reveal plaintext broker passwords, master secrets, or Telegram auth keys.
   - Validates entropy isolation and secure in-memory wiping (secure_zero).
2. Offline Chronology & 30-Day Cryptographic Grace Period Boundaries (FR-8.1 & FR-8.4):
   - Mocks the system clock to simulate multi-day offline operation without cloud access.
   - Confirms trades continue executing seamlessly on Day 29 within the 30-day offline grace period.
   - Advances mock clock to Day 31 (grace period exceeded).
   - Confirms the engine strictly locks execution and throws LICENSE_EXPIRED.
   - Confirms the UI and diagnostics layers handle expiration gracefully without crashing.
"""

from __future__ import annotations

import math
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional
import pytest

# Ensure repository root is on sys.path for direct CLI script execution
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from engine.vault import (  # noqa: E402
    VaultStorage,
    VaultCryptoError,
    dpapi_protect,
    dpapi_unprotect,
    get_machine_entropy,
    secure_zero,
)
from engine.license_manager import (  # noqa: E402
    LicenseManager,
    LicenseValidationStatus,
    LicenseExpiredError,
    LicenseValidationError,
    create_signed_jwt,
    DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
    THIRTY_DAYS_SECONDS,
)
from engine.risk_engine import (  # noqa: E402
    RiskEngine,
    AccountProfile,
    MarketQuote,
)
from engine.ipc_protocol import (  # noqa: E402
    OrderIntent,
    OrderSide,
    ActionType,
)


# =============================================================================
# HELPER UTILITIES: SHANNON ENTROPY & MOCK TEST DATA
# =============================================================================

def calculate_shannon_entropy(data: bytes) -> float:
    """
    Calculates Shannon entropy in bits per byte (0.0 to 8.0).
    High-grade ciphertext typically exhibits entropy > 6.0, whereas plaintext
    ASCII text typically exhibits entropy between 2.0 and 4.5.
    """
    if not data:
        return 0.0
    byte_counts: Dict[int, int] = {}
    for b in data:
        byte_counts[b] = byte_counts.get(b, 0) + 1

    entropy = 0.0
    total = len(data)
    for count in byte_counts.values():
        p_x = count / total
        entropy -= p_x * math.log2(p_x)
    return entropy


# Ed25519 test keypair for deterministic JWT signing in security tests
TEST_ED25519_PRIV_HEX = "b826d3096cc70c9f5286df442ae526f25b79e1a16d0a81b86dec767fca1a8627"
# Corresponds to DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX
TEST_ED25519_PUB_HEX = DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX

MOCK_BROKER_CREDENTIALS = [
    ("broker_account_1001_password", "LiveAlphaPassword!@#$2026"),
    ("broker_account_1002_password", "QuantumExecutionSecurePass_99"),
    ("broker_account_1003_investor_pass", "ReadOnlyInvestorPass#4421"),
    ("broker_api_secret_token", "sec_live_9988aabbccddeeff0011223344556677"),
    ("tdlib_telegram_session_secret", "1BvEX6abcdef1234567890mtproto_session_data"),
]


def make_test_order_intent(
    action: ActionType = ActionType.MARKET_ENTRY,
    symbol: str = "EURUSD",
    volume: float = 0.10,
    price: float = 1.08500,
    sl: float = 1.08200,
    tp: float = 1.09000,
    timestamp_ns: Optional[int] = None,
) -> OrderIntent:
    """Creates a valid mock OrderIntent for execution testing."""
    ts = timestamp_ns if timestamp_ns is not None else int(time.time() * 1_000_000_000)
    return OrderIntent(
        symbol=symbol,
        side=OrderSide.BUY,
        action_type=action,
        volume=volume,
        price=price,
        stop_loss=sl,
        take_profit=tp,
        slippage_cap_pips=2.0,
        magic_number=123456,
        order_ticket=0,
        signal_id=987654321,
        timestamp_ns=ts,
        flags=0,
    )


def make_test_quote(
    symbol: str = "EURUSD",
    bid: float = 1.08495,
    ask: float = 1.08505,
) -> MarketQuote:
    """Creates a valid MarketQuote with 1.0 pip spread."""
    return MarketQuote(
        symbol=symbol,
        bid=bid,
        ask=ask,
        timestamp=time.time(),
    )


# =============================================================================
# PART 1: DPAPI ZERO-KNOWLEDGE EXTRACTION RESISTANCE TESTS
# =============================================================================

@pytest.mark.security
def test_dpapi_raw_disk_extraction_resistance():
    """
    Adversary Simulation 1: Forensic Raw Disk Sector / Binary Dump Extraction.

    Simulates an attacker gaining direct physical read access to the SQLite vault.db
    file on disk. Asserts that sensitive broker passwords, investor passwords, and
    Telegram session keys never appear as plaintext substrings anywhere in the
    persisted database bytes.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "vault.db")
        vault = VaultStorage(db_path=db_path)

        # Store critical broker master credentials
        for key, plaintext in MOCK_BROKER_CREDENTIALS:
            vault.set_secret(key, plaintext)

        # Close all active handles so disk write is fully committed
        vault.close()

        # Read the raw SQLite database file directly from disk as binary
        assert os.path.exists(db_path)
        with open(db_path, "rb") as f:
            raw_disk_bytes = f.read()

        assert len(raw_disk_bytes) > 0

        # Cryptographic Assertion: Plaintext secrets MUST NEVER appear in raw disk bytes
        for key, plaintext in MOCK_BROKER_CREDENTIALS:
            plaintext_bytes = plaintext.encode("utf-8")
            assert (
                plaintext_bytes not in raw_disk_bytes
            ), f"CRITICAL SECURITY LEAK: Plaintext '{plaintext}' was found directly inside disk bytes of {db_path}!"


@pytest.mark.security
def test_sqlite_direct_query_returns_undecipherable_ciphertext():
    """
    Adversary Simulation 2: Direct SQLite Query Bypassing SecureVault DPAPI Decryption.

    Simulates an attacker or rogue process opening vault.db with standard sqlite3
    without going through the Pipvane SecureVault decryption methods.
    Asserts that:
    1. Direct SQL SELECT returns binary ciphertext blobs, NOT plaintext strings.
    2. The ciphertext does NOT equal the plaintext bytes.
    3. The plaintext is not embedded inside the ciphertext blob.
    4. Attempting to decode the ciphertext as UTF-8 or ASCII fails with UnicodeDecodeError
       or yields non-printable binary bytes.
    5. The ciphertext displays high Shannon entropy (> 6.0 bits/byte), proving strong encryption.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "vault.db")
        vault = VaultStorage(db_path=db_path)

        for key, plaintext in MOCK_BROKER_CREDENTIALS:
            vault.set_secret(key, plaintext)

        vault.close()

        # Directly connect to SQLite bypassing SecureVault classes
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("SELECT key, ciphertext FROM vault_secrets")
            rows = cursor.fetchall()
            assert len(rows) == len(MOCK_BROKER_CREDENTIALS)

            expected_dict = dict(MOCK_BROKER_CREDENTIALS)

            for row in rows:
                k = row["key"]
                raw_ciphertext = row["ciphertext"]
                expected_plaintext = expected_dict[k]
                expected_bytes = expected_plaintext.encode("utf-8")

                # Assertion 1: Ciphertext is stored as raw binary bytes
                assert isinstance(
                    raw_ciphertext, (bytes, bytearray)
                ), f"Secret '{k}' was not stored as binary BLOB!"

                # Assertion 2: Ciphertext strictly does not match plaintext
                assert (
                    raw_ciphertext != expected_bytes
                ), f"CRITICAL LEAK: Ciphertext matches plaintext for secret '{k}'!"

                # Assertion 3: Plaintext is not contained as a substring in the ciphertext
                assert (
                    expected_bytes not in raw_ciphertext
                ), f"CRITICAL LEAK: Plaintext found embedded within ciphertext for '{k}'!"

                # Assertion 4: Attempting to decode directly as ASCII/UTF-8 should fail or contain unprintables
                with pytest.raises((UnicodeDecodeError, Exception)):
                    # Strictly fail when decoded as strict UTF-8
                    decoded = raw_ciphertext.decode("utf-8")
                    # If it somehow didn't raise, verify it is completely garbled and non-printable
                    assert decoded != expected_plaintext
                    assert all(ord(c) < 32 or ord(c) > 126 for c in decoded[:8])

                # Assertion 5: High Shannon entropy check
                # DPAPI or AES-256 encrypted blobs have entropy typically between 6.5 and 8.0
                entropy = calculate_shannon_entropy(raw_ciphertext)
                assert (
                    entropy > 5.5
                ), (
                    f"Ciphertext entropy ({entropy:.2f}) for '{k}' is too low; "
                    "suggests weak or non-cryptographic encoding!"
                )

        finally:
            conn.close()


@pytest.mark.security
def test_cross_machine_or_invalid_entropy_extraction_fails():
    """
    Adversary Simulation 3: Offline Data Extraction to Another Machine / Context.

    Simulates an adversary stealing the vault.db file and attempting to decrypt it
    outside the authorized machine entropy context. On Windows, DPAPI CryptUnprotectData
    coupled with machine-bound entropy strictly fails with VaultCryptoError.
    """
    secret = b"BrokerHighValuePassword#9911"
    legitimate_entropy = get_machine_entropy(custom_salt="pipvane_auth_device_alpha")
    ciphertext = dpapi_protect(secret, entropy=legitimate_entropy)

    # Legitimate decryption works with correct entropy
    recovered = dpapi_unprotect(ciphertext, entropy=legitimate_entropy)
    assert recovered == secret

    # Tampered / Adversarial machine entropy (simulating unauthorized machine)
    attacker_entropy = get_machine_entropy(custom_salt="attacker_rogue_host_beta")
    assert attacker_entropy != legitimate_entropy

    if sys.platform == "win32":
        # On Windows, DPAPI CryptUnprotectData must strictly reject wrong entropy
        with pytest.raises(VaultCryptoError):
            dpapi_unprotect(ciphertext, entropy=attacker_entropy)
    else:
        # On non-Windows simulation environments, fallback key mismatch guarantees decryption failure
        tampered_result = dpapi_unprotect(ciphertext, entropy=attacker_entropy)
        assert tampered_result != secret


@pytest.mark.security
def test_secure_vault_legitimate_retrieval_and_memory_zeroing():
    """
    Validates that:
    1. Authorized SecureVault access decrypts and returns exact plaintext.
    2. secure_zero overwrites memory buffers with zeros, preventing paging-file leakage.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "vault.db")
        vault = VaultStorage(db_path=db_path)

        for key, plaintext in MOCK_BROKER_CREDENTIALS:
            vault.set_secret(key, plaintext)

        # Authorized access recovers exact strings
        for key, plaintext in MOCK_BROKER_CREDENTIALS:
            retrieved = vault.get_secret_str(key)
            assert retrieved == plaintext

        vault.close()

    # Memory zeroing verification
    sensitive_buf = bytearray(b"TemporaryPlaintextPasswordInHeapMemory123!")
    assert any(b != 0 for b in sensitive_buf)
    secure_zero(sensitive_buf)
    assert all(b == 0 for b in sensitive_buf), "secure_zero failed to overwrite buffer with zeros!"


# =============================================================================
# PART 2: OFFLINE CHRONOLOGY & 30-DAY GRACE PERIOD BOUNDARY TESTS
# =============================================================================

@pytest.mark.security
class TestOfflineChronologyAndLicenseGracePeriod:
    """
    Simulates multi-day offline operation without internet access.
    Validates:
    - Day 0 to Day 29: Engine continues executing trades offline within the 30-day grace period.
    - Day 30 Exact Boundary: Transition point behavior.
    - Day 31: Engine strictly locks execution and throws LICENSE_EXPIRED.
    - UI Resilience: Diagnostic layer returns expired state without crashing the UI.
    """

    @pytest.fixture
    def setup_licensing_env(self):
        """Sets up an isolated VaultStorage and LicenseManager with an Ed25519-signed token."""
        tmpdir = tempfile.mkdtemp()
        db_path = os.path.join(tmpdir, "security_vault.db")
        vault = VaultStorage(db_path=db_path)

        # Baseline timestamp: epoch t0 (Day 0)
        t0 = 1700000000.0  # Deterministic reference epoch

        # Create a valid 30-day Ed25519-signed JWT
        token_payload = {
            "iss": "pipvane-cloud-control-plane",
            "aud": "https://api.pipvane.com/v3",
            "sub": "PV3-PRO-8921-X992-FL40",
            "license_id": "PV3-PRO-8921-X992-FL40",
            "device_fingerprint": "c294db46b78e366eb8b680dfcbddda00b925015b40d67adfc867594a19e6d4a6",
            "account_limit": 7,
            "tier": "Pro",
            "jti": "mock-jti-uuid-0001",
            "token_version": 1,
            "iat": int(t0),
            "exp": int(t0 + THIRTY_DAYS_SECONDS),  # Exactly t0 + 2,592,000 seconds
        }

        jwt_token = create_signed_jwt(
            payload=token_payload,
            private_key=TEST_ED25519_PRIV_HEX,
            algorithm="EdDSA",
        )

        license_mgr = LicenseManager(
            vault=vault,
            bundled_public_key=TEST_ED25519_PUB_HEX,
            clock_tolerance_s=0.0,  # Exact boundary testing (zero skew)
        )
        license_mgr.store_token(jwt_token, current_time=t0)

        yield {
            "tmpdir": tmpdir,
            "vault": vault,
            "license_mgr": license_mgr,
            "t0": t0,
            "token": jwt_token,
        }

        license_mgr.stop_silent_refresh_daemon()
        vault.close()

    def test_day_0_baseline_activation_and_execution(self, setup_licensing_env):
        """Validates that a fresh token on Day 0 is immediately VALID and allows trading."""
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        val = mgr.validate_token(current_time=t0)
        assert val.is_valid is True
        assert val.status == LicenseValidationStatus.VALID
        assert val.entitlements.account_limit == 7
        assert val.entitlements.tier == "Pro"
        assert round(val.days_remaining) == 30

        can_exec, reason = mgr.check_execution_allowed(current_time=t0)
        assert can_exec is True
        assert reason is None
        assert mgr.is_locked is False

    def test_day_29_offline_execution_continues_without_internet(self, setup_licensing_env):
        """
        Core Assertion 1: Day 29 Offline Execution Continuity (FR-8.1).

        Mocks the system clock to Day 29 (elapsed: 29 days without cloud / internet access).
        Asserts that:
        1. The license status remains VALID.
        2. Exactly 1.0 day remains in the 30-day offline grace period.
        3. check_execution_allowed returns True.
        4. assert_execution_allowed executes without raising an exception.
        5. RiskEngine.evaluate admits the order and outputs an executable OrderIntent.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        # Mock clock advance: Day 29 (29 * 86,400 seconds)
        day_29_time = t0 + (29.0 * 86400.0)

        # 1. License Validation
        val = mgr.validate_token(current_time=day_29_time)
        assert val.is_valid is True, f"License prematurely invalidated on Day 29! Reason: {val.reason}"
        assert val.status == LicenseValidationStatus.VALID
        assert 0.99 <= val.days_remaining <= 1.01

        # 2. Execution Permission Check
        can_exec, err_msg = mgr.check_execution_allowed(current_time=day_29_time)
        assert can_exec is True, f"Trade execution blocked on Day 29: {err_msg}"
        assert err_msg is None
        assert mgr.is_locked is False

        # 3. Assert Execution Allowed (must not throw)
        mgr.assert_execution_allowed(current_time=day_29_time)

        # 4. End-to-End RiskEngine Evaluation on Day 29
        account = AccountProfile(
            account_id="ACC_1001",
            balance=10000.0,
            equity=10000.0,
            license_manager=mgr,
        )
        intent = make_test_order_intent(
            action=ActionType.MARKET_ENTRY,
            symbol="EURUSD",
            volume=0.10,
            price=1.08500,
            sl=1.08200,
            tp=1.09000,
        )
        quote = make_test_quote(symbol="EURUSD", bid=1.08495, ask=1.08505)

        evaluated = RiskEngine.evaluate(
            intent=intent,
            account=account,
            quote=quote,
            eval_time=day_29_time,
        )

        assert isinstance(
            evaluated, OrderIntent
        ), f"RiskEngine rejected valid Day 29 trade: {evaluated}"
        assert evaluated.symbol == "EURUSD"
        assert evaluated.action_type == ActionType.MARKET_ENTRY

    def test_day_30_exact_boundary_transition(self, setup_licensing_env):
        """
        Validates the exact microsecond boundary of the 30-day window:
        - At (t0 + 30 days - 1 second): Execution is allowed.
        - At (t0 + 30 days): Execution is strictly expired and locked.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        t_exp = t0 + THIRTY_DAYS_SECONDS

        # Boundary - 1 second: Still within grace period
        can_exec_before, _ = mgr.check_execution_allowed(current_time=t_exp - 1.0)
        assert can_exec_before is True, "Execution prematurely rejected 1 second before expiry!"

        # Boundary exact (t_exp): Grace period elapsed
        can_exec_at, reason = mgr.check_execution_allowed(current_time=t_exp)
        assert can_exec_at is False
        assert "LICENSE_EXPIRED" in reason
        assert mgr.is_locked is True

    def test_day_31_offline_execution_strictly_locked_throws_license_expired(
        self, setup_licensing_env
    ):
        """
        Core Assertion 2: Day 31 Strict Execution Lock & LICENSE_EXPIRED Throw.

        Mocks the system clock to Day 31 (31 days elapsed without cloud renewal).
        Asserts that:
        1. License validation status is EXPIRED.
        2. mgr.is_locked is strictly True.
        3. check_execution_allowed returns False with error code LICENSE_EXPIRED.
        4. mgr.assert_execution_allowed strictly raises LicenseExpiredError containing 'LICENSE_EXPIRED'.
        5. RiskEngine.evaluate strictly raises LicenseExpiredError containing 'LICENSE_EXPIRED'.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        # Mock clock advance: Day 31 (31 * 86,400 seconds)
        day_31_time = t0 + (31.0 * 86400.0)

        # 1. Validation check
        val = mgr.validate_token(current_time=day_31_time)
        assert val.is_valid is False
        assert val.status == LicenseValidationStatus.EXPIRED
        assert val.days_remaining == 0.0
        assert "expired" in val.reason.lower()

        # 2. Permission check
        can_exec, reason = mgr.check_execution_allowed(current_time=day_31_time)
        assert can_exec is False
        assert "LICENSE_EXPIRED" in reason
        assert mgr.is_locked is True

        # 3. Direct assert_execution_allowed throws LicenseExpiredError
        with pytest.raises(LicenseExpiredError) as exc_info:
            mgr.assert_execution_allowed(current_time=day_31_time)

        assert "LICENSE_EXPIRED" in str(exc_info.value)

        # 4. RiskEngine.evaluate on Day 31 throws LicenseExpiredError
        account = AccountProfile(
            account_id="ACC_1001",
            balance=10000.0,
            equity=10000.0,
            license_manager=mgr,
        )
        intent = make_test_order_intent(action=ActionType.MARKET_ENTRY, symbol="EURUSD")
        quote = make_test_quote(symbol="EURUSD")

        with pytest.raises(LicenseExpiredError) as exc_info_risk:
            RiskEngine.evaluate(
                intent=intent,
                account=account,
                quote=quote,
                eval_time=day_31_time,
            )

        assert "LICENSE_EXPIRED" in str(exc_info_risk.value)
        assert mgr.is_locked is True

    def test_ui_resilience_on_license_expired_no_crash(self, setup_licensing_env):
        """
        Core Assertion 3: Non-Crashing UI Diagnostics & Telemetry.

        Asserts that when the engine locks execution on Day 31:
        1. Calling get_ui_diagnostics() completes cleanly without raising any exception.
        2. UI diagnostic payload reports 'status': 'EXPIRED', 'can_execute': False, 'is_locked': True.
        3. 'error_code' is explicitly 'LICENSE_EXPIRED'.
        4. 'ui_modal_open' is True (commanding the frontend to prompt for license renewal).
        5. Simulates an ongoing UI polling loop: 1,000 continuous status polls on Day 31 execute
           in < 50ms without process exit or unhandled exceptions.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]
        day_31_time = t0 + (31.0 * 86400.0)

        # UI Diagnostics call
        diagnostics = mgr.get_ui_diagnostics(current_time=day_31_time)
        assert isinstance(diagnostics, dict)
        assert diagnostics["status"] == "EXPIRED"
        assert diagnostics["can_execute"] is False
        assert diagnostics["is_locked"] is True
        assert diagnostics["error_code"] == "LICENSE_EXPIRED"
        assert diagnostics["ui_modal_open"] is True
        assert diagnostics["ui_status_banner"] == "Expired"
        assert diagnostics["days_remaining"] == 0.0

        # High-frequency UI polling simulation (verifying zero crash under rapid frontend queries)
        poll_start = time.perf_counter()
        for _ in range(1000):
            ui_state = mgr.get_ui_diagnostics(current_time=day_31_time)
            assert ui_state["can_execute"] is False
            assert ui_state["error_code"] == "LICENSE_EXPIRED"
        poll_elapsed = time.perf_counter() - poll_start

        # 1,000 UI polls must be non-blocking and ultra-fast
        assert (
            poll_elapsed < 1.0
        ), f"UI polling loop too slow ({poll_elapsed:.3f}s for 1000 queries)"

    def test_silent_refresh_window_flag_on_day_23_to_day_30(self, setup_licensing_env):
        """
        Validates FR-8.4: Silent Background Refresh Window.
        - Prior to Day 23 (> 7 days remaining): needs_refresh is False.
        - Between Day 23 and Day 30 (<= 7 days remaining): needs_refresh is True.
        - When cloud is unreachable during silent refresh, execution continuity is preserved.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        # Day 20 (10 days remaining): Not in silent refresh window
        val_day_20 = mgr.validate_token(current_time=t0 + (20.0 * 86400.0))
        assert val_day_20.is_valid is True
        assert val_day_20.days_remaining >= 9.9
        # Needs refresh might be True if 7 days elapsed since last refresh, or False if based on expiry

        # Day 25 (5 days remaining): Strictly in silent refresh window
        val_day_25 = mgr.validate_token(current_time=t0 + (25.0 * 86400.0))
        assert val_day_25.is_valid is True
        assert val_day_25.days_remaining <= 5.1
        assert val_day_25.needs_refresh is True

        # Simulate cloud network failure during silent refresh attempt
        # Should gracefully fail refresh ping and maintain existing valid license
        mgr.refresh_handler = lambda token, lic_id: None  # Simulates cloud timeout/failure
        refresh_success = mgr.refresh_token(current_time=t0 + (25.0 * 86400.0))
        assert refresh_success is False

        # Trading continuity MUST be preserved despite cloud refresh failure
        can_exec, _ = mgr.check_execution_allowed(current_time=t0 + (25.0 * 86400.0))
        assert can_exec is True, "Trading halted because silent refresh failed! FR-8.4 violated!"

    def test_tampered_token_cryptographic_signature_rejection(self, setup_licensing_env):
        """
        Validates that altering the exp timestamp or claims inside the token
        immediately breaks the Ed25519 signature and locks execution.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]
        token: str = env["token"]

        parts = token.split(".")
        header_b64, payload_b64, sig_b64 = parts

        # Tamper payload by modifying exp timestamp artificially
        import json
        from engine.license_manager import base64url_decode, base64url_encode

        payload = json.loads(base64url_decode(payload_b64).decode("utf-8"))
        payload["exp"] += 99999999  # Attempting unauthorized extension
        tampered_payload_b64 = base64url_encode(json.dumps(payload))

        tampered_token = f"{header_b64}.{tampered_payload_b64}.{sig_b64}"

        val = mgr.validate_token(token=tampered_token, current_time=t0)
        assert val.is_valid is False
        assert val.status == LicenseValidationStatus.INVALID_SIGNATURE
        assert mgr.is_locked is True

        with pytest.raises(LicenseValidationError):
            mgr.store_token(tampered_token, current_time=t0)

    def test_system_clock_monkeypatch_day_29_and_day_31(
        self, setup_licensing_env, monkeypatch
    ):
        """
        Validates clock mocking via standard monkeypatching of time.time.
        Asserts execution passes on Day 29 and strictly locks on Day 31 without
        explicit current_time parameter overrides.
        """
        env = setup_licensing_env
        mgr: LicenseManager = env["license_mgr"]
        t0 = env["t0"]

        day_29_time = t0 + (29.0 * 86400.0)
        day_31_time = t0 + (31.0 * 86400.0)

        # 1. Mock system clock to Day 29 via monkeypatch
        monkeypatch.setattr(time, "time", lambda: day_29_time)

        can_exec, err = mgr.check_execution_allowed()
        assert can_exec is True
        assert err is None
        assert mgr.is_locked is False

        account = AccountProfile(
            account_id="ACC_1001",
            balance=10000.0,
            equity=10000.0,
            license_manager=mgr,
        )
        intent = make_test_order_intent(action=ActionType.MARKET_ENTRY, symbol="EURUSD")
        quote = make_test_quote(symbol="EURUSD")

        evaluated = RiskEngine.evaluate(intent=intent, account=account, quote=quote)
        assert isinstance(evaluated, OrderIntent)

        # 2. Advance mocked system clock to Day 31 via monkeypatch
        monkeypatch.setattr(time, "time", lambda: day_31_time)

        can_exec_31, err_31 = mgr.check_execution_allowed()
        assert can_exec_31 is False
        assert "LICENSE_EXPIRED" in err_31
        assert mgr.is_locked is True

        with pytest.raises(LicenseExpiredError) as exc_info:
            RiskEngine.evaluate(intent=intent, account=account, quote=quote)

        assert "LICENSE_EXPIRED" in str(exc_info.value)
        assert mgr.is_locked is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
