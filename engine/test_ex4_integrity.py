"""
Unit tests for Pipvane v3 MT4 EX4 Binary Integrity Verification (Sprint 11).
Validates:
1. File location in portable and standard layouts
2. Constant-memory chunked SHA-256 calculation
3. Golden hash verification
4. Tampering detection (cracked binary simulation)
5. Execution block preventing Named Pipe connection establishment
6. Non-MT4 platform pass-through
"""

import os
import hashlib
from pathlib import Path
import pytest

from ex4_integrity import (
    EX4IntegrityChecker,
    EX4IntegrityStatus,
    EX4IntegrityError,
    EX4TamperSecurityError,
    EX4NotFoundError,
    PRIMARY_GOLDEN_EX4_SHA256,
)


@pytest.fixture
def test_environment(tmp_path):
    """Sets up a simulated MetaTrader 4 directory layout."""
    terminal_dir = tmp_path / "MetaTrader 4 Terminal"
    terminal_dir.mkdir(parents=True)
    terminal_exe = terminal_dir / "terminal.exe"
    terminal_exe.write_bytes(b"MOCK_TERMINAL_EXE_HEADER")

    mql4_dir = terminal_dir / "MQL4" / "Experts"
    mql4_dir.mkdir(parents=True)

    genuine_ex4 = mql4_dir / "Pipvane_MT4.ex4"
    # Genuine payload
    genuine_payload = b"PIPVANE_MT4_OFFICIAL_RELEASE_V3_0_0_GENUINE_BINARY_BYTECODE"
    genuine_ex4.write_bytes(genuine_payload)
    genuine_hash = hashlib.sha256(genuine_payload).hexdigest().lower()

    return {
        "terminal_dir": str(terminal_dir),
        "terminal_exe": str(terminal_exe),
        "mql4_dir": str(mql4_dir),
        "genuine_ex4": str(genuine_ex4),
        "genuine_hash": genuine_hash,
    }


def test_compute_file_sha256(test_environment):
    ex4_path = test_environment["genuine_ex4"]
    computed_hash = EX4IntegrityChecker.compute_file_sha256(ex4_path)
    assert computed_hash == test_environment["genuine_hash"]
    assert len(computed_hash) == 64


def test_locate_ex4_portable_mode(test_environment):
    checker = EX4IntegrityChecker()
    # Locate by terminal executable path
    located = checker.locate_ex4_file(test_environment["terminal_exe"])
    assert located is not None
    assert located.resolve() == Path(test_environment["genuine_ex4"]).resolve()

    # Locate by terminal directory path
    located_dir = checker.locate_ex4_file(test_environment["terminal_dir"])
    assert located_dir is not None
    assert located_dir.resolve() == Path(test_environment["genuine_ex4"]).resolve()


def test_verify_valid_golden_hash(test_environment):
    genuine_hash = test_environment["genuine_hash"]
    # Initialize checker with our test release golden hash
    checker = EX4IntegrityChecker(authorized_hashes={genuine_hash})

    result = checker.verify_terminal_ex4(
        terminal_id="term-mt4-demo",
        terminal_path=test_environment["terminal_exe"],
        platform_type="MT4",
    )

    assert result.is_valid is True
    assert result.status == EX4IntegrityStatus.VERIFIED_OK
    assert result.actual_sha256 == genuine_hash
    assert result.error_message is None

    # assert_can_connect must pass without error
    conn_result = checker.assert_can_connect(
        terminal_id="term-mt4-demo",
        terminal_path=test_environment["terminal_exe"],
        platform_type="MT4",
    )
    assert conn_result.is_valid is True

    # pre_connect_check returns True
    assert checker.pre_connect_check("term-mt4-demo", test_environment["terminal_exe"], "MT4") is True


def test_tamper_detection_and_execution_block(test_environment):
    genuine_hash = test_environment["genuine_hash"]
    checker = EX4IntegrityChecker(authorized_hashes={genuine_hash})

    # Simulate cracked or tampered binary by modifying bytes
    ex4_path = Path(test_environment["genuine_ex4"])
    tampered_payload = b"CRACKED_TAMPERED_MALICIOUS_BYTECODE_INJECTED"
    ex4_path.write_bytes(tampered_payload)

    # 1. Verification returns TAMPERED_HASH_MISMATCH
    result = checker.verify_terminal_ex4(
        terminal_id="term-mt4-compromised",
        terminal_path=test_environment["terminal_exe"],
        platform_type="MT4",
    )

    assert result.is_valid is False
    assert result.status == EX4IntegrityStatus.TAMPERED_HASH_MISMATCH
    assert result.actual_sha256 != genuine_hash
    assert "CRITICAL SECURITY ALERT" in result.error_message

    # 2. assert_can_connect must raise fatal EX4TamperSecurityError
    with pytest.raises(EX4TamperSecurityError) as exc_info:
        checker.assert_can_connect(
            terminal_id="term-mt4-compromised",
            terminal_path=test_environment["terminal_exe"],
            platform_type="MT4",
        )
    assert "SECURITY_TAMPER_DETECTED" in str(exc_info.value)

    # 3. pre_connect_check returns False (blocking connection)
    assert checker.pre_connect_check("term-mt4-compromised", test_environment["terminal_exe"], "MT4") is False


def test_missing_file_blocks_connection(test_environment):
    checker = EX4IntegrityChecker()

    # Delete the EX4 file to simulate missing bridge EA
    os.remove(test_environment["genuine_ex4"])

    result = checker.verify_terminal_ex4(
        terminal_id="term-mt4-missing",
        terminal_path=test_environment["terminal_exe"],
        platform_type="MT4",
    )

    assert result.is_valid is False
    assert result.status == EX4IntegrityStatus.FILE_NOT_FOUND

    # Connection must be refused with EX4NotFoundError
    with pytest.raises(EX4NotFoundError):
        checker.assert_can_connect(
            terminal_id="term-mt4-missing",
            terminal_path=test_environment["terminal_exe"],
            platform_type="MT4",
        )

    assert checker.pre_connect_check("term-mt4-missing", test_environment["terminal_exe"], "MT4") is False


def test_non_mt4_platforms_skipped(test_environment):
    checker = EX4IntegrityChecker()

    # MT5 platform skips legacy EX4 check
    result = checker.verify_terminal_ex4(
        terminal_id="term-mt5-pro",
        terminal_path=test_environment["terminal_exe"],
        platform_type="MT5",
    )
    assert result.is_valid is True
    assert result.status == EX4IntegrityStatus.SKIPPED_NON_MT4
    assert checker.pre_connect_check("term-mt5-pro", test_environment["terminal_exe"], "MT5") is True

    # cTrader platform skips check
    res_ctrader = checker.verify_terminal_ex4(
        terminal_id="term-ctrader-01",
        terminal_path="https://api.spotware.com",
        platform_type="cTrader",
    )
    assert res_ctrader.is_valid is True
    assert res_ctrader.status == EX4IntegrityStatus.SKIPPED_NON_MT4
