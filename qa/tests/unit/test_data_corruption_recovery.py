"""
Unit and Integration Tests for Sprint 22: Data Corruption Auto-Recovery.
=======================================================================
Verifies:
1. Nominal PRAGMA integrity_check on healthy SQLite databases.
2. WAL restoration recovering last known good state when main DB is corrupted.
3. Automated failsafe rebuild: archiving corrupted files and generating fresh DB.
4. EngineBootstrap coordination of MessageBackfill (Sprint 16) and ReconciliationLoop (Sprint 14).
5. Integration with PipvaneMasterDaemon startup.
"""

import os
import time
import shutil
import sqlite3
import pytest
import asyncio
from typing import List, Dict, Any

from engine.secure_vault import SecureVault
from engine.vault import VaultStorage
from engine.engine_bootstrap import (
    EngineBootstrap,
    DatabaseCorruptionHandler,
    DatabaseHealthStatus,
    BootstrapResult,
)
from engine.main import PipvaneMasterDaemon, MasterDaemonConfig
from engine.message_backfill import MessageBackfill, BackfillSummary
from engine.execution_orchestrator import ExecutionOrchestrator


@pytest.fixture
def test_sandbox(tmp_path):
    """Provides an isolated directory for database corruption tests."""
    sandbox = tmp_path / "sandbox"
    sandbox.mkdir()
    return sandbox


# =============================================================================
# 1. SecureVault Integrity & Recovery Tests
# =============================================================================

def test_secure_vault_nominal_integrity_check(test_sandbox):
    """Verifies that a healthy SecureVault passes PRAGMA integrity_check."""
    db_file = str(test_sandbox / "vault.db")
    with SecureVault(db_path=db_file, _test_sandbox_dir=str(test_sandbox)) as vault:
        vault.save_broker_account(
            account_id="MT5_1001",
            platform="MT5",
            broker_server="Demo-Server",
            account_login=1001,
            master_password="StrongPassword123!",
        )

    is_ok, reasons = SecureVault.check_integrity(db_file)
    assert is_ok is True
    assert reasons == ["ok"]

    # Re-open vault
    with SecureVault(db_path=db_file, _test_sandbox_dir=str(test_sandbox)) as vault:
        assert vault.recovery_performed is False
        assert vault.rebuild_occurred is False
        rec = vault.get_broker_account("MT5_1001")
        assert rec is not None
        assert rec.master_password == "StrongPassword123!"


def test_secure_vault_wal_restoration_from_intact_wal(test_sandbox):
    """
    Verifies that if main DB is corrupted but the WAL file is intact,
    SecureVault auto-recovers the last known good state via WAL Restoration.
    """
    db_file = str(test_sandbox / "vault.db")
    wal_file = f"{db_file}-wal"

    # 1. Create DB and populate data with uncheckpointed WAL frames
    vault = SecureVault(db_path=db_file, _test_sandbox_dir=str(test_sandbox))
    vault.save_broker_account(
        account_id="ACC_RECOVER_01",
        platform="MT4",
        broker_server="Broker-Live",
        account_login=999111,
        master_password="SecretPassword!",
    )
    # Ensure WAL mode and force an uncheckpointed transaction into WAL
    vault.conn.execute("PRAGMA journal_mode = WAL;")
    vault.conn.execute("PRAGMA wal_autocheckpoint = 0;")
    vault.conn.execute(
        "INSERT INTO vault_secrets (key_name, category, secret_blob, created_at, updated_at) "
        "VALUES ('wal_key', 'test', X'112233', 100, 100);"
    )
    vault.conn.commit()

    # Snapshot uncheckpointed WAL and DB
    shutil.copy2(db_file, f"{db_file}.orig")
    if os.path.exists(wal_file):
        shutil.copy2(wal_file, f"{wal_file}.orig")
    vault.close()

    # Restore uncheckpointed WAL and DB
    shutil.copy2(f"{db_file}.orig", db_file)
    if os.path.exists(f"{wal_file}.orig"):
        shutil.copy2(f"{wal_file}.orig", wal_file)

    # 2. Corrupt the main DB: overwrite header bytes
    with open(db_file, "r+b") as f:
        f.seek(0)
        f.write(b"CORRUPTED_HEADER_MAGIC_")

    # 3. Re-initialize SecureVault: should auto-detect and execute WAL restoration
    with SecureVault(db_path=db_file, _test_sandbox_dir=str(test_sandbox)) as recovered_vault:
        assert recovered_vault.recovery_performed is True
        assert recovered_vault.recovery_method == "WAL_RESTORATION"
        assert recovered_vault.rebuild_occurred is False

        # Verify data was preserved
        acc = recovered_vault.get_broker_account("ACC_RECOVER_01")
        assert acc is not None
        assert acc.master_password == "SecretPassword!"

    # Verify integrity is now OK
    is_ok, reasons = SecureVault.check_integrity(db_file)
    assert is_ok is True
    assert reasons == ["ok"]


def test_secure_vault_irrecoverable_corruption_failsafe_rebuild(test_sandbox):
    """
    Verifies that if a database is irrecoverably corrupted (random garbage, no valid WAL),
    SecureVault archives the corrupted file to .bak and initializes a clean fresh schema.
    """
    db_file = str(test_sandbox / "vault.db")

    # 1. Create corrupted file containing random unparseable garbage
    with open(db_file, "wb") as f:
        f.write(b"UNPARSEABLE_CORRUPTED_GARBAGE_PAYLOAD_" * 100)

    # 2. Initialize SecureVault: auto-recovery should trigger failsafe rebuild
    with SecureVault(db_path=db_file, _test_sandbox_dir=str(test_sandbox)) as rebuilt_vault:
        assert rebuilt_vault.recovery_performed is True
        assert rebuilt_vault.rebuild_occurred is True
        assert rebuilt_vault.recovery_method == "FAILSAFE_REBUILD"
        assert rebuilt_vault.archived_path is not None
        assert os.path.exists(rebuilt_vault.archived_path)
        assert rebuilt_vault.archived_path.endswith(".bak")

        # Verify fresh database is provisioned and functional
        is_ok, reasons = SecureVault.check_integrity(db_file)
        assert is_ok is True
        assert reasons == ["ok"]

        # Able to write and read new records immediately
        rebuilt_vault.save_broker_account(
            account_id="ACC_FRESH_01",
            platform="MT5",
            broker_server="Fresh-Server",
            account_login=555222,
            master_password="BrandNewPassword!",
        )
        acc = rebuilt_vault.get_broker_account("ACC_FRESH_01")
        assert acc is not None
        assert acc.master_password == "BrandNewPassword!"


# =============================================================================
# 2. VaultStorage Recovery Tests
# =============================================================================

def test_vault_storage_auto_recovery_and_rebuild(test_sandbox):
    """Verifies VaultStorage auto-recovery and failsafe rebuild on corruption."""
    db_file = str(test_sandbox / "vault_storage.db")

    # Corrupt with garbage
    with open(db_file, "wb") as f:
        f.write(b"CORRUPTED_VAULT_STORAGE_BYTES_" * 50)

    storage = VaultStorage(db_path=db_file)
    try:
        assert storage.recovery_performed is True
        assert storage.rebuild_occurred is True
        assert storage.recovery_method == "FAILSAFE_REBUILD"
        assert storage.archived_path is not None
        assert os.path.exists(storage.archived_path)

        # Functional fresh database
        storage.set_secret("master_key", "SuperSecretToken")
        val = storage.get_secret("master_key")
        assert val == b"SuperSecretToken"
    finally:
        storage.close()


# =============================================================================
# 3. EngineBootstrap Tests
# =============================================================================

def test_engine_bootstrap_nominal_startup_checks(test_sandbox):
    """Verifies that EngineBootstrap confirms all intact databases on nominal boot."""
    vault_db = str(test_sandbox / "vault.db")
    ledger_db = str(test_sandbox / "ledger.db")
    orch_db = str(test_sandbox / "orchestrator.db")

    # Create healthy databases
    for path in [vault_db, ledger_db, orch_db]:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE test (id INT);")
        conn.commit()
        conn.close()

    bootstrap = EngineBootstrap(
        vault_path=vault_db,
        ledger_db_path=ledger_db,
        orchestrator_db_path=orch_db,
    )
    results = bootstrap.run_startup_database_checks()

    assert len(results) == 3
    for name, status in results.items():
        assert status.is_healthy is True
        assert status.status == "INTACT"
        assert status.rebuild_occurred is False
    assert bootstrap.failsafe_rebuild_triggered is False


def test_engine_bootstrap_wal_restoration(test_sandbox):
    """Verifies that EngineBootstrap successfully restores database via WAL."""
    db_file = str(test_sandbox / "ledger.db")
    wal_file = f"{db_file}-wal"

    # Create DB in WAL mode with uncheckpointed data
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA wal_autocheckpoint = 0;")
    conn.execute("CREATE TABLE messages (id INT PRIMARY KEY, text TEXT);")
    conn.execute("INSERT INTO messages VALUES (1, 'msg1');")
    conn.commit()

    shutil.copy2(db_file, f"{db_file}.bak")
    if os.path.exists(wal_file):
        shutil.copy2(wal_file, f"{wal_file}.bak")
    conn.close()

    shutil.copy2(f"{db_file}.bak", db_file)
    if os.path.exists(f"{wal_file}.bak"):
        shutil.copy2(f"{wal_file}.bak", wal_file)

    # Corrupt main DB header bytes
    with open(db_file, "r+b") as f:
        f.seek(0)
        f.write(b"CORRUPTED_HEADER_MAGIC_")

    bootstrap = EngineBootstrap(ledger_db_path=db_file)
    results = bootstrap.run_startup_database_checks()

    assert "ledger" in results
    status = results["ledger"]
    assert status.is_healthy is True
    assert status.status == "WAL_RECOVERED"
    assert status.recovery_performed is True
    assert status.rebuild_occurred is False


@pytest.mark.asyncio
async def test_engine_bootstrap_failsafe_rebuild_triggers_backfill_and_reconciliation(test_sandbox):
    """
    Critical Task 3 Test:
    When recovery is impossible, EngineBootstrap:
    1. Archives the corrupted file.
    2. Generates fresh empty DB.
    3. Triggers MessageBackfill (Sprint 16) and ReconciliationLoop (Sprint 14)
       to completely rebuild state from Telegram and Broker.
    """
    vault_db = str(test_sandbox / "vault.db")
    orch_db = str(test_sandbox / "orchestrator.db")

    # Corrupt orchestrator database with random garbage
    with open(orch_db, "wb") as f:
        f.write(b"TOTALLY_CORRUPTED_ORCHESTRATOR_DB_NO_WAL" * 50)

    bootstrap = EngineBootstrap(
        vault_path=vault_db,
        orchestrator_db_path=orch_db,
    )
    health = bootstrap.run_startup_database_checks()

    # Verify failsafe rebuild was triggered
    assert bootstrap.failsafe_rebuild_triggered is True
    orch_status = health["orchestrator"]
    assert orch_status.rebuild_occurred is True
    assert orch_status.status == "REBUILT_FROM_SCRATCH"
    assert orch_status.archived_path is not None
    assert os.path.exists(orch_status.archived_path)
    # Corrupted file was archived and removed from active path
    assert not os.path.exists(orch_db)

    # Mock Telegram Gateway / MessageBackfill
    class MockBackfillEngine:
        def __init__(self):
            self.backfill_called = False

        def execute_backfill(self, subscribed_channels=None, reconciled_orders=None):
            self.backfill_called = True
            summary = BackfillSummary(
                total_channels=2,
                total_messages_retrieved=15,
                total_signals_parsed=5,
                total_skipped_dup=2,
                total_executable=3,
                elapsed_s=0.05,
            )
            return summary

    class MockOrchestrator:
        def __init__(self):
            self.reconcile_called = False

        async def reconcile_all_terminals(self):
            self.reconcile_called = True
            return {
                "term_mt5_01": {
                    "matched": 3,
                    "closed_externally": 0,
                    "orphans": 0,
                }
            }

    mock_bf = MockBackfillEngine()
    mock_orch = MockOrchestrator()

    # Execute failsafe rebuild state reconstruction pipeline
    res = await bootstrap.execute_failsafe_rebuild_pipeline_async(
        orchestrator=mock_orch,
        backfill_engine=mock_bf,
    )

    assert res.success is True
    assert res.failsafe_rebuild_triggered is True
    assert res.message_backfill_executed is True
    assert res.reconciliation_executed is True
    assert mock_bf.backfill_called is True
    assert mock_orch.reconcile_called is True
    assert res.backfill_summary["total_messages_retrieved"] == 15
    assert res.backfill_summary["total_parsed"] == 5
    assert "term_mt5_01" in res.reconciliation_summary


# =============================================================================
# 4. Master Daemon Integration Test
# =============================================================================

@pytest.mark.asyncio
async def test_master_daemon_integration_with_corrupted_database(test_sandbox):
    """
    Verifies that PipvaneMasterDaemon detects corrupted database on startup,
    executes auto-recovery, archives the corrupt DB, and boots cleanly.
    """
    vault_db = str(test_sandbox / "vault.db")
    ledger_db = str(test_sandbox / "ledger.db")
    orch_db = str(test_sandbox / "orchestrator.db")

    # Inject corruption into orchestrator DB
    with open(orch_db, "wb") as f:
        f.write(b"CORRUPTED_ORCH_FILE_BEFORE_DAEMON_STARTUP" * 40)

    cfg = MasterDaemonConfig(
        node_id="test_recovery_node",
        vault_path=vault_db,
        ledger_db_path=ledger_db,
        orchestrator_db_path=orch_db,
        mock_terminals=True,
        mock_telegram=True,
        strict_desktop_check=False,
    )

    daemon = PipvaneMasterDaemon(config=cfg)
    try:
        # Assert bootstrap detected corruption and archived file
        assert daemon.bootstrap.failsafe_rebuild_triggered is True
        orch_status = daemon.bootstrap_health["orchestrator"]
        assert orch_status.status == "REBUILT_FROM_SCRATCH"
        assert orch_status.archived_path is not None
        assert os.path.exists(orch_status.archived_path)

        # Start daemon cleanly
        await daemon.start()
        assert daemon._is_running is True
    finally:
        await daemon.stop()
