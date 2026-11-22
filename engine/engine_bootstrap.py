"""
Pipvane v3: Engine Bootstrap & Data Corruption Auto-Recovery Subsystem (Sprint 22).
==================================================================================
Manages startup integrity checks, WAL restoration, and failsafe state rebuilds
for all local SQLite databases (Vault, Message Ledger, and Execution Orchestrator).

Strict Requirements (Sprint 22):
1. Integrity Check: On startup, run PRAGMA integrity_check on the local SQLite vault
   and registered engine databases.
2. WAL Restoration: If the main DB is corrupted but the Write-Ahead Log (WAL) is intact,
   attempt to recover the last known good state.
3. Failsafe Rebuild: If recovery is impossible, the Engine must automatically archive
   the corrupted file, generate a fresh empty DB, and trigger the MessageBackfill (Sprint 16)
   and ReconciliationLoop (Sprint 14) to completely rebuild the state from Telegram and the Broker.
"""

from __future__ import annotations

import os
import sys
import time
import shutil
import struct
import sqlite3
import logging
import asyncio
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple, Callable, Union

from engine.secure_vault import SecureVault

logger = logging.getLogger("pipvane.engine.bootstrap")


@dataclass
class DatabaseHealthStatus:
    """Detailed health status report for a single local SQLite database."""
    db_name: str
    db_path: str
    is_healthy: bool
    status: str  # "INTACT", "WAL_RECOVERED", "REBUILT_FROM_SCRATCH", "FAILED"
    recovery_performed: bool = False
    rebuild_occurred: bool = False
    archived_path: Optional[str] = None
    integrity_output: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class BootstrapResult:
    """Overall startup result and recovery outcome across all subsystems."""
    success: bool
    databases: Dict[str, DatabaseHealthStatus] = field(default_factory=dict)
    failsafe_rebuild_triggered: bool = False
    message_backfill_executed: bool = False
    backfill_summary: Optional[Dict[str, Any]] = None
    reconciliation_executed: bool = False
    reconciliation_summary: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)


class DatabaseCorruptionHandler:
    """
    Core utility for detecting SQLite corruption, restoring state via WAL,
    and executing automated failsafe file archiving.
    """

    @classmethod
    def check_integrity(cls, db_path: str) -> Tuple[bool, List[str]]:
        """
        Runs PRAGMA integrity_check and PRAGMA quick_check on target SQLite database.
        Returns (True, ["ok"]) if healthy, or (False, [error_reasons]) if corrupted.
        """
        if not os.path.exists(db_path):
            return True, ["file_not_found"]

        if os.path.getsize(db_path) == 0:
            return True, ["empty_file"]

        # Check raw SQLite file header magic
        try:
            with open(db_path, "rb") as f:
                header_magic = f.read(16)
                if header_magic != b"SQLite format 3\x00":
                    return False, ["invalid_sqlite_header"]
        except Exception as e:
            return False, [f"file_read_error: {e}"]

        conn = None
        try:
            conn = sqlite3.connect(
                f"file:{os.path.abspath(db_path)}?mode=rw",
                uri=True,
                timeout=5.0,
                check_same_thread=False,
            )
            cur = conn.cursor()
            cur.execute("PRAGMA integrity_check;")
            rows = [r[0] for r in cur.fetchall()]
            if rows == ["ok"]:
                cur.execute("PRAGMA quick_check;")
                q_rows = [r[0] for r in cur.fetchall()]
                if q_rows == ["ok"]:
                    return True, ["ok"]
                return False, q_rows
            return False, rows
        except (sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
            return False, [str(e)]
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    @classmethod
    def restore_wal_state(cls, db_path: str) -> bool:
        """
        Sprint 22 WAL Restoration:
        If main DB is corrupted but the Write-Ahead Log (WAL) is intact,
        attempts to recover the last known good state by checkpointing
        and restoring missing pages from the WAL file.
        """
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"

        if not os.path.exists(wal_path) or os.path.getsize(wal_path) < 32:
            return False

        # Verify WAL header magic (0x377f0682 or 0x377f0683)
        try:
            with open(wal_path, "rb") as f:
                magic_bytes = f.read(4)
                if magic_bytes not in (b"\x37\x7f\x06\x82", b"\x37\x7f\x06\x83"):
                    logger.debug("WAL header magic mismatch on %s: %s", wal_path, magic_bytes)
                    return False
        except Exception:
            return False

        # Attempt 1: Direct WAL Checkpoint (TRUNCATE)
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.close()
            conn = None

            is_ok, _ = cls.check_integrity(db_path)
            if is_ok:
                logger.info("Direct WAL checkpoint successfully restored %s", db_path)
                return True
        except Exception as e:
            logger.debug("Direct WAL checkpoint failed on %s: %s", db_path, e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # Attempt 2: Page extraction from WAL if main DB was corrupted
        try:
            with open(wal_path, "rb") as f:
                hdr = f.read(32)
                magic, ver, page_size = struct.unpack(">III", hdr[:12])
                if 512 <= page_size <= 65536:
                    frame_hdr_size = 24
                    frame_total_size = frame_hdr_size + page_size
                    wal_size = os.path.getsize(wal_path)
                    offset = 32
                    wal_pages = {}
                    while offset + frame_total_size <= wal_size:
                        f.seek(offset)
                        fhdr = f.read(24)
                        pgno = struct.unpack(">I", fhdr[:4])[0]
                        wal_pages[pgno] = f.read(page_size)
                        offset += frame_total_size

                    if wal_pages:
                        max_pg = max(wal_pages.keys())
                        needed_size = max_pg * page_size
                        curr_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
                        if curr_size < needed_size:
                            with open(db_path, "ab") as dbf:
                                dbf.write(b"\x00" * (needed_size - curr_size))

                        with open(db_path, "r+b") as dbf:
                            for pgno, pdata in wal_pages.items():
                                dbf.seek((pgno - 1) * page_size)
                                dbf.write(pdata)
                            dbf.flush()

                        c = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
                        c.execute("PRAGMA journal_mode = WAL;")
                        c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                        c.close()

                        is_ok, _ = cls.check_integrity(db_path)
                        if is_ok:
                            logger.info("Page extraction from WAL successfully restored %s", db_path)
                            return True
        except Exception as e:
            logger.debug("Page extraction from WAL failed on %s: %s", db_path, e)

        # Attempt 3: Table salvage dump to temporary database
        try:
            salvage_path = f"{db_path}.salvage.tmp"
            if os.path.exists(salvage_path):
                try:
                    os.remove(salvage_path)
                except Exception:
                    pass

            src_conn = sqlite3.connect(f"file:{os.path.abspath(db_path)}?mode=ro", uri=True, timeout=5.0)
            dst_conn = sqlite3.connect(salvage_path, timeout=5.0)

            src_cur = src_conn.cursor()
            src_cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = src_cur.fetchall()
            if tables:
                for tbl_name, tbl_sql in tables:
                    if tbl_sql:
                        dst_conn.execute(tbl_sql)
                        try:
                            rows = src_conn.execute(f"SELECT * FROM {tbl_name};").fetchall()
                            if rows:
                                placeholders = ",".join(["?"] * len(rows[0]))
                                dst_conn.executemany(f"INSERT OR REPLACE INTO {tbl_name} VALUES ({placeholders})", rows)
                        except Exception:
                            pass
                dst_conn.commit()
                dst_conn.close()
                src_conn.close()

                is_ok, _ = cls.check_integrity(salvage_path)
                if is_ok:
                    for p in [db_path, wal_path, shm_path]:
                        if os.path.exists(p):
                            try:
                                os.remove(p)
                            except Exception:
                                pass
                    shutil.move(salvage_path, db_path)
                    logger.info("Table salvage dump successfully recovered %s", db_path)
                    return True
            else:
                src_conn.close()
                dst_conn.close()
        except Exception as e:
            logger.debug("Table salvage failed on %s: %s", db_path, e)
        finally:
            if os.path.exists(f"{db_path}.salvage.tmp"):
                try:
                    os.remove(f"{db_path}.salvage.tmp")
                except Exception:
                    pass

        return False

    @classmethod
    def archive_corrupted_database(cls, db_path: str) -> str:
        """
        Sprint 22 Failsafe Rebuild:
        Archives the corrupted database and companion WAL/SHM files to an archive directory,
        removes the corrupted files from the active path, and returns the archive path.
        """
        parent_dir = os.path.dirname(os.path.abspath(db_path))
        archive_dir = os.path.join(parent_dir, "corrupted_archive")
        os.makedirs(archive_dir, exist_ok=True)

        timestamp = int(time.time() * 1000)
        base_name = os.path.basename(db_path)
        archive_main = os.path.join(archive_dir, f"{base_name}.corrupt.{timestamp}.bak")

        # Archive main DB
        if os.path.exists(db_path):
            try:
                shutil.copy2(db_path, archive_main)
            except Exception as e:
                logger.error("Failed to copy corrupted DB to archive: %s", e)

        # Archive WAL and SHM if present
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"
        if os.path.exists(wal_path):
            try:
                shutil.copy2(wal_path, f"{archive_main}-wal")
            except Exception:
                pass
        if os.path.exists(shm_path):
            try:
                shutil.copy2(shm_path, f"{archive_main}-shm")
            except Exception:
                pass

        # Safely remove active corrupt files
        for p in [db_path, wal_path, shm_path]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception as e:
                    logger.warning("Could not remove corrupted active file %s: %s", p, e)

        return archive_main


class EngineBootstrap:
    """
    Startup Coordinator for Pipvane v3.
    Coordinates database verification, WAL restoration, and automated failsafe state rebuilding.
    """

    def __init__(
        self,
        vault_path: Optional[str] = None,
        ledger_db_path: Optional[str] = None,
        orchestrator_db_path: Optional[str] = None,
        custom_databases: Optional[Dict[str, str]] = None,
    ) -> None:
        self.databases: Dict[str, str] = {}
        if vault_path:
            self.databases["vault"] = os.path.abspath(vault_path)
        if ledger_db_path:
            self.databases["ledger"] = os.path.abspath(ledger_db_path)
        if orchestrator_db_path:
            self.databases["orchestrator"] = os.path.abspath(orchestrator_db_path)
        if custom_databases:
            for name, path in custom_databases.items():
                self.databases[name] = os.path.abspath(path)

        self.handler = DatabaseCorruptionHandler()
        self.health_results: Dict[str, DatabaseHealthStatus] = {}
        self.failsafe_rebuild_triggered: bool = False

    def register_database(self, name: str, db_path: str) -> None:
        """Registers a SQLite database for startup integrity checks."""
        self.databases[name] = os.path.abspath(db_path)

    def run_startup_database_checks(self) -> Dict[str, DatabaseHealthStatus]:
        """
        Executes startup PRAGMA integrity_check across all registered SQLite databases.
        If a database is corrupted:
          1. Attempts WAL restoration.
          2. If restoration fails, archives the corrupted file and prepares path for fresh schema.
        """
        results: Dict[str, DatabaseHealthStatus] = {}
        self.failsafe_rebuild_triggered = False

        for name, path in self.databases.items():
            if not os.path.exists(path):
                results[name] = DatabaseHealthStatus(
                    db_name=name,
                    db_path=path,
                    is_healthy=True,
                    status="INTACT",
                )
                continue

            is_ok, reasons = self.handler.check_integrity(path)
            if is_ok:
                results[name] = DatabaseHealthStatus(
                    db_name=name,
                    db_path=path,
                    is_healthy=True,
                    status="INTACT",
                    integrity_output=reasons,
                )
                logger.info("BOOTSTRAP: Database '%s' (%s) integrity check passed (status=OK).", name, path)
            else:
                logger.warning(
                    "BOOTSTRAP: Database '%s' (%s) corrupted! Reasons: %s. Initiating recovery...",
                    name, path, reasons,
                )
                # Step 1: WAL restoration
                wal_ok = self.handler.restore_wal_state(path)
                if wal_ok:
                    results[name] = DatabaseHealthStatus(
                        db_name=name,
                        db_path=path,
                        is_healthy=True,
                        status="WAL_RECOVERED",
                        recovery_performed=True,
                        integrity_output=["ok"],
                    )
                    logger.info("BOOTSTRAP: WAL Restoration SUCCEEDED for '%s' (%s).", name, path)
                else:
                    # Step 2: Failsafe Rebuild
                    logger.critical(
                        "BOOTSTRAP: WAL Restoration impossible/failed for '%s' (%s). Executing FAILSAFE REBUILD...",
                        name, path,
                    )
                    archived = self.handler.archive_corrupted_database(path)
                    self.failsafe_rebuild_triggered = True
                    results[name] = DatabaseHealthStatus(
                        db_name=name,
                        db_path=path,
                        is_healthy=True,
                        status="REBUILT_FROM_SCRATCH",
                        recovery_performed=True,
                        rebuild_occurred=True,
                        archived_path=archived,
                        integrity_output=reasons,
                    )

        self.health_results = results
        return results

    async def execute_failsafe_rebuild_pipeline_async(
        self,
        gateway: Optional[Any] = None,
        orchestrator: Optional[Any] = None,
        backfill_engine: Optional[Any] = None,
        subscribed_channels: Optional[List[int]] = None,
    ) -> BootstrapResult:
        """
        Asynchronous Failsafe Rebuild Pipeline (Sprint 22):
        Triggers MessageBackfill (Sprint 16) and ReconciliationLoop (Sprint 14)
        to completely reconstruct system state from Telegram and Broker terminals.
        """
        res = BootstrapResult(
            success=True,
            databases=self.health_results,
            failsafe_rebuild_triggered=self.failsafe_rebuild_triggered,
        )

        logger.info("BOOTSTRAP PIPELINE: Initiating complete state reconstruction...")

        # ---------------------------------------------------------------------
        # Step A: MessageBackfill (Sprint 16)
        # ---------------------------------------------------------------------
        bf_engine = backfill_engine
        if bf_engine is None and gateway is not None and hasattr(gateway, "get_message_backfill"):
            try:
                bf_engine = gateway.get_message_backfill()
            except Exception as e:
                logger.warning("Could not acquire MessageBackfill from gateway: %s", e)

        if bf_engine is not None and hasattr(bf_engine, "execute_backfill"):
            try:
                logger.info("BOOTSTRAP PIPELINE [Step 1/2]: Executing historical MessageBackfill from Telegram...")
                # Run synchronous execute_backfill in threadpool to avoid blocking loop
                loop = asyncio.get_running_loop()
                summary = await loop.run_in_executor(
                    None,
                    bf_engine.execute_backfill,
                    subscribed_channels,
                    None,
                )
                res.message_backfill_executed = True
                res.backfill_summary = {
                    "total_retrieved": getattr(summary, "total_messages_retrieved", 0),
                    "total_messages_retrieved": getattr(summary, "total_messages_retrieved", 0),
                    "total_parsed": getattr(summary, "total_signals_parsed", 0),
                    "total_signals_parsed": getattr(summary, "total_signals_parsed", 0),
                    "total_skipped_dup": getattr(summary, "total_skipped_dup", 0),
                    "total_executable": getattr(summary, "total_executable", getattr(summary, "total_signals_executable", 0)),
                    "elapsed_seconds": getattr(summary, "elapsed_s", getattr(summary, "elapsed_seconds", 0.0)),
                }
                logger.info(
                    "BOOTSTRAP PIPELINE: MessageBackfill complete: %d messages retrieved, %d parsed.",
                    res.backfill_summary["total_retrieved"],
                    res.backfill_summary["total_parsed"],
                )
            except Exception as e:
                logger.error("Error executing MessageBackfill during bootstrap: %s", e, exc_info=True)
                res.errors.append(f"MessageBackfill error: {e}")
        elif gateway is not None and hasattr(gateway, "execute_backfill"):
            try:
                logger.info("BOOTSTRAP PIPELINE [Step 1/2]: Triggering gateway.execute_backfill()...")
                bf_res = gateway.execute_backfill()
                res.message_backfill_executed = True
                res.backfill_summary = {"status": "executed", "result": str(bf_res)}
            except Exception as e:
                logger.error("Error running gateway backfill during bootstrap: %s", e)
                res.errors.append(f"Gateway backfill error: {e}")

        # ---------------------------------------------------------------------
        # Step B: ReconciliationLoop (Sprint 14)
        # ---------------------------------------------------------------------
        if orchestrator is not None:
            try:
                logger.info("BOOTSTRAP PIPELINE [Step 2/2]: Executing ReconciliationLoop from Broker terminals...")
                if hasattr(orchestrator, "reconcile_all_terminals"):
                    recon_res = await orchestrator.reconcile_all_terminals()
                    res.reconciliation_executed = True
                    res.reconciliation_summary = recon_res
                    logger.info(
                        "BOOTSTRAP PIPELINE: ReconciliationLoop audit completed across %d terminal(s).",
                        len(recon_res),
                    )
                elif hasattr(orchestrator, "reconciliation_engine") and hasattr(orchestrator.reconciliation_engine, "audit_all"):
                    loop = asyncio.get_running_loop()
                    recon_res = await loop.run_in_executor(None, orchestrator.reconciliation_engine.audit_all)
                    res.reconciliation_executed = True
                    res.reconciliation_summary = recon_res
            except Exception as e:
                logger.error("Error executing ReconciliationLoop during bootstrap: %s", e, exc_info=True)
                res.errors.append(f"ReconciliationLoop error: {e}")

        logger.info(
            "BOOTSTRAP PIPELINE: Auto-recovery state reconstruction finished. (Backfill=%s, Reconciliation=%s)",
            res.message_backfill_executed,
            res.reconciliation_executed,
        )
        return res

    def execute_failsafe_rebuild_pipeline(
        self,
        gateway: Optional[Any] = None,
        orchestrator: Optional[Any] = None,
        backfill_engine: Optional[Any] = None,
        subscribed_channels: Optional[List[int]] = None,
    ) -> BootstrapResult:
        """Synchronous wrapper for execute_failsafe_rebuild_pipeline_async."""
        try:
            loop = asyncio.get_running_loop()
            # If already running on loop, spawn task or run in thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(
                    asyncio.run,
                    self.execute_failsafe_rebuild_pipeline_async(
                        gateway=gateway,
                        orchestrator=orchestrator,
                        backfill_engine=backfill_engine,
                        subscribed_channels=subscribed_channels,
                    ),
                )
                return fut.result()
        except RuntimeError:
            return asyncio.run(
                self.execute_failsafe_rebuild_pipeline_async(
                    gateway=gateway,
                    orchestrator=orchestrator,
                    backfill_engine=backfill_engine,
                    subscribed_channels=subscribed_channels,
                )
            )
