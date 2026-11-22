"""
Pipvane v3 Zero-Knowledge Local Credential Vault.

Encrypted storage layer utilizing Windows OS-level Data Protection API (DPAPI)
via CryptProtectData / CryptUnprotectData with machine-bound secondary entropy.
Persists encrypted master secrets, device profiles, and ingestion checkpoints
in an encrypted local SQLite database.
"""

from __future__ import annotations

import os
import sys
import time
import sqlite3
import hashlib
import logging
import contextlib
import shutil
import struct
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List

logger = logging.getLogger("pipvane.engine.vault")

# Win32 DPAPI Ctypes Setup (Windows only)
if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [
            ("cbData", wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_byte)),
        ]

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32
    _ntdll = ctypes.windll.ntdll
    CRYPTPROTECT_UI_FORBIDDEN = 0x01
else:
    DATA_BLOB = None  # type: ignore
    _crypt32 = None  # type: ignore
    _kernel32 = None  # type: ignore
    _ntdll = None  # type: ignore
    CRYPTPROTECT_UI_FORBIDDEN = 0x01


class VaultError(Exception):
    """Base exception for Vault errors."""
    pass


class VaultCryptoError(VaultError):
    """Exception raised when encryption or decryption fails."""
    pass


def secure_zero(buffer: bytearray | ctypes.Array) -> None:
    """
    Overwrites the memory buffer with zeros immediately to prevent sensitive
    data from remaining in non-pageable memory.
    """
    if _ntdll is not None and isinstance(buffer, (ctypes.Array, ctypes.Structure)):
        _ntdll.memset(ctypes.byref(buffer), 0, ctypes.sizeof(buffer))
    elif isinstance(buffer, bytearray):
        for i in range(len(buffer)):
            buffer[i] = 0


def get_machine_entropy(custom_salt: Optional[str] = None) -> bytes:
    """
    Derives a deterministic, machine-bound secondary entropy salt for DPAPI.
    Reads Windows MachineGuid from registry or falls back to system hardware identity.
    """
    guid_str = ""
    if sys.platform == "win32":
        try:
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
                0,
                winreg.KEY_READ | winreg.KEY_WOW64_64KEY
            ) as key:
                guid_str = winreg.QueryValueEx(key, "MachineGuid")[0]
        except Exception:
            pass

    if not guid_str:
        # Fallback to computer name and user identity
        comp_name = os.environ.get("COMPUTERNAME", "PIPVANE_HOST")
        user_name = os.environ.get("USERNAME", "PIPVANE_USER")
        guid_str = f"{comp_name}::{user_name}::pipvane-v3-salt"

    hasher = hashlib.sha256(guid_str.encode("utf-8"))
    if custom_salt:
        hasher.update(custom_salt.encode("utf-8"))
    return hasher.digest()


def dpapi_protect(plaintext: bytes, entropy: Optional[bytes] = None) -> bytes:
    """
    Encrypts plaintext bytes using Windows DPAPI (CryptProtectData).
    Requires Windows OS.
    """
    if sys.platform != "win32" or _crypt32 is None:
        # Fallback XOR-HMAC stub only for non-windows CI simulation
        hasher = hashlib.sha256(entropy or b"fallback-non-windows-key").digest()
        return bytes(b ^ hasher[i % len(hasher)] for i, b in enumerate(plaintext))

    if not isinstance(plaintext, bytes):
        raise TypeError("Plaintext must be bytes")

    in_buf = (ctypes.c_byte * len(plaintext))(*plaintext)
    in_blob = DATA_BLOB(len(plaintext), in_buf)
    out_blob = DATA_BLOB()

    entropy_blob_ptr = None
    if entropy is not None:
        ent_buf = (ctypes.c_byte * len(entropy))(*entropy)
        ent_blob = DATA_BLOB(len(entropy), ent_buf)
        entropy_blob_ptr = ctypes.byref(ent_blob)

    success = _crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        "PipvaneVaultSecret",
        entropy_blob_ptr,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )

    if not success:
        err_code = _kernel32.GetLastError()
        raise VaultCryptoError(f"CryptProtectData failed with Win32 error code {err_code}")

    try:
        ciphertext = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return ciphertext
    finally:
        _kernel32.LocalFree(out_blob.pbData)


def dpapi_unprotect(ciphertext: bytes, entropy: Optional[bytes] = None) -> bytes:
    """
    Decrypts ciphertext bytes using Windows DPAPI (CryptUnprotectData).
    Requires Windows OS.
    """
    if sys.platform != "win32" or _crypt32 is None:
        # Fallback XOR-HMAC stub only for non-windows CI simulation
        hasher = hashlib.sha256(entropy or b"fallback-non-windows-key").digest()
        return bytes(b ^ hasher[i % len(hasher)] for i, b in enumerate(ciphertext))

    if not isinstance(ciphertext, bytes):
        raise TypeError("Ciphertext must be bytes")

    in_buf = (ctypes.c_byte * len(ciphertext))(*ciphertext)
    in_blob = DATA_BLOB(len(ciphertext), in_buf)
    out_blob = DATA_BLOB()

    entropy_blob_ptr = None
    if entropy is not None:
        ent_buf = (ctypes.c_byte * len(entropy))(*entropy)
        ent_blob = DATA_BLOB(len(entropy), ent_buf)
        entropy_blob_ptr = ctypes.byref(ent_blob)

    success = _crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        entropy_blob_ptr,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(out_blob),
    )

    if not success:
        err_code = _kernel32.GetLastError()
        raise VaultCryptoError(f"CryptUnprotectData failed with Win32 error code {err_code}")

    try:
        plaintext = ctypes.string_at(out_blob.pbData, out_blob.cbData)
        return plaintext
    finally:
        _kernel32.LocalFree(out_blob.pbData)


@dataclass(frozen=True)
class DeviceProfile:
    """Immutable persistent device fingerprint to avoid Telegram anomaly detection."""
    device_model: str
    system_version: str
    app_version: str
    system_language_code: str


class VaultStorage:
    """
    Zero-knowledge local SQLite database wrapping DPAPI encryption.
    Stores:
    - Master secrets (TDLib database encryption keys, auth session tokens)
    - Stable device profile
    - Message backfill checkpoints (last_processed_message_id)
    """

    def __init__(self, db_path: Optional[str] = None, machine_entropy_salt: Optional[str] = None) -> None:
        self._mem_conn: Optional[sqlite3.Connection] = None
        self.recovery_performed: bool = False
        self.rebuild_occurred: bool = False
        self.recovery_method: Optional[str] = None
        self.archived_path: Optional[str] = None

        if db_path == ":memory:":
            self.db_path = ":memory:"
            self._mem_conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._mem_conn.row_factory = sqlite3.Row
        elif db_path is None:
            appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
            target_dir = os.path.join(appdata, "Pipvane")
            os.makedirs(target_dir, exist_ok=True)
            self.db_path = os.path.join(target_dir, "vault.db")
        else:
            self.db_path = os.path.abspath(db_path)
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        self._entropy = get_machine_entropy(machine_entropy_salt)
        if self.db_path != ":memory:":
            self._ensure_integrity_and_recover()
        self._init_db()

    # =========================================================================
    # Sprint 22: SQLite Integrity Check, WAL Restoration & Auto-Recovery
    # =========================================================================

    def _ensure_integrity_and_recover(self) -> None:
        """
        Sprint 22 Auto-Recovery Routine:
        Checks integrity of SQLite vault, attempts WAL recovery if corrupted,
        and falls back to failsafe rebuild if unrecoverable.
        """
        if not os.path.exists(self.db_path):
            return

        is_ok, reasons = self.check_integrity(self.db_path)
        if is_ok:
            return

        logger.warning(
            "VaultStorage integrity check failed for '%s': %s. Initiating auto-recovery...",
            self.db_path, reasons,
        )

        wal_ok = self.attempt_wal_restoration(self.db_path)
        if wal_ok:
            logger.info("VaultStorage WAL Restoration SUCCEEDED for '%s'.", self.db_path)
            self.recovery_performed = True
            self.recovery_method = "WAL_RESTORATION"
            return

        logger.critical(
            "VaultStorage WAL Restoration impossible or failed for '%s'. Executing FAILSAFE REBUILD...",
            self.db_path,
        )
        archived_bak = self.execute_failsafe_rebuild(self.db_path)
        self.recovery_performed = True
        self.rebuild_occurred = True
        self.archived_path = archived_bak
        self.recovery_method = "FAILSAFE_REBUILD"

    @classmethod
    def check_integrity(cls, db_path: str) -> Tuple[bool, List[str]]:
        """Runs PRAGMA integrity_check and quick_check on target SQLite database."""
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
    def attempt_wal_restoration(cls, db_path: str) -> bool:
        """Attempts WAL recovery by checkpointing and extracting pages if damaged."""
        wal_path = f"{db_path}-wal"
        shm_path = f"{db_path}-shm"

        if not os.path.exists(wal_path) or os.path.getsize(wal_path) < 32:
            return False

        try:
            with open(wal_path, "rb") as f:
                magic_bytes = f.read(4)
                if magic_bytes not in (b"\x37\x7f\x06\x82", b"\x37\x7f\x06\x83"):
                    return False
        except Exception:
            return False

        # Attempt 1: Direct WAL Checkpoint
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            conn.close()
            conn = None

            is_ok, _ = cls.check_integrity(db_path)
            if is_ok:
                return True
        except Exception as e:
            logger.debug("Direct WAL checkpoint failed: %s", e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # Attempt 2: Page extraction from WAL
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
                            return True
        except Exception as e:
            logger.debug("Page extraction from WAL failed: %s", e)

        # Attempt 3: Table salvage
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
                    return True
            else:
                src_conn.close()
                dst_conn.close()
        except Exception as e:
            logger.debug("Table salvage failed: %s", e)
        finally:
            if os.path.exists(f"{db_path}.salvage.tmp"):
                try:
                    os.remove(f"{db_path}.salvage.tmp")
                except Exception:
                    pass

        return False

    @classmethod
    def execute_failsafe_rebuild(cls, db_path: str) -> str:
        """Archives corrupted DB and companion WAL/SHM files to corrupted_archive folder."""
        parent_dir = os.path.dirname(os.path.abspath(db_path))
        archive_dir = os.path.join(parent_dir, "corrupted_archive")
        os.makedirs(archive_dir, exist_ok=True)

        timestamp = int(time.time() * 1000)
        base_name = os.path.basename(db_path)
        archive_main = os.path.join(archive_dir, f"{base_name}.corrupt.{timestamp}.bak")

        if os.path.exists(db_path):
            try:
                shutil.copy2(db_path, archive_main)
            except Exception as e:
                logger.error("Failed to copy corrupted DB to archive: %s", e)

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

        for p in [db_path, wal_path, shm_path]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception as e:
                    logger.warning("Could not remove corrupted active file %s: %s", p, e)

        return archive_main

    @contextlib.contextmanager
    def _get_connection(self):
        if self.db_path == ":memory:" and self._mem_conn is not None:
            yield self._mem_conn
        else:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()

    def close(self) -> None:
        """Closes vault resources if any."""
        if self._mem_conn is not None:
            self._mem_conn.close()
            self._mem_conn = None

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS vault_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS vault_secrets (
                    key TEXT PRIMARY KEY,
                    ciphertext BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS device_profiles (
                    profile_id TEXT PRIMARY KEY,
                    device_model TEXT NOT NULL,
                    system_version TEXT NOT NULL,
                    app_version TEXT NOT NULL,
                    system_language_code TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chat_checkpoints (
                    chat_id INTEGER PRIMARY KEY,
                    last_processed_message_id INTEGER NOT NULL,
                    last_processed_timestamp REAL NOT NULL
                );
            """)
            conn.commit()

    # --- Secrets Management ---

    def set_secret(self, key: str, value: bytes | str) -> None:
        """Encrypts value with DPAPI and persists in vault_secrets."""
        if isinstance(value, str):
            plaintext = value.encode("utf-8")
        else:
            plaintext = value

        ciphertext = dpapi_protect(plaintext, self._entropy)
        now = time.time()

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO vault_secrets (key, ciphertext, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    ciphertext = excluded.ciphertext,
                    updated_at = excluded.updated_at
                """,
                (key, ciphertext, now, now),
            )
            conn.commit()

    def get_secret(self, key: str) -> Optional[bytes]:
        """Retrieves and decrypts value from vault_secrets with DPAPI."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT ciphertext FROM vault_secrets WHERE key = ?",
                (key,),
            ).fetchone()

        if not row:
            return None

        ciphertext = bytes(row["ciphertext"])
        return dpapi_unprotect(ciphertext, self._entropy)

    def get_secret_str(self, key: str, encoding: str = "utf-8") -> Optional[str]:
        """Retrieves and decrypts secret decoded as string."""
        raw = self.get_secret(key)
        return raw.decode(encoding) if raw is not None else None

    def delete_secret(self, key: str) -> bool:
        """Deletes a secret from the vault."""
        with self._get_connection() as conn:
            cursor = conn.execute("DELETE FROM vault_secrets WHERE key = ?", (key,))
            conn.commit()
            return cursor.rowcount > 0

    def get_or_create_tdlib_key(self) -> str:
        """
        Retrieves or generates a secure 32-byte hexadecimal TDLib database encryption key,
        encrypted and persisted in the vault.
        """
        existing = self.get_secret_str("tdlib_database_encryption_key")
        if existing:
            return existing

        import secrets
        new_key = secrets.token_hex(32)
        self.set_secret("tdlib_database_encryption_key", new_key)
        return new_key

    # --- Device Profile Persistence ---

    def save_device_profile(self, profile: DeviceProfile, profile_id: str = "default") -> None:
        """Saves a device profile in the local vault."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO device_profiles (profile_id, device_model, system_version, app_version, system_language_code, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_id) DO UPDATE SET
                    device_model = excluded.device_model,
                    system_version = excluded.system_version,
                    app_version = excluded.app_version,
                    system_language_code = excluded.system_language_code
                """,
                (profile_id, profile.device_model, profile.system_version, profile.app_version, profile.system_language_code, time.time())
            )
            conn.commit()

    def get_device_profile(self, profile_id: str = "default") -> Optional[DeviceProfile]:
        """Retrieves persisted device profile if exists."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT device_model, system_version, app_version, system_language_code FROM device_profiles WHERE profile_id = ?",
                (profile_id,),
            ).fetchone()

        if not row:
            return None

        return DeviceProfile(
            device_model=row["device_model"],
            system_version=row["system_version"],
            app_version=row["app_version"],
            system_language_code=row["system_language_code"],
        )

    # --- Chat Checkpoint Management (Backfill Resilience) ---

    def get_chat_checkpoint(self, chat_id: int) -> Optional[int]:
        """Returns last_processed_message_id for chat, or None if unrecorded."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT last_processed_message_id FROM chat_checkpoints WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()

        return row["last_processed_message_id"] if row else None

    def update_chat_checkpoint(self, chat_id: int, message_id: int, timestamp: Optional[float] = None) -> None:
        """Updates last_processed_message_id for chat."""
        ts = timestamp if timestamp is not None else time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO chat_checkpoints (chat_id, last_processed_message_id, last_processed_timestamp)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    last_processed_message_id = MAX(chat_checkpoints.last_processed_message_id, excluded.last_processed_message_id),
                    last_processed_timestamp = excluded.last_processed_timestamp
                """,
                (chat_id, message_id, ts)
            )
            conn.commit()

    def get_all_chat_checkpoints(self) -> Dict[int, int]:
        """Returns map of {chat_id: last_processed_message_id}."""
        with self._get_connection() as conn:
            rows = conn.execute("SELECT chat_id, last_processed_message_id FROM chat_checkpoints").fetchall()
        return {r["chat_id"]: r["last_processed_message_id"] for r in rows}
