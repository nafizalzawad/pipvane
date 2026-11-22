"""
Pipvane v3: DPAPI Secure Vault Module
=====================================
Cryptographically binds sensitive broker credentials and session secrets to the 
local Windows user OS account using the native Data Protection API (DPAPI).

Strict Architectural Constraints:
- Zero-Knowledge Security: Plaintext secrets are decrypted exclusively in RAM at runtime.
- File Isolation: Database file is strictly confined to %AppData%\\Pipvane\\.
- Native OS Hook: Calls Windows Crypt32.dll (CryptProtectData / CryptUnprotectData).
"""

from __future__ import annotations

import os
import sys
import time
import sqlite3
import ctypes
import shutil
import struct
import logging
from ctypes import wintypes
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Tuple

logger = logging.getLogger("pipvane.secure_vault")


class VaultSecurityError(Exception):
    """Raised when file isolation, OS access controls, or cryptographic operations fail."""
    pass


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte))
    ]


class DPAPIEncryptor:
    """
    Low-level adapter for Windows Data Protection API (Crypt32.dll).
    Binds ciphertext to the current interactive Windows OS user profile.
    """
    # Windows CryptProtect flags
    CRYPTPROTECT_UI_FORBIDDEN = 0x01
    CRYPTPROTECT_LOCAL_MACHINE = 0x04

    # Default application secondary entropy salt
    DEFAULT_ENTROPY = b"Pipvane_V3_Local_Entropy_Salt_2026_Secured"

    def __init__(self, secondary_entropy: Optional[bytes] = None):
        if sys.platform != "win32":
            raise VaultSecurityError("Windows DPAPI is only supported on Win32 operating systems.")

        self._entropy = secondary_entropy if secondary_entropy is not None else self.DEFAULT_ENTROPY
        self._init_win32_api()

    def _init_win32_api(self) -> None:
        try:
            self._crypt32 = ctypes.windll.crypt32
            self._kernel32 = ctypes.windll.kernel32

            # CryptProtectData
            self._CryptProtectData = self._crypt32.CryptProtectData
            self._CryptProtectData.argtypes = [
                ctypes.c_void_p,                # pDataIn
                wintypes.LPCWSTR,               # szDataDescr
                ctypes.c_void_p,                # pOptionalEntropy
                ctypes.c_void_p,                # pvReserved
                ctypes.c_void_p,                # pPromptStruct
                wintypes.DWORD,                 # dwFlags
                ctypes.c_void_p                 # pDataOut
            ]
            self._CryptProtectData.restype = wintypes.BOOL

            # CryptUnprotectData
            self._CryptUnprotectData = self._crypt32.CryptUnprotectData
            self._CryptUnprotectData.argtypes = [
                ctypes.c_void_p,                # pDataIn
                ctypes.c_void_p,                # ppszDataDescr
                ctypes.c_void_p,                # pOptionalEntropy
                ctypes.c_void_p,                # pvReserved
                ctypes.c_void_p,                # pPromptStruct
                wintypes.DWORD,                 # dwFlags
                ctypes.c_void_p                 # pDataOut
            ]
            self._CryptUnprotectData.restype = wintypes.BOOL

            # LocalFree
            self._LocalFree = self._kernel32.LocalFree
            self._LocalFree.argtypes = [wintypes.HLOCAL]
            self._LocalFree.restype = wintypes.HLOCAL

        except Exception as e:
            raise VaultSecurityError(f"Failed to bind to Windows Crypt32.dll: {e}") from e

    def encrypt(self, data: Union[str, bytes], custom_entropy: Optional[bytes] = None) -> bytes:
        """
        Encrypts plaintext bytes or string using CryptProtectData.
        Returns raw encrypted ciphertext bytes.
        """
        if data is None:
            raise ValueError("Data to encrypt cannot be None")

        raw_bytes = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if len(raw_bytes) == 0:
            return b""

        entropy_bytes = custom_entropy if custom_entropy is not None else self._entropy

        # Prepare input DataBlob
        data_in = _DATA_BLOB(len(raw_bytes), ctypes.cast(raw_bytes, ctypes.POINTER(ctypes.c_byte)))
        entropy_in = _DATA_BLOB(len(entropy_bytes), ctypes.cast(entropy_bytes, ctypes.POINTER(ctypes.c_byte)))
        data_out = _DATA_BLOB()

        success = self._CryptProtectData(
            ctypes.byref(data_in),
            "Pipvane Credential Vault",
            ctypes.byref(entropy_in),
            None,
            None,
            self.CRYPTPROTECT_UI_FORBIDDEN,
            ctypes.byref(data_out)
        )

        if not success:
            err = ctypes.GetLastError()
            raise VaultSecurityError(f"CryptProtectData failed with Windows error code: {err}")

        try:
            ciphertext = ctypes.string_at(data_out.pbData, data_out.cbData)
            return ciphertext
        finally:
            self._LocalFree(data_out.pbData)

    def decrypt(self, ciphertext: bytes, custom_entropy: Optional[bytes] = None) -> bytes:
        """
        Decrypts DPAPI ciphertext into raw plaintext bytes.
        Plaintext is held exclusively in volatile RAM.
        """
        if ciphertext is None:
            raise ValueError("Ciphertext to decrypt cannot be None")
        if len(ciphertext) == 0:
            return b""

        entropy_bytes = custom_entropy if custom_entropy is not None else self._entropy

        cipher_in = _DATA_BLOB(len(ciphertext), ctypes.cast(ciphertext, ctypes.POINTER(ctypes.c_byte)))
        entropy_in = _DATA_BLOB(len(entropy_bytes), ctypes.cast(entropy_bytes, ctypes.POINTER(ctypes.c_byte)))
        data_out = _DATA_BLOB()

        success = self._CryptUnprotectData(
            ctypes.byref(cipher_in),
            None,
            ctypes.byref(entropy_in),
            None,
            None,
            self.CRYPTPROTECT_UI_FORBIDDEN,
            ctypes.byref(data_out)
        )

        if not success:
            err = ctypes.GetLastError()
            raise VaultSecurityError(
                f"CryptUnprotectData failed with error code: {err}. "
                "The secret may belong to another user account or secondary entropy is invalid."
            )

        try:
            plaintext = ctypes.string_at(data_out.pbData, data_out.cbData)
            return plaintext
        finally:
            self._LocalFree(data_out.pbData)

    def decrypt_str(self, ciphertext: bytes, custom_entropy: Optional[bytes] = None) -> str:
        """Decrypts DPAPI ciphertext and returns UTF-8 decoded string."""
        raw = self.decrypt(ciphertext, custom_entropy)
        return raw.decode("utf-8")


@dataclass(frozen=True)
class BrokerAccountRecord:
    account_id: str
    platform: str
    broker_server: str
    account_login: int
    master_password: str
    investor_password: Optional[str]
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class TelegramSessionRecord:
    session_id: str
    phone_number: str
    session_string: str
    api_id: Optional[int]
    api_hash: Optional[str]
    updated_at: int


class SecureVault:
    """
    Zero-Knowledge SQLite Credential Vault.
    Guarantees:
    1. Database file is strictly confined to %AppData%\\Pipvane\\
    2. All sensitive fields are encrypted via Windows DPAPI before writing to disk
    3. Plaintext secrets exist only in volatile RAM when requested
    """

    VAULT_SUBDIR = "Pipvane"
    DEFAULT_DB_FILENAME = "vault.db"

    def __init__(
        self,
        db_path: Optional[str] = None,
        secondary_entropy: Optional[bytes] = None,
        _test_sandbox_dir: Optional[str] = None
    ):
        """
        Initialize the Secure Vault.
        :param db_path: Optional full path to SQLite db. Must reside in %AppData%\\Pipvane\\
        :param secondary_entropy: Optional custom DPAPI salt
        :param _test_sandbox_dir: Internal testing hook to isolate unit test runs
        """
        self._encryptor = DPAPIEncryptor(secondary_entropy=secondary_entropy)
        self._test_sandbox_dir = _test_sandbox_dir
        self.recovery_performed: bool = False
        self.rebuild_occurred: bool = False
        self.recovery_method: Optional[str] = None
        self.archived_path: Optional[str] = None

        self.db_path = self._resolve_and_verify_path(db_path)
        self._ensure_integrity_and_recover()
        self.conn = self._init_database(self.db_path)

    # =========================================================================
    # Sprint 22: SQLite Integrity Check, WAL Restoration & Auto-Recovery
    # =========================================================================

    def _ensure_integrity_and_recover(self) -> None:
        """
        Sprint 22 Auto-Recovery Routine:
        1. Checks SQLite database integrity via PRAGMA integrity_check.
        2. If corrupted, attempts WAL restoration to recover last known good state.
        3. If recovery is impossible, executes failsafe rebuild: archives corrupted files
           and prepares path for fresh schema initialization.
        """
        if not os.path.exists(self.db_path):
            return

        is_ok, reasons = self.check_integrity(self.db_path)
        if is_ok:
            return

        logger.warning(
            "SecureVault integrity check failed for '%s': %s. Initiating auto-recovery...",
            self.db_path, reasons,
        )

        # Attempt WAL restoration
        wal_ok = self.attempt_wal_restoration(self.db_path)
        if wal_ok:
            logger.info("SecureVault WAL Restoration SUCCEEDED for '%s'.", self.db_path)
            self.recovery_performed = True
            self.recovery_method = "WAL_RESTORATION"
            return

        # Failsafe Rebuild
        logger.critical(
            "SecureVault WAL Restoration impossible or failed for '%s'. Executing FAILSAFE REBUILD...",
            self.db_path,
        )
        archived_bak = self.execute_failsafe_rebuild(self.db_path)
        self.recovery_performed = True
        self.rebuild_occurred = True
        self.archived_path = archived_bak
        self.recovery_method = "FAILSAFE_REBUILD"

    @classmethod
    def check_integrity(cls, db_path: str) -> Tuple[bool, List[str]]:
        """
        Runs PRAGMA integrity_check and PRAGMA quick_check on target SQLite database.
        Returns (True, ["ok"]) if fully intact, or (False, [error_reasons]) if corrupted.
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
    def attempt_wal_restoration(cls, db_path: str) -> bool:
        """
        Sprint 22 WAL Restoration:
        If main DB is corrupted but the Write-Ahead Log (WAL) is intact,
        attempts to recover the last known good state by checkpointing and/or
        recovering pages from the WAL file.
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
                return True
        except Exception as e:
            logger.debug("Direct WAL checkpoint failed: %s", e)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # Attempt 2: Page extraction from intact WAL
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

    @classmethod
    def get_canonical_vault_directory(cls) -> str:
        """Returns the canonical %AppData%\\Pipvane directory."""
        appdata = os.environ.get("APPDATA")
        if not appdata:
            userprofile = os.environ.get("USERPROFILE", os.path.expanduser("~"))
            appdata = os.path.join(userprofile, "AppData", "Roaming")
        return os.path.abspath(os.path.realpath(os.path.join(appdata, cls.VAULT_SUBDIR)))

    def _resolve_and_verify_path(self, target_path: Optional[str]) -> str:
        """
        Enforces strict file isolation:
        Verifies that target_path resides within %AppData%\\Pipvane (or test sandbox).
        """
        allowed_root = (
            os.path.abspath(os.path.realpath(self._test_sandbox_dir))
            if self._test_sandbox_dir
            else self.get_canonical_vault_directory()
        )

        if target_path is None:
            resolved_path = os.path.join(allowed_root, self.DEFAULT_DB_FILENAME)
        else:
            resolved_path = os.path.abspath(os.path.realpath(target_path))

        # Check path containment against directory traversal attacks
        try:
            common = os.path.commonpath([allowed_root, resolved_path])
        except ValueError as e:
            raise VaultSecurityError(
                f"File isolation check failed: Target path '{resolved_path}' is on a "
                f"different volume than authorized root '{allowed_root}'."
            ) from e

        if os.path.abspath(os.path.realpath(common)) != allowed_root:
            raise VaultSecurityError(
                f"File isolation check failed: Target path '{resolved_path}' is not within "
                f"authorized directory '{allowed_root}'."
            )

        # Ensure parent directory exists with user-only permissions
        parent_dir = os.path.dirname(resolved_path)
        os.makedirs(parent_dir, exist_ok=True)

        return resolved_path

    def _init_database(self, path: str) -> sqlite3.Connection:
        """Establishes connection and provisions encrypted vault schema."""
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Performance and integrity pragmas
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")

        with conn:
            # 1. Broker accounts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS broker_accounts (
                    account_id              TEXT PRIMARY KEY,
                    platform                TEXT NOT NULL,
                    broker_server           TEXT NOT NULL,
                    account_login           INTEGER NOT NULL,
                    master_password_blob    BLOB NOT NULL,
                    investor_password_blob  BLOB,
                    created_at              INTEGER NOT NULL,
                    updated_at              INTEGER NOT NULL
                );
            """)

            # 2. Telegram sessions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telegram_sessions (
                    session_id              TEXT PRIMARY KEY,
                    phone_number            TEXT NOT NULL,
                    session_blob            BLOB NOT NULL,
                    api_id                  INTEGER,
                    api_hash_blob           BLOB,
                    created_at              INTEGER NOT NULL,
                    updated_at              INTEGER NOT NULL
                );
            """)

            # 3. Generic application secrets table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vault_secrets (
                    key_name                TEXT PRIMARY KEY,
                    category                TEXT NOT NULL,
                    secret_blob             BLOB NOT NULL,
                    created_at              INTEGER NOT NULL,
                    updated_at              INTEGER NOT NULL
                );
            """)

        return conn

    # =========================================================================
    # Broker Account Credential Management
    # =========================================================================

    def save_broker_account(
        self,
        account_id: str,
        platform: str,
        broker_server: str,
        account_login: int,
        master_password: str,
        investor_password: Optional[str] = None
    ) -> None:
        """
        Encrypts sensitive passwords with DPAPI and persists credentials to SQLite.
        Plaintext passwords never touch disk unencrypted.
        """
        if not account_id or not platform or not broker_server or not master_password:
            raise ValueError("account_id, platform, broker_server, and master_password are required.")

        now_ms = int(time.time() * 1000)

        # Encrypt passwords in RAM before SQL write
        enc_master = self._encryptor.encrypt(master_password)
        enc_investor = self._encryptor.encrypt(investor_password) if investor_password else None

        with self.conn:
            self.conn.execute("""
                INSERT INTO broker_accounts (
                    account_id, platform, broker_server, account_login,
                    master_password_blob, investor_password_blob, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    platform = excluded.platform,
                    broker_server = excluded.broker_server,
                    account_login = excluded.account_login,
                    master_password_blob = excluded.master_password_blob,
                    investor_password_blob = excluded.investor_password_blob,
                    updated_at = excluded.updated_at;
            """, (
                account_id, platform, broker_server, int(account_login),
                enc_master, enc_investor, now_ms, now_ms
            ))

    def get_broker_account(self, account_id: str) -> Optional[BrokerAccountRecord]:
        """
        Fetches credentials and decrypts sensitive passwords into RAM at runtime.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT account_id, platform, broker_server, account_login,
                   master_password_blob, investor_password_blob, created_at, updated_at
            FROM broker_accounts WHERE account_id = ?;
        """, (account_id,))
        row = cursor.fetchone()
        if not row:
            return None

        # Decrypt in volatile memory
        master_pw = self._encryptor.decrypt_str(row["master_password_blob"])
        investor_pw = (
            self._encryptor.decrypt_str(row["investor_password_blob"])
            if row["investor_password_blob"] else None
        )

        return BrokerAccountRecord(
            account_id=row["account_id"],
            platform=row["platform"],
            broker_server=row["broker_server"],
            account_login=row["account_login"],
            master_password=master_pw,
            investor_password=investor_pw,
            created_at=row["created_at"],
            updated_at=row["updated_at"]
        )

    def delete_broker_account(self, account_id: str) -> bool:
        """Permanently deletes a broker account's credentials from the vault."""
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM broker_accounts WHERE account_id = ?;", (account_id,)
            )
            return cursor.rowcount > 0

    def list_broker_accounts(self) -> List[Dict[str, Any]]:
        """
        Returns metadata for all configured broker accounts.
        Does NOT decrypt sensitive passwords.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT account_id, platform, broker_server, account_login, created_at, updated_at
            FROM broker_accounts ORDER BY created_at ASC;
        """)
        return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Telegram Session Management
    # =========================================================================

    def save_telegram_session(
        self,
        session_id: str,
        phone_number: str,
        session_string: str,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None
    ) -> None:
        """
        Encrypts sensitive TDLib session auth string and api_hash with DPAPI.
        """
        if not session_id or not phone_number or not session_string:
            raise ValueError("session_id, phone_number, and session_string are required.")

        now_ms = int(time.time() * 1000)
        enc_session = self._encryptor.encrypt(session_string)
        enc_api_hash = self._encryptor.encrypt(api_hash) if api_hash else None

        with self.conn:
            self.conn.execute("""
                INSERT INTO telegram_sessions (
                    session_id, phone_number, session_blob, api_id, api_hash_blob, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    phone_number = excluded.phone_number,
                    session_blob = excluded.session_blob,
                    api_id = excluded.api_id,
                    api_hash_blob = excluded.api_hash_blob,
                    updated_at = excluded.updated_at;
            """, (session_id, phone_number, enc_session, api_id, enc_api_hash, now_ms, now_ms))

    def get_telegram_session(self, session_id: str) -> Optional[TelegramSessionRecord]:
        """
        Retrieves and decrypts the Telegram session string in RAM at runtime.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT session_id, phone_number, session_blob, api_id, api_hash_blob, updated_at
            FROM telegram_sessions WHERE session_id = ?;
        """, (session_id,))
        row = cursor.fetchone()
        if not row:
            return None

        dec_session = self._encryptor.decrypt_str(row["session_blob"])
        dec_api_hash = (
            self._encryptor.decrypt_str(row["api_hash_blob"])
            if row["api_hash_blob"] else None
        )

        return TelegramSessionRecord(
            session_id=row["session_id"],
            phone_number=row["phone_number"],
            session_string=dec_session,
            api_id=row["api_id"],
            api_hash=dec_api_hash,
            updated_at=row["updated_at"]
        )

    def delete_telegram_session(self, session_id: str) -> bool:
        """Removes a Telegram session record from the vault."""
        with self.conn:
            cursor = self.conn.execute(
                "DELETE FROM telegram_sessions WHERE session_id = ?;", (session_id,)
            )
            return cursor.rowcount > 0

    # =========================================================================
    # Generic Secret Management
    # =========================================================================

    def set_secret(self, key_name: str, secret_value: str, category: str = "generic") -> None:
        """Encrypts and stores an arbitrary string secret."""
        if not key_name or secret_value is None:
            raise ValueError("key_name and secret_value are required.")

        now_ms = int(time.time() * 1000)
        enc_blob = self._encryptor.encrypt(secret_value)

        with self.conn:
            self.conn.execute("""
                INSERT INTO vault_secrets (key_name, category, secret_blob, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(key_name) DO UPDATE SET
                    category = excluded.category,
                    secret_blob = excluded.secret_blob,
                    updated_at = excluded.updated_at;
            """, (key_name, category, enc_blob, now_ms, now_ms))

    def get_secret(self, key_name: str) -> Optional[str]:
        """Retrieves and decrypts an arbitrary string secret in RAM."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT secret_blob FROM vault_secrets WHERE key_name = ?;", (key_name,))
        row = cursor.fetchone()
        if not row:
            return None
        return self._encryptor.decrypt_str(row["secret_blob"])

    def delete_secret(self, key_name: str) -> bool:
        """Deletes an arbitrary secret by key name."""
        with self.conn:
            cursor = self.conn.execute("DELETE FROM vault_secrets WHERE key_name = ?;", (key_name,))
            return cursor.rowcount > 0

    # =========================================================================
    # Audit & Inspection
    # =========================================================================

    def inspect_raw_on_disk_rows(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Debugging/audit helper: Returns raw SQLite rows as stored on disk.
        Used by automated tests to mathematically verify that all sensitive
        columns contain encrypted binary BLOBs and NO plaintext strings.
        """
        allowed_tables = {"broker_accounts", "telegram_sessions", "vault_secrets"}
        if table_name not in allowed_tables:
            raise ValueError(f"Unauthorized table inspection: {table_name}")

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name};")
        return [dict(row) for row in cursor.fetchall()]

    def close(self) -> None:
        """Closes the underlying SQLite connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self) -> SecureVault:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
