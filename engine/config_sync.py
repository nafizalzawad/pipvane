"""
Pipvane v3: Encrypted State Synchronization Module (Sprint 15 Directive)
========================================================================
Synchronizes non-credential configuration profiles between Primary and Standby
devices using an end-to-end encrypted (E2EE) relay, ensuring the Cloud Control
Plane never sees plain text.

Key Mandates:
1. E2EE Payload Construction (Local):
   - Bundles all non-credential configuration JSONs (Channel Profiles, Risk Rules, Settings).
   - Strict Zero-Knowledge Sanitizer: Rejects/strips any credentials, passwords, or keys.
   - Encrypts locally using AES-256-GCM and PBKDF2-HMAC-SHA256 from a user-defined sync passphrase.
2. Relay Client:
   - Communicates with Cloud Control Plane /relay/sync endpoint.
   - Ephemeral 24-hour TTL payload exchange.
3. Standby State Ingestion:
   - Periodically polls the relay endpoint.
   - Downloads encrypted blob, authenticates GCM tag, and decrypts with shared passphrase.
   - Updates local SQLite database tables with zero data corruption.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_AESGCM = True
except ImportError:
    HAS_AESGCM = False

logger = logging.getLogger("pipvane.engine.config_sync")

# Cryptographic Constants
KDF_ALGORITHM = "PBKDF2-HMAC-SHA256"
PBKDF2_ITERATIONS = 100_000
SALT_BYTES = 16
IV_BYTES = 12
TAG_BYTES = 16
KEY_BYTES = 32

# Sanitizer forbidden keywords
FORBIDDEN_CREDENTIAL_KEYS = {
    "password", "secret", "token", "master_password", "investor_password",
    "master_password_blob", "investor_password_blob", "session_blob",
    "api_hash", "private_key", "credential", "auth_key", "api_key",
}


# =============================================================================
# EXCEPTIONS
# =============================================================================

class ConfigSyncError(Exception):
    """Base exception for configuration synchronization and E2EE failures."""
    pass


class PassphraseRequiredError(ConfigSyncError):
    """Raised when an operation requires a passphrase but none was provided."""
    pass


class InvalidPassphraseError(ConfigSyncError):
    """Raised when GCM tag verification fails (wrong passphrase or tampered payload)."""
    pass


class CredentialLeakPreventedError(ConfigSyncError):
    """Raised when an attempt to export or bundle sensitive credentials is detected."""
    pass


class RelayCommunicationError(ConfigSyncError):
    """Raised when communication with Cloud Relay fails."""
    pass


class SyncVersionOutdatedError(ConfigSyncError):
    """Raised when attempting to apply an older version than currently stored."""
    pass


# =============================================================================
# DATA STRUCTURES & PROFILES
# =============================================================================

@dataclass
class ChannelProfile:
    """Non-credential configuration profile for a monitored Telegram channel."""
    channel_id: int
    channel_name: str
    format_pack: str = "default"
    is_enabled: bool = True
    symbol_map: Dict[str, str] = field(default_factory=dict)
    risk_multiplier: float = 1.0
    sl_tp_defaults: Dict[str, Any] = field(default_factory=dict)
    custom_rules: Dict[str, Any] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ChannelProfile:
        return cls(
            channel_id=int(data["channel_id"]),
            channel_name=str(data.get("channel_name", f"Channel {data['channel_id']}")),
            format_pack=str(data.get("format_pack", "default")),
            is_enabled=bool(data.get("is_enabled", True)),
            symbol_map=dict(data.get("symbol_map", {})),
            risk_multiplier=float(data.get("risk_multiplier", 1.0)),
            sl_tp_defaults=dict(data.get("sl_tp_defaults", {})),
            custom_rules=dict(data.get("custom_rules", {})),
            updated_at=float(data.get("updated_at", time.time())),
        )


@dataclass
class RiskRulesProfile:
    """Non-credential risk configuration rules for an account or global engine."""
    account_id: str
    risk_model: str = "percent_equity"
    fixed_lot: float = 0.10
    equity_risk_pct: float = 1.0
    fixed_currency_risk: float = 50.0
    lot_multiplier: float = 1.0
    max_open_positions: int = 5
    max_drawdown_pct: float = 4.5
    daily_loss_limit_usd: float = 500.0
    max_loss_limit_usd: float = 1000.0
    news_filter_enabled: bool = True
    news_filter_window_minutes: int = 30
    slippage_tolerance_pips: float = 1.5
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RiskRulesProfile:
        return cls(
            account_id=str(data["account_id"]),
            risk_model=str(data.get("risk_model", "percent_equity")),
            fixed_lot=float(data.get("fixed_lot", 0.10)),
            equity_risk_pct=float(data.get("equity_risk_pct", 1.0)),
            fixed_currency_risk=float(data.get("fixed_currency_risk", 50.0)),
            lot_multiplier=float(data.get("lot_multiplier", 1.0)),
            max_open_positions=int(data.get("max_open_positions", 5)),
            max_drawdown_pct=float(data.get("max_drawdown_pct", 4.5)),
            daily_loss_limit_usd=float(data.get("daily_loss_limit_usd", 500.0)),
            max_loss_limit_usd=float(data.get("max_loss_limit_usd", 1000.0)),
            news_filter_enabled=bool(data.get("news_filter_enabled", True)),
            news_filter_window_minutes=int(data.get("news_filter_window_minutes", 30)),
            slippage_tolerance_pips=float(data.get("slippage_tolerance_pips", 1.5)),
            updated_at=float(data.get("updated_at", time.time())),
        )


@dataclass
class SyncBundle:
    """Unencrypted in-memory payload containing non-credential configuration state."""
    bundle_id: str
    version: int
    timestamp: float
    source_node_id: str
    license_id: str
    channel_profiles: List[Dict[str, Any]] = field(default_factory=list)
    risk_rules: Dict[str, Any] = field(default_factory=dict)
    general_settings: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SyncBundle:
        return cls(
            bundle_id=str(data["bundle_id"]),
            version=int(data["version"]),
            timestamp=float(data["timestamp"]),
            source_node_id=str(data["source_node_id"]),
            license_id=str(data["license_id"]),
            channel_profiles=list(data.get("channel_profiles", [])),
            risk_rules=dict(data.get("risk_rules", {})),
            general_settings=dict(data.get("general_settings", {})),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass
class EncryptedSyncEnvelope:
    """Wire-format envelope holding AES-256-GCM ciphertext and parameters."""
    license_id: str
    version: int
    timestamp: float
    source_node_id: str
    encrypted_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "license_id": self.license_id,
            "version": self.version,
            "timestamp": self.timestamp,
            "source_node_id": self.source_node_id,
            "encrypted_payload": self.encrypted_payload,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> EncryptedSyncEnvelope:
        license_id = str(data.get("license_id") or data.get("licenseId") or "")
        version = int(data.get("version", 1))
        timestamp = float(data.get("timestamp", time.time()))
        source_node_id = str(data.get("source_node_id") or data.get("sourceNodeId") or "node_unknown")
        enc_payload = dict(data.get("encrypted_payload") or data.get("encryptedPayload") or {})
        return cls(
            license_id=license_id,
            version=version,
            timestamp=timestamp,
            source_node_id=source_node_id,
            encrypted_payload=enc_payload,
        )


# =============================================================================
# ZERO-KNOWLEDGE SANITIZER
# =============================================================================

def sanitize_config_payload(obj: Any, strict: bool = True) -> Any:
    """
    Recursively inspects and purges any keys or values containing credentials,
    passwords, session blobs, or cryptographic secrets.
    """
    if isinstance(obj, dict):
        sanitized = {}
        for k, v in obj.items():
            k_lower = str(k).lower().strip()
            for forbidden in FORBIDDEN_CREDENTIAL_KEYS:
                if forbidden in k_lower:
                    if strict:
                        raise CredentialLeakPreventedError(
                            f"Zero-Knowledge Violation: Forbidden credential key '{k}' found in sync payload."
                        )
                    continue  # Strip key if not strict
            sanitized[k] = sanitize_config_payload(v, strict=strict)
        return sanitized
    elif isinstance(obj, list):
        return [sanitize_config_payload(item, strict=strict) for item in obj]
    else:
        return obj


# =============================================================================
# CRYPTOGRAPHIC UTILITY (AES-256-GCM + PBKDF2)
# =============================================================================

class ConfigCrypto:
    """
    Local End-to-End Encryption engine using AES-256-GCM and PBKDF2-HMAC-SHA256.
    Ensures that plain configuration data is never accessible to the cloud relay.
    """

    @staticmethod
    def derive_key(passphrase: str, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
        """Derives a 256-bit symmetric encryption key from user passphrase using PBKDF2."""
        if not passphrase:
            raise PassphraseRequiredError("Sync passphrase must not be empty.")
        return hashlib.pbkdf2_hmac(
            "sha256",
            passphrase.encode("utf-8"),
            salt,
            iterations,
            dklen=KEY_BYTES,
        )

    @classmethod
    def encrypt_bundle(
        cls,
        bundle: Union[SyncBundle, Dict[str, Any]],
        passphrase: str,
    ) -> EncryptedSyncEnvelope:
        """
        Encrypts a configuration bundle with AES-256-GCM and returns an EncryptedSyncEnvelope.
        """
        if not HAS_AESGCM:
            raise ConfigSyncError("cryptography package is required for AES-GCM encryption.")

        if not passphrase:
            raise PassphraseRequiredError("User sync passphrase is required for encryption.")

        # Extract bundle metadata
        if isinstance(bundle, SyncBundle):
            bundle_dict = bundle.to_dict()
            license_id = bundle.license_id
            version = bundle.version
            source_node_id = bundle.source_node_id
            timestamp = bundle.timestamp
        else:
            bundle_dict = dict(bundle)
            license_id = str(bundle_dict.get("license_id", "UNKNOWN"))
            version = int(bundle_dict.get("version", 1))
            source_node_id = str(bundle_dict.get("source_node_id", "unknown_node"))
            timestamp = float(bundle_dict.get("timestamp", time.time()))

        # Sanitize to guarantee zero credentials
        clean_bundle = sanitize_config_payload(bundle_dict, strict=True)
        plaintext_bytes = json.dumps(clean_bundle, sort_keys=True, separators=(",", ":")).encode("utf-8")

        salt = secrets.token_bytes(SALT_BYTES)
        key = cls.derive_key(passphrase, salt, PBKDF2_ITERATIONS)
        iv = secrets.token_bytes(IV_BYTES)

        # Authenticated Data binds the license_id and version to ciphertext
        aad_str = f"pipvane:sync:{license_id}:{version}"
        aad_bytes = aad_str.encode("utf-8")

        aesgcm = AESGCM(key)
        # encrypt returns ciphertext + 16-byte authentication tag
        raw_cipher_and_tag = aesgcm.encrypt(iv, plaintext_bytes, aad_bytes)
        ciphertext = raw_cipher_and_tag[:-TAG_BYTES]
        tag = raw_cipher_and_tag[-TAG_BYTES:]

        enc_payload = {
            "kdf": KDF_ALGORITHM,
            "iterations": PBKDF2_ITERATIONS,
            "salt": base64.b64encode(salt).decode("ascii"),
            "iv": base64.b64encode(iv).decode("ascii"),
            "tag": base64.b64encode(tag).decode("ascii"),
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
            "aad": aad_str,
        }

        return EncryptedSyncEnvelope(
            license_id=license_id,
            version=version,
            timestamp=timestamp,
            source_node_id=source_node_id,
            encrypted_payload=enc_payload,
        )

    @classmethod
    def decrypt_envelope(
        cls,
        envelope: Union[EncryptedSyncEnvelope, Dict[str, Any]],
        passphrase: str,
    ) -> SyncBundle:
        """
        Decrypts an EncryptedSyncEnvelope with user passphrase, verifies GCM tag,
        and returns the original SyncBundle.
        """
        if not HAS_AESGCM:
            raise ConfigSyncError("cryptography package is required for AES-GCM decryption.")

        if not passphrase:
            raise PassphraseRequiredError("User sync passphrase is required for decryption.")

        if isinstance(envelope, EncryptedSyncEnvelope):
            enc_payload = envelope.encrypted_payload
            license_id = envelope.license_id
            version = envelope.version
        else:
            enc_payload = envelope.get("encrypted_payload") or envelope.get("encryptedPayload") or {}
            license_id = envelope.get("license_id") or envelope.get("licenseId") or ""
            version = int(envelope.get("version", 1))

        salt_b64 = enc_payload.get("salt")
        iv_b64 = enc_payload.get("iv")
        tag_b64 = enc_payload.get("tag")
        ciphertext_b64 = enc_payload.get("ciphertext")
        iterations = int(enc_payload.get("iterations", PBKDF2_ITERATIONS))
        aad_str = enc_payload.get("aad") or f"pipvane:sync:{license_id}:{version}"

        if not (salt_b64 and iv_b64 and tag_b64 and ciphertext_b64):
            raise ConfigSyncError("Malformed encrypted envelope: missing required crypto fields.")

        try:
            salt = base64.b64decode(salt_b64)
            iv = base64.b64decode(iv_b64)
            tag = base64.b64decode(tag_b64)
            ciphertext = base64.b64decode(ciphertext_b64)
            aad_bytes = aad_str.encode("utf-8")
        except Exception as e:
            raise ConfigSyncError(f"Failed to base64 decode encrypted payload: {e}") from e

        key = cls.derive_key(passphrase, salt, iterations)
        aesgcm = AESGCM(key)

        try:
            # AESGCM.decrypt expects ciphertext with appended tag
            plaintext_bytes = aesgcm.decrypt(iv, ciphertext + tag, aad_bytes)
        except Exception as e:
            # Tag mismatch or key mismatch
            raise InvalidPassphraseError(
                "Decryption failed: Invalid sync passphrase or corrupted payload."
            ) from e

        try:
            bundle_dict = json.loads(plaintext_bytes.decode("utf-8"))
            return SyncBundle.from_dict(bundle_dict)
        except Exception as e:
            raise ConfigSyncError(f"Failed to parse decrypted configuration JSON: {e}") from e


# =============================================================================
# LOCAL SQLITE DATABASE STORAGE
# =============================================================================

class ConfigSyncStore:
    """
    Manages local SQLite database storage for non-credential configuration profiles
    (Channel Profiles, Risk Rules, General Settings) and synchronization state audit log.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or ":memory:"
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        """Initializes tables for non-credential profiles and sync state audit log."""
        with self._conn:
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS channel_profiles (
                    channel_id INTEGER PRIMARY KEY,
                    channel_name TEXT NOT NULL,
                    format_pack TEXT NOT NULL DEFAULT 'default',
                    is_enabled INTEGER NOT NULL DEFAULT 1,
                    symbol_map_json TEXT NOT NULL DEFAULT '{}',
                    risk_multiplier REAL NOT NULL DEFAULT 1.0,
                    sl_tp_defaults_json TEXT NOT NULL DEFAULT '{}',
                    custom_rules_json TEXT NOT NULL DEFAULT '{}',
                    updated_at REAL NOT NULL
                );
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS risk_rules (
                    account_id TEXT PRIMARY KEY,
                    risk_model TEXT NOT NULL DEFAULT 'percent_equity',
                    fixed_lot REAL NOT NULL DEFAULT 0.10,
                    equity_risk_pct REAL NOT NULL DEFAULT 1.0,
                    fixed_currency_risk REAL NOT NULL DEFAULT 50.0,
                    lot_multiplier REAL NOT NULL DEFAULT 1.0,
                    max_open_positions INTEGER NOT NULL DEFAULT 5,
                    max_drawdown_pct REAL NOT NULL DEFAULT 4.5,
                    daily_loss_limit_usd REAL NOT NULL DEFAULT 500.0,
                    max_loss_limit_usd REAL NOT NULL DEFAULT 1000.0,
                    news_filter_enabled INTEGER NOT NULL DEFAULT 1,
                    news_filter_window_minutes INTEGER NOT NULL DEFAULT 30,
                    slippage_tolerance_pips REAL NOT NULL DEFAULT 1.5,
                    updated_at REAL NOT NULL
                );
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_applied_version INTEGER NOT NULL DEFAULT 0,
                    last_applied_timestamp REAL NOT NULL DEFAULT 0.0,
                    last_bundle_id TEXT NOT NULL DEFAULT '',
                    last_sync_status TEXT NOT NULL DEFAULT 'INITIAL',
                    source_node_id TEXT NOT NULL DEFAULT '',
                    details TEXT NOT NULL DEFAULT ''
                );
            """)
            self._conn.execute("""
                INSERT OR IGNORE INTO sync_state (
                    id, last_applied_version, last_applied_timestamp,
                    last_bundle_id, last_sync_status, source_node_id, details
                ) VALUES (1, 0, 0.0, '', 'INITIAL', '', 'Initial state');
            """)

    # -------------------------------------------------------------------------
    # Channel Profiles Operations
    # -------------------------------------------------------------------------

    def save_channel_profile(self, profile: ChannelProfile) -> None:
        """Upserts a channel profile."""
        with self._conn:
            self._conn.execute("""
                INSERT INTO channel_profiles (
                    channel_id, channel_name, format_pack, is_enabled,
                    symbol_map_json, risk_multiplier, sl_tp_defaults_json,
                    custom_rules_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    channel_name = excluded.channel_name,
                    format_pack = excluded.format_pack,
                    is_enabled = excluded.is_enabled,
                    symbol_map_json = excluded.symbol_map_json,
                    risk_multiplier = excluded.risk_multiplier,
                    sl_tp_defaults_json = excluded.sl_tp_defaults_json,
                    custom_rules_json = excluded.custom_rules_json,
                    updated_at = excluded.updated_at;
            """, (
                profile.channel_id,
                profile.channel_name,
                profile.format_pack,
                1 if profile.is_enabled else 0,
                json.dumps(profile.symbol_map),
                profile.risk_multiplier,
                json.dumps(profile.sl_tp_defaults),
                json.dumps(profile.custom_rules),
                profile.updated_at,
            ))

    def get_channel_profile(self, channel_id: int) -> Optional[ChannelProfile]:
        """Fetches a channel profile by channel_id."""
        with self._conn:
            cur = self._conn.execute(
                "SELECT * FROM channel_profiles WHERE channel_id = ?;", (channel_id,)
            )
            row = cur.fetchone()
            if not row:
                return None
            return ChannelProfile(
                channel_id=row["channel_id"],
                channel_name=row["channel_name"],
                format_pack=row["format_pack"],
                is_enabled=bool(row["is_enabled"]),
                symbol_map=json.loads(row["symbol_map_json"]),
                risk_multiplier=row["risk_multiplier"],
                sl_tp_defaults=json.loads(row["sl_tp_defaults_json"]),
                custom_rules=json.loads(row["custom_rules_json"]),
                updated_at=row["updated_at"],
            )

    def list_channel_profiles(self) -> List[ChannelProfile]:
        """Lists all channel profiles."""
        with self._conn:
            cur = self._conn.execute("SELECT * FROM channel_profiles ORDER BY channel_id ASC;")
            profiles = []
            for row in cur.fetchall():
                profiles.append(ChannelProfile(
                    channel_id=row["channel_id"],
                    channel_name=row["channel_name"],
                    format_pack=row["format_pack"],
                    is_enabled=bool(row["is_enabled"]),
                    symbol_map=json.loads(row["symbol_map_json"]),
                    risk_multiplier=row["risk_multiplier"],
                    sl_tp_defaults=json.loads(row["sl_tp_defaults_json"]),
                    custom_rules=json.loads(row["custom_rules_json"]),
                    updated_at=row["updated_at"],
                ))
            return profiles

    # -------------------------------------------------------------------------
    # Risk Rules Operations
    # -------------------------------------------------------------------------

    def save_risk_rules(self, rules: RiskRulesProfile) -> None:
        """Upserts risk rules for an account or global profile."""
        with self._conn:
            self._conn.execute("""
                INSERT INTO risk_rules (
                    account_id, risk_model, fixed_lot, equity_risk_pct,
                    fixed_currency_risk, lot_multiplier, max_open_positions,
                    max_drawdown_pct, daily_loss_limit_usd, max_loss_limit_usd,
                    news_filter_enabled, news_filter_window_minutes,
                    slippage_tolerance_pips, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    risk_model = excluded.risk_model,
                    fixed_lot = excluded.fixed_lot,
                    equity_risk_pct = excluded.equity_risk_pct,
                    fixed_currency_risk = excluded.fixed_currency_risk,
                    lot_multiplier = excluded.lot_multiplier,
                    max_open_positions = excluded.max_open_positions,
                    max_drawdown_pct = excluded.max_drawdown_pct,
                    daily_loss_limit_usd = excluded.daily_loss_limit_usd,
                    max_loss_limit_usd = excluded.max_loss_limit_usd,
                    news_filter_enabled = excluded.news_filter_enabled,
                    news_filter_window_minutes = excluded.news_filter_window_minutes,
                    slippage_tolerance_pips = excluded.slippage_tolerance_pips,
                    updated_at = excluded.updated_at;
            """, (
                rules.account_id,
                rules.risk_model,
                rules.fixed_lot,
                rules.equity_risk_pct,
                rules.fixed_currency_risk,
                rules.lot_multiplier,
                rules.max_open_positions,
                rules.max_drawdown_pct,
                rules.daily_loss_limit_usd,
                rules.max_loss_limit_usd,
                1 if rules.news_filter_enabled else 0,
                rules.news_filter_window_minutes,
                rules.slippage_tolerance_pips,
                rules.updated_at,
            ))

    def get_risk_rules(self, account_id: str) -> Optional[RiskRulesProfile]:
        """Fetches risk rules for an account_id."""
        with self._conn:
            cur = self._conn.execute(
                "SELECT * FROM risk_rules WHERE account_id = ?;", (account_id,)
            )
            row = cur.fetchone()
            if not row:
                return None
            return RiskRulesProfile(
                account_id=row["account_id"],
                risk_model=row["risk_model"],
                fixed_lot=row["fixed_lot"],
                equity_risk_pct=row["equity_risk_pct"],
                fixed_currency_risk=row["fixed_currency_risk"],
                lot_multiplier=row["lot_multiplier"],
                max_open_positions=row["max_open_positions"],
                max_drawdown_pct=row["max_drawdown_pct"],
                daily_loss_limit_usd=row["daily_loss_limit_usd"],
                max_loss_limit_usd=row["max_loss_limit_usd"],
                news_filter_enabled=bool(row["news_filter_enabled"]),
                news_filter_window_minutes=row["news_filter_window_minutes"],
                slippage_tolerance_pips=row["slippage_tolerance_pips"],
                updated_at=row["updated_at"],
            )

    def list_risk_rules(self) -> List[RiskRulesProfile]:
        """Lists all risk rules profiles."""
        with self._conn:
            cur = self._conn.execute("SELECT * FROM risk_rules ORDER BY account_id ASC;")
            rules_list = []
            for row in cur.fetchall():
                rules_list.append(RiskRulesProfile(
                    account_id=row["account_id"],
                    risk_model=row["risk_model"],
                    fixed_lot=row["fixed_lot"],
                    equity_risk_pct=row["equity_risk_pct"],
                    fixed_currency_risk=row["fixed_currency_risk"],
                    lot_multiplier=row["lot_multiplier"],
                    max_open_positions=row["max_open_positions"],
                    max_drawdown_pct=row["max_drawdown_pct"],
                    daily_loss_limit_usd=row["daily_loss_limit_usd"],
                    max_loss_limit_usd=row["max_loss_limit_usd"],
                    news_filter_enabled=bool(row["news_filter_enabled"]),
                    news_filter_window_minutes=row["news_filter_window_minutes"],
                    slippage_tolerance_pips=row["slippage_tolerance_pips"],
                    updated_at=row["updated_at"],
                ))
            return rules_list

    # -------------------------------------------------------------------------
    # Synchronization State & Bundle Import/Export
    # -------------------------------------------------------------------------

    def get_sync_state(self) -> Dict[str, Any]:
        """Retrieves current local synchronization state pointer."""
        with self._conn:
            cur = self._conn.execute("SELECT * FROM sync_state WHERE id = 1;")
            row = cur.fetchone()
            if not row:
                return {"last_applied_version": 0, "last_bundle_id": "", "status": "UNKNOWN"}
            return dict(row)

    def export_bundle(
        self,
        license_id: str,
        source_node_id: str,
        version: Optional[int] = None,
        general_settings: Optional[Dict[str, Any]] = None,
    ) -> SyncBundle:
        """Bundles all local non-credential configuration into an exportable SyncBundle."""
        state = self.get_sync_state()
        export_version = version if version is not None else int(state.get("last_applied_version", 0)) + 1
        bundle_id = f"bundle_{export_version}_{secrets.token_hex(4)}"

        channels = [p.to_dict() for p in self.list_channel_profiles()]
        rules = {r.account_id: r.to_dict() for r in self.list_risk_rules()}
        settings = dict(general_settings or {})

        bundle = SyncBundle(
            bundle_id=bundle_id,
            version=export_version,
            timestamp=time.time(),
            source_node_id=source_node_id,
            license_id=license_id,
            channel_profiles=channels,
            risk_rules=rules,
            general_settings=settings,
            metadata={"exported_at": time.time(), "channel_count": len(channels)},
        )

        # Update local version state
        with self._conn:
            self._conn.execute("""
                UPDATE sync_state SET
                    last_applied_version = ?,
                    last_applied_timestamp = ?,
                    last_bundle_id = ?,
                    last_sync_status = 'EXPORTED',
                    source_node_id = ?,
                    details = 'Primary configuration exported'
                WHERE id = 1;
            """, (export_version, bundle.timestamp, bundle_id, source_node_id))

        return bundle

    def apply_bundle(self, bundle: Union[SyncBundle, Dict[str, Any]], force: bool = False) -> bool:
        """
        Applies a decrypted SyncBundle to the local SQLite database.
        Overwrites/merges channel profiles and risk rules atomically.
        """
        if isinstance(bundle, dict):
            bundle = SyncBundle.from_dict(bundle)

        current_state = self.get_sync_state()
        local_version = int(current_state.get("last_applied_version", 0))

        if not force and bundle.version <= local_version:
            logger.info(
                "Skipping bundle %s: version %d <= local version %d",
                bundle.bundle_id, bundle.version, local_version,
            )
            return False

        logger.info(
            "Applying sync bundle %s (v%d from node %s) containing %d channel profiles",
            bundle.bundle_id, bundle.version, bundle.source_node_id, len(bundle.channel_profiles),
        )

        with self._conn:
            # 1. Apply channel profiles
            for ch_dict in bundle.channel_profiles:
                p = ChannelProfile.from_dict(ch_dict)
                self.save_channel_profile(p)

            # 2. Apply risk rules
            if isinstance(bundle.risk_rules, dict):
                for acc_id, r_data in bundle.risk_rules.items():
                    if isinstance(r_data, dict):
                        r = RiskRulesProfile.from_dict(r_data)
                        self.save_risk_rules(r)

            # 3. Update sync state pointer
            self._conn.execute("""
                UPDATE sync_state SET
                    last_applied_version = ?,
                    last_applied_timestamp = ?,
                    last_bundle_id = ?,
                    last_sync_status = 'APPLIED',
                    source_node_id = ?,
                    details = ?
                WHERE id = 1;
            """, (
                bundle.version,
                bundle.timestamp,
                bundle.bundle_id,
                bundle.source_node_id,
                f"Successfully ingested {len(bundle.channel_profiles)} channel profiles",
            ))

        return True


# =============================================================================
# RELAY HTTP CLIENT
# =============================================================================

class ConfigSyncRelayClient:
    """HTTP client for communicating with Cloud Relay /relay/sync endpoint."""

    def __init__(self, relay_base_url: str = "http://127.0.0.1:8080"):
        self.relay_base_url = relay_base_url.rstrip("/")

    def upload_encrypted_envelope(
        self,
        envelope: EncryptedSyncEnvelope,
        timeout_s: float = 5.0,
    ) -> Dict[str, Any]:
        """POSTs the encrypted configuration envelope to /relay/sync."""
        url = f"{self.relay_base_url}/relay/sync"
        payload_bytes = json.dumps(envelope.to_dict()).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload_bytes,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Pipvane-Engine-ConfigSync/3.0",
                "X-License-ID": envelope.license_id,
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else ""
            raise RelayCommunicationError(
                f"Relay POST error HTTP {e.code}: {e.reason} ({err_body})"
            ) from e
        except Exception as e:
            raise RelayCommunicationError(f"Failed to upload sync envelope to relay: {e}") from e

    def fetch_encrypted_envelope(
        self,
        license_id: str,
        timeout_s: float = 5.0,
    ) -> Optional[EncryptedSyncEnvelope]:
        """GETs the active encrypted configuration envelope for a license_id from /relay/sync."""
        query = urllib.parse.urlencode({"license_id": license_id})
        url = f"{self.relay_base_url}/relay/sync?{query}"

        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "Pipvane-Engine-ConfigSync/3.0",
                "X-License-ID": license_id,
            },
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body)
                if data.get("success") and "payload" in data:
                    return EncryptedSyncEnvelope.from_dict(data["payload"])
                return None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                # No payload currently available on relay
                return None
            err_body = e.read().decode("utf-8") if e.fp else ""
            raise RelayCommunicationError(
                f"Relay GET error HTTP {e.code}: {e.reason} ({err_body})"
            ) from e
        except Exception as e:
            raise RelayCommunicationError(f"Failed to fetch sync envelope from relay: {e}") from e


# =============================================================================
# HIGH-LEVEL CONFIG SYNC MANAGER & STANDBY POLLING DAEMON
# =============================================================================

class ConfigSyncManager:
    """
    Coordinates end-to-end encrypted profile export from Primary device and
    automated background state ingestion on Standby device.
    """

    def __init__(
        self,
        license_id: str,
        node_id: str,
        store: Optional[ConfigSyncStore] = None,
        relay_client: Optional[ConfigSyncRelayClient] = None,
        relay_url: str = "http://127.0.0.1:8080",
        orchestrator: Optional[Any] = None,
        failover_manager: Optional[Any] = None,
    ):
        self.license_id = license_id
        self.node_id = node_id
        self.store = store or ConfigSyncStore()
        self.relay_client = relay_client or ConfigSyncRelayClient(relay_url)
        self.orchestrator = orchestrator
        self.failover_manager = failover_manager

        self._standby_task: Optional[asyncio.Task] = None
        self._is_running = False

    def publish_primary_sync(
        self,
        passphrase: str,
        general_settings: Optional[Dict[str, Any]] = None,
    ) -> EncryptedSyncEnvelope:
        """
        Primary device workflow:
        1. Bundles all local non-credential configuration profiles.
        2. Encrypts payload with user passphrase using AES-256-GCM.
        3. Uploads the encrypted envelope to the Cloud Relay.
        """
        bundle = self.store.export_bundle(
            license_id=self.license_id,
            source_node_id=self.node_id,
            general_settings=general_settings,
        )
        envelope = ConfigCrypto.encrypt_bundle(bundle, passphrase=passphrase)
        receipt = self.relay_client.upload_encrypted_envelope(envelope)
        logger.info(
            "Primary configuration published successfully (Bundle %s, v%d, Expires: %s)",
            bundle.bundle_id, bundle.version, receipt.get("expires_at"),
        )
        return envelope

    def poll_and_apply_standby(
        self,
        passphrase: str,
        force: bool = False,
    ) -> Optional[SyncBundle]:
        """
        Standby device workflow:
        1. Queries Cloud Relay for latest encrypted payload.
        2. If available, verifies and decrypts using shared local passphrase.
        3. Ingests profiles into local SQLite database.
        """
        envelope = self.relay_client.fetch_encrypted_envelope(self.license_id)
        if not envelope:
            logger.debug("No new sync payload on Cloud Relay for license %s", self.license_id)
            return None

        # Check version against local store
        local_state = self.store.get_sync_state()
        local_v = int(local_state.get("last_applied_version", 0))

        if not force and envelope.version <= local_v:
            logger.debug(
                "Remote envelope v%d is not newer than local v%d. Skipping.",
                envelope.version, local_v,
            )
            return None

        # Decrypt payload using passphrase
        bundle = ConfigCrypto.decrypt_envelope(envelope, passphrase=passphrase)

        # Apply to local SQLite
        applied = self.store.apply_bundle(bundle, force=force)
        if applied:
            logger.info(
                "Successfully synchronized configuration from Primary (Bundle %s, v%d)",
                bundle.bundle_id, bundle.version,
            )
            return bundle
        return None

    # -------------------------------------------------------------------------
    # Standby Automated Polling Daemon
    # -------------------------------------------------------------------------

    async def start_standby_sync_loop(
        self,
        passphrase: str,
        poll_interval_s: float = 30.0,
    ) -> None:
        """
        Starts an asynchronous background daemon that periodically polls Cloud Relay
        and ingests configuration changes while the node is operating in Standby mode.
        """
        self._is_running = True
        logger.info(
            "Starting Standby Config Sync Daemon (License=%s, Interval=%.1fs)",
            self.license_id, poll_interval_s,
        )

        while self._is_running:
            try:
                # Only active in Standby mode if orchestrator / failover manager is connected
                should_sync = True
                if self.failover_manager and hasattr(self.failover_manager, "is_standby"):
                    should_sync = self.failover_manager.is_standby
                elif self.orchestrator and hasattr(self.orchestrator, "is_standby"):
                    should_sync = self.orchestrator.is_standby()

                if should_sync:
                    # Run sync in threadpool to avoid blocking event loop
                    await asyncio.to_thread(self.poll_and_apply_standby, passphrase)
            except InvalidPassphraseError as e:
                logger.error("Standby Sync Passphrase Failure: %s", e)
            except Exception as e:
                logger.warning("Standby sync poll encounter: %s", e)

            try:
                await asyncio.sleep(poll_interval_s)
            except asyncio.CancelledError:
                break

    def stop_standby_sync_loop(self) -> None:
        """Stops the standby polling daemon."""
        self._is_running = False
        if self._standby_task and not self._standby_task.done():
            self._standby_task.cancel()
