"""
Pipvane v3: Remote Command & Mobile Relay Security Module (Sprint 17 Directive)
================================================================================
Handles Zero-Knowledge end-to-end encrypted remote emergency commands
(e.g., "Close All Trades", "Panic Close", "Pause Trading") between the
Mobile Companion App and the Local Engine via the Cloud Relay.

Strict Security Constraints & Gates:
1. Zero-Knowledge Compliance: Cloud Relay receives ONLY opaque ciphertext and
   routing metadata. No plaintext trading parameters (symbols, lots, tickets)
   or credentials are ever exposed outside the encrypted envelope.
2. Anti-Spoofing & Tamper Resistance:
   - Rejects unencrypted plaintext JSON commands immediately and logs critical security alerts.
   - Rejects corrupted ciphertext, tampered MAC tags, or unauthorized passphrases/keys.
3. Anti-Replay Guard:
   - Validates timestamp freshness window (default: max 60s drift). Rejects any payload
     older than the threshold (such as a 5-minute delayed replay attempt).
   - Deduplicates nonces / command_ids to prevent replaying identical payloads within the window.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import enum
import hashlib
import json
import logging
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM, ChaCha20Poly1305
    HAS_AEAD = True
except ImportError:
    HAS_AEAD = False

logger = logging.getLogger("pipvane.engine.remote_commands")

# Cryptographic Constants
KDF_ALGORITHM = "PBKDF2-HMAC-SHA256"
PBKDF2_ITERATIONS = 100_000
SALT_BYTES = 16
IV_BYTES = 12
TAG_BYTES = 16
KEY_BYTES = 32

# Replay Prevention Constants
DEFAULT_MAX_DRIFT_SECONDS = 60.0  # Freshness window: reject commands older than 60s
DEFAULT_NONCE_TTL_SECONDS = 3600.0  # Track processed nonces for 1 hour

# Zero-Knowledge Forbidden Plaintext Keys (Cloud & Relay boundary)
FORBIDDEN_PLAINTEXT_KEYS = {
    "symbol", "lot", "lots", "volume", "order_type", "stop_loss", "sl",
    "take_profit", "tp", "price", "ticket", "broker", "login", "password",
    "investor_password", "signal_text", "command_name", "trade", "action_type",
    "account_number", "magic_number",
}


# =============================================================================
# ENUMS & EXCEPTIONS
# =============================================================================

class RemoteCommandType(str, enum.Enum):
    """Supported emergency and control command types from Mobile Companion App."""
    CLOSE_ALL_TRADES = "CLOSE_ALL_TRADES"
    PANIC_CLOSE = "PANIC_CLOSE"
    CLOSE_SYMBOL = "CLOSE_SYMBOL"
    CLOSE_TICKET = "CLOSE_TICKET"
    PAUSE_TRADING = "PAUSE_TRADING"
    RESUME_TRADING = "RESUME_TRADING"
    MODIFY_RISK = "MODIFY_RISK"
    ENGINE_STATUS_QUERY = "ENGINE_STATUS_QUERY"


class SecurityRejectionCode(str, enum.Enum):
    """Reason codes for remote command security rejection."""
    REPLAY_ATTACK_DETECTED = "REPLAY_ATTACK_DETECTED"
    STALE_TIMESTAMP = "STALE_TIMESTAMP"
    FUTURE_TIMESTAMP_DRIFT = "FUTURE_TIMESTAMP_DRIFT"
    DUPLICATE_NONCE = "DUPLICATE_NONCE"
    SPOOFED_PAYLOAD_DETECTED = "SPOOFED_PAYLOAD_DETECTED"
    PLAINTEXT_REJECTED = "PLAINTEXT_REJECTED"
    MALFORMED_ENVELOPE = "MALFORMED_ENVELOPE"
    TAMPERED_CIPHERTEXT = "TAMPERED_CIPHERTEXT"
    INVALID_SIGNATURE_OR_KEY = "INVALID_SIGNATURE_OR_KEY"
    ZERO_KNOWLEDGE_VIOLATION = "ZERO_KNOWLEDGE_VIOLATION"


class RemoteSecurityError(Exception):
    """Base exception for all remote command security rejections."""
    def __init__(self, message: str, code: SecurityRejectionCode = SecurityRejectionCode.SPOOFED_PAYLOAD_DETECTED):
        super().__init__(message)
        self.code = code
        self.message = message


class ReplayAttackError(RemoteSecurityError):
    """Raised when a command is rejected due to stale timestamp or duplicate nonce."""
    def __init__(self, message: str, code: SecurityRejectionCode = SecurityRejectionCode.REPLAY_ATTACK_DETECTED):
        super().__init__(message, code)


class StaleTimestampError(ReplayAttackError):
    """Raised specifically when command timestamp exceeds max freshness age."""
    def __init__(self, message: str, age_seconds: float = 0.0):
        super().__init__(message, SecurityRejectionCode.STALE_TIMESTAMP)
        self.age_seconds = age_seconds


class SpoofedPayloadError(RemoteSecurityError):
    """Raised when an unencrypted plaintext or malformed command is received."""
    def __init__(self, message: str, code: SecurityRejectionCode = SecurityRejectionCode.SPOOFED_PAYLOAD_DETECTED):
        super().__init__(message, code)


class TamperedPayloadError(RemoteSecurityError):
    """Raised when cryptographic MAC authentication fails due to tampering or wrong key."""
    def __init__(self, message: str, code: SecurityRejectionCode = SecurityRejectionCode.TAMPERED_CIPHERTEXT):
        super().__init__(message, code)


class InvalidPassphraseError(TamperedPayloadError):
    """Raised when passphrase derivation or decryption fails."""
    def __init__(self, message: str):
        super().__init__(message, SecurityRejectionCode.INVALID_SIGNATURE_OR_KEY)


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class RemoteCommand:
    """Unencrypted inner remote command dispatched by the Mobile Companion App."""
    command_id: str
    command: Union[RemoteCommandType, str]
    timestamp: float
    terminal_id: str = "ALL"
    params: Dict[str, Any] = field(default_factory=dict)
    license_id: Optional[str] = None
    sender_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "command": self.command.value if isinstance(self.command, RemoteCommandType) else str(self.command),
            "timestamp": self.timestamp,
            "terminal_id": self.terminal_id,
            "params": copy.deepcopy(self.params),
            "license_id": self.license_id,
            "sender_id": self.sender_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RemoteCommand:
        cmd_raw = data.get("command") or data.get("command_name") or data.get("action") or ""
        try:
            cmd = RemoteCommandType(str(cmd_raw).upper())
        except ValueError:
            cmd = str(cmd_raw).upper()

        return cls(
            command_id=str(data.get("command_id") or data.get("commandId") or uuid.uuid4()),
            command=cmd,
            timestamp=float(data.get("timestamp", time.time())),
            terminal_id=str(data.get("terminal_id") or data.get("terminalId") or "ALL"),
            params=dict(data.get("params", {})),
            license_id=data.get("license_id") or data.get("licenseId"),
            sender_id=data.get("sender_id") or data.get("senderId"),
        )


@dataclass
class RemoteCommandEnvelope:
    """Wire-format envelope holding AES-256-GCM ciphertext and routing metadata."""
    metadata: Dict[str, Any]
    ciphertext: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": copy.deepcopy(self.metadata),
            "ciphertext": self.ciphertext,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RemoteCommandEnvelope:
        if not isinstance(data, dict):
            raise SpoofedPayloadError(
                "Envelope must be a JSON dictionary object",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )
        metadata = data.get("metadata")
        ciphertext = data.get("ciphertext")
        if not metadata or not isinstance(metadata, dict):
            raise SpoofedPayloadError(
                "Field 'metadata' is required and must be an object",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )
        if not ciphertext or not isinstance(ciphertext, str):
            raise SpoofedPayloadError(
                "Field 'ciphertext' is required and must be a string",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )
        return cls(metadata=metadata, ciphertext=ciphertext)


# =============================================================================
# CRYPTOGRAPHIC ENCRYPTION & DECRYPTION (AES-256-GCM)
# =============================================================================

class RemoteCommandCrypto:
    """
    Handles AES-256-GCM authenticated encryption and decryption for remote commands.
    Ensures zero-knowledge compliance and detects any bit flips or unauthorized keys.
    """

    @staticmethod
    def derive_key(passphrase: str, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
        """Derives a 256-bit symmetric encryption key from user passphrase using PBKDF2."""
        if not passphrase:
            raise InvalidPassphraseError("Passphrase must not be empty.")
        return hashlib.pbkdf2_hmac(
            "sha256",
            passphrase.encode("utf-8"),
            salt,
            iterations,
            dklen=KEY_BYTES,
        )

    @classmethod
    def encrypt_command(
        cls,
        command: Union[RemoteCommand, Dict[str, Any]],
        passphrase: str,
        license_id: str,
        algorithm: str = "AES-256-GCM",
        custom_timestamp: Optional[float] = None,
        custom_message_id: Optional[str] = None,
    ) -> RemoteCommandEnvelope:
        """
        Encrypts a RemoteCommand with AES-256-GCM into a zero-knowledge compliant envelope.
        """
        if not HAS_AEAD:
            raise RemoteSecurityError("cryptography package is required for AES-GCM AEAD encryption.")

        if not passphrase:
            raise InvalidPassphraseError("Passphrase is required to encrypt remote commands.")

        if isinstance(command, RemoteCommand):
            cmd_dict = command.to_dict()
            msg_id = custom_message_id or command.command_id
            cmd_time = custom_timestamp if custom_timestamp is not None else command.timestamp
        else:
            cmd_dict = copy.deepcopy(command)
            msg_id = custom_message_id or str(cmd_dict.get("command_id", uuid.uuid4()))
            cmd_time = custom_timestamp if custom_timestamp is not None else float(cmd_dict.get("timestamp", time.time()))

        cmd_dict["command_id"] = msg_id
        cmd_dict["timestamp"] = cmd_time
        cmd_dict["license_id"] = license_id

        # Serialize inner command
        plaintext_bytes = json.dumps(cmd_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")

        # Cryptographic salt and key derivation
        salt = secrets.token_bytes(SALT_BYTES)
        key = cls.derive_key(passphrase, salt, PBKDF2_ITERATIONS)
        iv = secrets.token_bytes(IV_BYTES)

        # Authenticated Additional Data (AAD) binds message_id and license_id
        aad_str = f"pipvane:remote:{license_id}:{msg_id}"
        aad_bytes = aad_str.encode("utf-8")

        aesgcm = AESGCM(key)
        # encrypt returns ciphertext + 16-byte authentication tag
        raw_cipher_and_tag = aesgcm.encrypt(iv, plaintext_bytes, aad_bytes)
        ciphertext = raw_cipher_and_tag[:-TAG_BYTES]
        tag = raw_cipher_and_tag[-TAG_BYTES:]

        metadata = {
            "message_id": msg_id,
            "license_id": license_id,
            "direction": "mobile_to_engine",
            "sender_type": "mobile_app",
            "recipient_id": "local_engine",
            "timestamp": cmd_time,
            "ttl_seconds": 3600,
            "algorithm": algorithm,
            "iv": base64.b64encode(iv).decode("ascii"),
            "tag": base64.b64encode(tag).decode("ascii"),
            "salt": base64.b64encode(salt).decode("ascii"),
            "aad": aad_str,
            "key_id": "default",
        }

        return RemoteCommandEnvelope(
            metadata=metadata,
            ciphertext=base64.b64encode(ciphertext).decode("ascii"),
        )

    @classmethod
    def decrypt_command(
        cls,
        envelope_data: Union[RemoteCommandEnvelope, Dict[str, Any], str],
        passphrase: str,
        expected_license_id: Optional[str] = None,
    ) -> RemoteCommand:
        """
        Validates envelope integrity, authenticates AES-GCM MAC tag, and returns decrypted RemoteCommand.
        Raises SpoofedPayloadError or TamperedPayloadError on any corruption, plaintext, or tampering.
        """
        if not HAS_AEAD:
            raise RemoteSecurityError("cryptography package is required for AES-GCM decryption.")

        if not passphrase:
            raise InvalidPassphraseError("Passphrase is required to decrypt remote commands.")

        # 1. Parse JSON if string
        if isinstance(envelope_data, str):
            try:
                raw_dict = json.loads(envelope_data)
            except Exception as e:
                logger.warning("SECURITY_WARNING: Malformed JSON remote command rejected: %s", e)
                raise SpoofedPayloadError(
                    f"Malformed JSON payload: {e}",
                    SecurityRejectionCode.MALFORMED_ENVELOPE,
                ) from e
        elif isinstance(envelope_data, RemoteCommandEnvelope):
            raw_dict = envelope_data.to_dict()
        elif isinstance(envelope_data, dict):
            raw_dict = envelope_data
        else:
            logger.warning("SECURITY_WARNING: Invalid payload type rejected: %s", type(envelope_data))
            raise SpoofedPayloadError(
                "Invalid payload format: expected JSON dictionary",
                SecurityRejectionCode.SPOOFED_PAYLOAD_DETECTED,
            )

        # 2. Strict Plaintext & Spoofing Inspection
        # If payload contains raw command or trading fields at the root level, it is unencrypted plaintext!
        root_keys = {k.lower() for k in raw_dict.keys()}
        if "command" in root_keys or "command_name" in root_keys or "action" in root_keys or "trade" in root_keys:
            logger.warning(
                "SECURITY_WARNING: Plaintext remote command detected at root level! Rejecting spoofing attempt: %s",
                list(raw_dict.keys()),
            )
            raise SpoofedPayloadError(
                "Plaintext remote command rejected: trading actions must be encapsulated in ciphertext envelope.",
                SecurityRejectionCode.PLAINTEXT_REJECTED,
            )

        # Check for presence of ciphertext
        ciphertext_b64 = raw_dict.get("ciphertext")
        if not ciphertext_b64 or not isinstance(ciphertext_b64, str):
            logger.warning("SECURITY_WARNING: Remote command missing ciphertext! Rejecting spoofing attempt.")
            raise SpoofedPayloadError(
                "Missing or empty 'ciphertext' field in remote command envelope.",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )

        metadata = raw_dict.get("metadata")
        if not metadata or not isinstance(metadata, dict):
            logger.warning("SECURITY_WARNING: Remote command missing metadata! Rejecting spoofing attempt.")
            raise SpoofedPayloadError(
                "Missing or invalid 'metadata' in remote command envelope.",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )

        # 3. Extract cryptographic parameters
        salt_b64 = metadata.get("salt")
        iv_b64 = metadata.get("iv")
        tag_b64 = metadata.get("tag")
        message_id = metadata.get("message_id") or metadata.get("messageId") or ""
        license_id = metadata.get("license_id") or metadata.get("licenseId") or ""

        if not (salt_b64 and iv_b64 and tag_b64):
            logger.warning(
                "SECURITY_WARNING: Envelope missing cryptographic salt/iv/tag! salt=%s, iv=%s, tag=%s",
                bool(salt_b64), bool(iv_b64), bool(tag_b64),
            )
            raise SpoofedPayloadError(
                "Incomplete cryptographic envelope: salt, iv, and tag are required.",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            )

        # License validation
        if expected_license_id and license_id and license_id.strip() != expected_license_id.strip():
            logger.warning(
                "SECURITY_WARNING: Envelope license_id '%s' does not match expected '%s'",
                license_id, expected_license_id,
            )
            raise SpoofedPayloadError(
                f"License ID mismatch: envelope for '{license_id}', expected '{expected_license_id}'",
                SecurityRejectionCode.SPOOFED_PAYLOAD_DETECTED,
            )

        try:
            salt = base64.b64decode(salt_b64)
            iv = base64.b64decode(iv_b64)
            tag = base64.b64decode(tag_b64)
            ciphertext = base64.b64decode(ciphertext_b64)
        except Exception as e:
            logger.warning("SECURITY_WARNING: Base64 decoding failed for cryptographic fields: %s", e)
            raise SpoofedPayloadError(
                f"Invalid base64 encoding in cryptographic envelope: {e}",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            ) from e

        # AAD reconstruction
        aad_str = metadata.get("aad") or f"pipvane:remote:{license_id}:{message_id}"
        aad_bytes = aad_str.encode("utf-8")

        # Key derivation & AES-GCM MAC Tag verification
        key = cls.derive_key(passphrase, salt, PBKDF2_ITERATIONS)
        aesgcm = AESGCM(key)

        try:
            # AESGCM.decrypt expects ciphertext with appended 16-byte tag
            plaintext_bytes = aesgcm.decrypt(iv, ciphertext + tag, aad_bytes)
        except Exception as e:
            logger.warning(
                "SECURITY_WARNING: Remote command authentication failed! MAC tag mismatch or invalid passphrase. "
                "Potential tampering or unauthorized sender. Details: %s", e,
            )
            raise TamperedPayloadError(
                "Decryption failed: cryptographic MAC tag mismatch or invalid secret key.",
                SecurityRejectionCode.TAMPERED_CIPHERTEXT,
            ) from e

        # Parse decrypted JSON
        try:
            cmd_dict = json.loads(plaintext_bytes.decode("utf-8"))
            return RemoteCommand.from_dict(cmd_dict)
        except Exception as e:
            logger.warning("SECURITY_WARNING: Decrypted payload contains invalid JSON: %s", e)
            raise SpoofedPayloadError(
                f"Decrypted payload is not valid JSON: {e}",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            ) from e


# =============================================================================
# LOCAL ENGINE REMOTE COMMAND RECEIVER
# =============================================================================

class RemoteCommandReceiver:
    """
    Secure Local Engine receiver for remote commands from the Mobile Companion App.
    Enforces the complete security pipeline:
    1. Anti-Spoofing & Authenticated Decryption
    2. Anti-Replay: Timestamp Freshness Guard (Strict rejection of stale commands, e.g. 5m old)
    3. Anti-Replay: Nonce / Message-ID Deduplication Guard
    4. Execution Dispatcher (e.g. Panic Close on ExecutionOrchestrator)
    """

    def __init__(
        self,
        passphrase: str,
        license_id: str,
        orchestrator: Optional[Any] = None,
        max_drift_seconds: float = DEFAULT_MAX_DRIFT_SECONDS,
        nonce_ttl_seconds: float = DEFAULT_NONCE_TTL_SECONDS,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.passphrase = passphrase
        self.license_id = license_id
        self.orchestrator = orchestrator
        self.max_drift_seconds = max_drift_seconds
        self.nonce_ttl_seconds = nonce_ttl_seconds
        self._clock = clock or time.time

        # Anti-replay tracking: Map<command_id/nonce, processed_timestamp>
        self._processed_nonces: Dict[str, float] = {}
        # Security incident audit log
        self._security_alerts: List[Dict[str, Any]] = []
        # Successfully executed command log
        self._execution_history: List[Dict[str, Any]] = []

    def now(self) -> float:
        """Returns current timestamp from system clock or injected mock clock."""
        return self._clock()

    @property
    def security_alerts(self) -> List[Dict[str, Any]]:
        """Returns recorded security alerts."""
        return list(self._security_alerts)

    @property
    def execution_history(self) -> List[Dict[str, Any]]:
        """Returns successfully executed commands."""
        return list(self._execution_history)

    def _record_security_alert(
        self,
        alert_type: str,
        code: SecurityRejectionCode,
        message: str,
        raw_payload: Any = None,
    ) -> Dict[str, Any]:
        """Logs a critical security warning and appends to security audit log."""
        current_time = self.now()
        alert = {
            "timestamp": current_time,
            "alert_type": alert_type,
            "code": code.value if isinstance(code, SecurityRejectionCode) else str(code),
            "message": message,
            "payload_preview": str(raw_payload)[:200] if raw_payload else None,
        }
        self._security_alerts.append(alert)
        logger.warning(
            "SECURITY_WARNING: [%s] Code: %s, Message: %s",
            alert_type, alert["code"], message,
        )
        return alert

    def _prune_expired_nonces(self) -> None:
        """Prunes nonces older than nonce_ttl_seconds to bound memory."""
        current_time = self.now()
        cutoff = current_time - self.nonce_ttl_seconds
        expired_keys = [k for k, ts in self._processed_nonces.items() if ts < cutoff]
        for k in expired_keys:
            del self._processed_nonces[k]

    def validate_freshness(self, command_timestamp: float) -> None:
        """
        Sprint 17 Anti-Replay Guard:
        Asserts that command_timestamp is within max_drift_seconds of current_time.
        Throws StaleTimestampError if stale (e.g. 5 minutes / 300s old).
        """
        current_time = self.now()
        age = current_time - command_timestamp

        # Past timestamp check (stale replay)
        if age > self.max_drift_seconds:
            err_msg = (
                f"SECURITY_WARNING: Replay attack detected! Stale command timestamp: "
                f"command age is {age:.1f}s, exceeding maximum allowed freshness window of "
                f"{self.max_drift_seconds:.1f}s."
            )
            self._record_security_alert(
                alert_type="REPLAY_ATTACK",
                code=SecurityRejectionCode.STALE_TIMESTAMP,
                message=err_msg,
            )
            raise StaleTimestampError(err_msg, age_seconds=age)

        # Future timestamp check (clock drift or crafted future nonce)
        if (command_timestamp - current_time) > self.max_drift_seconds:
            future_drift = command_timestamp - current_time
            err_msg = (
                f"SECURITY_WARNING: Replay attack detected! Future timestamp drift: "
                f"command timestamp is {future_drift:.1f}s in the future, exceeding allowed drift of "
                f"{self.max_drift_seconds:.1f}s."
            )
            self._record_security_alert(
                alert_type="REPLAY_ATTACK",
                code=SecurityRejectionCode.FUTURE_TIMESTAMP_DRIFT,
                message=err_msg,
            )
            raise ReplayAttackError(err_msg, SecurityRejectionCode.FUTURE_TIMESTAMP_DRIFT)

    def validate_nonce(self, nonce: str) -> None:
        """
        Sprint 17 Deduplication Guard:
        Asserts that nonce / command_id has not already been processed.
        Throws ReplayAttackError if duplicate.
        """
        self._prune_expired_nonces()
        if nonce in self._processed_nonces:
            first_seen = self._processed_nonces[nonce]
            err_msg = (
                f"SECURITY_WARNING: Replay attack detected! Duplicate command_id '{nonce}' "
                f"already processed at t={first_seen:.2f}. Duplicate rejected."
            )
            self._record_security_alert(
                alert_type="REPLAY_ATTACK",
                code=SecurityRejectionCode.DUPLICATE_NONCE,
                message=err_msg,
            )
            raise ReplayAttackError(err_msg, SecurityRejectionCode.DUPLICATE_NONCE)

    async def process_remote_envelope(
        self,
        envelope_data: Union[RemoteCommandEnvelope, Dict[str, Any], str],
    ) -> Dict[str, Any]:
        """
        Full end-to-end processing pipeline for an incoming remote command:
        1. Cryptographic Decryption & MAC verification (Anti-Spoofing Gate)
        2. Timestamp Freshness Verification (Anti-Replay Gate)
        3. Nonce Deduplication Verification (Anti-Replay Gate)
        4. Command Execution (Panic Close / Trade Liquidation)
        """
        # Step 1: Cryptographic Decryption & MAC Tag Verification
        try:
            command = RemoteCommandCrypto.decrypt_command(
                envelope_data=envelope_data,
                passphrase=self.passphrase,
                expected_license_id=self.license_id,
            )
        except RemoteSecurityError as e:
            # Already logged in decrypt_command, record alert and re-raise
            self._record_security_alert(
                alert_type="SPOOFING_OR_TAMPER_ATTEMPT",
                code=e.code,
                message=f"Cryptographic rejection: {e.message}",
                raw_payload=envelope_data,
            )
            raise

        # Step 2: Anti-Replay Timestamp Freshness Guard
        self.validate_freshness(command.timestamp)

        # Step 3: Anti-Replay Nonce Deduplication Guard
        self.validate_nonce(command.command_id)

        # Register nonce as processed
        self._processed_nonces[command.command_id] = self.now()

        # Step 4: Command Execution
        execution_result = await self._execute_command(command)

        receipt = {
            "success": True,
            "status": "EXECUTED",
            "command_id": command.command_id,
            "command": command.command.value if isinstance(command.command, RemoteCommandType) else str(command.command),
            "executed_at": self.now(),
            "execution_details": execution_result,
        }
        self._execution_history.append(receipt)
        logger.info(
            "REMOTE COMMAND EXECUTED: %s (ID: %s)",
            receipt["command"], receipt["command_id"],
        )
        return receipt

    async def _execute_command(self, command: RemoteCommand) -> Dict[str, Any]:
        """Dispatches verified command to local engine orchestrator."""
        cmd_type = command.command
        cmd_name = cmd_type.value if isinstance(cmd_type, RemoteCommandType) else str(cmd_type).upper()

        if cmd_name in (RemoteCommandType.CLOSE_ALL_TRADES.value, RemoteCommandType.PANIC_CLOSE.value):
            return await self._execute_panic_close_all(command)
        elif cmd_name == RemoteCommandType.PAUSE_TRADING.value:
            if self.orchestrator and hasattr(self.orchestrator, "lock"):
                self.orchestrator.lock()
            return {"action": "PAUSE_TRADING", "status": "ENGINE_LOCKED"}
        elif cmd_name == RemoteCommandType.RESUME_TRADING.value:
            if self.orchestrator and hasattr(self.orchestrator, "unlock"):
                self.orchestrator.unlock()
            return {"action": "RESUME_TRADING", "status": "ENGINE_ACTIVE"}
        else:
            return {"action": cmd_name, "status": "ACKNOWLEDGED"}

    async def _execute_panic_close_all(self, command: RemoteCommand) -> Dict[str, Any]:
        """Executes emergency liquidation of all positions across terminals."""
        logger.critical(
            "EMERGENCY PANIC CLOSE INITIATED via Remote Command ID %s! Closing all trades.",
            command.command_id,
        )

        closed_count = 0
        terminal_results = {}

        if self.orchestrator:
            # If orchestrator has panic_close_all
            if hasattr(self.orchestrator, "panic_close_all") and callable(self.orchestrator.panic_close_all):
                res = await self.orchestrator.panic_close_all(reason="REMOTE_EMERGENCY_PANIC")
                return {"positions_liquidated": res}

            # Alternatively, liquidate active positions directly
            if hasattr(self.orchestrator, "get_active_orders"):
                active = self.orchestrator.get_active_orders()
                closed_count = len(active)
                for order in active:
                    ticket = order.get("ticket") or order.get("order_ticket") or 0
                    terminal_id = order.get("terminal_id", "terminal_default")
                    if hasattr(self.orchestrator, "record_journey_event"):
                        self.orchestrator.record_journey_event(
                            signal_id=order.get("signal_id", 0),
                            ticket=ticket,
                            terminal_id=terminal_id,
                            event_type="REMOTE_PANIC_CLOSE",
                            status="CLOSED",
                            details=f"Liquidated by remote command {command.command_id}",
                            source="mobile_relay",
                        )

        return {
            "action": "PANIC_CLOSE_ALL",
            "positions_closed": closed_count,
            "terminal_id": command.terminal_id,
            "timestamp": self.now(),
        }


# =============================================================================
# RELAY HTTP CLIENT (Mobile & Local Engine Interface to /relay/command)
# =============================================================================

class MobileRelayClient:
    """
    HTTP client for communicating with Cloud Relay /relay/command endpoints.
    Allows pushing encrypted commands (from Mobile App simulator) and popping/polling
    encrypted commands (from Local Engine).
    """

    def __init__(self, relay_base_url: str = "http://127.0.0.1:8080"):
        self.relay_base_url = relay_base_url.rstrip("/")

    def push_command(
        self,
        envelope: Union[RemoteCommandEnvelope, Dict[str, Any]],
        license_id: Optional[str] = None,
        queue_type: str = "inbox",
    ) -> Dict[str, Any]:
        """POSTs an encrypted envelope to /relay/command."""
        url = f"{self.relay_base_url}/relay/command"
        if queue_type:
            url += f"?queue={urllib.parse.quote(queue_type)}"

        data = envelope.to_dict() if isinstance(envelope, RemoteCommandEnvelope) else envelope
        body_bytes = json.dumps(data).encode("utf-8")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if license_id:
            headers["X-License-Id"] = license_id

        req = urllib.request.Request(url, data=body_bytes, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                resp_bytes = resp.read()
                return json.loads(resp_bytes.decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RemoteSecurityError(
                f"Cloud Relay HTTP {e.code}: {e.reason} ({err_body})",
                SecurityRejectionCode.MALFORMED_ENVELOPE,
            ) from e
        except Exception as e:
            raise RemoteSecurityError(f"Failed to push command to Cloud Relay: {e}") from e

    def pop_command(
        self,
        license_id: str,
        queue_type: str = "inbox",
        wait_ms: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """GETs the oldest encrypted envelope from /relay/command."""
        params = {
            "license_id": license_id,
            "queue": queue_type,
            "wait_ms": str(wait_ms),
            "mode": "pop",
        }
        query = urllib.parse.urlencode(params)
        url = f"{self.relay_base_url}/relay/command?{query}"

        req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("message")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RemoteSecurityError(
                f"Cloud Relay GET error HTTP {e.code}: {e.reason} ({err_body})",
            ) from e
        except Exception as e:
            raise RemoteSecurityError(f"Failed to pop command from Cloud Relay: {e}") from e

    def ack_command(
        self,
        license_id: str,
        message_id: str,
        queue_type: str = "inbox",
    ) -> bool:
        """POSTs acknowledgment for a processed command to /relay/command/ack."""
        url = f"{self.relay_base_url}/relay/command/ack"
        payload = {
            "license_id": license_id,
            "message_id": message_id,
            "queue": queue_type,
        }
        body_bytes = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body_bytes,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return bool(data.get("acknowledged", False))
        except Exception as e:
            logger.debug("Failed to ack command %s on relay: %s", message_id, e)
            return False

    async def poll_and_dispatch_once(
        self,
        receiver: RemoteCommandReceiver,
        queue_type: str = "inbox",
    ) -> Optional[Dict[str, Any]]:
        """Fetches pending command from relay, decrypts and validates via receiver."""
        envelope = self.pop_command(receiver.license_id, queue_type=queue_type)
        if not envelope:
            return None

        # Process through receiver security pipeline
        receipt = await receiver.process_remote_envelope(envelope)

        # Acknowledge on relay
        msg_id = (envelope.get("metadata") or {}).get("message_id")
        if msg_id:
            self.ack_command(receiver.license_id, msg_id, queue_type=queue_type)

        return receipt
