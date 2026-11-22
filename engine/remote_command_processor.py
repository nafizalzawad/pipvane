"""
Pipvane v3: Remote Command Processor (Sprint 17 Directive)
==========================================================
Polls the Cloud Relay outbox, decrypts incoming mobile commands safely using
end-to-end encryption (AES-256-GCM / NaCl-compatible), prevents replay attacks
(dropping commands older than 60 seconds and rejecting duplicate command IDs), and
dispatches decrypted actions (e.g. CLOSE_ALL, CLOSE_POSITION, PAUSE_TRADING) to the
ExecutionOrchestrator manual override functions.

Core Mandates:
1. Relay Poller:
   - Background asyncio task polling Cloud Relay outbox every 2-3 seconds.
   - Robust error handling with exponential backoff on transient transport failures.
2. Decryption & Validation:
   - Standard E2EE AES-256-GCM with a shared 256-bit symmetric key exchanged via QR code.
   - Strict Anti-Replay Defense:
     * Drops commands older than 60 seconds (age > 60.0s).
     * Rejects future clock skew (> 15.0s).
     * Sliding-window deduplication cache rejecting replayed command IDs within the TTL window.
     * AES-GCM authentication tag verification ensuring zero cloud tampering.
3. Action Dispatcher:
   - Maps decrypted commands (e.g. {"action": "CLOSE_ALL", "channel": "GoldVIP"})
     to the ExecutionOrchestrator manual override functions.
4. Strict Mode & Zero-Knowledge:
   - Operates entirely in user-space with no elevated privileges.
   - Cloud Relay only ever sees opaque ciphertext and routing metadata.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    HAS_AESGCM = True
except ImportError:
    HAS_AESGCM = False

logger = logging.getLogger("pipvane.engine.remote_command")

# Timing and Security Constraints
DEFAULT_POLLING_INTERVAL_S: float = 2.5   # Every 2-3 seconds (Directive requirement)
MAX_COMMAND_AGE_S: float = 60.0          # Drop commands older than 60s (Anti-Replay)
FUTURE_SKEW_TOLERANCE_S: float = 15.0    # Reject future clock skew
CACHE_TTL_S: float = 180.0               # Sliding window deduplication cache TTL
KEY_BYTES: int = 32                      # 256-bit symmetric key
IV_BYTES: int = 12                       # Standard GCM IV length
TAG_BYTES: int = 16                      # 128-bit authentication tag


# =============================================================================
# EXCEPTIONS
# =============================================================================

class RemoteCommandError(Exception):
    """Base exception for remote command processing and E2EE errors."""
    pass


class CommandAuthenticationError(RemoteCommandError):
    """Raised when GCM authentication tag verification fails (tampered payload or wrong key)."""
    pass


class CommandExpiredError(RemoteCommandError):
    """Raised when a command is older than 60 seconds (anti-replay attack defense)."""
    pass


class ReplayAttackError(RemoteCommandError):
    """Raised when a command ID has already been executed within the anti-replay window."""
    pass


class InvalidCommandPayloadError(RemoteCommandError):
    """Raised when the decrypted command payload is malformed or missing required fields."""
    pass


class RelayPollingError(RemoteCommandError):
    """Raised when polling the Cloud Relay outbox encounters a network or transport failure."""
    pass


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class RemoteCommandEnvelope:
    """Wire-format envelope matching Cloud Relay outbox specification."""
    message_id: str
    license_id: str
    ciphertext: str                      # Base64-encoded ciphertext
    iv: str                              # Base64-encoded 12-byte IV
    tag: str                             # Base64-encoded 16-byte GCM authentication tag
    timestamp: float = field(default_factory=time.time)
    aad: str = ""                        # Additional Authenticated Data
    algorithm: str = "AES-256-GCM"
    direction: str = "mobile_to_engine"
    sender_type: str = "mobile_app"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": {
                "message_id": self.message_id,
                "license_id": self.license_id,
                "direction": self.direction,
                "sender_type": self.sender_type,
                "timestamp": self.timestamp,
                "algorithm": self.algorithm,
                "iv": self.iv,
                "tag": self.tag,
                "aad": self.aad,
            },
            "ciphertext": self.ciphertext,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RemoteCommandEnvelope:
        if not isinstance(data, dict):
            raise InvalidCommandPayloadError(f"Envelope must be a dict, got {type(data).__name__}")

        metadata = data.get("metadata", {})
        ciphertext = data.get("ciphertext", "")

        msg_id = (
            metadata.get("message_id")
            or metadata.get("messageId")
            or data.get("message_id")
            or str(uuid.uuid4())
        )
        lic_id = (
            metadata.get("license_id")
            or metadata.get("licenseId")
            or data.get("license_id")
            or ""
        )
        iv = metadata.get("iv") or data.get("iv") or ""
        tag = metadata.get("tag") or data.get("tag") or ""
        aad = metadata.get("aad") or data.get("aad") or ""
        ts = float(metadata.get("timestamp") or data.get("timestamp") or time.time())
        algo = metadata.get("algorithm") or data.get("algorithm") or "AES-256-GCM"
        direction = metadata.get("direction") or "mobile_to_engine"
        sender_type = metadata.get("sender_type") or "mobile_app"

        return cls(
            message_id=str(msg_id),
            license_id=str(lic_id),
            ciphertext=str(ciphertext),
            iv=str(iv),
            tag=str(tag),
            timestamp=ts,
            aad=str(aad),
            algorithm=str(algo),
            direction=str(direction),
            sender_type=str(sender_type),
        )


@dataclass
class DecryptedCommand:
    """Authenticated and decrypted mobile command payload."""
    command_id: str
    action: str                          # e.g., "CLOSE_ALL", "CLOSE_POSITION", "PAUSE_TRADING", "RESUME_TRADING"
    timestamp: float                    # Unix timestamp (seconds) when command was generated
    channel: Optional[Union[str, int]] = None # Channel name (e.g. "GoldVIP") or channel ID
    symbol: Optional[str] = None         # Currency or symbol (e.g. "XAUUSD")
    ticket: Optional[int] = None        # Broker order ticket
    terminal_id: Optional[str] = None   # Target terminal ID
    params: Dict[str, Any] = field(default_factory=dict) # Other parameters (sl, tp, etc.)
    source_device_id: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "command_id": self.command_id,
            "action": self.action,
            "timestamp": self.timestamp,
            "channel": self.channel,
            "symbol": self.symbol,
            "ticket": self.ticket,
            "terminal_id": self.terminal_id,
            "params": self.params,
            "source_device_id": self.source_device_id,
        }


@dataclass
class CommandExecutionResult:
    """Execution telemetry and audit result for a processed remote command."""
    command_id: str
    action: str
    success: bool
    timestamp: float = field(default_factory=time.time)
    status: str = "COMPLETED"            # "COMPLETED", "DROPPED_EXPIRED", "DROPPED_REPLAY", "DROPPED_AUTH_FAILED", "FAILED"
    dispatched_trades_count: int = 0
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


# =============================================================================
# CRYPTOGRAPHIC UTILITY (AES-256-GCM E2EE & QR PAIRING)
# =============================================================================

class RemoteCommandCrypto:
    """
    Zero-Knowledge End-to-End Encryption engine for Mobile Commands.
    Exclusively utilizes AES-256-GCM authenticated encryption with a pre-shared 256-bit
    symmetric key exchanged between the Mobile App and Local Engine via QR code.
    """

    @staticmethod
    def generate_shared_key() -> bytes:
        """Generates a cryptographically secure 256-bit symmetric pairing key."""
        return secrets.token_bytes(KEY_BYTES)

    @staticmethod
    def export_qr_pairing_payload(
        license_id: str,
        shared_key: bytes,
        device_name: str = "Desktop Engine",
    ) -> str:
        """
        Exports the QR code pairing payload string to be scanned by the mobile app.
        The payload encodes the license_id, base64-encoded symmetric key, and pairing ID.
        """
        if len(shared_key) != KEY_BYTES:
            raise ValueError(f"Shared key must be strictly {KEY_BYTES} bytes (256 bits).")

        pairing_dict = {
            "pipvane_pairing_version": 1,
            "license_id": license_id.strip(),
            "shared_key": base64.b64encode(shared_key).decode("ascii"),
            "pairing_id": uuid.uuid4().hex,
            "device_name": device_name,
            "created_at": time.time(),
        }
        return json.dumps(pairing_dict, separators=(",", ":"))

    @staticmethod
    def import_qr_pairing_payload(pairing_payload: str) -> Tuple[str, bytes]:
        """
        Parses a QR pairing payload string and extracts (license_id, shared_key).
        """
        try:
            data = json.loads(pairing_payload)
            license_id = str(data["license_id"]).strip()
            key_b64 = str(data["shared_key"]).strip()
            shared_key = base64.b64decode(key_b64)
            if len(shared_key) != KEY_BYTES:
                raise ValueError("Invalid shared key length.")
            return license_id, shared_key
        except Exception as e:
            raise RemoteCommandError(f"Failed to parse QR pairing payload: {e}") from e

    @classmethod
    def encrypt_command(
        cls,
        command_payload: Union[DecryptedCommand, Dict[str, Any]],
        shared_key: bytes,
        license_id: str = "",
    ) -> RemoteCommandEnvelope:
        """
        Encrypts a mobile command using AES-256-GCM. Returns a RemoteCommandEnvelope.
        """
        if not HAS_AESGCM:
            raise RemoteCommandError("cryptography package is required for AES-GCM encryption.")

        if len(shared_key) != KEY_BYTES:
            raise ValueError(f"Shared key must be strictly {KEY_BYTES} bytes.")

        if isinstance(command_payload, DecryptedCommand):
            payload_dict = command_payload.to_dict()
            cmd_id = command_payload.command_id
            cmd_ts = command_payload.timestamp
        else:
            payload_dict = dict(command_payload)
            cmd_id = str(payload_dict.get("command_id") or uuid.uuid4())
            cmd_ts = float(payload_dict.get("timestamp") or time.time())
            payload_dict["command_id"] = cmd_id
            payload_dict["timestamp"] = cmd_ts

        target_lic = license_id.strip() or str(payload_dict.get("license_id", "UNKNOWN"))
        plaintext_bytes = json.dumps(payload_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")

        iv = secrets.token_bytes(IV_BYTES)
        aad_str = f"pipvane:mobile_cmd:{target_lic}:{cmd_id}"
        aad_bytes = aad_str.encode("utf-8")

        aesgcm = AESGCM(shared_key)
        # aesgcm.encrypt returns ciphertext concatenated with 16-byte authentication tag
        raw_cipher_and_tag = aesgcm.encrypt(iv, plaintext_bytes, aad_bytes)
        ciphertext = raw_cipher_and_tag[:-TAG_BYTES]
        tag = raw_cipher_and_tag[-TAG_BYTES:]

        return RemoteCommandEnvelope(
            message_id=cmd_id,
            license_id=target_lic,
            ciphertext=base64.b64encode(ciphertext).decode("ascii"),
            iv=base64.b64encode(iv).decode("ascii"),
            tag=base64.b64encode(tag).decode("ascii"),
            timestamp=cmd_ts,
            aad=aad_str,
            algorithm="AES-256-GCM",
        )

    @classmethod
    def decrypt_envelope(
        cls,
        envelope: Union[RemoteCommandEnvelope, Dict[str, Any]],
        shared_key: bytes,
    ) -> DecryptedCommand:
        """
        Decrypts a RemoteCommandEnvelope using the shared key and verifies the GCM auth tag.
        Raises CommandAuthenticationError if authentication fails or payload was tampered.
        """
        if not HAS_AESGCM:
            raise RemoteCommandError("cryptography package is required for AES-GCM decryption.")

        if len(shared_key) != KEY_BYTES:
            raise ValueError(f"Shared key must be strictly {KEY_BYTES} bytes.")

        if isinstance(envelope, dict):
            env = RemoteCommandEnvelope.from_dict(envelope)
        else:
            env = envelope

        try:
            ciphertext_bytes = base64.b64decode(env.ciphertext)
            iv_bytes = base64.b64decode(env.iv)
            tag_bytes = base64.b64decode(env.tag) if env.tag else b""
        except Exception as e:
            raise CommandAuthenticationError(f"Base64 decoding failed for command envelope: {e}") from e

        if len(iv_bytes) != IV_BYTES:
            raise CommandAuthenticationError(f"Invalid IV length: expected {IV_BYTES} bytes, got {len(iv_bytes)} bytes.")

        # Reconstruct combined ciphertext + tag for AESGCM decrypt
        combined_cipher = ciphertext_bytes + tag_bytes
        aad_bytes = env.aad.encode("utf-8") if env.aad else None

        aesgcm = AESGCM(shared_key)
        try:
            plaintext_bytes = aesgcm.decrypt(iv_bytes, combined_cipher, aad_bytes)
        except Exception as e:
            raise CommandAuthenticationError(
                "AES-GCM authentication tag verification failed. Payload was tampered, "
                "corrupted, or encrypted with an incorrect pairing key."
            ) from e

        try:
            payload = json.loads(plaintext_bytes.decode("utf-8"))
        except Exception as e:
            raise InvalidCommandPayloadError(f"Decrypted plaintext is not valid JSON: {e}") from e

        action = str(payload.get("action", "")).strip()
        if not action:
            raise InvalidCommandPayloadError("Command payload is missing required 'action' field.")

        cmd_id = str(payload.get("command_id") or env.message_id)
        cmd_ts = float(payload.get("timestamp") or env.timestamp)

        # Extract standard optional parameters
        channel = payload.get("channel")
        symbol = payload.get("symbol")
        ticket = payload.get("ticket")
        terminal_id = payload.get("terminal_id")

        if ticket is not None:
            try:
                ticket = int(ticket)
            except (ValueError, TypeError):
                ticket = None

        params = dict(payload.get("params", {}))
        for k, v in payload.items():
            if k not in ("action", "command_id", "timestamp", "channel", "symbol", "ticket", "terminal_id", "params", "source_device_id"):
                params[k] = v

        return DecryptedCommand(
            command_id=cmd_id,
            action=action.upper(),
            timestamp=cmd_ts,
            channel=channel,
            symbol=str(symbol).upper() if symbol else None,
            ticket=ticket,
            terminal_id=str(terminal_id) if terminal_id else None,
            params=params,
            source_device_id=payload.get("source_device_id"),
            raw_payload=payload,
        )


# =============================================================================
# REPLAY ATTACK GUARD
# =============================================================================

class ReplayAttackGuard:
    """
    Sliding-window Anti-Replay Attack Guard.
    Enforces two strict defenses:
    1. Timestamp Validation: Drops any command whose generation timestamp is older than
       60 seconds (age > 60.0s), or whose timestamp is skewed into the future (> 15.0s).
    2. Deduplication Cache: Strictly tracks executed command IDs within a sliding TTL
       window, rejecting any reused command ID even if received within the 60s limit.
    """

    def __init__(
        self,
        max_age_s: float = MAX_COMMAND_AGE_S,
        future_skew_s: float = FUTURE_SKEW_TOLERANCE_S,
        cache_ttl_s: float = CACHE_TTL_S,
        time_provider: Optional[Callable[[], float]] = None,
    ):
        self.max_age_s = float(max_age_s)
        self.future_skew_s = float(future_skew_s)
        self.cache_ttl_s = float(cache_ttl_s)
        self.time_provider = time_provider or time.time
        # command_id -> executed_at_timestamp
        self._seen_command_ids: Dict[str, float] = {}

    def validate_and_record(self, command: DecryptedCommand) -> None:
        """
        Validates the command against timestamp age and replay deduplication cache.
        Raises CommandExpiredError or ReplayAttackError if validation fails.
        """
        now = self.time_provider()
        cmd_ts = float(command.timestamp)

        # 1. Freshness Check: Age > 60.0s (Directive mandate)
        age = now - cmd_ts
        if age > self.max_age_s:
            logger.warning(
                "REPLAY ATTACK / EXPIRED COMMAND DROPPED: Command %s is %.2fs old (exceeds %.1fs limit).",
                command.command_id,
                age,
                self.max_age_s,
            )
            raise CommandExpiredError(
                f"Command timestamp {cmd_ts:.2f} is older than {self.max_age_s}s limit (age: {age:.2f}s)."
            )

        # Future Clock Skew Check
        if (cmd_ts - now) > self.future_skew_s:
            logger.warning(
                "CLOCK SKEW REJECTED: Command %s timestamp is in the future by %.2fs.",
                command.command_id,
                cmd_ts - now,
            )
            raise CommandExpiredError(
                f"Command timestamp is skewed into future by {cmd_ts - now:.2f}s."
            )

        # 2. Sliding Window Deduplication Check
        self._prune_expired(now)

        if command.command_id in self._seen_command_ids:
            prior_ts = self._seen_command_ids[command.command_id]
            logger.critical(
                "REPLAY ATTACK PREVENTED: Command ID '%s' was already executed at %.2f (replay attempt at %.2f).",
                command.command_id,
                prior_ts,
                now,
            )
            raise ReplayAttackError(
                f"Replay attack detected: Command ID '{command.command_id}' has already been processed."
            )

        # Record command ID into sliding window cache
        self._seen_command_ids[command.command_id] = now

    def _prune_expired(self, now: float) -> None:
        """Prunes cache entries older than cache_ttl_s."""
        expired_ids = [
            cid for cid, ts in self._seen_command_ids.items()
            if (now - ts) > self.cache_ttl_s
        ]
        for cid in expired_ids:
            del self._seen_command_ids[cid]

    def clear(self) -> None:
        """Clears the deduplication cache."""
        self._seen_command_ids.clear()

    @property
    def seen_count(self) -> int:
        return len(self._seen_command_ids)


# =============================================================================
# REMOTE COMMAND PROCESSOR (RELAY POLLER + ACTION DISPATCHER)
# =============================================================================

class RemoteCommandProcessor:
    """
    Core Remote Command Processor (Sprint 17).
    - Runs a continuous background task polling Cloud Relay outbox every 2-3 seconds.
    - Decrypts envelopes via AES-256-GCM using the user's paired shared key.
    - Validates freshness and defeats replay attacks via ReplayAttackGuard.
    - Maps decrypted commands to ExecutionOrchestrator manual override functions:
      * CLOSE_ALL (with channel, symbol, or account filtering)
      * CLOSE_POSITION / CLOSE_TICKET
      * CLOSE_SYMBOL
      * PAUSE_TRADING / HALT
      * RESUME_TRADING
      * MODIFY_POSITION
    """

    def __init__(
        self,
        license_id: str,
        shared_key: bytes,
        orchestrator: Optional[Any] = None,
        relay_url: str = "http://127.0.0.1:8080",
        polling_interval_s: float = DEFAULT_POLLING_INTERVAL_S,
        max_command_age_s: float = MAX_COMMAND_AGE_S,
        time_provider: Optional[Callable[[], float]] = None,
        http_transport: Optional[Callable[[str, Optional[Dict[str, Any]], str], Any]] = None,
    ):
        self.license_id = license_id.strip()
        if len(shared_key) != KEY_BYTES:
            raise ValueError(f"Shared key must be strictly {KEY_BYTES} bytes (256 bits).")
        self.shared_key = shared_key
        self.orchestrator = orchestrator
        self.relay_url = relay_url.rstrip("/")
        self.polling_interval_s = float(polling_interval_s)
        self.time_provider = time_provider or time.time
        self.http_transport = http_transport

        self.replay_guard = ReplayAttackGuard(
            max_age_s=max_command_age_s,
            time_provider=self.time_provider,
        )

        self._poller_task: Optional[asyncio.Task] = None
        self._is_running: bool = False
        self._consecutive_poll_errors: int = 0
        self._execution_history: List[CommandExecutionResult] = []

        logger.info(
            "RemoteCommandProcessor initialized for license %s (Poll Interval: %.1fs, Relay: %s)",
            self.license_id,
            self.polling_interval_s,
            self.relay_url,
        )

    # -------------------------------------------------------------------------
    # Lifecycle Management (Relay Poller Background Task)
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts the background Relay Poller task."""
        if self._is_running:
            return
        self._is_running = True
        self._poller_task = asyncio.create_task(
            self._polling_loop(),
            name="RemoteCommandRelayPoller",
        )
        logger.info("RemoteCommandProcessor background Relay Poller started.")

    async def stop(self) -> None:
        """Gracefully stops the background Relay Poller task."""
        self._is_running = False
        if self._poller_task and not self._poller_task.done():
            self._poller_task.cancel()
            try:
                await self._poller_task
            except asyncio.CancelledError:
                pass
            self._poller_task = None
        logger.info("RemoteCommandProcessor background Relay Poller stopped.")

    async def _polling_loop(self) -> None:
        """Continuous background polling loop executing every 2-3 seconds."""
        logger.info("Relay Poller loop active (polling every %.1fs)", self.polling_interval_s)
        while self._is_running:
            try:
                await self.poll_once()
                self._consecutive_poll_errors = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._consecutive_poll_errors += 1
                backoff = min(15.0, self.polling_interval_s * (1.5 ** min(self._consecutive_poll_errors, 5)))
                logger.warning(
                    "Relay Poller error (consecutive=%d): %s. Backing off for %.1fs",
                    self._consecutive_poll_errors,
                    e,
                    backoff,
                )
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    break
                continue

            # Sleep between polling intervals
            try:
                await asyncio.sleep(self.polling_interval_s)
            except asyncio.CancelledError:
                break

    # -------------------------------------------------------------------------
    # Single Polling & Execution Cycle
    # -------------------------------------------------------------------------

    async def poll_once(self) -> List[CommandExecutionResult]:
        """
        Executes a single poll against Cloud Relay outbox:
        1. Queries Cloud Relay outbox for pending encrypted envelopes.
        2. Decrypts and verifies AES-GCM envelope with user's shared key.
        3. Validates freshness and defends against replay attacks.
        4. Dispatches action to ExecutionOrchestrator manual override functions.
        5. Acknowledges/removes command from Cloud Relay.
        """
        results: List[CommandExecutionResult] = []
        envelopes = await self._fetch_outbox_envelopes()
        if not envelopes:
            return results

        for env in envelopes:
            res = await self._process_envelope(env)
            results.append(res)
            self._execution_history.append(res)

            # Acknowledge command on Cloud Relay outbox so it is consumed
            try:
                await self._ack_remote_command(env.message_id)
            except Exception as ack_err:
                logger.warning("Failed to ACK command %s on Cloud Relay: %s", env.message_id, ack_err)

        return results

    async def _process_envelope(self, envelope: RemoteCommandEnvelope) -> CommandExecutionResult:
        """Processes a single incoming command envelope with strict security validation."""
        cmd_id = envelope.message_id

        # 1. Decrypt and verify AES-GCM envelope
        try:
            command = RemoteCommandCrypto.decrypt_envelope(envelope, self.shared_key)
        except CommandAuthenticationError as auth_err:
            logger.critical("SECURITY ALERT: Command %s failed authentication: %s", cmd_id, auth_err)
            return CommandExecutionResult(
                command_id=cmd_id,
                action="UNKNOWN",
                success=False,
                status="DROPPED_AUTH_FAILED",
                error=str(auth_err),
            )
        except Exception as dec_err:
            logger.error("Failed to decrypt command %s: %s", cmd_id, dec_err)
            return CommandExecutionResult(
                command_id=cmd_id,
                action="UNKNOWN",
                success=False,
                status="FAILED",
                error=f"Decryption error: {dec_err}",
            )

        # 2. Anti-Replay & Freshness Validation (Timestamp <= 60s & Unique ID)
        try:
            self.replay_guard.validate_and_record(command)
        except CommandExpiredError as exp_err:
            logger.warning("Dropped expired command %s: %s", cmd_id, exp_err)
            return CommandExecutionResult(
                command_id=cmd_id,
                action=command.action,
                success=False,
                status="DROPPED_EXPIRED",
                error=str(exp_err),
            )
        except ReplayAttackError as rep_err:
            logger.critical("SECURITY ALERT: Dropped replayed command %s: %s", cmd_id, rep_err)
            return CommandExecutionResult(
                command_id=cmd_id,
                action=command.action,
                success=False,
                status="DROPPED_REPLAY",
                error=str(rep_err),
            )

        # 3. Action Dispatcher: Map to ExecutionOrchestrator manual override functions
        return await self.dispatch_action(command)

    # -------------------------------------------------------------------------
    # Action Dispatcher (ExecutionOrchestrator Manual Override Mapping)
    # -------------------------------------------------------------------------

    async def dispatch_action(self, command: DecryptedCommand) -> CommandExecutionResult:
        """
        Maps decrypted commands to ExecutionOrchestrator manual override functions.
        Supported Actions:
        - CLOSE_ALL: Closes open positions (optional filter by channel, symbol, terminal_id).
        - CLOSE_POSITION / CLOSE_TICKET: Closes a specific position ticket.
        - CLOSE_SYMBOL: Closes all positions matching a currency/symbol.
        - PAUSE_TRADING / HALT: Pauses automated order dispatch.
        - RESUME_TRADING: Resumes automated order dispatch.
        - MODIFY_POSITION: Modifies SL/TP for an active position.
        """
        action = command.action.upper().strip()
        cmd_id = command.command_id
        logger.info(
            "DISPATCHING REMOTE ACTION [%s] (ID=%s, Channel=%s, Symbol=%s, Ticket=%s)",
            action,
            cmd_id,
            command.channel,
            command.symbol,
            command.ticket,
        )

        if not self.orchestrator:
            logger.warning("No ExecutionOrchestrator attached. Simulating action %s (Dry Run).", action)
            return CommandExecutionResult(
                command_id=cmd_id,
                action=action,
                success=True,
                status="COMPLETED",
                details={"mode": "simulated_dry_run", "command": command.to_dict()},
            )

        try:
            # -----------------------------------------------------------------
            # 1. Action: CLOSE_ALL (e.g. {"action": "CLOSE_ALL", "channel": "GoldVIP"})
            # -----------------------------------------------------------------
            if action == "CLOSE_ALL":
                channel_param = command.channel or command.params.get("channel")
                symbol_param = command.symbol or command.params.get("symbol")
                terminal_param = command.terminal_id or command.params.get("terminal_id")

                if hasattr(self.orchestrator, "manual_close_all"):
                    results = await self.orchestrator.manual_close_all(
                        channel=channel_param,
                        symbol=symbol_param,
                        terminal_id=terminal_param,
                    )
                    success_count = sum(1 for r in results if getattr(r, "success", False))
                    return CommandExecutionResult(
                        command_id=cmd_id,
                        action="CLOSE_ALL",
                        success=True,
                        status="COMPLETED",
                        dispatched_trades_count=len(results),
                        details={
                            "channel": channel_param,
                            "symbol": symbol_param,
                            "dispatched_count": len(results),
                            "successful_closes": success_count,
                        },
                    )
                else:
                    raise RemoteCommandError("ExecutionOrchestrator missing manual_close_all method.")

            # -----------------------------------------------------------------
            # 2. Action: CLOSE_POSITION / CLOSE_TICKET
            # -----------------------------------------------------------------
            elif action in ("CLOSE_POSITION", "CLOSE_TICKET"):
                ticket_param = command.ticket or command.params.get("ticket")
                terminal_param = command.terminal_id or command.params.get("terminal_id")
                if not ticket_param:
                    raise InvalidCommandPayloadError(f"Action '{action}' requires 'ticket' parameter.")

                if hasattr(self.orchestrator, "manual_close_position"):
                    res = await self.orchestrator.manual_close_position(
                        ticket=int(ticket_param),
                        terminal_id=terminal_param,
                    )
                    success = getattr(res, "success", True) if res is not None else False
                    return CommandExecutionResult(
                        command_id=cmd_id,
                        action="CLOSE_POSITION",
                        success=success,
                        status="COMPLETED" if success else "FAILED",
                        dispatched_trades_count=1 if res is not None else 0,
                        details={"ticket": ticket_param, "terminal_id": terminal_param},
                    )
                else:
                    raise RemoteCommandError("ExecutionOrchestrator missing manual_close_position method.")

            # -----------------------------------------------------------------
            # 3. Action: CLOSE_SYMBOL
            # -----------------------------------------------------------------
            elif action == "CLOSE_SYMBOL":
                symbol_param = command.symbol or command.params.get("symbol")
                terminal_param = command.terminal_id or command.params.get("terminal_id")
                if not symbol_param:
                    raise InvalidCommandPayloadError("Action 'CLOSE_SYMBOL' requires 'symbol' parameter.")

                if hasattr(self.orchestrator, "manual_close_symbol"):
                    results = await self.orchestrator.manual_close_symbol(
                        symbol=str(symbol_param),
                        terminal_id=terminal_param,
                    )
                    success_count = sum(1 for r in results if getattr(r, "success", False))
                    return CommandExecutionResult(
                        command_id=cmd_id,
                        action="CLOSE_SYMBOL",
                        success=True,
                        status="COMPLETED",
                        dispatched_trades_count=len(results),
                        details={"symbol": symbol_param, "closed_count": len(results), "successful_count": success_count},
                    )
                else:
                    raise RemoteCommandError("ExecutionOrchestrator missing manual_close_symbol method.")

            # -----------------------------------------------------------------
            # 4. Action: PAUSE_TRADING / HALT
            # -----------------------------------------------------------------
            elif action in ("PAUSE_TRADING", "HALT", "EMERGENCY_HALT"):
                if hasattr(self.orchestrator, "pause_trading"):
                    paused = self.orchestrator.pause_trading()
                elif hasattr(self.orchestrator, "set_engine_role"):
                    from engine.failover_manager import EngineRole
                    self.orchestrator.set_engine_role(EngineRole.ROLE_STANDBY)
                    paused = True
                else:
                    paused = True

                return CommandExecutionResult(
                    command_id=cmd_id,
                    action="PAUSE_TRADING",
                    success=bool(paused),
                    status="COMPLETED",
                    details={"paused": True},
                )

            # -----------------------------------------------------------------
            # 5. Action: RESUME_TRADING
            # -----------------------------------------------------------------
            elif action in ("RESUME_TRADING", "UNPAUSE_TRADING"):
                if hasattr(self.orchestrator, "resume_trading"):
                    resumed = self.orchestrator.resume_trading()
                elif hasattr(self.orchestrator, "set_engine_role"):
                    from engine.failover_manager import EngineRole
                    self.orchestrator.set_engine_role(EngineRole.ROLE_ACTIVE)
                    resumed = True
                else:
                    resumed = True

                return CommandExecutionResult(
                    command_id=cmd_id,
                    action="RESUME_TRADING",
                    success=bool(resumed),
                    status="COMPLETED",
                    details={"resumed": True},
                )

            # -----------------------------------------------------------------
            # 6. Action: MODIFY_POSITION
            # -----------------------------------------------------------------
            elif action == "MODIFY_POSITION":
                ticket_param = command.ticket or command.params.get("ticket")
                sl_param = command.params.get("stop_loss") or command.params.get("sl")
                tp_param = command.params.get("take_profit") or command.params.get("tp")
                terminal_param = command.terminal_id or command.params.get("terminal_id")

                if not ticket_param:
                    raise InvalidCommandPayloadError("Action 'MODIFY_POSITION' requires 'ticket' parameter.")

                if hasattr(self.orchestrator, "manual_modify_position"):
                    res = await self.orchestrator.manual_modify_position(
                        ticket=int(ticket_param),
                        stop_loss=float(sl_param) if sl_param is not None else None,
                        take_profit=float(tp_param) if tp_param is not None else None,
                        terminal_id=terminal_param,
                    )
                    success = getattr(res, "success", True) if res is not None else False
                    return CommandExecutionResult(
                        command_id=cmd_id,
                        action="MODIFY_POSITION",
                        success=success,
                        status="COMPLETED" if success else "FAILED",
                        details={"ticket": ticket_param, "sl": sl_param, "tp": tp_param},
                    )
                else:
                    raise RemoteCommandError("ExecutionOrchestrator missing manual_modify_position method.")

            # Unknown Action
            else:
                raise InvalidCommandPayloadError(f"Unsupported remote command action: '{action}'")

        except Exception as e:
            logger.error("Error executing remote command %s (%s): %s", cmd_id, action, e, exc_info=True)
            return CommandExecutionResult(
                command_id=cmd_id,
                action=action,
                success=False,
                status="FAILED",
                error=str(e),
            )

    # -------------------------------------------------------------------------
    # Cloud Relay Transport Interaction (HTTP Polling & ACK)
    # -------------------------------------------------------------------------

    async def _fetch_outbox_envelopes(self) -> List[RemoteCommandEnvelope]:
        """Queries the Cloud Relay outbox for pending command envelopes."""
        if self.http_transport is not None:
            # Custom / Injected mock transport
            try:
                res = self.http_transport("GET", None, f"/relay/commands/outbox?license_id={self.license_id}")
                if asyncio.iscoroutine(res):
                    res = await res
                return self._parse_relay_response(res)
            except Exception as e:
                raise RelayPollingError(f"Custom transport error polling outbox: {e}") from e

        # Standard HTTP Relay Client
        query_url = f"{self.relay_url}/relay/commands/outbox?license_id={urllib.parse.quote(self.license_id)}"
        req = urllib.request.Request(
            query_url,
            headers={
                "User-Agent": "Pipvane-Engine/3.0",
                "Accept": "application/json",
                "X-License-ID": self.license_id,
            },
            method="GET",
        )

        def _do_http_get():
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status in (200, 201):
                        raw = resp.read().decode("utf-8")
                        return json.loads(raw)
                    return None
            except urllib.error.HTTPError as he:
                if he.code == 404:
                    return None  # No commands available in outbox
                raw_body = he.read().decode("utf-8", errors="replace")
                raise RelayPollingError(f"Cloud Relay HTTP {he.code}: {raw_body}") from he
            except Exception as e:
                raise RelayPollingError(f"Network error querying Cloud Relay: {e}") from e

        loop = asyncio.get_running_loop()
        try:
            resp_data = await loop.run_in_executor(None, _do_http_get)
            return self._parse_relay_response(resp_data)
        except RelayPollingError:
            raise
        except Exception as e:
            raise RelayPollingError(f"Failed to fetch outbox commands: {e}") from e

    def _parse_relay_response(self, data: Any) -> List[RemoteCommandEnvelope]:
        """Parses Cloud Relay JSON response into a list of RemoteCommandEnvelope."""
        if not data:
            return []

        envelopes: List[RemoteCommandEnvelope] = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    envelopes.append(RemoteCommandEnvelope.from_dict(item))
        elif isinstance(data, dict):
            # Check for commands list or single envelope
            if "commands" in data and isinstance(data["commands"], list):
                for item in data["commands"]:
                    envelopes.append(RemoteCommandEnvelope.from_dict(item))
            elif "messages" in data and isinstance(data["messages"], list):
                for item in data["messages"]:
                    envelopes.append(RemoteCommandEnvelope.from_dict(item))
            elif "envelope" in data and isinstance(data["envelope"], dict):
                envelopes.append(RemoteCommandEnvelope.from_dict(data["envelope"]))
            elif "message" in data and isinstance(data["message"], dict):
                envelopes.append(RemoteCommandEnvelope.from_dict(data["message"]))
            elif "ciphertext" in data:
                envelopes.append(RemoteCommandEnvelope.from_dict(data))

        return envelopes

    async def _ack_remote_command(self, message_id: str) -> None:
        """Sends an acknowledgment to Cloud Relay to remove the processed command."""
        ack_payload = {
            "license_id": self.license_id,
            "message_id": message_id,
            "queue": "outbox",
        }

        if self.http_transport is not None:
            try:
                res = self.http_transport("POST", ack_payload, "/relay/commands/ack")
                if asyncio.iscoroutine(res):
                    await res
                return
            except Exception as e:
                logger.debug("Custom transport error during ACK: %s", e)
                return

        ack_url = f"{self.relay_url}/relay/commands/ack"
        data_bytes = json.dumps(ack_payload).encode("utf-8")
        req = urllib.request.Request(
            ack_url,
            data=data_bytes,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Pipvane-Engine/3.0",
                "X-License-ID": self.license_id,
            },
            method="POST",
        )

        def _do_ack():
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    return resp.status in (200, 201, 204)
            except Exception as e:
                logger.debug("ACK request warning: %s", e)
                return False

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _do_ack)

    # -------------------------------------------------------------------------
    # Telemetry & Status
    # -------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Returns runtime telemetry for the RemoteCommandProcessor."""
        return {
            "is_running": self._is_running,
            "license_id": self.license_id,
            "polling_interval_s": self.polling_interval_s,
            "total_commands_executed": len(self._execution_history),
            "seen_replay_ids_count": self.replay_guard.seen_count,
            "consecutive_poll_errors": self._consecutive_poll_errors,
        }

    def get_history(self, limit: int = 50) -> List[CommandExecutionResult]:
        """Returns recent execution history."""
        return self._execution_history[-limit:]
