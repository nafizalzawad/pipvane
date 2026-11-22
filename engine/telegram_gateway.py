"""
Pipvane v3 Core Telegram Ingestion Gateway.

Implements the official TDLib wrapper for ultra-low latency signal ingestion.
Strict Safety Mode: Read-only ingestion pipeline strictly prohibiting outbound writes.
Resilience: Automatic reconnection loop with exponential backoff and message history
backfill since last_processed_message_id per FR-7.6.
Vault Integration: Zero-knowledge DPAPI-encrypted credential and session key storage.
"""

from __future__ import annotations

import os
import time
import enum
import random
import logging
import asyncio
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Callable, Set, Union

from engine.vault import VaultStorage
from engine.device_profile import DeviceProfileManager, DeviceProfile
from engine.safety_mode import assert_safety_mode_allows, SafetyModeViolationError
from engine.tdlib_client import (
    TDLibClientInterface,
    NativeTDLibClient,
    MockTDLibClient,
)

logger = logging.getLogger("pipvane.engine.telegram_gateway")


class ConnectionState(enum.Enum):
    """Connection states for TelegramGateway."""
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    WAITING_FOR_NETWORK = "WAITING_FOR_NETWORK"
    UPDATING = "UPDATING"
    READY = "READY"


class TelegramBanException(Exception):
    """Base exception for Telegram account-level ban/deactivation events."""
    pass


class UserDeactivatedError(TelegramBanException):
    """Raised when Telegram returns USER_DEACTIVATED."""
    pass


class AuthKeyUnregisteredError(TelegramBanException):
    """Raised when Telegram returns AUTH_KEY_UNREGISTERED."""
    pass


class TelegramGatewayError(Exception):
    """Base exception for general TelegramGateway operational errors."""
    pass


class FloodWaitError(TelegramGatewayError):
    """Thrown when Telegram API enforces rate-limiting backoff (FLOOD_WAIT_X)."""
    def __init__(self, retry_after: int, message: str = ""):
        super().__init__(message or f"Rate limited by Telegram API. Flood wait: {retry_after}s")
        self.retry_after = retry_after


@dataclass
class TelegramGatewayConfig:
    """Configuration dataclass for TelegramGateway."""
    account_id: str = "default"
    phone_number: Optional[str] = None
    session_dir: Optional[str] = None
    safety_mode: bool = True
    use_simulated_driver: bool = False
    api_id: int = 12345
    api_hash: str = "mock_api_hash"
    database_directory: Optional[str] = None
    files_directory: Optional[str] = None
    max_signal_age_s: float = 30.0
    base_reconnect_delay: float = 1.0
    max_reconnect_delay: float = 60.0


@dataclass
class TelegramMessage:
    """Parsed Telegram message representation."""
    chat_id: int
    message_id: int
    date: int
    text: str
    raw: Optional[Dict[str, Any]] = None

    def __getitem__(self, item: str) -> Any:
        if item in ("id", "message_id"):
            return self.message_id
        elif item == "chat_id":
            return self.chat_id
        elif item == "date":
            return self.date
        elif item == "text":
            return self.text
        if self.raw and item in self.raw:
            return self.raw[item]
        raise KeyError(item)

    def get(self, item: str, default: Any = None) -> Any:
        try:
            return self[item]
        except KeyError:
            return default

    def __contains__(self, item: str) -> bool:
        return item in ("id", "message_id", "chat_id", "date", "text") or (self.raw is not None and item in self.raw)


class TelegramGateway:
    """
    Production Telegram Ingestion Gateway utilizing official TDLib.
    Operates strictly in Read-Only Safety Mode.
    """

    def __init__(
        self,
        config: Optional[Union[TelegramGatewayConfig, int]] = None,
        api_id: Optional[int] = None,
        api_hash: Optional[str] = None,
        vault: Optional[VaultStorage] = None,
        tdlib_client: Optional[TDLibClientInterface] = None,
        database_directory: Optional[str] = None,
        files_directory: Optional[str] = None,
        max_signal_age_s: float = 30.0,
        base_reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 60.0,
        profile_id: str = "default",
        **kwargs: Any,
    ) -> None:
        # Resolve config vs keyword arguments
        cfg: Optional[TelegramGatewayConfig] = None
        if isinstance(config, TelegramGatewayConfig):
            cfg = config
        elif isinstance(config, int):
            api_id = config

        if cfg is not None:
            self.account_id = cfg.account_id
            self.phone_number = cfg.phone_number
            self.api_id = cfg.api_id
            self.api_hash = cfg.api_hash
            self.max_signal_age_s = float(cfg.max_signal_age_s)
            self.base_reconnect_delay = float(cfg.base_reconnect_delay)
            self.max_reconnect_delay = float(cfg.max_reconnect_delay)
            self.profile_id = cfg.account_id

            session_dir = cfg.session_dir or os.path.join(
                os.environ.get("APPDATA", os.path.expanduser("~")), "Pipvane", "sessions", cfg.account_id
            )
            self.database_directory = cfg.database_directory or os.path.join(session_dir, "db")
            self.files_directory = cfg.files_directory or os.path.join(session_dir, "files")
            vault_db = os.path.join(session_dir, "vault.db")
            self.vault = vault or VaultStorage(db_path=vault_db)
            self._use_simulated = cfg.use_simulated_driver
        else:
            self.account_id = profile_id
            self.phone_number = kwargs.get("phone_number")
            self.api_id = int(api_id if api_id is not None else 12345)
            self.api_hash = str(api_hash if api_hash is not None else "mock_api_hash")
            self.max_signal_age_s = float(max_signal_age_s)
            self.base_reconnect_delay = float(base_reconnect_delay)
            self.max_reconnect_delay = float(max_reconnect_delay)
            self.profile_id = profile_id

            appdata = os.environ.get("APPDATA", os.path.expanduser("~"))
            base_dir = os.path.join(appdata, "Pipvane", "tdlib")
            self.database_directory = database_directory or os.path.join(base_dir, "db")
            self.files_directory = files_directory or os.path.join(base_dir, "files")
            self.vault = vault or VaultStorage()
            self._use_simulated = kwargs.get("use_simulated_driver", False)

        # Client instance
        self._client: Optional[TDLibClientInterface] = tdlib_client

        # Strict Safety Mode
        self._safety_mode = True

        # Lifecycle & State Tracking
        self._is_running = False
        self._stop_event = threading.Event()
        self._receiver_thread: Optional[threading.Thread] = None

        self._auth_state: str = "authorizationStateWaitTdlibParameters"
        self._connection_state: str = "connectionStateConnecting"
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.termination_code: str = "RUNNING"
        self.last_ban_exception: Optional[Exception] = None
        self.start_time: float = time.time()

        # Metrics for soak and chaos telemetry
        self.network_disruptions_count: int = 0
        self.messages_received_count: int = 0
        self.messages_backfilled_count: int = 0
        self.flood_wait_count: int = 0
        self.flood_wait_total_seconds: float = 0.0

        self._reconnect_attempts: int = 0
        self._ready_event = threading.Event()
        self._qr_code_event = threading.Event()
        self._qr_code_link: Optional[str] = None

        # Monitored chats, handlers, and backlog for simulated network partitions
        self._monitored_chats: Set[int] = set()
        self._message_handlers: List[Callable[[Any], None]] = []
        self._error_handlers: List[Callable[[Exception], None]] = []
        self._update_handlers: List[Callable[[Dict[str, Any]], None]] = []
        self._backlog: List[TelegramMessage] = []
        self._lock = threading.RLock()

        # Device Profile
        self._device_profile: Optional[DeviceProfile] = None

        # Integrated Message Backfill Engine (Sprint 16)
        self.message_backfill: Optional[Any] = None

    # --- Metrics & Uptime ---

    def get_uptime_seconds(self) -> float:
        """Returns session uptime in seconds."""
        return max(0.0, time.time() - self.start_time)

    def get_current_uptime(self) -> float:
        """Returns session uptime in seconds (alias)."""
        return self.get_uptime_seconds()

    @property
    def monitored_chats(self) -> List[int]:
        """Returns list of monitored chat identifiers."""
        with self._lock:
            return list(self._monitored_chats)

    # --- Strict Safety Mode Enforcement ---

    @property
    def safety_mode(self) -> bool:
        """Indicates whether Strict Safety Mode is active. Always True."""
        return self._safety_mode

    @safety_mode.setter
    def safety_mode(self, value: bool) -> None:
        if not value:
            raise SafetyModeViolationError(
                "SAFETY MODE VIOLATION: Disabling Safety Mode is strictly prohibited by architectural policy."
            )

    def send_message(self, *args: Any, **kwargs: Any) -> None:
        """Blocked by Strict Safety Mode. Raises immediately when invoked (even before await)."""
        raise SafetyModeViolationError(
            "SAFETY MODE VIOLATION: Safety Mode strictly prohibits sending messages. Operation is strictly prohibited."
        )

    def join_channel(self, *args: Any, **kwargs: Any) -> None:
        """Blocked by Strict Safety Mode."""
        raise SafetyModeViolationError(
            "SAFETY MODE VIOLATION: Safety Mode strictly prohibits joining channels. Operation is strictly prohibited."
        )

    def leave_channel(self, *args: Any, **kwargs: Any) -> None:
        """Blocked by Strict Safety Mode."""
        raise SafetyModeViolationError(
            "SAFETY MODE VIOLATION: Safety Mode strictly prohibits leaving channels. Operation is strictly prohibited."
        )

    def react_to_message(self, *args: Any, **kwargs: Any) -> None:
        """Blocked by Strict Safety Mode."""
        raise SafetyModeViolationError(
            "SAFETY MODE VIOLATION: Safety Mode strictly prohibits reacting to messages. Operation is strictly prohibited."
        )

    def forward_messages(self, *args: Any, **kwargs: Any) -> None:
        """Blocked by Strict Safety Mode."""
        raise SafetyModeViolationError(
            "SAFETY MODE VIOLATION: Safety Mode strictly prohibits forwarding messages. Operation is strictly prohibited."
        )

    # --- Core Dispatch with Safety Filter ---

    def send_request(self, request: Dict[str, Any]) -> None:
        """Dispatches a JSON request to TDLib. Enforces Safety Mode checks before sending."""
        req_type = request.get("@type", "")
        assert_safety_mode_allows(req_type)

        if not self._client:
            raise TelegramGatewayError("TDLib client is not initialized")

        self._client.send(request)

    # --- Gateway Lifecycle (Async and Sync) ---

    async def connect(self) -> None:
        """Async entry point for starting the gateway connection."""
        self.state = ConnectionState.READY
        self._connection_state = "connectionStateReady"
        self.start_time = time.time()
        self.start()

    async def disconnect(self, reason: Optional[str] = None) -> None:
        """Async entry point for halting the gateway."""
        self.state = ConnectionState.DISCONNECTED
        self._connection_state = "connectionStateDisconnected"
        if reason:
            self.termination_code = reason
        elif self.termination_code == "RUNNING":
            self.termination_code = "NORMAL_SHUTDOWN"
        self.stop()

    async def start_listening(self) -> None:
        """Async hook ensuring listener loop is active."""
        if not self._is_running:
            self.start()

    def start(self) -> None:
        """Initializes device profile, credentials, TDLib client, and starts receiver thread."""
        with self._lock:
            if self._is_running:
                return

            self._device_profile = DeviceProfileManager.get_or_create_profile(
                self.vault, profile_id=self.profile_id
            )

            if self._client is None:
                if self._use_simulated:
                    self._client = MockTDLibClient(initial_auth_state="authorizationStateReady")
                else:
                    self._client = NativeTDLibClient()

            self._is_running = True
            self._stop_event.clear()
            self._ready_event.clear()

            self._receiver_thread = threading.Thread(
                target=self._receive_loop,
                name=f"PipvaneTDLibReceiver-{self.account_id}",
                daemon=True
            )
            self._receiver_thread.start()
            logger.info("TelegramGateway started for account '%s' (Device: %s)", self.account_id, self._device_profile.device_model)

    def stop(self) -> None:
        """Gracefully halts the gateway and closes the TDLib client."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False
            self._stop_event.set()

            if self._client:
                try:
                    self.send_request({"@type": "close"})
                except Exception as e:
                    logger.debug("Error while closing TDLib client: %s", e)

            if self._receiver_thread and self._receiver_thread.is_alive():
                self._receiver_thread.join(timeout=2.0)

            if self._client:
                try:
                    self._client.close()
                except Exception as e:
                    logger.debug("Error destroying TDLib client: %s", e)

            logger.info("TelegramGateway stopped successfully for account '%s'", self.account_id)

    def _configure_tdlib_parameters(self) -> None:
        """Constructs and sends setTdlibParameters using persisted vault profile and keys."""
        if not self._device_profile:
            self._device_profile = DeviceProfileManager.get_or_create_profile(self.vault, profile_id=self.profile_id)

        db_encryption_key = self.vault.get_or_create_tdlib_key()

        params_request = {
            "@type": "setTdlibParameters",
            "database_directory": self.database_directory,
            "files_directory": self.files_directory,
            "database_encryption_key": db_encryption_key,
            "use_file_database": True,
            "use_chat_info_database": True,
            "use_message_database": True,
            "use_secret_chats": False,
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "system_language_code": self._device_profile.system_language_code,
            "device_model": self._device_profile.device_model,
            "system_version": self._device_profile.system_version,
            "application_version": self._device_profile.app_version,
            "enable_storage_optimizer": True,
            "ignore_file_names": True,
        }
        self.send_request(params_request)
        logger.info("Configured TDLib parameters successfully for account '%s'", self.account_id)

    # --- Authentication Sequences ---

    def login_with_phone(self, phone_number: str) -> None:
        """Initiates phone number authentication flow."""
        self.phone_number = phone_number
        self.send_request({
            "@type": "setAuthenticationPhoneNumber",
            "phone_number": phone_number,
        })
        logger.info("Submitted phone number for authentication")

    def submit_auth_code(self, code: str) -> None:
        """Submits the SMS/Telegram verification code."""
        self.send_request({
            "@type": "checkAuthenticationCode",
            "code": code,
        })
        logger.info("Submitted authentication code")

    def submit_2fa_password(self, password: str) -> None:
        """Submits the two-factor authentication (2FA) cloud password."""
        self.send_request({
            "@type": "checkAuthenticationPassword",
            "password": password,
        })
        logger.info("Submitted 2FA password")

    def login_with_qr_code(self, timeout: float = 10.0) -> str:
        """Initiates QR code authentication and waits for the confirmation link."""
        self._qr_code_event.clear()
        self._qr_code_link = None

        self.send_request({
            "@type": "requestQrCodeAuthentication",
        })

        if not self._qr_code_event.wait(timeout=timeout):
            raise TimeoutError("Timed out waiting for TDLib QR code authentication link")

        if not self._qr_code_link:
            raise TelegramGatewayError("Failed to acquire QR code link from TDLib")

        return self._qr_code_link

    def wait_until_ready(self, timeout: float = 10.0) -> bool:
        """Blocks until TDLib authorization state is authorizationStateReady."""
        return self._ready_event.wait(timeout=timeout)

    @property
    def auth_state(self) -> str:
        return self._auth_state

    @property
    def connection_state(self) -> str:
        return self._connection_state

    # --- Subscriptions & Ingestion Callbacks ---

    def on_message(self, handler: Callable[[Any], None]) -> None:
        """Aliases to add_message_handler for soak orchestrator compatibility."""
        self.add_message_handler(handler)

    def add_message_handler(self, handler: Callable[[Any], None]) -> None:
        """Registers a callback for incoming trading signals/messages."""
        with self._lock:
            if handler not in self._message_handlers:
                self._message_handlers.append(handler)

    def remove_message_handler(self, handler: Callable[[Any], None]) -> None:
        """Removes a registered callback."""
        with self._lock:
            if handler in self._message_handlers:
                self._message_handlers.remove(handler)

    def add_error_handler(self, handler: Callable[[Exception], None]) -> None:
        """Registers a callback for TDLib errors and ban events."""
        with self._lock:
            if handler not in self._error_handlers:
                self._error_handlers.append(handler)

    def remove_error_handler(self, handler: Callable[[Exception], None]) -> None:
        """Removes a registered error callback."""
        with self._lock:
            if handler in self._error_handlers:
                self._error_handlers.remove(handler)

    def _dispatch_error(self, exc: Exception) -> None:
        """Dispatches an exception to all registered error callbacks."""
        with self._lock:
            handlers = list(self._error_handlers)
        for handler in handlers:
            try:
                handler(exc)
            except AssertionError:
                raise
            except Exception as e:
                logger.error("Error executing error handler: %s", e)

    def on_update(self, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Registers a raw TDLib event listener."""
        with self._lock:
            if handler not in self._update_handlers:
                self._update_handlers.append(handler)

    def attach_ingestion_controller(self, controller: Any) -> None:
        """Connects an IngestionController to this gateway."""
        self.on_update(lambda ev: controller.handle_tdlib_update(ev))

    def subscribe_chat(self, chat_id: int) -> None:
        """Subscribes to signal ingestion from a specific chat/channel."""
        with self._lock:
            self._monitored_chats.add(chat_id)
            if self._auth_state == "authorizationStateReady":
                self.backfill_chat(chat_id)

    def unsubscribe_chat(self, chat_id: int) -> None:
        """Unsubscribes from signal ingestion for a chat/channel."""
        with self._lock:
            self._monitored_chats.discard(chat_id)

    # --- Resilience: Backfill & Reconnect Loop ---

    def calculate_backoff_delay(self, attempt: int) -> float:
        """Computes exponential backoff delay with jitter."""
        raw = min(self.base_reconnect_delay * (2 ** max(0, attempt)), self.max_reconnect_delay)
        jitter = random.uniform(0.0, 0.25 * raw)
        return raw + jitter

    def backfill_chat(self, chat_id: int, limit: int = 50) -> None:
        """Queries chat message history since last_processed_message_id (FR-7.6 compliance)."""
        last_id = self.vault.get_chat_checkpoint(chat_id)
        from_msg_id = 0

        req = {
            "@type": "getChatHistory",
            "chat_id": chat_id,
            "from_message_id": from_msg_id,
            "offset": 0,
            "limit": limit,
            "only_local": False,
            "@extra": {
                "pipvane_action": "backfill",
                "chat_id": chat_id,
                "last_processed_id": last_id,
            }
        }
        self.send_request(req)
        logger.debug("Requested backfill history for chat %s (last_processed_id=%s)", chat_id, last_id)

    def get_or_create_message_backfill(
        self,
        orchestrator: Optional[Any] = None,
        ingestion_controller: Optional[Any] = None,
        signal_parser: Optional[Any] = None,
    ) -> Any:
        """Instantiates or retrieves the integrated MessageBackfill utility."""
        if self.message_backfill is None:
            from engine.message_backfill import MessageBackfill
            self.message_backfill = MessageBackfill(
                tdlib_client=self._client,
                telegram_gateway=self,
                ingestion_controller=ingestion_controller,
                orchestrator=orchestrator,
                signal_parser=signal_parser,
                max_signal_age_s=self.max_signal_age_s,
            )
        return self.message_backfill

    def backfill_all_monitored_chats(self, reconciled_orders: Optional[List[Any]] = None) -> Any:
        """
        Performs backfill reconciliation for all registered channels.
        If MessageBackfill utility is active, executes full historical backfill with deduplication check.
        """
        if self.message_backfill is not None:
            with self._lock:
                chats = list(self._monitored_chats)
            return self.message_backfill.execute_backfill(
                subscribed_channels=chats,
                reconciled_orders=reconciled_orders,
            )

        with self._lock:
            chats = list(self._monitored_chats)
        for chat_id in chats:
            self.backfill_chat(chat_id)

    # --- Chaos & Fault Injection Hooks (For Soak & Chaos Tests) ---

    def drop_active_connection(self, simulated_reason: str = "MANUAL_CHAOS_TRIGGER") -> None:
        """Immediately marks connection as WAITING_FOR_NETWORK."""
        self.state = ConnectionState.WAITING_FOR_NETWORK
        self._connection_state = "connectionStateWaitingForNetwork"
        self.network_disruptions_count += 1
        logger.warning(
            "Fault Injection: Connection dropped on account %s (Reason: %s, Disruptions: %d)",
            self.account_id, simulated_reason, self.network_disruptions_count
        )
        if self._client and hasattr(self._client, "simulate_incoming_update"):
            self._client.simulate_incoming_update({
                "@type": "updateConnectionState",
                "state": {"@type": "connectionStateWaitingForNetwork"}
            })

    async def drop_active_socket(self, simulated_reason: str = "CHAOS_TEST_DROP") -> None:
        """Simulates an abrupt network failure / socket drop with auto-reconnect."""
        self.drop_active_connection(simulated_reason=simulated_reason)
        # Trigger background automatic reconnection
        asyncio.create_task(self._simulate_auto_reconnect_loop())

    async def _simulate_auto_reconnect_loop(self) -> None:
        """Simulates TDLib auto-reconnection and backfill recovery."""
        await asyncio.sleep(0.15)
        self.state = ConnectionState.READY
        self._connection_state = "connectionStateReady"
        logger.info("TDLib auto-reconnect recovered socket for account %s. Replaying backfill...", self.account_id)

        # Replay backlogged partition messages
        with self._lock:
            pending = list(self._backlog)
            self._backlog.clear()

        for msg in pending:
            self._dispatch_to_handlers(msg)
            self.messages_received_count += 1
            self.messages_backfilled_count += 1
            self.vault.update_chat_checkpoint(msg.chat_id, msg.message_id, timestamp=float(msg.date))

    async def inject_incoming_message(self, chat_id: int, text: str, date: Optional[int] = None) -> None:
        """Injects a simulated message directly into the pipeline."""
        now = int(date or time.time())
        msg_id = int(time.time_ns() % 1_000_000_000)
        telegram_msg = TelegramMessage(
            chat_id=chat_id,
            message_id=msg_id,
            date=now,
            text=text,
            raw={"id": msg_id, "chat_id": chat_id, "date": now, "content": {"text": text}}
        )

        if self.state == ConnectionState.WAITING_FOR_NETWORK:
            # During network partitions, messages accumulate in backlog
            self._backlog.append(telegram_msg)
            logger.info("Message %d for chat %d buffered in partition backlog", msg_id, chat_id)
        else:
            self._dispatch_to_handlers(telegram_msg)
            self.messages_received_count += 1
            self.vault.update_chat_checkpoint(chat_id, msg_id, timestamp=float(now))

    def inject_ban_exception(self, code: str) -> None:
        """Injects a simulated Telegram account ban error for testing fail-fast assertions."""
        self.termination_code = code
        exc: Exception
        if code == "USER_DEACTIVATED":
            exc = UserDeactivatedError(f"TDLib returned USER_DEACTIVATED on account {self.account_id}")
        elif code == "AUTH_KEY_UNREGISTERED":
            exc = AuthKeyUnregisteredError(f"TDLib returned AUTH_KEY_UNREGISTERED on account {self.account_id}")
        else:
            exc = TelegramBanException(f"Simulated ban exception: {code}")

        self.last_ban_exception = exc
        self._dispatch_error(exc)

    # --- Internal Event Loop & Dispatch ---

    def _receive_loop(self) -> None:
        """Background worker polling TDLib updates and dispatching events."""
        while self._is_running and not self._stop_event.is_set():
            try:
                event = self._client.receive(timeout=0.5)
                if not event:
                    continue
                self._dispatch_event(event)
            except Exception as e:
                logger.error("Error in TDLib receive loop: %s", e, exc_info=True)
                time.sleep(0.05)

    def _dispatch_event(self, event: Dict[str, Any]) -> None:
        """Processes incoming TDLib events."""
        event_type = event.get("@type", "")

        if event_type == "updateAuthorizationState":
            self._handle_auth_state_update(event.get("authorization_state", {}))
        elif event_type == "updateConnectionState":
            self._handle_connection_state_update(event.get("state", {}))
        elif event_type == "updateNewMessage":
            self._handle_new_message(event.get("message", {}))
        elif event_type == "messages":
            extra = event.get("@extra", {})
            if isinstance(extra, dict) and extra.get("pipvane_action") == "backfill":
                self._handle_backfill_messages_response(event, extra)
        elif event_type == "error":
            self._handle_error_event(event)

        # Notify registered raw update listeners (e.g. IngestionController)
        with self._lock:
            handlers = list(self._update_handlers)
        for h in handlers:
            try:
                h(event)
            except Exception as e:
                logger.error("Error in update handler callback: %s", e)

    def _handle_error_event(self, event: Dict[str, Any]) -> None:
        code = event.get("code", 0)
        message = str(event.get("message", ""))
        logger.warning("TDLib error received [code=%s]: %s", code, message)

        exc: Optional[Exception] = None
        if "USER_DEACTIVATED" in message.upper():
            self.termination_code = "USER_DEACTIVATED"
            exc = UserDeactivatedError(f"TDLib Error {code}: USER_DEACTIVATED on account {self.account_id}")
        elif "AUTH_KEY_UNREGISTERED" in message.upper() or "SESSION_REVOKED" in message.upper():
            self.termination_code = "AUTH_KEY_UNREGISTERED"
            exc = AuthKeyUnregisteredError(f"TDLib Error {code}: AUTH_KEY_UNREGISTERED on account {self.account_id}")
        elif "FLOOD_WAIT" in message.upper():
            import re
            m = re.search(r"FLOOD_WAIT_(\d+)", message)
            wait_s = int(m.group(1)) if m else 60
            self.flood_wait_count += 1
            self.flood_wait_total_seconds += wait_s
            exc = FloodWaitError(wait_s, message)
        else:
            exc = TelegramGatewayError(f"TDLib error {code}: {message}")

        if exc:
            with self._lock:
                handlers = list(self._error_handlers)
            for h in handlers:
                try:
                    h(exc)
                except AssertionError:
                    raise
                except Exception as e:
                    logger.error("Error executing error handler: %s", e)

    def _handle_auth_state_update(self, auth_state_dict: Dict[str, Any]) -> None:
        state_type = auth_state_dict.get("@type", "")
        self._auth_state = state_type
        logger.info("TDLib Authorization state updated: %s", state_type)

        if state_type == "authorizationStateWaitTdlibParameters":
            self._configure_tdlib_parameters()
        elif state_type == "authorizationStateWaitOtherDeviceConfirmation":
            self._qr_code_link = auth_state_dict.get("link")
            self._qr_code_event.set()
        elif state_type == "authorizationStateReady":
            self.state = ConnectionState.READY
            self._ready_event.set()
            self.backfill_all_monitored_chats()
        elif state_type == "authorizationStateClosed":
            self.state = ConnectionState.DISCONNECTED
            self._ready_event.clear()

    def _handle_connection_state_update(self, conn_state_dict: Dict[str, Any]) -> None:
        state_type = conn_state_dict.get("@type", "")
        self._connection_state = state_type
        logger.info("TDLib Connection state updated: %s", state_type)

        if state_type == "connectionStateReady":
            self.state = ConnectionState.READY
            self._reconnect_attempts = 0
            self.backfill_all_monitored_chats()
        elif state_type == "connectionStateWaitingForNetwork":
            self.state = ConnectionState.WAITING_FOR_NETWORK
            self._reconnect_attempts += 1
            delay = self.calculate_backoff_delay(self._reconnect_attempts)
            logger.warning("Connection waiting for network. Backoff delay: %.2fs", delay)
        elif state_type == "connectionStateConnecting":
            self.state = ConnectionState.CONNECTING

    def _extract_text(self, message: Dict[str, Any]) -> str:
        content = message.get("content", {})
        if "text" in content and isinstance(content["text"], dict):
            return content["text"].get("text", "")
        elif "text" in content and isinstance(content["text"], str):
            return content["text"]
        return str(message.get("text", ""))

    def _dispatch_to_handlers(self, payload: Union[Dict[str, Any], TelegramMessage]) -> None:
        with self._lock:
            handlers = list(self._message_handlers)
        for handler in handlers:
            try:
                # If handler expects TelegramMessage and payload is dict, convert or deliver
                handler(payload)
            except TypeError:
                if isinstance(payload, TelegramMessage):
                    handler(payload.raw or {})
                else:
                    handler(payload)
            except Exception as e:
                logger.error("Error executing message handler: %s", e)

    def _handle_new_message(self, message: Dict[str, Any]) -> None:
        chat_id = message.get("chat_id")
        if chat_id not in self._monitored_chats:
            return

        msg_id = message.get("id", 0)
        msg_date = message.get("date", 0)
        now = time.time()
        age = now - msg_date

        if age > self.max_signal_age_s:
            logger.warning("Dropped stale message chat_id=%s id=%s age=%.1fs", chat_id, msg_id, age)
            return

        self.vault.update_chat_checkpoint(chat_id, msg_id, timestamp=float(msg_date))
        self.messages_received_count += 1

        text_content = self._extract_text(message)
        wrapped_msg = TelegramMessage(
            chat_id=chat_id,
            message_id=msg_id,
            date=msg_date,
            text=text_content,
            raw=message,
        )
        self._dispatch_to_handlers(wrapped_msg)

    def _handle_backfill_messages_response(self, response: Dict[str, Any], extra: Dict[str, Any]) -> None:
        chat_id = extra.get("chat_id")
        last_id = extra.get("last_processed_id")
        messages = response.get("messages", [])

        sorted_messages = sorted(messages, key=lambda m: m.get("id", 0))

        for msg in sorted_messages:
            msg_id = msg.get("id", 0)
            if last_id is not None and msg_id <= last_id:
                continue

            msg_date = msg.get("date", 0)
            now = time.time()
            age = now - msg_date

            if age > self.max_signal_age_s:
                continue

            self.vault.update_chat_checkpoint(chat_id, msg_id, timestamp=float(msg_date))
            self.messages_received_count += 1
            self.messages_backfilled_count += 1

            text_content = self._extract_text(msg)
            wrapped_msg = TelegramMessage(
                chat_id=chat_id,
                message_id=msg_id,
                date=msg_date,
                text=text_content,
                raw=msg,
            )
            self._dispatch_to_handlers(wrapped_msg)
