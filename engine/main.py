"""
Pipvane v3 Master Engine Daemon & Production Orchestrator CLI.
==============================================================
Central orchestration daemon that ties together all decoupled Pipvane v3 subsystems:
1. Ingestion & Normalization:
   - TelegramGateway (TDLib in strict read-only Safety Mode)
   - IngestionController (SQLite WAL append-only ledger with SHA-256 hash chaining)
   - TextNormalizer (<50us functional ADR-002 normalizer)
   - SignalParser (Format packs, intent classification, geometry validation)
2. Risk & Execution:
   - RiskEngine (Sizing modes, entry guards, circuit breaker, economic calendar filter)
   - LicenseManager (Cryptographic offline RS256 JWT, 30-day grace, 7-day silent refresh)
   - ExecutionOrchestrator (Hedging TP ladders, MT5 Netting Virtual TPs, Break-Even, 30s reconciliation)
   - OrphanManager (Auto-adopt or close untracked positions with magic number 123456)
3. IPC & MetaTrader Bridge:
   - TerminalDiscovery (Auto-detects MT4/MT5, verifies EX4 integrity, dynamic IPC mapping)
   - IPCDispatcher (SPSC Shared Memory Ring Buffer Writer <1us + Async Named Pipe Server)
4. High-Availability & Failover:
   - FailoverManager (mTLS WebSocket lease coordinator client, 10s heartbeat, 30s TTL,
     clock-based self-fencing into ROLE_LOCKED, anti-split-brain invariant |A(t)| <= 1)
5. Telemetry & User Interface:
   - LocalAPIServer (Strict loopback 127.0.0.1 REST & WebSocket server, session token auth)
   - NotificationEngine (Intelligent 2.0s burst digest batcher & Telegram rate limiter)
   - EventNotificationEngine (Windows Desktop Toasts, Discord/Slack webhooks, SMTP alerts)
   - TelegramControlBot (Long-polling personal slash command bot: /status, /pause, /resume)
6. System Constraints:
   - Interactive desktop GUI constraint check (warns on Session 0 headless service)
   - Windows DPAPI Zero-Knowledge credential storage in %APPDATA%\\Pipvane\\vault.db
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import json
import logging
import os
import signal
import struct
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# Ensure root directory is on sys.path when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# -----------------------------------------------------------------------------
# Internal Engine Imports
# -----------------------------------------------------------------------------
from engine.vault import (
    VaultStorage,
    VaultError,
    dpapi_protect,
    dpapi_unprotect,
    secure_zero,
)
from engine.device_profile import DeviceProfileManager, DeviceProfile
from engine.license_manager import (
    LicenseManager,
    LicenseEntitlements,
    LicenseValidationResult,
    LicenseValidationStatus,
    LicenseExpiredError,
    VAULT_LICENSE_KEY,
    create_signed_jwt,
    DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX,
)
from engine.ingestion_controller import (
    IngestionController,
    RawMessageRecord,
    GENESIS_HASH,
)
from engine.normalizer import (
    TextNormalizer,
    clean_text,
    normalize_text,
)
from engine.signal_parser import (
    SignalParser,
    ParsedSignal,
    MessageType,
    SKIP_GEOMETRY,
    GeometryValidationError,
)
from engine.risk_engine import (
    RiskEngine,
    RiskModel,
    AccountProfile,
    MarketQuote,
    SkipReason,
    SKIP_LICENSE_EXPIRED,
    SKIP_DUP,
    quantize_lot,
    DEFAULT_SYMBOL_CATALOG,
)

SKIP_STANDBY_LOCK = "SKIP_STANDBY_LOCK"
from engine.trade_orchestrator import (
    TradeOrchestrator,
    AccountMode,
)
from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    TerminalStatus,
    AccountState,
    PositionItem,
    PositionsSyncPayload,
    InitHelloPayload,
    NamedPipeOpcode,
    pack_pipe_frame,
    deserialize_order_intent,
)
from engine.ipc_dispatcher import (
    IPCDispatcher,
    TerminalContext,
)
from engine.terminal_discovery import (
    TerminalDiscovery,
    DiscoveredTerminal,
)
from engine.ex4_integrity import (
    EX4IntegrityChecker,
    default_integrity_checker,
)
from engine.orphan_manager import (
    OrphanManager,
    OrphanResolutionPolicy,
    DEFAULT_PIPVANE_MAGIC,
)
from engine.failover_manager import (
    FailoverManager,
    FailoverManagerConfig,
    EngineRole,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    ROLE_INITIALIZING,
    ROLE_SAFE_HALT,
    ROLE_LOCKED,
    StandbyExecutionBlockedError,
)
from engine.reconciliation import (
    ReconciliationEngine,
    ResolutionPolicy,
)
from engine.message_backfill import (
    MessageBackfill,
    compute_signal_id,
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    ExecutionResult,
)
from engine.notifications import (
    NotificationEngine,
    TelegramBotClient,
    MockTelegramBotClient,
    TradeNotificationEvent,
    DEFAULT_BATCH_WINDOW_SECONDS,
)
from engine.notification_engine import (
    EventNotificationEngine,
    NotificationEvent,
    NotificationEventType,
    explain_skip_reason,
)
from engine.telegram_control_bot import (
    TelegramControlBot,
    TelegramControlBotConfig,
    TelegramBotApiClient,
    HttpTelegramBotApiClient,
    MockTelegramBotApiClient,
)
from engine.local_api_server import (
    LocalAPIServer,
    APIServerConfig,
    get_default_session_token_path,
)
from engine.telegram_gateway import (
    TelegramGateway,
    TelegramGatewayConfig,
    TelegramMessage,
    ConnectionState,
)
from engine.tdlib_client import (
    TDLibClientInterface,
    NativeTDLibClient,
    MockTDLibClient,
)

logger = logging.getLogger("pipvane.engine.daemon")

VERSION = "3.0.0"
APP_NAME = "Pipvane v3 Local Agent"


# =============================================================================
# Helper Utilities: Paths and Windows Environment Check
# =============================================================================

def get_default_pipvane_data_dir() -> Path:
    """Return default storage path: %APPDATA%\\Pipvane or ~/.pipvane."""
    appdata = os.environ.get("APPDATA")
    if appdata:
        base = Path(appdata) / "Pipvane"
    else:
        base = Path.home() / ".pipvane"
    base.mkdir(parents=True, exist_ok=True)
    return base


def is_running_in_session_zero() -> bool:
    """
    Checks if current process is running in Windows Session 0 (non-interactive service).
    V3 Architectural Correction 1: MetaTrader terminals require an interactive Win32 desktop.
    """
    if sys.platform != "win32":
        return False
    try:
        kernel32 = ctypes.windll.kernel32
        session_id = ctypes.c_uint32()
        process_id = kernel32.GetCurrentProcessId()
        if kernel32.ProcessIdToSessionId(process_id, ctypes.byref(session_id)):
            return session_id.value == 0
    except Exception:
        pass
    return False


# =============================================================================
# Master Daemon Configuration
# =============================================================================

@dataclass
class MasterDaemonConfig:
    """Configuration specification for Pipvane Master Orchestration Daemon."""
    app_data_dir: Path = field(default_factory=get_default_pipvane_data_dir)
    config_path: Optional[Path] = None
    vault_path: Optional[Path] = None
    ledger_db_path: Optional[Path] = None
    orchestrator_db_path: Optional[Path] = None
    api_host: str = "127.0.0.1"
    api_port: int = 5001
    ws_port: int = 5002
    role: str = "auto"                     # "active", "standby", or "auto"
    node_id: str = field(default_factory=lambda: f"node_{uuid.uuid4().hex[:8]}")
    license_jwt: Optional[str] = None
    coordinator_url: str = "ws://127.0.0.1:8080/ws"
    mock_telegram: bool = False
    mock_terminals: bool = False
    telegram_bot_token: Optional[str] = None
    telegram_owner_chat_id: Optional[Union[str, int]] = None
    reconciliation_interval_s: float = 30.0
    headless: bool = False
    log_level: str = "INFO"
    strict_desktop_check: bool = False

    def __post_init__(self):
        self.app_data_dir = Path(self.app_data_dir)
        self.app_data_dir.mkdir(parents=True, exist_ok=True)
        if self.config_path is None:
            self.config_path = self.app_data_dir / "config.json"
        else:
            self.config_path = Path(self.config_path)

        if self.vault_path is None:
            self.vault_path = self.app_data_dir / "vault.db"
        else:
            self.vault_path = Path(self.vault_path)

        if self.ledger_db_path is None:
            self.ledger_db_path = self.app_data_dir / "ledger.db"
        else:
            self.ledger_db_path = Path(self.ledger_db_path)

        if self.orchestrator_db_path is None:
            self.orchestrator_db_path = self.app_data_dir / "orchestrator.db"
        else:
            self.orchestrator_db_path = Path(self.orchestrator_db_path)

    @classmethod
    def load_from_file(cls, path: Union[str, Path]) -> MasterDaemonConfig:
        """Loads configuration overrides from JSON file."""
        p = Path(path)
        if not p.is_file():
            return cls(config_path=p)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return cls(
                config_path=p,
                api_port=int(data.get("api_port", 5001)),
                ws_port=int(data.get("ws_port", 5002)),
                role=str(data.get("role", "auto")),
                node_id=str(data.get("node_id", f"node_{uuid.uuid4().hex[:8]}")),
                license_jwt=data.get("license_jwt"),
                coordinator_url=str(data.get("coordinator_url", "ws://127.0.0.1:8080/ws")),
                mock_telegram=bool(data.get("mock_telegram", False)),
                mock_terminals=bool(data.get("mock_terminals", False)),
                telegram_bot_token=data.get("telegram_bot_token"),
                telegram_owner_chat_id=data.get("telegram_owner_chat_id"),
                reconciliation_interval_s=float(data.get("reconciliation_interval_s", 30.0)),
                headless=bool(data.get("headless", False)),
                log_level=str(data.get("log_level", "INFO")),
            )
        except Exception as e:
            logger.warning("Error reading config file %s: %s. Using default config.", p, e)
            return cls(config_path=p)

    def save_to_file(self, path: Optional[Union[str, Path]] = None) -> None:
        """Saves current non-credential configuration to JSON file."""
        target = Path(path) if path else self.config_path
        target.parent.mkdir(parents=True, exist_ok=True)
        dump_data = {
            "api_port": self.api_port,
            "ws_port": self.ws_port,
            "role": self.role,
            "node_id": self.node_id,
            "coordinator_url": self.coordinator_url,
            "mock_telegram": self.mock_telegram,
            "mock_terminals": self.mock_terminals,
            "telegram_owner_chat_id": self.telegram_owner_chat_id,
            "reconciliation_interval_s": self.reconciliation_interval_s,
            "headless": self.headless,
            "log_level": self.log_level,
        }
        target.write_text(json.dumps(dump_data, indent=2), encoding="utf-8")


# =============================================================================
# Master Pipvane Orchestration Daemon
# =============================================================================

class PipvaneMasterDaemon:
    """
    Central Orchestration Daemon for Pipvane v3.
    Integrates all pipelines: TDLib -> Ledger -> Normalizer -> Parser -> Risk -> Orchestrator -> IPC.
    """

    def __init__(
        self,
        config: Optional[MasterDaemonConfig] = None,
        vault: Optional[VaultStorage] = None,
        tdlib_client: Optional[TDLibClientInterface] = None,
        telegram_bot_client: Optional[TelegramBotClient] = None,
        control_bot_api_client: Optional[TelegramBotApiClient] = None,
    ) -> None:
        self.config = config or MasterDaemonConfig()
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self._mock_terminal_task: Optional[asyncio.Task] = None

        # 1. Desktop Session Guard (V3 Architectural Correction 1)
        if self.config.strict_desktop_check and is_running_in_session_zero():
            raise RuntimeError(
                "CRITICAL ARCHITECTURAL VIOLATION: Pipvane v3 cannot execute inside Windows Session 0 "
                "(headless service). MetaTrader terminals require an active interactive desktop context. "
                "Please run via HKCU\\Run or standard user logon per VPS_DEPLOYMENT_GUIDE.md."
            )

        # Sprint 22: EngineBootstrap Auto-Recovery Checks (Integrity, WAL restoration, Failsafe Rebuild)
        from engine.engine_bootstrap import EngineBootstrap
        self.bootstrap = EngineBootstrap(
            vault_path=str(self.config.vault_path) if self.config.vault_path else None,
            ledger_db_path=str(self.config.ledger_db_path) if self.config.ledger_db_path else None,
            orchestrator_db_path=str(self.config.orchestrator_db_path) if self.config.orchestrator_db_path else None,
        )
        self.bootstrap_health = self.bootstrap.run_startup_database_checks()

        # 2. Storage & Vault (Zero-Knowledge DPAPI)
        self.vault = vault or VaultStorage(db_path=str(self.config.vault_path))
        self.device_profile_mgr = DeviceProfileManager()

        # 3. Licensing Client
        pub_key = (
            DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX
            if (self.config.mock_terminals and not self.config.license_jwt)
            else None
        )
        self.license_mgr = LicenseManager(
            vault=self.vault,
            bundled_public_key=pub_key,
        )
        if self.config.license_jwt:
            self.license_mgr.store_token(self.config.license_jwt)
        elif self.config.mock_terminals:
            # Seed bootstrap test/dev license for mock terminals
            dev_jwt = create_signed_jwt(
                payload={
                    "license_id": f"PV3-DEV-{self.config.node_id}",
                    "tier": "Pro Multi-Terminal",
                    "account_limit": 7,
                    "hardware_fingerprint": self.config.node_id,
                    "iat": int(time.time()),
                    "exp": int(time.time() + 30 * 86400),
                },
                private_key="b826d3096cc70c9f5286df442ae526f25b79e1a16d0a81b86dec767fca1a8627",
                algorithm="EdDSA",
            )
            self.license_mgr.store_token(dev_jwt)

        # 4. Ingestion, Normalization & Signal Parser
        self.ingestion = IngestionController(db_path=str(self.config.ledger_db_path))
        self.normalizer = TextNormalizer()
        self.parser = SignalParser()

        # 5. Risk Engine & Account Profile
        self.risk_engine = RiskEngine()
        self.account_profile = AccountProfile(
            account_id=0,
            risk_model=RiskModel.PERCENT_EQUITY,
            equity_risk_pct=1.0,
            fixed_lot=0.10,
            max_open_positions=5,
            max_signal_age_s=30.0,
            max_drift_pips=5.0,
            max_spread_pips=3.0,
            license_manager=self.license_mgr,
        )

        # 6. IPC Bridge & Terminal Discovery
        self.ipc_dispatcher = IPCDispatcher()
        self.terminal_discovery = TerminalDiscovery()
        self.integrity_checker = default_integrity_checker

        # 7. Failover Manager
        initial_role = ROLE_ACTIVE if self.config.role == "active" else (
            ROLE_STANDBY if self.config.role == "standby" else ROLE_INITIALIZING
        )
        self.failover_mgr = FailoverManager(
            config=FailoverManagerConfig(
                node_id=self.config.node_id,
                ws_url=self.config.coordinator_url,
                role_preference=self.config.role,
                lease_ttl_s=30.0,
            ),
        )
        if initial_role == ROLE_ACTIVE:
            self.failover_mgr.lease_expires_at = time.time() + 86400 * 365
        self.failover_mgr.set_role(initial_role)

        # 8. Execution Orchestrator
        self.execution_orchestrator = ExecutionOrchestrator(
            ipc_dispatcher=self.ipc_dispatcher,
            risk_engine=self.risk_engine,
            db_path=str(self.config.orchestrator_db_path),
            reconciliation_interval_s=self.config.reconciliation_interval_s,
            failover_manager=self.failover_mgr,
            initial_role=initial_role,
        )

        # 9. Orphan Manager
        self.orphan_mgr = OrphanManager(
            default_policy=OrphanResolutionPolicy.CLOSE_IMMEDIATELY,
            pipvane_magic_number=DEFAULT_PIPVANE_MAGIC,
            orchestrator=self.execution_orchestrator,
            ipc_dispatcher=self.ipc_dispatcher,
        )
        self.execution_orchestrator.orphan_manager = self.orphan_mgr

        # 10. Notification Engines
        self.telegram_bot_client = telegram_bot_client or MockTelegramBotClient()
        self.notification_engine = NotificationEngine(
            bot_client=self.telegram_bot_client,
            default_chat_id=str(self.config.telegram_owner_chat_id or "0"),
            batch_window_seconds=DEFAULT_BATCH_WINDOW_SECONDS,
        )
        self.execution_orchestrator.notification_engine = self.notification_engine

        self.event_notification_engine = EventNotificationEngine()

        # 11. Personal Telegram Control Bot
        self.control_bot: Optional[TelegramControlBot] = None
        if self.config.telegram_bot_token or control_bot_api_client is not None:
            cbot_client = control_bot_api_client or (
                MockTelegramBotApiClient()
                if self.config.mock_telegram
                else HttpTelegramBotApiClient(bot_token=self.config.telegram_bot_token or "mock_token")
            )
            self.control_bot = TelegramControlBot(
                config=TelegramControlBotConfig(
                    bot_token=self.config.telegram_bot_token or "mock_token",
                    owner_chat_id=self.config.telegram_owner_chat_id or 0,
                ),
                orchestrator=self.execution_orchestrator,
                failover_manager=self.failover_mgr,
                api_client=cbot_client,
            )

        # 12. Local REST & WebSocket API Server
        session_token_file = self.config.app_data_dir / "session.token"
        self.api_server = LocalAPIServer(
            config=APIServerConfig(
                host="127.0.0.1",
                port=self.config.api_port,
                ws_port=self.config.ws_port,
                token_path=session_token_file,
            ),
            engine_context={
                "ipc_dispatcher": self.ipc_dispatcher,
                "risk_engine": self.risk_engine,
                "license_manager": self.license_mgr,
                "execution_orchestrator": self.execution_orchestrator,
                "failover_manager": self.failover_mgr,
                "terminal_discovery": self.terminal_discovery,
                "daemon": self,
            },
        )

        # 13. Telegram Ingestion Gateway (Strict Read-Only Safety Mode)
        td_client = tdlib_client or (
            MockTDLibClient() if self.config.mock_telegram else NativeTDLibClient()
        )
        self.telegram_gateway = TelegramGateway(
            config=TelegramGatewayConfig(
                account_id="primary",
                safety_mode=True,
                database_directory=str(self.config.app_data_dir / "tdlib_data"),
                files_directory=str(self.config.app_data_dir / "tdlib_files"),
            ),
            vault=self.vault,
            tdlib_client=td_client,
        )

        # 14. Wire Event Handlers & Callbacks
        self._wire_event_handlers()

        # Metrics & Telemetry
        self.signals_received_count: int = 0
        self.orders_executed_count: int = 0
        self.signals_skipped_count: int = 0

    # -------------------------------------------------------------------------
    # Event Wiring & Pipeline Hookup
    # -------------------------------------------------------------------------

    def _wire_event_handlers(self) -> None:
        """Binds callbacks across Telegram Ingestion, Risk, Orchestrator, and Telemetry."""
        # A. Telegram Gateway Inbound Messages
        self.telegram_gateway.add_message_handler(self.handle_inbound_telegram_message)

        # B. Execution Orchestrator Order Lifecycle Events
        self.execution_orchestrator.add_event_listener(self._handle_orchestrator_event)

        # C. IPC Dispatcher Terminal Feedback & State Updates
        self.ipc_dispatcher.on_execution_feedback(self._handle_terminal_feedback)
        self.ipc_dispatcher.on_account_state(self._handle_account_state_update)

        # D. Failover Manager Role Transitions
        self.failover_mgr.add_role_change_listener(self._handle_role_transition)

        # E. Event Notification Engine Subsystem Hooks
        if hasattr(self.event_notification_engine, "attach_orchestrator"):
            self.event_notification_engine.attach_orchestrator(self.execution_orchestrator)
        if hasattr(self.event_notification_engine, "attach_failover_manager"):
            self.event_notification_engine.attach_failover_manager(self.failover_mgr)

    # -------------------------------------------------------------------------
    # Core Signal Processing Pipeline
    # -------------------------------------------------------------------------

    def handle_inbound_telegram_message(self, message: Union[TelegramMessage, Dict[str, Any]]) -> None:
        """
        Main processing pipeline for all incoming Telegram channel messages:
        1. Ledger Storage: Record raw message in SQLite WAL with SHA-256 hash chaining.
        2. ADR-002 Normalization: Clean homoglyphs, emojis, invisibles, and numerals (<50us).
        3. Grammar Parsing: Classify message (signal/update/chatter) and extract fields.
        4. Standby & Circuit Breaker Check: Verify execution authorization.
        5. Risk Engine Evaluation: Check entry guards and compute quantized lot sizing.
        6. Execution Orchestration: Dispatch to MT4/MT5 terminals via SPSC Shared Memory.
        """
        # Extract fields
        if isinstance(message, TelegramMessage):
            chat_id = message.chat_id
            msg_id = message.message_id
            msg_date = message.date
            raw_text = message.text
        else:
            chat_id = int(message.get("chat_id", 0))
            msg_id = int(message.get("message_id", message.get("id", 0)))
            msg_date = int(message.get("date", time.time()))
            raw_text = str(message.get("text", ""))

        if not raw_text.strip():
            return

        self.signals_received_count += 1
        now = time.time()

        # Step 1: Record in SQLite Append-Only Ledger
        try:
            self.ingestion.handle_new_message(
                channel_id=chat_id,
                message_id=msg_id,
                raw_text=raw_text,
                timestamp=msg_date,
            )
        except Exception as e:
            logger.warning("Error recording message in ingestion ledger: %s", e)

        # Step 2: Deterministic Normalization (<50us)
        cleaned_text = clean_text(raw_text)

        # Step 3: Parse Signal and Format Pack Matching
        parsed = self.parser.parse(
            raw_text=cleaned_text,
        )

        # Step 4: Handle Message Classification
        if parsed.message_type == MessageType.CHATTER.value:
            logger.debug("Chatter message ignored: %s", raw_text[:40])
            return

        # Handle Management Updates (BE, Close, Partial Close, SL/TP Modify)
        if parsed.message_type == MessageType.UPDATE.value or parsed.is_modification:
            self._handle_management_update(parsed, chat_id, msg_id)
            return

        # Handle New Trade Directives (BUY / SELL)
        if parsed.message_type == MessageType.SIGNAL.value:
            self._handle_trade_signal(parsed, chat_id, msg_id, raw_text)

    def _handle_trade_signal(
        self, parsed: ParsedSignal, chat_id: int, msg_id: int, raw_text: str
    ) -> None:
        """Processes a newly parsed trade order directive through Risk and Orchestrator."""
        now = time.time()
        signal_id = compute_signal_id(chat_id, msg_id)

        # 1. High-Availability / Standby Mode Check (Invariant |A(t)| <= 1)
        if not self.failover_mgr.can_execute_orders:
            role_name = self.failover_mgr.role.value
            logger.warning(
                "STANDBY LOCKOUT: Signal %d on %s blocked. Local node is in %s mode.",
                signal_id, parsed.symbol, role_name,
            )
            self._record_skip(
                code="SKIP_STANDBY_LOCK",
                reason=f"Standby Lockout enforced: Node is in {role_name} mode.",
                signal_id=signal_id,
                symbol=parsed.symbol or "UNKNOWN",
            )
            return

        # 2. Geometry Validation Check
        if not parsed.geometry_valid:
            logger.warning("SKIP_GEOMETRY: Signal %d failed geometry: %s", signal_id, parsed.skip_reason)
            self._record_skip(
                code=SKIP_GEOMETRY,
                reason=parsed.skip_reason or "Invalid SL/TP market geometry",
                signal_id=signal_id,
                symbol=parsed.symbol or "UNKNOWN",
            )
            return

        # 3. Market Quote Resolution
        symbol = (parsed.symbol or "EURUSD").upper()
        quote = self._resolve_current_quote(symbol, parsed.entry_price)

        # 4. Synthesize Base OrderIntent
        side = OrderSide.BUY if parsed.action == "BUY" else OrderSide.SELL
        order_type = ActionType.MARKET_ENTRY
        if parsed.order_type == "LIMIT":
            order_type = ActionType.LIMIT_ENTRY
        elif parsed.order_type == "STOP":
            order_type = ActionType.STOP_ENTRY

        base_intent = OrderIntent(
            symbol=symbol,
            side=side,
            action_type=order_type,
            volume=0.01,  # Temporary; quantized below
            price=parsed.entry_price or quote.ask if side == OrderSide.BUY else quote.bid,
            stop_loss=parsed.stop_loss or 0.0,
            take_profit=parsed.take_profits[0] if parsed.take_profits else 0.0,
            slippage_cap_pips=1.5,
            magic_number=DEFAULT_PIPVANE_MAGIC,
            signal_id=signal_id,
            timestamp_ns=time.time_ns(),
        )

        # 5. Risk Engine Evaluation
        try:
            eval_result = self.risk_engine.evaluate(
                intent=base_intent,
                account=self.account_profile,
                quote=quote,
                eval_time=now,
            )
        except LicenseExpiredError as lee:
            logger.error("LICENSE_EXPIRED during risk evaluation: %s", lee)
            self._record_skip(
                code=SKIP_LICENSE_EXPIRED,
                reason=str(lee),
                signal_id=signal_id,
                symbol=symbol,
            )
            return
        except Exception as e:
            logger.error("Unexpected error in RiskEngine.evaluate: %s", e)
            return

        # If rejected by risk guards
        if isinstance(eval_result, SkipReason):
            logger.warning("Trade rejected by Risk Guard: %s (%s)", eval_result.code, eval_result.reason)
            self._record_skip(
                code=eval_result.code,
                reason=eval_result.reason,
                signal_id=signal_id,
                symbol=symbol,
            )
            return

        # Risk check passed: eval_result contains validated OrderIntent with quantized volume
        validated_intent = eval_result

        # 6. Dispatch to Registered MetaTrader Terminals
        terminals = self.ipc_dispatcher.get_registered_terminals()
        if not terminals:
            logger.info("No MetaTrader terminals enrolled. OrderIntent %d buffered.", signal_id)
            return

        # Submit to all registered active terminals asynchronously
        for tid in terminals:
            # Handle TP Ladders vs Virtual Netting
            tp_ladder = parsed.take_profits if len(parsed.take_profits) > 1 else None
            asyncio.create_task(
                self._dispatch_to_terminal(tid, validated_intent, tp_ladder=tp_ladder)
            )

    async def _dispatch_to_terminal(
        self, terminal_id: str, intent: OrderIntent, tp_ladder: Optional[List[float]] = None
    ) -> None:
        """Asynchronously dispatches order intent to target terminal serialization queue."""
        try:
            intents_to_submit = [intent]
            if tp_ladder and len(tp_ladder) > 1:
                trade_orch = TradeOrchestrator(account_mode=AccountMode.HEDGING)
                intents_to_submit = trade_orch.orchestrate_order(intent=intent, tp_ladder=tp_ladder)

            for sub_intent in intents_to_submit:
                res = await self.execution_orchestrator.submit_order(
                    terminal_id=terminal_id,
                    intent=sub_intent,
                    account_profile=self.account_profile,
                )
                if res.success:
                    self.orders_executed_count += 1
                    logger.info(
                        "Order executed successfully on %s for signal %d (Status=%s)",
                        terminal_id, sub_intent.signal_id, res.status.name,
                    )
                    self.api_server.broadcast_trade_alert({
                        "terminal_id": terminal_id,
                        "signal_id": sub_intent.signal_id,
                        "symbol": sub_intent.symbol,
                        "side": sub_intent.side.name,
                        "volume": sub_intent.volume,
                        "price": sub_intent.price,
                        "status": "FILLED",
                    })
                else:
                    skip_code = res.skip_reason.code if res.skip_reason else res.status.name
                    skip_msg = res.skip_reason.reason if res.skip_reason else "Execution rejected by terminal"
                    logger.warning(
                        "Order execution rejected on %s for signal %d: %s",
                        terminal_id, sub_intent.signal_id, skip_msg,
                    )
                    self._record_skip(skip_code, skip_msg, sub_intent.signal_id, sub_intent.symbol)
        except Exception as e:
            logger.error("Exception dispatching order to terminal %s: %s", terminal_id, e)

    def _handle_management_update(self, parsed: ParsedSignal, chat_id: int, msg_id: int) -> None:
        """Processes position management commands (Partial Close, Break-Even, SL/TP Modify, Full Close)."""
        logger.info(
            "Management update received: MoveToBE=%s, PartialPct=%s, FullClose=%s",
            parsed.move_to_be, parsed.partial_close_pct, parsed.full_close,
        )
        # Dispatch to execution orchestrator
        if parsed.full_close:
            asyncio.create_task(
                self.execution_orchestrator.manual_close_all(reason=f"Telegram channel {chat_id} full close directive")
            )

    def _record_skip(self, code: str, reason: str, signal_id: int, symbol: str) -> None:
        """Records a trade skip event, alerts UI, and notifies event listeners."""
        self.signals_skipped_count += 1
        friendly_explanation = explain_skip_reason(code)

        # Broadcast to Local Web UI
        self.api_server.add_log("WARNING", f"Trade Skipped [{code}]: {symbol} - {friendly_explanation}")
        self.api_server.broadcast_event("trade_skipped", {
            "code": code,
            "reason": reason,
            "explanation": friendly_explanation,
            "signal_id": signal_id,
            "symbol": symbol,
            "timestamp": time.time(),
        })

        # Dispatch structured notification event
        if hasattr(self.event_notification_engine, "notify_signal_skipped"):
            self.event_notification_engine.notify_signal_skipped(
                symbol=symbol,
                skip_reason=f"{code}: {reason}",
            )
        elif hasattr(self.event_notification_engine, "emit"):
            self.event_notification_engine.emit(
                NotificationEvent(
                    event_type=NotificationEventType.SIGNAL_SKIPPED,
                    data={"code": code, "reason": reason, "signal_id": signal_id, "symbol": symbol},
                )
            )

    def _resolve_current_quote(self, symbol: str, hint_price: Optional[float] = None) -> MarketQuote:
        """Resolves current market quote from terminal telemetry or baseline hint price."""
        ref_price = hint_price or (1.08500 if "EUR" in symbol else (2000.0 if "XAU" in symbol else 100.0))
        pip_size = 0.01 if "JPY" in symbol else (0.10 if "XAU" in symbol or "GOLD" in symbol else 0.0001)
        spread_val = pip_size * 1.2
        return MarketQuote(
            symbol=symbol,
            bid=ref_price,
            ask=ref_price + spread_val,
            pip_size=pip_size,
        )

    # -------------------------------------------------------------------------
    # Telemetry & Callbacks from IPC and Subsystems
    # -------------------------------------------------------------------------

    def _handle_orchestrator_event(self, evt: Dict[str, Any]) -> None:
        """Dispatches orchestrator lifecycle events to LocalAPIServer and logs."""
        event_type = evt.get("event_type", "UNKNOWN")
        ticket = evt.get("ticket", 0)
        t_id = evt.get("terminal_id", "")
        self.api_server.add_log("INFO", f"Orchestrator [{event_type}]: Ticket #{ticket} on {t_id}")
        self.api_server.broadcast_event("orchestrator_event", evt)

    def _handle_terminal_feedback(self, terminal_id: str, feedback: ExecutionFeedback) -> None:
        """Processes execution feedback from C++ bridge."""
        self.api_server.broadcast_trade_alert({
            "terminal_id": terminal_id,
            "order_ticket": feedback.order_ticket,
            "signal_id": feedback.signal_id,
            "executed_volume": feedback.executed_volume,
            "executed_price": feedback.executed_price,
            "status": feedback.execution_status.name,
            "retcode": feedback.broker_retcode,
        })

    def _handle_account_state_update(self, terminal_id: str, state: AccountState) -> None:
        """Processes live equity and margin telemetry streamed from MetaTrader Named Pipe."""
        self.api_server.update_terminal_state(terminal_id, {
            "balance": state.balance,
            "equity": state.equity,
            "margin": state.margin,
            "free_margin": state.free_margin,
            "open_positions": state.open_positions_count,
            "connected_to_broker": state.is_connected_to_broker,
        })
        # Update global account profile references for Risk Engine
        self.account_profile.equity = self.execution_orchestrator.get_total_equity() or state.equity
        self.account_profile.balance = self.execution_orchestrator.get_total_balance() or state.balance

    def _handle_role_transition(self, old_role: EngineRole, new_role: EngineRole) -> None:
        """Handles high-availability role change (e.g. STANDBY -> ACTIVE)."""
        logger.warning("Daemon handling role transition: %s -> %s", old_role.value, new_role.value)
        self.execution_orchestrator.set_engine_role(new_role)
        self.api_server.add_log(
            "INFO" if new_role == ROLE_ACTIVE else "WARNING",
            f"High-Availability Role: {new_role.value}",
        )
        self.api_server.broadcast_event("role_changed", {
            "old_role": old_role.value,
            "new_role": new_role.value,
            "timestamp": time.time(),
        })

    # -------------------------------------------------------------------------
    # MetaTrader Terminal Setup & Dynamic Enrollment
    # -------------------------------------------------------------------------

    def setup_terminals(self) -> None:
        """
        Discovers local MetaTrader installations and dynamically enrolls them
        into the IPCDispatcher and ExecutionOrchestrator.
        """
        logger.info("Scanning host system for MetaTrader terminals...")
        discovered = self.terminal_discovery.scan_all()

        if discovered:
            logger.info("Found %d MetaTrader terminal(s) on host.", len(discovered))
            for term in discovered:
                # Enroll into IPC Dispatcher with EX4 integrity validation
                try:
                    self.terminal_discovery.connect_terminal(
                        dispatcher=self.ipc_dispatcher,
                        terminal_id=term.terminal_id,
                        use_mock=self.config.mock_terminals,
                    )
                    # Register configuration with ExecutionOrchestrator
                    self.execution_orchestrator.register_terminal_config(
                        TerminalExecutionConfig(
                            terminal_id=term.terminal_id,
                            platform_type=0 if term.platform_type == "MT4" else 1,
                            supports_hedging=True,
                            is_netting=False,
                            is_ecn=False,
                        )
                    )
                    # Register with Local API server
                    self.api_server.register_terminal({
                        "id": term.terminal_id,
                        "name": term.name or f"{term.platform_type} Terminal",
                        "type": term.platform_type,
                        "path": term.executable_path,
                        "status": "connected",
                        "latency_ms": 0.85,
                        "balance": 10000.0,
                        "equity": 10000.0,
                    })
                except Exception as e:
                    logger.warning("Error enrolling terminal %s: %s", term.terminal_id, e)
        else:
            logger.info("No active MetaTrader installations discovered on standard paths.")
            if self.config.mock_terminals:
                self._seed_mock_terminal()

    def _seed_mock_terminal(self) -> None:
        """Seeds a mock MT5 demo terminal for development and testing environments."""
        mock_tid = f"mock_mt5_{uuid.uuid4().hex[:6]}"
        logger.info("Seeding mock terminal '%s' for testing/standalone operation.", mock_tid)
        ctx = self.ipc_dispatcher.register_terminal(
            terminal_id=mock_tid,
            shm_name=f"Local\\Pipvane_SHM_{mock_tid}",
            pipe_name=f"\\\\.\\pipe\\pipvane_{mock_tid}",
            use_mock=True,
        )
        # Advance mock terminal to CONNECTED status via simulated pipe handshake
        hello = InitHelloPayload(
            terminal_id=mock_tid,
            platform_type=1,  # MT5
            account_login=999888,
            broker_server="Pipvane-Simulated-Demo",
        )
        ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
        state = AccountState(
            account_login=999888,
            balance=10000.0,
            equity=10000.0,
            margin=0.0,
            free_margin=10000.0,
            is_connected_to_broker=True,
        )
        ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack()))

        self.execution_orchestrator.register_terminal_config(
            TerminalExecutionConfig(
                terminal_id=mock_tid,
                platform_type=1,
                supports_hedging=True,
                is_netting=False,
            )
        )
        self.api_server.register_terminal({
            "id": mock_tid,
            "name": "Simulated MT5 Demo Terminal",
            "type": "MT5",
            "path": "C:\\Program Files\\MetaTrader 5\\terminal64.exe",
            "status": "connected",
            "latency_ms": 0.42,
            "balance": 10000.0,
            "equity": 10000.0,
            "margin": 0.0,
            "free_margin": 10000.0,
        })

    async def _run_mock_terminal_loop(self) -> None:
        """Background simulator draining mock terminal SHM rings and sending execution feedback."""
        ticket_counter = 100001
        while self._is_running:
            try:
                for tid, ctx in list(self.ipc_dispatcher._terminals.items()):
                    if not ctx.shm_writer.use_mock_buffer:
                        continue
                    # Keep mock terminal heartbeat fresh
                    ctx.last_heartbeat_time = time.time()
                    buf = ctx.shm_writer._get_buffer()
                    write_head = struct.unpack_from("<Q", buf, 64)[0]
                    read_tail = struct.unpack_from("<Q", buf, 128)[0]
                    while read_tail < write_head:
                        slot_idx = read_tail & (ctx.shm_writer.capacity - 1)
                        slot_offset = ctx.shm_writer.header_size + (slot_idx * ctx.shm_writer.slot_size)
                        intent = deserialize_order_intent(buf, slot_offset)
                        struct.pack_into("<Q", buf, 128, read_tail + 1)
                        read_tail += 1

                        exec_price = intent.price if intent.price > 0 else 1.08500
                        fb = ExecutionFeedback(
                            signal_id=intent.signal_id,
                            order_ticket=ticket_counter,
                            execution_status=ExecutionStatus.FILLED,
                            executed_volume=intent.volume,
                            executed_price=exec_price,
                        )
                        ticket_counter += 1
                        ctx.pipe_server.simulate_inbound_data(
                            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in mock terminal simulation loop: %s", e)
            await asyncio.sleep(0.01)

    # -------------------------------------------------------------------------
    # Daemon Async Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts all Pipvane v3 daemon subsystems in coordinated sequence."""
        if self._is_running:
            return
        self._is_running = True

        logger.info("=" * 65)
        logger.info("Starting %s (v%s)...", APP_NAME, VERSION)
        logger.info("Node ID: %s | Role: %s", self.config.node_id, self.config.role)
        logger.info("Data Directory: %s", self.config.app_data_dir)
        logger.info("=" * 65)

        # 1. Start Local API Server (HTTP REST & WebSocket telemetry)
        self.api_server.start()
        logger.info("Local API Server: %s", self.api_server.get_http_url())
        logger.info("Session Token persisted: %s", self.api_server.token_path)

        # 2. Discover and Enroll MetaTrader Terminals
        self.setup_terminals()

        # 3. Start IPC Dispatcher (SHM & Named Pipes)
        await self.ipc_dispatcher.start()

        # 4. Start Execution Orchestrator (Reconciliation loop & workers)
        await self.execution_orchestrator.start()

        # 5. Start Failover Manager
        if self.config.role == "auto":
            try:
                await self.failover_mgr.start()
            except Exception as e:
                logger.warning("Cloud lease coordinator offline: %s. Defaulting to Standby mode.", e)
                self.failover_mgr.set_role(ROLE_STANDBY)
        elif self.config.role == "active":
            self.failover_mgr.lease_expires_at = time.time() + 86400 * 365
            self.failover_mgr.set_role(ROLE_ACTIVE)
            self.execution_orchestrator.unlock()
        else:
            self.failover_mgr.set_role(ROLE_STANDBY)
            self.execution_orchestrator.lock()

        # 6. Start Event Notification Engine
        if hasattr(self.event_notification_engine, "start"):
            await self.event_notification_engine.start()

        # 6. Start Personal Telegram Control Bot
        if self.control_bot:
            await self.control_bot.start()

        # 7. Start Telegram Ingestion Gateway (Strict Safety Mode)
        try:
            self.telegram_gateway.start()
            logger.info("Telegram Ingestion Gateway online in Strict Safety Mode.")
        except Exception as e:
            logger.warning("Could not connect to Telegram Gateway: %s", e)

        # 8. Start Mock Terminal Simulator if running with mock terminals
        if self.config.mock_terminals:
            self._mock_terminal_task = asyncio.create_task(self._run_mock_terminal_loop())

        # 9. Sprint 22: If failsafe rebuild occurred, execute state reconstruction pipeline
        if getattr(self.bootstrap, "failsafe_rebuild_triggered", False):
            logger.warning("Failsafe rebuild was triggered during bootstrap. Rebuilding state from Telegram & Broker...")
            try:
                await self.bootstrap.execute_failsafe_rebuild_pipeline_async(
                    gateway=self.telegram_gateway,
                    orchestrator=self.execution_orchestrator,
                )
            except Exception as e:
                logger.error("Error executing failsafe rebuild state reconstruction: %s", e)

        logger.info("All Pipvane v3 subsystems started successfully.")

    async def stop(self) -> None:
        """Gracefully halts all subsystems in reverse dependency order."""
        if not self._is_running:
            return
        logger.info("Initiating graceful shutdown of Pipvane Master Daemon...")
        self._is_running = False

        # Stop mock terminal simulation worker
        if self._mock_terminal_task and not self._mock_terminal_task.done():
            self._mock_terminal_task.cancel()
            try:
                await self._mock_terminal_task
            except asyncio.CancelledError:
                pass

        # 1. Stop Telegram Ingestion Gateway
        try:
            self.telegram_gateway.stop()
        except Exception as e:
            logger.debug("Error stopping Telegram Gateway: %s", e)

        # 2. Stop Control Bot
        if self.control_bot:
            try:
                await self.control_bot.stop()
            except Exception as e:
                logger.debug("Error stopping Control Bot: %s", e)

        # 3. Stop Execution Orchestrator & 30s Reconciliation loop
        try:
            await self.execution_orchestrator.stop()
        except Exception as e:
            logger.debug("Error stopping Execution Orchestrator: %s", e)

        # 4. Stop Failover Manager
        try:
            await self.failover_mgr.stop()
        except Exception as e:
            logger.debug("Error stopping Failover Manager: %s", e)

        # 5. Stop IPC Dispatcher
        try:
            await self.ipc_dispatcher.stop()
        except Exception as e:
            logger.debug("Error stopping IPC Dispatcher: %s", e)

        # 6. Stop Local API Server
        try:
            self.api_server.stop()
        except Exception as e:
            logger.debug("Error stopping Local API Server: %s", e)

        # 7. Stop Event Notification Engine
        if hasattr(self.event_notification_engine, "stop"):
            try:
                await self.event_notification_engine.stop()
            except Exception as e:
                logger.debug("Error stopping Event Notification Engine: %s", e)

        # 7. Close SQLite Ingestion Ledger
        try:
            self.ingestion.close()
        except Exception as e:
            logger.debug("Error closing Ingestion ledger: %s", e)

        self._shutdown_event.set()
        logger.info("Pipvane Master Daemon shutdown complete.")

    async def run(self) -> None:
        """Runs the daemon until shutdown signal is received."""
        await self.start()
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    def request_shutdown(self) -> None:
        """Thread-safe request to initiate daemon shutdown."""
        self._shutdown_event.set()


# =============================================================================
# CLI Entry Point & Signal Handling
# =============================================================================

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parses command-line arguments for the Pipvane daemon."""
    parser = argparse.ArgumentParser(
        prog="pipvane",
        description=f"{APP_NAME} (v{VERSION}) - Production Signal Copier & Trade Automation Engine",
    )
    parser.add_argument(
        "--config", "-c",
        type=str,
        default=None,
        help="Path to JSON configuration file (defaults to %%APPDATA%%\\Pipvane\\config.json)",
    )
    parser.add_argument(
        "--role", "-r",
        choices=["active", "standby", "auto"],
        default="auto",
        help="High-availability execution role (default: auto)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=5001,
        help="HTTP REST port for local API server (default: 5001)",
    )
    parser.add_argument(
        "--ws-port",
        type=int,
        default=5002,
        help="WebSocket telemetry port (default: 5002)",
    )
    parser.add_argument(
        "--mock-telegram",
        action="store_true",
        help="Use simulated TDLib driver for offline testing",
    )
    parser.add_argument(
        "--mock-terminals",
        action="store_true",
        help="Seed mock MetaTrader terminals for development",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run without opening browser or visual tray components",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose DEBUG logging",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"{APP_NAME} {VERSION}",
    )
    return parser.parse_args(argv)


def setup_logging(verbose: bool = False) -> None:
    """Initializes structured console logging with timestamping."""
    lvl = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=lvl,
        format="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(argv: Optional[List[str]] = None) -> int:
    """CLI Main Entry Point for Pipvane daemon."""
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)

    # Load configuration
    cfg = MasterDaemonConfig.load_from_file(args.config) if args.config else MasterDaemonConfig()
    cfg.role = args.role
    cfg.api_port = args.port
    cfg.ws_port = args.ws_port
    if args.mock_telegram:
        cfg.mock_telegram = True
    if args.mock_terminals:
        cfg.mock_terminals = True
    if args.headless:
        cfg.headless = True

    daemon = PipvaneMasterDaemon(config=cfg)

    # Windows & POSIX Signal Handlers
    def _handle_signal(sig, frame):
        logger.info("Received termination signal (%s). Requesting shutdown...", sig)
        daemon.request_shutdown()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        asyncio.run(daemon.run())
        return 0
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Exiting.")
        return 0
    except Exception as e:
        logger.critical("Fatal daemon crash: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
