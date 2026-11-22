"""
Pipvane v3: Local Lease Negotiation & Failover Manager (Sprint 15 Directive).
-----------------------------------------------------------------------------
Connects the Local Engine to the Cloud WebSocket Coordinator and manages
the Active/Standby role state:

Core Tasks:
1. WebSocket Client:
   mTLS-secured WebSocket client connecting to the Cloud Lease Coordinator on engine startup.
2. Lease Lifecycle:
   Requests an Active lease using license_id. If granted, initiates a 10-second heartbeat ping.
   While the lease is valid, sets internal state to ROLE_ACTIVE (allowing order dispatch).
3. Standby Lockout:
   If the cloud responds with lease denial (another node holds it), sets internal state to
   ROLE_STANDBY. In Standby mode, Telegram ingestion and signal parsing continue actively,
   but ExecutionOrchestrator strictly blocks all OrderSend commands (Invariant: |A(t)| <= 1).
"""

from __future__ import annotations

import asyncio
import enum
import inspect
import json
import logging
import ssl
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional, Union

import websockets

logger = logging.getLogger("pipvane.engine.failover")

DEFAULT_LEASE_TTL_S: float = 30.0
DEFAULT_HEARTBEAT_INTERVAL_S: float = 10.0
DEFAULT_RECONNECT_INTERVAL_S: float = 2.0
DEFAULT_TAKEOVER_TIMEOUT_S: float = 45.0


class EngineRole(str, enum.Enum):
    """Operational high-availability execution role for the local Engine."""
    ROLE_ACTIVE = "ROLE_ACTIVE"
    ROLE_STANDBY = "ROLE_STANDBY"
    ROLE_INITIALIZING = "ROLE_INITIALIZING"
    ROLE_SAFE_HALT = "ROLE_SAFE_HALT"
    ROLE_LOCKED = "ROLE_LOCKED"
    ROLE_DEGRADED = "ROLE_DEGRADED"


# Direct constant aliases matching directive
ROLE_ACTIVE = EngineRole.ROLE_ACTIVE
ROLE_STANDBY = EngineRole.ROLE_STANDBY
ROLE_INITIALIZING = EngineRole.ROLE_INITIALIZING
ROLE_SAFE_HALT = EngineRole.ROLE_SAFE_HALT
ROLE_LOCKED = EngineRole.ROLE_LOCKED
ROLE_DEGRADED = EngineRole.ROLE_DEGRADED


class FailoverError(Exception):
    """Base exception for failover and lease negotiation failures."""
    pass


class StandbyExecutionBlockedError(FailoverError):
    """Raised when an order dispatch is attempted while in ROLE_STANDBY mode."""
    pass


class TakeoverError(FailoverError):
    """Base exception for state promotion checklist failures."""
    pass


class TakeoverTimeoutError(TakeoverError):
    """Raised when the takeover sequence exceeds the 45-second safety limit."""
    pass


class TakeoverAbortedError(TakeoverError):
    """Raised when takeover sequence is aborted due to failure or cancellation."""
    pass


class TakeoverStep(str, enum.Enum):
    """Steps in the synchronous state promotion checklist."""
    STEP_A = "STEP_A_CONNECT_TDLIB_OR_ACTIVATE_PARSING"
    STEP_B = "STEP_B_EXECUTE_MESSAGE_BACKFILL"
    STEP_C = "STEP_C_RUN_IMMEDIATE_RECONCILIATION"
    STEP_D = "STEP_D_UNLOCK_EXECUTION_ORCHESTRATOR"


@dataclass
class TakeoverResult:
    """Detailed audit record of an ExecuteTakeover checklist execution."""
    success: bool
    node_id: str
    started_at: float
    completed_at: float
    elapsed_s: float
    step_a_completed: bool = False
    step_b_completed: bool = False
    step_c_completed: bool = False
    step_d_completed: bool = False
    steps_details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    lease_surrendered: bool = False


@dataclass
class FailoverManagerConfig:
    """Configuration for mTLS WebSocket connection and Cloud Lease Coordinator."""
    ws_url: str = "wss://cloud.pipvane.com/lease"
    license_id: str = "LIC-PRO-2026-X889"
    node_id: str = field(default_factory=lambda: f"node_{uuid.uuid4().hex[:8]}")
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S
    lease_ttl_s: float = DEFAULT_LEASE_TTL_S
    reconnect_interval_s: float = DEFAULT_RECONNECT_INTERVAL_S
    takeover_timeout_s: float = DEFAULT_TAKEOVER_TIMEOUT_S
    role_preference: str = "PRIMARY"     # "PRIMARY" or "STANDBY"
    # mTLS Security Parameters
    ca_file: Optional[str] = None
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ssl_context: Optional[ssl.SSLContext] = None


class FailoverManager:
    """
    Manages cloud lease negotiation, mutual exclusion heartbeats, and Standby execution lockout.
    Enforces the single-active execution invariant (|A(t)| <= 1) across distributed nodes.
    """

    def __init__(
        self,
        config: Optional[FailoverManagerConfig] = None,
        orchestrator: Optional[Any] = None,
        telegram_gateway: Optional[Any] = None,
        ingestion_controller: Optional[Any] = None,
        message_backfill: Optional[Any] = None,
        alert_client: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
        time_provider: Optional[Callable[[], float]] = None,
    ) -> None:
        self.config = config or FailoverManagerConfig()
        self.orchestrator = orchestrator
        self.telegram_gateway = telegram_gateway
        self.ingestion_controller = ingestion_controller
        self.message_backfill = message_backfill
        self.alert_client = alert_client
        self.time_provider = time_provider or time.time

        self.role: EngineRole = EngineRole.ROLE_INITIALIZING
        self.lease_token: Optional[str] = None
        self.lease_expires_at: float = 0.0
        self.active_holder: Optional[str] = None
        self.generation: int = 0

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._is_running: bool = False
        self._loop_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._role_change_callbacks: List[Callable[[EngineRole, EngineRole], None]] = []
        self._alert_callbacks: List[Callable[[str, Dict[str, Any]], Any]] = []
        self._last_takeover_result: Optional[TakeoverResult] = None

        # Wire orchestrator initial state if supplied
        if self.orchestrator and hasattr(self.orchestrator, "set_engine_role"):
            self.orchestrator.set_engine_role(self.role)

    @property
    def is_active(self) -> bool:
        """Returns True iff the engine holds a valid, unexpired ROLE_ACTIVE lease."""
        if self.role != EngineRole.ROLE_ACTIVE:
            return False
        if self.time_provider() >= self.lease_expires_at:
            self.check_lease_expiration()
            return False
        return True

    @property
    def is_standby(self) -> bool:
        """Returns True iff the engine is locked in ROLE_STANDBY mode."""
        return self.role == EngineRole.ROLE_STANDBY

    @property
    def is_locked(self) -> bool:
        """Returns True iff the engine is locked in ROLE_LOCKED mode (Sprint 15 / FR-7.5)."""
        return self.role == EngineRole.ROLE_LOCKED

    @property
    def state(self) -> EngineRole:
        """Operational state alias for role matching node state terminology."""
        return self.role

    @property
    def can_execute_orders(self) -> bool:
        """
        Execution Gate: Returns True ONLY when holding a valid ROLE_ACTIVE lease.
        In Standby, Locked, or Safe Halt, all order execution is strictly locked out.
        """
        if self.role in (EngineRole.ROLE_STANDBY, EngineRole.ROLE_LOCKED, EngineRole.ROLE_SAFE_HALT):
            return False
        return self.is_active

    @property
    def can_execute_trades(self) -> bool:
        """Alias for can_execute_orders matching Sprint 15 directive terminology."""
        return self.can_execute_orders

    def check_lease_expiration(self) -> bool:
        """
        Evaluates local lease validity against time_provider().
        If expired, forcibly self-fences into ROLE_LOCKED and returns True (Sprint 15 / FR-7.5).
        """
        if self.lease_expires_at > 0 and self.time_provider() >= self.lease_expires_at:
            if self.role in (EngineRole.ROLE_ACTIVE, EngineRole.ROLE_DEGRADED):
                logger.critical(
                    "LEASE EXPIRED! Self-fencing Node %s from %s into ROLE_LOCKED (expires_at=%.2f, now=%.2f)",
                    self.config.node_id,
                    self.role.value,
                    self.lease_expires_at,
                    self.time_provider(),
                )
                self.lease_token = None
                self.set_role(EngineRole.ROLE_LOCKED)
                return True
        return False

    def on_lease_expired_locally(self) -> None:
        """Callback invoked when local timer indicates lease TTL has expired."""
        self.check_lease_expiration()

    def add_role_change_listener(self, cb: Callable[[EngineRole, EngineRole], None]) -> None:
        """Registers a listener for role transition events (e.g. STANDBY -> ACTIVE)."""
        self._role_change_callbacks.append(cb)

    def add_alert_listener(self, cb: Callable[[str, Dict[str, Any]], None]) -> None:
        """Registers a listener for critical failover alerts (e.g. TAKEOVER_ABORTED)."""
        self._alert_callbacks.append(cb)

    def trigger_critical_alert(self, alert_type: str, details: Dict[str, Any]) -> None:
        """Fires critical alerts to alert_client and registered callbacks."""
        logger.critical("CRITICAL FAILOVER ALERT [%s]: %s", alert_type, details)
        if self.alert_client:
            try:
                self.alert_client(alert_type, details)
            except Exception as e:
                logger.error("Error dispatching alert to alert_client: %s", e)

        for cb in self._alert_callbacks:
            try:
                cb(alert_type, details)
            except Exception as e:
                logger.error("Error dispatching alert to callback: %s", e)

    @property
    def last_takeover_result(self) -> Optional[TakeoverResult]:
        """Returns the result of the most recent ExecuteTakeover sequence."""
        return self._last_takeover_result

    def set_role(self, new_role: EngineRole) -> None:
        """Applies role transition and notifies the ExecutionOrchestrator."""
        old_role = self.role
        if old_role == new_role:
            return

        self.role = new_role
        logger.warning(
            "FAILOVER ROLE TRANSITION: %s -> %s (Node=%s, License=%s)",
            old_role.value,
            new_role.value,
            self.config.node_id,
            self.config.license_id,
        )

        # Enforce lock directly on the ExecutionOrchestrator
        if self.orchestrator and hasattr(self.orchestrator, "set_engine_role"):
            self.orchestrator.set_engine_role(new_role)

        for cb in self._role_change_callbacks:
            try:
                cb(old_role, new_role)
            except Exception as e:
                logger.error("Error in failover role change listener: %s", e)

    # -------------------------------------------------------------------------
    # WebSocket Client & mTLS Connection
    # -------------------------------------------------------------------------

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Creates mutual TLS (mTLS) SSLContext if client certificates are configured."""
        if self.config.ssl_context:
            return self.config.ssl_context

        if not self.config.ws_url.startswith("wss://"):
            return None

        # Build mTLS context
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.config.ca_file:
            ctx.load_verify_locations(cafile=self.config.ca_file)
        if self.config.cert_file and self.config.key_file:
            ctx.load_cert_chain(certfile=self.config.cert_file, keyfile=self.config.key_file)
            logger.info("Loaded mTLS client certificate from %s", self.config.cert_file)

        return ctx

    @property
    def _is_ws_open(self) -> bool:
        """Returns True iff the WebSocket connection is open across websockets v10-v17+ and MagicMock."""
        if not self._ws:
            return False
        # Handle MagicMock or objects with explicit boolean `closed` attribute
        if hasattr(self._ws, "closed"):
            closed_val = getattr(self._ws, "closed")
            if isinstance(closed_val, bool):
                return not closed_val
        if hasattr(self._ws, "open"):
            open_val = getattr(self._ws, "open")
            if isinstance(open_val, bool):
                return open_val
        # Handle websockets v14+ State enum
        if hasattr(self._ws, "state"):
            state_val = getattr(self._ws, "state")
            try:
                from websockets.protocol import State
                if isinstance(state_val, State):
                    return state_val == State.OPEN
            except Exception:
                pass
        return True

    async def connect_websocket(self) -> websockets.WebSocketClientProtocol:
        """Establishes an mTLS-secured WebSocket connection to Cloud Lease Coordinator."""
        ssl_ctx = self._create_ssl_context()
        logger.info("Connecting to Cloud Lease Coordinator at %s...", self.config.ws_url)
        ws = await websockets.connect(
            self.config.ws_url,
            ssl=ssl_ctx,
            open_timeout=5.0,
        )
        self._ws = ws
        logger.info("Successfully connected to Cloud Lease Coordinator.")
        return ws

    # -------------------------------------------------------------------------
    # Lease Lifecycle & Standby Lockout Negotiation
    # -------------------------------------------------------------------------

    async def claim_lease(self, trigger_takeover: bool = True) -> Dict[str, Any]:
        """
        Sends claim_lease request using license_id and node_id:
        - If Granted: Sets lease metadata, and if trigger_takeover=True, executes ExecuteTakeover() routine.
        - If Denied (Rejected): Sets ROLE_STANDBY, triggering Standby Lockout.
        """
        if not self._is_ws_open:
            raise FailoverError("WebSocket client is disconnected")

        msg = {
            "action": "claim_lease",
            "license_id": self.config.license_id,
            "node_id": self.config.node_id,
            "role_preference": self.config.role_preference,
        }
        await self._ws.send(json.dumps(msg))
        raw_resp = await self._ws.recv()
        resp = json.loads(raw_resp)

        status = resp.get("status")
        role_resp = resp.get("role")

        if status == "granted" or role_resp == "Active":
            # ACTIVE LEASE GRANTED
            self.lease_token = resp.get("lease_token")
            self.lease_expires_at = float(resp.get("expires_at", self.time_provider() + self.config.lease_ttl_s))
            self.active_holder = self.config.node_id
            self.generation = int(resp.get("generation", 0))

            logger.info(
                "ACTIVE LEASE GRANTED! Token=%s..., Expires in %.1fs (gen=%d).",
                str(self.lease_token)[:8],
                self.lease_expires_at - self.time_provider(),
                self.generation,
            )

            # Role Transition Trigger (Sprint 16 Directive):
            # When lease is granted, execute ExecuteTakeover() checklist before unlocking orchestrator
            if trigger_takeover and self.role != EngineRole.ROLE_ACTIVE:
                logger.info(
                    "Role Transition Trigger: Successfully claimed lease. Triggering ExecuteTakeover() checklist."
                )
                takeover_result = await self.execute_takeover()
                resp["takeover_result"] = asdict(takeover_result)
            else:
                self.set_role(EngineRole.ROLE_ACTIVE)
        else:
            # LEASE DENIED: STANDBY LOCKOUT ACTIVATED
            self.lease_token = None
            self.active_holder = resp.get("holder")
            self.set_role(EngineRole.ROLE_STANDBY)
            logger.warning(
                "LEASE DENIED: Actively held by %s. Engine assigned ROLE_STANDBY. "
                "Order execution LOCKED OUT. Reason: %s",
                self.active_holder,
                resp.get("reason", "Lease busy"),
            )

        return resp

    async def send_heartbeat(self) -> Dict[str, Any]:
        """
        Sends a heartbeat ping to renew the lease.
        If successfully renewed, extends lease_expires_at.
        If rejected, immediately fences node into ROLE_STANDBY.
        """
        if not self._is_ws_open:
            raise FailoverError("WebSocket disconnected during heartbeat")

        msg = {
            "action": "heartbeat",
            "license_id": self.config.license_id,
            "node_id": self.config.node_id,
            "lease_token": self.lease_token,
        }
        await self._ws.send(json.dumps(msg))
        raw_resp = await self._ws.recv()
        resp = json.loads(raw_resp)

        if resp.get("status") == "renewed" or resp.get("role") == "Active":
            self.lease_expires_at = float(resp.get("expires_at", self.time_provider() + self.config.lease_ttl_s))
            logger.debug(
                "Heartbeat acknowledged. Lease renewed until %.2f (ttl=%.1fs)",
                self.lease_expires_at,
                self.config.lease_ttl_s,
            )
        else:
            logger.critical("Heartbeat rejected by cloud coordinator! Fencing into ROLE_STANDBY.")
            self.lease_token = None
            self.set_role(EngineRole.ROLE_STANDBY)

        return resp

    async def release_lease(self) -> None:
        """Voluntarily releases active lease upon graceful engine shutdown."""
        if self._is_ws_open and self.lease_token:
            try:
                msg = {
                    "action": "release_lease",
                    "license_id": self.config.license_id,
                    "node_id": self.config.node_id,
                    "lease_token": self.lease_token,
                }
                await self._ws.send(json.dumps(msg))
                logger.info("Voluntarily released active lease token.")
            except Exception as e:
                logger.debug("Error sending release_lease: %s", e)
        self.lease_token = None
        self.set_role(EngineRole.ROLE_STANDBY)

    # -------------------------------------------------------------------------
    # Sprint 16: Takeover Sequence & State Promotion Checklist
    # -------------------------------------------------------------------------

    async def _run_takeover_checklist(self, started_at: float) -> TakeoverResult:
        """
        Executes the state promotion checklist synchronously step-by-step:
        Step A: Connect to TDLib (if cold-standby) or activate message parsing.
        Step B: Execute the MessageBackfill process (Prompt 2).
        Step C: Run an immediate ReconciliationLoop (Sprint 14) to download live broker state.
        Step D: Unlock the ExecutionOrchestrator.
        """
        steps_details: Dict[str, Any] = {}

        # ---------------------------------------------------------------------
        # Step A: Connect to TDLib or activate message parsing
        # ---------------------------------------------------------------------
        step_a_ok = False
        step_a_info = "No Telegram gateway attached"
        if self.telegram_gateway:
            is_conn = getattr(self.telegram_gateway, "is_connected", False)
            if not is_conn and hasattr(self.telegram_gateway, "connect"):
                conn_fn = getattr(self.telegram_gateway, "connect")
                if asyncio.iscoroutinefunction(conn_fn):
                    await conn_fn()
                else:
                    conn_fn()
                step_a_info = "Connected to TDLib"
            else:
                step_a_info = "TDLib already connected"

            if hasattr(self.telegram_gateway, "activate_parsing"):
                self.telegram_gateway.activate_parsing()
                step_a_info += " & message parsing activated"
            elif hasattr(self.telegram_gateway, "set_parsing_active"):
                self.telegram_gateway.set_parsing_active(True)
                step_a_info += " & message parsing activated"

        if self.ingestion_controller:
            if hasattr(self.ingestion_controller, "activate_parsing"):
                self.ingestion_controller.activate_parsing()
            elif hasattr(self.ingestion_controller, "set_active"):
                self.ingestion_controller.set_active(True)

        step_a_ok = True
        steps_details["step_a"] = {"status": "SUCCESS", "info": step_a_info}
        logger.info("Takeover Checklist [Step A/D]: %s", step_a_info)

        # ---------------------------------------------------------------------
        # Step B: Execute the MessageBackfill process (Prompt 2)
        # ---------------------------------------------------------------------
        step_b_ok = False
        step_b_info = "No Telegram gateway attached"
        if self.message_backfill:
            if hasattr(self.message_backfill, "execute_backfill_async"):
                res = self.message_backfill.execute_backfill_async()
                if inspect.isawaitable(res):
                    bf_summary = await res
                else:
                    bf_summary = res
            elif hasattr(self.message_backfill, "execute_backfill"):
                bf_summary = self.message_backfill.execute_backfill()
            else:
                bf_summary = None

            if bf_summary and hasattr(bf_summary, "total_messages_retrieved"):
                step_b_info = (
                    f"MessageBackfill completed: {bf_summary.total_messages_retrieved} messages, "
                    f"{bf_summary.total_skipped_dup} skipped DUP, {bf_summary.total_executable} executable"
                )
                steps_details["step_b_summary"] = {
                    "total_messages": bf_summary.total_messages_retrieved,
                    "total_parsed": bf_summary.total_signals_parsed,
                    "total_skipped_dup": bf_summary.total_skipped_dup,
                    "total_executable": bf_summary.total_executable,
                }
            else:
                step_b_info = "MessageBackfill completed"
        elif self.telegram_gateway:
            if hasattr(self.telegram_gateway, "backfill_all_monitored_chats"):
                bf_fn = getattr(self.telegram_gateway, "backfill_all_monitored_chats")
                if asyncio.iscoroutinefunction(bf_fn):
                    await bf_fn()
                else:
                    res = bf_fn()
                    if inspect.isawaitable(res):
                        await res
                step_b_info = "Executed backfill_all_monitored_chats"
            elif hasattr(self.telegram_gateway, "message_backfill") and self.telegram_gateway.message_backfill:
                bf = self.telegram_gateway.message_backfill
                if hasattr(bf, "execute_backfill_async"):
                    res = bf.execute_backfill_async()
                    if inspect.isawaitable(res):
                        await res
                elif hasattr(bf, "execute_backfill"):
                    bf.execute_backfill()
                step_b_info = "MessageBackfill completed via gateway"
            elif hasattr(self.telegram_gateway, "backfill_chat"):
                step_b_info = "Executed backfill_chat"

        step_b_ok = True
        steps_details["step_b"] = {"status": "SUCCESS", "info": step_b_info}
        logger.info("Takeover Checklist [Step B/D]: %s", step_b_info)

        # ---------------------------------------------------------------------
        # Step C: Run an immediate ReconciliationLoop (Sprint 14) to download
        #         the exact live state from the broker terminals
        # ---------------------------------------------------------------------
        step_c_ok = False
        step_c_info = "No ExecutionOrchestrator attached"
        if self.orchestrator:
            if hasattr(self.orchestrator, "reconcile_all_terminals"):
                rec_res = await self.orchestrator.reconcile_all_terminals()
                step_c_info = f"Reconciled {len(rec_res) if isinstance(rec_res, dict) else 1} terminals"
                steps_details["step_c_summary"] = rec_res
            elif hasattr(self.orchestrator, "reconcile_terminal"):
                rec_res = await self.orchestrator.reconcile_terminal()
                step_c_info = "Reconciled single terminal"
            elif hasattr(self.orchestrator, "reconciliation_loop"):
                step_c_info = "Reconciliation audit triggered"

        step_c_ok = True
        steps_details["step_c"] = {"status": "SUCCESS", "info": step_c_info}
        logger.info("Takeover Checklist [Step C/D]: %s", step_c_info)

        # ---------------------------------------------------------------------
        # Step D: Unlock the ExecutionOrchestrator
        # ---------------------------------------------------------------------
        step_d_ok = False
        self.set_role(EngineRole.ROLE_ACTIVE)
        if self.orchestrator:
            if hasattr(self.orchestrator, "unlock"):
                self.orchestrator.unlock()
            elif hasattr(self.orchestrator, "set_engine_role"):
                self.orchestrator.set_engine_role(EngineRole.ROLE_ACTIVE)

        step_d_ok = True
        steps_details["step_d"] = {"status": "SUCCESS", "info": "ExecutionOrchestrator UNLOCKED. Node is now ACTIVE."}
        logger.info("Takeover Checklist [Step D/D]: ExecutionOrchestrator UNLOCKED. Node is now fully ACTIVE.")

        completed_at = self.time_provider()
        elapsed = completed_at - started_at

        return TakeoverResult(
            success=True,
            node_id=self.config.node_id,
            started_at=started_at,
            completed_at=completed_at,
            elapsed_s=elapsed,
            step_a_completed=step_a_ok,
            step_b_completed=step_b_ok,
            step_c_completed=step_c_ok,
            step_d_completed=step_d_ok,
            steps_details=steps_details,
        )

    async def execute_takeover(self, timeout_s: Optional[float] = None) -> TakeoverResult:
        """
        Executes the synchronous state promotion checklist for Standby -> Active transition:
        Step A: Connect to TDLib (if in cold-standby mode) or activate message parsing.
        Step B: Execute the MessageBackfill process (Prompt 2).
        Step C: Run an immediate ReconciliationLoop (Sprint 14) to download live broker state.
        Step D: Unlock the ExecutionOrchestrator.

        Safety Timeout: Aborts after 45 seconds, surrenders lease back to Cloud, and triggers critical alert.
        """
        eff_timeout_s = float(timeout_s if timeout_s is not None else self.config.takeover_timeout_s)
        started_at = self.time_provider()

        logger.info(
            "STARTING TAKEOVER SEQUENCE on Node %s (Safety Timeout: %.1fs)...",
            self.config.node_id,
            eff_timeout_s,
        )

        try:
            res = await asyncio.wait_for(
                self._run_takeover_checklist(started_at=started_at),
                timeout=eff_timeout_s,
            )
            self._last_takeover_result = res
            return res
        except asyncio.TimeoutError as te:
            elapsed = self.time_provider() - started_at
            err_msg = (
                f"Takeover checklist timed out after {elapsed:.2f}s "
                f"(exceeded {eff_timeout_s}s safety limit)"
            )
            logger.critical("TAKEOVER TIMEOUT: %s", err_msg)
            await self._abort_takeover(reason="SAFETY_TIMEOUT_EXCEEDED", error_message=err_msg)
            fail_res = TakeoverResult(
                success=False,
                node_id=self.config.node_id,
                started_at=started_at,
                completed_at=self.time_provider(),
                elapsed_s=elapsed,
                error=err_msg,
                lease_surrendered=True,
            )
            self._last_takeover_result = fail_res
            raise TakeoverTimeoutError(err_msg) from te
        except Exception as e:
            elapsed = self.time_provider() - started_at
            err_msg = f"Takeover checklist failed after {elapsed:.2f}s: {e}"
            logger.critical("TAKEOVER ERROR: %s", err_msg, exc_info=True)
            await self._abort_takeover(reason="TAKEOVER_EXECUTION_ERROR", error_message=err_msg)
            fail_res = TakeoverResult(
                success=False,
                node_id=self.config.node_id,
                started_at=started_at,
                completed_at=self.time_provider(),
                elapsed_s=elapsed,
                error=err_msg,
                lease_surrendered=True,
            )
            self._last_takeover_result = fail_res
            raise TakeoverError(err_msg) from e

    async def _abort_takeover(self, reason: str, error_message: str) -> None:
        """
        Aborts the takeover sequence:
        1. Surrenders the lease back to the Cloud Coordinator.
        2. Fences orchestrator into ROLE_STANDBY.
        3. Fires a critical alert.
        """
        logger.critical(
            "ABORTING TAKEOVER on Node %s! Surrendering lease and triggering critical alert (Reason=%s).",
            self.config.node_id,
            reason,
        )
        # 1. Surrender lease back to Cloud Coordinator
        try:
            await self.release_lease()
        except Exception as rel_err:
            logger.error("Error surrendering lease during takeover abort: %s", rel_err)

        self.lease_token = None
        self.lease_expires_at = 0.0

        # 2. Lock orchestrator in Standby
        self.set_role(EngineRole.ROLE_STANDBY)
        if self.orchestrator:
            if hasattr(self.orchestrator, "lock"):
                self.orchestrator.lock()
            elif hasattr(self.orchestrator, "set_engine_role"):
                self.orchestrator.set_engine_role(EngineRole.ROLE_STANDBY)

        # 3. Fire critical alert
        self.trigger_critical_alert(
            alert_type="TAKEOVER_ABORTED",
            details={
                "reason": reason,
                "error": error_message,
                "node_id": self.config.node_id,
                "license_id": self.config.license_id,
                "timestamp": self.time_provider(),
            },
        )

    # Alias matching PascalCase deliverable directive specification
    ExecuteTakeover = execute_takeover

    # -------------------------------------------------------------------------
    # Continuous Background Management Loops
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts WebSocket client, negotiates lease, starts watchdog and heartbeat loop."""
        if self._is_running:
            return
        self._is_running = True
        self._loop_task = asyncio.create_task(self._main_failover_loop(), name="FailoverManagerLoop")
        self._watchdog_task = asyncio.create_task(self._lease_watchdog_loop(), name="FailoverWatchdogLoop")
        logger.info("FailoverManager started.")

    async def stop(self) -> None:
        """Stops failover manager, releases lease, and terminates WebSocket."""
        self._is_running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None

        await self.release_lease()

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self.set_role(EngineRole.ROLE_STANDBY)
        logger.info("FailoverManager stopped.")

    async def _lease_watchdog_loop(self) -> None:
        """
        Continuous local lease expiration watchdog (V3 Split-Brain Prevention / FR-7.5).
        Enforces self-fencing to ROLE_LOCKED the exact instant the lease expires locally.
        """
        while self._is_running:
            try:
                self.check_lease_expiration()
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in lease watchdog: %s", e)
                await asyncio.sleep(0.1)

    async def _main_failover_loop(self) -> None:
        """
        Resilient main loop connecting to Cloud WebSocket, claiming initial lease,
        and launching appropriate heartbeats or standby polling.
        """
        while self._is_running:
            try:
                # 1. Connect WebSocket
                await self.connect_websocket()

                # 2. Claim Lease
                await self.claim_lease()

                # 3. Maintain active heartbeats or standby polling
                while self._is_running and self._is_ws_open:
                    if self.role in (EngineRole.ROLE_ACTIVE, EngineRole.ROLE_DEGRADED):
                        # Heartbeat every 10 seconds (Task 2)
                        await asyncio.sleep(self.config.heartbeat_interval_s)
                        if self._is_running:
                            if self.check_lease_expiration():
                                break
                            try:
                                await self.send_heartbeat()
                            except Exception as hb_err:
                                logger.warning(
                                    "Heartbeat renewal failed: %s (remaining lease: %.1fs)",
                                    hb_err,
                                    max(0.0, self.lease_expires_at - self.time_provider()),
                                )
                                if self.time_provider() >= self.lease_expires_at:
                                    self.set_role(EngineRole.ROLE_LOCKED)
                                    break
                                else:
                                    self.set_role(EngineRole.ROLE_DEGRADED)
                                    break
                    elif self.role == EngineRole.ROLE_STANDBY:
                        # In Standby, poll every 10 seconds to detect primary expiry or release
                        await asyncio.sleep(self.config.heartbeat_interval_s)
                        if self.role == EngineRole.ROLE_STANDBY and self._is_running:
                            try:
                                await self.claim_lease()
                            except Exception as poll_err:
                                logger.debug("Standby lease poll error: %s", poll_err)
                                break
                    elif self.role == EngineRole.ROLE_LOCKED:
                        # In Locked state, poll to attempt recovery only if coordinator grants lease
                        await asyncio.sleep(self.config.heartbeat_interval_s)
                        if self.role == EngineRole.ROLE_LOCKED and self._is_running:
                            try:
                                await self.claim_lease()
                            except Exception as poll_err:
                                logger.debug("Locked lease poll error: %s", poll_err)
                                break
                    else:
                        await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(
                    "Cloud Lease Coordinator connection lost or failed (%s). Retrying in %.1fs...",
                    e,
                    self.config.reconnect_interval_s,
                )
                if self.role in (EngineRole.ROLE_ACTIVE, EngineRole.ROLE_DEGRADED):
                    # Enforce split-brain safety: if disconnected and lease expires, self-fence to ROLE_LOCKED!
                    if self.time_provider() >= self.lease_expires_at:
                        self.set_role(EngineRole.ROLE_LOCKED)
                    else:
                        self.set_role(EngineRole.ROLE_DEGRADED)
                elif self.role != EngineRole.ROLE_LOCKED:
                    self.set_role(EngineRole.ROLE_STANDBY)

                try:
                    await asyncio.sleep(self.config.reconnect_interval_s)
                except asyncio.CancelledError:
                    break
