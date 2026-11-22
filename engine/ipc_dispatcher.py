"""
Pipvane v3 Multi-Terminal IPC Dispatcher (ADR-001).

Orchestrates the Dual-Channel Hybrid IPC Architecture across multiple MetaTrader terminals:
1. Critical Execution Path: Writes serialized 128-byte OrderIntent structs directly
   into dedicated SPSC Shared Memory ring buffers (<50 µs latency).
2. Control & Telemetry Path: Hosts asynchronous Named Pipe servers to stream real-time
   account state (equity, margin), position sync snapshots, and execution confirmations.
3. Resilient Error Handling & Fault Isolation: Detects terminal crashes, pipe breaks,
   and heartbeat timeouts, immediately pausing routing to affected terminals while keeping
   healthy terminals fully operational.
"""

from __future__ import annotations

import time
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable

from engine.ipc_protocol import (
    OrderIntent,
    AccountState,
    InitHelloPayload,
    ExecutionFeedback,
    PositionsSyncPayload,
    SymbolsSyncPayload,
    CapabilitiesPayload,
    TerminalStatus,
    NamedPipeOpcode,
    PositionItem,
)
from engine.shm_writer import ShmRingBufferWriter
from engine.named_pipe_server import AsyncNamedPipeServer
from engine.ex4_integrity import (
    EX4IntegrityChecker,
    EX4IntegrityError,
    default_integrity_checker,
)

logger = logging.getLogger("pipvane.engine.ipc_dispatcher")


class IPCDispatcherError(Exception):
    """Base exception for IPCDispatcher errors."""
    pass


class TerminalNotRegisteredError(IPCDispatcherError):
    """Raised when an operation targets an unregistered terminal ID."""
    pass


@dataclass
class TerminalContext:
    """Encapsulates all IPC channels and operational telemetry for a single terminal."""
    terminal_id: str
    shm_writer: ShmRingBufferWriter
    pipe_server: AsyncNamedPipeServer
    status: TerminalStatus = TerminalStatus.DISCONNECTED
    last_heartbeat_time: float = 0.0
    account_state: Optional[AccountState] = None
    hello_info: Optional[InitHelloPayload] = None
    execution_history: List[ExecutionFeedback] = field(default_factory=list)
    capabilities: Optional[CapabilitiesPayload] = None
    ex4_path: Optional[str] = None
    expected_ex4_hash: Optional[str] = None


class IPCDispatcher:
    """
    Central Engine IPC Dispatcher managing up to 7 concurrent MetaTrader terminals.
    Provides sub-microsecond OrderIntent dispatch and non-blocking state ingestion.
    """

    def __init__(
        self,
        heartbeat_timeout_s: float = 5.0,
        watchdog_interval_s: float = 1.0,
        use_mock_buffers: bool = False,
        integrity_checker: Optional[EX4IntegrityChecker] = None,
    ) -> None:
        self.heartbeat_timeout_s = float(heartbeat_timeout_s)
        self.watchdog_interval_s = float(watchdog_interval_s)
        self.use_mock_buffers = use_mock_buffers
        self.integrity_checker = integrity_checker or default_integrity_checker

        self._terminals: Dict[str, TerminalContext] = {}
        self._is_running = False
        self._watchdog_task: Optional[asyncio.Task] = None

        # Global Dispatcher Event Callbacks
        self._on_state_update_callbacks: List[Callable[[str, AccountState], None]] = []
        self._on_feedback_callbacks: List[Callable[[str, ExecutionFeedback], None]] = []
        self._on_status_change_callbacks: List[Callable[[str, TerminalStatus, str], None]] = []
        self._on_positions_sync_callbacks: List[Callable[[str, PositionsSyncPayload], None]] = []
        self._on_symbols_sync_callbacks: List[Callable[[str, SymbolsSyncPayload], None]] = []
        self._on_capabilities_callbacks: List[Callable[[str, CapabilitiesPayload], None]] = []
        self._pending_positions_req: Dict[str, asyncio.Future[PositionsSyncPayload]] = {}
        self._pending_symbols_req: Dict[str, asyncio.Future[SymbolsSyncPayload]] = {}
        self._cached_positions: Dict[str, List[PositionItem]] = {}
        self._cached_symbols: Dict[str, List[str]] = {}

    # --- Terminal Registration & Lifecycle ---

    def register_terminal(
        self,
        terminal_id: str,
        shm_name: Optional[str] = None,
        pipe_name: Optional[str] = None,
        use_mock: Optional[bool] = None,
        ex4_path: Optional[str] = None,
        expected_ex4_hash: Optional[str] = None,
    ) -> TerminalContext:
        """
        Initializes dedicated SHM writer and Named Pipe server for a terminal.
        If ex4_path is provided, performs cryptographic runtime SHA-256 integrity check (FR-6.3 & FR-6.4).
        """
        if terminal_id in self._terminals:
            logger.warning("Terminal %s is already registered. Returning existing context.", terminal_id)
            return self._terminals[terminal_id]

        mock_flag = self.use_mock_buffers if use_mock is None else use_mock

        # Pre-Execution Disk Check (FR-6.3 & FR-6.4)
        if ex4_path:
            is_valid, actual_hash, reason = self.integrity_checker.verify_file(
                ex4_path, expected_ex4_hash
            )
            if not is_valid:
                logger.critical(
                    "CRITICAL SECURITY ALERT: Rejecting terminal %s registration! "
                    "EX4 integrity violation: %s",
                    terminal_id, reason
                )
                raise EX4IntegrityError(f"EX4 Integrity Check Failed: {reason}")

        shm_writer = ShmRingBufferWriter(
            terminal_id=terminal_id,
            shm_name=shm_name,
            use_mock_buffer=mock_flag,
        )

        pipe_server = AsyncNamedPipeServer(
            terminal_id=terminal_id,
            pipe_name=pipe_name,
        )

        ctx = TerminalContext(
            terminal_id=terminal_id,
            shm_writer=shm_writer,
            pipe_server=pipe_server,
            status=TerminalStatus.DISCONNECTED,
            last_heartbeat_time=time.time(),
            ex4_path=ex4_path,
            expected_ex4_hash=expected_ex4_hash,
        )

        # Wire internal pipe server callbacks to dispatcher handlers
        pipe_server.on_connected(lambda tid: self._handle_terminal_connected(tid))
        pipe_server.on_disconnected(lambda tid, exc: self._handle_terminal_disconnected(tid, exc))
        pipe_server.on_hello(lambda tid, hello: self._handle_terminal_hello(tid, hello))
        pipe_server.on_account_state(lambda tid, state: self._handle_terminal_account_state(tid, state))
        pipe_server.on_execution_feedback(lambda tid, fb: self._handle_terminal_feedback(tid, fb))
        pipe_server.on_positions_sync(lambda tid, sync: self._handle_terminal_positions_sync(tid, sync))
        pipe_server.on_symbols_sync(lambda tid, sync: self._handle_terminal_symbols_sync(tid, sync))
        pipe_server.on_capabilities(lambda tid, caps: self._handle_terminal_capabilities(tid, caps))

        self._terminals[terminal_id] = ctx
        logger.info("Registered terminal %s with IPCDispatcher", terminal_id)
        return ctx

    def unregister_terminal(self, terminal_id: str) -> None:
        """Stops and cleans up resources for a specific terminal."""
        ctx = self._terminals.pop(terminal_id, None)
        if not ctx:
            return

        ctx.shm_writer.close()
        # Schedule pipe server close
        if ctx.pipe_server.is_connected:
            ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_SHUTDOWN)

        logger.info("Unregistered terminal %s from IPCDispatcher", terminal_id)

    async def start(self) -> None:
        """Starts all registered terminal Named Pipe servers and watchdog."""
        if self._is_running:
            return
        self._is_running = True

        for tid, ctx in self._terminals.items():
            await ctx.pipe_server.start()

        self._watchdog_task = asyncio.create_task(self._heartbeat_watchdog_loop())
        logger.info("IPCDispatcher started across %d terminals", len(self._terminals))

    async def stop(self) -> None:
        """Gracefully shuts down all terminal servers and SHM writers."""
        if not self._is_running:
            return
        self._is_running = False

        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass

        for tid, ctx in list(self._terminals.items()):
            ctx.shm_writer.close()
            await ctx.pipe_server.stop()

        logger.info("IPCDispatcher stopped successfully")

    # --- Critical Path: Fast OrderIntent Dispatch ---

    def can_route_to_terminal(self, terminal_id: str) -> bool:
        """
        Verifies if terminal is eligible to receive OrderIntent execution:
        1. Terminal is registered.
        2. Status is CONNECTED.
        3. SHM ring buffer is not paused.
        4. MetaTrader is connected to broker trade server.
        5. Heartbeat is fresh.
        """
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return False

        if ctx.status != TerminalStatus.CONNECTED:
            return False

        if ctx.shm_writer.is_paused():
            return False

        # If we have account state, ensure broker connectivity
        if ctx.account_state is not None and not ctx.account_state.is_connected_to_broker:
            return False

        # Verify heartbeat freshness
        if (time.time() - ctx.last_heartbeat_time) > self.heartbeat_timeout_s:
            return False

        return True

    def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        """
        Serializes and writes OrderIntent into terminal's Shared Memory ring buffer.
        Returns True on successful dispatch; False if terminal is paused, disconnected, or saturated.
        """
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            logger.error("Cannot dispatch intent: Terminal %s is not registered", terminal_id)
            return False

        # Routing availability guard
        if not self.can_route_to_terminal(terminal_id):
            logger.warning(
                "Routing paused/unavailable for terminal %s (status=%s, broker_connected=%s). Intent dropped.",
                terminal_id, ctx.status.value,
                ctx.account_state.is_connected_to_broker if ctx.account_state else "UNKNOWN"
            )
            return False

        # Push to SPSC Shared Memory ring buffer (<50 µs zero-copy operation)
        success = ctx.shm_writer.push_order_intent(intent)
        if success:
            logger.debug(
                "Dispatched OrderIntent to terminal %s (Symbol=%s, Side=%s, Vol=%.2f, SignalId=%d)",
                terminal_id, intent.symbol, intent.side.name, intent.volume, intent.signal_id
            )
        else:
            logger.error("Failed to push OrderIntent to terminal %s: Ring buffer queue full!", terminal_id)

        return success

    # --- Routing Control & Graceful Pause ---

    def pause_terminal(self, terminal_id: str, reason: str = "Manual Pause") -> None:
        """Pauses order routing to a specific terminal."""
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            raise TerminalNotRegisteredError(f"Terminal {terminal_id} is not registered")

        ctx.status = TerminalStatus.PAUSED
        ctx.shm_writer.pause()
        logger.warning("Paused terminal %s. Reason: %s", terminal_id, reason)
        self._notify_status_change(terminal_id, TerminalStatus.PAUSED, reason)

    def resume_terminal(self, terminal_id: str) -> None:
        """Resumes order routing to a connected terminal."""
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            raise TerminalNotRegisteredError(f"Terminal {terminal_id} is not registered")

        ctx.shm_writer.resume()
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time()
        logger.info("Resumed terminal %s routing", terminal_id)
        self._notify_status_change(terminal_id, TerminalStatus.CONNECTED, "Resumed")

    # --- Inbound State & Telemetry Handlers ---

    def _handle_terminal_connected(self, terminal_id: str) -> None:
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return
        ctx.status = TerminalStatus.CONNECTING
        ctx.last_heartbeat_time = time.time()
        logger.info("Terminal %s connected. Awaiting INIT_HELLO handshake...", terminal_id)

    def _handle_terminal_hello(self, terminal_id: str, hello: InitHelloPayload) -> None:
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return

        # Runtime EX4 Integrity Check if ex4_path is configured (FR-6.3 & FR-6.4)
        if ctx.ex4_path:
            is_valid, actual_hash, reason = self.integrity_checker.verify_file(
                ctx.ex4_path, ctx.expected_ex4_hash
            )
            if not is_valid:
                logger.critical(
                    "CRITICAL SECURITY ALERT: [SECURITY_TAMPER_DETECTED] "
                    "Terminal %s connection rejected! EX4 binary integrity violation: %s. "
                    "Refusing to connect IPC bridge to terminal.",
                    terminal_id, reason
                )
                ctx.status = TerminalStatus.SECURITY_BLOCKED
                ctx.shm_writer.pause()
                if ctx.pipe_server.is_connected:
                    ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_SHUTDOWN)
                self._notify_status_change(terminal_id, TerminalStatus.SECURITY_BLOCKED, "EX4 Integrity Violation")
                return

        ctx.hello_info = hello
        ctx.status = TerminalStatus.CONNECTED
        ctx.shm_writer.resume()
        ctx.last_heartbeat_time = time.time()

        # Query tradable symbols dump upon initialization handshake
        if ctx.pipe_server.is_connected:
            ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_SYMBOLS_REQ)

        logger.info(
            "Terminal %s handshake complete. Status=CONNECTED (Account: %s, Server: %s, Platform: %s)",
            terminal_id, hello.account_login, hello.broker_server,
            "MT4" if hello.platform_type == 0 else "MT5"
        )
        self._notify_status_change(terminal_id, TerminalStatus.CONNECTED, "Handshake Established")

    def _handle_terminal_account_state(self, terminal_id: str, state: AccountState) -> None:
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return

        ctx.account_state = state
        ctx.last_heartbeat_time = time.time()

        # If terminal was paused due to heartbeat timeout, automatically recover on state receipt
        if ctx.status == TerminalStatus.PAUSED:
            self.resume_terminal(terminal_id)

        # Notify registered callbacks without blocking event loop
        for cb in self._on_state_update_callbacks:
            try:
                cb(terminal_id, state)
            except Exception as e:
                logger.error("Error in on_state_update callback: %s", e)

    def _handle_terminal_feedback(self, terminal_id: str, feedback: ExecutionFeedback) -> None:
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return

        ctx.execution_history.append(feedback)
        ctx.last_heartbeat_time = time.time()

        for cb in self._on_feedback_callbacks:
            try:
                cb(terminal_id, feedback)
            except Exception as e:
                logger.error("Error in on_feedback callback: %s", e)

    def _handle_terminal_positions_sync(self, terminal_id: str, payload: PositionsSyncPayload) -> None:
        ctx = self._terminals.get(terminal_id)
        if ctx:
            ctx.last_heartbeat_time = time.time()

        # Cache latest positions snapshot
        self._cached_positions[terminal_id] = payload.positions

        # Resolve any pending fetch_positions_dump future
        fut = self._pending_positions_req.pop(terminal_id, None)
        if fut and not fut.done():
            fut.set_result(payload)

        for cb in self._on_positions_sync_callbacks:
            try:
                cb(terminal_id, payload)
            except Exception as e:
                logger.error("Error in on_positions_sync callback: %s", e)

    def get_broker_positions(self, terminal_id: Optional[str] = None) -> List[PositionItem]:
        """Returns latest cached broker positions for specified terminal or all terminals."""
        if terminal_id:
            return list(self._cached_positions.get(terminal_id, []))
        all_pos: List[PositionItem] = []
        for p_list in self._cached_positions.values():
            all_pos.extend(p_list)
        return all_pos

    def request_positions_sync(self, terminal_id: str) -> bool:
        """Sends MSG_POSITIONS_SYNC request frame over Named Pipe to terminal."""
        ctx = self._terminals.get(terminal_id)
        if not ctx or not ctx.pipe_server.is_connected:
            return False
        return ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_POSITIONS_SYNC)

    async def fetch_positions_dump(
        self, terminal_id: str, timeout_s: float = 3.0
    ) -> Optional[PositionsSyncPayload]:
        """
        Sends MSG_POSITIONS_SYNC request over Named Pipe and awaits position snapshot response.
        If timeout occurs or pipe is disconnected, returns cached snapshot if available or None.
        """
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return None

        if not ctx.pipe_server.is_connected:
            return getattr(ctx.pipe_server, "last_positions_sync", None)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[PositionsSyncPayload] = loop.create_future()
        self._pending_positions_req[terminal_id] = fut

        sent = ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_POSITIONS_SYNC)
        if not sent:
            self._pending_positions_req.pop(terminal_id, None)
            return getattr(ctx.pipe_server, "last_positions_sync", None)

        try:
            return await asyncio.wait_for(fut, timeout=timeout_s)
        except asyncio.TimeoutError:
            self._pending_positions_req.pop(terminal_id, None)
            logger.warning("Timeout waiting for MSG_POSITIONS_SYNC from terminal %s", terminal_id)
            return getattr(ctx.pipe_server, "last_positions_sync", None)

    def _handle_terminal_symbols_sync(self, terminal_id: str, payload: SymbolsSyncPayload) -> None:
        ctx = self._terminals.get(terminal_id)
        if ctx:
            ctx.last_heartbeat_time = time.time()

        self._cached_symbols[terminal_id] = list(payload.symbols)

        fut = self._pending_symbols_req.pop(terminal_id, None)
        if fut and not fut.done():
            fut.set_result(payload)

        for cb in self._on_symbols_sync_callbacks:
            try:
                cb(terminal_id, payload)
            except Exception as e:
                logger.error("Error in on_symbols_sync callback: %s", e)

    def _handle_terminal_capabilities(self, terminal_id: str, caps: CapabilitiesPayload) -> None:
        """Processes MSG_CAPABILITIES_RESP telemetry streamed over Named Pipe."""
        ctx = self._terminals.get(terminal_id)
        if ctx:
            ctx.last_heartbeat_time = time.time()
            ctx.capabilities = caps
        for cb in self._on_capabilities_callbacks:
            try:
                cb(terminal_id, caps)
            except Exception as e:
                logger.error("Error in on_capabilities callback: %s", e)

    def get_terminal_capabilities(self, terminal_id: str) -> Optional[CapabilitiesPayload]:
        """Retrieves negotiated broker capabilities for a terminal if available."""
        ctx = self._terminals.get(terminal_id)
        return ctx.capabilities if ctx else None

    def get_stops_level(self, terminal_id: str, symbol: Optional[str] = None) -> int:
        """
        Sprint 20: Reads SYMBOL_TRADE_STOPS_LEVEL in points from terminal via IPC.
        """
        caps = self.get_terminal_capabilities(terminal_id)
        return caps.stop_level_points if caps else 0

    def get_freeze_level(self, terminal_id: str, symbol: Optional[str] = None) -> int:
        """
        Sprint 20: Reads SYMBOL_TRADE_FREEZE_LEVEL in points from terminal via IPC.
        """
        caps = self.get_terminal_capabilities(terminal_id)
        return caps.freeze_level_points if caps else 0

    def get_broker_symbols(self, terminal_id: Optional[str] = None) -> List[str]:
        """Returns latest cached broker symbols for specified terminal or all terminals."""
        if terminal_id:
            return list(self._cached_symbols.get(terminal_id, []))
        all_syms: List[str] = []
        for s_list in self._cached_symbols.values():
            for s in s_list:
                if s not in all_syms:
                    all_syms.append(s)
        return all_syms

    def request_symbols_sync(self, terminal_id: str) -> bool:
        """Sends MSG_SYMBOLS_REQ frame over Named Pipe to terminal."""
        ctx = self._terminals.get(terminal_id)
        if not ctx or not ctx.pipe_server.is_connected:
            return False
        return ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_SYMBOLS_REQ)

    async def fetch_symbols_dump(
        self, terminal_id: str, timeout_s: float = 3.0
    ) -> Optional[SymbolsSyncPayload]:
        """
        Sends MSG_SYMBOLS_REQ over Named Pipe and awaits symbols snapshot response.
        If timeout occurs or pipe is disconnected, returns cached snapshot if available or None.
        """
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return None

        if not ctx.pipe_server.is_connected:
            last_sync = getattr(ctx.pipe_server, "last_symbols_sync", None)
            if last_sync:
                return last_sync
            cached = self._cached_symbols.get(terminal_id)
            if cached is not None:
                return SymbolsSyncPayload(terminal_id=terminal_id, symbols=cached)
            return None

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[SymbolsSyncPayload] = loop.create_future()
        self._pending_symbols_req[terminal_id] = fut

        sent = ctx.pipe_server.send_frame(NamedPipeOpcode.MSG_SYMBOLS_REQ)
        if not sent:
            self._pending_symbols_req.pop(terminal_id, None)
            return getattr(ctx.pipe_server, "last_symbols_sync", None)

        try:
            return await asyncio.wait_for(fut, timeout=timeout_s)
        except asyncio.TimeoutError:
            self._pending_symbols_req.pop(terminal_id, None)
            logger.warning("Timeout waiting for MSG_SYMBOLS_RESP from terminal %s", terminal_id)
            return getattr(ctx.pipe_server, "last_symbols_sync", None)

    def _handle_terminal_disconnected(self, terminal_id: str, exc: Optional[Exception]) -> None:
        """
        Invoked when the Named Pipe breaks (terminal crash, process kill, or closed socket).
        Gracefully pauses routing to the affected terminal.
        """
        ctx = self._terminals.get(terminal_id)
        if not ctx:
            return

        reason = f"Pipe broken / disconnect: {exc}" if exc else "Terminal disconnected"
        logger.warning(
            "CRITICAL: Terminal %s disconnected from Named Pipe. Gracefully pausing routing. (%s)",
            terminal_id, reason
        )

        ctx.status = TerminalStatus.DISCONNECTED
        ctx.shm_writer.pause()
        self._notify_status_change(terminal_id, TerminalStatus.DISCONNECTED, reason)

    # --- Heartbeat Watchdog ---

    async def _heartbeat_watchdog_loop(self) -> None:
        """Background monitor detecting terminal freezes or silent disconnects."""
        while self._is_running:
            try:
                await asyncio.sleep(self.watchdog_interval_s)
                now = time.time()

                for tid, ctx in list(self._terminals.items()):
                    if ctx.status == TerminalStatus.CONNECTED:
                        elapsed = now - ctx.last_heartbeat_time
                        if elapsed > self.heartbeat_timeout_s:
                            logger.warning(
                                "Terminal %s heartbeat timed out (%.2fs > %.2fs). Automatically pausing routing.",
                                tid, elapsed, self.heartbeat_timeout_s
                            )
                            self.pause_terminal(tid, reason=f"Heartbeat timeout ({elapsed:.1f}s)")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in heartbeat watchdog: %s", e)

    # --- Public Accessors & Callbacks ---

    def get_terminal_state(self, terminal_id: str) -> Optional[AccountState]:
        """Returns the latest AccountState (equity, margin) for a terminal."""
        ctx = self._terminals.get(terminal_id)
        return ctx.account_state if ctx else None

    # Defensive alias
    get_latest_state = get_terminal_state

    def get_terminal_status(self, terminal_id: str) -> TerminalStatus:
        """Returns the current operational status of a terminal."""
        ctx = self._terminals.get(terminal_id)
        return ctx.status if ctx else TerminalStatus.DISCONNECTED

    def get_registered_terminals(self) -> List[str]:
        """Returns list of all registered terminal identifiers."""
        return list(self._terminals.keys())

    def get_active_terminals(self) -> List[str]:
        """Returns list of all active and routable terminal identifiers."""
        return [tid for tid in self._terminals if self.can_route_to_terminal(tid)]

    def on_account_state(self, callback: Callable[[str, AccountState], None]) -> None:
        self._on_state_update_callbacks.append(callback)

    def on_execution_feedback(self, callback: Callable[[str, ExecutionFeedback], None]) -> None:
        self._on_feedback_callbacks.append(callback)

    def on_positions_sync(self, callback: Callable[[str, PositionsSyncPayload], None]) -> None:
        self._on_positions_sync_callbacks.append(callback)

    def on_symbols_sync(self, callback: Callable[[str, SymbolsSyncPayload], None]) -> None:
        self._on_symbols_sync_callbacks.append(callback)

    def on_capabilities(self, callback: Callable[[str, CapabilitiesPayload], None]) -> None:
        self._on_capabilities_callbacks.append(callback)

    def on_status_change(self, callback: Callable[[str, TerminalStatus, str], None]) -> None:
        self._on_status_change_callbacks.append(callback)

    def _notify_status_change(self, terminal_id: str, status: TerminalStatus, reason: str) -> None:
        for cb in self._on_status_change_callbacks:
            try:
                cb(terminal_id, status, reason)
            except Exception as e:
                logger.error("Error in status change callback: %s", e)
