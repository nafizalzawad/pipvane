"""
Pipvane v3 Asynchronous Named Pipe Server (ADR-001).

Hosts full-duplex Named Pipe server on `\\\\.\\pipe\\pipvane_mt_{terminal_id}`.
Asynchronously processes the incoming state stream (equity, margin, free margin,
execution feedback, capabilities) without blocking the main event loop.
Provides robust reconnection, disconnect detection, and message wire framing.
"""

from __future__ import annotations

import sys
import asyncio
import logging
import binascii
from typing import Optional, Dict, Any, Callable, List

from engine.ipc_protocol import (
    NAMED_PIPE_HEADER_SIZE,
    NamedPipeOpcode,
    NamedPipeFrameHeader,
    AccountState,
    InitHelloPayload,
    ExecutionFeedback,
    PositionsSyncPayload,
    SymbolsSyncPayload,
    CapabilitiesPayload,
    pack_pipe_frame,
    unpack_pipe_frame_header,
)

logger = logging.getLogger("pipvane.engine.named_pipe_server")


class NamedPipeServerError(Exception):
    """Base exception for Named Pipe Server errors."""
    pass


class _MockTransport:
    """Mock asyncio transport for headless test simulation."""
    def __init__(self) -> None:
        self.sent_data: List[bytes] = []

    def write(self, data: bytes) -> None:
        self.sent_data.append(data)

    def close(self) -> None:
        pass


class _PipeServerProtocol(asyncio.Protocol):
    """Internal asyncio protocol handling binary framed stream over Named Pipe."""

    def __init__(self, server: AsyncNamedPipeServer) -> None:
        self.server = server
        self.transport: Optional[asyncio.BaseTransport] = None
        self._inbound_buffer = bytearray()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport
        self.server._on_client_connected(self, transport)

    def data_received(self, data: bytes) -> None:
        self._inbound_buffer.extend(data)
        self._process_frames()

    def _process_frames(self) -> None:
        """Parses all complete binary frames accumulated in buffer."""
        while len(self._inbound_buffer) >= NAMED_PIPE_HEADER_SIZE:
            header_slice = bytes(self._inbound_buffer[:NAMED_PIPE_HEADER_SIZE])
            try:
                hdr = unpack_pipe_frame_header(header_slice)
            except ValueError as e:
                logger.error("Corrupted frame header on terminal %s: %s. Discarding byte.", self.server.terminal_id, e)
                del self._inbound_buffer[0]
                continue

            total_frame_len = NAMED_PIPE_HEADER_SIZE + hdr.payload_len
            if len(self._inbound_buffer) < total_frame_len:
                # Incomplete frame; wait for remaining bytes
                break

            payload = bytes(self._inbound_buffer[NAMED_PIPE_HEADER_SIZE:total_frame_len])
            del self._inbound_buffer[:total_frame_len]

            # Verify CRC32 checksum if payload is present
            if hdr.payload_len > 0 and hdr.crc32 != 0:
                calc_crc = binascii.crc32(payload) & 0xFFFFFFFF
                if calc_crc != hdr.crc32:
                    logger.error(
                        "Payload CRC32 mismatch on opcode 0x%04X (expected 0x%08X, calculated 0x%08X)",
                        hdr.opcode, hdr.crc32, calc_crc
                    )
                    continue

            # Route message to server dispatcher
            self.server._dispatch_incoming_frame(self, hdr.opcode, payload)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.server._on_client_disconnected(self, exc)
        self.transport = None


class AsyncNamedPipeServer:
    """
    Asynchronous Windows Named Pipe Server listening for MT4/MT5 C++ DLL clients.
    """

    def __init__(
        self,
        terminal_id: str,
        pipe_name: Optional[str] = None,
    ) -> None:
        self.terminal_id = terminal_id
        self.pipe_name = pipe_name or fr"\\.\pipe\pipvane_mt_{terminal_id}"

        self._active_protocol: Optional[_PipeServerProtocol] = None
        self._active_transport: Optional[asyncio.BaseTransport] = None
        self._servers: List[Any] = []
        self._is_running = False

        # Event Callbacks
        self._on_connected_callbacks: List[Callable[[str], None]] = []
        self._on_disconnected_callbacks: List[Callable[[str, Optional[Exception]], None]] = []
        self._on_account_state_callbacks: List[Callable[[str, AccountState], None]] = []
        self._on_execution_feedback_callbacks: List[Callable[[str, ExecutionFeedback], None]] = []
        self._on_hello_callbacks: List[Callable[[str, InitHelloPayload], None]] = []
        self._on_positions_sync_callbacks: List[Callable[[str, PositionsSyncPayload], None]] = []
        self._on_symbols_sync_callbacks: List[Callable[[str, SymbolsSyncPayload], None]] = []
        self._on_capabilities_callbacks: List[Callable[[str, CapabilitiesPayload], None]] = []

        # Latest state cache
        self.last_account_state: Optional[AccountState] = None
        self.last_hello: Optional[InitHelloPayload] = None
        self.last_feedback: Optional[ExecutionFeedback] = None
        self.last_positions_sync: Optional[PositionsSyncPayload] = None
        self.last_symbols_sync: Optional[SymbolsSyncPayload] = None
        self.last_capabilities: Optional[CapabilitiesPayload] = None

    @property
    def is_connected(self) -> bool:
        """Returns True if a MetaTrader C++ DLL is actively connected to the pipe."""
        return self._active_transport is not None

    # --- Callbacks Registration ---

    def on_connected(self, callback: Callable[[str], None]) -> None:
        self._on_connected_callbacks.append(callback)

    def on_disconnected(self, callback: Callable[[str, Optional[Exception]], None]) -> None:
        self._on_disconnected_callbacks.append(callback)

    def on_account_state(self, callback: Callable[[str, AccountState], None]) -> None:
        self._on_account_state_callbacks.append(callback)

    def on_execution_feedback(self, callback: Callable[[str, ExecutionFeedback], None]) -> None:
        self._on_execution_feedback_callbacks.append(callback)

    def on_hello(self, callback: Callable[[str, InitHelloPayload], None]) -> None:
        self._on_hello_callbacks.append(callback)

    # --- Server Lifecycle ---

    async def start(self) -> None:
        """Starts the asynchronous Named Pipe listener."""
        if self._is_running:
            return

        loop = asyncio.get_running_loop()

        # On Windows with ProactorEventLoop, start native Named Pipe server
        if sys.platform == "win32" and hasattr(loop, "start_serving_pipe"):
            try:
                self._servers = await loop.start_serving_pipe(
                    lambda: _PipeServerProtocol(self),
                    self.pipe_name
                )
                self._is_running = True
                logger.info("Named Pipe Server listening on %s for terminal %s", self.pipe_name, self.terminal_id)
            except Exception as e:
                logger.error("Failed to start Named Pipe server on %s: %s", self.pipe_name, e)
                raise NamedPipeServerError(f"Could not bind Named Pipe '{self.pipe_name}': {e}") from e
        else:
            # Fallback mock mode for offline testing / non-Windows
            self._is_running = True
            logger.info("Named Pipe Server initialized in simulation mode for %s", self.terminal_id)

    async def stop(self) -> None:
        """Stops the Named Pipe server and closes all active transports."""
        if not self._is_running:
            return
        self._is_running = False

        if self._active_transport:
            try:
                self._active_transport.close()
            except Exception:
                pass
            self._active_transport = None
            self._active_protocol = None

        for s in self._servers:
            try:
                s.close()
            except Exception:
                pass
        self._servers.clear()
        logger.info("Stopped Named Pipe Server for terminal %s", self.terminal_id)

    # --- Outbound Transmission ---

    def send_frame(self, opcode: NamedPipeOpcode | int, payload: bytes = b"") -> bool:
        """
        Dispatches a binary framed message to the connected MetaTrader C++ DLL.
        Returns True if sent; False if client is not connected.
        """
        if not self._active_transport:
            return False

        frame_data = pack_pipe_frame(opcode, payload)
        try:
            if hasattr(self._active_transport, "write"):
                self._active_transport.write(frame_data)
                return True
        except Exception as e:
            logger.error("Failed to write to Named Pipe transport: %s", e)
        return False

    # --- Inbound Event Handlers ---

    def _on_client_connected(self, protocol: _PipeServerProtocol, transport: asyncio.BaseTransport) -> None:
        self._active_protocol = protocol
        self._active_transport = transport
        logger.info("MetaTrader DLL connected on pipe %s (terminal %s)", self.pipe_name, self.terminal_id)
        for cb in self._on_connected_callbacks:
            try:
                cb(self.terminal_id)
            except Exception as e:
                logger.error("Error in on_connected callback: %s", e)

    def _on_client_disconnected(self, protocol: _PipeServerProtocol, exc: Optional[Exception]) -> None:
        if self._active_protocol == protocol:
            self._active_protocol = None
            self._active_transport = None

        logger.warning(
            "MetaTrader DLL disconnected from pipe %s (terminal %s, reason: %s)",
            self.pipe_name, self.terminal_id, exc
        )
        for cb in self._on_disconnected_callbacks:
            try:
                cb(self.terminal_id, exc)
            except Exception as e:
                logger.error("Error in on_disconnected callback: %s", e)

    def _dispatch_incoming_frame(self, protocol: _PipeServerProtocol, opcode: int, payload: bytes) -> None:
        """Processes framed payload received from MetaTrader DLL."""
        if opcode == NamedPipeOpcode.MSG_INIT_HELLO:
            try:
                hello = InitHelloPayload.unpack(payload)
                self.last_hello = hello
                logger.info(
                    "Received MSG_INIT_HELLO from terminal %s (Account: %s, Build: %d, Server: %s)",
                    self.terminal_id, hello.account_login, hello.terminal_build, hello.broker_server
                )
                # Respond with capabilities request
                self.send_frame(NamedPipeOpcode.MSG_CAPABILITIES_REQ)
                for cb in self._on_hello_callbacks:
                    cb(self.terminal_id, hello)
            except Exception as e:
                logger.error("Error decoding MSG_INIT_HELLO: %s", e)

        elif opcode == NamedPipeOpcode.MSG_CAPABILITIES_RESP:
            try:
                capabilities = CapabilitiesPayload.unpack(payload)
                self.last_capabilities = capabilities
                logger.info(
                    "Capabilities on terminal %s: Hedging=%s, FIFO=%s, StopsLevel=%d, FreezeLevel=%d, Leverage=1:%d",
                    self.terminal_id, capabilities.supports_hedging, capabilities.is_fifo_enforced,
                    capabilities.stop_level_points, capabilities.freeze_level_points, capabilities.leverage
                )
                for cb in self._on_capabilities_callbacks:
                    cb(self.terminal_id, capabilities)
            except Exception as e:
                logger.error("Error decoding MSG_CAPABILITIES_RESP: %s", e)

        elif opcode == NamedPipeOpcode.MSG_ACCOUNT_STATE:
            try:
                state = AccountState.unpack(payload)
                self.last_account_state = state
                logger.debug(
                    "State update on terminal %s: Equity=%.2f, Margin=%.2f, Free=%.2f, Positions=%d",
                    self.terminal_id, state.equity, state.margin, state.free_margin, state.open_positions_count
                )
                for cb in self._on_account_state_callbacks:
                    cb(self.terminal_id, state)
            except Exception as e:
                logger.error("Error decoding MSG_ACCOUNT_STATE: %s", e)

        elif opcode == NamedPipeOpcode.MSG_EXECUTION_FEEDBACK:
            try:
                feedback = ExecutionFeedback.unpack(payload)
                self.last_feedback = feedback
                logger.debug(
                    "Execution feedback on terminal %s: Signal=%d, Ticket=%d, Status=%s, Latency=%d us",
                    self.terminal_id, feedback.signal_id, feedback.order_ticket,
                    feedback.execution_status.name, feedback.execution_latency_us
                )
                for cb in self._on_execution_feedback_callbacks:
                    cb(self.terminal_id, feedback)
            except Exception as e:
                logger.error("Error decoding MSG_EXECUTION_FEEDBACK: %s", e)

        elif opcode == NamedPipeOpcode.MSG_POSITIONS_SYNC:
            try:
                sync_payload = PositionsSyncPayload.unpack(payload)
                self.last_positions_sync = sync_payload
                logger.debug(
                    "Positions sync on terminal %s: Account=%d, Count=%d",
                    self.terminal_id, sync_payload.account_login, len(sync_payload.positions)
                )
                for cb in self._on_positions_sync_callbacks:
                    cb(self.terminal_id, sync_payload)
            except Exception as e:
                logger.error("Error decoding MSG_POSITIONS_SYNC: %s", e)

        elif opcode in (NamedPipeOpcode.MSG_SYMBOLS_RESP, NamedPipeOpcode.MSG_SYMBOLS_REQ):
            try:
                symbols_payload = SymbolsSyncPayload.unpack(payload)
                if not symbols_payload.terminal_id:
                    symbols_payload.terminal_id = self.terminal_id
                self.last_symbols_sync = symbols_payload
                logger.debug(
                    "Symbols sync on terminal %s: Count=%d",
                    self.terminal_id, len(symbols_payload.symbols)
                )
                for cb in self._on_symbols_sync_callbacks:
                    cb(self.terminal_id, symbols_payload)
            except Exception as e:
                logger.error("Error decoding MSG_SYMBOLS_RESP: %s", e)

        elif opcode == NamedPipeOpcode.MSG_HEARTBEAT_PING:
            # Echo pong response for latency monitoring
            self.send_frame(NamedPipeOpcode.MSG_HEARTBEAT_PONG, payload)

    def on_positions_sync(self, callback: Callable[[str, PositionsSyncPayload], None]) -> None:
        """Registers a callback for MSG_POSITIONS_SYNC telemetry."""
        self._on_positions_sync_callbacks.append(callback)

    def on_symbols_sync(self, callback: Callable[[str, SymbolsSyncPayload], None]) -> None:
        """Registers a callback for MSG_SYMBOLS_RESP telemetry."""
        self._on_symbols_sync_callbacks.append(callback)

    def on_capabilities(self, callback: Callable[[str, CapabilitiesPayload], None]) -> None:
        """Registers a callback for MSG_CAPABILITIES_RESP telemetry."""
        self._on_capabilities_callbacks.append(callback)

    # --- Test & Simulation Helper ---

    def simulate_inbound_data(self, data: bytes) -> None:
        """Injects raw wire bytes to test protocol parsing without physical pipe."""
        if self._active_transport is None:
            self._active_transport = _MockTransport()
        if self._active_protocol is None:
            self._active_protocol = _PipeServerProtocol(self)
            self._active_protocol.transport = self._active_transport
        self._active_protocol.data_received(data)
