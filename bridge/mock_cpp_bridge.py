"""
Pipvane v3 Mock C++ MetaTrader Bridge Executable Simulator.

Simulates the MetaTrader 4 / MetaTrader 5 C++ Bridge DLL (ADR-001):
1. Connects to the Engine's Windows Named Pipe server (\\\\.\\pipe\\pipvane_mt_{terminal_id}).
2. Performs the MSG_INIT_HELLO and MSG_ACCOUNT_STATE handshake.
3. Maps into the Windows Shared Memory segment (Local\\pipvane_shm_{terminal_id}).
4. Consumes 128-byte OrderIntent structs from the lock-free SPSC circular queue
   using a hybrid spin-wait with Win32 Event fallback (WaitForSingleObject).
5. Dispatches execution feedback frames (MSG_EXECUTION_FEEDBACK, opcode 0x0020)
   with sub-millisecond round-trip acknowledgments over the Named Pipe.
"""

from __future__ import annotations

import os
import sys
import time
import mmap
import struct
import argparse
import logging
import threading
from typing import Optional

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_protocol import (  # noqa: E402
    TOTAL_SLOTS,
    SLOT_SIZE,
    HEADER_SIZE,
    TOTAL_SHM_SIZE,
    NamedPipeOpcode,
    ExecutionStatus,
    ActionType,
    PositionItem,
    PositionsSyncPayload,
    InitHelloPayload,
    AccountState,
    ExecutionFeedback,
    pack_pipe_frame,
    deserialize_order_intent,
)

logger = logging.getLogger("pipvane.mock_cpp_bridge")

# Win32 Kernel32 constants
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
EVENT_ALL_ACCESS = 0x1F0003

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes
    _kernel32 = ctypes.windll.kernel32
else:
    _kernel32 = None  # type: ignore


class MockCppBridgeWorker:
    """
    Simulates the native C++ MetaTrader DLL bridge client.
    """

    def __init__(
        self,
        terminal_id: str = "mock_terminal_0",
        pipe_name: Optional[str] = None,
        shm_name: Optional[str] = None,
        event_name: Optional[str] = None,
        account_login: int = 12345678,
        broker_server: str = "Pipvane-Demo01",
        spin_cycles: int = 100,
        quiet: bool = False,
        log_file: Optional[str] = None,
        platform_type: int = 1,
    ) -> None:
        self.terminal_id = terminal_id
        self.pipe_name = pipe_name or fr"\\.\pipe\pipvane_mt_{terminal_id}"
        self.shm_name = shm_name or fr"Local\pipvane_shm_{terminal_id}"
        self.event_name = event_name or fr"Local\pipvane_event_{terminal_id}"
        self.account_login = account_login
        self.broker_server = broker_server
        self.spin_cycles = spin_cycles
        self.quiet = quiet
        self.log_file = log_file
        self.platform_type = platform_type

        self._pipe_handle: int = -1
        self._event_handle: Optional[int] = None
        self._shm_map: Optional[mmap.mmap] = None
        self._is_running: bool = False

        # Live broker state array of active positions (Sprint 14)
        self.broker_positions: Dict[int, PositionItem] = {}

    def connect_pipe(self, timeout_s: float = 5.0) -> bool:
        """Connects to the Engine's duplex Named Pipe server."""
        if sys.platform != "win32" or _kernel32 is None:
            raise RuntimeError("MockCppBridge requires Windows OS for Named Pipe / SHM primitives")

        start = time.time()
        while time.time() - start < timeout_s:
            handle = _kernel32.CreateFileW(
                self.pipe_name,
                GENERIC_READ | GENERIC_WRITE,
                0,
                None,
                OPEN_EXISTING,
                0,
                None,
            )
            if handle != -1 and handle != 0:
                self._pipe_handle = handle
                if not self.quiet:
                    logger.info("Connected to Named Pipe: %s", self.pipe_name)
                return True
            time.sleep(0.02)

        logger.error("Timed out connecting to Named Pipe: %s", self.pipe_name)
        return False

    def send_init_handshake(self) -> None:
        """Sends MSG_INIT_HELLO and MSG_ACCOUNT_STATE over Named Pipe."""
        hello = InitHelloPayload(
            terminal_id=self.terminal_id,
            platform_type=self.platform_type,  # 0: MT4, 1: MT5
            is_portable=False,
            terminal_build=1420 if self.platform_type == 0 else 4150,
            dll_bridge_version=0x03000001,
            account_login=self.account_login,
            broker_server=self.broker_server or ("Pipvane-MT4Demo" if self.platform_type == 0 else "Pipvane-Demo01"),
            company_name="Pipvane Testing Lab",
            shm_segment_name=self.shm_name,
            shm_capacity=TOTAL_SLOTS,
        )
        frame_hello = pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack(), seq_id=1)
        self._write_pipe(frame_hello)

        account = AccountState(
            account_login=self.account_login,
            balance=100000.0,
            equity=100000.0,
            margin=0.0,
            free_margin=100000.0,
            margin_level=0.0,
            open_positions_count=0,
            pending_orders_count=0,
            is_connected_to_broker=True,
        )
        frame_acc = pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, account.pack(), seq_id=2)
        self._write_pipe(frame_acc)

    def attach_shm(self) -> None:
        """Opens shared memory segment and notification event."""
        self._shm_map = mmap.mmap(-1, TOTAL_SHM_SIZE, tagname=self.shm_name)
        if _kernel32 is not None:
            self._event_handle = _kernel32.OpenEventW(EVENT_ALL_ACCESS, False, self.event_name)

    def _write_pipe(self, data: bytes) -> None:
        """Writes raw framed bytes to Named Pipe handle."""
        if self._pipe_handle == -1 or _kernel32 is None:
            return
        written = wintypes.DWORD()
        _kernel32.WriteFile(
            self._pipe_handle,
            data,
            len(data),
            ctypes.byref(written),
            None,
        )

    def run_consumer_loop(
        self,
        target_iterations: int = 0,
        stop_event: Optional[threading.Event] = None,
    ) -> int:
        """
        Consumes OrderIntent objects and replies with MSG_EXECUTION_FEEDBACK.
        If target_iterations > 0, halts after that count.
        If stop_event is provided, halts when stop_event is set.
        Returns the number of processed intents.
        """
        if self._shm_map is None:
            raise RuntimeError("Shared memory is not attached")

        shm = self._shm_map
        h_event = self._event_handle
        processed = 0
        seq_id = 10

        while (stop_event is None or not stop_event.is_set()) and (target_iterations <= 0 or processed < target_iterations):
            # SPSC Lock-Free Read: read_tail at offset 128, write_head at offset 64
            tail = struct.unpack_from("<Q", shm, 128)[0]
            head = struct.unpack_from("<Q", shm, 64)[0]

            if tail >= head:
                # Hybrid latency strategy: spin brief, then wait for Win32 notification event
                spin = 0
                while spin < self.spin_cycles and tail >= head:
                    spin += 1
                    head = struct.unpack_from("<Q", shm, 64)[0]

                if tail >= head:
                    if h_event and _kernel32 is not None:
                        _kernel32.WaitForSingleObject(h_event, 5)
                    else:
                        time.sleep(0.001)
                    continue

            # Queue has an intent ready
            slot_idx = tail & (TOTAL_SLOTS - 1)
            slot_offset = HEADER_SIZE + (slot_idx * SLOT_SIZE)

            try:
                intent = deserialize_order_intent(shm, slot_offset)
            except Exception as e:
                logger.error("Error deserializing OrderIntent slot %d: %s", slot_idx, e)
                # Advance tail to prevent permanent stall
                struct.pack_into("<Q", shm, 128, tail + 1)
                continue

            # Advance consumer read_tail at offset 128
            struct.pack_into("<Q", shm, 128, tail + 1)

            # Update consumer telemetry
            cons_count = struct.unpack_from("<Q", shm, 136)[0]
            struct.pack_into("<Q", shm, 136, cons_count + 1)
            struct.pack_into("<Q", shm, 200, intent.signal_id)

            now_ns = time.time_ns()
            transit_us = (now_ns - intent.timestamp_ns) // 1000 if now_ns > intent.timestamp_ns else 0
            side_str = "BUY" if intent.side == 0 else "SELL"
            exec_price = intent.price if intent.price > 0 else 1.08500

            # State tracking in mock broker terminal
            if intent.action_type in (ActionType.FULL_CLOSE, 6):
                ticket = intent.order_ticket if intent.order_ticket > 0 else (1000000 + processed)
                self.broker_positions.pop(ticket, None)
            elif intent.action_type in (ActionType.PARTIAL_CLOSE, 5):
                ticket = intent.order_ticket if intent.order_ticket > 0 else (1000000 + processed)
                if ticket in self.broker_positions:
                    p = self.broker_positions[ticket]
                    p.volume = max(0.0, round(p.volume - intent.volume, 2))
                    if p.volume <= 0:
                        self.broker_positions.pop(ticket, None)
            else:
                ticket = 1000000 + processed
                self.broker_positions[ticket] = PositionItem(
                    ticket=ticket,
                    magic_number=intent.magic_number,
                    open_time_unix=int(time.time()),
                    symbol=intent.symbol,
                    position_type=int(intent.side),
                    volume=intent.volume,
                    open_price=exec_price,
                    current_price=exec_price,
                    stop_loss=intent.stop_loss,
                    take_profit=intent.take_profit,
                    comment=f"sig_{intent.signal_id}" if intent.signal_id else "",
                )

            # Write MT4 / MT5 terminal log entries if log_file is specified
            retcode = 0 if self.platform_type == 0 else 10009
            if self.log_file:
                try:
                    now_str = time.strftime("%Y.%m.%d %H:%M:%S")
                    if self.platform_type == 0:
                        # MT4 MQL4 OrderSend logging
                        trade_entry = (
                            f"{now_str}.120 OrderSend: market {side_str.lower()} {intent.volume:.2f} "
                            f"{intent.symbol} at {exec_price:.5f} (ticket #{ticket})\n"
                        )
                    else:
                        # MT5 MQL5 CTrade logging
                        trade_entry = (
                            f"{now_str}.120 CTrade::OrderSend: market {side_str.lower()} {intent.volume:.2f} "
                            f"{intent.symbol} at {exec_price:.5f} [done at {exec_price:.5f}] (ticket #{ticket})\n"
                        )
                    ea_entry = (
                        f"{now_str}.125 [Pipvane EXEC] Market {side_str} {intent.symbol}: "
                        f"Vol={intent.volume:.2f}, Ticket={ticket}, Price={exec_price:.5f}, "
                        f"Status=0, Latency={transit_us} us, Retcode={retcode} (Done)\n"
                    )
                    with open(self.log_file, "a", encoding="utf-8") as f_log:
                        f_log.write(trade_entry)
                        f_log.write(ea_entry)
                        f_log.flush()
                except Exception as log_err:
                    logger.error("Failed to write to terminal log file %s: %s", self.log_file, log_err)

            # Formulate acknowledgment execution feedback
            seq_id += 1
            feedback = ExecutionFeedback(
                signal_id=intent.signal_id,
                order_ticket=ticket,
                execution_status=ExecutionStatus.FILLED,
                broker_retcode=retcode,  # MT4: 0 (ERR_NO_ERROR) / MT5: 10009 (TRADE_RETCODE_DONE)
                requested_volume=intent.volume,
                executed_volume=intent.volume,
                requested_price=intent.price,
                executed_price=exec_price,
                slippage_pips=0.0,
                execution_latency_us=transit_us,
                error_description="OK",
            )

            frame = pack_pipe_frame(
                opcode=NamedPipeOpcode.MSG_EXECUTION_FEEDBACK,
                payload=feedback.pack(),
                seq_id=seq_id,
            )
            self._write_pipe(frame)
            processed += 1

        return processed

    def close(self) -> None:
        """Closes all OS handles."""
        if self._pipe_handle != -1 and _kernel32 is not None:
            _kernel32.CloseHandle(self._pipe_handle)
            self._pipe_handle = -1

        if self._event_handle and _kernel32 is not None:
            _kernel32.CloseHandle(self._event_handle)
            self._event_handle = None

        if self._shm_map is not None:
            try:
                self._shm_map.close()
            except Exception:
                pass
            self._shm_map = None

    def simulate_manual_close(self, ticket: int) -> bool:
        """
        Sprint 14 Task 1: Simulates a manual user close inside the broker terminal,
        removing the position from the broker's active state array without notifying the Engine.
        """
        if ticket in self.broker_positions:
            del self.broker_positions[ticket]
            logger.info("Simulated manual broker close on ticket #%d", ticket)
            return True
        return False

    def inject_orphan_position(
        self,
        ticket: int,
        magic_number: int = 123456,
        symbol: str = "EURUSD",
        volume: float = 0.50,
        side: int = 0,
        open_price: float = 1.08500,
        stop_loss: float = 0.0,
        take_profit: float = 0.0,
    ) -> PositionItem:
        """
        Sprint 14 Task 2: Injects a fake position directly into the mock broker's state array
        carrying the Pipvane Magic Number, completely bypassing the Engine.
        """
        item = PositionItem(
            ticket=ticket,
            magic_number=magic_number,
            open_time_unix=int(time.time()),
            symbol=symbol,
            position_type=side,
            volume=volume,
            open_price=open_price,
            current_price=open_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment="Bypassed Orphan",
        )
        self.broker_positions[ticket] = item
        logger.info(
            "Injected orphan position #%d on broker (Magic=%d, Sym=%s, Vol=%.2f)",
            ticket, magic_number, symbol, volume
        )
        return item

    def send_positions_sync(self) -> bool:
        """
        Transmits MSG_POSITIONS_SYNC frame over Named Pipe carrying current open positions.
        """
        if self._pipe_handle == -1:
            return False
        payload = PositionsSyncPayload(
            account_login=self.account_login,
            positions=list(self.broker_positions.values()),
        )
        frame = pack_pipe_frame(
            opcode=NamedPipeOpcode.MSG_POSITIONS_SYNC,
            payload=payload.pack(),
            seq_id=int(time.time_ns() & 0x7FFFFFFF),
        )
        self._write_pipe(frame)
        return True


def run_mock_bridge_cli() -> None:
    """CLI Entrypoint for running the mock C++ bridge as a standalone process."""
    parser = argparse.ArgumentParser(description="Pipvane v3 Mock C++ MetaTrader DLL Bridge")
    parser.add_argument("--terminal-id", default="bench_terminal_0", help="Terminal identifier")
    parser.add_argument("--iterations", type=int, default=10000, help="Number of intents to consume")
    parser.add_argument("--pipe-name", default=None, help="Custom Named Pipe path")
    parser.add_argument("--shm-name", default=None, help="Custom Shared Memory tag name")
    parser.add_argument("--event-name", default=None, help="Custom Win32 Event name")
    parser.add_argument("--account-login", type=int, default=12345678, help="Simulated account login")
    parser.add_argument("--broker-server", default="Pipvane-Demo01", help="Simulated broker server")
    parser.add_argument("--quiet", action="store_true", help="Suppress informational logging")
    parser.add_argument("--log-file", default=None, help="Path to terminal log file for CTrade output validation")
    parser.add_argument("--platform-type", type=int, default=1, choices=[0, 1], help="Platform: 0=MT4, 1=MT5")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s [%(levelname)s] (MockBridge) %(message)s",
    )

    bridge = MockCppBridgeWorker(
        terminal_id=args.terminal_id,
        pipe_name=args.pipe_name,
        shm_name=args.shm_name,
        event_name=args.event_name,
        account_login=args.account_login,
        broker_server=args.broker_server,
        quiet=args.quiet,
        log_file=args.log_file,
        platform_type=args.platform_type,
    )

    try:
        connected = bridge.connect_pipe(timeout_s=10.0)
        if not connected:
            sys.exit(2)

        bridge.send_init_handshake()
        bridge.attach_shm()
        bridge.run_consumer_loop(args.iterations)
    finally:
        bridge.close()

    sys.exit(0)


if __name__ == "__main__":
    run_mock_bridge_cli()
