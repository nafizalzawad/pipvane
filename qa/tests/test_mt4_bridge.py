"""
Pipvane v3: Sprint 11 - MT4 Execution & Integrity Tests (FR-6.3 & FR-6.4).

Quality & Verification Requirements:
1. Happy Path Execution: Sends a BUY OrderIntent through the Engine to an MT4 demo account
   and asserts successful execution over Shared Memory and Named Pipe.
2. Tampering Simulation: Modifies a single byte in the Pipvane_MT4.ex4 binary file on disk
   simulating an EA hijacking / malicious payload replacement attempt.
3. Integrity Assertion: Asserts that EX4IntegrityChecker immediately catches the modified byte,
   logs a critical security alert (CRITICAL [SECURITY_TAMPER_DETECTED]), and strictly refuses
   to connect the IPC bridge to that terminal.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import pytest

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_dispatcher import IPCDispatcher  # noqa: E402
from engine.ipc_protocol import (  # noqa: E402
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionStatus,
    ExecutionFeedback,
    TerminalStatus,
    NamedPipeOpcode,
)
from engine.ex4_integrity import (  # noqa: E402
    EX4IntegrityChecker,
    EX4IntegrityError,
    default_integrity_checker,
)

logger = logging.getLogger("pipvane.qa.mt4_bridge")


def create_authentic_ex4_binary(file_path: Path) -> Tuple[Path, str]:
    """
    Creates a simulated authentic Pipvane_MT4.ex4 compiled binary on disk.
    Returns (Path, sha256_hash).
    """
    # 2048-byte realistic mock MQL4 bytecode payload with magic header
    magic_header = b"\x45\x58\x34\x00\x03\x00\x00\x00\x8C\x05\x00\x00"  # EX4 magic + build 1420
    payload_body = bytes([(i * 37 + 13) % 256 for i in range(2036)])
    binary_content = magic_header + payload_body

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(binary_content)

    expected_hash = EX4IntegrityChecker.compute_sha256(file_path)
    return file_path, expected_hash


class MT4OrderDispatcherHarness:
    """
    Test harness for MT4 demo terminal execution and integrity gating.
    """

    def __init__(
        self,
        terminal_id: str,
        ex4_path: Optional[str] = None,
        expected_ex4_hash: Optional[str] = None,
        log_file: Optional[str] = None,
        iterations_capacity: int = 50,
    ) -> None:
        self.terminal_id = terminal_id
        self.ex4_path = ex4_path
        self.expected_ex4_hash = expected_ex4_hash
        self.log_file = log_file
        self.iterations_capacity = iterations_capacity

        self.dispatcher: Optional[IPCDispatcher] = None
        self.bridge_proc: Optional[subprocess.Popen] = None
        self.pending_feedbacks: Dict[int, asyncio.Future] = {}
        self.received_feedbacks: List[ExecutionFeedback] = []

    async def start(self) -> None:
        """Starts the IPCDispatcher and spawns the MT4 C++ bridge simulator."""
        self.dispatcher = IPCDispatcher()
        self.dispatcher.register_terminal(
            self.terminal_id,
            ex4_path=self.ex4_path,
            expected_ex4_hash=self.expected_ex4_hash,
        )
        await self.dispatcher.start()

        def _on_feedback(term_id: str, fb: ExecutionFeedback) -> None:
            if term_id == self.terminal_id:
                t1_ns = time.perf_counter_ns()
                self.received_feedbacks.append(fb)
                fut = self.pending_feedbacks.pop(fb.signal_id, None)
                if fut and not fut.done():
                    fut.set_result((fb, t1_ns))

        self.dispatcher.on_execution_feedback(_on_feedback)

        # Spawn bridge process in MT4 mode (platform_type = 0)
        worker_script = os.path.join(PROJECT_ROOT, "bridge", "mock_cpp_bridge.py")
        cmd = [
            sys.executable,
            worker_script,
            "--terminal-id", self.terminal_id,
            "--platform-type", "0",  # MT4
            "--broker-server", "Pipvane-MT4Demo",
            "--iterations", str(self.iterations_capacity),
            "--quiet",
        ]
        if self.log_file:
            cmd.extend(["--log-file", self.log_file])

        self.bridge_proc = subprocess.Popen(cmd)

        # Await connection and routing readiness
        ready = False
        for _ in range(100):
            if self.dispatcher.can_route_to_terminal(self.terminal_id):
                ready = True
                break
            await asyncio.sleep(0.02)

        if not ready:
            raise RuntimeError(
                f"MT4 Bridge failed to connect and establish handshake on terminal '{self.terminal_id}' within 2.0s"
            )

    async def push_buy_order(
        self,
        symbol: str = "EURUSD",
        volume: float = 0.10,
        price: float = 1.08500,
        stop_loss: float = 1.08000,
        take_profit: float = 1.09500,
        magic_number: int = 142099,
        signal_id: Optional[int] = None,
    ) -> Tuple[OrderIntent, asyncio.Future, int]:
        """Pushes a BUY OrderIntent into the MT4 terminal's shared memory ring buffer."""
        if not self.dispatcher:
            raise RuntimeError("Dispatcher is not started")

        if signal_id is None:
            signal_id = int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)

        intent = OrderIntent(
            symbol=symbol,
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=volume,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            slippage_cap_pips=1.5,
            magic_number=magic_number,
            signal_id=signal_id,
            timestamp_ns=time.time_ns(),
        )

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_feedbacks[signal_id] = fut

        t0_ns = time.perf_counter_ns()
        dispatched = self.dispatcher.dispatch_order_intent(self.terminal_id, intent)
        if not dispatched:
            raise RuntimeError(f"Failed to dispatch MT4 OrderIntent for signal {signal_id}")

        return intent, fut, t0_ns

    async def teardown(self) -> None:
        """Terminates bridge process and dispatcher."""
        if self.bridge_proc:
            try:
                self.bridge_proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.bridge_proc.kill()
                self.bridge_proc.wait()
            self.bridge_proc = None

        if self.dispatcher:
            await self.dispatcher.stop()
            self.dispatcher = None


# ==============================================================================
# Task 1: Happy Path Execution (MT4 Demo Account)
# ==============================================================================

@pytest.mark.asyncio
async def test_mt4_happy_path_execution(tmp_path):
    """
    Task 1: Verify happy path execution of a BUY OrderIntent (0.10 Lots EURUSD)
    routed to an MT4 demo account, asserting successful order placement and ticket assignment.
    """
    terminal_id = "test_mt4_happy_demo_0"
    log_file = str(tmp_path / "mt4_experts.log")
    ex4_file, expected_hash = create_authentic_ex4_binary(tmp_path / "MQL4" / "Experts" / "Pipvane_MT4.ex4")

    harness = MT4OrderDispatcherHarness(
        terminal_id=terminal_id,
        ex4_path=str(ex4_file),
        expected_ex4_hash=expected_hash,
        log_file=log_file,
        iterations_capacity=10,
    )

    try:
        await harness.start()

        # Verify MT4 terminal context was initialized with MT4 platform type
        ctx = harness.dispatcher._terminals[terminal_id]
        assert ctx.hello_info is not None
        assert ctx.hello_info.platform_type == 0  # 0 = MT4
        assert ctx.hello_info.broker_server == "Pipvane-MT4Demo"
        assert ctx.status == TerminalStatus.CONNECTED

        # Push BUY OrderIntent: 0.10 Lots EURUSD
        intent, fut, t0_ns = await harness.push_buy_order(
            symbol="EURUSD",
            volume=0.10,
            price=1.08500,
            stop_loss=1.08000,
            take_profit=1.09500,
            magic_number=142099,
        )

        # Await Named Pipe execution feedback
        feedback, t1_ns = await asyncio.wait_for(fut, timeout=3.0)

        # Assert successful execution outcomes
        assert feedback.signal_id == intent.signal_id
        assert feedback.execution_status == ExecutionStatus.FILLED
        assert feedback.order_ticket > 0, f"Expected valid ticket > 0, got {feedback.order_ticket}"
        assert feedback.executed_volume == pytest.approx(0.10, 1e-4)

        # Assert MT4 terminal log file output
        await asyncio.sleep(0.1)
        assert os.path.exists(log_file)
        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

        logger.info("Captured MT4 Log Output:\n%s", log_content)
        assert "OrderSend: market buy 0.10 EURUSD" in log_content
        assert f"ticket #{feedback.order_ticket}" in log_content
        assert "[Pipvane EXEC] Market BUY EURUSD" in log_content
    finally:
        await harness.teardown()


# ==============================================================================
# Task 2 & 3: Tampering Simulation & EX4 Integrity Gate Assertions
# ==============================================================================

def test_ex4_integrity_checker_detects_single_byte_tampering(tmp_path, caplog):
    """
    Task 2 & 3: Modifies a single byte in the Pipvane_MT4.ex4 file on disk.
    Asserts that EX4IntegrityChecker immediately catches the modified byte
    and logs a critical security alert.
    """
    ex4_path = tmp_path / "Pipvane_MT4.ex4"
    _, original_hash = create_authentic_ex4_binary(ex4_path)

    checker = EX4IntegrityChecker()
    checker.register_manifest_hash("Pipvane_MT4.ex4", original_hash)

    # 1. Authentic file verify passes
    is_valid, hash_val, msg = checker.verify_file(ex4_path)
    assert is_valid is True
    assert hash_val == original_hash
    assert msg == "OK"

    # 2. Tampering Simulation: Modify a single byte in the binary
    with open(ex4_path, "r+b") as f:
        f.seek(64)  # Byte index 64 in bytecode
        original_byte = f.read(1)[0]
        tampered_byte = bytes([(original_byte ^ 0xFF)])  # Flip all bits of this byte
        f.seek(64)
        f.write(tampered_byte)

    tampered_hash = EX4IntegrityChecker.compute_sha256(ex4_path)
    assert tampered_hash != original_hash, "Hash should change after single byte modification!"

    # 3. Integrity Assertion: Run checker on tampered binary
    with caplog.at_level(logging.CRITICAL):
        is_valid_after_tamper, actual_hash, reason = checker.verify_file(ex4_path)

    # Assert integrity violation caught immediately
    assert is_valid_after_tamper is False
    assert actual_hash == tampered_hash
    assert "SECURITY_TAMPER_DETECTED" in reason

    # Assert critical security alert logged
    critical_logs = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical_logs) > 0
    assert any("SECURITY_TAMPER_DETECTED" in r.message for r in critical_logs)
    assert any("Binary tampering detected" in r.message for r in critical_logs)


def test_ex4_integrity_checker_verify_and_guard_raises_error(tmp_path):
    """
    Task 3: Assert that verify_and_guard raises EX4IntegrityError when a file is modified.
    """
    ex4_path = tmp_path / "Pipvane_MT4.ex4"
    _, original_hash = create_authentic_ex4_binary(ex4_path)

    checker = EX4IntegrityChecker()

    # Modify single byte
    with open(ex4_path, "r+b") as f:
        f.seek(10)
        f.write(b"\x99")

    with pytest.raises(EX4IntegrityError) as exc_info:
        checker.verify_and_guard(ex4_path, expected_hash=original_hash)

    assert "SECURITY_TAMPER_DETECTED" in str(exc_info.value)


@pytest.mark.asyncio
async def test_engine_refuses_to_register_terminal_with_tampered_ex4(tmp_path, caplog):
    """
    Task 3: When an MT4 terminal specifies an EX4 binary that has been tampered with,
    the Engine's IPCDispatcher must log a critical alert and immediately refuse to register
    or connect the IPC bridge to that terminal.
    """
    terminal_id = "test_mt4_tampered_reg"
    ex4_path = tmp_path / "Pipvane_MT4.ex4"
    _, original_hash = create_authentic_ex4_binary(ex4_path)

    # Modify byte 120 on disk
    with open(ex4_path, "r+b") as f:
        f.seek(120)
        f.write(b"\x00")

    dispatcher = IPCDispatcher()

    with caplog.at_level(logging.CRITICAL):
        with pytest.raises(EX4IntegrityError) as exc_info:
            dispatcher.register_terminal(
                terminal_id,
                ex4_path=str(ex4_path),
                expected_ex4_hash=original_hash,
            )

    assert "EX4 Integrity Check Failed" in str(exc_info.value)

    # Verify critical security alert logged
    critical_records = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any("Rejecting terminal" in r.message for r in critical_records)

    # Assert terminal cannot route
    assert dispatcher.can_route_to_terminal(terminal_id) is False


@pytest.mark.asyncio
async def test_engine_blocks_terminal_hello_when_tampered_between_sessions(tmp_path, caplog):
    """
    Task 3: Simulates dynamic hijacking: Terminal registers with authentic file,
    but an attacker modifies Pipvane_MT4.ex4 on disk right before the bridge connects.
    The Engine must catch the modified byte during the INIT_HELLO handshake,
    log a critical alert, set status to SECURITY_BLOCKED, and refuse connection.
    """
    terminal_id = "test_mt4_tampered_handshake"
    ex4_path = tmp_path / "Pipvane_MT4.ex4"
    _, original_hash = create_authentic_ex4_binary(ex4_path)

    dispatcher = IPCDispatcher()
    # Register while file is intact
    ctx = dispatcher.register_terminal(
        terminal_id,
        ex4_path=str(ex4_path),
        expected_ex4_hash=original_hash,
    )
    await dispatcher.start()

    try:
        # Attacker injects tampering into the .ex4 file on disk
        with open(ex4_path, "r+b") as f:
            f.seek(500)
            f.write(b"\xCC\xCC\xCC")  # Injected INT 3 breakpoint instructions

        # Spawn MT4 bridge process attempting to connect
        worker_script = os.path.join(PROJECT_ROOT, "bridge", "mock_cpp_bridge.py")
        cmd = [
            sys.executable,
            worker_script,
            "--terminal-id", terminal_id,
            "--platform-type", "0",
            "--iterations", "5",
            "--quiet",
        ]

        with caplog.at_level(logging.CRITICAL):
            bridge_proc = subprocess.Popen(cmd)
            try:
                # Wait for handshake attempt (poll up to 3.0s)
                for _ in range(30):
                    if ctx.status == TerminalStatus.SECURITY_BLOCKED:
                        break
                    await asyncio.sleep(0.1)

                # ASSERTION: The Engine caught the tampering and blocked the terminal
                assert ctx.status == TerminalStatus.SECURITY_BLOCKED
                assert dispatcher.can_route_to_terminal(terminal_id) is False

                # ASSERTION: Critical security alert logged
                critical_logs = [r for r in caplog.records if r.levelno == logging.CRITICAL]
                assert any("SECURITY_TAMPER_DETECTED" in r.message for r in critical_logs)
                assert any("Refusing to connect IPC bridge" in r.message for r in critical_logs)

                # ASSERTION: Dispatching to this terminal is strictly refused
                intent = OrderIntent(
                    symbol="EURUSD",
                    side=OrderSide.BUY,
                    action_type=ActionType.MARKET_ENTRY,
                    volume=0.10,
                    signal_id=99988877,
                )
                dispatched = dispatcher.dispatch_order_intent(terminal_id, intent)
                assert dispatched is False, "Engine must strictly reject dispatch to SECURITY_BLOCKED terminal!"
            finally:
                bridge_proc.kill()
                bridge_proc.wait()
    finally:
        await dispatcher.stop()


# ==============================================================================
# Edge Cases: Missing File & Tamper Recovery
# ==============================================================================

def test_ex4_integrity_checker_missing_file_handling(tmp_path):
    """Verify that a non-existent EX4 binary returns False and is flagged safely."""
    missing_file = tmp_path / "NonExistent_Pipvane_MT4.ex4"
    checker = EX4IntegrityChecker()

    is_valid, actual_hash, reason = checker.verify_file(missing_file)
    assert is_valid is False
    assert "missing on disk" in reason


def test_tamper_recovery_restores_integrity(tmp_path):
    """
    Verify that restoring authentic bytes to a tampered file resolves the integrity gate.
    """
    ex4_path = tmp_path / "Pipvane_MT4.ex4"
    _, original_hash = create_authentic_ex4_binary(ex4_path)

    checker = EX4IntegrityChecker()

    # 1. Tamper single byte
    with open(ex4_path, "r+b") as f:
        f.seek(32)
        f.write(b"\xEE")

    assert checker.verify_file(ex4_path, original_hash)[0] is False

    # 2. Restore authentic binary
    create_authentic_ex4_binary(ex4_path)

    # 3. Assert verified again
    assert checker.verify_file(ex4_path, original_hash)[0] is True
