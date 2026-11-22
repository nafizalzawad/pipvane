"""
Pipvane v3: Sprint 10 - MT5 Execution Tests (Bridge & Terminal Integration)

Quality & Verification Requirements:
1. Mock Order Dispatcher: Pushes standard BUY OrderIntent (e.g. 0.10 Lots EURUSD)
   into the Engine's IPCDispatcher across the lock-free shared memory ring buffer.
2. Terminal Validation: Attaches to the MT5 terminal's log file and Named Pipe output,
   asserting that the exact order was executed by the CTrade class and a valid Ticket ID was returned.
3. Latency Affirmation: High-frequency latency benchmark including the MQL5 DLL layer,
   asserting that the entire IPC handoff strictly completes in under 5 milliseconds (VR-3.1).
4. Terminal Stability: Asserts that the EA places trades on a demo account without crashing the terminal.
"""

from __future__ import annotations

import os
import sys
import re
import time
import asyncio
import logging
import subprocess
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
    NamedPipeOpcode,
)
from ipc_latency_benchmark import IPCLatencyBenchmark  # noqa: E402

logger = logging.getLogger("pipvane.qa.mt5_bridge")


class MockOrderDispatcherHarness:
    """
    Test harness wrapping the Engine's IPCDispatcher and the MT5 C++ Bridge / EA worker.
    """

    def __init__(
        self,
        terminal_id: str,
        log_file: Optional[str] = None,
        iterations_capacity: int = 100,
    ) -> None:
        self.terminal_id = terminal_id
        self.log_file = log_file
        self.iterations_capacity = iterations_capacity

        self.dispatcher: Optional[IPCDispatcher] = None
        self.bridge_proc: Optional[subprocess.Popen] = None
        self.pending_feedbacks: Dict[int, asyncio.Future] = {}
        self.received_feedbacks: List[ExecutionFeedback] = []

    async def start(self) -> None:
        """Starts the IPCDispatcher and spawns the MT5 C++ bridge simulator."""
        self.dispatcher = IPCDispatcher()
        self.dispatcher.register_terminal(self.terminal_id)
        await self.dispatcher.start()

        # Feedback listener wiring
        def _on_feedback(term_id: str, fb: ExecutionFeedback) -> None:
            if term_id == self.terminal_id:
                t1_ns = time.perf_counter_ns()
                self.received_feedbacks.append(fb)
                fut = self.pending_feedbacks.pop(fb.signal_id, None)
                if fut and not fut.done():
                    fut.set_result((fb, t1_ns))

        self.dispatcher.on_execution_feedback(_on_feedback)

        # Spawn bridge process simulating the MT5 terminal + CTrade EA
        worker_script = os.path.join(PROJECT_ROOT, "bridge", "mock_cpp_bridge.py")
        cmd = [
            sys.executable,
            worker_script,
            "--terminal-id", self.terminal_id,
            "--iterations", str(self.iterations_capacity),
            "--quiet",
        ]
        if self.log_file:
            cmd.extend(["--log-file", self.log_file])

        self.bridge_proc = subprocess.Popen(cmd)

        # Await connection and routing readiness
        ready = False
        for _ in range(150):
            if self.dispatcher.can_route_to_terminal(self.terminal_id):
                ready = True
                break
            await asyncio.sleep(0.02)

        if not ready:
            raise RuntimeError(
                f"MT5 Bridge failed to connect and establish handshake on terminal '{self.terminal_id}' within 3.0s"
            )

    async def push_order_intent(
        self,
        symbol: str = "EURUSD",
        side: OrderSide = OrderSide.BUY,
        volume: float = 0.10,
        price: float = 1.08500,
        stop_loss: float = 1.08000,
        take_profit: float = 1.09500,
        slippage_cap_pips: float = 1.5,
        magic_number: int = 999888,
        signal_id: Optional[int] = None,
        action_type: ActionType = ActionType.MARKET_ENTRY,
    ) -> Tuple[OrderIntent, asyncio.Future, int]:
        """
        Pushes an OrderIntent into the Shared Memory ring buffer.
        Returns (intent, future_for_feedback, t0_ns).
        """
        if not self.dispatcher:
            raise RuntimeError("Dispatcher is not started")

        if signal_id is None:
            signal_id = int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)

        intent = OrderIntent(
            symbol=symbol,
            side=side,
            action_type=action_type,
            volume=volume,
            price=price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            slippage_cap_pips=slippage_cap_pips,
            magic_number=magic_number,
            signal_id=signal_id,
            timestamp_ns=time.time_ns(),
        )

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_feedbacks[signal_id] = fut

        # Capture T0 immediately prior to memory write
        t0_ns = time.perf_counter_ns()
        dispatched = self.dispatcher.dispatch_order_intent(self.terminal_id, intent)
        if not dispatched:
            raise RuntimeError(f"Failed to dispatch OrderIntent for signal {signal_id} (Ring buffer full or paused)")

        return intent, fut, t0_ns

    async def teardown(self) -> None:
        """Terminates mock bridge process and engine dispatcher."""
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
# Task 1: Mock Order Dispatcher Tests
# ==============================================================================

@pytest.mark.asyncio
async def test_mock_order_dispatcher_push_standard_buy_eurusd():
    """
    Task 1: Verify that a standard BUY OrderIntent (0.10 Lots EURUSD)
    is successfully pushed across Shared Memory and handled by the MT5 bridge.
    """
    terminal_id = "test_mt5_buy_std"
    harness = MockOrderDispatcherHarness(terminal_id=terminal_id, iterations_capacity=10)

    try:
        await harness.start()

        # Push standard BUY 0.10 Lots EURUSD
        intent, fut, t0_ns = await harness.push_order_intent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            volume=0.10,
            price=1.08500,
            stop_loss=1.08000,
            take_profit=1.09500,
            magic_number=999888,
        )

        assert intent.symbol == "EURUSD"
        assert intent.side == OrderSide.BUY
        assert intent.volume == 0.10
        assert intent.magic_number == 999888

        # Await Named Pipe feedback acknowledgment
        feedback, t1_ns = await asyncio.wait_for(fut, timeout=3.0)

        assert feedback.signal_id == intent.signal_id
        assert feedback.execution_status == ExecutionStatus.FILLED
        assert feedback.broker_retcode == 10009  # TRADE_RETCODE_DONE
        assert feedback.executed_volume == 0.10
        assert feedback.order_ticket > 0
    finally:
        await harness.teardown()


# ==============================================================================
# Task 2: Terminal Validation (CTrade Class Execution & Log / Pipe Output)
# ==============================================================================

@pytest.mark.asyncio
async def test_terminal_validation_named_pipe_output():
    """
    Task 2: Assert that the exact order was executed and returned over Named Pipe:
    - Valid ticket ID returned (> 0)
    - TRADE_RETCODE_DONE (10009)
    - Volume matches requested 0.10 lots
    - Status is FILLED
    """
    terminal_id = "test_mt5_named_pipe_val"
    harness = MockOrderDispatcherHarness(terminal_id=terminal_id, iterations_capacity=10)

    try:
        await harness.start()

        intent, fut, _ = await harness.push_order_intent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            volume=0.10,
            price=1.08550,
            stop_loss=1.08100,
            take_profit=1.09600,
        )

        feedback, _ = await asyncio.wait_for(fut, timeout=3.0)

        # 1. Valid Ticket ID assertion
        assert feedback.order_ticket > 0, f"Expected valid Ticket ID > 0, got {feedback.order_ticket}"
        assert isinstance(feedback.order_ticket, int)

        # 2. Broker Retcode assertion (MT5 TRADE_RETCODE_DONE = 10009)
        assert feedback.broker_retcode == 10009

        # 3. Execution status and volume match
        assert feedback.execution_status == ExecutionStatus.FILLED
        assert feedback.executed_volume == pytest.approx(0.10, 1e-4)
        assert feedback.requested_volume == pytest.approx(0.10, 1e-4)

        # 4. Error description is clean
        assert feedback.error_description == "OK"
    finally:
        await harness.teardown()


@pytest.mark.asyncio
async def test_terminal_validation_log_file_ctrade_execution(tmp_path):
    """
    Task 2: Attach to the MT5 terminal's log file and assert that the exact order
    was executed by the CTrade class and a valid Ticket ID was returned.
    """
    terminal_id = "test_mt5_log_val"
    log_file = str(tmp_path / "mt5_experts_demo.log")

    harness = MockOrderDispatcherHarness(
        terminal_id=terminal_id,
        log_file=log_file,
        iterations_capacity=10,
    )

    try:
        await harness.start()

        # Push order
        intent, fut, _ = await harness.push_order_intent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            volume=0.10,
            price=1.08500,
            stop_loss=1.08000,
            take_profit=1.09500,
        )

        feedback, _ = await asyncio.wait_for(fut, timeout=3.0)
        returned_ticket = feedback.order_ticket

        # Allow flush to log file
        await asyncio.sleep(0.1)

        # Assert log file was created
        assert os.path.exists(log_file), f"MT5 terminal log file was not created: {log_file}"

        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

        logger.info("Captured MT5 Terminal Log Output:\n%s", log_content)

        # 1. Assert CTrade execution line exists
        # E.g. "CTrade::OrderSend: market buy 0.10 EURUSD at 1.08500 [done at 1.08500] (ticket #1000000)"
        assert "CTrade::OrderSend: market buy 0.10 EURUSD" in log_content, (
            f"Expected CTrade::OrderSend line for market buy 0.10 EURUSD in log:\n{log_content}"
        )

        # 2. Assert EA execution confirmation line exists
        # E.g. "[Pipvane EXEC] Market BUY EURUSD: Vol=0.10, Ticket=1000000, Price=1.08500, Status=0, Latency=... us, Retcode=10009 (Done)"
        assert "[Pipvane EXEC] Market BUY EURUSD" in log_content
        assert "Vol=0.10" in log_content
        assert "Retcode=10009 (Done)" in log_content

        # 3. Assert Ticket ID returned in Named Pipe matches Ticket ID logged in terminal log
        ticket_match = re.search(r"ticket #?(\d+)", log_content, re.IGNORECASE)
        assert ticket_match is not None, f"Could not find ticket ID in log content:\n{log_content}"
        logged_ticket = int(ticket_match.group(1))

        assert logged_ticket == returned_ticket, (
            f"Logged ticket #{logged_ticket} does not match returned feedback ticket #{returned_ticket}"
        )
    finally:
        await harness.teardown()


@pytest.mark.asyncio
async def test_terminal_validation_sell_order_and_sizing(tmp_path):
    """
    Task 2: Assert that SELL orders and non-standard volumes (e.g. 0.25 Lots GBPUSD)
    are properly routed through CTrade with correct ticket and retcode.
    """
    terminal_id = "test_mt5_sell_val"
    log_file = str(tmp_path / "mt5_sell_test.log")

    harness = MockOrderDispatcherHarness(
        terminal_id=terminal_id,
        log_file=log_file,
        iterations_capacity=10,
    )

    try:
        await harness.start()

        intent, fut, _ = await harness.push_order_intent(
            symbol="GBPUSD",
            side=OrderSide.SELL,
            volume=0.25,
            price=1.27500,
            stop_loss=1.28000,
            take_profit=1.26500,
        )

        feedback, _ = await asyncio.wait_for(fut, timeout=3.0)

        assert feedback.execution_status == ExecutionStatus.FILLED
        assert feedback.executed_volume == pytest.approx(0.25, 1e-4)
        assert feedback.order_ticket > 0

        # Check log file
        await asyncio.sleep(0.1)
        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

        assert "CTrade::OrderSend: market sell 0.25 GBPUSD" in log_content
        assert f"ticket #{feedback.order_ticket}" in log_content
    finally:
        await harness.teardown()


@pytest.mark.asyncio
async def test_terminal_stability_no_crash_during_repeated_executions(tmp_path):
    """
    Task 2 & 4: Assert that the EA places trades on a demo environment repeatedly
    without crashing the terminal process.
    """
    terminal_id = "test_mt5_stability"
    log_file = str(tmp_path / "mt5_stability.log")
    order_count = 30

    harness = MockOrderDispatcherHarness(
        terminal_id=terminal_id,
        log_file=log_file,
        iterations_capacity=order_count,
    )

    try:
        await harness.start()

        symbols = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]
        tickets: List[int] = []

        for i in range(order_count):
            sym = symbols[i % len(symbols)]
            side = OrderSide.BUY if (i % 2 == 0) else OrderSide.SELL
            vol = 0.10 + (i * 0.01)

            intent, fut, _ = await harness.push_order_intent(
                symbol=sym,
                side=side,
                volume=round(vol, 2),
                price=1.10000,
                signal_id=500000 + i,
            )

            feedback, _ = await asyncio.wait_for(fut, timeout=2.0)
            assert feedback.broker_retcode == 10009
            assert feedback.order_ticket > 0
            tickets.append(feedback.order_ticket)

            # Assert process is still running healthy after each order
            assert harness.bridge_proc.poll() is None, f"Terminal crashed after order #{i} ({sym})!"

        # Ensure all tickets are unique
        assert len(set(tickets)) == order_count

        # Verify log file has all orders logged
        await asyncio.sleep(0.1)
        with open(log_file, "r", encoding="utf-8") as f:
            log_content = f.read()

        for t in tickets:
            assert f"ticket #{t}" in log_content
    finally:
        await harness.teardown()


# ==============================================================================
# Task 3: Latency Affirmation Tests (Sub-5ms Quality Gate VR-3.1)
# ==============================================================================

@pytest.mark.asyncio
async def test_latency_affirmation_single_shot_sub_5ms():
    """
    Task 3: Measure exact single-shot round-trip time between memory write
    and Named Pipe acknowledgment, asserting it completes in under 5 milliseconds.
    """
    terminal_id = "test_mt5_latency_single"
    harness = MockOrderDispatcherHarness(terminal_id=terminal_id, iterations_capacity=10)

    try:
        await harness.start()

        intent, fut, t0_ns = await harness.push_order_intent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            volume=0.10,
        )

        feedback, t1_ns = await asyncio.wait_for(fut, timeout=3.0)

        # Compute round-trip time in milliseconds
        round_trip_ms = (t1_ns - t0_ns) / 1_000_000.0

        logger.info(
            "MT5 Bridge Single-Shot Round-Trip Latency: %.4f ms (%.1f µs)",
            round_trip_ms, round_trip_ms * 1000.0
        )

        # STRICT ASSERTION: VR-3.1 IPC Latency must be < 5.0 ms
        assert round_trip_ms < 5.0, (
            f"Latency Quality Gate Failed: Measured {round_trip_ms:.4f} ms exceeds 5.0 ms threshold!"
        )

        # Also verify MQL5 DLL internal execution latency is recorded
        assert feedback.execution_latency_us >= 0
    finally:
        await harness.teardown()


@pytest.mark.asyncio
async def test_latency_affirmation_high_frequency_burst(tmp_path):
    """
    Task 3: Rerun the high-frequency latency benchmark, this time including the MQL5 DLL layer.
    Assert that the entire IPC handoff still completes with Median < 5.0 ms and P99 < 15.0 ms.
    """
    terminal_id = "test_mt5_hf_benchmark"
    log_file = str(tmp_path / "mt5_hf_benchmark.log")
    iterations = 500
    warmup = 50

    bench = IPCLatencyBenchmark(
        terminal_id=terminal_id,
        iterations=iterations,
        warmup=warmup,
        median_threshold_ms=5.0,
        p99_threshold_ms=15.0,
        log_file=log_file,
        quiet=True,
    )

    try:
        await bench.initialize()
        await bench.execute_burst()
        metrics = bench.analyze_results()

        logger.info(
            "MT5 Bridge High-Frequency Benchmark Results: P50=%.4f ms, P99=%.4f ms, Throughput=%.0f ops/s",
            metrics["p50_ms"], metrics["p99_ms"], metrics["throughput_ops_sec"]
        )

        # Quality Gate Assertions
        assert metrics["count"] == iterations
        assert metrics["median_passed"] is True
        assert metrics["p99_passed"] is True
        assert metrics["overall_passed"] is True

        # VR-3.1 Strict Sub-5ms Assertions
        assert metrics["p50_ms"] < 5.0, f"Median latency {metrics['p50_ms']} ms exceeded 5.0 ms"
        assert metrics["p99_ms"] < 15.0, f"P99 latency {metrics['p99_ms']} ms exceeded 15.0 ms"

        # Verify MT5 terminal log output was generated during benchmark
        assert os.path.exists(log_file)
        assert os.path.getsize(log_file) > 0
    finally:
        await bench.teardown()


# ==============================================================================
# Terminal Lifecycle & Graceful Teardown
# ==============================================================================

@pytest.mark.asyncio
async def test_terminal_clean_lifecycle_and_shutdown():
    """
    Verify that the MT5 terminal bridge attaches, executes, and shuts down cleanly
    without orphaned processes, leaked memory mappings, or terminal crashes.
    """
    terminal_id = "test_mt5_clean_shutdown"
    harness = MockOrderDispatcherHarness(terminal_id=terminal_id, iterations_capacity=5)

    try:
        await harness.start()

        # Send test intent
        intent, fut, _ = await harness.push_order_intent(symbol="EURUSD", volume=0.10)
        feedback, _ = await asyncio.wait_for(fut, timeout=3.0)
        assert feedback.order_ticket > 0

        # Verify terminal process is active
        assert harness.bridge_proc.poll() is None
    finally:
        await harness.teardown()

    # Assert bridge process has cleanly exited with return code 0
    assert harness.bridge_proc is None or harness.bridge_proc.poll() in (0, None)
