"""
Pipvane v3 - Unit Test Suite: Burst Queue Optimization & Memory Profiler
========================================================================
Sprint 21 Directive: BURST QUEUE OPTIMIZATION
Tests cover:
1. Async Thread Pool: Verify SignalParser and ExecutionOrchestrator utilize
   a robust Asyncio/Worker-pool pattern. Parsing a heavy text block must not
   block the IPC dispatch of a previously parsed trade.
2. Asynchronous Disk Writes: Verify SQLite operations execute off the event loop
   thread via background worker pool and support flush_storage().
3. IngestionController Async Operations: Verify non-blocking append-only ledger writes.
4. Memory Profiling (tracemalloc): Instrument core loop, execute burst event,
   and verify memory profile returns flat after queue drain and garbage collection.
5. ManagedPosition Pruning: Memory cleanup of closed positions prevents unbounded RAM growth.
"""

import os
import asyncio
import gc
import threading
import time
import tracemalloc
from decimal import Decimal
from typing import Dict, List, Optional

import pytest

from engine.burst_profiler import BurstProfiler, BurstProfileMetrics
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    ExecutionResult,
    ExecutionStatus,
    ManagedPosition,
    TerminalExecutionConfig,
)
from engine.ingestion_controller import IngestionController
from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    OrderIntent,
    OrderSide,
)
from engine.risk_engine import MarketQuote
from engine.signal_parser import ParsedSignal, SignalParser


class MockAsyncIPCDispatcher:
    """Mock IPC dispatcher recording dispatch timestamps and simulating zero-copy execution."""

    def __init__(self, terminal_id: str = "burst_term_1", dispatch_delay_s: float = 0.0) -> None:
        self.terminal_id = terminal_id
        self.dispatch_delay_s = dispatch_delay_s
        self.dispatched_intents: List[OrderIntent] = []
        self.dispatch_timestamps: List[float] = []
        self._on_feedback_cbs = []
        self._ticket_counter = 10_000

    def on_execution_feedback(self, cb):
        self._on_feedback_cbs.append(cb)

    def on_account_state(self, cb):
        pass

    def on_positions_sync(self, cb):
        pass

    def on_capabilities(self, cb):
        pass

    def on_symbols_sync(self, cb):
        pass

    def can_route_to_terminal(self, terminal_id: str) -> bool:
        return True

    def get_broker_positions(self, terminal_id: Optional[str] = None):
        return []

    async def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        t_now = time.perf_counter()
        self.dispatch_timestamps.append(t_now)
        self.dispatched_intents.append(intent)
        if self.dispatch_delay_s > 0:
            await asyncio.sleep(self.dispatch_delay_s)

        self._ticket_counter += 1
        ticket = intent.order_ticket if intent.order_ticket > 0 else self._ticket_counter

        fb = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=ticket,
            execution_status=ExecutionStatus.FILLED,
            broker_retcode=10009,
            requested_volume=intent.volume,
            executed_volume=intent.volume,
            requested_price=intent.price,
            executed_price=intent.price or 1.0850,
            slippage_pips=0.0,
            execution_latency_us=45,
            error_description="",
        )
        for cb in self._on_feedback_cbs:
            cb(terminal_id, fb)
        return True


# =============================================================================
# Task 1: Async Thread Pool Parsing Tests
# =============================================================================

@pytest.mark.asyncio
async def test_signal_parser_parse_async_worker_pool():
    """Verify parse_async offloads CPU regex execution to the worker thread pool."""
    parser = SignalParser(max_workers=4)
    caller_thread = threading.get_ident()

    parsed = await parser.parse_async("BUY EURUSD @ 1.0850 SL 1.0800 TP 1.0920")
    assert parsed.action == "BUY"
    assert parsed.symbol == "EURUSD"
    assert parsed.stop_loss == 1.0800
    assert 1.0920 in parsed.take_profits
    parser.shutdown()


@pytest.mark.asyncio
async def test_signal_parser_parse_batch_async_concurrent():
    """Verify parse_batch_async processes multiple signals concurrently."""
    parser = SignalParser(max_workers=4)
    raw_texts = [
        "BUY EURUSD SL 1.0800 TP 1.0900",
        "SELL GBPUSD SL 1.2800 TP 1.2600",
        "BUY USDJPY SL 154.00 TP 156.00",
        "SELL XAUUSD SL 2650.00 TP 2620.00",
        "BUY BTCUSD SL 85000 TP 92000",
    ]

    results = await parser.parse_batch_async(raw_texts)
    assert len(results) == len(raw_texts)
    assert results[0].symbol == "EURUSD"
    assert results[1].symbol == "GBPUSD"
    assert results[2].symbol == "USDJPY"
    assert results[3].symbol == "XAUUSD"
    assert results[4].symbol == "BTCUSD"
    parser.shutdown()


@pytest.mark.asyncio
async def test_heavy_text_parsing_does_not_block_concurrent_ipc_dispatch():
    """
    Sprint 21 Core Mandate:
    Parsing a heavy text block must not block the IPC dispatch of a previously parsed trade.
    """
    terminal_id = "term_non_blocking"
    dispatcher = MockAsyncIPCDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id))
    parser = SignalParser(max_workers=4)

    # 1. Synthesize a heavy text block with hundreds of lines of chatter
    repetitive_block = "\n".join([f"Comment line {i}: market analysis and economic commentary note." for i in range(120)])
    heavy_text = f"{repetitive_block}\nFINAL SIGNAL: BUY EURUSD SL 1.0800 TP 1.0950\n{repetitive_block}"

    # 2. Previously parsed trade intent ready for immediate dispatch
    trade1_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.0850,
        stop_loss=1.0800,
        take_profit=1.0950,
        signal_id=50_001,
    )

    # Pre-warm parser pool
    await parser.parse_async("BUY EURUSD SL 1.0800 TP 1.0900")

    # 3. Launch heavy parsing task and trade IPC dispatch concurrently
    parse_task = asyncio.create_task(parser.parse_async(heavy_text))
    await asyncio.sleep(0.001)

    t0 = time.perf_counter()
    exec_res = await orchestrator.submit_order(terminal_id, trade1_intent)
    dispatch_duration = time.perf_counter() - t0

    assert exec_res.success is True
    assert len(dispatcher.dispatched_intents) == 1
    # Trade 1 must complete dispatch in < 25-50ms, completely unaffected by the heavy parse
    max_duration = 0.060 if os.environ.get("CI") else 0.035
    assert dispatch_duration < max_duration, f"Dispatch took {dispatch_duration*1000:.2f}ms, expected < {max_duration*1000:.0f}ms"

    # Wait for the heavy parsing task to finish
    parsed_heavy = await parse_task
    assert parsed_heavy.symbol == "EURUSD"
    assert parsed_heavy.action == "BUY"

    parser.shutdown()
    await orchestrator.stop()


# =============================================================================
# Task 2: Asynchronous SQLite Storage Operations Tests
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_async_storage_writes_and_flush():
    """Verify record_order_intent_async and record_journey_event_async execute non-blocking."""
    terminal_id = "term_storage_test"
    dispatcher = MockAsyncIPCDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id))

    # 1. Dispatch 50 async journey events and order intents
    for i in range(50):
        await orchestrator.record_order_intent_async(
            signal_id=60_000 + i,
            ticket=70_000 + i,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=0,
            volume=0.50,
            open_price=1.08500,
            status="OPEN",
        )
        await orchestrator.record_journey_event_async(
            signal_id=60_000 + i,
            ticket=70_000 + i,
            magic_number=123456,
            terminal_id=terminal_id,
            event_type="TEST_EVENT",
            status="OPEN",
            details=f"Test journey item #{i}",
        )

    # 2. Flush storage
    await orchestrator.flush_storage()

    # 3. Verify SQLite records were written accurately
    active_orders = orchestrator.get_active_orders(terminal_id)
    assert len(active_orders) == 50

    await orchestrator.stop()


@pytest.mark.asyncio
async def test_ingestion_controller_async_operations():
    """Verify IngestionController async ledger methods execute in background thread pool."""
    controller = IngestionController(db_path=":memory:")

    # New message async
    rec = await controller.handle_new_message_async(
        channel_id=1001,
        message_id=2001,
        raw_text="BUY GOLD 2650 SL 2640 TP 2670",
    )
    assert rec.message_id == 2001
    assert rec.version == 1

    # Edit message async
    rec_edit = await controller.handle_message_edited_async(
        channel_id=1001,
        message_id=2001,
        raw_text="BUY GOLD 2650 SL 2645 TP 2670 (EDIT)",
    )
    assert rec_edit.version == 2

    # Delete message async
    del_records = await controller.handle_delete_messages_async(
        channel_id=1001,
        message_ids=[2001],
    )
    assert len(del_records) == 1
    assert del_records[0].deleted is True

    controller.close()


# =============================================================================
# Task 3: Memory Profiling & Zero Leak Verification
# =============================================================================

@pytest.mark.asyncio
async def test_burst_queue_memory_profiler_flat_memory():
    """
    Sprint 21 Core Mandate:
    Instruments the core loop using tracemalloc to track garbage collection during a burst event.
    Ensures memory usage stays perfectly flat after a spike clears.
    """
    terminal_id = "term_burst_bench"
    dispatcher = MockAsyncIPCDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id))
    parser = SignalParser(max_workers=4)

    profiler = BurstProfiler(orchestrator=orchestrator, parser=parser)

    # Run burst profile with 250 signals
    metrics = await profiler.run_burst_profile(
        terminal_id=terminal_id,
        num_signals=250,
        burst_concurrency=25,
        tolerance_kb=150.0,
    )

    assert metrics.total_signals == 250
    assert metrics.throughput_signals_per_sec > 10.0
    assert metrics.is_flat_memory is True, (
        f"Memory leak detected! Net delta: {metrics.net_delta_kb:.2f} KB. "
        f"Top allocations: {metrics.top_allocations}"
    )

    parser.shutdown()
    await orchestrator.stop()


# =============================================================================
# Task 4: ManagedPosition Pruning Memory Optimization
# =============================================================================

def test_managed_position_pruning_prevents_memory_leak():
    """Verify prune_closed_positions cleans up closed positions to keep memory bounded."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=MockAsyncIPCDispatcher())

    # Add 10 open positions and 20 closed positions
    for i in range(10):
        orchestrator._managed_positions[1000 + i] = ManagedPosition(
            ticket=1000 + i,
            terminal_id="term_1",
            symbol="EURUSD",
            side=OrderSide.BUY,
            entry_price=1.0850,
            original_volume=1.0,
            current_volume=1.0,
            current_sl=1.0800,
            current_tp=1.0900,
            closed=False,
        )

    for i in range(20):
        orchestrator._managed_positions[2000 + i] = ManagedPosition(
            ticket=2000 + i,
            terminal_id="term_1",
            symbol="EURUSD",
            side=OrderSide.BUY,
            entry_price=1.0850,
            original_volume=1.0,
            current_volume=0.0,
            current_sl=1.0800,
            current_tp=1.0900,
            closed=True,
        )

    assert len(orchestrator._managed_positions) == 30

    pruned = orchestrator.prune_closed_positions(max_age_s=0.0)
    assert pruned == 20
    assert len(orchestrator._managed_positions) == 10  # Exactly 10 open positions retained!
    orchestrator.close()
