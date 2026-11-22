"""
Pipvane v3 - Unit Test Suite: Broker Freeze & Stops Level Guard
==============================================================
Sprint 20 Directive: BROKER FREEZE LEVELS
Tests cover:
1. Level Query: Reads SYMBOL_TRADE_STOPS_LEVEL and SYMBOL_TRADE_FREEZE_LEVEL
   from terminal capabilities and IPC bridge before order execution.
2. Dynamic Adjustment: If an incoming signal requests an SL that sits inside the
   Stops Level, applies FreezePolicy:
   - ADJUST_TO_MINIMUM: Widens SL/TP mathematically to exactly match broker's minimum.
   - SKIP_TRADE: Intercepts and returns SKIP_STOPS_LEVEL skip reason.
3. Freeze Level Protection: Blocks order modifications when market price is within
   the broker freeze level of active stops.
4. Precision across Asset Classes: Forex (5-digit), JPY (3-digit), Metals (XAUUSD), Indices.
5. Performance: Pure in-memory latency < 100 microseconds (< 5ms engine budget).
"""

import time
import pytest
from decimal import Decimal
from typing import Dict, List, Optional

from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    FreezeLevelGuard,
    TerminalExecutionConfig,
    ExecutionResult,
    ExecutionStatus,
    SkipCode,
)
from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    OrderIntent,
    OrderSide,
    PositionItem,
    CapabilitiesPayload,
    NamedPipeOpcode,
)
from engine.risk_engine import (
    AccountProfile,
    FreezePolicy,
    MarketQuote,
    SkipReason,
    SKIP_STOPS_LEVEL,
    SKIP_FREEZE_LEVEL,
)
from engine.symbol_mapper import PipConverter, default_pip_converter


class MockDispatcher:
    """Mock IPC dispatcher providing simulated execution feedback and capabilities."""

    def __init__(self, terminal_id: str = "term_fl_1") -> None:
        self.terminal_id = terminal_id
        self.dispatched_intents: List[OrderIntent] = []
        self._on_feedback_cbs = []
        self._ticket_counter = 5000
        self._capabilities: Optional[CapabilitiesPayload] = None

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

    def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        self.dispatched_intents.append(intent)
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
            executed_price=intent.price or 1.08500,
            slippage_pips=0.0,
            execution_latency_us=50,
            error_description="",
        )
        for cb in self._on_feedback_cbs:
            cb(terminal_id, fb)
        return True


# =============================================================================
# Level Query & Capabilities Telemetry Tests
# =============================================================================

def test_freeze_level_guard_query_levels_from_config():
    """Verify level query reads stops and freeze points from TerminalExecutionConfig."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=MockDispatcher("term_query_test"))
    config = TerminalExecutionConfig(
        terminal_id="term_query_test",
        stops_level_points=50,  # 50 points = 5.0 pips (mult=10)
        freeze_level_points=30, # 30 points = 3.0 pips
    )
    orchestrator.register_terminal_config(config)

    guard = FreezeLevelGuard(orchestrator=orchestrator)
    levels = guard.query_levels("term_query_test", "EURUSD")

    assert levels["stops_level_points"] == 50
    assert levels["freeze_level_points"] == 30
    assert abs(levels["stops_level_pips"] - 5.0) < 1e-5
    assert abs(levels["freeze_level_pips"] - 3.0) < 1e-5
    assert abs(levels["stops_level_price_offset"] - 0.00050) < 1e-7


def test_freeze_level_guard_dynamic_symbol_registration():
    """Verify dynamic symbol-specific stops and freeze levels take priority."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=MockDispatcher("term_dyn_test"))
    config = TerminalExecutionConfig(
        terminal_id="term_dyn_test",
        stops_level_points=10,
        freeze_level_points=10,
    )
    orchestrator.register_terminal_config(config)

    guard = FreezeLevelGuard(orchestrator=orchestrator)
    guard.register_symbol_levels(
        terminal_id="term_dyn_test",
        symbol="GBPJPY",
        stops_level_points=80,  # 80 points = 8.0 pips for JPY
        freeze_level_points=40, # 40 points = 4.0 pips
    )

    levels = guard.query_levels("term_dyn_test", "GBPJPY")
    assert levels["stops_level_points"] == 80
    assert levels["freeze_level_points"] == 40
    assert abs(levels["stops_level_pips"] - 8.0) < 1e-5
    assert abs(levels["freeze_level_pips"] - 4.0) < 1e-5


# =============================================================================
# Dynamic Adjustment Tests (ADJUST_TO_MINIMUM)
# =============================================================================

@pytest.mark.asyncio
async def test_stops_level_adjust_widens_buy_sl_to_minimum():
    """
    Directive Mandate: If an incoming signal requests an SL inside Stops Level,
    ADJUST_TO_MINIMUM widens SL mathematically to exactly match broker's minimum.
    BUY: Entry = 1.08500, tight SL = 1.08480 (2 pips), Stops Level = 5 pips (0.00050).
    Expected adjusted SL = 1.08500 - 0.00050 = 1.08450.
    """
    terminal_id = "term_adjust_buy"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08480,  # 2.0 pips SL distance (breaches 5.0 pip min)
        take_profit=1.09500,
        signal_id=901,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True
    assert len(dispatcher.dispatched_intents) == 1

    dispatched = dispatcher.dispatched_intents[0]
    assert abs(dispatched.stop_loss - 1.08450) < 1e-5  # Exactly widened to 5.0 pips!


@pytest.mark.asyncio
async def test_stops_level_adjust_widens_sell_sl_to_minimum():
    """
    SELL: Entry = 1.08500, tight SL = 1.08520 (2 pips), Stops Level = 5 pips (0.00050).
    Expected adjusted SL = 1.08500 + 0.00050 = 1.08550.
    """
    terminal_id = "term_adjust_sell"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08520,  # 2.0 pips SL distance
        take_profit=1.07500,
        signal_id=902,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True
    assert len(dispatcher.dispatched_intents) == 1

    dispatched = dispatcher.dispatched_intents[0]
    assert abs(dispatched.stop_loss - 1.08550) < 1e-5  # Exactly widened to 5.0 pips!


@pytest.mark.asyncio
async def test_stops_level_adjust_widens_take_profit_to_minimum():
    """
    BUY: Entry = 1.08500, tight TP = 1.08520 (2 pips), Stops Level = 5 pips (0.00050).
    Expected adjusted TP = 1.08500 + 0.00050 = 1.08550.
    """
    terminal_id = "term_adjust_tp"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=1.08520,  # 2.0 pips TP distance (breaches 5.0 pip min)
        signal_id=903,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True
    assert len(dispatcher.dispatched_intents) == 1

    dispatched = dispatcher.dispatched_intents[0]
    assert abs(dispatched.take_profit - 1.08550) < 1e-5


# =============================================================================
# Rejection Tests (SKIP_TRADE Policy)
# =============================================================================

@pytest.mark.asyncio
async def test_stops_level_skip_trade_policy_rejects_tight_sl():
    """
    Under FreezePolicy.SKIP_TRADE, an order with SL distance < stops_level
    must be immediately rejected with SKIP_STOPS_LEVEL and zero broker dispatch.
    """
    terminal_id = "term_skip_sl"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.SKIP_TRADE,
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08470,  # 3.0 pips SL distance (breaches 5.0 pip min)
        signal_id=904,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason is not None
    assert res.skip_reason.code in (SKIP_STOPS_LEVEL, "SKIP_STOPS_LEVEL_BREACH")
    assert "Stop Loss distance" in res.skip_reason.reason
    assert len(dispatcher.dispatched_intents) == 0  # No IPC dispatch!


@pytest.mark.asyncio
async def test_stops_level_skip_trade_policy_rejects_tight_tp():
    """
    Under FreezePolicy.SKIP_TRADE, an order with TP distance < stops_level
    must be immediately rejected with SKIP_STOPS_LEVEL.
    """
    terminal_id = "term_skip_tp"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.SKIP_TRADE,
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.09000,
        take_profit=1.08480,  # 2.0 pips TP distance (breaches 5.0 pip min)
        signal_id=905,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason is not None
    assert res.skip_reason.code in (SKIP_STOPS_LEVEL, "SKIP_STOPS_LEVEL_BREACH")
    assert "Take Profit distance" in res.skip_reason.reason
    assert len(dispatcher.dispatched_intents) == 0


# =============================================================================
# Freeze Level Modification Guard Tests
# =============================================================================

@pytest.mark.asyncio
async def test_freeze_level_blocks_order_modification_near_market():
    """
    Tests Freeze Level guard:
    When current market price is within freeze_level_pips of active SL/TP,
    broker prohibits modifications. Orchestrator preemptively rejects with SKIP_FREEZE_LEVEL.
    """
    terminal_id = "term_freeze_mod"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        freeze_level_pips=5.0,
    ))

    # 1. Open an active position: Ticket 5001 at 1.08500 with SL 1.08450
    open_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08450,
        signal_id=906,
    )
    open_res = await orchestrator.submit_order(terminal_id, open_intent)
    assert open_res.success is True
    ticket = open_res.ticket

    # 2. Market price drifts to 1.08460 (1.0 pip from active SL 1.08450)
    # Inside 5.0 pip Freeze Level!
    await orchestrator.on_price_update("EURUSD", 1.08460, 1.08470)

    # 3. Attempt to modify SL/TP
    mod_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MODIFY_SL_TP,
        volume=1.00,
        price=1.08460,
        stop_loss=1.08400,
        order_ticket=ticket,
        signal_id=907,
    )

    res = await orchestrator.submit_order(terminal_id, mod_intent)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason is not None
    assert res.skip_reason.code in (SKIP_FREEZE_LEVEL, "SKIP_FREEZE_LEVEL_BREACH")
    assert "freeze level" in res.skip_reason.reason.lower()


@pytest.mark.asyncio
async def test_freeze_level_allows_modification_outside_freeze_zone():
    """
    When current market price is safely outside freeze_level_pips of active SL/TP,
    order modification proceeds smoothly.
    """
    terminal_id = "term_freeze_safe"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        freeze_level_pips=5.0,
    ))

    # 1. Open position
    open_res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=1.00,
            price=1.08500,
            stop_loss=1.08300,  # 20 pips away
            signal_id=908,
        ),
    )
    ticket = open_res.ticket

    # 2. Market price is at 1.08500 (20 pips away from SL 1.08300, well outside 5 pips)
    await orchestrator.on_price_update("EURUSD", 1.08500, 1.08510)

    # 3. Modify SL to 1.08350
    mod_res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MODIFY_SL_TP,
            volume=1.00,
            price=1.08500,
            stop_loss=1.08350,
            order_ticket=ticket,
            signal_id=909,
        ),
    )
    assert mod_res.success is True


# =============================================================================
# Precision across Asset Classes (JPY & Metals)
# =============================================================================

@pytest.mark.asyncio
async def test_stops_level_jpy_precision():
    """
    USDJPY (3 digits, 1 pip = 0.01). Stops Level = 4.0 pips (0.040).
    BUY entry at 155.000, requested SL 154.980 (2 pips tight).
    Adjusted SL must widen mathematically to 155.000 - 0.040 = 154.960.
    """
    terminal_id = "term_jpy_test"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=4.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(
            symbol="USDJPY",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=1.00,
            price=155.000,
            stop_loss=154.980,  # 2.0 pips SL distance
            signal_id=910,
        ),
    )
    assert res.success is True
    dispatched = dispatcher.dispatched_intents[-1]
    assert abs(dispatched.stop_loss - 154.960) < 1e-4


@pytest.mark.asyncio
async def test_stops_level_xauusd_gold_precision():
    """
    XAUUSD Gold (2 digits, 1 pip = 0.10). Stops Level = 20.0 pips ($2.00).
    SELL entry at 2650.00, requested SL at 2651.00 ($1.00 / 10 pips tight).
    Adjusted SL must widen mathematically to 2650.00 + 2.00 = 2652.00.
    """
    terminal_id = "term_gold_test"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=20.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(
            symbol="XAUUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.50,
            price=2650.00,
            stop_loss=2651.00,  # 10 pips distance
            signal_id=911,
        ),
    )
    assert res.success is True
    dispatched = dispatcher.dispatched_intents[-1]
    assert abs(dispatched.stop_loss - 2652.00) < 1e-3


# =============================================================================
# Policy Precedence & Account Profile Tests
# =============================================================================

@pytest.mark.asyncio
async def test_account_profile_freeze_policy_precedence():
    """
    AccountProfile freeze_policy overrides TerminalExecutionConfig freeze_policy.
    Terminal config has ADJUST_TO_MINIMUM, but profile specifies SKIP_TRADE.
    Breaching order must be rejected.
    """
    terminal_id = "term_precedence"
    dispatcher = MockDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))

    profile = AccountProfile(
        account_id="acc_strict_freeze",
        equity=10000.0,
        freeze_policy=FreezePolicy.SKIP_TRADE,
    )

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08470,  # 3.0 pips SL distance (breaches 5.0)
        signal_id=912,
    )

    res = await orchestrator.submit_order(terminal_id, intent, account_profile=profile)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason.code in (SKIP_STOPS_LEVEL, "SKIP_STOPS_LEVEL_BREACH")


# =============================================================================
# Performance Latency Benchmark (<100 microseconds)
# =============================================================================

def test_freeze_level_guard_latency_benchmark():
    """
    Validates that FreezeLevelGuard.evaluate runs entirely in memory and satisfies
    the strict performance budget (< 100 microseconds, well within the < 5ms total engine budget).
    """
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=MockDispatcher("term_bench"))
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id="term_bench",
        stops_level_pips=5.0,
        freeze_policy=FreezePolicy.ADJUST_TO_MINIMUM,
    ))
    guard = FreezeLevelGuard(orchestrator=orchestrator)

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08480,
        take_profit=1.09500,
        signal_id=999,
    )

    # Warmup
    for _ in range(10):
        guard.evaluate("term_bench", intent)

    # Benchmark 1000 evaluations
    start = time.perf_counter()
    iterations = 1000
    for _ in range(iterations):
        guard.evaluate("term_bench", intent)
    elapsed_total = time.perf_counter() - start

    avg_us = (elapsed_total / iterations) * 1_000_000.0
    # Average execution time must be under 100 microseconds
    assert avg_us < 100.0, f"FreezeLevelGuard evaluation took {avg_us:.2f} us, expected < 100 us"
