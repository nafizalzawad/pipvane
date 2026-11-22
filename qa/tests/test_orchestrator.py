"""
Pipvane v3: Sprint 12 - Trade Orchestrator & Execution Management Test Suite.

Quality & Verification Requirements:
1. TP Ladder Test:
   Mock an OrderIntent with a 1.0 Lot size and 3 TPs.
   Assert that the Orchestrator splits this into 3 broker calls of 0.5, 0.3, and 0.2 Lots respectively.
2. Virtual Netting Test:
   Simulate an MT5 Netting account (supports_hedging = 0).
   Assert that the Orchestrator holds the TPs virtually in memory and correctly issues
   a 0.5 Lot ClosePartial command when the mocked market price crosses the TP1 threshold.
3. Break-Even Test:
   Mock price movement crossing the defined Break-Even threshold.
   Assert that the Orchestrator dispatches an OrderModify command updating the SL to the exact Entry Price.
4. Coverage:
   100% test coverage across all trade management functions.
"""

from __future__ import annotations

import os
import sys
import time
import pytest
from typing import List
from unittest.mock import MagicMock

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionStatus,
    ExecutionFeedback,
)
from engine.trade_orchestrator import (
    TradeOrchestrator,
    AccountMode,
    TradeIntent,
    ManagedPosition,
    calculate_ladder_volumes,
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
)
from engine.ipc_dispatcher import IPCDispatcher


# ==============================================================================
# Task 1: TP Ladder Tests
# ==============================================================================

def test_tp_ladder_split_1_lot_into_3_broker_calls():
    """
    Task 1: Mock an OrderIntent with a 1.0 Lot size and 3 TPs.
    Assert that the Orchestrator splits this into 3 broker calls of 0.5, 0.3, and 0.2 Lots respectively.
    """
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)

    # 1. Mock OrderIntent with 1.0 Lot and 3 Take Profit levels
    tp_targets = [1.09000, 1.09500, 1.10000]
    master_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09000,
        slippage_cap_pips=1.5,
        magic_number=888777,
        signal_id=12345678,
    )

    # 2. Decompose and orchestrate via TradeOrchestrator
    broker_calls = orchestrator.orchestrate_order(
        intent=master_intent,
        tp_ladder=tp_targets,
    )

    # 3. Assertions on broker calls
    # Exactly 3 broker calls generated
    assert len(broker_calls) == 3, f"Expected 3 broker calls, got {len(broker_calls)}"

    # Exact volume split assertions: 0.5, 0.3, and 0.2 Lots respectively
    assert broker_calls[0].volume == pytest.approx(0.50, 1e-4), f"Call 1 volume was {broker_calls[0].volume}, expected 0.50"
    assert broker_calls[1].volume == pytest.approx(0.30, 1e-4), f"Call 2 volume was {broker_calls[1].volume}, expected 0.30"
    assert broker_calls[2].volume == pytest.approx(0.20, 1e-4), f"Call 3 volume was {broker_calls[2].volume}, expected 0.20"

    # Total volume must sum to exact 1.0 Lot
    total_volume = sum(c.volume for c in broker_calls)
    assert total_volume == pytest.approx(1.00, 1e-4)

    # Take profit targets assigned sequentially to each sub-order
    assert broker_calls[0].take_profit == 1.09000
    assert broker_calls[1].take_profit == 1.09500
    assert broker_calls[2].take_profit == 1.10000

    # Common parameters preserved across all sub-orders
    for call in broker_calls:
        assert call.symbol == "EURUSD"
        assert call.side == OrderSide.BUY
        assert call.action_type == ActionType.MARKET_ENTRY
        assert call.price == 1.08500
        assert call.stop_loss == 1.08000
        assert call.magic_number == 888777


def test_execution_orchestrator_decompose_tp_ladder():
    """
    Task 1: Verify ExecutionOrchestrator's native decompose_tp_ladder method
    produces the exact 0.5, 0.3, 0.2 lot split for 1.0 Lot across 3 TPs.
    """
    dispatcher = IPCDispatcher(use_mock_buffers=True)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=0.0,
    )

    tps = [1.09000, 1.09500, 1.10000]
    sub_intents = orchestrator.decompose_tp_ladder(intent, tp_ladder=tps)

    assert len(sub_intents) == 3
    assert [sub.volume for sub in sub_intents] == [0.50, 0.30, 0.20]
    assert [sub.take_profit for sub in sub_intents] == [1.09000, 1.09500, 1.10000]


def test_calculate_ladder_volumes_custom_ratios_and_edge_cases():
    """Verify volume calculation math, custom weight normalization, and rounding integrity."""
    # 3 TPs default: [0.5, 0.3, 0.2]
    assert calculate_ladder_volumes(1.0, [0.5, 0.3, 0.2]) == [0.50, 0.30, 0.20]
    assert calculate_ladder_volumes(2.0, [0.5, 0.3, 0.2]) == [1.00, 0.60, 0.40]

    # Non-round volume (e.g. 0.05 lots over 3 TPs)
    vols = calculate_ladder_volumes(0.05, [0.5, 0.3, 0.2])
    assert sum(vols) == pytest.approx(0.05, 1e-4)

    # 2 TPs equal split
    assert calculate_ladder_volumes(1.0, [0.5, 0.5]) == [0.50, 0.50]

    # Single level / empty
    assert calculate_ladder_volumes(1.0, []) == [1.00]
    assert calculate_ladder_volumes(1.0, [1.0]) == [1.00]


# ==============================================================================
# Task 2: Virtual Netting Tests (MT5 Netting Simulation)
# ==============================================================================

def test_virtual_netting_holds_tps_and_issues_close_partial_at_tp1():
    """
    Task 2: Simulate an MT5 Netting account.
    Assert that the Orchestrator holds the TPs virtually and correctly issues
    a 0.5 Lot ClosePartial command when the mocked market price crosses the TP1 threshold.
    """
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)

    # 1. Initial 1.0 Lot BUY OrderIntent with 3 TPs on an MT5 Netting account
    master_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09000,
        magic_number=777888,
        signal_id=98765432,
    )

    tp_targets = [1.09000, 1.09500, 1.10000]

    # 2. Orchestrate order in NETTING mode
    initial_calls = orchestrator.orchestrate_order(
        intent=master_intent,
        tp_ladder=tp_targets,
        account_mode=AccountMode.NETTING,
    )

    # In Netting mode, exactly 1 broker call is dispatched initially (full volume 1.0 Lot)
    assert len(initial_calls) == 1
    assert initial_calls[0].volume == pytest.approx(1.00, 1e-4)
    # Crucial Assertion: Broker take_profit is held virtually (0.0 on broker to avoid single-position overwrite)
    assert initial_calls[0].take_profit == 0.0

    # 3. Simulate broker fill confirmation (assign ticket #884411)
    orchestrator.on_execution_feedback(
        terminal_id="default_terminal",
        feedback=ExecutionFeedback(
            signal_id=98765432,
            order_ticket=884411,
            execution_status=ExecutionStatus.FILLED,
            broker_retcode=10009,
            requested_volume=1.00,
            executed_volume=1.00,
            requested_price=1.08500,
            executed_price=1.08500,
        ),
    )

    pos = orchestrator.get_position(884411)
    assert pos is not None
    assert pos.current_volume == 1.00
    assert pos.tp_ladder == tp_targets
    assert pos.tp_volumes == [0.50, 0.30, 0.20]
    assert pos.tp_triggered == [False, False, False]

    # 4. Price Simulation 1: Market moves below TP1 (e.g. 1.08800) -> No action
    commands_p1 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.08800, ask=1.08810)
    assert len(commands_p1) == 0
    assert pos.current_volume == 1.00

    # 5. Price Simulation 2: Market crosses TP1 threshold (1.09000) -> e.g. Bid = 1.09050
    commands_tp1 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.09050, ask=1.09060)

    # ASSERTION: Orchestrator correctly issues a 0.5 Lot ClosePartial command!
    assert len(commands_tp1) >= 1
    partial_close_cmd = [c for c in commands_tp1 if c.action_type in (ActionType.PARTIAL_CLOSE, ActionType.FULL_CLOSE)][0]

    assert partial_close_cmd.action_type == ActionType.PARTIAL_CLOSE
    assert partial_close_cmd.volume == pytest.approx(0.50, 1e-4), (
        f"Expected 0.50 Lot ClosePartial command, got {partial_close_cmd.volume}"
    )
    assert partial_close_cmd.order_ticket == 884411
    assert partial_close_cmd.symbol == "EURUSD"

    # Assert position's internal volume is decremented from 1.00 to 0.50 Lots
    assert pos.current_volume == pytest.approx(0.50, 1e-4)
    assert pos.tp_triggered[0] is True
    assert pos.tp_triggered[1] is False


def test_virtual_netting_full_cascade_tp2_and_tp3():
    """
    Task 2: Verify subsequent ticks crossing TP2 and TP3 correctly issue
    0.3 Lot ClosePartial and final 0.2 Lot FullClose respectively.
    """
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)

    pos = ManagedPosition(
        ticket=771122,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=1.00,
        current_volume=1.00,
        entry_price=1.08500,
        stop_loss=1.08000,
        tp_ladder=[1.09000, 1.09500, 1.10000],
        tp_volumes=[0.50, 0.30, 0.20],
        tp_triggered=[False, False, False],
        account_mode=AccountMode.NETTING,
    )
    orchestrator.register_open_position(pos)

    # 1. Trigger TP1 (bid 1.09020)
    cmds_1 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.09020, ask=1.09030)
    close_1 = [c for c in cmds_1 if c.action_type == ActionType.PARTIAL_CLOSE][0]
    assert close_1.volume == pytest.approx(0.50, 1e-4)
    assert pos.current_volume == pytest.approx(0.50, 1e-4)

    # 2. Trigger TP2 (bid 1.09510) -> Expect 0.30 Lot ClosePartial
    cmds_2 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.09510, ask=1.09520)
    close_2 = [c for c in cmds_2 if c.action_type == ActionType.PARTIAL_CLOSE][0]
    assert close_2.volume == pytest.approx(0.30, 1e-4)
    assert pos.current_volume == pytest.approx(0.20, 1e-4)

    # 3. Trigger TP3 (bid 1.10050) -> Expect final 0.20 Lot Close
    cmds_3 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.10050, ask=1.10060)
    close_3 = [c for c in cmds_3 if c.action_type in (ActionType.FULL_CLOSE, ActionType.PARTIAL_CLOSE)][0]
    assert close_3.volume == pytest.approx(0.20, 1e-4)
    assert pos.current_volume == 0.0
    assert pos.is_closed is True


def test_virtual_netting_sell_order_execution():
    """Verify virtual netting logic correctly executes for SELL orders (bid vs ask direction)."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)

    pos = ManagedPosition(
        ticket=662211,
        symbol="GBPUSD",
        side=OrderSide.SELL,
        initial_volume=1.00,
        current_volume=1.00,
        entry_price=1.28000,
        stop_loss=1.28500,
        tp_ladder=[1.27500, 1.27000, 1.26500],
        tp_volumes=[0.50, 0.30, 0.20],
        tp_triggered=[False, False, False],
        account_mode=AccountMode.NETTING,
    )
    orchestrator.register_open_position(pos)

    # In SELL, market price to close is the ASK. Crossing TP1 requires Ask <= 1.27500.
    # Price above TP1 -> No action
    assert len(orchestrator.on_price_tick("GBPUSD", bid=1.27550, ask=1.27560)) == 0

    # Price crosses TP1: Ask = 1.27480
    cmds = orchestrator.on_price_tick("GBPUSD", bid=1.27470, ask=1.27480)
    assert len(cmds) >= 1
    close_cmd = [c for c in cmds if c.action_type in (ActionType.PARTIAL_CLOSE, ActionType.FULL_CLOSE)][0]
    assert close_cmd.volume == pytest.approx(0.50, 1e-4)
    assert pos.current_volume == pytest.approx(0.50, 1e-4)


# ==============================================================================
# Task 3: Break-Even Tests
# ==============================================================================

def test_break_even_modifies_sl_to_exact_entry_price_when_threshold_crossed():
    """
    Task 3: Mock price movement crossing the defined Break-Even threshold.
    Assert that the Orchestrator dispatches an OrderModify command updating the SL to the exact Entry Price.
    """
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)

    # 1. Managed position: BUY EURUSD at Entry 1.08500 with initial SL 1.08000
    # Break-Even threshold defined at price 1.08700 (+20 pips)
    pos = ManagedPosition(
        ticket=100200,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=0.50,
        current_volume=0.50,
        entry_price=1.08500,
        stop_loss=1.08000,
        tp_ladder=[1.09500],
        tp_volumes=[0.50],
        tp_triggered=[False],
        account_mode=AccountMode.HEDGING,
        break_even_enabled=True,
        break_even_trigger_price=1.08700,
        break_even_triggered=False,
    )
    orchestrator.register_open_position(pos)

    # 2. Price movement 1: Price below threshold (Bid = 1.08680) -> No modify
    cmds_1 = orchestrator.on_price_tick(symbol="EURUSD", bid=1.08680, ask=1.08690)
    assert len(cmds_1) == 0
    assert pos.break_even_triggered is False
    assert pos.stop_loss == 1.08000

    # 3. Price movement 2: Price crosses Break-Even threshold (Bid = 1.08720 >= 1.08700)
    cmds_be = orchestrator.on_price_tick(symbol="EURUSD", bid=1.08720, ask=1.08730)

    # ASSERTION: Orchestrator dispatches an OrderModify command!
    assert len(cmds_be) == 1
    modify_cmd = cmds_be[0]

    assert modify_cmd.action_type == ActionType.MODIFY_SL_TP
    assert modify_cmd.order_ticket == 100200
    assert modify_cmd.symbol == "EURUSD"

    # ASSERTION: Stop Loss is updated to the EXACT Entry Price!
    assert modify_cmd.stop_loss == pytest.approx(1.08500, 1e-5), (
        f"Expected SL to be set to exact Entry Price (1.08500), got {modify_cmd.stop_loss}"
    )

    # Assert position internal state is updated
    assert pos.break_even_triggered is True
    assert pos.stop_loss == pytest.approx(1.08500, 1e-5)

    # 4. Subsequent higher ticks should not re-trigger Break-Even (idempotent)
    cmds_subsequent = orchestrator.on_price_tick(symbol="EURUSD", bid=1.08900, ask=1.08910)
    assert len(cmds_subsequent) == 0


def test_break_even_pips_gain_trigger():
    """Verify break-even triggers when price gain in pips meets or exceeds break_even_pips threshold."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)

    # Position with break_even_pips = 15.0 pips (15 * 0.00010 = +0.00150)
    # Entry: 1.08500 -> BE triggers when Bid >= 1.08650
    pos = ManagedPosition(
        ticket=100300,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=0.20,
        current_volume=0.20,
        entry_price=1.08500,
        stop_loss=1.08000,
        tp_ladder=[1.09500],
        break_even_enabled=True,
        break_even_pips=15.0,
        break_even_triggered=False,
    )
    orchestrator.register_open_position(pos)

    # Gain of 10 pips (1.08600) -> No trigger
    assert len(orchestrator.on_price_tick("EURUSD", bid=1.08600, ask=1.08610)) == 0

    # Gain of 15.5 pips (1.08655) -> Trigger BE
    cmds = orchestrator.on_price_tick("EURUSD", bid=1.08655, ask=1.08665)
    assert len(cmds) == 1
    assert cmds[0].action_type == ActionType.MODIFY_SL_TP
    assert cmds[0].stop_loss == pytest.approx(1.08500, 1e-5)
    assert pos.break_even_triggered is True


def test_break_even_for_sell_positions():
    """Verify break-even modification dispatches correctly for SELL orders."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)

    # SELL GBPUSD: Entry 1.30000, SL 1.30500, BE threshold at 1.29800
    pos = ManagedPosition(
        ticket=200100,
        symbol="GBPUSD",
        side=OrderSide.SELL,
        initial_volume=0.40,
        current_volume=0.40,
        entry_price=1.30000,
        stop_loss=1.30500,
        tp_ladder=[1.29000],
        break_even_enabled=True,
        break_even_trigger_price=1.29800,
        break_even_triggered=False,
    )
    orchestrator.register_open_position(pos)

    # Ask above threshold (1.29820) -> No trigger
    assert len(orchestrator.on_price_tick("GBPUSD", bid=1.29810, ask=1.29820)) == 0

    # Ask crosses below threshold (1.29790) -> Trigger BE
    cmds = orchestrator.on_price_tick("GBPUSD", bid=1.29780, ask=1.29790)
    assert len(cmds) == 1
    modify_cmd = cmds[0]
    assert modify_cmd.action_type == ActionType.MODIFY_SL_TP
    assert modify_cmd.stop_loss == pytest.approx(1.30000, 1e-5)
    assert pos.break_even_triggered is True


def test_break_even_with_positive_offset():
    """Verify break-even with offset (e.g. +1.0 pip to cover broker commission)."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)

    pos = ManagedPosition(
        ticket=100400,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=0.10,
        current_volume=0.10,
        entry_price=1.08500,
        stop_loss=1.08000,
        tp_ladder=[1.09500],
        break_even_enabled=True,
        break_even_trigger_price=1.08700,
        be_offset_pips=1.0,  # Entry + 1 pip (1.08510)
        break_even_triggered=False,
    )
    orchestrator.register_open_position(pos)

    cmds = orchestrator.on_price_tick("EURUSD", bid=1.08710, ask=1.08720)
    assert len(cmds) == 1
    # SL should be set to 1.08500 + 0.00010 = 1.08510
    assert cmds[0].stop_loss == pytest.approx(1.08510, 1e-5)


# ==============================================================================
# Additional Integration & Listener Coverage
# ==============================================================================

def test_trade_orchestrator_broker_listener_notifications():
    """Verify subscribers receive all dispatched broker calls."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)
    dispatched_list: List[OrderIntent] = []

    orchestrator.add_broker_call_listener(lambda order: dispatched_list.append(order))

    intent = TradeIntent(
        symbol="XAUUSD",
        side=OrderSide.BUY,
        volume=1.00,
        price=2350.00,
        tp_ladder=[2360.0, 2370.0, 2380.0],
    )

    calls = orchestrator.orchestrate_order(intent)
    assert len(calls) == 3
    assert len(dispatched_list) == 3
    assert len(orchestrator.dispatched_calls) == 3
    assert [c.volume for c in dispatched_list] == [0.50, 0.30, 0.20]


def test_calculate_ladder_volumes_zero_sum_ratios():
    """Verify fallback when ratios sum to 0 or negative, and rounding remainder allocation."""
    vols = calculate_ladder_volumes(1.0, [0.0, 0.0])
    assert len(vols) == 2
    assert sum(vols) == pytest.approx(1.0, 1e-4)

    # 1.0 divided across 3 equal parts tests remainder assignment (line 140)
    vols_3 = calculate_ladder_volumes(1.0, [1.0, 1.0, 1.0])
    assert sum(vols_3) == pytest.approx(1.0, 1e-4)
    assert vols_3[0] == 0.34
    assert vols_3[1] == 0.33
    assert vols_3[2] == 0.33


def test_on_price_tick_ignores_unmatched_or_closed_positions():
    """Verify on_price_tick skips positions for different symbols or closed positions."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)
    pos = ManagedPosition(
        ticket=789,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=1.0,
        current_volume=1.0,
        entry_price=1.0850,
        stop_loss=1.0800,
        tp_ladder=[1.0900],
    )
    orchestrator.register_open_position(pos)
    # Ticking a different symbol skips EURUSD position
    assert len(orchestrator.on_price_tick("USDJPY", bid=155.0, ask=155.01)) == 0
    # Closed position is also skipped
    pos.is_closed = True
    assert len(orchestrator.on_price_tick("EURUSD", bid=1.0910, ask=1.0911)) == 0


def test_trade_orchestrator_hedging_no_tp():
    """Verify single broker call is created in Hedging mode when no TP ladder is provided."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)
    intent = TradeIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=1.00,
        price=1.0850,
        stop_loss=1.0800,
        tp_ladder=[],
    )
    calls = orchestrator.orchestrate_order(intent)
    assert len(calls) == 1
    assert calls[0].volume == 1.00
    assert calls[0].take_profit == 0.0


def test_trade_orchestrator_custom_pip_sizes_and_get_position():
    """Verify custom pip size configuration and get_position lookup."""
    orchestrator = TradeOrchestrator(
        account_mode=AccountMode.HEDGING,
        pip_sizes={"BTCUSD": 1.0, "ETHUSD": 0.1}
    )
    assert orchestrator.get_pip_size("BTCUSD") == 1.0
    assert orchestrator.get_pip_size("ETHUSD") == 0.1

    pos = ManagedPosition(
        ticket=555123,
        symbol="BTCUSD",
        side=OrderSide.BUY,
        initial_volume=0.5,
        current_volume=0.5,
        entry_price=65000.0,
        stop_loss=64000.0,
        tp_ladder=[70000.0],
    )
    orchestrator.register_open_position(pos)
    assert orchestrator.get_position(555123) is pos
    assert orchestrator.get_position(999999) is None


def test_trade_orchestrator_on_execution_feedback_existing_ticket():
    """Verify on_execution_feedback updates an existing ticket position."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)
    pos = ManagedPosition(
        ticket=12345,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=1.0,
        current_volume=1.0,
        entry_price=1.0800,
        stop_loss=1.0750,
        tp_ladder=[1.0900],
    )
    orchestrator.register_open_position(pos)

    fb = ExecutionFeedback(
        signal_id=999,
        order_ticket=12345,
        execution_status=ExecutionStatus.FILLED,
        executed_price=1.0805,
        executed_volume=1.0,
    )
    orchestrator.on_execution_feedback("term_1", fb)
    assert pos.entry_price == 1.0805


def test_trade_orchestrator_listener_exception_and_dispatcher():
    """Verify listener exceptions are swallowed gracefully and IPCDispatcher is notified."""
    mock_dispatcher = MagicMock()
    mock_dispatcher.can_route_to_terminal.return_value = True

    orchestrator = TradeOrchestrator(
        account_mode=AccountMode.HEDGING,
        dispatcher=mock_dispatcher,
        terminal_id="mt5_term_1",
    )

    def faulty_listener(order):
        raise ValueError("Simulated subscriber error")

    orchestrator.add_broker_call_listener(faulty_listener)

    intent = TradeIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=0.5,
        tp_ladder=[1.0900],
    )
    calls = orchestrator.orchestrate_order(intent)
    assert len(calls) == 1
    mock_dispatcher.dispatch_order_intent.assert_called_once_with("mt5_term_1", calls[0])


def test_trade_orchestrator_tp1_automatic_break_even():
    """Verify automatic break-even activation triggered by TP1 hit without explicit trigger price."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.NETTING)
    pos = ManagedPosition(
        ticket=45678,
        symbol="EURUSD",
        side=OrderSide.BUY,
        initial_volume=1.0,
        current_volume=1.0,
        entry_price=1.0850,
        stop_loss=1.0800,
        tp_ladder=[1.0900, 1.0950],
        tp_volumes=[0.5, 0.5],
        tp_triggered=[False, False],
        break_even_enabled=True,
        break_even_trigger_price=None,
        break_even_pips=None,
        break_even_triggered=False,
        account_mode=AccountMode.NETTING,
    )
    orchestrator.register_open_position(pos)

    # Tick crossing TP1 (1.0905)
    cmds = orchestrator.on_price_tick("EURUSD", bid=1.0905, ask=1.0906)
    # Should produce 2 commands: ClosePartial for TP1, and MODIFY_SL_TP for Break-Even
    assert len(cmds) == 2
    actions = [c.action_type for c in cmds]
    assert ActionType.PARTIAL_CLOSE in actions
    assert ActionType.MODIFY_SL_TP in actions
    modify_cmd = [c for c in cmds if c.action_type == ActionType.MODIFY_SL_TP][0]
    assert modify_cmd.stop_loss == pytest.approx(1.0850, 1e-5)
    assert pos.break_even_triggered is True


def test_trade_orchestrator_intent_account_mode_override():
    """Verify that account_mode specified in TradeIntent overrides orchestrator default."""
    orchestrator = TradeOrchestrator(account_mode=AccountMode.HEDGING)
    intent = TradeIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=1.0,
        price=1.0850,
        tp_ladder=[1.0900, 1.0950, 1.1000],
        account_mode=AccountMode.NETTING,  # Override to netting
    )
    calls = orchestrator.orchestrate_order(intent)
    # In Netting mode, single broker call is generated with 1.0 Lot and take_profit = 0.0
    assert len(calls) == 1
    assert calls[0].volume == 1.00
    assert calls[0].take_profit == 0.0

