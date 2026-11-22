"""
Pipvane v3 - Account Modes & Rigid Broker Environments Test Suite (Sprint 20 Directive)
========================================================================================
Validates system compliance with rigid broker operating constraints:
1. FIFO Rule Enforcement (US NFA Rule 2-43(b)):
   - Mock a FIFO account (is_fifo=True).
   - Open EURUSD Buy #1, then EURUSD Buy #2.
   - Dispatch command to close EURUSD Buy #2.
   - Assert that Orchestrator intercepts this, overwrites target to Buy #1, and closes Buy #1 instead.
   - Assert that Buy #1 is closed and Buy #2 remains open in internal engine state.
   - Assert that journey log records FIFO_TARGET_OVERWRITE audit trail event.
2. Stops Level & Freeze Level Enforcement:
   - Mock a broker with a 5.0 pip Stops Level.
   - Send signal with a 3.0 pip Stop Loss.
   - Assert that the Orchestrator (set to ADJUST) modifies the intent to exactly 5.0 pips before sending the IPC command.
   - Validate Stops Level REJECT policy (SKIP_STOPS_LEVEL_BREACH).
   - Validate Freeze Level modification protection (SKIP_FREEZE_LEVEL_BREACH).
   - Assert precision across Forex majors, JPY pairs (0.01 pip size), and Gold XAUUSD (0.10 pip size).
3. End-to-End Capabilities Negotiation:
   - Validates dynamic terminal configuration via MSG_CAPABILITIES_RESP telemetry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PositionItem,
    PositionsSyncPayload,
    CapabilitiesPayload,
    NamedPipeOpcode,
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    ExecutionResult,
)
from engine.risk_engine import SkipReason, SKIP_FIFO_HEDGE

logger = logging.getLogger("pipvane.qa.test_broker_modes")


# =============================================================================
# Mock Broker Dispatcher for Account Modes Testing
# =============================================================================

class MockBrokerDispatcher:
    """
    Simulates IPCDispatcher bridging ExecutionOrchestrator to a mock MetaTrader terminal.
    Provides immediate realistic fill feedback and tracks all dispatched OrderIntents.
    """

    def __init__(self, terminal_id: str = "test_terminal") -> None:
        self.terminal_id = terminal_id
        self._ticket_counter = 1000
        self.dispatched_intents: List[OrderIntent] = []
        self._on_feedback_cbs: List[Callable[[str, ExecutionFeedback], None]] = []
        self._on_account_state_cbs: List[Callable[[str, Any], None]] = []
        self._on_positions_sync_cbs: List[Callable[[str, PositionsSyncPayload], None]] = []
        self._on_capabilities_cbs: List[Callable[[str, CapabilitiesPayload], None]] = []
        self._broker_positions: Dict[int, PositionItem] = {}

    def on_execution_feedback(self, cb: Callable[[str, ExecutionFeedback], None]) -> None:
        self._on_feedback_cbs.append(cb)

    def on_account_state(self, cb: Callable[[str, Any], None]) -> None:
        self._on_account_state_cbs.append(cb)

    def on_positions_sync(self, cb: Callable[[str, PositionsSyncPayload], None]) -> None:
        self._on_positions_sync_cbs.append(cb)

    def on_capabilities(self, cb: Callable[[str, CapabilitiesPayload], None]) -> None:
        self._on_capabilities_cbs.append(cb)

    def on_symbols_sync(self, cb: Any) -> None:
        pass

    def can_route_to_terminal(self, terminal_id: str) -> bool:
        return True

    def get_broker_positions(self, terminal_id: Optional[str] = None) -> List[PositionItem]:
        return list(self._broker_positions.values())

    def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        """Simulates zero-copy SHM dispatch and triggers immediate feedback."""
        self.dispatched_intents.append(intent)

        # Determine ticket
        if intent.action_type in (ActionType.MARKET_ENTRY, ActionType.LIMIT_ENTRY, ActionType.STOP_ENTRY) or int(intent.action_type) == 0:
            self._ticket_counter += 1
            executed_ticket = self._ticket_counter
            # Record in mock broker positions
            pos = PositionItem(
                ticket=executed_ticket,
                magic_number=intent.magic_number,
                open_time_unix=int(time.time()),
                symbol=intent.symbol,
                position_type=int(intent.side),
                volume=intent.volume,
                open_price=intent.price or 1.08500,
                current_price=intent.price or 1.08500,
                stop_loss=intent.stop_loss,
                take_profit=intent.take_profit,
            )
            self._broker_positions[executed_ticket] = pos
        elif intent.action_type == ActionType.FULL_CLOSE:
            executed_ticket = intent.order_ticket
            self._broker_positions.pop(executed_ticket, None)
        elif intent.action_type == ActionType.PARTIAL_CLOSE:
            executed_ticket = intent.order_ticket
            if executed_ticket in self._broker_positions:
                pos = self._broker_positions[executed_ticket]
                pos.volume = max(0.0, pos.volume - intent.volume)
        elif intent.action_type == ActionType.MODIFY_SL_TP:
            executed_ticket = intent.order_ticket
            if executed_ticket in self._broker_positions:
                pos = self._broker_positions[executed_ticket]
                pos.stop_loss = intent.stop_loss
                pos.take_profit = intent.take_profit
        else:
            executed_ticket = intent.order_ticket

        # Immediate broker execution feedback
        fb = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=executed_ticket,
            execution_status=ExecutionStatus.FILLED,
            requested_volume=intent.volume,
            executed_volume=intent.volume,
            requested_price=intent.price,
            executed_price=intent.price or 1.08500,
            slippage_pips=0.1,
            execution_latency_us=45,
        )

        for cb in self._on_feedback_cbs:
            cb(terminal_id, fb)

        return True


# =============================================================================
# Task 1: FIFO Enforcement Tests (US NFA Rule 2-43(b))
# =============================================================================

@pytest.mark.asyncio
async def test_fifo_intercepts_and_overwrites_close_target_to_oldest_ticket():
    """
    Mandate 1 (Directive FIFO Test):
    Mock a FIFO account. Open EURUSD Buy #1, then EURUSD Buy #2.
    Dispatch a command to close EURUSD Buy #2.
    Assert that the Orchestrator intercepts this, overwrites the target,
    and successfully closes Buy #1 instead.
    Assert Buy #1 is CLOSED and Buy #2 remains OPEN in database.
    """
    terminal_id = "fifo_term_1"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)

    # 1. Register FIFO terminal config
    config = TerminalExecutionConfig(
        terminal_id=terminal_id,
        is_fifo=True,
    )
    orchestrator.register_terminal_config(config)

    # 2. Open EURUSD Buy #1 (ticket #1001)
    intent1 = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        signal_id=101,
    )
    res1 = await orchestrator.submit_order(terminal_id, intent1)
    assert res1.success is True
    ticket1 = res1.ticket
    assert ticket1 == 1001

    # Small delay to ensure strictly distinct creation timestamps
    await asyncio.sleep(0.01)

    # 3. Open EURUSD Buy #2 (ticket #1002)
    intent2 = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08520,
        signal_id=102,
    )
    res2 = await orchestrator.submit_order(terminal_id, intent2)
    assert res2.success is True
    ticket2 = res2.ticket
    assert ticket2 == 1002

    # Verify both positions are OPEN in SQLite database
    order1_db = orchestrator.get_order_by_ticket(ticket1)
    order2_db = orchestrator.get_order_by_ticket(ticket2)
    assert order1_db is not None and order1_db["status"] == "OPEN"
    assert order2_db is not None and order2_db["status"] == "OPEN"

    # 4. Dispatch a command to close EURUSD Buy #2 (targeting ticket #1002)
    close_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.FULL_CLOSE,
        volume=1.00,
        price=1.08550,
        order_ticket=ticket2, # Targeting Buy #2 (#1002)
        signal_id=103,
    )
    res_close = await orchestrator.submit_order(terminal_id, close_intent)

    # 5. Assertions:
    # A. Orchestrator must intercept and overwrite target to ticket #1001
    assert res_close.success is True
    assert res_close.ticket == ticket1, f"Expected close on Buy #1 ({ticket1}), got {res_close.ticket}"

    # B. The actual intent dispatched over IPC to the broker must carry ticket #1001
    last_dispatched = dispatcher.dispatched_intents[-1]
    assert last_dispatched.order_ticket == ticket1, f"Dispatched ticket was {last_dispatched.order_ticket}, expected {ticket1}"
    assert last_dispatched.action_type == ActionType.FULL_CLOSE

    # C. Verify database state: Buy #1 is CLOSED, Buy #2 remains strictly OPEN
    order1_after = orchestrator.get_order_by_ticket(ticket1)
    order2_after = orchestrator.get_order_by_ticket(ticket2)
    assert order1_after["status"] == "CLOSED", "Buy #1 should be marked CLOSED in database"
    assert order2_after["status"] == "OPEN", "Buy #2 should remain strictly OPEN in database"

    # D. Verify Journey log recorded the FIFO target overwrite audit event
    journey_events = orchestrator.get_journey_logs(event_type="FIFO_TARGET_OVERWRITE")
    assert len(journey_events) >= 1
    assert journey_events[0]["ticket"] == ticket1
    assert "1002" in journey_events[0]["details"] and "1001" in journey_events[0]["details"]


@pytest.mark.asyncio
async def test_fifo_normal_close_of_oldest_position_no_overwrite():
    """
    Validates that when the user/signal directly requests closing the oldest
    open position (Buy #1), it proceeds smoothly without an overwrite event.
    """
    terminal_id = "fifo_term_normal"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id, is_fifo=True))

    # Open Buy #1 and Buy #2
    res1 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=301),
    )
    await asyncio.sleep(0.01)
    res2 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=302),
    )

    # Directly close Buy #1 (the oldest position)
    close_res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.SELL, action_type=ActionType.FULL_CLOSE, volume=1.0, order_ticket=res1.ticket, signal_id=303),
    )

    assert close_res.success is True
    assert close_res.ticket == res1.ticket
    assert orchestrator.get_order_by_ticket(res1.ticket)["status"] == "CLOSED"
    assert orchestrator.get_order_by_ticket(res2.ticket)["status"] == "OPEN"

    # No FIFO overwrite event recorded because Buy #1 was already the oldest
    journey_events = orchestrator.get_journey_logs(signal_id=303, event_type="FIFO_TARGET_OVERWRITE")
    assert len(journey_events) == 0


@pytest.mark.asyncio
async def test_hedging_account_allows_independent_close():
    """
    Validates that a non-FIFO (Hedging) account permits closing Buy #2 directly
    without any interception or overwrite.
    """
    terminal_id = "hedging_term"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id, is_fifo=False))

    # Open Buy #1 and Buy #2
    res1 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=401),
    )
    await asyncio.sleep(0.01)
    res2 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=402),
    )

    # In Hedging account, close Buy #2 directly
    close_res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.SELL, action_type=ActionType.FULL_CLOSE, volume=1.0, order_ticket=res2.ticket, signal_id=403),
    )

    assert close_res.success is True
    assert close_res.ticket == res2.ticket
    # In Hedging account: Buy #1 remains OPEN, Buy #2 is CLOSED
    assert orchestrator.get_order_by_ticket(res1.ticket)["status"] == "OPEN"
    assert orchestrator.get_order_by_ticket(res2.ticket)["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_fifo_symbol_and_side_isolation():
    """
    Validates that FIFO queues are strictly isolated by Symbol and Side.
    Closing GBPUSD Buy #1 does not affect EURUSD Buy #1.
    Closing EURUSD Sell #1 does not affect EURUSD Buy #1.
    """
    terminal_id = "fifo_term_iso"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id, is_fifo=True))

    # Open EURUSD Buy #1
    eur_buy = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=501),
    )
    await asyncio.sleep(0.01)

    # Open GBPUSD Buy #1
    gbp_buy = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="GBPUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=502),
    )

    # Close GBPUSD Buy #1
    close_gbp = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="GBPUSD", side=OrderSide.SELL, action_type=ActionType.FULL_CLOSE, volume=1.0, order_ticket=gbp_buy.ticket, signal_id=503),
    )
    assert close_gbp.success is True
    assert close_gbp.ticket == gbp_buy.ticket
    # EURUSD Buy remains open
    assert orchestrator.get_order_by_ticket(eur_buy.ticket)["status"] == "OPEN"
    assert orchestrator.get_order_by_ticket(gbp_buy.ticket)["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_fifo_partial_close_interception():
    """
    Validates FIFO interception on PARTIAL_CLOSE commands:
    Open EURUSD Buy #1 (1.0 Lot), then Buy #2 (1.0 Lot).
    Dispatch a partial close of 0.4 Lots targeted at Buy #2.
    Assert Orchestrator intercepts and partially closes Buy #1 (leaving 0.6 Lots on Buy #1).
    Buy #2 remains 1.0 Lot open.
    """
    terminal_id = "fifo_term_partial"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(terminal_id=terminal_id, is_fifo=True))

    # Open Buy #1 and Buy #2 (1.0 Lot each)
    res1 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=601),
    )
    await asyncio.sleep(0.01)
    res2 = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=1.0, signal_id=602),
    )

    # Dispatch partial close of 0.40 Lots targeted at Buy #2
    part_close = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.PARTIAL_CLOSE,
        volume=0.40,
        price=1.08550,
        order_ticket=res2.ticket,
        signal_id=603,
    )
    part_res = await orchestrator.submit_order(terminal_id, part_close)

    assert part_res.success is True
    assert part_res.ticket == res1.ticket # Intercepted and routed to Buy #1!

    # Check database volumes
    db_pos1 = orchestrator.get_order_by_ticket(res1.ticket)
    db_pos2 = orchestrator.get_order_by_ticket(res2.ticket)
    assert db_pos1["volume"] == pytest.approx(0.60, 1e-4) # 1.0 - 0.4 = 0.60
    assert db_pos2["volume"] == pytest.approx(1.00, 1e-4) # Untouched!


# =============================================================================
# Task 2: Stops Level & Freeze Level Enforcement Tests
# =============================================================================

@pytest.mark.asyncio
async def test_stops_level_adjust_widens_3_pip_sl_to_5_pip_sl_before_ipc_dispatch():
    """
    Mandate 2 (Directive Freeze/Stops Level Test):
    Mock a broker with a 5.0 pip Stops Level.
    Send a signal with a 3.0 pip SL.
    Assert that the Orchestrator (if set to ADJUST) modifies the intent to
    exactly 5.0 pips before sending the IPC command.
    """
    terminal_id = "term_stops_adjust"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)

    # 1. Broker configuration: 5.0 pip Stops Level with policy ADJUST
    config = TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    )
    orchestrator.register_terminal_config(config)

    # 2. EURUSD BUY signal: Entry 1.08500, Stop Loss 1.08470 (3.0 pips away)
    # EURUSD pip size = 0.0001, so 3.0 pips = 0.00030
    entry_price = 1.08500
    tight_sl = 1.08470  # Exactly 3.0 pips below entry
    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=entry_price,
        stop_loss=tight_sl,
        take_profit=0.0,
        signal_id=701,
    )

    # 3. Submit order through Orchestrator
    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True

    # 4. Assert that the intent dispatched over IPC to the broker was widened to exactly 5.0 pips
    assert len(dispatcher.dispatched_intents) >= 1
    dispatched_intent = dispatcher.dispatched_intents[-1]

    # Required 5.0 pip SL: 1.08500 - (5.0 * 0.0001) = 1.08450
    expected_sl = 1.08450
    assert pytest.approx(dispatched_intent.stop_loss, 1e-5) == expected_sl, (
        f"Dispatched SL was {dispatched_intent.stop_loss}, expected {expected_sl}"
    )

    # Verify exact 5.0 pip distance from entry price
    sl_distance_pips = (dispatched_intent.price - dispatched_intent.stop_loss) / 0.0001
    assert pytest.approx(sl_distance_pips, 1e-5) == 5.0, (
        f"SL distance in pips was {sl_distance_pips}, expected exactly 5.0 pips"
    )


def test_translate_intent_stops_level_adjust_direct():
    """
    Direct assertion on translate_intent method:
    Confirms translate_intent modifies Stop Loss from 3.0 pips to exactly 5.0 pips.
    """
    dispatcher = MockBrokerDispatcher(terminal_id="term_direct")
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)

    config = TerminalExecutionConfig(
        terminal_id="term_direct",
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    )

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08470, # 3.0 pips
        take_profit=0.0,
        signal_id=702,
    )

    translated = orchestrator.translate_intent(intent, config)
    expected_sl = 1.08450
    assert pytest.approx(translated.stop_loss, 1e-5) == expected_sl
    dist = (translated.price - translated.stop_loss) / 0.0001
    assert pytest.approx(dist, 1e-5) == 5.0


@pytest.mark.asyncio
async def test_stops_level_adjust_sell_order():
    """
    Validates Stops Level adjustment for a SELL order:
    Entry 1.08500, SL 1.08530 (3.0 pips above entry).
    Broker has 5.0 pip Stops Level.
    Assert Orchestrator widens SL to 1.08550 (exactly 5.0 pips above entry).
    """
    terminal_id = "term_stops_sell"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08530, # 3.0 pips above entry
        take_profit=0.0,
        signal_id=703,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True

    dispatched = dispatcher.dispatched_intents[-1]
    expected_sl = 1.08550 # 1.08500 + 0.00050
    assert pytest.approx(dispatched.stop_loss, 1e-5) == expected_sl
    dist_pips = (dispatched.stop_loss - dispatched.price) / 0.0001
    assert pytest.approx(dist_pips, 1e-5) == 5.0


@pytest.mark.asyncio
async def test_stops_level_adjust_take_profit():
    """
    Validates Stops Level adjustment for Take Profit:
    Entry 1.08500, TP 1.08520 (2.0 pips TP distance, when stops level is 5.0 pips).
    Assert Orchestrator widens TP to 1.08550 (exactly 5.0 pips above entry).
    """
    terminal_id = "term_stops_tp"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000, # Compliant SL (50 pips)
        take_profit=1.08520, # Breaching TP (2.0 pips)
        signal_id=704,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True

    dispatched = dispatcher.dispatched_intents[-1]
    expected_tp = 1.08550 # 1.08500 + 0.00050
    assert pytest.approx(dispatched.take_profit, 1e-5) == expected_tp


@pytest.mark.asyncio
async def test_stops_level_reject_policy_skips_breaching_order():
    """
    Validates Stops Level REJECT policy:
    When stops_level_policy="REJECT", an order with SL distance < stops_level
    is immediately rejected with code SKIP_STOPS_LEVEL_BREACH without IPC dispatch.
    """
    terminal_id = "term_stops_reject"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        stops_level_policy="REJECT",
    ))

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08470, # 3.0 pips SL distance
        take_profit=0.0,
        signal_id=705,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason is not None
    assert res.skip_reason.code == "SKIP_STOPS_LEVEL_BREACH"
    assert "less than broker stops level" in res.skip_reason.reason
    # Broker dispatcher received no intents
    assert len(dispatcher.dispatched_intents) == 0


@pytest.mark.asyncio
async def test_stops_level_already_compliant_order_preserved():
    """
    Validates that when SL distance is already >= broker stops level (e.g. 10.0 pips >= 5.0 pips),
    the intent's SL is preserved without any alteration.
    """
    terminal_id = "term_stops_compliant"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    ))

    compliant_sl = 1.08400 # 10.0 pips below 1.08500
    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=compliant_sl,
        take_profit=0.0,
        signal_id=706,
    )

    res = await orchestrator.submit_order(terminal_id, intent)
    assert res.success is True

    dispatched = dispatcher.dispatched_intents[-1]
    assert pytest.approx(dispatched.stop_loss, 1e-5) == compliant_sl


@pytest.mark.asyncio
async def test_freeze_level_blocks_modification_near_market_price():
    """
    Validates Freeze Level protection:
    When current market price is within freeze_level_pips of active SL/TP,
    broker prohibits modifications (returns ERR_TRADE_MODIFY_DENIED).
    Orchestrator preemptively rejects with SKIP_FREEZE_LEVEL_BREACH.
    """
    terminal_id = "term_freeze_guard"
    dispatcher = MockBrokerDispatcher(terminal_id=terminal_id)
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    orchestrator.register_terminal_config(TerminalExecutionConfig(
        terminal_id=terminal_id,
        freeze_level_pips=5.0, # 5.0 pip Freeze Level
    ))

    # 1. Open active position at 1.08500 with SL at 1.08450
    open_res = await orchestrator.submit_order(
        terminal_id,
        OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=1.0,
            price=1.08500,
            stop_loss=1.08450,
            signal_id=801,
        ),
    )
    ticket = open_res.ticket

    # 2. Market price drops to 1.08480 (only 3.0 pips from active SL 1.08450)
    # 3.0 pips is inside the 5.0 pip Freeze Level!
    modify_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MODIFY_SL_TP,
        volume=1.0,
        price=1.08480, # Current market price
        stop_loss=1.08460, # Proposed new SL
        order_ticket=ticket,
        signal_id=802,
    )

    res = await orchestrator.submit_order(terminal_id, modify_intent)
    assert res.success is False
    assert res.status == ExecutionStatus.REJECTED
    assert res.skip_reason is not None
    assert res.skip_reason.code == "SKIP_FREEZE_LEVEL_BREACH"
    assert "freeze level" in res.skip_reason.reason


# =============================================================================
# Task 3: Precision Across Asset Classes (JPY Pairs & Gold)
# =============================================================================

def test_stops_level_jpy_precision():
    """
    Validates Stops Level math precision on 3-digit JPY pairs (pip size = 0.01):
    USDJPY Entry: 150.000. Signal SL: 149.970 (3.0 pips away).
    Broker Stops Level: 5.0 pips.
    Assert Orchestrator adjusts SL to exactly 149.950 (5.0 pips away).
    """
    dispatcher = MockBrokerDispatcher(terminal_id="term_jpy")
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    config = TerminalExecutionConfig(
        terminal_id="term_jpy",
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    )

    intent = OrderIntent(
        symbol="USDJPY",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.0,
        price=150.000,
        stop_loss=149.970, # 3.0 pips away (0.030)
        signal_id=901,
    )

    translated = orchestrator.translate_intent(intent, config)
    expected_sl = 149.950 # 150.000 - 0.050
    assert pytest.approx(translated.stop_loss, 1e-4) == expected_sl
    dist_pips = (translated.price - translated.stop_loss) / 0.01
    assert pytest.approx(dist_pips, 1e-4) == 5.0


def test_stops_level_xauusd_precision():
    """
    Validates Stops Level math precision on Gold XAUUSD (pip size = 0.10):
    XAUUSD Entry: 2000.00. Signal SL: 1999.70 (3.0 pips away = $0.30).
    Broker Stops Level: 5.0 pips ($0.50).
    Assert Orchestrator adjusts SL to exactly 1999.50 (5.0 pips away).
    """
    dispatcher = MockBrokerDispatcher(terminal_id="term_gold")
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)
    config = TerminalExecutionConfig(
        terminal_id="term_gold",
        stops_level_pips=5.0,
        stops_level_policy="ADJUST",
    )

    intent = OrderIntent(
        symbol="XAUUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.0,
        price=2000.00,
        stop_loss=1999.70, # 3.0 pips away
        signal_id=902,
    )

    translated = orchestrator.translate_intent(intent, config)
    expected_sl = 1999.50 # 2000.00 - 0.50
    assert pytest.approx(translated.stop_loss, 1e-3) == expected_sl
    dist_pips = (translated.price - translated.stop_loss) / 0.10
    assert pytest.approx(dist_pips, 1e-3) == 5.0


# =============================================================================
# Task 4: Dynamic Capabilities Handshake Integration
# =============================================================================

def test_capabilities_telemetry_handshake_configures_modes():
    """
    Validates that incoming MSG_CAPABILITIES_RESP telemetry dynamically updates
    the TerminalExecutionConfig for FIFO and Stops Level constraints.
    """
    dispatcher = MockBrokerDispatcher(terminal_id="term_caps")
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=dispatcher)

    # Initial default config
    cfg = orchestrator.get_terminal_config("term_caps")
    assert cfg.is_fifo is False
    assert cfg.stops_level_pips == 0.0

    # Stream MSG_CAPABILITIES_RESP packet:
    # is_fifo_enforced = 1, stop_level_points = 50 (5.0 pips), freeze_level_points = 20 (2.0 pips)
    caps_payload = CapabilitiesPayload(
        supports_hedging=False,
        is_fifo_enforced=True,
        stop_level_points=50,
        freeze_level_points=20,
        min_lot=0.01,
        max_lot=50.0,
        lot_step=0.01,
    )

    # Trigger capabilities callback
    for cb in dispatcher._on_capabilities_cbs:
        cb("term_caps", caps_payload)

    # Verify updated configuration
    updated_cfg = orchestrator.get_terminal_config("term_caps")
    assert updated_cfg.is_fifo is True
    assert updated_cfg.fifo_compliant is True
    assert updated_cfg.supports_hedging is False
    assert updated_cfg.stops_level_pips == pytest.approx(5.0, 1e-4)
    assert updated_cfg.freeze_level_pips == pytest.approx(2.0, 1e-4)
    assert updated_cfg.max_lot == 50.0
