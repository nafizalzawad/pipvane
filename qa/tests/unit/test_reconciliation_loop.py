"""
Unit tests for Position Reconciliation Loop & Manual Close Detection (Sprint 14 Directive).

Validates:
1. State Audit Polling:
   - Asynchronous 30-second reconciliation loop periodically polls connected terminals.
   - Disconnected/unready terminals are safely skipped to avoid false positive closes.
2. Ticket Matching:
   - Matches Ticket ID and Magic Number against the internal SQLite OrderIntents database.
   - Preserves OPEN status when broker confirms position is active with matching ticket/magic.
3. Manual Close Detection:
   - When SQLite shows an order is OPEN, but the broker reports it gone, marks trade
     as CLOSED_EXTERNALLY in both order_intents and Journey audit log.
   - Deactivates automated TP/SL and Break-Even management for the externally closed trade.
4. End-to-End Execution & Named Pipe Integration:
   - Validates live simulation of order execution -> registration -> external closure ->
     suppression of subsequent price tick modifications.
"""

import asyncio
import struct
import time
from typing import List
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    TerminalStatus,
    InitHelloPayload,
    AccountState,
    PositionItem,
    PositionsSyncPayload,
    pack_pipe_frame,
    NamedPipeOpcode,
    HEADER_SIZE,
    TOTAL_SLOTS,
    SLOT_SIZE,
    deserialize_order_intent,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    BreakEvenConfig,
    ManagedPosition,
    VirtualTPLevel,
)


@pytest.fixture
def mock_dispatcher():
    """Provides an IPCDispatcher instance using mock shared-memory buffers."""
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    return dispatcher


async def setup_active_terminal(
    dispatcher: IPCDispatcher,
    terminal_id: str,
    platform_type: int = 1,
    account_login: int = 888999,
):
    """Helper to register and advance terminal state to fully CONNECTED."""
    ctx = dispatcher.register_terminal(terminal_id)
    hello = InitHelloPayload(
        terminal_id=terminal_id,
        platform_type=platform_type,
        account_login=account_login,
        broker_server="ICMarkets-Demo",
    )
    ctx.pipe_server.simulate_inbound_data(
        pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack())
    )
    state = AccountState(
        account_login=account_login,
        balance=10000.0,
        equity=10000.0,
        margin=0.0,
        free_margin=10000.0,
        is_connected_to_broker=True,
    )
    ctx.pipe_server.simulate_inbound_data(
        pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack())
    )
    assert dispatcher.can_route_to_terminal(terminal_id) is True
    return ctx


# =============================================================================
# 1. Ticket Matching Tests (Ticket ID + Magic Number)
# =============================================================================

@pytest.mark.asyncio
async def test_ticket_matching_retains_open_state(mock_dispatcher):
    """When broker reports matching (ticket, magic), internal order remains OPEN."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        reconciliation_interval_s=30.0,
    )
    await setup_active_terminal(mock_dispatcher, "term_1")

    # Seed order into internal SQLite state
    orchestrator.record_order_intent(
        signal_id=1001,
        ticket=555111,
        magic_number=123456,
        terminal_id="term_1",
        symbol="EURUSD",
        side=int(OrderSide.BUY),
        volume=0.5,
        open_price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09500,
        status="OPEN",
    )

    # Broker position dump confirming ticket 555111 with Magic 123456 is active
    broker_positions = [
        PositionItem(
            ticket=555111,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="EURUSD",
            position_type=0,
            volume=0.5,
            open_price=1.08500,
            current_price=1.08700,
            stop_loss=1.08000,
            take_profit=1.09500,
            profit=100.0,
        )
    ]

    summary = await orchestrator.reconcile_terminal("term_1", broker_positions=broker_positions)

    assert summary["audited_orders"] == 1
    assert summary["externally_closed"] == 0
    assert summary["active_remaining"] == 1

    # Verify order is still OPEN in database
    order = orchestrator.get_order_by_ticket(555111)
    assert order is not None
    assert order["status"] == "OPEN"

    # Verify no CLOSED_EXTERNALLY event logged
    journey = orchestrator.get_journey_logs(ticket=555111, event_type="CLOSED_EXTERNALLY")
    assert len(journey) == 0

    await orchestrator.stop()


# =============================================================================
# 2. Manual Close Detection Tests
# =============================================================================

@pytest.mark.asyncio
async def test_manual_close_detection_marks_closed_externally(mock_dispatcher):
    """
    If SQLite DB shows order is OPEN, but broker dump reports it gone,
    it must be marked CLOSED_EXTERNALLY in DB and Journey log.
    """
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        reconciliation_interval_s=30.0,
    )
    await setup_active_terminal(mock_dispatcher, "term_1")

    # Seed 2 orders: one will remain open, one was closed externally
    orchestrator.record_order_intent(
        signal_id=2001,
        ticket=600001,
        magic_number=123456,
        terminal_id="term_1",
        symbol="XAUUSD",
        side=int(OrderSide.BUY),
        volume=0.2,
        open_price=2000.0,
        status="OPEN",
    )
    orchestrator.record_order_intent(
        signal_id=2002,
        ticket=600002,
        magic_number=123456,
        terminal_id="term_1",
        symbol="GBPUSD",
        side=int(OrderSide.SELL),
        volume=0.1,
        open_price=1.2600,
        status="OPEN",
    )

    # Broker dump only contains 600001 (600002 was manually closed by trader)
    broker_positions = [
        PositionItem(
            ticket=600001,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="XAUUSD",
            position_type=0,
            volume=0.2,
            open_price=2000.0,
            current_price=2005.0,
        )
    ]

    summary = await orchestrator.reconcile_terminal("term_1", broker_positions=broker_positions)

    assert summary["audited_orders"] == 2
    assert summary["externally_closed"] == 1
    assert summary["active_remaining"] == 1
    assert summary["closed_tickets"] == [600002]

    # Check 600001 remains OPEN
    order_1 = orchestrator.get_order_by_ticket(600001)
    assert order_1["status"] == "OPEN"

    # Check 600002 is CLOSED_EXTERNALLY
    order_2 = orchestrator.get_order_by_ticket(600002)
    assert order_2["status"] == "CLOSED_EXTERNALLY"
    assert order_2["closed_at"] is not None

    # Verify Journey log entry
    journey = orchestrator.get_journey_logs(ticket=600002, event_type="CLOSED_EXTERNALLY")
    assert len(journey) == 1
    assert journey[0]["status"] == "CLOSED_EXTERNALLY"
    assert "Manual/External close detected" in journey[0]["details"]

    await orchestrator.stop()


# =============================================================================
# 3. Disabling TP/SL Modifications on External Close
# =============================================================================

@pytest.mark.asyncio
async def test_reconciliation_stops_automated_tpsl_modifications(mock_dispatcher):
    """
    Once marked CLOSED_EXTERNALLY, the orchestrator ceases all attempts
    to modify the position's Stop Loss or Take Profit.
    """
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    await setup_active_terminal(mock_dispatcher, "term_1")

    ticket = 700001
    # Register in DB
    orchestrator.record_order_intent(
        signal_id=3001,
        ticket=ticket,
        magic_number=123456,
        terminal_id="term_1",
        symbol="XAUUSD",
        side=int(OrderSide.BUY),
        volume=0.1,
        open_price=2000.0,
        stop_loss=1990.0,
        take_profit=2020.0,
        status="OPEN",
    )

    # Register in in-memory ManagedPositions with Break-Even enabled
    be_config = BreakEvenConfig(
        enabled=True,
        trigger_mode="pips",
        trigger_pips=10.0, # +10 pips profit triggers BE
        offset_pips=2.0,
    )
    managed_pos = ManagedPosition(
        ticket=ticket,
        terminal_id="term_1",
        symbol="XAUUSD",
        side=OrderSide.BUY,
        entry_price=2000.0,
        original_volume=0.1,
        current_volume=0.1,
        current_sl=1990.0,
        current_tp=2020.0,
        pip_size=0.1, # Gold pip size
        break_even_config=be_config,
    )
    orchestrator.register_managed_position(managed_pos)

    # 1. Before reconciliation: price movement toward profit would trigger BE
    assert managed_pos.closed is False
    assert managed_pos.break_even_config.enabled is True

    # 2. Reconcile with empty broker positions (trade closed externally)
    summary = await orchestrator.reconcile_terminal("term_1", broker_positions=[])
    assert summary["externally_closed"] == 1

    # 3. Verify in-memory position state deactivated
    assert managed_pos.closed is True
    assert managed_pos.break_even_config.enabled is False

    # 4. Now simulate incoming tick at 2005.0 (+50 pips, well past BE threshold)
    # Orchestrator must ignore this closed position and dispatch NO orders
    results = await orchestrator.on_price_update(
        symbol="XAUUSD",
        bid=2005.0,
        ask=2005.5,
        terminal_id="term_1",
    )
    assert len(results) == 0

    await orchestrator.stop()


# =============================================================================
# 4. Named Pipe MSG_POSITIONS_SYNC Request/Response
# =============================================================================

@pytest.mark.asyncio
async def test_fetch_positions_dump_over_named_pipe(mock_dispatcher):
    """Named Pipe request/response cycle for MSG_POSITIONS_SYNC."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    ctx = await setup_active_terminal(mock_dispatcher, "term_1")

    # Simulate terminal responding to MSG_POSITIONS_SYNC
    mock_payload = PositionsSyncPayload(
        account_login=888999,
        positions=[
            PositionItem(
                ticket=800001,
                magic_number=999888,
                open_time_unix=int(time.time()),
                symbol="EURUSD",
                position_type=0,
                volume=1.0,
                open_price=1.0800,
                current_price=1.0820,
            )
        ],
    )

    # In background, when pipe server sends request frame, simulate response
    async def respond_to_sync():
        await asyncio.sleep(0.01)
        # Inject MSG_POSITIONS_SYNC frame back into pipe server
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_POSITIONS_SYNC, mock_payload.pack())
        )

    task = asyncio.create_task(respond_to_sync())

    dump = await mock_dispatcher.fetch_positions_dump("term_1", timeout_s=1.0)
    await task

    assert dump.account_login == 888999
    assert len(dump.positions) == 1
    assert dump.positions[0].ticket == 800001
    assert dump.positions[0].symbol == "EURUSD"

    await orchestrator.stop()


# =============================================================================
# 5. Periodic Reconciliation Loop Execution & Skipping Inactive Terminals
# =============================================================================

@pytest.mark.asyncio
async def test_reconciliation_loop_skips_unconnected_terminals(mock_dispatcher):
    """Reconciliation loop executes every interval and safely skips disconnected terminals."""
    # Set fast interval for test
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        reconciliation_interval_s=0.05,
    )

    # Register term_1 (connected) and term_2 (disconnected)
    ctx1 = await setup_active_terminal(mock_dispatcher, "term_1")
    ctx2 = mock_dispatcher.register_terminal("term_2") # Disconnected state

    # Provide cached active position snapshot for term_1 so polling succeeds instantly
    ctx1.pipe_server.last_positions_sync = PositionsSyncPayload(
        account_login=888999,
        positions=[
            PositionItem(
                ticket=900001,
                magic_number=123456,
                open_time_unix=int(time.time()),
                symbol="EURUSD",
                position_type=0,
                volume=0.1,
                open_price=1.0800,
                current_price=1.0800,
            )
        ],
    )

    # Seed order for term_1 and term_2
    orchestrator.record_order_intent(
        signal_id=4001,
        ticket=900001,
        magic_number=123456,
        terminal_id="term_1",
        symbol="EURUSD",
        side=0,
        volume=0.1,
        open_price=1.0800,
        status="OPEN",
    )
    orchestrator.record_order_intent(
        signal_id=4002,
        ticket=900002,
        magic_number=123456,
        terminal_id="term_2",
        symbol="EURUSD",
        side=0,
        volume=0.1,
        open_price=1.0800,
        status="OPEN",
    )

    # Start orchestrator which initiates the reconciliation loop
    await orchestrator.start()

    # Allow reconciliation loop to execute at least 2 cycles
    for _ in range(25):
        if orchestrator.get_reconciliation_stats()["total_cycles"] >= 2:
            break
        await asyncio.sleep(0.04)

    stats = orchestrator.get_reconciliation_stats()
    assert stats["total_cycles"] >= 2

    # term_2 was disconnected, so its order must NOT be prematurely marked CLOSED_EXTERNALLY
    order_term2 = orchestrator.get_order_by_ticket(900002)
    assert order_term2["status"] == "OPEN"

    await orchestrator.stop()


# =============================================================================
# 6. Deliverable Method Aliasing
# =============================================================================

def test_reconciliation_loop_alias(mock_dispatcher):
    """Verifies that ReconciliationLoop method exists and points to reconciliation_loop."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    assert hasattr(orchestrator, "ReconciliationLoop")
    assert hasattr(orchestrator, "reconciliation_loop")
    assert orchestrator.ReconciliationLoop == orchestrator.reconciliation_loop


# =============================================================================
# 7. Multi-Terminal State Isolation
# =============================================================================

@pytest.mark.asyncio
async def test_reconciliation_multi_terminal_isolation(mock_dispatcher):
    """Auditing term_1 does not affect or mutate active orders on term_2."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    await setup_active_terminal(mock_dispatcher, "term_A")
    await setup_active_terminal(mock_dispatcher, "term_B")

    # Order on term_A and Order on term_B
    orchestrator.record_order_intent(
        signal_id=5001,
        ticket=111111,
        magic_number=123456,
        terminal_id="term_A",
        symbol="EURUSD",
        side=0,
        volume=0.1,
        open_price=1.0800,
        status="OPEN",
    )
    orchestrator.record_order_intent(
        signal_id=5002,
        ticket=222222,
        magic_number=123456,
        terminal_id="term_B",
        symbol="EURUSD",
        side=0,
        volume=0.1,
        open_price=1.0800,
        status="OPEN",
    )

    # Reconcile term_A with an empty broker dump (term_A trade was closed)
    res_A = await orchestrator.reconcile_terminal("term_A", broker_positions=[])
    assert res_A["externally_closed"] == 1
    assert res_A["closed_tickets"] == [111111]

    # term_A order is CLOSED_EXTERNALLY
    order_A = orchestrator.get_order_by_ticket(111111)
    assert order_A["status"] == "CLOSED_EXTERNALLY"

    # term_B order MUST STILL BE OPEN!
    order_B = orchestrator.get_order_by_ticket(222222)
    assert order_B["status"] == "OPEN"

    await orchestrator.stop()


# =============================================================================
# 8. Untracked / Foreign Broker Positions
# =============================================================================

@pytest.mark.asyncio
async def test_untracked_foreign_positions_do_not_interfere(mock_dispatcher):
    """Broker positions with different magic numbers or unknown tickets are safely ignored."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    await setup_active_terminal(mock_dispatcher, "term_1")

    # Our managed trade
    orchestrator.record_order_intent(
        signal_id=6001,
        ticket=333333,
        magic_number=123456,
        terminal_id="term_1",
        symbol="USDJPY",
        side=0,
        volume=0.5,
        open_price=150.0,
        status="OPEN",
    )

    # Broker dump contains our trade PLUS a foreign manual trade (magic 0 or different)
    broker_positions = [
        PositionItem(
            ticket=333333,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="USDJPY",
            position_type=0,
            volume=0.5,
            open_price=150.0,
            current_price=150.2,
        ),
        PositionItem(
            ticket=999999, # Manual trade from MT5 GUI
            magic_number=0,
            open_time_unix=int(time.time()),
            symbol="EURUSD",
            position_type=1,
            volume=1.0,
            open_price=1.0850,
            current_price=1.0840,
        ),
    ]

    summary = await orchestrator.reconcile_terminal("term_1", broker_positions=broker_positions)
    assert summary["audited_orders"] == 1
    assert summary["externally_closed"] == 0
    assert summary["active_remaining"] == 1

    order = orchestrator.get_order_by_ticket(333333)
    assert order["status"] == "OPEN"

    await orchestrator.stop()


# =============================================================================
# 9. Virtual TP Netting Position External Closure
# =============================================================================

@pytest.mark.asyncio
async def test_netting_virtual_tp_external_closure_prevents_partial_close(mock_dispatcher):
    """
    In Netting accounts, if the parent order is closed externally,
    virtual TP levels are marked triggered so process_virtual_tps skips them.
    """
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    await setup_active_terminal(mock_dispatcher, "term_net")

    ticket = 444444
    orchestrator.record_order_intent(
        signal_id=7001,
        ticket=ticket,
        magic_number=123456,
        terminal_id="term_net",
        symbol="XAUUSD",
        side=0,
        volume=1.0,
        open_price=2000.0,
        status="OPEN",
    )

    # Register netting position with virtual TPs
    vtps = [
        VirtualTPLevel(level_index=0, target_price=2010.0, volume_to_close=0.5),
        VirtualTPLevel(level_index=1, target_price=2020.0, volume_to_close=0.5),
    ]
    managed_pos = ManagedPosition(
        ticket=ticket,
        terminal_id="term_net",
        symbol="XAUUSD",
        side=OrderSide.BUY,
        entry_price=2000.0,
        original_volume=1.0,
        current_volume=1.0,
        current_sl=1990.0,
        current_tp=0.0,
        is_netting=True,
        virtual_tps=vtps,
    )
    orchestrator.register_managed_position(managed_pos)

    # Reconcile with empty positions (user closed netting position in MT5 terminal)
    summary = await orchestrator.reconcile_terminal("term_net", broker_positions=[])
    assert summary["externally_closed"] == 1

    # Verify position is closed and virtual TPs are neutralized
    assert managed_pos.closed is True
    assert all(vtp.is_triggered for vtp in managed_pos.virtual_tps)

    # Feed price tick at 2015.0 (which would have triggered Virtual TP 1)
    results = await orchestrator.process_virtual_tps(
        symbol="XAUUSD",
        bid=2015.0,
        ask=2015.5,
        terminal_id="term_net",
    )
    # Must dispatch NO close commands because position was already closed externally
    assert len(results) == 0

    await orchestrator.stop()

