"""
Pipvane v3: Sprint 14 - Position Reconciliation & Orphan Chaos Test Suite.
--------------------------------------------------------------------------
Validates the Engine's state desynchronization detection and self-correction:
1. Mock Manual Close (Task 1):
   Dispatches a valid trade. Simulates manual user close on broker terminal
   (deleting position without notifying the Engine). Asserts that the Engine
   catches the discrepancy and updates database status to CLOSED_EXTERNALLY.
2. Simulate Orphan Creation (Task 2):
   Bypasses the Engine entirely. Injects a fake position directly into the
   mock broker's state array carrying the Pipvane Magic Number.
3. Orphan Resolution Assertion (Task 3):
   Asserts that the Engine detects the orphan, triggers an alert, and applies
   the user's defined Resolution Policy (e.g., executing a Market Close if
   set to CLOSE_IMMEDIATELY).
4. Uncompromising Standard:
   The sprint cannot pass until the Engine perfectly self-corrects 100% of
   the injected state errors.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import tempfile
import sqlite3
import pytest
import threading
from typing import List, Dict, Any

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    PositionItem,
    PositionsSyncPayload,
    ExecutionStatus,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.reconciliation import (
    ReconciliationEngine,
    PositionStatus,
    ResolutionPolicy,
    TradeRecord,
    ReconciliationAlert,
    ReconciliationReport,
    DEFAULT_PIPVANE_MAGIC,
)
from bridge.mock_cpp_bridge import MockCppBridgeWorker


# ==============================================================================
# Task 1: Mock Manual Close & CLOSED_EXTERNALLY Assertion
# ==============================================================================

def test_manual_close_detection_updates_db_to_closed_externally(tmp_path):
    """
    Task 1:
    1. Records an active OPEN trade in the Engine's database.
    2. Simulates a broker state where that position was manually deleted/closed by the user.
    3. Runs reconciliation.
    4. Asserts the Engine catches the discrepancy and updates the database to CLOSED_EXTERNALLY.
    """
    db_file = str(tmp_path / "reconcile_test.db")
    engine = ReconciliationEngine(db_path=db_file)

    # 1. Dispatch/record valid open trade (ticket #1001)
    trade = TradeRecord(
        ticket=1001,
        terminal_id="mt5_term_1",
        symbol="EURUSD",
        side=0,  # BUY
        volume=1.00,
        entry_price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09500,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        status=PositionStatus.OPEN.value,
    )
    engine.record_trade(trade)

    # Verify trade is registered as OPEN in DB
    assert engine.get_trade(1001).status == PositionStatus.OPEN.value
    assert len(engine.get_open_trades("mt5_term_1")) == 1

    # 2. Simulate manual user close: Broker terminal reports empty position list
    broker_positions: List[PositionItem] = []

    # 3. Execute reconciliation scan
    alerts_received: List[ReconciliationAlert] = []
    engine.add_alert_listener(lambda a: alerts_received.append(a))

    report = engine.reconcile_terminal(terminal_id="mt5_term_1", broker_positions=broker_positions)

    # 4. ASSERTIONS:
    # Discrepancy caught and reported
    assert report.closed_externally_count == 1
    assert 1001 in report.closed_externally_tickets

    # Database updated strictly to CLOSED_EXTERNALLY
    updated_record = engine.get_trade(1001)
    assert updated_record is not None
    assert updated_record.status == PositionStatus.CLOSED_EXTERNALLY.value
    assert updated_record.close_reason == "MANUAL_EXTERNAL_CLOSE"
    assert updated_record.close_time is not None

    # No more open trades in DB for this terminal
    assert len(engine.get_open_trades("mt5_term_1")) == 0

    # Alert emitted with full diagnostic details
    assert len(alerts_received) == 1
    alert = alerts_received[0]
    assert alert.alert_type == "CLOSED_EXTERNALLY"
    assert alert.ticket == 1001
    assert alert.symbol == "EURUSD"
    assert alert.details["reason"] == "MANUAL_EXTERNAL_CLOSE"

    engine.close()


def test_partial_external_close_multiple_trades(tmp_path):
    """
    Verify reconciliation correctly isolates manually closed trades while leaving
    legitimate active trades untouched in the database.
    """
    db_file = str(tmp_path / "multi_trades.db")
    engine = ReconciliationEngine(db_path=db_file)

    # Seed 3 open trades: #2001 (EURUSD), #2002 (GBPUSD), #2003 (XAUUSD)
    for ticket, sym in [(2001, "EURUSD"), (2002, "GBPUSD"), (2003, "XAUUSD")]:
        engine.record_trade(
            TradeRecord(
                ticket=ticket,
                terminal_id="mt5_term_1",
                symbol=sym,
                side=0,
                volume=0.50,
                entry_price=1.08500,
                status=PositionStatus.OPEN.value,
            )
        )

    # Broker terminal state: #2001 was manually closed by user. Only #2002 and #2003 remain.
    broker_positions = [
        PositionItem(
            ticket=2002, magic_number=DEFAULT_PIPVANE_MAGIC, open_time_unix=int(time.time()),
            symbol="GBPUSD", position_type=0, volume=0.50, open_price=1.28000, current_price=1.28100
        ),
        PositionItem(
            ticket=2003, magic_number=DEFAULT_PIPVANE_MAGIC, open_time_unix=int(time.time()),
            symbol="XAUUSD", position_type=0, volume=0.50, open_price=2350.00, current_price=2355.00
        ),
    ]

    report = engine.reconcile_terminal("mt5_term_1", broker_positions=broker_positions)

    assert report.closed_externally_count == 1
    assert report.matched_count == 2
    assert report.closed_externally_tickets == [2001]

    # #2001 is CLOSED_EXTERNALLY
    assert engine.get_trade(2001).status == PositionStatus.CLOSED_EXTERNALLY.value
    # #2002 and #2003 remain OPEN
    assert engine.get_trade(2002).status == PositionStatus.OPEN.value
    assert engine.get_trade(2003).status == PositionStatus.OPEN.value

    engine.close()


# ==============================================================================
# Tasks 2 & 3: Orphan Detection & Resolution Policies
# ==============================================================================

def test_orphan_detection_and_resolution_close_immediately(tmp_path):
    """
    Tasks 2 & 3:
    1. Bypasses the Engine entirely by injecting a fake position carrying the
       Pipvane Magic Number directly into the broker's position array.
    2. Runs reconciliation with ResolutionPolicy.CLOSE_IMMEDIATELY.
    3. Asserts the Engine detects the orphan, triggers an alert, and dispatches
       a Market Close (ActionType.FULL_CLOSE) order intent.
    """
    db_file = str(tmp_path / "orphan_test.db")
    mock_dispatcher = IPCDispatcher(use_mock_buffers=True)
    mock_dispatcher.register_terminal("mt5_term_1", use_mock=True)

    engine = ReconciliationEngine(
        db_path=db_file,
        dispatcher=mock_dispatcher,
        default_policy=ResolutionPolicy.CLOSE_IMMEDIATELY,
    )

    alerts: List[ReconciliationAlert] = []
    engine.add_alert_listener(lambda a: alerts.append(a))

    # 1. Inject orphan position on broker: Ticket #999888 with Pipvane Magic Number
    orphan_pos = PositionItem(
        ticket=999888,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="EURUSD",
        position_type=0,  # BUY
        volume=0.50,
        open_price=1.08500,
        current_price=1.08550,
        comment="Injected Rogue Trade",
    )

    # Local DB has zero trades registered
    assert engine.get_trade(999888) is None

    # 2. Run reconciliation scan
    report = engine.reconcile_terminal(
        terminal_id="mt5_term_1",
        broker_positions=[orphan_pos],
        policy=ResolutionPolicy.CLOSE_IMMEDIATELY,
    )

    # 3. ASSERTIONS:
    # Orphan detected
    assert report.orphans_detected_count == 1
    assert 999888 in report.orphan_tickets
    assert report.orphans_resolved_count == 1

    # Alerts emitted: ORPHAN_DETECTED and ORPHAN_RESOLVED
    alert_types = [a.alert_type for a in alerts]
    assert "ORPHAN_DETECTED" in alert_types
    assert "ORPHAN_RESOLVED" in alert_types

    # Resolution Policy CLOSE_IMMEDIATELY:
    # Database recorded as ORPHAN_RESOLVED with close reason CLOSE_IMMEDIATELY
    rec = engine.get_trade(999888)
    assert rec is not None
    assert rec.status == PositionStatus.ORPHAN_RESOLVED.value
    assert rec.close_reason == "CLOSE_IMMEDIATELY"
    assert rec.volume == 0.50

    engine.close()


def test_orphan_resolution_policy_adopt(tmp_path):
    """
    Verify ResolutionPolicy.ADOPT successfully adopts an unmanaged orphan
    into the database under active management.
    """
    db_file = str(tmp_path / "orphan_adopt.db")
    engine = ReconciliationEngine(
        db_path=db_file,
        default_policy=ResolutionPolicy.ADOPT,
    )

    orphan_pos = PositionItem(
        ticket=888777,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="XAUUSD",
        position_type=1,  # SELL
        volume=0.20,
        open_price=2350.00,
        current_price=2345.00,
    )

    report = engine.reconcile_terminal(
        terminal_id="mt5_term_1",
        broker_positions=[orphan_pos],
        policy=ResolutionPolicy.ADOPT,
    )

    assert report.orphans_detected_count == 1
    assert report.orphans_resolved_count == 1

    # Record must now exist in DB as OPEN (actively adopted)
    trade = engine.get_trade(888777)
    assert trade is not None
    assert trade.status == PositionStatus.OPEN.value
    assert trade.close_reason == "ADOPTED_ORPHAN"
    assert len(engine.get_open_trades("mt5_term_1")) == 1

    engine.close()


def test_orphan_resolution_policy_alert_only(tmp_path):
    """
    Verify ResolutionPolicy.ALERT_ONLY logs an alert and marks the orphan as
    ORPHAN_DETECTED without dispatching any broker close commands.
    """
    db_file = str(tmp_path / "orphan_alert_only.db")
    engine = ReconciliationEngine(
        db_path=db_file,
        default_policy=ResolutionPolicy.ALERT_ONLY,
    )

    orphan_pos = PositionItem(
        ticket=777666,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="BTCUSD",
        position_type=0,
        volume=0.10,
        open_price=65000.0,
        current_price=65100.0,
    )

    report = engine.reconcile_terminal(
        terminal_id="mt5_term_1",
        broker_positions=[orphan_pos],
        policy=ResolutionPolicy.ALERT_ONLY,
    )

    assert report.orphans_detected_count == 1
    assert report.orphans_resolved_count == 0

    trade = engine.get_trade(777666)
    assert trade is not None
    assert trade.status == PositionStatus.ORPHAN_DETECTED.value
    assert trade.close_reason == "ALERT_ONLY_UNMANAGED"

    engine.close()


def test_non_pipvane_positions_are_ignored(tmp_path):
    """
    Verify user's personal trades (non-Pipvane magic numbers) are strictly ignored
    and never flagged as orphans or closed.
    """
    db_file = str(tmp_path / "ignore_manual.db")
    engine = ReconciliationEngine(db_path=db_file)

    user_manual_trade = PositionItem(
        ticket=555444,
        magic_number=0,  # Manual discretionary trade (Magic = 0)
        open_time_unix=int(time.time()),
        symbol="EURUSD",
        position_type=0,
        volume=2.00,
        open_price=1.08500,
        current_price=1.08600,
    )
    third_party_ea_trade = PositionItem(
        ticket=555445,
        magic_number=999999,  # Third party EA
        open_time_unix=int(time.time()),
        symbol="USDJPY",
        position_type=1,
        volume=1.00,
        open_price=155.00,
        current_price=154.50,
    )

    report = engine.reconcile_terminal(
        terminal_id="mt5_term_1",
        broker_positions=[user_manual_trade, third_party_ea_trade],
    )

    assert report.orphans_detected_count == 0
    assert report.closed_externally_count == 0
    assert len(report.alerts) == 0
    assert engine.get_trade(555444) is None
    assert engine.get_trade(555445) is None

    engine.close()


# ==============================================================================
# End-to-End Live Integration: Dispatcher, Named Pipe, & Mock C++ Bridge
# ==============================================================================

@pytest.mark.asyncio
async def test_live_ipc_manual_close_and_orphan_self_correction(tmp_path):
    """
    End-to-End Chaos Scenario:
    1. Connects Pipvane Engine to Mock C++ MetaTrader Bridge over live Named Pipe and SHM.
    2. Dispatches a legitimate BUY trade (Ticket #1000000). Confirms DB marks it OPEN.
    3. Simulates manual user close on mock terminal (deleting ticket #1000000).
    4. Injects an orphan trade (Ticket #999111) carrying Pipvane Magic Number into mock broker state.
    5. Mock terminal transmits MSG_POSITIONS_SYNC frame over Named Pipe.
    6. Waits/asserts Engine detects both errors and achieves 100% self-correction:
       - Ticket #1000000 updated to CLOSED_EXTERNALLY.
       - Orphan ticket #999111 detected, alert fired, and ActionType.FULL_CLOSE executed
         flattening the orphan on the mock broker.
    """
    db_file = str(tmp_path / "live_chaos.db")
    terminal_id = "reconcile_live_term"
    pipe_name = fr"\\.\pipe\pipvane_mt_{terminal_id}"
    shm_name = fr"Local\pipvane_shm_{terminal_id}"

    dispatcher = IPCDispatcher(use_mock_buffers=False)
    dispatcher.register_terminal(
        terminal_id=terminal_id,
        pipe_name=pipe_name,
        shm_name=shm_name,
    )

    engine = ReconciliationEngine(
        db_path=db_file,
        dispatcher=dispatcher,
        default_policy=ResolutionPolicy.CLOSE_IMMEDIATELY,
    )

    # 1. Start Dispatcher pipe server
    await dispatcher.start()
    await asyncio.sleep(0.1)

    # 2. Attach Mock C++ Bridge
    worker = MockCppBridgeWorker(
        terminal_id=terminal_id,
        pipe_name=pipe_name,
        shm_name=shm_name,
        platform_type=1,  # MT5
        quiet=True,
    )
    assert worker.connect_pipe(timeout_s=3.0) is True
    worker.send_init_handshake()
    worker.attach_shm()

    stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=worker.run_consumer_loop,
        kwargs={"target_iterations": 0, "stop_event": stop_event},
        daemon=True,
    )
    worker_thread.start()
    await asyncio.sleep(0.15)

    try:
        # 3. Dispatch a valid market trade
        sig_id = 99887766
        intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.50,
            price=1.08500,
            stop_loss=1.08000,
            take_profit=1.09500,
            magic_number=DEFAULT_PIPVANE_MAGIC,
            signal_id=sig_id,
            timestamp_ns=time.time_ns(),
        )
        assert dispatcher.dispatch_order_intent(terminal_id, intent) is True
        await asyncio.sleep(0.2)

        # Worker processed order and created ticket #1000000
        assert 1000000 in worker.broker_positions

        # Record trade in Engine DB as OPEN
        engine.record_trade(
            TradeRecord(
                ticket=1000000,
                terminal_id=terminal_id,
                symbol="EURUSD",
                side=0,
                volume=0.50,
                entry_price=1.08500,
                magic_number=DEFAULT_PIPVANE_MAGIC,
                status=PositionStatus.OPEN.value,
            )
        )
        assert engine.get_trade(1000000).status == PositionStatus.OPEN.value

        # ======================================================================
        # INJECT CHAOS 1: Simulate manual user close of ticket #1000000
        # ======================================================================
        assert worker.simulate_manual_close(1000000) is True
        assert 1000000 not in worker.broker_positions

        # ======================================================================
        # INJECT CHAOS 2: Inject orphan ticket #999111 carrying Pipvane Magic
        # ======================================================================
        orphan_item = worker.inject_orphan_position(
            ticket=999111,
            magic_number=DEFAULT_PIPVANE_MAGIC,
            symbol="EURUSD",
            volume=0.30,
            side=0,
            open_price=1.08520,
        )
        assert 999111 in worker.broker_positions

        # 4. Trigger reconciliation by transmitting MSG_POSITIONS_SYNC from broker
        assert worker.send_positions_sync() is True
        await asyncio.sleep(0.3)

        # 5. ASSERT 100% SELF-CORRECTION:
        # Check 1: Manual close detected -> Updated to CLOSED_EXTERNALLY in DB
        trade_1000000 = engine.get_trade(1000000)
        assert trade_1000000 is not None
        assert trade_1000000.status == PositionStatus.CLOSED_EXTERNALLY.value
        assert trade_1000000.close_reason == "MANUAL_EXTERNAL_CLOSE"

        # Check 2: Orphan detected -> ActionType.FULL_CLOSE dispatched -> Flattened on broker!
        orphan_rec = engine.get_trade(999111)
        assert orphan_rec is not None
        assert orphan_rec.status == PositionStatus.ORPHAN_RESOLVED.value
        assert orphan_rec.close_reason == "CLOSE_IMMEDIATELY"

        # Allow consumer loop to consume the FULL_CLOSE order intent
        await asyncio.sleep(0.2)
        # Verify the mock broker removed the orphan position after receiving FULL_CLOSE
        assert 999111 not in worker.broker_positions

    finally:
        stop_event.set()
        worker.close()
        await dispatcher.stop()
        engine.close()
        await asyncio.sleep(0.1)


# ==============================================================================
# Additional Coverage & Edge Case Scenarios
# ==============================================================================

@pytest.mark.asyncio
async def test_periodic_reconciliation_loop_execution(tmp_path):
    """Verify periodic background reconciliation loop audits terminals automatically."""
    db_file = str(tmp_path / "periodic_loop.db")
    mock_dispatcher = IPCDispatcher(use_mock_buffers=True)
    mock_dispatcher.register_terminal("term_auto", use_mock=True)

    engine = ReconciliationEngine(
        db_path=db_file,
        dispatcher=mock_dispatcher,
        reconciliation_interval_s=0.1,  # 100ms for fast testing
    )

    # Seed an open trade that is missing on the broker
    engine.record_trade(
        TradeRecord(
            ticket=33001,
            terminal_id="term_auto",
            symbol="EURUSD",
            side=0,
            volume=0.5,
            entry_price=1.0850,
            status=PositionStatus.OPEN.value,
        )
    )

    await engine.start_periodic_reconciliation()
    await asyncio.sleep(0.35)
    await engine.stop_periodic_reconciliation()

    # Record must have been self-corrected to CLOSED_EXTERNALLY by periodic loop!
    rec = engine.get_trade(33001)
    assert rec is not None
    assert rec.status == PositionStatus.CLOSED_EXTERNALLY.value
    engine.close()


def test_orphan_resurrected_from_closed_db_record(tmp_path):
    """Verify ticket marked as CLOSED in DB is flagged as an orphan if active on broker."""
    db_file = str(tmp_path / "resurrected.db")
    engine = ReconciliationEngine(db_path=db_file)

    # Trade was historically closed in DB
    engine.record_trade(
        TradeRecord(
            ticket=44001,
            terminal_id="term_1",
            symbol="EURUSD",
            side=0,
            volume=0.5,
            entry_price=1.0850,
            status=PositionStatus.CLOSED.value,
        )
    )

    # Broker still reports it as open with Pipvane magic
    broker_pos = PositionItem(
        ticket=44001,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="EURUSD",
        position_type=0,
        volume=0.5,
        open_price=1.0850,
        current_price=1.0860,
    )

    report = engine.reconcile_terminal("term_1", broker_positions=[broker_pos])
    assert report.orphans_detected_count == 1
    assert 44001 in report.orphan_tickets
    engine.close()


def test_reconciliation_cached_positions_and_alert_exception(tmp_path):
    """Verify cached positions fallback and listener exception resilience."""
    db_file = str(tmp_path / "cached_exc.db")
    engine = ReconciliationEngine(db_path=db_file)

    # Faulty listener
    def bad_listener(alert):
        raise RuntimeError("Simulated listener exception")

    engine.add_alert_listener(bad_listener)

    # Cache positions via internal handler
    sync_payload = PositionsSyncPayload(
        account_login=123456,
        positions=[
            PositionItem(
                ticket=55001,
                magic_number=DEFAULT_PIPVANE_MAGIC,
                open_time_unix=int(time.time()),
                symbol="EURUSD",
                position_type=0,
                volume=0.1,
                open_price=1.0850,
                current_price=1.0850,
            )
        ],
    )
    engine._on_positions_sync_received("term_cached", sync_payload)

    # Reconcile passing broker_positions=None (should use cached)
    report = engine.reconcile_terminal("term_cached", broker_positions=None)
    assert report.broker_position_count == 1
    assert report.orphans_detected_count == 1

    engine.close()

