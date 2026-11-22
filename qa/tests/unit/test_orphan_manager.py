"""
Unit tests for OrphanManager & Orphan Trade Handler (Sprint 14 Directive).

Validates:
1. Orphan Detection:
   - Identifies positions carrying Pipvane Magic Number with no matching OrderIntent in SQLite.
   - Foreign trades (different magic number) and legitimate active trades are not flagged.
   - Previously closed/resurrected trades are correctly flagged as orphans.
2. Resolution Policies:
   - IGNORE: Leaves trade untouched on broker, records audit telemetry in Journey log.
   - ADOPT: Ingests trade into internal SQLite database and registers into in-memory ManagedPosition.
   - CLOSE_IMMEDIATELY: Dispatches market close (ActionType.FULL_CLOSE) command to liquidate for safety.
3. Alerting & Notification:
   - Emits immediate explicit alerts on detection with full diagnostic telemetry.
   - Supports webhook dispatch and notification callbacks.
4. Integration with Reconciliation Cycle:
   - Full integration with ExecutionOrchestrator's reconciliation cycle and report summary.
"""

import asyncio
import time
from typing import List
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PositionItem,
    PositionsSyncPayload,
    pack_pipe_frame,
    NamedPipeOpcode,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    ManagedPosition,
)
from engine.orphan_manager import (
    OrphanManager,
    OrphanResolutionPolicy,
    OrphanAlert,
    OrphanNotificationClient,
    DEFAULT_PIPVANE_MAGIC,
)


@pytest.fixture
def mock_dispatcher():
    """Provides an IPCDispatcher instance using mock shared-memory buffers."""
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    return dispatcher


# =============================================================================
# 1. Orphan Detection Logic Tests
# =============================================================================

def test_is_orphan_classification():
    """Validates the orphan identification boundary rules."""
    mgr = OrphanManager(pipvane_magic_number=DEFAULT_PIPVANE_MAGIC)

    pos_pipvane = PositionItem(
        ticket=1001,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="EURUSD",
        position_type=0,
        volume=0.5,
        open_price=1.0850,
        current_price=1.0860,
    )

    pos_foreign = PositionItem(
        ticket=1002,
        magic_number=999888, # Different EA or manual trade
        open_time_unix=int(time.time()),
        symbol="EURUSD",
        position_type=0,
        volume=0.5,
        open_price=1.0850,
        current_price=1.0860,
    )

    # 1. Foreign trade is NEVER an orphan for Pipvane
    assert mgr.is_orphan(pos_foreign, existing_order=None) is False

    # 2. Pipvane trade with NO internal record IS an orphan
    assert mgr.is_orphan(pos_pipvane, existing_order=None) is True

    # 3. Pipvane trade with active OPEN status is NOT an orphan
    existing_open = {"ticket": 1001, "status": "OPEN", "magic_number": DEFAULT_PIPVANE_MAGIC}
    assert mgr.is_orphan(pos_pipvane, existing_order=existing_open) is False

    # 4. Pipvane trade with CLOSED or CLOSED_EXTERNALLY in DB but active on broker IS an orphan
    existing_closed = {"ticket": 1001, "status": "CLOSED", "magic_number": DEFAULT_PIPVANE_MAGIC}
    assert mgr.is_orphan(pos_pipvane, existing_order=existing_closed) is True

    existing_ext_closed = {"ticket": 1001, "status": "CLOSED_EXTERNALLY", "magic_number": DEFAULT_PIPVANE_MAGIC}
    assert mgr.is_orphan(pos_pipvane, existing_order=existing_ext_closed) is True


# =============================================================================
# 2. Resolution Policy: CLOSE_IMMEDIATELY Tests
# =============================================================================

@pytest.mark.asyncio
async def test_orphan_resolution_close_immediately(mock_dispatcher):
    """
    CLOSE_IMMEDIATELY policy:
    1. Triggers ORPHAN_DETECTED alert.
    2. Dispatches ActionType.FULL_CLOSE command to broker.
    3. Records event in Journey log.
    4. Emits ORPHAN_RESOLVED alert.
    """
    alerts: List[OrphanAlert] = []
    notif_client = OrphanNotificationClient(callbacks=[lambda a: alerts.append(a)])

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    mock_dispatcher.register_terminal("term_1", use_mock=True)

    mgr = OrphanManager(
        default_policy=OrphanResolutionPolicy.CLOSE_IMMEDIATELY,
        pipvane_magic_number=DEFAULT_PIPVANE_MAGIC,
        orchestrator=orchestrator,
        ipc_dispatcher=mock_dispatcher,
        notification_client=notif_client,
    )

    orphan_pos = PositionItem(
        ticket=555444,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="GBPUSD",
        position_type=0, # BUY -> close will be SELL
        volume=0.25,
        open_price=1.2800,
        current_price=1.2810,
    )

    res = await mgr.handle_orphan("term_1", orphan_pos)

    assert res["action_taken"] == "CLOSED_IMMEDIATELY"
    assert res["ticket"] == 555444

    # Check alerts emitted: 1 ORPHAN_DETECTED, 1 ORPHAN_RESOLVED
    alert_types = [a.alert_type for a in alerts]
    assert "ORPHAN_DETECTED" in alert_types
    assert "ORPHAN_RESOLVED" in alert_types

    detect_alert = next(a for a in alerts if a.alert_type == "ORPHAN_DETECTED")
    assert detect_alert.ticket == 555444
    assert detect_alert.symbol == "GBPUSD"
    assert detect_alert.policy == "CLOSE_IMMEDIATELY"

    # Verify Journey log record
    journey_logs = orchestrator.get_journey_logs(ticket=555444, event_type="ORPHAN_CLOSED_IMMEDIATELY")
    assert len(journey_logs) == 1
    assert "FULL_CLOSE" in journey_logs[0]["details"]

    await orchestrator.stop()


# =============================================================================
# 3. Resolution Policy: ADOPT Tests
# =============================================================================

@pytest.mark.asyncio
async def test_orphan_resolution_adopt(mock_dispatcher):
    """
    ADOPT policy:
    1. Triggers ORPHAN_DETECTED alert.
    2. Ingests new record into SQLite order_intents as OPEN.
    3. Registers in in-memory ManagedPositions for TP/SL tracking.
    4. Emits ORPHAN_RESOLVED alert.
    """
    alerts: List[OrphanAlert] = []
    notif_client = OrphanNotificationClient(callbacks=[lambda a: alerts.append(a)])

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )

    mgr = OrphanManager(
        default_policy=OrphanResolutionPolicy.ADOPT,
        pipvane_magic_number=DEFAULT_PIPVANE_MAGIC,
        orchestrator=orchestrator,
        notification_client=notif_client,
    )

    orphan_pos = PositionItem(
        ticket=777888,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="XAUUSD",
        position_type=0,
        volume=0.5,
        open_price=2050.0,
        current_price=2055.0,
        stop_loss=2040.0,
        take_profit=2070.0,
    )

    res = await mgr.handle_orphan("term_1", orphan_pos)

    assert res["action_taken"] == "ADOPTED"
    assert res["success"] is True

    # 1. Verify ingested in SQLite order_intents
    order = orchestrator.get_order_by_ticket(777888)
    assert order is not None
    assert order["status"] == "OPEN"
    assert order["symbol"] == "XAUUSD"
    assert order["volume"] == 0.5
    assert order["open_price"] == 2050.0

    # 2. Verify registered in ManagedPositions
    managed_pos = orchestrator.get_managed_position(777888)
    assert managed_pos is not None
    assert managed_pos.ticket == 777888
    assert managed_pos.closed is False
    assert managed_pos.current_sl == 2040.0

    # 3. Check Journey log
    journey = orchestrator.get_journey_logs(ticket=777888, event_type="ORPHAN_ADOPTED")
    assert len(journey) == 1

    # 4. Check alerts
    alert_types = [a.alert_type for a in alerts]
    assert "ORPHAN_DETECTED" in alert_types
    assert "ORPHAN_RESOLVED" in alert_types

    await orchestrator.stop()


# =============================================================================
# 4. Resolution Policy: IGNORE Tests
# =============================================================================

@pytest.mark.asyncio
async def test_orphan_resolution_ignore(mock_dispatcher):
    """
    IGNORE policy:
    1. Triggers ORPHAN_DETECTED alert.
    2. Leaves trade unmanaged on broker.
    3. Emits ORPHAN_IGNORED alert and records Journey log.
    """
    alerts: List[OrphanAlert] = []
    notif_client = OrphanNotificationClient(callbacks=[lambda a: alerts.append(a)])

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )

    mgr = OrphanManager(
        default_policy=OrphanResolutionPolicy.IGNORE,
        pipvane_magic_number=DEFAULT_PIPVANE_MAGIC,
        orchestrator=orchestrator,
        notification_client=notif_client,
    )

    orphan_pos = PositionItem(
        ticket=999111,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_time_unix=int(time.time()),
        symbol="USDJPY",
        position_type=1, # SELL
        volume=0.3,
        open_price=150.0,
        current_price=149.8,
    )

    res = await mgr.handle_orphan("term_1", orphan_pos)

    assert res["action_taken"] == "IGNORED"
    assert res["success"] is True

    # Position is NOT added as OPEN in database
    order = orchestrator.get_order_by_ticket(999111)
    assert order is None

    # Alerts emitted
    alert_types = [a.alert_type for a in alerts]
    assert "ORPHAN_DETECTED" in alert_types
    assert "ORPHAN_IGNORED" in alert_types

    # Journey log contains ORPHAN_IGNORED
    journey = orchestrator.get_journey_logs(ticket=999111, event_type="ORPHAN_IGNORED")
    assert len(journey) == 1

    await orchestrator.stop()


# =============================================================================
# 5. Integration with Orchestrator Reconciliation Cycle
# =============================================================================

@pytest.mark.asyncio
async def test_reconciliation_cycle_discovers_and_resolves_orphan(mock_dispatcher):
    """
    End-to-end integration:
    During a reconciliation cycle, if broker dump contains an orphan position,
    reconcile_terminal automatically catches it, alerts, and applies the policy.
    """
    alerts: List[OrphanAlert] = []
    notif_client = OrphanNotificationClient(callbacks=[lambda a: alerts.append(a)])

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        orphan_policy=OrphanResolutionPolicy.ADOPT, # Policy: ADOPT
    )
    orchestrator.orphan_manager.notification_client = notif_client

    # Broker dump with 1 normal trade and 1 orphan trade
    # Normal trade in DB
    orchestrator.record_order_intent(
        signal_id=1234,
        ticket=101010,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        terminal_id="term_1",
        symbol="EURUSD",
        side=0,
        volume=0.1,
        open_price=1.0800,
        status="OPEN",
    )

    broker_positions = [
        PositionItem(
            ticket=101010, # Normal managed trade
            magic_number=DEFAULT_PIPVANE_MAGIC,
            open_time_unix=int(time.time()),
            symbol="EURUSD",
            position_type=0,
            volume=0.1,
            open_price=1.0800,
            current_price=1.0810,
        ),
        PositionItem(
            ticket=202020, # Injected orphan trade!
            magic_number=DEFAULT_PIPVANE_MAGIC,
            open_time_unix=int(time.time()),
            symbol="GBPUSD",
            position_type=1,
            volume=0.4,
            open_price=1.2750,
            current_price=1.2740,
        ),
    ]

    summary = await orchestrator.reconcile_terminal("term_1", broker_positions=broker_positions)

    assert summary["audited_orders"] == 1
    assert summary["externally_closed"] == 0
    assert summary["orphans_detected"] == 1
    assert len(summary["orphans_handled"]) == 1
    assert summary["orphans_handled"][0]["ticket"] == 202020
    assert summary["orphans_handled"][0]["action_taken"] == "ADOPTED"

    # Orphan was adopted into DB
    adopted_record = orchestrator.get_order_by_ticket(202020)
    assert adopted_record is not None
    assert adopted_record["status"] == "OPEN"
    assert adopted_record["symbol"] == "GBPUSD"

    # Alert was dispatched
    assert any(a.ticket == 202020 and a.alert_type == "ORPHAN_DETECTED" for a in alerts)

    await orchestrator.stop()


# =============================================================================
# 6. Webhook Notification Dispatch Simulation
# =============================================================================

@pytest.mark.asyncio
async def test_webhook_alert_dispatch():
    """Validates that webhook dispatch method packages valid JSON payload."""
    dispatched_payloads = []

    client = OrphanNotificationClient(webhook_url="http://mock-webhook.local/alerts")

    # Monkeypatch _dispatch_webhook to capture the payload without actual network call
    async def mock_dispatch(alert: OrphanAlert) -> bool:
        dispatched_payloads.append(alert.to_dict())
        return True

    client._dispatch_webhook = mock_dispatch

    alert = OrphanAlert(
        alert_type="ORPHAN_DETECTED",
        terminal_id="term_1",
        ticket=303030,
        symbol="XAUUSD",
        volume=1.0,
        magic_number=DEFAULT_PIPVANE_MAGIC,
        open_price=2000.0,
        side=0,
        policy="CLOSE_IMMEDIATELY",
        details={"reason": "UNAUTHORIZED_BROKER_INJECTION"},
    )

    await client.notify(alert)

    assert len(dispatched_payloads) == 1
    p = dispatched_payloads[0]
    assert p["alert_type"] == "ORPHAN_DETECTED"
    assert p["ticket"] == 303030
    assert p["symbol"] == "XAUUSD"
    assert p["side"] == "BUY"
    assert p["policy"] == "CLOSE_IMMEDIATELY"
