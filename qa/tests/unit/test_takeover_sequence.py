"""
Unit tests for FailoverManager State Promotion Checklist (Sprint 16 Directive).
================================================================================
Validates the ExecuteTakeover routine:
1. Role Transition Trigger: claim_lease triggers ExecuteTakeover on promotion.
2. Step A: Connect to TDLib or activate message parsing.
3. Step B: Execute MessageBackfill process.
4. Step C: Run immediate ReconciliationLoop to download live broker positions.
5. Step D: Unlock ExecutionOrchestrator.
6. Safety Timeout: If checklist exceeds 45s (configurable for testing), aborts,
   surrenders lease back to Cloud, triggers critical alert, and keeps orchestrator locked.
7. Error Handling: Failure in any step aborts and surrenders lease.
"""

import asyncio
import json
import time
from typing import Dict, Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.failover_manager import (
    FailoverManager,
    FailoverManagerConfig,
    EngineRole,
    TakeoverError,
    TakeoverTimeoutError,
    TakeoverStep,
    TakeoverResult,
    DEFAULT_TAKEOVER_TIMEOUT_S,
)
from engine.execution_orchestrator import ExecutionOrchestrator
from engine.ipc_dispatcher import IPCDispatcher


@pytest.fixture
def mock_dispatcher():
    return IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)


@pytest.fixture
def mock_orchestrator(mock_dispatcher):
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        initial_role=EngineRole.ROLE_STANDBY,
    )
    return orchestrator


@pytest.mark.asyncio
async def test_full_takeover_sequence_success(mock_orchestrator):
    """
    Verifies that ExecuteTakeover executes Step A -> Step B -> Step C -> Step D
    synchronously in sequence, and successfully unlocks ExecutionOrchestrator.
    """
    call_order: List[str] = []

    # Mock Telegram Gateway
    mock_gateway = MagicMock()
    mock_gateway.is_connected = False

    async def fake_connect():
        call_order.append("STEP_A_CONNECT")
        mock_gateway.is_connected = True

    def fake_activate_parsing():
        call_order.append("STEP_A_ACTIVATE_PARSING")

    async def fake_backfill():
        call_order.append("STEP_B_BACKFILL")

    mock_gateway.connect = fake_connect
    mock_gateway.activate_parsing = fake_activate_parsing
    mock_gateway.backfill_all_monitored_chats = fake_backfill

    # Mock Ingestion Controller
    mock_ingestion = MagicMock()
    def fake_ingestion_activate():
        call_order.append("STEP_A_INGESTION_ACTIVATE")
    mock_ingestion.activate_parsing = fake_ingestion_activate

    # Mock Orchestrator Reconciliation
    async def fake_reconciliation():
        call_order.append("STEP_C_RECONCILIATION")
        return {"term-1": {"audited_orders": 2, "externally_closed": 0}}

    mock_orchestrator.reconcile_all_terminals = fake_reconciliation

    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-PROMO",
            node_id="standby_node_01",
            takeover_timeout_s=45.0,
        ),
        orchestrator=mock_orchestrator,
        telegram_gateway=mock_gateway,
        ingestion_controller=mock_ingestion,
    )
    mgr.role = EngineRole.ROLE_STANDBY
    mgr.lease_token = "token_granted_01"
    mgr.lease_expires_at = time.time() + 30.0
    mock_orchestrator.set_engine_role(EngineRole.ROLE_STANDBY)

    # Pre-condition: Orchestrator is locked
    assert mock_orchestrator.is_standby() is True
    assert mock_orchestrator.is_active() is False

    # Execute Takeover
    result: TakeoverResult = await mgr.execute_takeover()

    # Verify result status
    assert result.success is True
    assert result.step_a_completed is True
    assert result.step_b_completed is True
    assert result.step_c_completed is True
    assert result.step_d_completed is True
    assert result.lease_surrendered is False

    # Verify synchronous sequence order
    assert "STEP_A_CONNECT" in call_order
    assert "STEP_A_ACTIVATE_PARSING" in call_order
    assert "STEP_B_BACKFILL" in call_order
    assert "STEP_C_RECONCILIATION" in call_order

    idx_a = call_order.index("STEP_A_CONNECT")
    idx_b = call_order.index("STEP_B_BACKFILL")
    idx_c = call_order.index("STEP_C_RECONCILIATION")

    assert idx_a < idx_b < idx_c, f"Steps must execute in order A -> B -> C, got {call_order}"

    # Step D Verification: Orchestrator is now UNLOCKED
    assert mgr.role == EngineRole.ROLE_ACTIVE
    assert mgr.is_active is True
    assert mock_orchestrator.is_active() is True
    assert mock_orchestrator.is_standby() is False
    assert mgr.can_execute_orders is True


@pytest.mark.asyncio
async def test_claim_lease_triggers_execute_takeover(mock_orchestrator):
    """
    Verifies the Role Transition Trigger: When claim_lease successfully claims
    a lease from the Cloud Coordinator, it triggers ExecuteTakeover().
    """
    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-PROMO",
            node_id="standby_node_02",
        ),
        orchestrator=mock_orchestrator,
    )
    mgr.role = EngineRole.ROLE_STANDBY
    mock_orchestrator.set_engine_role(EngineRole.ROLE_STANDBY)

    # Track takeover invocation
    takeover_invoked = False
    async def fake_takeover(timeout_s=None):
        nonlocal takeover_invoked
        takeover_invoked = True
        mock_orchestrator.unlock()
        mgr.set_role(EngineRole.ROLE_ACTIVE)
        return TakeoverResult(
            success=True,
            node_id="standby_node_02",
            started_at=time.time(),
            completed_at=time.time(),
            elapsed_s=0.01,
            step_a_completed=True,
            step_b_completed=True,
            step_c_completed=True,
            step_d_completed=True,
        )

    mgr.execute_takeover = fake_takeover

    # Mock WebSocket receiving grant
    mock_ws = MagicMock()
    mock_ws.closed = False
    mock_ws.send = MagicMock(return_value=asyncio.sleep(0))
    grant_resp = json.dumps({
        "status": "granted",
        "role": "Active",
        "holder": "standby_node_02",
        "lease_token": "token_granted_44",
        "expires_at": time.time() + 30.0,
        "generation": 3,
    })
    fut = asyncio.Future()
    fut.set_result(grant_resp)
    mock_ws.recv = MagicMock(return_value=fut)
    mgr._ws = mock_ws

    resp = await mgr.claim_lease()

    assert resp["status"] == "granted"
    assert takeover_invoked is True
    assert mgr.role == EngineRole.ROLE_ACTIVE
    assert mock_orchestrator.is_active() is True
    assert "takeover_result" in resp


@pytest.mark.asyncio
async def test_takeover_safety_timeout_surrenders_lease_and_alerts(mock_orchestrator):
    """
    Safety Timeout Requirement:
    If the checklist takes longer than 45 seconds to complete:
    - Abort the takeover.
    - Surrender the lease back to the Cloud.
    - Trigger a critical alert.
    - Keep ExecutionOrchestrator locked in Standby mode.
    """
    alerts_fired: List[Dict[str, Any]] = []
    def alert_callback(alert_type: str, details: Dict[str, Any]):
        alerts_fired.append({"alert_type": alert_type, "details": details})

    # Step C hangs indefinitely
    async def hanging_reconciliation():
        await asyncio.sleep(10.0)
        return {}

    mock_orchestrator.reconcile_all_terminals = hanging_reconciliation

    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-TIMEOUT",
            node_id="timeout_node_01",
            takeover_timeout_s=0.05,  # Accelerated timeout for testing
        ),
        orchestrator=mock_orchestrator,
        alert_client=alert_callback,
    )
    mgr.role = EngineRole.ROLE_INITIALIZING
    mgr.lease_token = "token_timeout_test"
    mgr.lease_expires_at = time.time() + 30.0

    # Track release_lease call
    lease_surrendered = False
    async def mock_release():
        nonlocal lease_surrendered
        lease_surrendered = True

    mgr.release_lease = mock_release

    # Execute takeover with 0.05s safety timeout
    with pytest.raises(TakeoverTimeoutError) as exc_info:
        await mgr.execute_takeover(timeout_s=0.05)

    assert "timed out" in str(exc_info.value).lower()

    # 1. Lease was surrendered back to the Cloud
    assert lease_surrendered is True
    assert mgr.lease_token is None

    # 2. Critical alert was triggered
    assert len(alerts_fired) >= 1
    assert alerts_fired[0]["alert_type"] == "TAKEOVER_ABORTED"
    assert alerts_fired[0]["details"]["reason"] == "SAFETY_TIMEOUT_EXCEEDED"

    # 3. ExecutionOrchestrator remains LOCKED
    assert mgr.role == EngineRole.ROLE_STANDBY
    assert mock_orchestrator.is_standby() is True
    assert mock_orchestrator.is_active() is False
    assert mgr.can_execute_orders is False


@pytest.mark.asyncio
async def test_takeover_step_failure_aborts_and_surrenders_lease(mock_orchestrator):
    """
    If any step (e.g. Step C reconciliation) raises an unexpected error:
    - Aborts takeover.
    - Surrenders lease back to Cloud.
    - Triggers critical alert.
    - Keeps orchestrator locked.
    """
    alerts_fired: List[Dict[str, Any]] = []
    def alert_callback(alert_type: str, details: Dict[str, Any]):
        alerts_fired.append({"alert_type": alert_type, "details": details})

    # Step C fails with exception
    async def failing_reconciliation():
        raise RuntimeError("Broker Named Pipe connection lost during audit dump")

    mock_orchestrator.reconcile_all_terminals = failing_reconciliation

    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-FAIL",
            node_id="failing_node_01",
        ),
        orchestrator=mock_orchestrator,
        alert_client=alert_callback,
    )
    mgr.lease_token = "token_to_surrender"
    mgr.role = EngineRole.ROLE_INITIALIZING

    lease_surrendered = False
    async def mock_release():
        nonlocal lease_surrendered
        lease_surrendered = True

    mgr.release_lease = mock_release

    with pytest.raises(TakeoverError) as exc_info:
        await mgr.execute_takeover()

    assert "Broker Named Pipe connection lost" in str(exc_info.value)
    assert lease_surrendered is True
    assert mgr.lease_token is None
    assert mgr.role == EngineRole.ROLE_STANDBY
    assert mock_orchestrator.is_standby() is True

    # Alert was sent
    assert len(alerts_fired) >= 1
    assert alerts_fired[0]["alert_type"] == "TAKEOVER_ABORTED"
    assert alerts_fired[0]["details"]["reason"] == "TAKEOVER_EXECUTION_ERROR"


@pytest.mark.asyncio
async def test_execute_takeover_pascal_case_alias(mock_orchestrator):
    """Verifies ExecuteTakeover alias matches directive specification."""
    mgr = FailoverManager(orchestrator=mock_orchestrator)
    assert hasattr(mgr, "ExecuteTakeover")
    assert mgr.ExecuteTakeover == mgr.execute_takeover
    res = await mgr.ExecuteTakeover()
    assert res.success is True
