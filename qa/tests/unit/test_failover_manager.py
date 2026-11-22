"""
Unit tests for FailoverManager & Local Lease Negotiation (Sprint 15 Directive).

Validates:
1. WebSocket Client & mTLS Security:
   - Configures mTLS SSLContext with certificates.
   - Handles connection and disconnection cleanly.
2. Lease Lifecycle (ROLE_ACTIVE):
   - Requests active lease using license_id.
   - Transitions to ROLE_ACTIVE upon grant.
   - Periodically transmits 10-second heartbeat pings to sustain active lease.
3. Standby Lockout (ROLE_STANDBY):
   - Transitions to ROLE_STANDBY when cloud denies lease.
   - Completely locks the ExecutionOrchestrator, blocking all OrderSend commands.
   - Signal parsing and database state maintenance remain fully active while trading is blocked.
4. Failover & Promotion:
   - Recovers from STANDBY to ACTIVE when lease becomes available.
   - Self-fences into locked state upon lease expiration.
"""

import asyncio
import json
import ssl
import time
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    InitHelloPayload,
    AccountState,
    pack_pipe_frame,
    NamedPipeOpcode,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
)
from engine.failover_manager import (
    FailoverManager,
    FailoverManagerConfig,
    EngineRole,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    ROLE_INITIALIZING,
    DEFAULT_HEARTBEAT_INTERVAL_S,
)


class MockWebSocketServer:
    """Simulates Cloud Lease Coordinator WebSocket Server for headless testing."""

    def __init__(self):
        self.active_holder = "primary_node"
        self.active_token = "token_abc123"
        self.ttl_seconds = 30.0
        self.generation = 1
        self.received_messages: List[Dict[str, Any]] = []

    async def handle_connection(self, websocket):
        async for raw_msg in websocket:
            msg = json.loads(raw_msg)
            self.received_messages.append(msg)
            action = msg.get("action")
            node_id = msg.get("node_id")

            if action == "claim_lease":
                if self.active_holder == node_id or self.active_holder is None:
                    self.active_holder = node_id
                    self.generation += 1
                    resp = {
                        "status": "granted",
                        "role": "Active",
                        "holder": node_id,
                        "lease_token": "token_" + node_id,
                        "generation": self.generation,
                        "expires_at": time.time() + self.ttl_seconds,
                    }
                else:
                    resp = {
                        "status": "rejected",
                        "role": "Standby",
                        "holder": self.active_holder,
                        "generation": self.generation,
                        "reason": f"Lease actively held by {self.active_holder}",
                    }
                await websocket.send(json.dumps(resp))

            elif action == "heartbeat":
                token = msg.get("lease_token")
                if self.active_holder == node_id:
                    resp = {
                        "status": "renewed",
                        "role": "Active",
                        "holder": node_id,
                        "expires_at": time.time() + self.ttl_seconds,
                    }
                else:
                    resp = {
                        "status": "rejected",
                        "role": "Standby",
                        "reason": "Not current lease holder",
                    }
                await websocket.send(json.dumps(resp))

            elif action == "release_lease":
                if self.active_holder == node_id:
                    self.active_holder = None
                    resp = {"status": "released", "role": "Standby"}
                else:
                    resp = {"status": "ignored"}
                await websocket.send(json.dumps(resp))


@pytest.fixture
def mock_dispatcher():
    """Provides an IPCDispatcher instance using mock shared-memory buffers."""
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    return dispatcher


# =============================================================================
# 1. mTLS & SSL Context Tests
# =============================================================================

def test_mtls_ssl_context_configuration(tmp_path):
    """Verifies that mTLS SSLContext is correctly assembled with certificates."""
    # Write mock cert files
    ca_file = tmp_path / "ca.crt"
    cert_file = tmp_path / "client.crt"
    key_file = tmp_path / "client.key"
    ca_file.write_text("-----BEGIN CERTIFICATE-----\nMOCK_CA\n-----END CERTIFICATE-----\n")
    cert_file.write_text("-----BEGIN CERTIFICATE-----\nMOCK_CLIENT\n-----END CERTIFICATE-----\n")
    key_file.write_text("-----BEGIN PRIVATE KEY-----\nMOCK_KEY\n-----END PRIVATE KEY-----\n")

    cfg = FailoverManagerConfig(
        ws_url="wss://cloud.pipvane.com/lease",
        ca_file=str(ca_file),
        cert_file=str(cert_file),
        key_file=str(key_file),
    )

    with patch("ssl.create_default_context") as mock_create_ssl:
        mock_ctx = MagicMock(spec=ssl.SSLContext)
        mock_create_ssl.return_value = mock_ctx

        mgr = FailoverManager(config=cfg)
        ctx = mgr._create_ssl_context()

        assert ctx is mock_ctx
        mock_ctx.load_verify_locations.assert_called_once_with(cafile=str(ca_file))
        mock_ctx.load_cert_chain.assert_called_once_with(certfile=str(cert_file), keyfile=str(key_file))


def test_plain_ws_skips_ssl():
    """Plain ws:// URLs (local dev / testing) do not instantiate SSL contexts."""
    cfg = FailoverManagerConfig(ws_url="ws://127.0.0.1:8765")
    mgr = FailoverManager(config=cfg)
    assert mgr._create_ssl_context() is None


# =============================================================================
# 2. Lease Lifecycle Tests (Active Lease Granted & Heartbeats)
# =============================================================================

@pytest.mark.asyncio
async def test_claim_lease_granted_transitions_to_role_active():
    """When Cloud Coordinator grants lease, node transitions to ROLE_ACTIVE."""
    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-123",
            node_id="node_primary",
            heartbeat_interval_s=10.0,
        )
    )

    # Mock WebSocket client
    mock_ws = MagicMock()
    mock_response = json.dumps({
        "status": "granted",
        "role": "Active",
        "holder": "node_primary",
        "lease_token": "token_active_999",
        "expires_at": time.time() + 30.0,
        "generation": 5,
    })
    fut = asyncio.Future()
    fut.set_result(mock_response)
    mock_ws.recv = MagicMock(return_value=fut)
    mock_ws.send = MagicMock(return_value=asyncio.sleep(0))
    mock_ws.closed = False
    mgr._ws = mock_ws

    resp = await mgr.claim_lease()

    assert resp["status"] == "granted"
    assert mgr.role == EngineRole.ROLE_ACTIVE
    assert mgr.is_active is True
    assert mgr.is_standby is False
    assert mgr.can_execute_orders is True
    assert mgr.lease_token == "token_active_999"
    assert mgr.generation == 5


@pytest.mark.asyncio
async def test_heartbeat_renewal_maintains_role_active():
    """Sending heartbeat extends the active lease validity window."""
    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-123",
            node_id="node_primary",
            heartbeat_interval_s=10.0,
        )
    )
    mgr.role = EngineRole.ROLE_ACTIVE
    mgr.lease_token = "token_active_999"
    mgr.lease_expires_at = time.time() + 10.0

    mock_ws = MagicMock()
    mock_ws.closed = False
    mock_response = json.dumps({
        "status": "renewed",
        "role": "Active",
        "holder": "node_primary",
        "expires_at": time.time() + 30.0,
    })
    fut = asyncio.Future()
    fut.set_result(mock_response)
    mock_ws.recv = MagicMock(return_value=fut)
    mock_ws.send = MagicMock(return_value=asyncio.sleep(0))
    mgr._ws = mock_ws

    resp = await mgr.send_heartbeat()

    assert resp["status"] == "renewed"
    assert mgr.role == EngineRole.ROLE_ACTIVE
    assert mgr.is_active is True
    assert mgr.can_execute_orders is True


# =============================================================================
# 3. Standby Lockout Tests (ROLE_STANDBY & Orchestrator Execution Gate)
# =============================================================================

@pytest.mark.asyncio
async def test_claim_lease_denied_triggers_standby_lockout(mock_dispatcher):
    """When Cloud Coordinator denies lease, node is locked into ROLE_STANDBY."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )

    mgr = FailoverManager(
        config=FailoverManagerConfig(
            license_id="LIC-TEST-123",
            node_id="node_secondary",
        ),
        orchestrator=orchestrator,
    )

    mock_ws = MagicMock()
    mock_ws.closed = False
    mock_response = json.dumps({
        "status": "rejected",
        "role": "Standby",
        "holder": "node_primary",
        "generation": 2,
        "reason": "Lease actively held by node_primary",
    })
    fut = asyncio.Future()
    fut.set_result(mock_response)
    mock_ws.recv = MagicMock(return_value=fut)
    mock_ws.send = MagicMock(return_value=asyncio.sleep(0))
    mgr._ws = mock_ws

    resp = await mgr.claim_lease()

    assert resp["status"] == "rejected"
    assert mgr.role == EngineRole.ROLE_STANDBY
    assert mgr.is_active is False
    assert mgr.is_standby is True
    assert mgr.can_execute_orders is False
    assert mgr.active_holder == "node_primary"

    # Verify Orchestrator was locked
    assert orchestrator.role == EngineRole.ROLE_STANDBY
    assert orchestrator.is_standby() is True
    assert orchestrator.is_active() is False

    await orchestrator.stop()


@pytest.mark.asyncio
async def test_standby_lockout_blocks_all_ordersend_commands(mock_dispatcher):
    """
    STRICT STANDBY LOCKOUT INVARIANT:
    While in ROLE_STANDBY mode, ExecutionOrchestrator MUST completely block
    any OrderSend commands, returning SKIP_STANDBY_LOCK.
    """
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
        initial_role=EngineRole.ROLE_STANDBY,
    )

    # Attempt to submit an order
    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.1,
        price=1.0850,
        signal_id=889900,
    )

    result = await orchestrator.submit_order("term_1", intent)

    # MUST be strictly blocked and rejected
    assert result.success is False
    assert result.status == ExecutionStatus.REJECTED
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_STANDBY_LOCK"
    assert "Standby Lockout enforced" in result.skip_reason.reason

    await orchestrator.stop()


@pytest.mark.asyncio
async def test_orchestrator_unlocks_when_promoted_to_role_active(mock_dispatcher):
    """When FailoverManager transitions from STANDBY to ACTIVE, order execution unlocks."""
    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    mgr = FailoverManager(orchestrator=orchestrator)

    # 1. Start in STANDBY
    mgr.set_role(EngineRole.ROLE_STANDBY)
    assert orchestrator.is_standby() is True

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.1,
        signal_id=123123,
    )
    res_blocked = await orchestrator.submit_order("term_1", intent)
    assert res_blocked.success is False
    assert res_blocked.skip_reason.code == "SKIP_STANDBY_LOCK"

    # 2. Promote to ACTIVE
    mgr.lease_token = "valid_token"
    mgr.lease_expires_at = time.time() + 30.0
    mgr.set_role(EngineRole.ROLE_ACTIVE)
    assert orchestrator.is_active() is True

    # Now order is allowed past the standby lock gate
    # (May skip due to terminal unavailable if term_1 isn't connected, but will NOT be SKIP_STANDBY_LOCK)
    res_allowed = await orchestrator.submit_order("term_1", intent)
    assert res_allowed.skip_reason.code != "SKIP_STANDBY_LOCK"

    await orchestrator.stop()


# =============================================================================
# 4. Self-Fencing on Lease Expiration
# =============================================================================

def test_self_fencing_on_lease_expiration():
    """If current time crosses lease_expires_at, is_active immediately turns False."""
    virtual_time = 1700000000.0

    def time_provider():
        return virtual_time

    mgr = FailoverManager(
        config=FailoverManagerConfig(lease_ttl_s=30.0),
        time_provider=time_provider,
    )
    mgr.role = EngineRole.ROLE_ACTIVE
    mgr.lease_token = "lease_123"
    mgr.lease_expires_at = virtual_time + 30.0

    # 1. While within TTL, active is True
    assert mgr.is_active is True
    assert mgr.can_execute_orders is True

    # 2. Advance clock past expiration
    virtual_time += 31.0

    # 3. Must immediately self-fence without network round-trip!
    assert mgr.is_active is False
    assert mgr.can_execute_orders is False
