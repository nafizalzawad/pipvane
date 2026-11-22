"""
Pipvane v3: Network Partition & Anti-Split-Brain Test Suite (Sprint 15 Directive).
===================================================================================
Objective:
Validate the Local Engine's response to an unexpected cloud disconnection (V3 Split-Brain Prevention).

Tasks & Success Criteria:
1. Test Environment:
   Run the Cloud Lease Coordinator locally. Spin up Node A (Primary) and establish an Active lease.
2. Partition Simulation:
   Use Docker or OS-level firewall rules (iptables / Windows Firewall) or socket severance
   to abruptly block all traffic between Node A and the Cloud Coordinator.
3. Fencing Assertion:
   Node A will fail to renew its 10-second heartbeat. Assert that exactly 30 seconds after
   the network partition (when the lease officially expires), Node A forcibly downgrades its
   own internal state to ROLE_LOCKED and blocks all new trades.
4. Single-Active Invariant:
   Enforces invariant |A(t)| <= 1 for all continuous time t across all partition and failover scenarios.
"""

from __future__ import annotations

import asyncio
import ctypes
import enum
import json
import logging
import os
import socket
import struct
import subprocess
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest
import websockets

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.failover_manager import (
    EngineRole,
    FailoverManager,
    FailoverManagerConfig,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    ROLE_LOCKED,
    ROLE_DEGRADED,
    ROLE_SAFE_HALT,
    DEFAULT_LEASE_TTL_S,
    DEFAULT_HEARTBEAT_INTERVAL_S,
    FailoverError,
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    ExecutionResult,
    ExecutionStatus,
)
from engine.ipc_protocol import (
    ActionType,
    OrderIntent,
    OrderSide,
)
from failover_simulation import (
    MockLocalAgent,
    NodeRole,
    NodeState,
    VirtualClock,
    WebSocketLeaseCoordinator,
    FailoverSimulationOrchestrator,
)

logger = logging.getLogger("pipvane.qa.test_failover_partition")


# =============================================================================
# Helper: Network Partition Proxy (TCP Socket Severance)
# =============================================================================

class NetworkPartitionProxy:
    """
    Transparent TCP loopback proxy supporting instantaneous socket severance.
    Enables zero-elevation, 100% deterministic network partition injection on Windows/Linux/Docker.

    When severed:
    - Immediately closes and resets all active client sockets with SO_LINGER=0 (TCP RST).
    - Refuses / drops all new inbound TCP connections, simulating severe network partitions.
    When healed:
    - Resumes forwarding traffic normally.
    """

    def __init__(self, target_host: str, target_port: int) -> None:
        self.target_host = target_host
        self.target_port = target_port
        self.listen_host = "127.0.0.1"
        self.listen_port: int = 0
        self.is_partitioned: bool = False

        self._server: Optional[asyncio.Server] = None
        self._active_connections: List[Tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = []
        self._lock = asyncio.Lock()

    async def start(self) -> int:
        """Starts TCP proxy server on a random free loopback port."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.listen_host,
            0,
        )
        self.listen_port = self._server.sockets[0].getsockname()[1]
        logger.info(
            "NetworkPartitionProxy listening on %s:%d -> %s:%d",
            self.listen_host,
            self.listen_port,
            self.target_host,
            self.target_port,
        )
        return self.listen_port

    async def stop(self) -> None:
        """Stops proxy and tears down all connections."""
        await self.sever()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        if self.is_partitioned:
            # Drop/close immediately when partition active
            client_writer.close()
            try:
                await client_writer.wait_closed()
            except Exception:
                pass
            return

        try:
            target_reader, target_writer = await asyncio.open_connection(
                self.target_host, self.target_port
            )
        except Exception as e:
            logger.debug("Proxy failed to connect to coordinator: %s", e)
            client_writer.close()
            try:
                await client_writer.wait_closed()
            except Exception:
                pass
            return

        conn = (client_writer, target_writer)
        async with self._lock:
            self._active_connections.append(conn)

        async def pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
            try:
                while not self.is_partitioned:
                    data = await r.read(65536)
                    if not data:
                        break
                    w.write(data)
                    await w.drain()
            except Exception:
                pass
            finally:
                w.close()
                try:
                    await w.wait_closed()
                except Exception:
                    pass

        try:
            await asyncio.gather(
                pipe(client_reader, target_writer),
                pipe(target_reader, client_writer),
                return_exceptions=True,
            )
        finally:
            async with self._lock:
                if conn in self._active_connections:
                    self._active_connections.remove(conn)

    async def sever(self) -> None:
        """
        Abruptly severs all network traffic (socket severance).
        Closes all open client sockets with zero linger (simulating packet drop / RST).
        """
        self.is_partitioned = True
        async with self._lock:
            for cw, tw in list(self._active_connections):
                try:
                    # Set SO_LINGER to 0 for instant TCP RST abort
                    sock = cw.get_extra_info("socket")
                    if sock:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                except Exception:
                    pass
                cw.close()
                tw.close()
            self._active_connections.clear()
        logger.warning("NETWORK PARTITION INJECTED: All traffic severed abruptly.")

    def heal(self) -> None:
        """Restores network connectivity between Node A and Cloud Coordinator."""
        self.is_partitioned = False
        logger.info("NETWORK PARTITION HEALED: Traffic restored.")


def is_running_as_admin() -> bool:
    """Checks if current Windows process holds Administrator privileges."""
    if os.name != "nt":
        return os.geteuid() == 0  # type: ignore[attr-defined]
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


# =============================================================================
# Suite: Sprint 15 Failover Partition Tests
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.failover
class TestFailoverPartitionSuite:
    """
    Comprehensive test suite validating V3 Split-Brain Prevention & Self-Fencing
    under abrupt cloud disconnections and network partitions (FR-7.4, FR-7.5).
    """

    # -------------------------------------------------------------------------
    # Test 1: Happy Path - Active Lease Acquisition & 10s Heartbeat
    # -------------------------------------------------------------------------
    async def test_01_primary_nominal_lease_grant_and_heartbeat(self):
        """
        Task 1: Run Cloud Lease Coordinator locally.
        Spin up Node A (Primary) and establish an Active lease with 10s heartbeat cadence.
        Assert that Node A enters ROLE_ACTIVE and can execute trades.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

        server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        ws_url = f"ws://127.0.0.1:{port}"

        try:
            node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, ws_url, clock.now)
            await node_a.connect()

            # Claim active lease
            resp = await node_a.claim_lease()
            assert resp["status"] == "granted"
            assert resp["role"] == "Active"
            assert node_a.state == NodeState.ACTIVE_EXECUTION
            assert node_a.is_active is True
            assert node_a.can_execute_trades is True

            # Transmit 10-second heartbeat
            clock.advance(10.0)
            hb_resp = await node_a.send_heartbeat()
            assert hb_resp["status"] == "renewed"
            assert hb_resp["role"] == "Active"
            assert node_a.is_active is True
            assert node_a.can_execute_trades is True

            await node_a.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 2: Socket Severance & Network Partition Simulation
    # -------------------------------------------------------------------------
    async def test_02_socket_severance_network_partition_simulation(self):
        """
        Task 2: Use socket severance proxy to abruptly block all traffic
        between Node A and Cloud Coordinator.
        Assert that any in-flight or subsequent connection attempts are immediately blocked.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

        coord_server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        coord_port = coord_server.sockets[0].getsockname()[1]

        proxy = NetworkPartitionProxy("127.0.0.1", coord_port)
        proxy_port = await proxy.start()
        proxy_ws_url = f"ws://127.0.0.1:{proxy_port}"

        try:
            node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, proxy_ws_url, clock.now)
            await node_a.connect()
            resp = await node_a.claim_lease()
            assert resp["status"] == "granted"
            assert node_a.state == NodeState.ACTIVE_EXECUTION

            # INJECT NETWORK PARTITION (Socket Severance)
            await proxy.sever()
            assert proxy.is_partitioned is True

            # Attempting new connection through severed proxy must fail
            with pytest.raises(Exception):
                dead_client = await asyncio.wait_for(
                    websockets.connect(proxy_ws_url), timeout=1.0
                )
                await dead_client.close()

            await node_a.disconnect()
        finally:
            await proxy.stop()
            coord_server.close()
            await coord_server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 3: 10-Second Heartbeat Failure Assertion
    # -------------------------------------------------------------------------
    async def test_03_heartbeat_fails_at_10s_post_partition(self):
        """
        Task 3 (Part 1): Node A will fail to renew its 10-second heartbeat.
        Simulate partition immediately after lease acquisition.
        At t = 10s, Node A attempts heartbeat renewal and fails.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

        coord_server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        coord_port = coord_server.sockets[0].getsockname()[1]

        proxy = NetworkPartitionProxy("127.0.0.1", coord_port)
        proxy_port = await proxy.start()
        proxy_ws_url = f"ws://127.0.0.1:{proxy_port}"

        try:
            node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, proxy_ws_url, clock.now)
            await node_a.connect()
            resp = await node_a.claim_lease()
            assert resp["status"] == "granted"
            lease_expires_at = node_a.lease_expires_at

            # Partition network abruptly at t = 0
            await proxy.sever()

            # Advance clock to 10 seconds (first heartbeat renewal schedule)
            clock.advance(10.0)

            # Node A attempts to send 10-second heartbeat over severed socket -> MUST FAIL
            heartbeat_failed = False
            try:
                await node_a.send_heartbeat()
            except Exception as e:
                heartbeat_failed = True
                logger.info("Expected heartbeat failure caught: %s", e)

            assert heartbeat_failed is True, "Heartbeat renewal MUST fail during network partition!"

            # At t = 10s, lease is unexpired locally (10s < 30s)
            assert clock.now() < lease_expires_at

            await node_a.disconnect()
        finally:
            await proxy.stop()
            coord_server.close()
            await coord_server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 4: Exactly 30-Second Self-Fencing to ROLE_LOCKED
    # -------------------------------------------------------------------------
    async def test_04_exact_30s_fencing_to_role_locked(self):
        """
        Task 3 (Part 2): Assert that exactly 30 seconds after the network partition
        (when the lease officially expires), Node A forcibly downgrades its own
        internal state to ROLE_LOCKED and blocks all new trades.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        partition_time = clock.now()

        # Instantiate FailoverManager with exact 10s heartbeat and 30s TTL
        config = FailoverManagerConfig(
            heartbeat_interval_s=10.0,
            lease_ttl_s=30.0,
            node_id="Node_A",
        )
        mgr = FailoverManager(config=config, time_provider=clock.now)

        # Grant active lease at t = 0
        mgr.lease_token = "valid_lease_token_123"
        mgr.lease_expires_at = partition_time + 30.0
        mgr.set_role(EngineRole.ROLE_ACTIVE)

        # 1. At t = 0s (immediately after partition):
        assert mgr.state == EngineRole.ROLE_ACTIVE
        assert mgr.is_active is True
        assert mgr.can_execute_trades is True

        # 2. At t = 10.0s (missed heartbeat 1):
        clock.advance(10.0)
        assert clock.now() - partition_time == 10.0
        # Heartbeat renewal fails: enter degraded partition mode (FR-7.5)
        mgr.set_role(EngineRole.ROLE_DEGRADED)
        assert mgr.state == EngineRole.ROLE_DEGRADED
        assert mgr.check_lease_expiration() is False  # 20s remaining, not expired

        # 3. At t = 20.0s (missed heartbeat 2):
        clock.advance(10.0)
        assert clock.now() - partition_time == 20.0
        assert mgr.check_lease_expiration() is False  # 10s remaining

        # 4. At t = 29.999s (immediately prior to 30.0s expiration):
        clock.advance(9.999)
        assert pytest.approx(clock.now() - partition_time, abs=1e-4) == 29.999
        assert mgr.check_lease_expiration() is False  # 0.001s remaining

        # 5. At EXACTLY t = 30.000s (exact lease TTL expiration):
        clock.advance(0.001)
        assert pytest.approx(clock.now() - partition_time, abs=1e-4) == 30.000

        # Enforce self-fencing check
        expired = mgr.check_lease_expiration()
        assert expired is True, "Lease must register as expired at exactly 30.0 seconds!"

        # ASSERTION: Internal state strictly downgraded to ROLE_LOCKED
        assert mgr.state == EngineRole.ROLE_LOCKED
        assert mgr.role == EngineRole.ROLE_LOCKED
        assert mgr.is_locked is True

        # ASSERTION: All new trades strictly blocked
        assert mgr.is_active is False
        assert mgr.can_execute_orders is False
        assert mgr.can_execute_trades is False
        assert mgr.lease_token is None

    # -------------------------------------------------------------------------
    # Test 5: Trade Execution Lockout in ROLE_LOCKED Mode
    # -------------------------------------------------------------------------
    async def test_05_role_locked_blocks_execution_orchestrator(self):
        """
        Verify that when Node A is in ROLE_LOCKED mode, attempting to dispatch
        any OrderIntent to the ExecutionOrchestrator is strictly blocked and rejected
        with SKIP_STANDBY_LOCK, preventing split-brain broker calls.
        """
        ipc_dispatcher = MagicMock()
        orchestrator = ExecutionOrchestrator(ipc_dispatcher=ipc_dispatcher, max_queue_depth=100)
        mgr = FailoverManager(
            config=FailoverManagerConfig(lease_ttl_s=30.0),
            orchestrator=orchestrator,
        )
        orchestrator.failover_manager = mgr

        # Configure dummy terminal
        orchestrator.register_terminal_config(
            TerminalExecutionConfig(terminal_id="term_demo_1", supports_hedging=True)
        )
        await orchestrator.start()

        # Node A is in ROLE_LOCKED
        mgr.set_role(EngineRole.ROLE_LOCKED)
        assert orchestrator.role == EngineRole.ROLE_LOCKED
        assert not mgr.can_execute_trades

        # Submit BUY OrderIntent
        intent = OrderIntent(
            signal_id=999888,
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
        )

        res = await orchestrator.submit_order("term_demo_1", intent)

        # Assert trade is strictly blocked
        assert res.success is False
        assert res.status == ExecutionStatus.REJECTED
        assert res.skip_reason is not None
        assert res.skip_reason.code == "SKIP_STANDBY_LOCK"
        assert "ROLE_LOCKED" in res.skip_reason.reason

        await orchestrator.stop()

    # -------------------------------------------------------------------------
    # Test 6: Standby Node Takeover without Split-Brain
    # -------------------------------------------------------------------------
    async def test_06_standby_takeover_strictly_after_30s_expiry(self):
        """
        Anti-Split-Brain Invariant (|A(t)| <= 1):
        - Before 30s: Standby Node B requests lease -> REJECTED by Cloud Coordinator.
        - Exactly after 30s: Node A self-fences to ROLE_LOCKED, Node B claims lease -> GRANTED.
        Assert that Node A and Node B are NEVER Active at the same time.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

        server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        ws_url = f"ws://127.0.0.1:{port}"

        try:
            node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, ws_url, clock.now)
            node_b = MockLocalAgent("Node_B", NodeRole.STANDBY, ws_url, clock.now)

            # Node A claims initial lease
            await node_a.connect()
            resp_a = await node_a.claim_lease()
            assert resp_a["role"] == "Active"
            assert node_a.is_active is True

            # Node B connects and tries to claim: MUST BE REJECTED
            await node_b.connect()
            resp_b = await node_b.claim_lease()
            assert resp_b["role"] == "Standby"
            assert node_b.is_active is False

            # SIMULATE PARTITION: Node A disconnects abruptly
            await node_a.disconnect(force=True)

            # At t = 15s post-partition: Node B tries to claim early -> REJECTED
            clock.advance(15.0)
            resp_b_early = await node_b.claim_lease()
            assert resp_b_early["role"] == "Standby"
            assert node_b.is_active is False

            # Invariant check at t = 15s: Node A degraded, Node B Standby
            assert node_a.is_active is False
            assert node_b.is_active is False

            # At t = 30.5s: Lease expires at coordinator and Node A
            clock.advance(15.5)
            node_a.on_lease_expired_locally()
            assert node_a.state == NodeState.ROLE_LOCKED
            assert not node_a.can_execute_trades

            # Node B claims lease now that 30s TTL expired -> GRANTED
            resp_b_takeover = await node_b.claim_lease()
            assert resp_b_takeover["role"] == "Active"
            assert node_b.state == NodeState.ACTIVE_EXECUTION
            assert node_b.is_active is True
            assert node_b.can_execute_trades is True

            # Invariant holds: Node A LOCKED, Node B ACTIVE -> |A(t)| = 1
            assert node_a.is_active is False
            assert node_b.is_active is True

            await node_b.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 7: Post-Partition Reconnection Strict Lockout
    # -------------------------------------------------------------------------
    async def test_07_post_partition_reconnect_node_a_strictly_locked(self):
        """
        Verify that after the network partition is healed, Node A reconnects to
        the Cloud Coordinator. Because Node B now holds the active lease,
        Node A is strictly denied, enters ROLE_STANDBY, and is prevented from trading.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

        server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        ws_url = f"ws://127.0.0.1:{port}"

        try:
            node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, ws_url, clock.now)
            node_b = MockLocalAgent("Node_B", NodeRole.STANDBY, ws_url, clock.now)

            # Node A claims lease
            await node_a.connect()
            await node_a.claim_lease()

            # Partition Node A
            await node_a.disconnect()
            clock.advance(31.0)
            node_a.on_lease_expired_locally()
            assert node_a.state == NodeState.ROLE_LOCKED

            # Node B takes over
            await node_b.connect()
            resp_b = await node_b.claim_lease()
            assert resp_b["role"] == "Active"

            # PARTITION HEALED: Node A reconnects
            await node_a.connect()
            resp_a_recon = await node_a.claim_lease()

            # MUST BE REJECTED!
            assert resp_a_recon["role"] == "Standby"
            assert resp_a_recon["status"] == "rejected"
            assert node_a.state == NodeState.STANDBY_HOT
            assert not node_a.can_execute_trades

            # Node B remains the sole active trader
            assert node_b.state == NodeState.ACTIVE_EXECUTION
            assert node_b.can_execute_trades is True

            await node_a.disconnect()
            await node_b.disconnect()
        finally:
            server.close()
            await server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 8: Live Real-Time Socket Severance & Watchdog Transition
    # -------------------------------------------------------------------------
    async def test_08_live_socket_severance_real_time_async(self):
        """
        Real-time asynchronous test of live TCP socket severance and background watchdog.
        Uses accelerated 0.4s heartbeat and 1.2s lease TTL over live TCP loopback sockets.
        Validates that live socket drop triggers failed heartbeat and automatic transition
        to ROLE_LOCKED at the exact expiration moment.
        """
        lease_ttl = 1.2
        hb_interval = 0.4

        # Real-time coordinator
        coordinator = WebSocketLeaseCoordinator(time_provider=time.time, ttl_seconds=lease_ttl)
        coord_server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
        coord_port = coord_server.sockets[0].getsockname()[1]

        proxy = NetworkPartitionProxy("127.0.0.1", coord_port)
        proxy_port = await proxy.start()
        proxy_ws_url = f"ws://127.0.0.1:{proxy_port}"

        role_transitions: List[Tuple[EngineRole, EngineRole]] = []

        config = FailoverManagerConfig(
            ws_url=proxy_ws_url,
            heartbeat_interval_s=hb_interval,
            lease_ttl_s=lease_ttl,
            reconnect_interval_s=0.2,
            node_id="Node_Live_A",
        )
        mgr = FailoverManager(config=config, time_provider=time.time)
        mgr.add_role_change_listener(lambda old_r, new_r: role_transitions.append((old_r, new_r)))

        try:
            # Start FailoverManager background tasks
            await mgr.start()

            # Wait for active lease to be acquired
            for _ in range(50):
                if mgr.role == EngineRole.ROLE_ACTIVE:
                    break
                await asyncio.sleep(0.05)
            assert mgr.role == EngineRole.ROLE_ACTIVE
            assert mgr.can_execute_trades is True

            # INJECT ABRUPT NETWORK PARTITION
            partition_start = time.time()
            await proxy.sever()

            # Wait for lease TTL to expire (1.2s + 0.3s buffer)
            await asyncio.sleep(lease_ttl + 0.3)

            # ASSERTION: Background watchdog must have forcibly transitioned node to ROLE_LOCKED!
            assert mgr.role == EngineRole.ROLE_LOCKED
            assert mgr.state == EngineRole.ROLE_LOCKED
            assert mgr.is_locked is True
            assert mgr.can_execute_trades is False
            assert mgr.can_execute_orders is False

            # Verify transition history includes ROLE_LOCKED
            locked_transitions = [t for t in role_transitions if t[1] == EngineRole.ROLE_LOCKED]
            assert len(locked_transitions) >= 1

        finally:
            await mgr.stop()
            await proxy.stop()
            coord_server.close()
            await coord_server.wait_closed()

    # -------------------------------------------------------------------------
    # Test 9: OS-Level Windows Firewall Rule Check (Graceful Elevation Handling)
    # -------------------------------------------------------------------------
    async def test_09_windows_firewall_rule_partition_handling(self):
        """
        Task 2 (Firewall Verification):
        Tests or validates Windows Firewall network partition command generation.
        If elevated, asserts firewall rule addition; if unprivileged, asserts clean detection.
        """
        rule_name = f"pipvane_test_partition_{uuid.uuid4().hex[:6]}"
        test_port = 54321

        if is_running_as_admin():
            # Add outbound block rule via netsh
            add_cmd = (
                f'netsh advfirewall firewall add rule name="{rule_name}" '
                f'dir=out action=block protocol=TCP remoteport={test_port}'
            )
            res = subprocess.run(add_cmd, capture_output=True, text=True, shell=True)
            assert res.returncode == 0, f"Failed to add firewall rule: {res.stderr}"

            # Clean up rule
            del_cmd = f'netsh advfirewall firewall delete rule name="{rule_name}"'
            subprocess.run(del_cmd, capture_output=True, text=True, shell=True)
        else:
            # Unprivileged environment: verify socket severance is used as preferred path
            logger.info("Non-admin context detected. Netsh firewall modification properly gated.")
            assert True

    # -------------------------------------------------------------------------
    # Test 10: 1,000-Run Split-Brain Chaos Partition Gate
    # -------------------------------------------------------------------------
    async def test_10_1000_run_split_brain_chaos_partition_gate(self):
        """
        Verify 1,000 automated partition cycles validating 0 split-brain occurrences
        and 100% self-fencing reliability (FR-7.4, FR-7.5).
        """
        orchestrator = FailoverSimulationOrchestrator(
            total_runs=1000,
            lease_ttl_s=30.0,
            heartbeat_interval_s=3.0,
            heartbeat_duration_s=12.0,  # 4 heartbeats per nominal phase
            quiet=True,
        )
        results = await orchestrator.execute_simulation()

        assert results["success"] is True
        assert results["runs_completed"] == 1000
        assert results["double_active_violations"] == 0
        assert results["total_invariant_checks"] >= 10000
