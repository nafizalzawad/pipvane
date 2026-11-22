"""
Pipvane v3: Sprint 16 Takeover Integration Test Suite (test_full_failover.py).
=============================================================================
Objective:
Prove that the Failover and Backfill systems perfectly prevent duplicate trades.

Chaos Lifecycle:
1. Chaos Setup:
   Spin up Node A (Primary) and Node B (Standby) with a mock Telegram API and mock Broker API.
2. The Incident:
   Dispatch "Signal 1". Node A receives it, sends it to the broker, and immediately crashes
   (force kill process simulation) before syncing its state to the cloud.
3. The Takeover:
   Wait 30 seconds for the lease to expire. Node B claims the active lease.
4. Assertion:
   Feed "Signal 1" into Node B's TDLib backfill stream. Assert that Node B executes the
   ReconciliationLoop, discovers "Signal 1" is already open on the broker, flags it as SKIP_DUP,
   and DOES NOT send a duplicate order.
5. Reliability Requirement:
   Execute this chaos scenario flawlessly 100 times in a row without a single duplicate trade.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    ExecutionResult,
    ExecutionStatus,
)
from engine.failover_manager import (
    EngineRole,
    FailoverManager,
    FailoverManagerConfig,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    ROLE_LOCKED,
)
from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionStatus as ProtoExecutionStatus,
    OrderIntent,
    OrderSide,
    PositionItem,
    PositionsSyncPayload,
)
from engine.orphan_manager import OrphanManager, OrphanResolutionPolicy
from engine.reconciliation import ReconciliationEngine, ResolutionPolicy
from engine.risk_engine import SkipReason, SKIP_DUP
from engine.telegram_gateway import TelegramMessage
from failover_simulation import (
    MockLocalAgent,
    NodeRole,
    NodeState,
    VirtualClock,
    WebSocketLeaseCoordinator,
)

logger = logging.getLogger("pipvane.qa.test_full_failover")


# =============================================================================
# Mock Broker API (Simulates Broker Terminal Execution & Positions State Array)
# =============================================================================

class MockBrokerApi:
    """
    Simulates a MetaTrader broker terminal with an in-memory position state array.
    Tracks all open tickets, magic numbers, symbols, volumes, and comments.
    """

    def __init__(self, terminal_id: str = "term_broker_1", default_magic: int = 123456) -> None:
        self.terminal_id = terminal_id
        self.default_magic = default_magic
        self.positions: Dict[int, PositionItem] = {}
        self.order_history: List[OrderIntent] = []
        self._ticket_counter = 100000

    def reset(self) -> None:
        """Clears all broker positions and order history for clean test runs."""
        self.positions.clear()
        self.order_history.clear()
        self._ticket_counter = 100000

    def execute_order(self, intent: OrderIntent) -> PositionItem:
        """
        Executes an OrderIntent on the broker and stores it in the active position array.
        Records comment with signal_id for reconciliation discovery.
        """
        self._ticket_counter += 1
        ticket = self._ticket_counter
        self.order_history.append(intent)

        comment_str = f"sig_{intent.signal_id}" if intent.signal_id else ""
        pos = PositionItem(
            ticket=ticket,
            magic_number=intent.magic_number or self.default_magic,
            open_time_unix=int(time.time()),
            symbol=intent.symbol,
            position_type=int(intent.side),
            volume=intent.volume,
            open_price=intent.price or 1.08500,
            current_price=intent.price or 1.08500,
            stop_loss=intent.stop_loss,
            take_profit=intent.take_profit,
            profit=0.0,
            comment=comment_str,
        )
        self.positions[ticket] = pos
        logger.info(
            "MockBrokerApi: Executed Order Ticket #%d for Signal %d (%s %.2f Lots, Comment='%s')",
            ticket, intent.signal_id, pos.symbol, pos.volume, comment_str,
        )
        return pos

    def get_positions(self) -> List[PositionItem]:
        """Returns snapshot array of all active positions currently open on the broker."""
        return list(self.positions.values())

    def get_positions_sync_payload(self) -> PositionsSyncPayload:
        """Packages positions into binary sync payload matching Named Pipe frame."""
        positions_list = self.get_positions()
        return PositionsSyncPayload(
            total_positions=len(positions_list),
            positions=positions_list,
        )


# =============================================================================
# Mock Dispatcher Bridge (Routes between Engine Orchestrator and Mock Broker)
# =============================================================================

class MockBrokerDispatcher:
    """Simulates IPCDispatcher interface routing orders directly to MockBrokerApi."""

    def __init__(self, broker: MockBrokerApi) -> None:
        self.broker = broker
        self.terminal_id = broker.terminal_id
        self.total_dispatches: int = 0
        self.dispatched_intents: List[OrderIntent] = []
        self._on_feedback_cbs: List[Callable[[str, ExecutionFeedback], None]] = []
        self._on_account_state_cbs: List[Callable[[str, Any], None]] = []
        self._on_positions_sync_cbs: List[Callable[[str, PositionsSyncPayload], None]] = []

    def on_execution_feedback(self, cb: Callable[[str, ExecutionFeedback], None]) -> None:
        self._on_feedback_cbs.append(cb)

    def on_account_state(self, cb: Callable[[str, Any], None]) -> None:
        self._on_account_state_cbs.append(cb)

    def on_positions_sync(self, cb: Callable[[str, PositionsSyncPayload], None]) -> None:
        self._on_positions_sync_cbs.append(cb)

    def can_route_to_terminal(self, terminal_id: str) -> bool:
        return terminal_id == self.terminal_id

    def dispatch_order_intent(self, terminal_id: str, intent: OrderIntent) -> bool:
        """Dispatches order directly to broker, creates open position, and notifies feedback."""
        self.total_dispatches += 1
        self.dispatched_intents.append(intent)
        pos = self.broker.execute_order(intent)

        # Notify feedback to resolve orchestrator's feedback_future
        feedback = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=pos.ticket,
            execution_status=ProtoExecutionStatus.FILLED,
            broker_retcode=10009,
            requested_volume=pos.volume,
            executed_volume=pos.volume,
            requested_price=pos.open_price,
            executed_price=pos.open_price,
        )
        for cb in self._on_feedback_cbs:
            try:
                cb(terminal_id, feedback)
            except Exception as e:
                logger.error("Error in on_feedback callback: %s", e)

        return True

    def get_registered_terminals(self) -> List[str]:
        return [self.terminal_id]

    def get_broker_positions(self, terminal_id: Optional[str] = None) -> List[PositionItem]:
        return self.broker.get_positions()

    async def fetch_positions_dump(
        self, terminal_id: str, timeout_s: float = 1.0
    ) -> Optional[PositionsSyncPayload]:
        return self.broker.get_positions_sync_payload()


# =============================================================================
# Node Engine Scaffold (Represents a Local Engine Instance: Primary or Standby)
# =============================================================================

class LocalEngineNode:
    """
    Encapsulates a full Pipvane Local Engine instance with isolated SQLite state,
    ExecutionOrchestrator, FailoverManager, and TDLib backfill stream receiver.
    """

    def __init__(
        self,
        node_id: str,
        role: EngineRole,
        broker: MockBrokerApi,
        time_provider: Callable[[], float],
        lease_ttl_s: float = 30.0,
    ) -> None:
        self.node_id = node_id
        self.role = role
        self.broker = broker
        self.time_provider = time_provider
        self.lease_ttl_s = lease_ttl_s
        self.is_crashed = False

        self.dispatcher = MockBrokerDispatcher(broker)
        self.failover_mgr = FailoverManager(
            config=FailoverManagerConfig(
                node_id=node_id,
                lease_ttl_s=lease_ttl_s,
                heartbeat_interval_s=10.0,
            ),
            time_provider=time_provider,
        )
        self.failover_mgr.role = role
        if role == EngineRole.ROLE_ACTIVE:
            self.failover_mgr.lease_token = f"token_{node_id}"
            self.failover_mgr.lease_expires_at = time_provider() + lease_ttl_s

        # Isolated in-memory SQLite database
        self.orchestrator = ExecutionOrchestrator(
            ipc_dispatcher=self.dispatcher,  # type: ignore[arg-type]
            db_path=":memory:",
            initial_role=role,
            failover_manager=self.failover_mgr,
            orphan_policy=OrphanResolutionPolicy.ADOPT,
        )
        self.orchestrator.register_terminal_config(
            TerminalExecutionConfig(terminal_id=broker.terminal_id, supports_hedging=True)
        )

        # Backfill ingestion telemetry
        self.backfill_signals_processed: List[Dict[str, Any]] = []

    async def start(self) -> None:
        """Starts engine orchestrator services."""
        await self.orchestrator.start()

    async def stop(self) -> None:
        """Stops engine services."""
        await self.orchestrator.stop()

    def force_kill(self) -> None:
        """Simulates sudden hard crash (kill -9 / power loss) before syncing to cloud."""
        self.is_crashed = True
        self.failover_mgr.role = EngineRole.ROLE_LOCKED
        self.failover_mgr.lease_token = None
        self.orchestrator.set_engine_role(EngineRole.ROLE_LOCKED)
        logger.warning("CRASH SIMULATION: Node %s abruptly force-killed!", self.node_id)

    async def execute_reconciliation_loop(self) -> Dict[str, Any]:
        """Runs the ReconciliationLoop against the broker terminal."""
        positions = self.broker.get_positions()
        summary = await self.orchestrator.reconcile_terminal(
            terminal_id=self.broker.terminal_id,
            broker_positions=positions,
        )
        return summary

    async def feed_tdlib_backfill_stream(self, signal: Dict[str, Any]) -> ExecutionResult:
        """
        Feeds a backfill message from TDLib into the node's execution pipeline.
        Parses intent and submits to the orchestrator.
        """
        sig_id = signal.get("signal_id", 1)
        symbol = signal.get("symbol", "EURUSD")
        side_val = signal.get("side", OrderSide.BUY)
        volume = signal.get("volume", 0.10)
        sl = signal.get("stop_loss", 1.08000)
        tp = signal.get("take_profit", 1.09000)

        intent = OrderIntent(
            signal_id=sig_id,
            symbol=symbol,
            side=side_val,
            action_type=ActionType.MARKET_ENTRY,
            volume=volume,
            stop_loss=sl,
            take_profit=tp,
            magic_number=self.broker.default_magic,
        )

        res = await self.orchestrator.submit_order(self.broker.terminal_id, intent)
        self.backfill_signals_processed.append({
            "signal_id": sig_id,
            "success": res.success,
            "status": res.status.name,
            "skip_reason": res.skip_reason.code if res.skip_reason else None,
        })
        return res


# =============================================================================
# Suite: Sprint 16 Takeover Integration Tests
# =============================================================================

@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.failover
class TestTakeoverIntegrationSuite:
    """
    Validates that failover takeover and TDLib backfill replay strictly prevent
    duplicate order execution when Primary crashes before cloud synchronization.
    """

    # -------------------------------------------------------------------------
    # Test 1: Single Takeover Scenario with Detailed Field & State Assertions
    # -------------------------------------------------------------------------
    async def test_single_takeover_duplicate_prevention_flow(self):
        """
        Executes one full incident lifecycle:
        1. Node A (Active) executes Signal 1 on broker.
        2. Node A immediately crashes before syncing state to cloud.
        3. 30s lease expires; Node B takes over and claims Active lease.
        4. Node B executes ReconciliationLoop and discovers Signal 1 on broker.
        5. "Signal 1" fed to Node B's TDLib backfill stream -> flagged SKIP_DUP.
        6. Total broker orders strictly remains 1.
        """
        clock = VirtualClock(initial_time=1700000000.0)
        broker = MockBrokerApi("term_broker_1")

        # 1. Chaos Setup: Node A (Active) and Node B (Standby)
        node_a = LocalEngineNode("Node_A", EngineRole.ROLE_ACTIVE, broker, clock.now, lease_ttl_s=30.0)
        node_b = LocalEngineNode("Node_B", EngineRole.ROLE_STANDBY, broker, clock.now, lease_ttl_s=30.0)

        await node_a.start()
        await node_b.start()

        try:
            # 2. The Incident: Dispatch "Signal 1" to Node A
            signal_1 = {
                "signal_id": 1001,
                "symbol": "EURUSD",
                "side": OrderSide.BUY,
                "volume": 0.10,
                "stop_loss": 1.08000,
                "take_profit": 1.09000,
            }
            res_a = await node_a.feed_tdlib_backfill_stream(signal_1)
            assert res_a.success is True, f"Node A should execute Signal 1, got: {res_a}"
            assert res_a.status == ExecutionStatus.FILLED

            # Verify broker now has exactly 1 position open
            broker_positions = broker.get_positions()
            assert len(broker_positions) == 1
            pos = broker_positions[0]
            assert pos.symbol == "EURUSD"
            assert pos.volume == 0.10
            assert "1001" in pos.comment
            assert broker.order_history[0].signal_id == 1001

            # Node A immediately crashes before syncing to cloud!
            node_a.force_kill()
            assert node_a.is_crashed is True

            # Assert Node B's local database does NOT have Signal 1 yet (unsynced)
            assert node_b.orchestrator.get_active_orders() == []

            # 3. The Takeover: 30 seconds elapse, lease expires, Node B promoted to Active
            clock.advance(30.0)
            node_b.failover_mgr.role = EngineRole.ROLE_ACTIVE
            node_b.failover_mgr.lease_expires_at = clock.now() + 30.0
            node_b.orchestrator.set_engine_role(EngineRole.ROLE_ACTIVE)
            assert node_b.orchestrator.is_active() is True
            assert node_b.failover_mgr.can_execute_trades is True

            # 4. Reconciliation: Node B executes ReconciliationLoop
            recon_summary = await node_b.execute_reconciliation_loop()
            assert recon_summary is not None
            assert recon_summary["terminal_id"] == "term_broker_1"

            # Assert Node B discovered Signal 1 on the broker
            assert node_b.orchestrator.is_signal_already_open(1001, "term_broker_1") is True

            # 5. Assertion: Feed "Signal 1" into Node B's TDLib backfill stream
            res_b = await node_b.feed_tdlib_backfill_stream(signal_1)

            # ASSERTION: Must be rejected with code SKIP_DUP!
            assert res_b.success is False, "Node B MUST reject duplicate backfill signal!"
            assert res_b.status == ExecutionStatus.REJECTED
            assert res_b.skip_reason is not None
            assert res_b.skip_reason.code == SKIP_DUP
            assert "SKIP_DUP" in res_b.skip_reason.code
            assert "1001" in res_b.skip_reason.reason

            # CRITICAL INVARIANT: Broker MUST NOT receive a second order!
            assert len(broker.get_positions()) == 1, "Duplicate order was dispatched to broker!"
            assert len(broker.order_history) == 1, "Duplicate OrderIntent reached broker!"
            assert node_b.dispatcher.total_dispatches == 0, "Node B dispatched duplicate order!"

        finally:
            await node_a.stop()
            await node_b.stop()

    # -------------------------------------------------------------------------
    # Test 2: In-Depth Diagnostic Payload Validation for SKIP_DUP
    # -------------------------------------------------------------------------
    async def test_skip_dup_diagnostic_details(self):
        """Verify the exact details payload of SKIP_DUP rejection."""
        clock = VirtualClock()
        broker = MockBrokerApi("term_broker_1")
        node_b = LocalEngineNode("Node_B", EngineRole.ROLE_ACTIVE, broker, clock.now)
        await node_b.start()

        try:
            # Pre-populate broker with an open position for signal 2002
            broker.execute_order(
                OrderIntent(
                    signal_id=2002,
                    symbol="GBPUSD",
                    side=OrderSide.SELL,
                    volume=0.25,
                    magic_number=123456,
                )
            )

            # Run reconciliation on Node B
            await node_b.execute_reconciliation_loop()

            # Attempt to submit duplicate signal 2002
            res = await node_b.feed_tdlib_backfill_stream({
                "signal_id": 2002,
                "symbol": "GBPUSD",
                "side": OrderSide.SELL,
                "volume": 0.25,
            })

            assert res.success is False
            assert res.status == ExecutionStatus.REJECTED
            assert res.skip_reason.code == "SKIP_DUP"
            assert res.skip_reason.details["code"] == "SKIP_DUP"
            assert res.skip_reason.details["signal_id"] == 2002
            assert res.skip_reason.details["terminal_id"] == "term_broker_1"
            assert len(broker.positions) == 1
        finally:
            await node_b.stop()

    # -------------------------------------------------------------------------
    # Test 3: Standby Mode Pre-Takeover Lockout Assertion
    # -------------------------------------------------------------------------
    async def test_standby_mode_blocks_backfill_before_30s_expiry(self):
        """
        Verify that if TDLib backfill messages arrive on Node B BEFORE the 30s
        lease expires (while Node B is still in ROLE_STANDBY), they are strictly
        blocked by the Standby Lockout (SKIP_STANDBY_LOCK).
        """
        clock = VirtualClock()
        broker = MockBrokerApi("term_broker_1")
        node_b = LocalEngineNode("Node_B", EngineRole.ROLE_STANDBY, broker, clock.now)
        await node_b.start()

        try:
            res = await node_b.feed_tdlib_backfill_stream({
                "signal_id": 3003,
                "symbol": "XAUUSD",
                "side": OrderSide.BUY,
                "volume": 0.05,
            })
            assert res.success is False
            assert res.skip_reason.code == "SKIP_STANDBY_LOCK"
            assert len(broker.order_history) == 0
        finally:
            await node_b.stop()

    # -------------------------------------------------------------------------
    # Test 4: Mixed Batch - Existing Duplicates vs Genuine New Signals
    # -------------------------------------------------------------------------
    async def test_mixed_batch_deduplication_and_new_order_execution(self):
        """
        Feed 3 signals during backfill:
        - Signal 1 & Signal 2: Already executed by crashed Node A -> flagged SKIP_DUP.
        - Signal 3: New signal generated after Node A crash -> executed successfully.
        Assert that exactly Signal 3 is executed on the broker, resulting in 3 total positions.
        """
        clock = VirtualClock()
        broker = MockBrokerApi("term_broker_1")

        node_a = LocalEngineNode("Node_A", EngineRole.ROLE_ACTIVE, broker, clock.now)
        node_b = LocalEngineNode("Node_B", EngineRole.ROLE_STANDBY, broker, clock.now)

        await node_a.start()
        await node_b.start()

        try:
            # Node A executes Signal 1 and Signal 2
            await node_a.feed_tdlib_backfill_stream({"signal_id": 4001, "symbol": "EURUSD", "volume": 0.1})
            await node_a.feed_tdlib_backfill_stream({"signal_id": 4002, "symbol": "GBPUSD", "volume": 0.2})
            assert len(broker.positions) == 2

            # Node A crashes abruptly
            node_a.force_kill()

            # Takeover: 30s expires, Node B becomes Active
            clock.advance(30.0)
            node_b.failover_mgr.role = EngineRole.ROLE_ACTIVE
            node_b.failover_mgr.lease_expires_at = clock.now() + 30.0
            node_b.orchestrator.set_engine_role(EngineRole.ROLE_ACTIVE)

            # Node B reconciles
            await node_b.execute_reconciliation_loop()

            # Replay backfill: Signal 1, Signal 2, and new Signal 3
            res_1 = await node_b.feed_tdlib_backfill_stream({"signal_id": 4001, "symbol": "EURUSD", "volume": 0.1})
            res_2 = await node_b.feed_tdlib_backfill_stream({"signal_id": 4002, "symbol": "GBPUSD", "volume": 0.2})
            res_3 = await node_b.feed_tdlib_backfill_stream({"signal_id": 4003, "symbol": "USDJPY", "volume": 0.3})

            # Signal 1 & 2 must be SKIP_DUP
            assert res_1.success is False
            assert res_1.skip_reason.code == "SKIP_DUP"
            assert res_2.success is False
            assert res_2.skip_reason.code == "SKIP_DUP"

            # Signal 3 must be successfully executed!
            assert res_3.success is True
            assert res_3.status == ExecutionStatus.FILLED

            # Broker total positions must be exactly 3 (4001, 4002, 4003)
            assert len(broker.positions) == 3
            assert broker.order_history[2].signal_id == 4003
            assert node_b.dispatcher.total_dispatches == 1  # Only Signal 3 was dispatched by Node B
        finally:
            await node_a.stop()
            await node_b.stop()

    # -------------------------------------------------------------------------
    # Test 5: The 100-Run Chaos Takeover Flawless Loop
    # -------------------------------------------------------------------------
    async def test_100_runs_consecutive_chaos_takeover_duplicate_prevention(self):
        """
        Sprint 16 Gate Requirement:
        "Do not pass this sprint until this chaos scenario is handled flawlessly 100 times in a row."

        Runs 100 consecutive chaos iterations:
        - Each iteration spins up fresh Primary & Standby contexts with mock broker.
        - Primary executes unique Signal N, then crashes immediately.
        - 30s lease expires; Standby claims lease, reconciles, and processes backfill Signal N.
        - Standby MUST flag Signal N as SKIP_DUP on every single run.
        - Duplicate broker orders strictly = 0 for all 100 iterations.
        """
        TOTAL_CHAOS_RUNS = 100
        total_duplicates_prevented = 0
        total_broker_violations = 0

        for run_idx in range(1, TOTAL_CHAOS_RUNS + 1):
            clock = VirtualClock(initial_time=1700000000.0 + run_idx * 100.0)
            broker = MockBrokerApi(f"term_broker_{run_idx}")

            node_a = LocalEngineNode(f"Node_A_{run_idx}", EngineRole.ROLE_ACTIVE, broker, clock.now)
            node_b = LocalEngineNode(f"Node_B_{run_idx}", EngineRole.ROLE_STANDBY, broker, clock.now)

            await node_a.start()
            await node_b.start()

            try:
                # 1. Primary receives Signal N and executes on broker
                sig_id = 50000 + run_idx
                signal_data = {
                    "signal_id": sig_id,
                    "symbol": "EURUSD" if run_idx % 2 == 0 else "GBPUSD",
                    "side": OrderSide.BUY if run_idx % 3 != 0 else OrderSide.SELL,
                    "volume": 0.10 + (run_idx % 10) * 0.01,
                    "stop_loss": 1.08000,
                    "take_profit": 1.09000,
                }
                res_a = await node_a.feed_tdlib_backfill_stream(signal_data)
                assert res_a.success is True, f"Run {run_idx}: Node A execution failed"
                assert len(broker.positions) == 1, f"Run {run_idx}: Broker missing initial trade"

                # 2. Node A immediately crashes before syncing state to cloud
                node_a.force_kill()

                # 3. Takeover: 30s TTL expires, Node B promoted to Active
                clock.advance(30.0)
                node_b.failover_mgr.role = EngineRole.ROLE_ACTIVE
                node_b.failover_mgr.lease_expires_at = clock.now() + 30.0
                node_b.orchestrator.set_engine_role(EngineRole.ROLE_ACTIVE)

                # 4. Node B executes ReconciliationLoop
                recon_res = await node_b.execute_reconciliation_loop()
                assert recon_res is not None

                # 5. Feed Signal N into Node B's TDLib backfill stream
                res_b = await node_b.feed_tdlib_backfill_stream(signal_data)

                # 6. Strict Assertions: Must be rejected as SKIP_DUP
                if res_b.success is True:
                    total_broker_violations += 1
                    pytest.fail(f"CRITICAL FAILURE at run {run_idx}: Duplicate trade executed on broker!")

                assert res_b.status == ExecutionStatus.REJECTED
                assert res_b.skip_reason is not None
                assert res_b.skip_reason.code == "SKIP_DUP", (
                    f"Run {run_idx}: Expected SKIP_DUP, got {res_b.skip_reason.code}"
                )

                # Assert broker order count strictly remains 1
                assert len(broker.positions) == 1, (
                    f"Run {run_idx}: Duplicate position added to broker! Count = {len(broker.positions)}"
                )
                assert len(broker.order_history) == 1, (
                    f"Run {run_idx}: Duplicate order recorded in broker history!"
                )
                assert node_b.dispatcher.total_dispatches == 0, (
                    f"Run {run_idx}: Node B unexpectedly dispatched {node_b.dispatcher.total_dispatches} orders!"
                )

                total_duplicates_prevented += 1

            finally:
                await node_a.stop()
                await node_b.stop()

        # Final Verification
        assert total_duplicates_prevented == TOTAL_CHAOS_RUNS == 100
        assert total_broker_violations == 0
        logger.info(
            "FLAWLESS TAKEOVER GATE: Completed %d / %d chaos iterations with 0 duplicate orders!",
            total_duplicates_prevented, TOTAL_CHAOS_RUNS
        )
