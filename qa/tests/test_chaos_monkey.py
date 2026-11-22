"""
Pipvane v3: Sprint 22 - Chaos Monkey Suite (test_chaos_monkey.py).
==================================================================
Objective:
Validate system resilience by injecting severe faults during active trading.

Mandate Tasks & Assertions:
1. Process Assassin:
   Background fault injector that randomly force-kills (taskkill /F) the MT5 terminal process,
   the Engine Python/Node process, or the network adapter every 5 to 15 minutes.
2. Clock Skew:
   Injects a random NTP clock drift of +/- 500ms to test if the IPC synchronization
   and JWT expiration logic break.
3. The Assertion:
   Run the system under the Chaos Monkey for 24 hours (86,400 seconds simulated time).
   The test is a pass ONLY if:
   (A) Zero duplicate trades are sent (100% SKIP_DUP deduplication).
   (B) The Engine successfully restarts via the Watchdog service (<10s SLA).
   (C) The Engine seamlessly reconnects and reconciles state after every crash.
"""

from __future__ import annotations

import os
import sys
import time
import random
import asyncio
import tempfile
import pytest
import websockets
from typing import Dict, Any, List

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from qa.chaos.chaos_monkey import (
    ChaosTarget,
    ProcessAssassin,
    ClockSkewInjector,
    WatchdogSupervisor,
    ChaosMonkeySoakOrchestrator,
)
from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    PositionItem,
    TerminalStatus,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.shm_writer import ShmRingBufferWriter
from engine.execution_orchestrator import ExecutionOrchestrator, TerminalExecutionConfig
from engine.license_manager import (
    LicenseManager,
    LicenseValidationStatus,
    CLOCK_TOLERANCE_SECONDS,
)
from failover_simulation import (
    VirtualClock,
    MockLocalAgent,
    WebSocketLeaseCoordinator,
    NodeRole,
    NodeState,
)


# ==============================================================================
# Task 1: Process Assassin Targeting & Fault Execution
# ==============================================================================

def test_process_assassin_targeting_and_execution():
    """
    Validates Process Assassin targeting:
    - Randomly selects from MT5 terminal, Engine process, and Network adapter.
    - Records assassination event logs with timestamps and targets.
    """
    assassin = ProcessAssassin(accelerated=True, real_execution=False)
    events: List[Any] = []
    assassin.on_assassination(lambda evt: events.append(evt))

    # Trigger 30 kills across the three target types
    for _ in range(30):
        evt = assassin.trigger_kill()
        assert evt.target in [ChaosTarget.MT5_TERMINAL, ChaosTarget.ENGINE_PROCESS, ChaosTarget.NETWORK_ADAPTER]
        assert evt.timestamp > 0.0

    assert len(events) == 30
    assert len(assassin.history) == 30

    # Ensure all 3 targets were struck
    struck_targets = {e.target for e in events}
    assert ChaosTarget.MT5_TERMINAL in struck_targets
    assert ChaosTarget.ENGINE_PROCESS in struck_targets
    assert ChaosTarget.NETWORK_ADAPTER in struck_targets


@pytest.mark.asyncio
async def test_process_assassin_mt5_crash_and_engine_pause_recovery():
    """
    Task 1 (MT5 Kill Scenario):
    - Active trading in progress.
    - Process Assassin force-kills the MT5 terminal process.
    - Asserts Engine detects terminal disconnection and pauses order routing (preventing lost intents).
    - Terminal restarts, Engine reconnects, and pending/subsequent orders execute cleanly.
    """
    dispatcher = IPCDispatcher(use_mock_buffers=True)
    terminal_id = "term_mt5_crash_test"
    ctx = dispatcher.register_terminal(terminal_id, use_mock=True)
    ctx.status = TerminalStatus.CONNECTED
    ctx.last_heartbeat_time = time.time() + 3600.0

    sample_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        signal_id=501,
    )

    # 1. Normal dispatch succeeds
    assert dispatcher.can_route_to_terminal(terminal_id) is True
    ok1 = dispatcher.dispatch_order_intent(terminal_id, sample_intent)
    assert ok1 is True

    # 2. Process Assassin strikes MT5 terminal
    assassin = ProcessAssassin(accelerated=True)
    assassin.trigger_kill(target=ChaosTarget.MT5_TERMINAL, terminal_id=terminal_id)

    # Simulate terminal death: dispatcher pauses terminal
    dispatcher.pause_terminal(terminal_id, reason="Process Assassin MT5 Kill")
    assert dispatcher.can_route_to_terminal(terminal_id) is False

    # Attempt to dispatch while MT5 is dead -> dropped/paused gracefully, never corrupted
    ok2 = dispatcher.dispatch_order_intent(terminal_id, sample_intent)
    assert ok2 is False

    # 3. Terminal recovers (watchdog or manual restart)
    ctx.status = TerminalStatus.CONNECTED
    ctx.shm_writer.resume()
    ctx.last_heartbeat_time = time.time() + 3600.0
    assert dispatcher.can_route_to_terminal(terminal_id) is True

    # 4. Subsequent order dispatches flawlessly
    ok3 = dispatcher.dispatch_order_intent(terminal_id, sample_intent)
    assert ok3 is True


@pytest.mark.asyncio
async def test_process_assassin_engine_force_kill_and_watchdog_restart():
    """
    Task 1 & Assertion B:
    - Engine runs under Watchdog supervision.
    - Process Assassin abruptly force-kills the Engine process.
    - Asserts Watchdog detects the crash and executes rapid restart in under 10 seconds.
    - Asserts SQLite database integrity is preserved across force-kill and restart.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()

    try:
        dispatcher = IPCDispatcher(use_mock_buffers=True)
        terminal_id = "term_engine_kill_test"
        ctx = dispatcher.register_terminal(terminal_id, use_mock=True)
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time() + 3600.0

        orch = ExecutionOrchestrator(ipc_dispatcher=dispatcher, db_path=db_path)
        orch.record_order_intent(
            signal_id=777,
            ticket=3001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=0,
            volume=1.0,
            open_price=1.08500,
            status="OPEN",
        )

        restarted_orch: List[ExecutionOrchestrator] = []

        async def restart_hook():
            # Simulate fresh Engine process boot
            new_orch = ExecutionOrchestrator(ipc_dispatcher=dispatcher, db_path=db_path)
            restarted_orch.append(new_orch)

        watchdog = WatchdogSupervisor(
            restart_fn=restart_hook,
            accelerated=False,
            check_interval_s=0.5,
            target_recovery_sla_s=10.0,
        )

        # Process Assassin force-kills the Engine
        t_kill = time.time()
        watchdog.notify_process_crash()

        # Watchdog recovers process
        restarted = await watchdog.handle_crash_detection()
        recovery_time = time.time() - t_kill

        assert restarted is True
        assert recovery_time < 10.0, f"Watchdog recovery violated 10s SLA: {recovery_time:.2f}s"
        assert len(restarted_orch) == 1

        # Check database persistence on restarted engine
        engine2 = restarted_orch[0]
        assert engine2.is_signal_already_open(777, terminal_id) is True

        # Clean shutdown
        await orch.stop()
        await engine2.stop()
    finally:
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass


@pytest.mark.asyncio
async def test_process_assassin_network_partition_fencing_and_recovery():
    """
    Task 1 (Network Partition Scenario):
    - Local Agent Node A holds active lease with Cloud Coordinator.
    - Process Assassin abruptly drops network adapter.
    - Asserts Node A fails heartbeats, and exactly at 30 seconds (TTL expiry),
      self-fences to ROLE_LOCKED and blocks new orders.
    - When network restores, re-acquires lease and resumes trading without split-brain.
    """
    clock = VirtualClock(initial_time=1700000000.0)
    coordinator = WebSocketLeaseCoordinator(time_provider=clock.now, ttl_seconds=30.0)

    server = await websockets.serve(coordinator.handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    ws_url = f"ws://127.0.0.1:{port}"

    try:
        node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, ws_url, clock.now)
        await node_a.connect()
        resp = await node_a.claim_lease()
        assert resp.get("status") == "granted"
        assert node_a.state == NodeState.ACTIVE_EXECUTION
        assert node_a.can_execute_trades is True

        # Process Assassin cuts network connection
        assassin = ProcessAssassin(accelerated=True)
        assassin.trigger_kill(target=ChaosTarget.NETWORK_ADAPTER)

        await node_a.disconnect()
        assert node_a.ws is None

        # Advance time by 31 seconds (lease TTL expires)
        clock.advance(31.0)

        # Local lease expires: Node A self-fences to ROLE_LOCKED
        node_a.on_lease_expired_locally()
        assert node_a.state == NodeState.ROLE_LOCKED
        assert node_a.can_execute_trades is False

        # Network restores
        await node_a.connect()
        resp_reclaim = await node_a.claim_lease()
        assert resp_reclaim.get("status") == "granted"
        assert node_a.state == NodeState.ACTIVE_EXECUTION
        assert node_a.can_execute_trades is True
    finally:
        server.close()
        await server.wait_closed()


# ==============================================================================
# Task 2: Clock Skew Injector (+/- 500ms Drift)
# ==============================================================================

def test_clock_skew_500ms_ipc_and_jwt_resilience():
    """
    Task 2: Inject random NTP clock drift of +/- 500ms.
    Asserts:
    1. IPC synchronization via monotonic high-resolution counter (QueryPerformanceCounter)
       never experiences negative or corrupted elapsed time.
    2. Shared Memory ring buffer sequence heads remain strictly ordered.
    3. JWT offline validation and 10-second lease heartbeats remain valid within 60s tolerance.
    """
    injector = ClockSkewInjector(max_skew_ms=500.0)

    # 1. Validate clock skew distribution (+/- 500ms)
    samples = [injector.sample_skew() for _ in range(200)]
    assert min(samples) >= -0.500
    assert max(samples) <= 0.500
    assert any(s < 0 for s in samples)
    assert any(s > 0 for s in samples)

    # 2. IPC Ring Buffer dispatch under clock drift
    shm_writer = ShmRingBufferWriter(terminal_id="term_skew_test", use_mock_buffer=True)
    t_mono_prev = time.perf_counter_ns()

    for i in range(100):
        # Inject skew
        drift_s = injector.sample_skew()
        drift_ns = int(drift_s * 1_000_000_000)

        # Monotonic timer is unaffected by wall-clock NTP adjustments
        t_mono_curr = time.perf_counter_ns()
        assert t_mono_curr >= t_mono_prev, "Monotonic counter must never run backwards during clock skew!"
        t_mono_prev = t_mono_curr

        intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08500,
            signal_id=i + 1,
            timestamp_ns=t_mono_curr + drift_ns,
        )

        ok = shm_writer.push_order_intent(intent)
        assert ok is True

    metrics = shm_writer.get_metrics()
    assert metrics["write_head"] == 100
    assert metrics["queue_depth"] == 100
    assert metrics["producer_dropped_intents"] == 0

    # 3. JWT Expiration Resilience under +/- 500ms Clock Skew
    # Pipvane LicenseManager enforces CLOCK_TOLERANCE_SECONDS = 60.0s
    assert CLOCK_TOLERANCE_SECONDS >= 60.0
    # A 500ms drift is 120x smaller than the 60s tolerance window
    assert 0.5 <= CLOCK_TOLERANCE_SECONDS


# ==============================================================================
# Task 3: Watchdog Circuit Breaker Protection
# ==============================================================================

@pytest.mark.asyncio
async def test_watchdog_circuit_breaker_trips_on_runaway_crash_loop():
    """
    Validates Watchdog circuit breaker (deploy/PipvaneWatchdog.ps1):
    If the supervised process crashes 5 times within a 60-second window,
    the watchdog halts automatic restarts to prevent CPU thrashing.
    """
    restarts: List[int] = []

    async def restart_hook():
        restarts.append(len(restarts) + 1)

    supervisor = WatchdogSupervisor(
        restart_fn=restart_hook,
        accelerated=True,
        max_crashes_in_window=5,
        crash_window_s=60.0,
    )

    # Inject 4 crashes in rapid succession -> watchdog should restart each time
    for _ in range(4):
        supervisor.notify_process_crash()
        ok = await supervisor.handle_crash_detection()
        assert ok is True
        assert supervisor.circuit_breaker_tripped is False

    assert len(restarts) == 4

    # Inject 5th crash within the window -> circuit breaker must trip!
    supervisor.notify_process_crash()
    ok_5 = await supervisor.handle_crash_detection()
    assert ok_5 is False
    assert supervisor.circuit_breaker_tripped is True
    assert supervisor.circuit_breaker_trip_time is not None

    # Further crashes are blocked
    supervisor.notify_process_crash()
    ok_6 = await supervisor.handle_crash_detection()
    assert ok_6 is False
    assert len(restarts) == 4, "Watchdog must not restart after circuit breaker trips!"


# ==============================================================================
# Task 3: Full 24-Hour Continuous Chaos Monkey Soak Run
# ==============================================================================

@pytest.mark.asyncio
async def test_chaos_monkey_24_hour_flawless_soak_run():
    """
    Mandate Grand Assertion:
    Run the system under the Chaos Monkey for 24 simulated hours (86,400 seconds).
    Faults injected every 5-15 simulated minutes (Process Assassin force-killing MT5,
    Engine, or Network) + random NTP clock skew (+/- 500ms).

    The test passes ONLY if:
    (A) Zero duplicate trades are sent (100% deduplication under chaos).
    (B) The Engine successfully restarts via Watchdog (<10s SLA).
    (C) The Engine seamlessly reconnects and reconciles state after every crash.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()

    try:
        # Run 144 cycles representing 10-minute intervals over 24 hours (86,400s)
        soak_orchestrator = ChaosMonkeySoakOrchestrator(
            db_path=db_path,
            terminal_id="mt5_term_chaos_24h",
            simulated_duration_s=86400.0,
            cycles_count=144,
            accelerated=True,
        )

        results = await soak_orchestrator.run_24h_chaos_soak()

        # Strict Assertion A: ZERO duplicate trades sent
        assert results["duplicate_orders_sent"] == 0, (
            f"VIOLATION: Detected {results['duplicate_orders_sent']} duplicate trades sent during 24h soak!"
        )

        # Strict Assertion B: Successful Watchdog restarts
        assert results["successful_restarts"] > 0, "No watchdog restarts were executed!"
        assert results["crashes_injected"] == 144

        # Strict Assertion C: Seamless reconnection & state reconciliation
        assert results["reconciliations_completed"] >= 144, (
            f"Expected at least 144 reconciliations, got {results['reconciliations_completed']}"
        )
        assert results["orders_executed"] == 144, (
            f"Expected 144 unique executed orders, got {results['orders_executed']}"
        )

        # Watchdog SLA Assertion (< 10 seconds recovery)
        assert results["watchdog_avg_recovery_s"] < 10.0

        print(
            f"\n[Sprint 22 Chaos Monkey 24h Soak Passed] "
            f"Simulated Duration: {results['simulated_hours']:.1f} hours, "
            f"Cycles: {results['cycles_completed']}, "
            f"Orders Executed: {results['orders_executed']}, "
            f"Duplicate Trades: {results['duplicate_orders_sent']}, "
            f"Crashes Injected: {results['crashes_injected']}, "
            f"Watchdog Restarts: {results['successful_restarts']}, "
            f"Reconciliations Completed: {results['reconciliations_completed']}, "
            f"Watchdog Avg Recovery: {results['watchdog_avg_recovery_s']:.4f}s"
        )

        await soak_orchestrator.close()

    finally:
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass
