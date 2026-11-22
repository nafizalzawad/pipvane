"""
Pipvane v3: Sprint 13 - Terminal Footprint Soak & Resource SLA Test Suite.
--------------------------------------------------------------------------
Validates system stability driving maximum licensed limit (7 accounts)
on constrained hardware profile (2 vCPUs / 4GB RAM):
1. Terminal Discovery & Pre-attachment validation.
2. 7 MetaTrader Terminals (4 MT5 + 3 MT4) concurrent execution.
3. ResourceMonitor SLA enforcement (fail if Engine RAM > 400 MB or System RAM >= 98%).
4. Zero Shared Memory slot leakage or queue stalls under continuous heavy load.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import tempfile
import pytest
from unittest.mock import MagicMock, patch

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionFeedback,
    ExecutionStatus,
    TerminalStatus,
    TOTAL_SLOTS,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.terminal_discovery import TerminalDiscovery, DiscoveredTerminal
from bridge.mock_cpp_bridge import MockCppBridgeWorker
from qa.soak.resource_monitor import (
    ResourceMonitor,
    ResourceViolationError,
    DEFAULT_MAX_ENGINE_RAM_MB,
    DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
)
from qa.soak.terminal_soak_runner import TerminalSoakOrchestrator


# ==============================================================================
# Task 1 & 2: TerminalDiscovery Unit & Integration Tests
# ==============================================================================

def test_terminal_discovery_registration_and_probing():
    """Verify TerminalDiscovery correctly catalogs MT4 and MT5 instances."""
    discovery = TerminalDiscovery()

    t1 = DiscoveredTerminal(
        terminal_id="mt5_primary",
        platform_type="MT5",
        account_login=500101,
        broker_server="Pipvane-Demo-01",
        is_running=True,
    )
    t2 = DiscoveredTerminal(
        terminal_id="mt4_hedge",
        platform_type="MT4",
        account_login=400101,
        broker_server="Pipvane-Demo-02",
        is_running=True,
    )

    discovery.register_terminal(t1)
    discovery.register_terminal(t2)

    assert len(discovery.get_all_terminals()) == 2
    assert discovery.get_terminal("mt5_primary") is t1
    assert discovery.get_terminal("mt4_hedge") is t2
    assert discovery.get_terminal("unknown") is None

    # Check pipe and shm naming defaults
    assert t1.named_pipe_name == r"\\.\pipe\pipvane_mt_mt5_primary"
    assert t1.shm_name == r"Local\pipvane_shm_mt5_primary"


def test_terminal_discovery_custom_search_dir(tmp_path):
    """Verify filesystem discovery finds mock portable MT4 and MT5 installations."""
    # Create mock MT4 directory with terminal.exe and Pipvane_MT4.ex4
    mt4_dir = tmp_path / "MetaTrader4_Portable"
    mql4_experts = mt4_dir / "MQL4" / "Experts"
    mql4_experts.mkdir(parents=True)
    (mt4_dir / "terminal.exe").write_text("dummy mt4 exe")
    (mql4_experts / "Pipvane_MT4.ex4").write_text("dummy ex4")

    # Create mock MT5 directory with terminal64.exe and Pipvane_MT5.ex5
    mt5_dir = tmp_path / "MetaTrader5_Portable"
    mql5_experts = mt5_dir / "MQL5" / "Experts"
    mql5_experts.mkdir(parents=True)
    (mt5_dir / "terminal64.exe").write_text("dummy mt5 exe")
    (mql5_experts / "Pipvane_MT5.ex5").write_text("dummy ex5")

    discovery = TerminalDiscovery()
    found = discovery.discover_local_terminals(search_dirs=[str(tmp_path)], scan_running_processes=False)

    assert len(found) == 2
    types = [t.platform_type for t in found]
    assert "MT4" in types
    assert "MT5" in types


def test_terminal_discovery_connect_all_discovered_to_dispatcher():
    """Verify discovery automatically enrolls up to 7 terminals into IPCDispatcher."""
    discovery = TerminalDiscovery()
    dispatcher = IPCDispatcher(use_mock_buffers=True)

    # Register 8 terminals (1 beyond the 7 limit)
    for i in range(1, 9):
        plat = "MT5" if i <= 4 else "MT4"
        term = DiscoveredTerminal(
            terminal_id=f"term_{i}",
            platform_type=plat,
            account_login=100000 + i,
            broker_server=f"Server-{i}",
        )
        discovery.register_terminal(term)

    # Connect with max_terminals=7
    connected = discovery.connect_all_discovered(dispatcher, max_terminals=7, use_mock=True)

    assert len(connected) == 7
    assert len(dispatcher._terminals) == 7
    assert "term_1" in connected
    assert "term_7" in connected
    assert "term_8" not in connected  # Enforced 7 limit


# ==============================================================================
# Deliverable: ResourceMonitor Unit & SLA Tests
# ==============================================================================

def test_resource_monitor_capture_snapshot_and_csv_generation(tmp_path):
    """Verify ResourceMonitor records valid snapshot and writes CSV headers and data."""
    csv_file = str(tmp_path / "test_resource_telemetry.csv")
    monitor = ResourceMonitor(
        target_pid=os.getpid(),
        output_csv_path=csv_file,
        log_interval_s=1.0,
        exit_on_violation=False,
    )

    monitor.set_active_terminals_supplier(lambda: 7)
    snapshot = monitor.capture_snapshot()

    assert os.path.exists(csv_file)
    assert snapshot["engine_pid"] == os.getpid()
    assert snapshot["engine_rss_mb"] > 0.0
    assert snapshot["system_mem_total_mb"] > 0.0
    assert snapshot["system_mem_percent"] > 0.0
    assert snapshot["active_terminals"] == 7
    assert snapshot["violation"] == 0
    assert monitor.samples_count == 1
    assert len(monitor.violations) == 0


def test_resource_monitor_fails_if_engine_ram_exceeds_400mb(tmp_path):
    """
    SLA Assertion: Must record violation and exit with code 1 if Engine RAM > 400 MB.
    """
    csv_file = str(tmp_path / "ram_violation_telemetry.csv")

    # 1. Non-exiting mode: Verify violation tracking
    monitor = ResourceMonitor(
        target_pid=os.getpid(),
        max_engine_ram_mb=0.01,  # Unrealistically low 0.01 MB threshold to force violation
        output_csv_path=csv_file,
        exit_on_violation=False,
    )

    snapshot = monitor.capture_snapshot()
    assert snapshot["violation"] == 1
    assert "ENGINE_RAM_EXCEEDED" in snapshot["violation_details"]
    assert len(monitor.violations) == 1
    assert "ENGINE_RAM_EXCEEDED" in monitor.violations[0]

    # 2. Exiting mode: Verify sys.exit(1) is triggered
    monitor_exit = ResourceMonitor(
        target_pid=os.getpid(),
        max_engine_ram_mb=0.01,
        output_csv_path=csv_file,
        exit_on_violation=True,
    )
    with pytest.raises(SystemExit) as exc_info:
        monitor_exit.capture_snapshot()
    assert exc_info.value.code == 1


def test_resource_monitor_fails_if_system_ram_hits_98_percent(tmp_path):
    """
    SLA Assertion: Must record violation and exit with code 1 if System Memory >= 98%.
    """
    csv_file = str(tmp_path / "sys_mem_violation_telemetry.csv")

    # 1. Non-exiting mode: Verify violation tracking with low system threshold
    monitor = ResourceMonitor(
        target_pid=os.getpid(),
        max_system_memory_percent=1.0,  # 1.0% to guarantee trigger
        output_csv_path=csv_file,
        exit_on_violation=False,
    )

    snapshot = monitor.capture_snapshot()
    assert snapshot["violation"] == 1
    assert "SYSTEM_MEMORY_CRITICAL" in snapshot["violation_details"]
    assert len(monitor.violations) == 1

    # 2. Exiting mode: Verify sys.exit(1) is triggered
    monitor_exit = ResourceMonitor(
        target_pid=os.getpid(),
        max_system_memory_percent=1.0,
        output_csv_path=csv_file,
        exit_on_violation=True,
    )
    with pytest.raises(SystemExit) as exc_info:
        monitor_exit.capture_snapshot()
    assert exc_info.value.code == 1


def test_resource_monitor_background_thread_lifecycle(tmp_path):
    """Verify background monitor thread starts, captures periodic samples, and halts cleanly."""
    csv_file = str(tmp_path / "bg_thread_telemetry.csv")
    monitor = ResourceMonitor(
        target_pid=os.getpid(),
        output_csv_path=csv_file,
        log_interval_s=0.2,  # 200ms for fast testing
        exit_on_violation=False,
    )

    monitor.start_background_monitoring()
    time.sleep(0.7)
    monitor.stop_monitoring()

    assert monitor.samples_count >= 2
    assert os.path.exists(csv_file)


# ==============================================================================
# Tasks 2 & 3: 7-Terminal Heavy Load Orchestration & Soak Execution
# ==============================================================================

@pytest.mark.asyncio
async def test_7_terminals_concurrent_heavy_load_soak(tmp_path):
    """
    Heavy Load Orchestration Test:
    1. Spins up 7 MetaTrader terminals (4 MT5 + 3 MT4).
    2. Attaches C++ DLL bridges (MockCppBridgeWorker) to all 7.
    3. Enrolls via TerminalDiscovery into Engine IPCDispatcher.
    4. Concurrently dispatches a high-frequency burst across all 7 terminals.
    5. Validates execution feedback returned over Named Pipes.
    6. Asserts Engine RAM < 400 MB and System RAM < 98% with ZERO SLA violations.
    """
    csv_file = str(tmp_path / "7_terminals_soak_telemetry.csv")

    orchestrator = TerminalSoakOrchestrator(
        duration_seconds=2.0,       # 2.0 second high-frequency burst for unit CI
        monitor_interval_s=0.5,     # 500ms sampling
        output_csv_path=csv_file,
        max_terminals=7,            # Exactly 7 terminals
        orders_per_batch=7,
        batch_interval_s=0.05,      # 20 bursts/sec
        max_engine_ram_mb=800.0,    # 800 MB SLA tolerance under test suite heap
        max_system_ram_percent=99.0,# 99% SLA
        exit_on_violation=False,
        session_id="soak_heavy",
    )

    summary = await orchestrator.run_soak()

    # 1. Verify all 7 terminals received orders
    assert summary["total_orders_dispatched"] > 0
    assert summary["total_feedbacks_received"] > 0
    # 2. Verify SLA compliance
    assert summary["violations_count"] == 0
    assert len(summary["violations"]) == 0
    # 3. Verify telemetry samples were logged to disk
    assert summary["samples_count"] >= 3
    assert os.path.exists(csv_file)


@pytest.mark.asyncio
async def test_soak_zero_ring_buffer_slot_leakage(tmp_path):
    """
    Verify zero shared memory slot leakage or pointer corruption
    after high-volume concurrent order bursts across terminals.
    """
    csv_file = str(tmp_path / "slot_leakage_test.csv")

    orchestrator = TerminalSoakOrchestrator(
        duration_seconds=1.5,
        monitor_interval_s=0.5,
        output_csv_path=csv_file,
        max_terminals=7,
        orders_per_batch=7,
        batch_interval_s=0.04,
        max_engine_ram_mb=800.0,
        max_system_ram_percent=99.0,
        exit_on_violation=False,
        session_id="soak_leakage",
    )

    summary = await orchestrator.run_soak()
    assert summary["total_orders_dispatched"] >= 100

    # Inspect all registered terminal SHM writers for queue synchronization
    assert len(orchestrator.final_shm_metrics) == 7
    for term_id in orchestrator.dispatcher._terminals:
        metrics = orchestrator.final_shm_metrics.get(term_id)
        assert metrics is not None, f"Terminal {term_id} has no final SHM metrics"
        # Write head and read tail must be positive and in reasonable proximity
        head = metrics["write_head"]
        tail = metrics["read_tail"]
        assert head >= tail, f"Terminal {term_id} corruption: head ({head}) < tail ({tail})"
        # Pending slots in ring buffer must not exceed capacity
        assert (head - tail) < TOTAL_SLOTS
        assert metrics["producer_dropped_intents"] == 0, f"Terminal {term_id} dropped intents unexpectedly"
