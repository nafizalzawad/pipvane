"""
Unit tests validating Anti-Split-Brain Failover Simulation (FR-7.4 & FR-7.5).
"""

from __future__ import annotations

import os
import sys
import pytest
import subprocess

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from failover_simulation import FailoverSimulationOrchestrator  # noqa: E402


@pytest.mark.asyncio
async def test_failover_simulation_single_cycle():
    """Verify single 4-phase chaos lifecycle."""
    orchestrator = FailoverSimulationOrchestrator(
        total_runs=1,
        lease_ttl_s=30.0,
        heartbeat_interval_s=3.0,
        heartbeat_duration_s=60.0,
        quiet=True,
    )
    await orchestrator.start_server()
    try:
        await orchestrator.run_single_cycle(1)
        assert orchestrator.double_active_violations == 0
        assert orchestrator.total_invariant_checks == 26
    finally:
        await orchestrator.stop_server()


@pytest.mark.asyncio
async def test_failover_simulation_100_runs_batch():
    """Verify 100 consecutive chaos iterations with zero split-brain violations."""
    orchestrator = FailoverSimulationOrchestrator(
        total_runs=100,
        lease_ttl_s=30.0,
        heartbeat_interval_s=3.0,
        heartbeat_duration_s=60.0,
        quiet=True,
    )
    results = await orchestrator.execute_simulation()

    assert results["success"] is True
    assert results["runs_completed"] == 100
    assert results["double_active_violations"] == 0
    assert results["total_invariant_checks"] == 2600
    assert results["wall_duration_seconds"] < 15.0


@pytest.mark.asyncio
async def test_failover_simulation_detects_synthetic_split_brain():
    """Verify that a double-active state is detected and halts simulation with failure."""
    orchestrator = FailoverSimulationOrchestrator(
        total_runs=10,
        lease_ttl_s=30.0,
        heartbeat_interval_s=3.0,
        heartbeat_duration_s=60.0,
        inject_split_brain_at=3,
        quiet=True,
    )
    results = await orchestrator.execute_simulation()

    assert results["success"] is False
    assert results["runs_completed"] == 2  # Aborted at cycle 3
    assert results["double_active_violations"] == 1
    assert len(results["violation_details"]) == 1
    assert results["violation_details"][0]["run_index"] == 3


@pytest.mark.unit
def test_failover_simulation_cli_run():
    """Verify that failover_simulation.py CLI executes and exits with code 0."""
    script_path = os.path.join(PROJECT_ROOT, "failover_simulation.py")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    res = subprocess.run(
        [sys.executable, script_path, "--runs", "50", "--quiet"],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
    )
    assert res.returncode == 0, f"Simulation failed: {res.stderr}\n{res.stdout}"
    assert "SPLIT-BRAIN GATE SATISFIED" in res.stdout
    assert "Dual-Active Violations:" in res.stdout
    assert "0 concurrent active states" in res.stdout


@pytest.mark.unit
def test_failover_simulation_cli_exit_code_1_on_violation():
    """Verify that failover_simulation.py CLI explicitly fails (exit code 1) on double-active state."""
    script_path = os.path.join(PROJECT_ROOT, "failover_simulation.py")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    res = subprocess.run(
        [sys.executable, script_path, "--runs", "10", "--inject-split-brain-at", "4", "--quiet"],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
    )
    assert res.returncode == 1, "CLI must exit with code 1 when split-brain is detected"
    assert "DOUBLE-ACTIVE STATE DETECTED" in res.stdout
    assert "Dual-Active Violations:" in res.stdout
    assert "[FAIL]" in res.stdout
