"""
Unit and integration tests for Pipvane v3 IPC Round-Trip Latency Benchmark (VR-3.1).
"""

from __future__ import annotations

import os
import sys
import csv
import pytest
import subprocess

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ipc_latency_benchmark import IPCLatencyBenchmark  # noqa: E402


@pytest.mark.asyncio
async def test_ipc_latency_benchmark_execution():
    """Verify that IPCLatencyBenchmark initializes, runs a burst, and computes valid metrics."""
    terminal_id = "test_pytest_bench_0"
    bench = IPCLatencyBenchmark(
        terminal_id=terminal_id,
        iterations=200,
        warmup=50,
        median_threshold_ms=5.0,
        p99_threshold_ms=15.0,
        quiet=True,
    )

    try:
        await bench.initialize()
        await bench.execute_burst()
        metrics = bench.analyze_results()

        assert metrics["count"] == 200
        assert metrics["median_passed"] is True
        assert metrics["p99_passed"] is True
        assert metrics["overall_passed"] is True
        assert metrics["p50_ms"] < 5.0
        assert metrics["p99_ms"] < 15.0
        assert metrics["throughput_ops_sec"] > 0
    finally:
        await bench.teardown()


@pytest.mark.asyncio
async def test_ipc_latency_benchmark_quality_gate_failure(tmp_path):
    """Verify that setting an impossibly strict threshold flags failure."""
    terminal_id = "test_pytest_bench_fail"
    csv_file = str(tmp_path / "latencies.csv")

    bench = IPCLatencyBenchmark(
        terminal_id=terminal_id,
        iterations=100,
        warmup=20,
        median_threshold_ms=0.00001,  # Impossible threshold
        p99_threshold_ms=15.0,
        export_csv=csv_file,
        quiet=True,
    )

    try:
        await bench.initialize()
        await bench.execute_burst()
        metrics = bench.analyze_results()

        assert metrics["count"] == 100
        assert metrics["median_passed"] is False
        assert metrics["overall_passed"] is False

        # Verify CSV export
        assert os.path.exists(csv_file)
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = list(csv.reader(f))
            assert reader[0] == ["iteration", "latency_ms", "latency_us"]
            assert len(reader) == 101  # Header + 100 samples
    finally:
        await bench.teardown()


@pytest.mark.unit
def test_ipc_latency_benchmark_cli_run():
    """Verify that the standalone CLI script runs and exits with code 0."""
    script_path = os.path.join(PROJECT_ROOT, "ipc_latency_benchmark.py")
    res = subprocess.run(
        [sys.executable, script_path, "--iterations", "300", "--warmup", "50", "--terminal-id", "test_cli_bench"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert res.returncode == 0, f"Benchmark script failed: {res.stderr}\n{res.stdout}"
    assert "QUALITY GATE SATISFIED" in res.stdout
    assert "Median Latency" in res.stdout
    assert "✓ PASS" in res.stdout
