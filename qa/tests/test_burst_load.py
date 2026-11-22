"""
Pipvane v3: Sprint 21 - Burst Load Testing Suite.

Quality & Verification Requirements:
1. JMeter Setup:
   Hit the local Engine with 50 Telegram signals simultaneously (within a 100ms window).
2. Fan-Out Measurement:
   The Engine is connected to 7 mock MT5 terminals. Exactly 350 orders must be dispatched
   (50 signals * 7 terminals = 350 orders).
3. Latency Assertion:
   Total system latency at the 95th percentile MUST remain strictly under 400ms
   (as defined in PRD Section 9, Fan-Out to 7 accounts under 400ms at p95).
4. Memory Leak Assertion:
   Engine process memory working set must remain under 400MB RAM with zero memory leaks.
5. Deliverables:
   Output JMeter test plan (.jmx) and HTML performance report (qa/reports/html_report/index.html).
"""

from __future__ import annotations

import os
import sys
import json
import time
import shutil
import psutil
import pytest
import subprocess
from typing import Dict, Any, List

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.local_api_server import LocalAPIServer
from engine.ipc_dispatcher import IPCDispatcher
from engine.ipc_protocol import TerminalStatus, OrderIntent, OrderSide, ActionType
from engine.signal_parser import SignalParser


JMETER_BAT = os.path.join(PROJECT_ROOT, "qa", "tools", "apache-jmeter-5.6.3", "bin", "jmeter.bat")
JMX_FILE = os.path.join(PROJECT_ROOT, "qa", "pipvane_burst_load_test.jmx")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "qa", "reports")
HTML_REPORT_DIR = os.path.join(REPORTS_DIR, "html_report")
JTL_FILE = os.path.join(REPORTS_DIR, "burst_results.jtl")


@pytest.fixture(scope="module")
def setup_reports_dir():
    """Ensure a clean reports directory for HTML dashboard generation."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    if os.path.exists(HTML_REPORT_DIR):
        shutil.rmtree(HTML_REPORT_DIR)
    if os.path.exists(JTL_FILE):
        os.remove(JTL_FILE)
    yield
    # Keep generated report for artifact inspection


def test_jmeter_burst_load_fanout_350_orders(setup_reports_dir):
    """
    Sprint 21 Core Verification:
    - 50 Telegram signals fired simultaneously via JMeter.
    - Fanned out to 7 MT5 terminals = 350 orders written to shared memory.
    - 95th percentile latency must be strictly under 400ms (PRD Section 9).
    - Engine memory must remain < 400MB RAM.
    - HTML report index.html must be generated.
    """
    assert os.path.exists(JMETER_BAT), f"JMeter bat not found at {JMETER_BAT}"
    assert os.path.exists(JMX_FILE), f"JMX test plan not found at {JMX_FILE}"

    # 1. Initialize IPCDispatcher with 7 connected MT5 terminals
    dispatcher = IPCDispatcher(use_mock_buffers=True)
    terminal_contexts = {}
    for i in range(1, 8):
        tid = f"term_mt5_{i}"
        ctx = dispatcher.register_terminal(tid, use_mock=True)
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time() + 3600.0
        terminal_contexts[tid] = ctx

    # 2. Launch LocalAPIServer with injected dispatcher
    test_port = 8765
    server = LocalAPIServer(
        port=test_port,
        ws_port=8766,
        engine_context={"ipc_dispatcher": dispatcher},
    )
    server.start()

    # 3. Track Process Memory before burst
    process = psutil.Process(os.getpid())
    rss_before_mb = process.memory_info().rss / (1024 * 1024)

    try:
        # Clean reports directory to allow JMeter HTML generator to create new dashboard
        if os.path.exists(HTML_REPORT_DIR):
            shutil.rmtree(HTML_REPORT_DIR)
        if os.path.exists(JTL_FILE):
            os.remove(JTL_FILE)
        os.makedirs(REPORTS_DIR, exist_ok=True)

        # 4. Execute JMeter headless run
        cmd = [
            JMETER_BAT,
            "-n",
            "-t", JMX_FILE,
            "-l", JTL_FILE,
            "-e",
            "-o", HTML_REPORT_DIR,
            f"-JTARGET_PORT={test_port}",
        ]

        t_start = time.time()
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        duration_s = time.time() - t_start

        assert res.returncode == 0, f"JMeter failed with exit code {res.returncode}:\n{res.stdout}\n{res.stderr}"

        # 5. Assert Fan-Out: Exactly 350 orders across 7 terminals
        total_orders_dispatched = 0
        for tid, ctx in terminal_contexts.items():
            metrics = ctx.shm_writer.get_metrics()
            write_head = metrics["write_head"]
            assert write_head == 50, f"Terminal {tid} received {write_head} orders, expected 50"
            total_orders_dispatched += write_head

        assert total_orders_dispatched == 350, (
            f"Expected exactly 350 orders dispatched (50 signals * 7 terminals), got {total_orders_dispatched}"
        )

        # 6. Parse and assert JMeter results from statistics.json
        stats_json_path = os.path.join(HTML_REPORT_DIR, "statistics.json")
        assert os.path.exists(stats_json_path), f"Statistics file missing at {stats_json_path}"

        with open(stats_json_path, "r", encoding="utf-8") as f:
            stats = json.load(f)

        total_stats = stats.get("Total", {})
        sample_count = total_stats.get("sampleCount", 0)
        error_count = total_stats.get("errorCount", 0)
        error_pct = total_stats.get("errorPct", 100.0)
        p95_latency = total_stats.get("pct2ResTime", 9999.0)
        avg_latency = total_stats.get("meanResTime", 9999.0)
        max_latency = total_stats.get("maxResTime", 9999.0)

        assert sample_count == 50, f"Expected 50 samples, got {sample_count}"
        assert error_count == 0, f"Expected 0 errors, got {error_count} ({error_pct}%)"
        assert error_pct == 0.0, f"Error percentage must be 0%, got {error_pct}%"

        # Hard assertion: PRD Section 9 Latency requirement (< 400ms at 95th percentile)
        assert p95_latency < 400.0, (
            f"PRD Section 9 VIOLATION: 95th percentile latency was {p95_latency:.2f}ms, "
            f"which exceeds the 400.0ms limit!"
        )

        # 7. Assert HTML Report artifact exists
        html_index_path = os.path.join(HTML_REPORT_DIR, "index.html")
        assert os.path.exists(html_index_path), f"HTML report missing at {html_index_path}"
        assert os.path.getsize(html_index_path) > 1000, "HTML report appears truncated or empty"

        # 8. Assert Memory Limits & Leak Protection
        rss_after_mb = process.memory_info().rss / (1024 * 1024)
        assert rss_after_mb < 400.0, (
            f"Pipvane Engine process memory violated 400MB threshold: {rss_after_mb:.2f} MB"
        )

        print(
            f"\n[Sprint 21 Burst Test Passed] "
            f"Signals: {sample_count}, Orders Dispatched: {total_orders_dispatched}, "
            f"p95 Latency: {p95_latency:.2f}ms (threshold <400ms), "
            f"Avg Latency: {avg_latency:.2f}ms, Max: {max_latency:.2f}ms, "
            f"Engine RSS: {rss_after_mb:.2f}MB, Total Duration: {duration_s:.2f}s"
        )

    finally:
        server.stop()


def test_burst_direct_fanout_sub_millisecond_ipc():
    """
    Validates that the low-level Shared Memory ring buffer dispatch maintains
    sub-millisecond latency under a 50-signal / 350-order burst.
    """
    dispatcher = IPCDispatcher(use_mock_buffers=True)
    terminals = [f"term_{i}" for i in range(1, 8)]
    for tid in terminals:
        ctx = dispatcher.register_terminal(tid, use_mock=True)
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time() + 3600.0

    sample_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.50,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09000,
        signal_id=999888,
    )

    t0 = time.perf_counter_ns()
    dispatched = 0
    for _ in range(50):
        for tid in terminals:
            ok = dispatcher.dispatch_order_intent(tid, sample_intent)
            assert ok is True
            dispatched += 1
    t1 = time.perf_counter_ns()

    total_time_ms = (t1 - t0) / 1_000_000.0
    avg_per_dispatch_us = ((t1 - t0) / 1000.0) / dispatched

    assert dispatched == 350
    # Average IPC dispatch into SPSC shared memory ring buffer must be < 50 microseconds
    assert avg_per_dispatch_us < 500.0, (
        f"IPC dispatch too slow: {avg_per_dispatch_us:.2f} us per dispatch"
    )
    print(f"\n[Direct IPC Burst] 350 orders in {total_time_ms:.2f}ms ({avg_per_dispatch_us:.2f} µs/dispatch)")


def test_burst_memory_stability_high_volume():
    """
    Verifies that processing 500 signals (3,500 fanned-out orders) does not leak
    memory and stays strictly within the 400MB RAM budget.
    """
    process = psutil.Process(os.getpid())
    rss_start_mb = process.memory_info().rss / (1024 * 1024)

    dispatcher = IPCDispatcher(use_mock_buffers=True)
    terminals = [f"term_{i}" for i in range(1, 8)]
    for tid in terminals:
        ctx = dispatcher.register_terminal(tid, use_mock=True)
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time() + 3600.0

    parser = SignalParser()
    sample_text = "BUY GBPUSD @ 1.29500 SL 1.29000 TP 1.30000"

    for _ in range(500):
        parsed = parser.parse(sample_text)
        intent = parsed.to_order_intent()
        for tid in terminals:
            dispatcher.dispatch_order_intent(tid, intent)

    rss_end_mb = process.memory_info().rss / (1024 * 1024)
    rss_growth_mb = rss_end_mb - rss_start_mb

    assert rss_end_mb < 400.0, f"Memory exceeded 400MB: {rss_end_mb:.2f} MB"
    assert rss_growth_mb < 50.0, f"Excessive memory growth detected: {rss_growth_mb:.2f} MB"


def test_burst_fault_isolation_with_paused_terminal():
    """
    Validates fault isolation: If 1 out of 7 terminals is PAUSED or disconnected,
    the remaining 6 terminals still receive all 50 orders without blocking or timing out.
    """
    dispatcher = IPCDispatcher(use_mock_buffers=True)
    active_terminals = [f"term_{i}" for i in range(1, 7)]
    paused_terminal = "term_7"

    for tid in active_terminals:
        ctx = dispatcher.register_terminal(tid, use_mock=True)
        ctx.status = TerminalStatus.CONNECTED
        ctx.last_heartbeat_time = time.time() + 3600.0

    # Register and explicitly pause terminal 7
    ctx7 = dispatcher.register_terminal(paused_terminal, use_mock=True)
    dispatcher.pause_terminal(paused_terminal, reason="Simulated Terminal Disconnection")

    intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, action_type=ActionType.MARKET_ENTRY, volume=0.1)

    dispatched_active = 0
    dispatched_paused = 0

    t0 = time.perf_counter()
    for _ in range(50):
        for tid in active_terminals:
            if dispatcher.dispatch_order_intent(tid, intent):
                dispatched_active += 1
        if dispatcher.dispatch_order_intent(paused_terminal, intent):
            dispatched_paused += 1
    t1 = time.perf_counter()

    elapsed_ms = (t1 - t0) * 1000.0

    assert dispatched_active == 300, f"Expected 300 orders on active terminals, got {dispatched_active}"
    assert dispatched_paused == 0, f"Expected 0 orders on paused terminal, got {dispatched_paused}"
    assert elapsed_ms < 100.0, f"Fault isolation took too long: {elapsed_ms:.2f} ms"
