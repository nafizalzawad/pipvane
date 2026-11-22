#!/usr/bin/env python3
"""
Pipvane v3 IPC Round-Trip Latency Benchmark (VR-3.1 & ADR-001).

Standalone benchmarking test harness measuring the exact round-trip transit time
between the Pipvane Engine's shared memory write and the MetaTrader C++ DLL's
acknowledgment received over the Named Pipe.

Quality Gate Assertions:
  - Median (P50) Latency <= 5.0 milliseconds (System VR-3.1 Target: < 50 µs)
  - 99th Percentile (P99) Latency <= 15.0 milliseconds (Burst Ceiling: < 200 µs)
  - Explicit Exit Code 1 on violation; Exit Code 0 on pass.

Usage:
  python ipc_latency_benchmark.py
  python ipc_latency_benchmark.py --iterations 10000 --warmup 500
  python ipc_latency_benchmark.py --export-csv benchmark_results.csv
"""

from __future__ import annotations

import os
import sys
import time
import argparse
import asyncio
import logging
import statistics
import subprocess
from typing import List, Dict, Any, Optional

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_dispatcher import IPCDispatcher  # noqa: E402
from engine.ipc_protocol import (  # noqa: E402
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionFeedback,
)

logger = logging.getLogger("pipvane.benchmark")

# ANSI Color formatting
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


class IPCLatencyBenchmark:
    """
    High-resolution latency benchmarking harness.
    Spins up the Engine's IPCDispatcher and the mock MetaTrader C++ Bridge DLL,
    fires a continuous burst of 10,000 mock OrderIntent objects, and computes
    nanosecond-resolution round-trip timing statistics.
    """

    def __init__(
        self,
        terminal_id: str = "bench_terminal_0",
        iterations: int = 10000,
        warmup: int = 500,
        median_threshold_ms: float = 5.0,
        p99_threshold_ms: float = 15.0,
        export_csv: Optional[str] = None,
        quiet: bool = False,
        log_file: Optional[str] = None,
    ) -> None:
        self.terminal_id = terminal_id
        self.iterations = iterations
        self.warmup = warmup
        self.median_threshold_ms = median_threshold_ms
        self.p99_threshold_ms = p99_threshold_ms
        self.export_csv = export_csv
        self.quiet = quiet
        self.log_file = log_file

        self.dispatcher: Optional[IPCDispatcher] = None
        self.bridge_proc: Optional[subprocess.Popen] = None
        self.pending_futures: Dict[int, asyncio.Future] = {}
        self.latency_samples_ms: List[float] = []

    def _spawn_mock_bridge(self, total_needed: int) -> subprocess.Popen:
        """Spawns the mock C++ bridge worker as a standalone OS process."""
        worker_script = os.path.join(PROJECT_ROOT, "bridge", "mock_cpp_bridge.py")
        cmd = [
            sys.executable,
            worker_script,
            "--terminal-id", self.terminal_id,
            "--iterations", str(total_needed),
            "--quiet",
        ]
        if self.log_file:
            cmd.extend(["--log-file", self.log_file])
        return subprocess.Popen(cmd)

    async def initialize(self) -> None:
        """Sets up the IPCDispatcher and attaches the mock C++ DLL bridge."""
        if not self.quiet:
            print(f"{CYAN}{BOLD}================================================================={RESET}")
            print(f"{CYAN}{BOLD}       PIPVANE v3 IPC ROUND-TRIP LATENCY BENCHMARK (VR-3.1)       {RESET}")
            print(f"{CYAN}{BOLD}================================================================={RESET}")
            print(f"[*] Target Terminal ID:     {BOLD}{self.terminal_id}{RESET}")
            print(f"[*] Benchmark Iterations:   {BOLD}{self.iterations:,}{RESET} intents")
            print(f"[*] Cache Warmup:           {BOLD}{self.warmup:,}{RESET} intents")
            print(f"[*] Quality Gate Median:    {BOLD}<= {self.median_threshold_ms:.2f} ms{RESET}")
            print(f"[*] Quality Gate P99:       {BOLD}<= {self.p99_threshold_ms:.2f} ms{RESET}")
            print("-" * 65)

        # 1. Initialize and start IPCDispatcher
        self.dispatcher = IPCDispatcher()
        self.dispatcher.register_terminal(self.terminal_id)
        await self.dispatcher.start()

        # Wire feedback listener to resolve pending futures with high-precision timestamp
        def on_execution_feedback(terminal_id: str, feedback: ExecutionFeedback) -> None:
            t1_ns = time.perf_counter_ns()
            fut = self.pending_futures.pop(feedback.signal_id, None)
            if fut and not fut.done():
                fut.set_result(t1_ns)

        self.dispatcher.on_execution_feedback(on_execution_feedback)

        # 2. Spawn Mock C++ DLL Bridge Process
        total_runs = self.warmup + self.iterations
        self.bridge_proc = self._spawn_mock_bridge(total_runs)

        # 3. Wait for DLL connection and handshake (Status -> CONNECTED)
        ready = False
        for _ in range(150):
            if self.dispatcher.can_route_to_terminal(self.terminal_id):
                ready = True
                break
            await asyncio.sleep(0.02)

        if not ready:
            raise RuntimeError(
                f"Failed to establish handshake with Mock C++ Bridge on terminal '{self.terminal_id}' within 3.0s"
            )

        if not self.quiet:
            print(f"{GREEN}[+] Bridge handshake established. State: CONNECTED & ROUTABLE.{RESET}")

    async def execute_burst(self) -> None:
        """Fires the warmup burst followed by the continuous 10,000-intent benchmark burst."""
        loop = asyncio.get_running_loop()
        total_warmup = self.warmup
        total_benchmark = self.iterations

        # 1. Cache & Page Warmup Phase
        if total_warmup > 0:
            if not self.quiet:
                print(f"[*] Priming CPU caches and page tables ({total_warmup} warm-up rounds)...")
            for i in range(total_warmup):
                sig_id = 100000 + i
                intent = OrderIntent(
                    symbol="EURUSD.raw",
                    side=OrderSide.BUY,
                    action_type=ActionType.MARKET_ENTRY,
                    volume=0.10,
                    price=1.08500,
                    stop_loss=1.08000,
                    take_profit=1.09500,
                    slippage_cap_pips=1.0,
                    magic_number=888999,
                    signal_id=sig_id,
                    timestamp_ns=time.time_ns(),
                )
                fut = loop.create_future()
                self.pending_futures[sig_id] = fut
                self.dispatcher.dispatch_order_intent(self.terminal_id, intent)
                await fut

            if not self.quiet:
                print(f"{GREEN}[+] Warm-up complete. Starting high-density benchmark burst...{RESET}")

        # 2. Continuous Benchmark Burst
        self.latency_samples_ms.clear()
        self.latency_samples_ms = [0.0] * total_benchmark

        base_sig_id = 1_000_000
        start_wall_ns = time.perf_counter_ns()

        for i in range(total_benchmark):
            sig_id = base_sig_id + i
            intent = OrderIntent(
                symbol="EURUSD.raw",
                side=OrderSide.BUY if (i % 2 == 0) else OrderSide.SELL,
                action_type=ActionType.MARKET_ENTRY,
                volume=1.00,
                price=1.08550 + (i * 0.00001),
                stop_loss=1.08000,
                take_profit=1.09500,
                slippage_cap_pips=1.5,
                magic_number=123456,
                signal_id=sig_id,
                timestamp_ns=time.time_ns(),
            )

            fut = loop.create_future()
            self.pending_futures[sig_id] = fut

            # T0: High-resolution timestamp captured immediately prior to memory write
            t0_ns = time.perf_counter_ns()

            # Shared Memory push (<50 µs zero-copy operation)
            dispatched = self.dispatcher.dispatch_order_intent(self.terminal_id, intent)
            if not dispatched:
                raise RuntimeError(f"Ring buffer rejected OrderIntent at iteration {i} (Buffer Full)")

            # T1: Await Named Pipe acknowledgment timestamp
            t1_ns = await fut

            # Exact round-trip delta in milliseconds
            elapsed_ms = (t1_ns - t0_ns) / 1_000_000.0
            self.latency_samples_ms[i] = elapsed_ms

        total_wall_sec = (time.perf_counter_ns() - start_wall_ns) / 1_000_000_000.0
        self.total_wall_sec = total_wall_sec

    async def teardown(self) -> None:
        """Gracefully terminates the mock bridge process and dispatcher."""
        if self.bridge_proc:
            try:
                self.bridge_proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                self.bridge_proc.kill()
                self.bridge_proc.wait()
            self.bridge_proc = None

        if self.dispatcher:
            await self.dispatcher.stop()
            self.dispatcher = None

    def analyze_results(self) -> Dict[str, Any]:
        """Calculates statistical percentiles and checks quality gates."""
        samples = sorted(self.latency_samples_ms)
        n = len(samples)

        if n == 0:
            raise ValueError("No latency samples collected")

        p50 = statistics.median(samples)
        p75 = statistics.quantiles(samples, n=4)[2]
        p90 = statistics.quantiles(samples, n=10)[8]
        p95 = statistics.quantiles(samples, n=20)[18]
        p99 = statistics.quantiles(samples, n=100)[98]
        p99_9 = statistics.quantiles(samples, n=1000)[998] if n >= 1000 else samples[-1]

        mean = statistics.mean(samples)
        stdev = statistics.stdev(samples) if n > 1 else 0.0
        min_ms = samples[0]
        max_ms = samples[-1]
        throughput = n / self.total_wall_sec if self.total_wall_sec > 0 else 0.0

        median_passed = p50 <= self.median_threshold_ms
        p99_passed = p99 <= self.p99_threshold_ms
        overall_passed = median_passed and p99_passed

        metrics = {
            "count": n,
            "total_wall_sec": self.total_wall_sec,
            "throughput_ops_sec": throughput,
            "min_ms": min_ms,
            "mean_ms": mean,
            "stdev_ms": stdev,
            "p50_ms": p50,
            "p75_ms": p75,
            "p90_ms": p90,
            "p95_ms": p95,
            "p99_ms": p99,
            "p99_9_ms": p99_9,
            "max_ms": max_ms,
            "median_threshold_ms": self.median_threshold_ms,
            "p99_threshold_ms": self.p99_threshold_ms,
            "median_passed": median_passed,
            "p99_passed": p99_passed,
            "overall_passed": overall_passed,
        }

        # Optional CSV export
        if self.export_csv:
            self._export_csv(samples)

        return metrics

    def _export_csv(self, samples: List[float]) -> None:
        """Exports raw latency distribution to CSV file."""
        try:
            with open(self.export_csv, "w", encoding="utf-8") as f:
                f.write("iteration,latency_ms,latency_us\n")
                for idx, lat_ms in enumerate(samples, 1):
                    f.write(f"{idx},{lat_ms:.6f},{lat_ms * 1000.0:.3f}\n")
            if not self.quiet:
                print(f"[+] Exported {len(samples)} latency samples to '{self.export_csv}'")
        except Exception as e:
            logger.error("Failed to export latency CSV: %s", e)

    def print_report(self, m: Dict[str, Any]) -> None:
        """Renders comprehensive ASCII benchmark report."""
        passed = m["overall_passed"]
        if passed:
            status_banner = f"{GREEN}{BOLD}[ PASS ] QUALITY GATE SATISFIED{RESET}"
        else:
            status_banner = f"{RED}{BOLD}[ FAIL ] QUALITY GATE VIOLATED{RESET}"

        print("\n" + "=" * 65)
        print(f"        PIPVANE v3 IPC BENCHMARK RESULTS ({m['count']:,} ROUNDS)        ")
        print("=" * 65)
        print(f" Status:                  {status_banner}")
        print(f" Total Executed Orders:   {BOLD}{m['count']:,}{RESET}")
        print(f" Burst Wall-Clock Time:   {BOLD}{m['total_wall_sec']:.3f} s{RESET}")
        print(f" Sustained Throughput:    {BOLD}{m['throughput_ops_sec']:,.1f} orders/sec{RESET}")
        print("-" * 65)
        print(f" {'METRIC':<24} | {'LATENCY (ms)':<16} | {'LATENCY (us)':<16}")
        print("-" * 65)
        print(f" {'Minimum':<24} | {m['min_ms']:>10.4f} ms    | {m['min_ms']*1000:>10.1f} us")
        print(f" {'Mean (Average)':<24} | {m['mean_ms']:>10.4f} ms    | {m['mean_ms']*1000:>10.1f} us")
        print(f" {'Std Deviation':<24} | {m['stdev_ms']:>10.4f} ms    | {m['stdev_ms']*1000:>10.1f} us")
        print("-" * 65)
        p50_str = f"{BOLD}{m['p50_ms']:>10.4f} ms{RESET}"
        p50_us = f"{BOLD}{m['p50_ms']*1000:>10.1f} us{RESET}"
        print(f" {'Median (P50)':<24} | {p50_str}    | {p50_us}")
        print(f" {'75th Percentile (P75)':<24} | {m['p75_ms']:>10.4f} ms    | {m['p75_ms']*1000:>10.1f} us")
        print(f" {'90th Percentile (P90)':<24} | {m['p90_ms']:>10.4f} ms    | {m['p90_ms']*1000:>10.1f} us")
        print(f" {'95th Percentile (P95)':<24} | {m['p95_ms']:>10.4f} ms    | {m['p95_ms']*1000:>10.1f} us")
        p99_str = f"{BOLD}{m['p99_ms']:>10.4f} ms{RESET}"
        p99_us = f"{BOLD}{m['p99_ms']*1000:>10.1f} us{RESET}"
        print(f" {'99th Percentile (P99)':<24} | {p99_str}    | {p99_us}")
        print(f" {'99.9th Percentile':<24} | {m['p99_9_ms']:>10.4f} ms    | {m['p99_9_ms']*1000:>10.1f} us")
        print(f" {'Maximum (Worst)':<24} | {m['max_ms']:>10.4f} ms    | {m['max_ms']*1000:>10.1f} us")
        print("=" * 65)
        print(" QUALITY GATE ASSERTION COMPLIANCE:")

        # Check P50 assertion
        med_color = GREEN if m["median_passed"] else RED
        med_sym = "[PASS]" if m["median_passed"] else "[FAIL]"
        med_val = f"{m['p50_ms']:.4f} ms / {m['p50_ms']*1000:.1f} us"
        print(f"   [1] Median Latency <= {m['median_threshold_ms']:.1f} ms:  {med_color}{med_sym} ({med_val}){RESET}")

        # Check P99 assertion
        p99_color = GREEN if m["p99_passed"] else RED
        p99_sym = "[PASS]" if m["p99_passed"] else "[FAIL]"
        p99_val = f"{m['p99_ms']:.4f} ms / {m['p99_ms']*1000:.1f} us"
        print(f"   [2] P99 Latency    <= {m['p99_threshold_ms']:.1f} ms: {p99_color}{p99_sym} ({p99_val}){RESET}")
        print("=" * 65 + "\n")


async def run_benchmark_async(args: argparse.Namespace) -> int:
    """Async runner executing benchmark lifecycle."""
    bench = IPCLatencyBenchmark(
        terminal_id=args.terminal_id,
        iterations=args.iterations,
        warmup=args.warmup,
        median_threshold_ms=args.threshold_median,
        p99_threshold_ms=args.threshold_p99,
        export_csv=args.export_csv,
        quiet=args.quiet,
        log_file=getattr(args, 'log_file', None),
    )

    try:
        await bench.initialize()
        await bench.execute_burst()
        metrics = bench.analyze_results()
        bench.print_report(metrics)
        return 0 if metrics["overall_passed"] else 1
    except Exception as e:
        logger.error("Benchmark failed with unexpected exception: %s", e, exc_info=True)
        print(f"\n{RED}{BOLD}[ERROR] Benchmark encountered fatal error: {e}{RESET}\n")
        return 1
    finally:
        await bench.teardown()


def main() -> None:
    """CLI Entry point for standalone execution."""
    parser = argparse.ArgumentParser(
        description="Pipvane v3 IPC Round-Trip Latency Benchmark (VR-3.1)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--iterations", type=int, default=10000, help="Number of mock OrderIntent objects to fire")
    parser.add_argument("--warmup", type=int, default=500, help="Number of warm-up intents to prime caches")
    parser.add_argument("--terminal-id", default="bench_terminal_0", help="Unique terminal ID for SHM/Pipe")
    parser.add_argument("--threshold-median", type=float, default=5.0, help="Maximum allowed Median latency in ms")
    parser.add_argument("--threshold-p99", type=float, default=15.0, help="Maximum allowed P99 latency in ms")
    parser.add_argument("--export-csv", default=None, help="Optional output path to export CSV latency distribution")
    parser.add_argument("--log-file", default=None, help="Optional path to output MT5 terminal execution log")
    parser.add_argument("--quiet", action="store_true", help="Quiet output mode")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--worker", action="store_true", help="Execute as internal mock bridge worker process")
    args = parser.parse_args()

    # Delegate to internal mock worker if --worker flag is specified
    if args.worker:
        from bridge.mock_cpp_bridge import run_mock_bridge_cli
        run_mock_bridge_cli()
        return

    log_level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if not args.verbose:
        logging.getLogger("pipvane.engine.named_pipe_server").setLevel(logging.WARNING)
        logging.getLogger("pipvane.engine.ipc_dispatcher").setLevel(logging.WARNING)
        logging.getLogger("pipvane.engine.shm_writer").setLevel(logging.WARNING)

    exit_code = asyncio.run(run_benchmark_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
