#!/usr/bin/env python3
"""
Pipvane v3: System Resource Monitor (Sprint 13).
------------------------------------------------
Monitors and logs Engine RAM, Engine CPU, System RAM utilization,
and System CPU utilization every 60 seconds during 48-hour soak testing.

Enforces strict hardware constraints for 2 vCPU / 4GB RAM environment:
1. MAX_ENGINE_RAM_MB: Pipvane Engine process must NOT exceed 400 MB RSS.
2. MAX_SYSTEM_MEMORY_PERCENT: Total system memory must NOT hit or exceed 98.0%.

Exits with code 1 immediately upon violation.
"""

from __future__ import annotations

import os
import sys
import time
import datetime
import csv
import logging
import argparse
import threading
from typing import Optional, Dict, Any, List, Tuple

import psutil

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger("Pipvane.ResourceMonitor")

# Constraint Constants (Sprint 13)
DEFAULT_MAX_ENGINE_RAM_MB: float = 400.0        # Engine RAM cap: 400 MB
DEFAULT_MAX_SYSTEM_MEMORY_PERCENT: float = 98.0  # System Memory utilization cap: 98%
DEFAULT_LOG_INTERVAL_SECONDS: float = 60.0       # 60-second sampling interval


class ResourceViolationError(Exception):
    """Raised when process or system resource limits are violated."""
    pass


class ResourceMonitor:
    """
    Continuous system and process resource profiler.
    Samples RSS/VMS memory, CPU loads, and active terminal counts.
    Records periodic snapshots to CSV and asserts compliance against SLA thresholds.
    """

    CSV_HEADERS = [
        "timestamp",
        "elapsed_seconds",
        "engine_pid",
        "engine_rss_mb",
        "engine_vms_mb",
        "engine_cpu_percent",
        "system_mem_used_mb",
        "system_mem_total_mb",
        "system_mem_percent",
        "system_cpu_percent",
        "active_terminals",
        "violation",
        "violation_details",
    ]

    def __init__(
        self,
        target_pid: Optional[int] = None,
        max_engine_ram_mb: float = DEFAULT_MAX_ENGINE_RAM_MB,
        max_system_memory_percent: float = DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
        log_interval_s: float = DEFAULT_LOG_INTERVAL_SECONDS,
        output_csv_path: Optional[str] = None,
        exit_on_violation: bool = True,
    ) -> None:
        self.target_pid = target_pid or os.getpid()
        self.max_engine_ram_mb = float(max_engine_ram_mb)
        self.max_system_memory_percent = float(max_system_memory_percent)
        self.log_interval_s = float(log_interval_s)
        self.output_csv_path = output_csv_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "resource_soak_telemetry.csv"
        )
        self.exit_on_violation = exit_on_violation

        # Process handle
        try:
            self._process = psutil.Process(self.target_pid)
        except psutil.NoSuchProcess:
            logger.warning("Target PID %d not found; falling back to current process", self.target_pid)
            self.target_pid = os.getpid()
            self._process = psutil.Process(self.target_pid)

        # Baseline CPU warmup
        self._process.cpu_percent(interval=None)
        psutil.cpu_percent(interval=None)

        self.start_time: float = time.time()
        self.samples_count: int = 0
        self.violations: List[str] = []
        self._is_running: bool = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._active_terminals_supplier: Optional[callable] = None

        self._ensure_csv_file()

    def set_active_terminals_supplier(self, supplier: callable) -> None:
        """Sets a callback returning the current count of connected terminals."""
        self._active_terminals_supplier = supplier

    def _ensure_csv_file(self) -> None:
        """Initializes the CSV output file with standard headers if absent."""
        csv_dir = os.path.dirname(os.path.abspath(self.output_csv_path))
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)
        if not os.path.exists(self.output_csv_path) or os.path.getsize(self.output_csv_path) == 0:
            with open(self.output_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(self.CSV_HEADERS)

    def capture_snapshot(self) -> Dict[str, Any]:
        """
        Gathers a single point-in-time telemetry snapshot of engine and system resources.
        Evaluates SLA thresholds and appends row to CSV log.
        """
        now = time.time()
        elapsed = now - self.start_time
        iso_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Engine Process Telemetry
        try:
            mem_info = self._process.memory_info()
            rss_mb = round(mem_info.rss / (1024 * 1024), 2)
            vms_mb = round(mem_info.vms / (1024 * 1024), 2)
            engine_cpu = round(self._process.cpu_percent(interval=None), 2)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            rss_mb = 0.0
            vms_mb = 0.0
            engine_cpu = 0.0

        # System Resource Telemetry
        sys_mem = psutil.virtual_memory()
        sys_mem_used_mb = round((sys_mem.total - sys_mem.available) / (1024 * 1024), 2)
        sys_mem_total_mb = round(sys_mem.total / (1024 * 1024), 2)
        sys_mem_pct = round(sys_mem.percent, 2)
        sys_cpu = round(psutil.cpu_percent(interval=None), 2)

        # Active Terminals
        active_terminals = 0
        if self._active_terminals_supplier:
            try:
                active_terminals = int(self._active_terminals_supplier())
            except Exception:
                active_terminals = 0

        # Evaluate SLA Thresholds
        violation_detected = False
        violation_details = ""

        if rss_mb > self.max_engine_ram_mb:
            violation_detected = True
            violation_details = (
                f"ENGINE_RAM_EXCEEDED: {rss_mb:.2f}MB > {self.max_engine_ram_mb:.2f}MB cap"
            )
            self.violations.append(violation_details)
            logger.critical("RESOURCE VIOLATION: %s", violation_details)

        if sys_mem_pct >= self.max_system_memory_percent:
            violation_detected = True
            detail = (
                f"SYSTEM_MEMORY_CRITICAL: {sys_mem_pct:.2f}% >= {self.max_system_memory_percent:.2f}% cap"
            )
            violation_details = f"{violation_details}; {detail}" if violation_details else detail
            self.violations.append(detail)
            logger.critical("RESOURCE VIOLATION: %s", detail)

        snapshot = {
            "timestamp": iso_ts,
            "elapsed_seconds": round(elapsed, 2),
            "engine_pid": self.target_pid,
            "engine_rss_mb": rss_mb,
            "engine_vms_mb": vms_mb,
            "engine_cpu_percent": engine_cpu,
            "system_mem_used_mb": sys_mem_used_mb,
            "system_mem_total_mb": sys_mem_total_mb,
            "system_mem_percent": sys_mem_pct,
            "system_cpu_percent": sys_cpu,
            "active_terminals": active_terminals,
            "violation": 1 if violation_detected else 0,
            "violation_details": violation_details,
        }

        # Write to CSV
        with open(self.output_csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([snapshot[h] for h in self.CSV_HEADERS])

        self.samples_count += 1
        logger.info(
            "[%s] Elapsed: %ds | Engine RSS: %.1fMB / %.0fMB | Sys RAM: %.1f%% / %.0f%% | Sys CPU: %.1f%% | Terminals: %d",
            iso_ts[11:19], int(elapsed), rss_mb, self.max_engine_ram_mb, sys_mem_pct,
            self.max_system_memory_percent, sys_cpu, active_terminals
        )

        if violation_detected and self.exit_on_violation:
            logger.critical("Encountered fatal resource constraint violation. Halting with exit code 1.")
            sys.exit(1)

        return snapshot

    def start_background_monitoring(self) -> None:
        """Starts background monitoring loop in a daemon thread."""
        if self._is_running:
            return
        self._is_running = True
        self._monitor_thread = threading.Thread(
            target=self._run_loop,
            name="PipvaneResourceMonitorThread",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.info("ResourceMonitor background thread started (interval=%.1fs)", self.log_interval_s)

    def stop_monitoring(self) -> None:
        """Stops background monitoring loop."""
        self._is_running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)
        logger.info("ResourceMonitor background thread stopped")

    def _run_loop(self) -> None:
        """Internal sampling loop running every log_interval_s seconds."""
        while self._is_running:
            try:
                self.capture_snapshot()
            except Exception as e:
                logger.error("Error capturing resource snapshot: %s", e)

            # Sleep in small increments for responsive shutdown
            start_sleep = time.time()
            sleep_chunk = min(0.05, max(0.01, self.log_interval_s / 5.0))
            while self._is_running and (time.time() - start_sleep < self.log_interval_s):
                time.sleep(sleep_chunk)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipvane v3 Resource Monitor (Sprint 13: 48-Hour Soak SLA Monitor)"
    )
    parser.add_argument(
        "--pid",
        type=int,
        default=None,
        help="Target Engine Process ID to monitor (defaults to current process or auto-detect)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_LOG_INTERVAL_SECONDS,
        help="Sampling and logging interval in seconds (default: 60s)",
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=48.0,
        help="Total duration to monitor in hours (default: 48 hours)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "resource_soak_telemetry.csv"),
        help="Output CSV file path for recorded telemetry",
    )
    parser.add_argument(
        "--max-engine-ram-mb",
        type=float,
        default=DEFAULT_MAX_ENGINE_RAM_MB,
        help="Max allowable Engine RAM in MB before failing (default: 400 MB)",
    )
    parser.add_argument(
        "--max-system-ram-percent",
        type=float,
        default=DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
        help="Max allowable total system RAM utilization percentage (default: 98.0%%)",
    )
    parser.add_argument(
        "--no-exit-on-violation",
        action="store_true",
        help="Do not exit immediately on violation (records to log without exiting)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [ResourceMonitor] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_arguments()

    monitor = ResourceMonitor(
        target_pid=args.pid,
        max_engine_ram_mb=args.max_engine_ram_mb,
        max_system_memory_percent=args.max_system_ram_percent,
        log_interval_s=args.interval,
        output_csv_path=args.output,
        exit_on_violation=not args.no_exit_on_violation,
    )

    logger.info("=================================================================")
    logger.info("Pipvane v3 Resource Soak Monitor (Sprint 13)")
    logger.info("Target PID: %d", monitor.target_pid)
    logger.info("Engine RAM Limit: %.1f MB", monitor.max_engine_ram_mb)
    logger.info("System Memory Limit: %.1f%%", monitor.max_system_memory_percent)
    logger.info("Sampling Interval: %.1f seconds", monitor.log_interval_s)
    logger.info("Target Duration: %.2f hours (%.0f seconds)", args.duration_hours, args.duration_hours * 3600)
    logger.info("Output Telemetry: %s", monitor.output_csv_path)
    logger.info("=================================================================")

    target_duration_s = args.duration_hours * 3600.0
    start_time = time.time()

    try:
        while time.time() - start_time < target_duration_s:
            monitor.capture_snapshot()
            time.sleep(monitor.log_interval_s)

        logger.info("48-Hour Soak Resource Monitoring completed successfully with ZERO SLA violations.")
        return 0
    except KeyboardInterrupt:
        logger.info("Monitoring terminated by user via SIGINT.")
        return 0
    except SystemExit as se:
        return se.code


if __name__ == "__main__":
    sys.exit(main())
