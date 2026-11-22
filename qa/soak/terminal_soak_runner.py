#!/usr/bin/env python3
"""
Pipvane v3: Sprint 13 - Multi-Terminal Soak & Heavy Load Orchestrator.
----------------------------------------------------------------------
Validates system stability driving the maximum licensed limit (7 accounts)
on a constrained hardware profile (2 vCPUs / 4GB RAM).

Architecture:
1. Orchestrates 7 MetaTrader terminals (4x MT5 and 3x MT4).
2. Attaches C++ DLL bridges (MockCppBridgeWorker) to all 7 instances.
3. Uses TerminalDiscovery to detect and enroll all 7 terminals into IPCDispatcher.
4. Feeds high-frequency continuous signal & tick stream across all 7 terminals.
5. Employs ResourceMonitor to log RAM and CPU usage every 60 seconds.
6. Enforces strict SLA constraints: halts with exit code 1 if Engine RAM exceeds
   400 MB or total system memory hits 98% utilization.
"""

from __future__ import annotations

import os
import sys
import time
import asyncio
import logging
import argparse
import threading
from typing import List, Dict, Optional, Any

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionFeedback,
    AccountState,
    TerminalStatus,
)
from engine.ipc_dispatcher import IPCDispatcher, TerminalContext
from engine.terminal_discovery import TerminalDiscovery, DiscoveredTerminal
from bridge.mock_cpp_bridge import MockCppBridgeWorker
from qa.soak.resource_monitor import (
    ResourceMonitor,
    ResourceViolationError,
    DEFAULT_MAX_ENGINE_RAM_MB,
    DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
)

logger = logging.getLogger("Pipvane.TerminalSoakRunner")


class TerminalSoakOrchestrator:
    """
    Manages heavy-load continuous soak testing across 7 MT4/MT5 terminals.
    """

    # 7 Licensed Terminals Profile (4x MT5 + 3x MT4)
    TERMINAL_CONFIGS = [
        {"id": "mt5_hedge_01", "type": "MT5", "login": 500101, "server": "Pipvane-Demo-MT5-01"},
        {"id": "mt5_hedge_02", "type": "MT5", "login": 500102, "server": "Pipvane-Demo-MT5-02"},
        {"id": "mt5_netting_03", "type": "MT5", "login": 500103, "server": "Pipvane-Demo-MT5-Netting"},
        {"id": "mt5_hedge_04", "type": "MT5", "login": 500104, "server": "Pipvane-Demo-MT5-04"},
        {"id": "mt4_std_01", "type": "MT4", "login": 400101, "server": "Pipvane-Demo-MT4-01"},
        {"id": "mt4_std_02", "type": "MT4", "login": 400102, "server": "Pipvane-Demo-MT4-02"},
        {"id": "mt4_std_03", "type": "MT4", "login": 400103, "server": "Pipvane-Demo-MT4-03"},
    ]

    SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSD"]

    def __init__(
        self,
        duration_seconds: float = 48.0 * 3600.0,
        monitor_interval_s: float = 60.0,
        output_csv_path: Optional[str] = None,
        max_terminals: int = 7,
        orders_per_batch: int = 14,
        batch_interval_s: float = 0.5,
        max_engine_ram_mb: float = DEFAULT_MAX_ENGINE_RAM_MB,
        max_system_ram_percent: float = DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
        exit_on_violation: bool = True,
        session_id: Optional[str] = None,
    ) -> None:
        self.duration_seconds = float(duration_seconds)
        self.monitor_interval_s = float(monitor_interval_s)
        self.output_csv_path = output_csv_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "resource_soak_telemetry.csv"
        )
        self.max_terminals = min(max_terminals, len(self.TERMINAL_CONFIGS))
        self.orders_per_batch = orders_per_batch
        self.batch_interval_s = batch_interval_s
        self.session_id = session_id

        # Build terminal configs with optional session_id prefix for IPC isolation
        self.active_configs: List[Dict[str, Any]] = []
        for cfg in self.TERMINAL_CONFIGS[:self.max_terminals]:
            t_id = f"{session_id}_{cfg['id']}" if session_id else cfg["id"]
            self.active_configs.append({
                "id": t_id,
                "type": cfg["type"],
                "login": cfg["login"],
                "server": cfg["server"],
            })

        self.discovery = TerminalDiscovery()
        self.dispatcher = IPCDispatcher(use_mock_buffers=False)
        self.resource_monitor = ResourceMonitor(
            target_pid=os.getpid(),
            max_engine_ram_mb=max_engine_ram_mb,
            max_system_memory_percent=max_system_ram_percent,
            log_interval_s=monitor_interval_s,
            output_csv_path=self.output_csv_path,
            exit_on_violation=exit_on_violation,
        )

        self.bridge_workers: List[MockCppBridgeWorker] = []
        self.bridge_threads: List[threading.Thread] = []
        self.stop_event = threading.Event()

        self.total_orders_dispatched: int = 0
        self.total_feedbacks_received: int = 0
        self.final_shm_metrics: Dict[str, Dict[str, Any]] = {}
        self._feedbacks_lock = threading.Lock()

    def setup_terminals(self) -> None:
        """
        Registers all 7 terminals via TerminalDiscovery, attaches C++ bridges,
        and enrolls them into IPCDispatcher.
        """
        logger.info("Initializing %d MetaTrader terminals for 48-hour soak...", self.max_terminals)

        # 1. Register terminals in TerminalDiscovery registry
        for cfg in self.active_configs:
            term = DiscoveredTerminal(
                terminal_id=cfg["id"],
                platform_type=cfg["type"],
                account_login=cfg["login"],
                broker_server=cfg["server"],
                is_running=True,
            )
            self.discovery.register_terminal(term)

        # 2. Connect discovered terminals to Engine's IPCDispatcher
        connected_contexts = self.discovery.connect_all_discovered(
            self.dispatcher, max_terminals=self.max_terminals
        )
        logger.info("Enrolled %d terminals into IPCDispatcher", len(connected_contexts))

        # 3. Setup dispatcher feedback handler to track throughput
        def on_feedback(terminal_id: str, fb: ExecutionFeedback) -> None:
            with self._feedbacks_lock:
                self.total_feedbacks_received += 1

        self.dispatcher.on_execution_feedback(on_feedback)

        # 4. Supply active terminal count to ResourceMonitor
        self.resource_monitor.set_active_terminals_supplier(
            lambda: len(self.dispatcher.get_active_terminals())
        )

    async def start_bridge_workers(self) -> None:
        """Starts asynchronous Named Pipe servers and background C++ bridge workers."""
        logger.info("Starting IPCDispatcher pipe servers...")
        await self.dispatcher.start()

        # Small delay to ensure Windows Named Pipe servers are listening
        await asyncio.sleep(0.15)

        logger.info("Spawning %d C++ Mock Bridge Workers...", self.max_terminals)
        for cfg in self.active_configs:
            worker = MockCppBridgeWorker(
                terminal_id=cfg["id"],
                account_login=cfg["login"],
                broker_server=cfg["server"],
                platform_type=0 if cfg["type"] == "MT4" else 1,
                quiet=True,
            )
            connected = worker.connect_pipe(timeout_s=3.0)
            if not connected:
                raise RuntimeError(f"Failed to connect C++ bridge to pipe for {cfg['id']}")

            worker.send_init_handshake()
            worker.attach_shm()
            self.bridge_workers.append(worker)

            # Start consumer loop in daemon thread
            t = threading.Thread(
                target=worker.run_consumer_loop,
                kwargs={"target_iterations": 0, "stop_event": self.stop_event},
                name=f"BridgeWorkerThread-{cfg['id']}",
                daemon=True,
            )
            t.start()
            self.bridge_threads.append(t)

        # Allow handshakes to settle
        await asyncio.sleep(0.2)
        active = self.dispatcher.get_active_terminals()
        logger.info("C++ Bridges attached! Active terminals in dispatcher: %d / %d", len(active), self.max_terminals)

    async def run_soak(self) -> Dict[str, Any]:
        """
        Executes the continuous load soak test across all 7 terminals.
        Fires high-frequency order bursts concurrently while recording resource metrics.
        """
        self.setup_terminals()
        await self.start_bridge_workers()

        # Start resource monitor background logging
        self.resource_monitor.start_background_monitoring()

        start_time = time.time()
        batch_seq = 0
        terminals = [cfg["id"] for cfg in self.active_configs]

        logger.info("Beginning high-frequency tick & signal generation (Duration: %.1f seconds)...", self.duration_seconds)

        try:
            while (time.time() - start_time) < self.duration_seconds:
                batch_seq += 1
                symbol = self.SYMBOLS[batch_seq % len(self.SYMBOLS)]
                side = OrderSide.BUY if (batch_seq % 2 == 0) else OrderSide.SELL

                # Dispatch concurrent order burst across all 7 terminals
                for term_id in terminals:
                    sig_id = int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)
                    intent = OrderIntent(
                        symbol=symbol,
                        side=side,
                        action_type=ActionType.MARKET_ENTRY,
                        volume=0.10,
                        price=1.08500 if "EUR" in symbol else (2350.0 if "XAU" in symbol else 155.0),
                        stop_loss=1.08000,
                        take_profit=1.09500,
                        signal_id=sig_id,
                        timestamp_ns=time.time_ns(),
                    )
                    try:
                        self.dispatcher.dispatch_order_intent(term_id, intent)
                        self.total_orders_dispatched += 1
                    except Exception as e:
                        logger.error("Failed to dispatch order to %s: %s", term_id, e)

                # Responsive sleep interval between batches
                await asyncio.sleep(self.batch_interval_s)

        finally:
            await self.shutdown()

        elapsed = time.time() - start_time
        summary = {
            "elapsed_seconds": round(elapsed, 2),
            "total_orders_dispatched": self.total_orders_dispatched,
            "total_feedbacks_received": self.total_feedbacks_received,
            "samples_count": self.resource_monitor.samples_count,
            "violations_count": len(self.resource_monitor.violations),
            "violations": self.resource_monitor.violations,
            "final_shm_metrics": self.final_shm_metrics,
        }
        logger.info("=================================================================")
        logger.info("Soak Test Complete! Summary: %s", summary)
        logger.info("=================================================================")
        return summary

    async def shutdown(self) -> None:
        """Shuts down all bridge workers, threads, dispatcher, and resource monitor."""
        logger.info("Shutting down soak orchestrator...")
        self.stop_event.set()
        self.resource_monitor.stop_monitoring()

        # Snapshot SHM metrics before destroying writers
        for tid, ctx in self.dispatcher._terminals.items():
            try:
                self.final_shm_metrics[tid] = ctx.shm_writer.get_metrics()
            except Exception:
                pass

        for worker in self.bridge_workers:
            try:
                worker.close()
            except Exception:
                pass

        await self.dispatcher.stop()
        await asyncio.sleep(0.1)  # Allow Windows proactor to clean up pipe handles
        logger.info("Soak orchestrator shutdown complete")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipvane v3 Sprint 13: 48-Hour Multi-Terminal Soak Test"
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=48.0,
        help="Duration of soak test in hours (default: 48.0)",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Duration of soak test in seconds (overrides --duration-hours for testing)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=60.0,
        help="Resource monitor sampling interval in seconds (default: 60s)",
    )
    parser.add_argument(
        "--terminals",
        type=int,
        default=7,
        help="Number of terminals to orchestrate (default: 7)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to output CSV log file for resource metrics",
    )
    parser.add_argument(
        "--max-engine-ram-mb",
        type=float,
        default=DEFAULT_MAX_ENGINE_RAM_MB,
        help="Maximum allowable Engine RAM in MB (default: 400 MB)",
    )
    parser.add_argument(
        "--max-system-ram-percent",
        type=float,
        default=DEFAULT_MAX_SYSTEM_MEMORY_PERCENT,
        help="Maximum allowable System RAM utilization percentage (default: 98.0%%)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [TerminalSoak] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_arguments()

    duration_s = args.duration_seconds if args.duration_seconds is not None else (args.duration_hours * 3600.0)

    orchestrator = TerminalSoakOrchestrator(
        duration_seconds=duration_s,
        monitor_interval_s=args.interval,
        output_csv_path=args.output,
        max_terminals=args.terminals,
        max_engine_ram_mb=args.max_engine_ram_mb,
        max_system_ram_percent=args.max_system_ram_percent,
        exit_on_violation=True,
    )

    try:
        summary = asyncio.run(orchestrator.run_soak())
        if summary["violations_count"] > 0:
            logger.critical("Soak test failed SLA resource limits with %d violations!", summary["violations_count"])
            return 1
        return 0
    except SystemExit as se:
        return se.code
    except KeyboardInterrupt:
        logger.info("Soak test interrupted by user")
        return 0
    except Exception as e:
        logger.critical("Fatal error in soak orchestrator: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
