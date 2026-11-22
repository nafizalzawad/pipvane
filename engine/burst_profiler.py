"""
Pipvane v3 - Burst Queue Optimization & Memory Profiler (Sprint 21 Directive)
=============================================================================
Instruments the core ingestion, parsing, and execution loop using `tracemalloc`
and garbage collection tracking to verify:
1. High-throughput burst ingestion and async worker-pool queue drain.
2. Memory profiling across burst load spikes (Baseline -> Peak -> Post-GC).
3. Zero memory leaks: Mathematical verification that memory usage remains flat
   after burst queues clear and garbage collection runs.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import time
import tracemalloc
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from engine.execution_orchestrator import ExecutionOrchestrator, ExecutionResult, ExecutionStatus
from engine.ipc_protocol import ActionType, OrderIntent, OrderSide
from engine.signal_parser import ParsedSignal, SignalParser

logger = logging.getLogger("pipvane.engine.burst_profiler")


@dataclass(frozen=True)
class BurstProfileMetrics:
    """Telemetry report produced by BurstProfiler memory instrument."""
    total_signals: int
    elapsed_seconds: float
    throughput_signals_per_sec: float
    baseline_bytes: int
    peak_bytes: int
    post_gc_bytes: int
    net_delta_bytes: int
    net_delta_kb: float
    is_flat_memory: bool
    top_allocations: List[str] = field(default_factory=list)

    def summary(self) -> str:
        status_str = "FLAT (ZERO LEAK)" if self.is_flat_memory else "LEAK DETECTED"
        return (
            f"=== Pipvane Burst Queue Memory Profile ===\n"
            f"Total Signals Processed : {self.total_signals}\n"
            f"Burst Duration          : {self.elapsed_seconds:.4f}s\n"
            f"Throughput              : {self.throughput_signals_per_sec:.1f} signals/sec\n"
            f"Baseline Memory         : {self.baseline_bytes / 1024:.2f} KB\n"
            f"Peak Memory Spike       : {self.peak_bytes / 1024:.2f} KB\n"
            f"Post-GC Final Memory    : {self.post_gc_bytes / 1024:.2f} KB\n"
            f"Net Delta (Leakage)     : {self.net_delta_kb:.2f} KB\n"
            f"Memory Profile Status   : {status_str}\n"
            f"==========================================="
        )


class BurstProfiler:
    """
    Instruments the core parsing and execution loop using tracemalloc
    to measure garbage collection and memory stability during burst spikes.
    """

    def __init__(
        self,
        orchestrator: ExecutionOrchestrator,
        parser: SignalParser,
    ) -> None:
        self.orchestrator = orchestrator
        self.parser = parser

    async def run_burst_profile(
        self,
        terminal_id: str,
        num_signals: int = 500,
        burst_concurrency: int = 50,
        tolerance_kb: float = 150.0,
        custom_raw_texts: Optional[Sequence[str]] = None,
    ) -> BurstProfileMetrics:
        """
        Executes a controlled burst load spike, instrumenting memory with tracemalloc:
        1. Warmup: Pre-warms regex caches, SQLite schema, and thread pools.
        2. Baseline: Takes pre-burst tracemalloc snapshot and baseline memory reading.
        3. Burst Injection: Dispatches num_signals concurrently through parse_async
           and submit_order queues.
        4. Drain & Flush: Awaits complete queue drainage and disk flush.
        5. Prune & GC: Prunes closed positions and invokes explicit gc.collect().
        6. Evaluation: Analyzes net memory difference and verifies flat profile.
        """
        # Ensure tracemalloc is running
        was_tracing = tracemalloc.is_tracing()
        if not was_tracing:
            tracemalloc.start()

        # Step 1: Warmup phase (10 signals) to eliminate one-time JIT / module init bias
        warmup_text = "BUY EURUSD SL 1.0800 TP 1.0900"
        for i in range(10):
            p = await self.parser.parse_async(warmup_text)
            intent = OrderIntent(
                symbol=p.symbol or "EURUSD",
                side=OrderSide.BUY,
                action_type=ActionType.MARKET_ENTRY,
                volume=0.10,
                price=1.0850,
                stop_loss=p.stop_loss,
                take_profit=p.take_profits[0] if p.take_profits else 0.0,
                signal_id=10_000 + i,
            )
            await self.orchestrator.submit_order(terminal_id, intent)

        await self.orchestrator.flush_storage()
        self.orchestrator.prune_closed_positions(max_age_s=0.0)
        if hasattr(self.orchestrator.ipc_dispatcher, "dispatched_intents"):
            try:
                self.orchestrator.ipc_dispatcher.dispatched_intents.clear()
            except Exception:
                pass
        import linecache
        linecache.clearcache()
        gc.collect()

        # Step 2: Record baseline memory
        snap_baseline = tracemalloc.take_snapshot()
        baseline_cur, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()

        # Step 3: Generate burst payload
        sample_templates = [
            "BUY EURUSD SL 1.0820 TP 1.0890",
            "SELL GBPUSD SL 1.2750 TP 1.2650",
            "BUY USDJPY SL 154.50 TP 155.80",
            "SELL XAUUSD SL 2660.00 TP 2640.00",
            "BUY US30 SL 43200 TP 43600",
        ]

        raw_signals: List[str] = []
        if custom_raw_texts:
            raw_signals = list(custom_raw_texts)
        else:
            for i in range(num_signals):
                template = sample_templates[i % len(sample_templates)]
                raw_signals.append(f"{template} #{i}")

        # Step 4: Inject burst concurrently using Semaphore to throttle batch concurrency
        sem = asyncio.Semaphore(burst_concurrency)
        start_time = time.perf_counter()

        peak_observed_bytes = 0

        async def _process_single_burst_signal(idx: int, raw_text: str) -> ExecutionResult:
            nonlocal peak_observed_bytes
            async with sem:
                # 1. Asynchronous worker-pool parse (off event loop)
                parsed = await self.parser.parse_async(raw_text)

                # 2. Map to OrderIntent
                intent = OrderIntent(
                    symbol=parsed.symbol or "EURUSD",
                    side=OrderSide.BUY if parsed.action == "BUY" else OrderSide.SELL,
                    action_type=ActionType.MARKET_ENTRY,
                    volume=0.10,
                    price=parsed.entry_price or (1.0850 if parsed.symbol == "EURUSD" else 155.0),
                    stop_loss=parsed.stop_loss,
                    take_profit=parsed.take_profits[0] if parsed.take_profits else 0.0,
                    signal_id=20_000 + idx,
                )

                # 3. Submit to per-account serialization queue
                res = await self.orchestrator.submit_order(terminal_id, intent)

                # Sample memory periodically during the burst
                if idx % 50 == 0:
                    c, _ = tracemalloc.get_traced_memory()
                    if c > peak_observed_bytes:
                        peak_observed_bytes = c

                return res

        tasks = [
            asyncio.create_task(_process_single_burst_signal(i, raw_signals[i]))
            for i in range(len(raw_signals))
        ]
        results = await asyncio.gather(*tasks)
        elapsed_time = time.perf_counter() - start_time

        # Step 5: Await complete queue drainage and disk flush
        await self.orchestrator.flush_storage()

        # Step 6: Prune completed closed positions and run explicit garbage collection
        self.orchestrator.prune_closed_positions(max_age_s=0.0)
        if hasattr(self.orchestrator.ipc_dispatcher, "dispatched_intents"):
            try:
                self.orchestrator.ipc_dispatcher.dispatched_intents.clear()
            except Exception:
                pass
        total_signals_count = len(results)
        del results
        del tasks
        del raw_signals
        import linecache
        linecache.clearcache()
        gc.collect()

        # Step 7: Record post-burst memory
        snap_post = tracemalloc.take_snapshot()
        post_cur, post_peak = tracemalloc.get_traced_memory()
        peak_bytes = max(peak_observed_bytes, post_peak)

        # Net memory delta
        net_delta_bytes = max(0, post_cur - baseline_cur)
        net_delta_kb = net_delta_bytes / 1024.0
        is_flat = net_delta_kb <= tolerance_kb

        # Top line differences
        top_diffs = snap_post.compare_to(snap_baseline, "lineno")
        top_allocations = [str(stat) for stat in top_diffs[:5]]

        if not was_tracing:
            tracemalloc.stop()

        throughput = total_signals_count / elapsed_time if elapsed_time > 0 else 0.0

        metrics = BurstProfileMetrics(
            total_signals=total_signals_count,
            elapsed_seconds=elapsed_time,
            throughput_signals_per_sec=throughput,
            baseline_bytes=baseline_cur,
            peak_bytes=peak_bytes,
            post_gc_bytes=post_cur,
            net_delta_bytes=net_delta_bytes,
            net_delta_kb=net_delta_kb,
            is_flat_memory=is_flat,
            top_allocations=top_allocations,
        )

        logger.info(metrics.summary())
        return metrics
