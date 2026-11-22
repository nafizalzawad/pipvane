#!/usr/bin/env python3
"""
Pipvane v3: Network Chaos & Fault Injection Module
--------------------------------------------------
Implements automated, randomized network disruptions every 4 to 12 hours to validate
TDLib auto-reconnection resilience and channel backfill integrity under continuous soak load.
"""

import asyncio
import logging
import random
import os
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from typing import Callable, List, Optional  # noqa: E402
from engine.telegram_gateway import TelegramGateway  # noqa: E402


logger = logging.getLogger("Pipvane.FaultInjector")


class NetworkChaosFaultInjector:
    """
    Cron-style fault injector that periodically and randomly drops active sockets
    across monitored TelegramGateway sessions.
    """

    def __init__(
        self,
        min_interval_seconds: float = 4.0 * 3600.0,   # Default: 4 hours
        max_interval_seconds: float = 12.0 * 3600.0,  # Default: 12 hours
        accelerated_mode: bool = False,
    ):
        self.accelerated_mode = accelerated_mode
        if accelerated_mode:
            # Accelerated interval for unit tests & CI validation (0.2s - 0.8s)
            self.min_interval = 0.2
            self.max_interval = 0.8
        else:
            self.min_interval = min_interval_seconds
            self.max_interval = max_interval_seconds

        self._running: bool = False
        self._task: Optional[asyncio.Task] = None
        self.disruption_count: int = 0
        self._on_disruption_callbacks: List[Callable[[str, TelegramGateway], None]] = []

    def on_disruption(self, callback: Callable[[str, TelegramGateway], None]) -> None:
        self._on_disruption_callbacks.append(callback)

    async def start(self, gateways: List[TelegramGateway]) -> None:
        """Start the randomized chaos scheduler."""
        if self._running:
            return
        self._running = True
        logger.info(
            f"NetworkChaosFaultInjector started. Interval: [{self.min_interval:.1f}s - "
            f"{self.max_interval:.1f}s] (Accelerated={self.accelerated_mode})."
        )
        self._task = asyncio.create_task(self._chaos_loop(gateways))

    async def stop(self) -> None:
        """Stop the fault injector."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("NetworkChaosFaultInjector stopped.")

    async def inject_immediate_socket_drop(
        self,
        gateway: TelegramGateway,
        reason: str = "MANUAL_CHAOS_TRIGGER"
    ) -> None:
        """Immediately trigger a socket drop on a specific gateway instance."""
        self.disruption_count += 1
        logger.warning(f"Injecting socket drop on {gateway.profile_id} ({reason}).")
        gateway.drop_active_connection(simulated_reason=reason)

        for cb in self._on_disruption_callbacks:
            try:
                cb(reason, gateway)
            except Exception as e:
                logger.error(f"Error in disruption callback: {e}")

        # Simulate network recovery after transient outage (0.1s in accelerated, 2.0s in prod)
        recovery_delay = 0.1 if self.accelerated_mode else random.uniform(1.0, 5.0)
        await asyncio.sleep(recovery_delay)

        # Restore connection
        if gateway._client and hasattr(gateway._client, "simulate_incoming_update"):
            gateway._client.simulate_incoming_update({
                "@type": "updateConnectionState",
                "state": {"@type": "connectionStateReady"}
            })

    async def _chaos_loop(self, gateways: List[TelegramGateway]) -> None:
        """Background loop executing randomized disruptions."""
        while self._running:
            interval = random.uniform(self.min_interval, self.max_interval)
            logger.info(f"Next fault injection scheduled in {interval:.2f} seconds.")

            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

            if not self._running or not gateways:
                break

            target_gw = random.choice(gateways)
            reason = f"RANDOM_CHAOS_SOCKET_DROP_CYCLE_{self.disruption_count + 1}"
            await self.inject_immediate_socket_drop(target_gw, reason=reason)
