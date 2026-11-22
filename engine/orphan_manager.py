"""
Pipvane v3: Orphan Trade Handler & Manager (Sprint 14 Directive).
------------------------------------------------------------------
Detects and safely manages trades that exist on the broker but have no
corresponding record in the internal Engine database.

Core Tasks:
1. Orphan Detection:
   In the Reconciliation loop, if a broker position carries the Pipvane
   Magic Number but no matching OrderIntent exists in SQLite, flag it as ORPHANED.
2. Resolution Policy:
   Enforces user-configurable resolution policies:
   - IGNORE: Leave the orphan trade untouched on the broker, record audit telemetry.
   - ADOPT: Create a new internal record in SQLite and in-memory managed positions to manage it.
   - CLOSE_IMMEDIATELY: Immediately dispatch a market close command to liquidate for safety.
3. Alerting:
   Whenever an orphan is detected, immediately trigger an explicit alert via
   the Notification/Webhook dispatch mechanism.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from engine.ipc_protocol import (
    ActionType,
    OrderIntent,
    OrderSide,
    PositionItem,
)

logger = logging.getLogger("pipvane.engine.orphan_manager")

DEFAULT_PIPVANE_MAGIC: int = 123456


class OrphanResolutionPolicy(str, enum.Enum):
    """User-defined policy for resolving detected orphan positions."""
    IGNORE = "IGNORE"
    ADOPT = "ADOPT"
    CLOSE_IMMEDIATELY = "CLOSE_IMMEDIATELY"
    # Backward compatibility alias
    ALERT_ONLY = "IGNORE"


# Direct alias for backward compatibility
ResolutionPolicy = OrphanResolutionPolicy


@dataclass
class OrphanAlert:
    """Security and discrepancy alert emitted when an orphan trade is detected or resolved."""
    alert_type: str                     # "ORPHAN_DETECTED", "ORPHAN_RESOLVED", "ORPHAN_IGNORED"
    terminal_id: str
    ticket: int
    symbol: str
    volume: float
    magic_number: int
    open_price: float
    side: int                           # 0: BUY, 1: SELL
    policy: str
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_type": self.alert_type,
            "terminal_id": self.terminal_id,
            "ticket": self.ticket,
            "symbol": self.symbol,
            "volume": self.volume,
            "magic_number": self.magic_number,
            "open_price": self.open_price,
            "side": "BUY" if self.side == 0 else "SELL",
            "policy": self.policy,
            "timestamp": self.timestamp,
            "details": self.details,
        }


class OrphanNotificationClient:
    """
    Handles immediate alert dispatches via callbacks, Webhook HTTP POST,
    or WebSocket broadcasts when an orphan trade is detected.
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        callbacks: Optional[List[Callable[[OrphanAlert], Any]]] = None,
    ) -> None:
        self.webhook_url = webhook_url
        self._callbacks: List[Callable[[OrphanAlert], Any]] = list(callbacks or [])
        self._dispatched_alerts: List[OrphanAlert] = []

    def add_callback(self, cb: Callable[[OrphanAlert], Any]) -> None:
        """Register a notification callback listener."""
        self._callbacks.append(cb)

    async def notify(self, alert: OrphanAlert) -> None:
        """Dispatches alert to all listeners and external webhook if configured."""
        self._dispatched_alerts.append(alert)
        logger.warning(
            "ALERT [%s]: Orphan position #%d on %s (Sym=%s, Vol=%.2f, Magic=%d, Policy=%s)",
            alert.alert_type,
            alert.ticket,
            alert.terminal_id,
            alert.symbol,
            alert.volume,
            alert.magic_number,
            alert.policy,
        )

        # 1. Internal listener callbacks
        for cb in self._callbacks:
            try:
                res = cb(alert)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error("Error executing orphan alert callback: %s", e)

        # 2. External Webhook HTTP POST (if configured)
        if self.webhook_url:
            await self._dispatch_webhook(alert)

    async def _dispatch_webhook(self, alert: OrphanAlert) -> bool:
        """Asynchronously dispatches webhook payload without blocking the event loop."""
        if not self.webhook_url:
            return False

        payload_bytes = json.dumps(alert.to_dict()).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=payload_bytes,
            headers={"Content-Type": "application/json", "User-Agent": "Pipvane-Engine/3.0"},
            method="POST",
        )

        def _send() -> bool:
            try:
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    return resp.status in (200, 201, 202, 204)
            except Exception as exc:
                logger.error("Failed to POST orphan alert to webhook %s: %s", self.webhook_url, exc)
                return False

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _send)

    def get_dispatched_alerts(self) -> List[OrphanAlert]:
        """Returns history of all dispatched alerts."""
        return list(self._dispatched_alerts)


class OrphanManager:
    """
    Orphan Trade Handler & Resolution Manager.
    
    Detects broker positions carrying the Pipvane Magic Number that have no matching
    OrderIntent in the local database, applies configured resolution policies (IGNORE,
    ADOPT, CLOSE_IMMEDIATELY), and dispatches immediate security alerts.
    """

    def __init__(
        self,
        default_policy: OrphanResolutionPolicy = OrphanResolutionPolicy.CLOSE_IMMEDIATELY,
        pipvane_magic_number: int = DEFAULT_PIPVANE_MAGIC,
        orchestrator: Optional[Any] = None,
        ipc_dispatcher: Optional[Any] = None,
        webhook_url: Optional[str] = None,
        notification_client: Optional[OrphanNotificationClient] = None,
    ) -> None:
        self.default_policy = default_policy
        self.pipvane_magic_number = pipvane_magic_number
        self.orchestrator = orchestrator
        self.ipc_dispatcher = ipc_dispatcher
        self.terminal_policies: Dict[str, OrphanResolutionPolicy] = {}

        self.notification_client = notification_client or OrphanNotificationClient(
            webhook_url=webhook_url
        )
        self._detected_orphans: Dict[int, Dict[str, Any]] = {}

    def set_policy(
        self,
        policy: Union[OrphanResolutionPolicy, str],
        terminal_id: Optional[str] = None,
    ) -> None:
        """Sets resolution policy globally or for a specific terminal."""
        pol_enum = OrphanResolutionPolicy(policy)
        if terminal_id:
            self.terminal_policies[terminal_id] = pol_enum
            logger.info("Configured orphan policy for terminal %s to %s", terminal_id, pol_enum.value)
        else:
            self.default_policy = pol_enum
            logger.info("Configured default global orphan policy to %s", pol_enum.value)

    def get_policy(self, terminal_id: Optional[str] = None) -> OrphanResolutionPolicy:
        """Returns the active policy for the specified terminal or default."""
        if terminal_id and terminal_id in self.terminal_policies:
            return self.terminal_policies[terminal_id]
        return self.default_policy

    @staticmethod
    def extract_signal_id(position: PositionItem) -> Optional[int]:
        """Extracts signal_id from position comment if available (e.g. 'sig_1001', '1001')."""
        if not position or not getattr(position, "comment", None):
            return None
        import re
        m = re.search(r'(?:sig(?:nal)?_?|PV_?)?(\d+)', str(position.comment))
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
        return None

    def is_orphan(
        self,
        broker_pos: PositionItem,
        existing_order: Optional[Dict[str, Any]],
    ) -> bool:
        """
        Determines if a broker position qualifies as an ORPHAN:
        1. Carries Pipvane specific Magic Number.
        2. No matching active record exists in SQLite (None, or status not OPEN).
        """
        if broker_pos.magic_number != self.pipvane_magic_number:
            return False

        if existing_order is None:
            return True

        status = str(existing_order.get("status", "")).upper()
        if status not in ("OPEN", "PENDING"):
            # Position was closed in DB but resurrected or still active on broker
            return True

        return False

    async def handle_orphan(
        self,
        terminal_id: str,
        position: PositionItem,
        policy: Optional[Union[OrphanResolutionPolicy, str]] = None,
    ) -> Dict[str, Any]:
        """
        Processes an identified orphan position:
        1. Emits immediate ORPHAN_DETECTED alert via Notification/Webhook module.
        2. Applies configured Resolution Policy (IGNORE, ADOPT, CLOSE_IMMEDIATELY).
        3. Returns resolution execution telemetry.
        """
        pol = OrphanResolutionPolicy(policy) if policy else self.get_policy(terminal_id)
        now = time.time()

        # Step 1: Immediately Trigger Explicit Alert (Task 3)
        detect_alert = OrphanAlert(
            alert_type="ORPHAN_DETECTED",
            terminal_id=terminal_id,
            ticket=position.ticket,
            symbol=position.symbol,
            volume=position.volume,
            magic_number=position.magic_number,
            open_price=position.open_price,
            side=position.position_type,
            policy=pol.value,
            timestamp=now,
            details={
                "current_price": position.current_price,
                "profit": position.profit,
                "comment": position.comment,
            },
        )
        await self.notification_client.notify(detect_alert)

        # Step 2: Apply Resolution Policy (Task 2)
        result: Dict[str, Any] = {
            "ticket": position.ticket,
            "terminal_id": terminal_id,
            "symbol": position.symbol,
            "policy": pol.value,
            "handled_at": now,
            "action_taken": None,
            "success": False,
        }

        if pol == OrphanResolutionPolicy.IGNORE:
            result["action_taken"] = "IGNORED"
            result["success"] = True
            logger.warning(
                "Orphan #%d on %s: Policy is IGNORE. Leaving position unmanaged on broker.",
                position.ticket,
                terminal_id,
            )

            # Record in Journey log if orchestrator is available
            if self.orchestrator and hasattr(self.orchestrator, "record_journey_event"):
                self.orchestrator.record_journey_event(
                    signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                    ticket=position.ticket,
                    magic_number=position.magic_number,
                    terminal_id=terminal_id,
                    event_type="ORPHAN_IGNORED",
                    status="ORPHANED",
                    details=f"Orphan trade #{position.ticket} ({position.symbol}, {position.volume:.2f} lots) detected. Policy=IGNORE.",
                    source="orphan_manager",
                )

            ignore_alert = OrphanAlert(
                alert_type="ORPHAN_IGNORED",
                terminal_id=terminal_id,
                ticket=position.ticket,
                symbol=position.symbol,
                volume=position.volume,
                magic_number=position.magic_number,
                open_price=position.open_price,
                side=position.position_type,
                policy=pol.value,
                timestamp=time.time(),
                details={"status": "LEAVE_UNMANAGED"},
            )
            await self.notification_client.notify(ignore_alert)

        elif pol == OrphanResolutionPolicy.ADOPT:
            # ADOPT: Create internal SQLite record and register in active management
            extracted_sig_id = self.extract_signal_id(position)
            orphan_sig_id = extracted_sig_id if extracted_sig_id is not None else int(time.time_ns() & 0x7FFFFFFFFFFFFFFF)
            if self.orchestrator:
                if hasattr(self.orchestrator, "record_order_intent"):
                    self.orchestrator.record_order_intent(
                        signal_id=orphan_sig_id,
                        ticket=position.ticket,
                        magic_number=position.magic_number,
                        terminal_id=terminal_id,
                        symbol=position.symbol,
                        side=position.position_type,
                        volume=position.volume,
                        open_price=position.open_price,
                        stop_loss=position.stop_loss,
                        take_profit=position.take_profit,
                        status="OPEN",
                    )
                if hasattr(self.orchestrator, "record_journey_event"):
                    self.orchestrator.record_journey_event(
                        signal_id=orphan_sig_id,
                        ticket=position.ticket,
                        magic_number=position.magic_number,
                        terminal_id=terminal_id,
                        event_type="ORPHAN_ADOPTED",
                        status="OPEN",
                        details=f"Adopted orphan trade #{position.ticket} into active management.",
                        source="orphan_manager",
                    )
                # Register in in-memory ManagedPosition for TP/BE tracking
                from engine.execution_orchestrator import ManagedPosition
                adopted_pos = ManagedPosition(
                    ticket=position.ticket,
                    terminal_id=terminal_id,
                    symbol=position.symbol,
                    side=OrderSide(position.position_type),
                    entry_price=position.open_price,
                    original_volume=position.volume,
                    current_volume=position.volume,
                    current_sl=position.stop_loss,
                    current_tp=position.take_profit,
                )
                self.orchestrator.register_managed_position(adopted_pos)

            result["action_taken"] = "ADOPTED"
            result["signal_id"] = orphan_sig_id
            result["success"] = True
            logger.info("Orphan #%d on %s: Adopted into internal database state.", position.ticket, terminal_id)

            adopt_alert = OrphanAlert(
                alert_type="ORPHAN_RESOLVED",
                terminal_id=terminal_id,
                ticket=position.ticket,
                symbol=position.symbol,
                volume=position.volume,
                magic_number=position.magic_number,
                open_price=position.open_price,
                side=position.position_type,
                policy=pol.value,
                timestamp=time.time(),
                details={"action": "ADOPTED", "signal_id": orphan_sig_id},
            )
            await self.notification_client.notify(adopt_alert)

        elif pol == OrphanResolutionPolicy.CLOSE_IMMEDIATELY:
            # CLOSE_IMMEDIATELY: Liquidate position for safety
            close_intent = OrderIntent(
                symbol=position.symbol,
                side=OrderSide.SELL if position.position_type == 0 else OrderSide.BUY,
                action_type=ActionType.FULL_CLOSE,
                volume=position.volume,
                price=position.current_price,
                order_ticket=position.ticket,
                magic_number=position.magic_number,
                signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                timestamp_ns=time.time_ns(),
            )

            dispatched = False
            if self.orchestrator and hasattr(self.orchestrator, "submit_order"):
                try:
                    res = await self.orchestrator.submit_order(terminal_id, close_intent)
                    dispatched = res.success
                except Exception as e:
                    logger.error("Error submitting close intent for orphan #%d: %s", position.ticket, e)
            elif self.ipc_dispatcher and hasattr(self.ipc_dispatcher, "dispatch_order_intent"):
                dispatched = self.ipc_dispatcher.dispatch_order_intent(terminal_id, close_intent)

            result["action_taken"] = "CLOSED_IMMEDIATELY"
            result["dispatched"] = dispatched
            result["success"] = dispatched

            if self.orchestrator and hasattr(self.orchestrator, "record_journey_event"):
                self.orchestrator.record_journey_event(
                    signal_id=close_intent.signal_id,
                    ticket=position.ticket,
                    magic_number=position.magic_number,
                    terminal_id=terminal_id,
                    event_type="ORPHAN_CLOSED_IMMEDIATELY",
                    status="CLOSED",
                    details=f"Dispatched FULL_CLOSE for orphan #{position.ticket} (dispatched={dispatched}).",
                    source="orphan_manager",
                )

            logger.info(
                "Orphan #%d on %s: Dispatched FULL_CLOSE (success=%s)",
                position.ticket,
                terminal_id,
                dispatched,
            )

            resolve_alert = OrphanAlert(
                alert_type="ORPHAN_RESOLVED",
                terminal_id=terminal_id,
                ticket=position.ticket,
                symbol=position.symbol,
                volume=position.volume,
                magic_number=position.magic_number,
                open_price=position.open_price,
                side=position.position_type,
                policy=pol.value,
                timestamp=time.time(),
                details={"action": "CLOSED_IMMEDIATELY", "dispatched": dispatched},
            )
            await self.notification_client.notify(resolve_alert)

        self._detected_orphans[position.ticket] = result
        return result
