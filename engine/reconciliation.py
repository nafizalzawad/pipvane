"""
Pipvane v3: Position Reconciliation & Orphan Management Subsystem (Sprint 14).
-------------------------------------------------------------------------------
Maintains strict state synchronization between the Pipvane Engine trade database
and attached MetaTrader broker terminals.

Handles:
1. Manual External Closes: Catches trades closed directly on the terminal/broker
   without Engine notification, marking their database records as CLOSED_EXTERNALLY.
2. Orphan Detection: Detects positions carrying the Pipvane Magic Number that exist
   on the broker terminal but have no corresponding Engine entry.
3. Automated Orphan Resolution Policy: Immediately flattens unauthorized orphans
   when set to CLOSE_IMMEDIATELY (or adopts them into active management).
"""

from __future__ import annotations

import os
import sys
import time
import sqlite3
import asyncio
import logging
import enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Callable

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
    PositionItem,
    PositionsSyncPayload,
)
from engine.ipc_dispatcher import IPCDispatcher

logger = logging.getLogger("pipvane.engine.reconciliation")

DEFAULT_PIPVANE_MAGIC: int = 123456
DEFAULT_RECONCILIATION_INTERVAL_S: float = 30.0


class PositionStatus(str, enum.Enum):
    """Lifecycle status of a managed trade in the database."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CLOSED_EXTERNALLY = "CLOSED_EXTERNALLY"
    ORPHAN_DETECTED = "ORPHAN_DETECTED"
    ORPHAN_RESOLVED = "ORPHAN_RESOLVED"


class ResolutionPolicy(str, enum.Enum):
    """User-defined policy for resolving detected orphan positions."""
    CLOSE_IMMEDIATELY = "CLOSE_IMMEDIATELY"
    ADOPT = "ADOPT"
    ALERT_ONLY = "ALERT_ONLY"


@dataclass
class TradeRecord:
    """Persistent database record for a managed trade position."""
    ticket: int
    terminal_id: str
    symbol: str
    side: int                            # 0: BUY, 1: SELL
    volume: float
    entry_price: float
    stop_loss: float = 0.0
    take_profit: float = 0.0
    magic_number: int = DEFAULT_PIPVANE_MAGIC
    status: str = PositionStatus.OPEN.value
    close_price: Optional[float] = None
    close_time: Optional[int] = None
    close_reason: Optional[str] = None
    opened_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))


@dataclass
class ReconciliationAlert:
    """Security and discrepancy alert emitted during reconciliation."""
    alert_type: str                      # "CLOSED_EXTERNALLY", "ORPHAN_DETECTED", "ORPHAN_RESOLVED"
    terminal_id: str
    ticket: int
    symbol: str
    timestamp: int
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReconciliationReport:
    """Audit summary produced by a reconciliation scan."""
    terminal_id: str
    reconciled_at: int
    db_open_count: int = 0
    broker_position_count: int = 0
    matched_count: int = 0
    closed_externally_count: int = 0
    orphans_detected_count: int = 0
    orphans_resolved_count: int = 0
    closed_externally_tickets: List[int] = field(default_factory=list)
    orphan_tickets: List[int] = field(default_factory=list)
    alerts: List[ReconciliationAlert] = field(default_factory=list)


class ReconciliationEngine:
    """
    Core state reconciliation engine.
    Audits broker terminal position arrays against the local SQLite trade repository.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        dispatcher: Optional[IPCDispatcher] = None,
        pipvane_magic_number: int = DEFAULT_PIPVANE_MAGIC,
        default_policy: ResolutionPolicy = ResolutionPolicy.CLOSE_IMMEDIATELY,
        reconciliation_interval_s: float = DEFAULT_RECONCILIATION_INTERVAL_S,
    ) -> None:
        self.db_path = db_path
        self.dispatcher = dispatcher
        self.pipvane_magic_number = pipvane_magic_number
        self.default_policy = default_policy
        self.reconciliation_interval_s = float(reconciliation_interval_s)

        self._alert_listeners: List[Callable[[ReconciliationAlert], None]] = []
        self._cached_broker_positions: Dict[str, List[PositionItem]] = {}
        self._periodic_task: Optional[asyncio.Task] = None
        self._is_running = False

        # Initialize SQLite database
        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_db()

        # Connect dispatcher positions sync listener if dispatcher provided
        if self.dispatcher:
            self.dispatcher.on_positions_sync(self._on_positions_sync_received)

    def _init_db(self) -> None:
        """Initializes SQLite schema for trade state persistence."""
        with self._db:
            self._db.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_trades (
                    ticket INTEGER PRIMARY KEY,
                    terminal_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side INTEGER NOT NULL,
                    volume REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_loss REAL,
                    take_profit REAL,
                    magic_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER,
                    close_price REAL,
                    close_reason TEXT,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self._db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trades_terminal_status
                ON managed_trades (terminal_id, status)
                """
            )

    # --- Database Operations ---

    def record_trade(self, trade: TradeRecord) -> None:
        """Inserts or updates a trade record in the database."""
        now = int(time.time())
        with self._db:
            self._db.execute(
                """
                INSERT INTO managed_trades (
                    ticket, terminal_id, symbol, side, volume, entry_price,
                    stop_loss, take_profit, magic_number, status, open_time,
                    close_time, close_price, close_reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticket) DO UPDATE SET
                    status=excluded.status,
                    close_time=excluded.close_time,
                    close_price=excluded.close_price,
                    close_reason=excluded.close_reason,
                    updated_at=excluded.updated_at
                """,
                (
                    trade.ticket,
                    trade.terminal_id,
                    trade.symbol,
                    trade.side,
                    trade.volume,
                    trade.entry_price,
                    trade.stop_loss,
                    trade.take_profit,
                    trade.magic_number,
                    trade.status,
                    trade.opened_at,
                    trade.close_time,
                    trade.close_price,
                    trade.close_reason,
                    now,
                ),
            )

    def get_trade(self, ticket: int) -> Optional[TradeRecord]:
        """Retrieves a single trade record by ticket number."""
        cursor = self._db.cursor()
        cursor.execute("SELECT * FROM managed_trades WHERE ticket = ?", (ticket,))
        row = cursor.fetchone()
        if not row:
            return None
        return TradeRecord(
            ticket=row["ticket"],
            terminal_id=row["terminal_id"],
            symbol=row["symbol"],
            side=row["side"],
            volume=row["volume"],
            entry_price=row["entry_price"],
            stop_loss=row["stop_loss"] or 0.0,
            take_profit=row["take_profit"] or 0.0,
            magic_number=row["magic_number"],
            status=row["status"],
            close_price=row["close_price"],
            close_time=row["close_time"],
            close_reason=row["close_reason"],
            opened_at=row["open_time"],
            updated_at=row["updated_at"],
        )

    def get_open_trades(self, terminal_id: Optional[str] = None) -> List[TradeRecord]:
        """Returns all trades currently marked as OPEN in the database."""
        cursor = self._db.cursor()
        if terminal_id:
            cursor.execute(
                "SELECT * FROM managed_trades WHERE status = ? AND terminal_id = ?",
                (PositionStatus.OPEN.value, terminal_id),
            )
        else:
            cursor.execute(
                "SELECT * FROM managed_trades WHERE status = ?",
                (PositionStatus.OPEN.value,),
            )
        rows = cursor.fetchall()
        return [
            TradeRecord(
                ticket=r["ticket"],
                terminal_id=r["terminal_id"],
                symbol=r["symbol"],
                side=r["side"],
                volume=r["volume"],
                entry_price=r["entry_price"],
                stop_loss=r["stop_loss"] or 0.0,
                take_profit=r["take_profit"] or 0.0,
                magic_number=r["magic_number"],
                status=r["status"],
                close_price=r["close_price"],
                close_time=r["close_time"],
                close_reason=r["close_reason"],
                opened_at=r["open_time"],
                updated_at=r["updated_at"],
            )
            for r in rows
        ]

    def update_trade_status(
        self,
        ticket: int,
        status: str,
        close_reason: Optional[str] = None,
        close_price: Optional[float] = None,
    ) -> None:
        """Updates trade status, close reason, and close timestamp."""
        now = int(time.time())
        with self._db:
            self._db.execute(
                """
                UPDATE managed_trades
                SET status = ?, close_reason = ?, close_price = ?, close_time = ?, updated_at = ?
                WHERE ticket = ?
                """,
                (status, close_reason, close_price, now, now, ticket),
            )

    def is_signal_open(self, signal_id: int) -> bool:
        """Returns True if a position associated with signal_id is actively open on any broker terminal."""
        if signal_id <= 0:
            return False
        # Check cached broker positions
        for terminal_id, pos_list in self._cached_broker_positions.items():
            for p in pos_list:
                if p.magic_number == self.pipvane_magic_number:
                    from engine.orphan_manager import OrphanManager
                    if OrphanManager.extract_signal_id(p) == signal_id:
                        return True
        return False

    # --- Telemetry & Listeners ---

    def add_alert_listener(self, listener: Callable[[ReconciliationAlert], None]) -> None:
        """Registers a listener callback for discrepancy and orphan alerts."""
        self._alert_listeners.append(listener)

    def _emit_alert(self, alert: ReconciliationAlert) -> None:
        """Dispatches an alert to all registered listeners."""
        for listener in self._alert_listeners:
            try:
                listener(alert)
            except Exception as e:
                logger.error("Error in reconciliation alert listener: %s", e)

    def _on_positions_sync_received(self, terminal_id: str, payload: PositionsSyncPayload) -> None:
        """Caches positions sync frame from dispatcher and triggers reconciliation."""
        self._cached_broker_positions[terminal_id] = payload.positions
        self.reconcile_terminal(terminal_id, broker_positions=payload.positions)

    # --- Core Reconciliation Logic ---

    def reconcile_terminal(
        self,
        terminal_id: str,
        broker_positions: Optional[List[PositionItem]] = None,
        policy: Optional[ResolutionPolicy] = None,
    ) -> ReconciliationReport:
        """
        Executes bidirectional state audit between local database and broker terminal positions.
        """
        now = int(time.time())
        effective_policy = policy or self.default_policy

        if broker_positions is None:
            broker_positions = self._cached_broker_positions.get(terminal_id, [])

        report = ReconciliationReport(
            terminal_id=terminal_id,
            reconciled_at=now,
            broker_position_count=len(broker_positions),
        )

        # 1. Fetch all local trades expected to be OPEN on this terminal
        db_open_trades = self.get_open_trades(terminal_id=terminal_id)
        report.db_open_count = len(db_open_trades)

        broker_tickets_map = {pos.ticket: pos for pos in broker_positions}

        # ======================================================================
        # Check 1: Manual External Close Detection (Task 1)
        # Position is OPEN in local database, but missing from broker terminal.
        # ======================================================================
        for trade in db_open_trades:
            if trade.ticket not in broker_tickets_map:
                # The position was manually closed by the user on the broker terminal!
                logger.warning(
                    "DISCREPANCY: Trade #%d (%s) missing from terminal %s! Updating to CLOSED_EXTERNALLY.",
                    trade.ticket, trade.symbol, terminal_id
                )
                self.update_trade_status(
                    ticket=trade.ticket,
                    status=PositionStatus.CLOSED_EXTERNALLY.value,
                    close_reason="MANUAL_EXTERNAL_CLOSE",
                )
                report.closed_externally_count += 1
                report.closed_externally_tickets.append(trade.ticket)

                alert = ReconciliationAlert(
                    alert_type="CLOSED_EXTERNALLY",
                    terminal_id=terminal_id,
                    ticket=trade.ticket,
                    symbol=trade.symbol,
                    timestamp=now,
                    details={
                        "volume": trade.volume,
                        "entry_price": trade.entry_price,
                        "reason": "MANUAL_EXTERNAL_CLOSE",
                    },
                )
                report.alerts.append(alert)
                self._emit_alert(alert)
            else:
                report.matched_count += 1

        # ======================================================================
        # Check 2: Orphan Detection & Resolution (Tasks 2 & 3)
        # Position exists on broker carrying Pipvane Magic Number, but has no
        # record in the Engine's local database.
        # ======================================================================
        for b_pos in broker_positions:
            if b_pos.magic_number == self.pipvane_magic_number:
                # Check if this ticket is in the database
                existing_record = self.get_trade(b_pos.ticket)

                is_orphan = False
                if not existing_record:
                    is_orphan = True
                elif existing_record.status in (
                    PositionStatus.CLOSED.value,
                    PositionStatus.CLOSED_EXTERNALLY.value,
                    PositionStatus.ORPHAN_RESOLVED.value,
                ):
                    # Ticket was closed in DB but resurrected or still active on broker
                    is_orphan = True

                if is_orphan:
                    report.orphans_detected_count += 1
                    report.orphan_tickets.append(b_pos.ticket)

                    logger.critical(
                        "ORPHAN DETECTED: Terminal %s has orphan ticket #%d (Magic=%d, Sym=%s, Vol=%.2f)! Applying policy: %s",
                        terminal_id, b_pos.ticket, b_pos.magic_number, b_pos.symbol, b_pos.volume, effective_policy.value
                    )

                    alert = ReconciliationAlert(
                        alert_type="ORPHAN_DETECTED",
                        terminal_id=terminal_id,
                        ticket=b_pos.ticket,
                        symbol=b_pos.symbol,
                        timestamp=now,
                        details={
                            "volume": b_pos.volume,
                            "magic_number": b_pos.magic_number,
                            "open_price": b_pos.open_price,
                            "resolution_policy": effective_policy.value,
                        },
                    )
                    report.alerts.append(alert)
                    self._emit_alert(alert)

                    # Execute Resolution Policy
                    self._resolve_orphan(terminal_id, b_pos, effective_policy, report)

        logger.info(
            "Reconciliation complete for %s: Matched=%d, ClosedExternally=%d, OrphansDetected=%d, OrphansResolved=%d",
            terminal_id, report.matched_count, report.closed_externally_count,
            report.orphans_detected_count, report.orphans_resolved_count
        )
        return report

    def _resolve_orphan(
        self,
        terminal_id: str,
        b_pos: PositionItem,
        policy: ResolutionPolicy,
        report: ReconciliationReport,
    ) -> None:
        """Applies configured ResolutionPolicy to flatten or adopt orphan."""
        now = int(time.time())

        if policy == ResolutionPolicy.CLOSE_IMMEDIATELY:
            # Dispatch Market Close command immediately to flatten the position
            close_intent = OrderIntent(
                symbol=b_pos.symbol,
                side=OrderSide.SELL if b_pos.position_type == 0 else OrderSide.BUY,
                action_type=ActionType.FULL_CLOSE,
                volume=b_pos.volume,
                price=b_pos.current_price,
                order_ticket=b_pos.ticket,
                magic_number=b_pos.magic_number,
                signal_id=int(time.time_ns() & 0x7FFFFFFFFFFFFFFF),
                timestamp_ns=time.time_ns(),
            )

            dispatched = False
            if self.dispatcher and self.dispatcher.can_route_to_terminal(terminal_id):
                dispatched = self.dispatcher.dispatch_order_intent(terminal_id, close_intent)
                logger.info(
                    "Orphan #%d: Dispatched ActionType.FULL_CLOSE to terminal %s (success=%s)",
                    b_pos.ticket, terminal_id, dispatched
                )

            # Record in DB as resolved orphan
            self.record_trade(
                TradeRecord(
                    ticket=b_pos.ticket,
                    terminal_id=terminal_id,
                    symbol=b_pos.symbol,
                    side=b_pos.position_type,
                    volume=b_pos.volume,
                    entry_price=b_pos.open_price,
                    stop_loss=b_pos.stop_loss,
                    take_profit=b_pos.take_profit,
                    magic_number=b_pos.magic_number,
                    status=PositionStatus.ORPHAN_RESOLVED.value,
                    close_price=b_pos.current_price,
                    close_time=now,
                    close_reason="CLOSE_IMMEDIATELY",
                )
            )
            report.orphans_resolved_count += 1

            resolve_alert = ReconciliationAlert(
                alert_type="ORPHAN_RESOLVED",
                terminal_id=terminal_id,
                ticket=b_pos.ticket,
                symbol=b_pos.symbol,
                timestamp=now,
                details={
                    "resolution": "MARKET_CLOSE_EXECUTED",
                    "dispatched": dispatched,
                },
            )
            report.alerts.append(resolve_alert)
            self._emit_alert(resolve_alert)

        elif policy == ResolutionPolicy.ADOPT:
            # Adopt orphan into the database under active management
            self.record_trade(
                TradeRecord(
                    ticket=b_pos.ticket,
                    terminal_id=terminal_id,
                    symbol=b_pos.symbol,
                    side=b_pos.position_type,
                    volume=b_pos.volume,
                    entry_price=b_pos.open_price,
                    stop_loss=b_pos.stop_loss,
                    take_profit=b_pos.take_profit,
                    magic_number=b_pos.magic_number,
                    status=PositionStatus.OPEN.value,
                    close_reason="ADOPTED_ORPHAN",
                )
            )
            report.orphans_resolved_count += 1
            logger.info("Orphan #%d: Adopted into active database management", b_pos.ticket)

        elif policy == ResolutionPolicy.ALERT_ONLY:
            # Leave unmanaged, record alert status
            self.record_trade(
                TradeRecord(
                    ticket=b_pos.ticket,
                    terminal_id=terminal_id,
                    symbol=b_pos.symbol,
                    side=b_pos.position_type,
                    volume=b_pos.volume,
                    entry_price=b_pos.open_price,
                    stop_loss=b_pos.stop_loss,
                    take_profit=b_pos.take_profit,
                    magic_number=b_pos.magic_number,
                    status=PositionStatus.ORPHAN_DETECTED.value,
                    close_reason="ALERT_ONLY_UNMANAGED",
                )
            )

    # --- Periodic Background Reconciliation Loop ---

    async def start_periodic_reconciliation(self) -> None:
        """Starts background periodic reconciliation task."""
        if self._is_running:
            return
        self._is_running = True
        self._periodic_task = asyncio.create_task(self._periodic_loop())
        logger.info(
            "Started periodic reconciliation loop (interval=%.1fs)", self.reconciliation_interval_s
        )

    async def stop_periodic_reconciliation(self) -> None:
        """Halts background periodic reconciliation task."""
        self._is_running = False
        if self._periodic_task and not self._periodic_task.done():
            self._periodic_task.cancel()
            try:
                await self._periodic_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped periodic reconciliation loop")

    async def _periodic_loop(self) -> None:
        """Internal periodic audit loop."""
        while self._is_running:
            try:
                await asyncio.sleep(self.reconciliation_interval_s)
                if self.dispatcher:
                    for tid in self.dispatcher.get_registered_terminals():
                        self.reconcile_terminal(tid)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in periodic reconciliation loop: %s", e)

    def close(self) -> None:
        """Closes SQLite database handle."""
        try:
            self._db.close()
        except Exception:
            pass
