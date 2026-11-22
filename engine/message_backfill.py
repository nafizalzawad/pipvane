"""
Pipvane v3: Historical Message Backfill Engine (Sprint 16 Directive)
====================================================================
Queries Telegram for historical messages missed during node downtime:
1. Last Processed State:
   Queries local SQLite RawMessage database (or timeline meta) to identify
   the highest message_id for every subscribed channel.
2. TDLib History Request:
   Uses the official TDLib getChatHistory endpoint to iterate backward from
   the current live state until retrieved message_id matches the local state.
3. Deduplication Check:
   Processes backfilled messages through the normal pipeline (normalization, parsing).
   Before sending to the Orchestrator, cross-references Signal Hash and Signal ID
   against newly reconciled broker orders (from Step C of takeover).
   If a trade for that signal already exists, strictly marks it as SKIP_DUP.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from engine.ipc_protocol import OrderIntent, OrderSide, ActionType
from engine.normalizer import clean_text, normalize_raw_text
from engine.signal_parser import SignalParser, ParsedSignal
from engine.risk_engine import SkipReason, SKIP_DUP

logger = logging.getLogger("pipvane.engine.message_backfill")


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class BackfillMessageItem:
    """A raw or parsed historical message retrieved during backfill."""
    chat_id: int
    message_id: int
    date: int
    raw_text: str
    parsed_signal: Optional[ParsedSignal] = None
    order_intent: Optional[OrderIntent] = None
    signal_hash: str = ""
    signal_id: int = 0
    is_duplicate: bool = False
    skip_reason: Optional[str] = None
    processed_at: float = field(default_factory=time.time)


@dataclass
class BackfillChannelReport:
    """Summary of backfill progress for an individual channel."""
    channel_id: int
    highest_local_message_id: int
    messages_retrieved: int = 0
    signals_parsed: int = 0
    signals_skipped_dup: int = 0
    signals_executable: int = 0
    new_highest_message_id: int = 0
    is_complete: bool = False
    error: Optional[str] = None


@dataclass
class BackfillSummary:
    """Global result summary returned by MessageBackfill execution."""
    total_channels: int
    total_messages_retrieved: int
    total_signals_parsed: int
    total_skipped_dup: int
    total_executable: int
    channel_reports: Dict[int, BackfillChannelReport] = field(default_factory=dict)
    backfilled_items: List[BackfillMessageItem] = field(default_factory=list)
    elapsed_s: float = 0.0


# =============================================================================
# HASH & IDENTIFIER HELPERS
# =============================================================================

def compute_signal_hash(
    channel_id: int,
    message_id: int,
    raw_text: str,
    symbol: Optional[str] = None,
    action: Optional[str] = None,
    entry_price: Optional[float] = None,
) -> str:
    """
    Computes deterministic SHA-256 signal hash for deduplication cross-referencing.
    """
    clean_sym = (symbol or "").strip().upper()
    act = (action or "").strip().upper()
    ep = f"{float(entry_price):.5f}" if entry_price is not None else ""
    canonical = f"{channel_id}:{message_id}:{clean_sym}:{act}:{ep}:{raw_text.strip()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_signal_id(channel_id: int, message_id: int) -> int:
    """
    Computes a canonical 63-bit integer signal_id from channel_id and message_id.
    Ensures consistent cross-referencing between Telegram messages and broker tickets.
    """
    raw_key = f"{channel_id}:{message_id}"
    digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    # 15 hex characters = 60 bits, safely fits in signed 64-bit int
    return int(digest[:15], 16)


# =============================================================================
# MESSAGE BACKFILL UTILITY CLASS
# =============================================================================

class MessageBackfill:
    """
    Telegram Historical Message Backfill Engine (Sprint 16).
    Reconciles missing signals during downtime and deduplicates against live broker orders.
    """

    def __init__(
        self,
        tdlib_client: Optional[Any] = None,
        telegram_gateway: Optional[Any] = None,
        ingestion_controller: Optional[Any] = None,
        orchestrator: Optional[Any] = None,
        signal_parser: Optional[SignalParser] = None,
        raw_db_path: Optional[str] = None,
        page_limit: int = 50,
        max_messages_per_channel: int = 200,
        max_signal_age_s: float = 86400.0,  # 24 hours
    ) -> None:
        self.tdlib_client = tdlib_client or (getattr(telegram_gateway, "_client", None) if telegram_gateway else None)
        self.telegram_gateway = telegram_gateway
        self.ingestion_controller = ingestion_controller
        self.orchestrator = orchestrator
        self.signal_parser = signal_parser or SignalParser()
        self.raw_db_path = raw_db_path
        self.page_limit = page_limit
        self.max_messages_per_channel = max_messages_per_channel
        self.max_signal_age_s = max_signal_age_s

    # -------------------------------------------------------------------------
    # Task 1: Last Processed State
    # -------------------------------------------------------------------------

    def get_last_processed_state(
        self,
        subscribed_channels: Optional[List[int]] = None,
    ) -> Dict[int, int]:
        """
        Queries the local SQLite RawMessage database to find the highest message_id
        for every subscribed channel.
        """
        state: Dict[int, int] = {}
        target_chats: Set[int] = set(subscribed_channels or [])

        # Add monitored chats from telegram_gateway if available
        if self.telegram_gateway and hasattr(self.telegram_gateway, "get_monitored_chats"):
            try:
                target_chats.update(self.telegram_gateway.get_monitored_chats())
            except Exception:
                pass

        # 1. Query IngestionController if attached
        if self.ingestion_controller and hasattr(self.ingestion_controller, "conn"):
            try:
                cur = self.ingestion_controller.conn.cursor()
                # Check channel_timeline_meta
                cur.execute("SELECT channel_id, last_message_id FROM channel_timeline_meta;")
                for row in cur.fetchall():
                    cid = int(row["channel_id"])
                    lmid = int(row["last_message_id"])
                    state[cid] = max(state.get(cid, 0), lmid)

                # Check max(message_id) from raw_messages
                cur.execute("SELECT channel_id, MAX(message_id) AS max_id FROM raw_messages GROUP BY channel_id;")
                for row in cur.fetchall():
                    cid = int(row["channel_id"])
                    mid = int(row["max_id"] or 0)
                    state[cid] = max(state.get(cid, 0), mid)
            except Exception as e:
                logger.debug("Error querying IngestionController for last processed state: %s", e)

        # 2. Query direct SQLite database file if raw_db_path is provided
        elif self.raw_db_path:
            try:
                conn = sqlite3.connect(self.raw_db_path)
                conn.row_factory = sqlite3.Row
                with conn:
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT channel_id, last_message_id FROM channel_timeline_meta;")
                        for row in cur.fetchall():
                            cid = int(row["channel_id"])
                            lmid = int(row["last_message_id"])
                            state[cid] = max(state.get(cid, 0), lmid)
                    except sqlite3.OperationalError:
                        pass

                    try:
                        cur.execute("SELECT channel_id, MAX(message_id) AS max_id FROM raw_messages GROUP BY channel_id;")
                        for row in cur.fetchall():
                            cid = int(row["channel_id"])
                            mid = int(row["max_id"] or 0)
                            state[cid] = max(state.get(cid, 0), mid)
                    except sqlite3.OperationalError:
                        pass
                conn.close()
            except Exception as e:
                logger.debug("Error querying raw_db_path '%s': %s", self.raw_db_path, e)

        # 3. Query TelegramGateway vault chat checkpoints if available
        if self.telegram_gateway and getattr(self.telegram_gateway, "vault", None):
            vault = self.telegram_gateway.vault
            if hasattr(vault, "get_chat_checkpoint"):
                for cid in target_chats:
                    try:
                        cp = vault.get_chat_checkpoint(cid)
                        if cp is not None:
                            state[cid] = max(state.get(cid, 0), int(cp))
                    except Exception:
                        pass

        # Ensure every requested channel has an explicit entry (defaulting to 0)
        for cid in target_chats:
            if cid not in state:
                state[cid] = 0

        logger.info(
            "Identified last processed state across %d channels: %s",
            len(state),
            state,
        )
        return state

    # -------------------------------------------------------------------------
    # Task 2: TDLib History Request (getChatHistory)
    # -------------------------------------------------------------------------

    def fetch_chat_history_backward(
        self,
        chat_id: int,
        last_processed_id: int,
    ) -> List[Dict[str, Any]]:
        """
        Uses TDLib getChatHistory endpoint to iterate backward from the current live
        state until retrieved message_id matches or falls below local database state.
        """
        client = self.tdlib_client
        if client is None and self.telegram_gateway:
            client = getattr(self.telegram_gateway, "_client", None)

        if client is None:
            logger.warning("No TDLib client available to execute getChatHistory for chat %s", chat_id)
            return []

        collected_messages: List[Dict[str, Any]] = []
        seen_ids: Set[int] = set()
        from_msg_id = 0  # 0 indicates the current live state
        reached_local_state = False

        while len(collected_messages) < self.max_messages_per_channel and not reached_local_state:
            req = {
                "@type": "getChatHistory",
                "chat_id": chat_id,
                "from_message_id": from_msg_id,
                "offset": 0,
                "limit": self.page_limit,
                "only_local": False,
                "@extra": {"backfill_chat_id": chat_id, "from_msg_id": from_msg_id},
            }

            resp = None
            # getChatHistory is an asynchronous TDLib request sent via send() and received via receive()
            client.send(req)
            for _ in range(50):
                event = client.receive(timeout=0.1)
                if not event:
                    continue
                if event.get("@type") == "messages":
                    extra = event.get("@extra", {})
                    if not extra or extra.get("backfill_chat_id") == chat_id:
                        resp = event
                        break
                elif event.get("@type") == "error":
                    extra = event.get("@extra", {})
                    if not extra or extra.get("backfill_chat_id") == chat_id:
                        logger.error("TDLib getChatHistory error for chat %s: %s", chat_id, event)
                        return collected_messages

            if not resp or resp.get("@type") != "messages":
                break

            messages = resp.get("messages", [])
            if not messages:
                # No more older messages available in Telegram
                break

            # In TDLib getChatHistory, messages are returned in reverse-chronological order (newest -> oldest)
            new_in_batch = 0
            for msg in messages:
                msg_id = msg.get("id", 0)

                # Skip already seen message IDs to avoid duplicates from TDLib pagination overlap
                if msg_id in seen_ids:
                    continue

                # Stop iteration backward when we match or pass the local database state
                if last_processed_id > 0 and msg_id <= last_processed_id:
                    reached_local_state = True
                    break

                # Discard messages older than max_signal_age_s
                msg_date = msg.get("date", 0)
                if msg_date > 0 and (time.time() - msg_date) > self.max_signal_age_s:
                    reached_local_state = True
                    break

                seen_ids.add(msg_id)
                collected_messages.append(msg)
                new_in_batch += 1

            if reached_local_state:
                break

            # Advance pagination to the oldest message in this batch
            oldest_id_in_batch = messages[-1].get("id", 0)
            if oldest_id_in_batch == from_msg_id or oldest_id_in_batch == 0 or new_in_batch == 0:
                break
            from_msg_id = oldest_id_in_batch

        # Sort chronologically (oldest to newest) for sequential replay
        collected_messages.sort(key=lambda m: m.get("id", 0))
        logger.info(
            "Backfilled %d historical messages for chat %s (iterated backward to local state %s)",
            len(collected_messages), chat_id, last_processed_id,
        )
        return collected_messages

    # -------------------------------------------------------------------------
    # Task 3: Deduplication Check & Reconciliation Cross-Reference
    # -------------------------------------------------------------------------

    def is_duplicate_signal(
        self,
        signal_id: int,
        signal_hash: str,
        parsed_signal: Optional[ParsedSignal] = None,
        reconciled_orders: Optional[List[Any]] = None,
        discovered_open_signals: Optional[Dict[int, int]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Cross-references the Signal Hash and Signal ID against newly reconciled
        broker orders (from Step C of takeover) and SQLite active database records.
        If a trade for that signal already exists, returns (True, "SKIP_DUP").
        """
        # 1. Cross-reference against in-memory discovered signals from Step C reconciliation
        if discovered_open_signals and signal_id in discovered_open_signals:
            ticket = discovered_open_signals[signal_id]
            return True, f"SKIP_DUP: Signal {signal_id} matched open broker position #{ticket} (Step C reconciliation)"

        # 2. Check ExecutionOrchestrator discovered signals map
        if self.orchestrator:
            if hasattr(self.orchestrator, "_discovered_open_signals"):
                if signal_id in self.orchestrator._discovered_open_signals:
                    ticket = self.orchestrator._discovered_open_signals[signal_id]
                    return True, f"SKIP_DUP: Signal {signal_id} matched open broker position #{ticket} (Orchestrator)"

            # Check orchestrator deduplication gate
            if hasattr(self.orchestrator, "is_signal_already_open"):
                if self.orchestrator.is_signal_already_open(signal_id):
                    return True, f"SKIP_DUP: Signal {signal_id} is already open on broker terminal"

        # 3. Check raw reconciled_orders dump passed from Step C
        if reconciled_orders:
            for item in reconciled_orders:
                # If item is PositionItem or dict
                ticket = getattr(item, "ticket", None) or (item.get("ticket") if isinstance(item, dict) else None)
                comment = getattr(item, "comment", "") or (item.get("comment", "") if isinstance(item, dict) else "")
                symbol = getattr(item, "symbol", "") or (item.get("symbol", "") if isinstance(item, dict) else "")

                # Extract signal ID from comment if present
                from engine.orphan_manager import OrphanManager
                if hasattr(OrphanManager, "extract_signal_id"):
                    extracted = OrphanManager.extract_signal_id(item) if not isinstance(item, dict) else None
                    if extracted == signal_id:
                        return True, f"SKIP_DUP: Signal {signal_id} matched reconciled position #{ticket}"

                # Match by string representation
                if str(signal_id) in str(comment) or signal_hash[:12] in str(comment):
                    return True, f"SKIP_DUP: Signal {signal_id} / hash {signal_hash[:8]} matched position comment '{comment}'"

                # Match by identical symbol and entry parameters
                if parsed_signal and symbol and parsed_signal.symbol.upper() == symbol.upper():
                    pos_price = getattr(item, "open_price", 0.0) or (item.get("open_price", 0.0) if isinstance(item, dict) else 0.0)
                    if parsed_signal.entry_price and abs(float(pos_price) - float(parsed_signal.entry_price)) < 0.0001:
                        return True, f"SKIP_DUP: Identical trade on {symbol} at price {pos_price} already active on broker"

        return False, None

    # -------------------------------------------------------------------------
    # Full Message Backfill Orchestration Pipeline
    # -------------------------------------------------------------------------

    def process_backfilled_message(
        self,
        raw_msg: Dict[str, Any],
        chat_id: int,
        reconciled_orders: Optional[List[Any]] = None,
        discovered_open_signals: Optional[Dict[int, int]] = None,
    ) -> BackfillMessageItem:
        """
        Executes normal normalization, parsing, and deduplication pipeline on a message.
        """
        msg_id = raw_msg.get("id", 0)
        msg_date = raw_msg.get("date", 0)

        # 1. Extract text content
        raw_text = ""
        content = raw_msg.get("content", {})
        if isinstance(content, dict):
            text_obj = content.get("text", {})
            if isinstance(text_obj, dict):
                raw_text = text_obj.get("text", "")
            elif isinstance(text_obj, str):
                raw_text = text_obj
            elif "caption" in content:
                cap = content.get("caption", {})
                raw_text = cap.get("text", "") if isinstance(cap, dict) else str(cap)
        elif isinstance(content, str):
            raw_text = content
        elif "text" in raw_msg:
            raw_text = str(raw_msg["text"])

        # 2. Text Normalization
        cleaned = clean_text(raw_text)

        # 3. Signal Parsing
        parsed: Optional[ParsedSignal] = None
        sig_id = compute_signal_id(chat_id, msg_id)
        sig_hash = compute_signal_hash(chat_id, msg_id, raw_text)

        if cleaned and self.signal_parser:
            try:
                parsed = self.signal_parser.parse(cleaned)
                if parsed and parsed.symbol:
                    sig_hash = compute_signal_hash(
                        chat_id=chat_id,
                        message_id=msg_id,
                        raw_text=raw_text,
                        symbol=parsed.symbol,
                        action=parsed.action,
                        entry_price=parsed.entry_price,
                    )
            except Exception as e:
                logger.debug("Parsing exception on backfill message #%d: %s", msg_id, e)

        # 4. Deduplication Check against Step C Reconciled Orders
        is_dup, skip_reason = False, None
        if parsed and parsed.message_type == "signal":
            is_dup, skip_reason = self.is_duplicate_signal(
                signal_id=sig_id,
                signal_hash=sig_hash,
                parsed_signal=parsed,
                reconciled_orders=reconciled_orders,
                discovered_open_signals=discovered_open_signals,
            )
            if is_dup:
                parsed.skip_reason = SKIP_DUP
                logger.info(
                    "DEDUPLICATION HIT: Backfilled message #%d (chat %s, signal %d) marked SKIP_DUP: %s",
                    msg_id, chat_id, sig_id, skip_reason,
                )

        # Construct OrderIntent if executable and not duplicated
        order_intent = None
        if parsed and parsed.message_type == "signal" and not is_dup:
            try:
                order_intent = parsed.to_order_intent(signal_id=sig_id)
            except Exception:
                order_intent = None

        return BackfillMessageItem(
            chat_id=chat_id,
            message_id=msg_id,
            date=msg_date,
            raw_text=raw_text,
            parsed_signal=parsed,
            order_intent=order_intent,
            signal_hash=sig_hash,
            signal_id=sig_id,
            is_duplicate=is_dup,
            skip_reason=skip_reason if is_dup else None,
        )

    def execute_backfill(
        self,
        subscribed_channels: Optional[List[int]] = None,
        reconciled_orders: Optional[List[Any]] = None,
    ) -> BackfillSummary:
        """
        Executes complete synchronous backfill reconciliation across all subscribed channels:
        Step 1: Identifies highest local message_id per channel from SQLite.
        Step 2: Queries TDLib getChatHistory backward to local state.
        Step 3: Normalizes, parses, and cross-references signal hash against reconciled broker orders.
        """
        started_at = time.time()

        # Step 1: Query Last Processed State
        local_state = self.get_last_processed_state(subscribed_channels)
        channel_reports: Dict[int, BackfillChannelReport] = {}
        all_backfilled_items: List[BackfillMessageItem] = []

        total_retrieved = 0
        total_parsed = 0
        total_skipped_dup = 0
        total_executable = 0

        # Discovered open signals from orchestrator
        discovered_open = getattr(self.orchestrator, "_discovered_open_signals", {}) if self.orchestrator else {}

        for chat_id, last_processed_id in local_state.items():
            report = BackfillChannelReport(
                channel_id=chat_id,
                highest_local_message_id=last_processed_id,
                new_highest_message_id=last_processed_id,
            )

            try:
                # Step 2: Fetch Chat History Backward via TDLib
                raw_messages = self.fetch_chat_history_backward(
                    chat_id=chat_id,
                    last_processed_id=last_processed_id,
                )
                report.messages_retrieved = len(raw_messages)
                total_retrieved += len(raw_messages)

                # Step 3: Process messages and execute Deduplication Check
                for raw_msg in raw_messages:
                    msg_id = raw_msg.get("id", 0)
                    item = self.process_backfilled_message(
                        raw_msg=raw_msg,
                        chat_id=chat_id,
                        reconciled_orders=reconciled_orders,
                        discovered_open_signals=discovered_open,
                    )
                    all_backfilled_items.append(item)

                    if item.parsed_signal and item.parsed_signal.message_type == "signal":
                        report.signals_parsed += 1
                        total_parsed += 1

                        if item.is_duplicate:
                            report.signals_skipped_dup += 1
                            total_skipped_dup += 1
                        elif item.order_intent:
                            report.signals_executable += 1
                            total_executable += 1

                    # Persist to IngestionController raw_messages ledger if attached
                    if self.ingestion_controller and hasattr(self.ingestion_controller, "handle_new_message"):
                        try:
                            self.ingestion_controller.handle_new_message(raw_msg)
                        except Exception:
                            pass

                    # Update local state pointer
                    report.new_highest_message_id = max(report.new_highest_message_id, msg_id)

                # Update vault checkpoint if attached
                if self.telegram_gateway and getattr(self.telegram_gateway, "vault", None):
                    try:
                        self.telegram_gateway.vault.update_chat_checkpoint(
                            chat_id, report.new_highest_message_id
                        )
                    except Exception:
                        pass

                report.is_complete = True

            except Exception as e:
                report.error = str(e)
                logger.error("Error executing backfill for chat %s: %s", chat_id, e, exc_info=True)

            channel_reports[chat_id] = report

        elapsed = time.time() - started_at
        summary = BackfillSummary(
            total_channels=len(local_state),
            total_messages_retrieved=total_retrieved,
            total_signals_parsed=total_parsed,
            total_skipped_dup=total_skipped_dup,
            total_executable=total_executable,
            channel_reports=channel_reports,
            backfilled_items=all_backfilled_items,
            elapsed_s=elapsed,
        )

        logger.info(
            "MessageBackfill complete across %d channels: %d messages, %d parsed, "
            "%d skipped DUP, %d executable (%.2fs)",
            summary.total_channels, summary.total_messages_retrieved,
            summary.total_signals_parsed, summary.total_skipped_dup,
            summary.total_executable, summary.elapsed_s,
        )
        return summary

    async def execute_backfill_async(
        self,
        subscribed_channels: Optional[List[int]] = None,
        reconciled_orders: Optional[List[Any]] = None,
    ) -> BackfillSummary:
        """Asynchronous execution wrapper running backfill in worker thread."""
        return await asyncio.to_thread(
            self.execute_backfill,
            subscribed_channels,
            reconciled_orders,
        )
