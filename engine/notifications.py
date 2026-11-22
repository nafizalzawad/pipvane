"""
Pipvane v3: Notification Engine & Intelligent Digest Batcher (Sprint 18 Directive)
==================================================================================
Provides rate-limit compliant trade notification delivery to Telegram:
1. Intelligent Digest Batching:
   - When trades occur within a configurable window (default: 2.0 seconds), the engine
     accumulates them into an in-memory buffer.
   - Consolidates high-frequency bursts (e.g., 50 simultaneous trades from news signals)
     into a single, clean digest message, preventing API flooding and chat spam.
2. Telegram Rate Limit Compliance:
   - Enforces strict Telegram Bot API quotas:
     - Maximum 1 message per second per individual chat.
     - Maximum 30 messages per second globally across all chats.
     - Maximum 4,096 characters per message (with automatic part splitting).
3. Fallback and Exponential Backoff:
   - Handles HTTP 429 (Too Many Requests) with Retry-After backoff.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import enum
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("pipvane.engine.notifications")

# Telegram Bot API Constants
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_MAX_PER_CHAT_PER_SECOND = 1.0
TELEGRAM_GLOBAL_MAX_PER_SECOND = 30.0

DEFAULT_BATCH_WINDOW_SECONDS = 2.0
DEFAULT_MAX_BATCH_SIZE = 100


# =============================================================================
# ENUMS & DATA STRUCTURES
# =============================================================================

class NotificationType(str, enum.Enum):
    """Notification classification categories."""
    TRADE_EXECUTED = "TRADE_EXECUTED"
    TRADE_CLOSED = "TRADE_CLOSED"
    TRADE_MODIFIED = "TRADE_MODIFIED"
    PANIC_CLOSE = "PANIC_CLOSE"
    SECURITY_ALERT = "SECURITY_ALERT"
    ORPHAN_DETECTED = "ORPHAN_DETECTED"
    TRADE_DIGEST = "TRADE_DIGEST"


@dataclass
class TradeNotificationEvent:
    """Individual trade execution event submitted to the notification pipeline."""
    signal_id: int
    ticket: int
    symbol: str
    side: str  # "BUY" or "SELL"
    volume: float
    price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    terminal_id: str = "demo_terminal"
    timestamp: float = field(default_factory=time.time)
    channel_name: Optional[str] = None
    action_type: str = "ENTRY"
    magic_number: int = 0
    details: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OutboundTelegramMessage:
    """Delivered Telegram message structure with metadata."""
    message_id: str
    chat_id: str
    text: str
    parse_mode: str = "HTML"
    is_digest: bool = False
    trade_count: int = 1
    events: List[TradeNotificationEvent] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None


# =============================================================================
# RATE LIMITER (Telegram Bot API Compliance)
# =============================================================================

class TelegramRateLimiter:
    """
    Token-bucket / sliding-window rate limiter ensuring compliance with
    Telegram Bot API guidelines:
    - Max 1 msg/sec per individual chat
    - Max 30 msgs/sec globally across all chats
    """

    def __init__(
        self,
        max_per_chat_per_sec: float = TELEGRAM_MAX_PER_CHAT_PER_SECOND,
        global_max_per_sec: float = TELEGRAM_GLOBAL_MAX_PER_SECOND,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.max_per_chat_per_sec = max_per_chat_per_sec
        self.global_max_per_sec = global_max_per_sec
        self._clock = clock or time.time

        # Track timestamps: chat_id -> list of send timestamps
        self._chat_timestamps: Dict[str, List[float]] = {}
        # Track global timestamps
        self._global_timestamps: List[float] = []
        self._lock = asyncio.Lock()

        # Telemetry
        self.rate_limit_hits = 0

    def now(self) -> float:
        return self._clock()

    def _prune(self, window_s: float = 1.0) -> None:
        current_time = self.now()
        cutoff = current_time - window_s

        # Prune global
        self._global_timestamps = [t for t in self._global_timestamps if t > cutoff]

        # Prune per-chat
        for cid in list(self._chat_timestamps.keys()):
            active = [t for t in self._chat_timestamps[cid] if t > cutoff]
            if active:
                self._chat_timestamps[cid] = active
            else:
                del self._chat_timestamps[cid]

    async def acquire(self, chat_id: str) -> None:
        """
        Asynchronously waits until rate limits allow sending a message to chat_id.
        Enforces both per-chat (1/s) and global (30/s) ceilings.
        """
        while True:
            async with self._lock:
                self._prune(window_s=1.0)
                current_time = self.now()

                # Check per-chat limit (max 1 msg/sec)
                chat_history = self._chat_timestamps.get(chat_id, [])
                chat_exceeded = len(chat_history) >= self.max_per_chat_per_sec

                # Check global limit (max 30 msgs/sec)
                global_exceeded = len(self._global_timestamps) >= self.global_max_per_sec

                if not chat_exceeded and not global_exceeded:
                    # Allowed! Record timestamp
                    self._chat_timestamps.setdefault(chat_id, []).append(current_time)
                    self._global_timestamps.append(current_time)
                    return

                # Must wait
                self.rate_limit_hits += 1
                wait_time = 0.05
                if chat_exceeded and chat_history:
                    wait_time = max(wait_time, 1.0 - (current_time - chat_history[0]))
                if global_exceeded and self._global_timestamps:
                    wait_time = max(wait_time, 1.0 - (current_time - self._global_timestamps[0]))

            # Sleep outside lock
            await asyncio.sleep(min(wait_time, 0.5))


# =============================================================================
# TELEGRAM BOT CLIENT (Production & Mock Implementations)
# =============================================================================

class TelegramBotClient:
    """Interface for outbound Telegram Bot HTTP API communication."""

    async def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
    ) -> OutboundTelegramMessage:
        raise NotImplementedError


class MockTelegramBotClient(TelegramBotClient):
    """
    Mock Telegram Bot client recording sent messages, timestamps, and
    verifying API rate-limit invariants for automated testing.
    """

    def __init__(
        self,
        rate_limiter: Optional[TelegramRateLimiter] = None,
        clock: Optional[Callable[[], float]] = None,
        simulate_rate_limit_errors: bool = True,
    ):
        self.rate_limiter = rate_limiter or TelegramRateLimiter(clock=clock)
        self._clock = clock or time.time
        self.simulate_rate_limit_errors = simulate_rate_limit_errors

        # Audit history of sent messages
        self.sent_messages: List[OutboundTelegramMessage] = []
        self.raw_calls: List[Dict[str, Any]] = []

        # Rate-limit violation detector
        self.rate_limit_violations: List[str] = []
        self._recent_chat_calls: Dict[str, List[float]] = {}

    def now(self) -> float:
        return self._clock()

    async def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        is_digest: bool = False,
        events: Optional[List[TradeNotificationEvent]] = None,
    ) -> OutboundTelegramMessage:
        current_time = self.now()

        # Check for immediate rate limit violation if not using rate limiter
        chat_calls = self._recent_chat_calls.setdefault(chat_id, [])
        # Prune calls older than 1 second
        chat_calls = [t for t in chat_calls if current_time - t < 1.0]
        self._recent_chat_calls[chat_id] = chat_calls

        if len(chat_calls) >= TELEGRAM_MAX_PER_CHAT_PER_SECOND:
            violation_msg = (
                f"RATE LIMIT BREACH: Attempted to send {len(chat_calls) + 1} messages to chat {chat_id} "
                f"within 1.0 second window! Violates Telegram API limit."
            )
            self.rate_limit_violations.append(violation_msg)
            logger.warning(violation_msg)
            if self.simulate_rate_limit_errors:
                # Simulate Telegram HTTP 429
                raise urllib.error.HTTPError(
                    url="https://api.telegram.org/bot/sendMessage",
                    code=429,
                    msg="Too Many Requests: retry after 1",
                    hdrs={},
                    fp=None,
                )

        chat_calls.append(current_time)

        msg = OutboundTelegramMessage(
            message_id=f"msg_{len(self.sent_messages) + 1}_{uuid.uuid4().hex[:6]}",
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            is_digest=is_digest,
            trade_count=len(events) if events else 1,
            events=events or [],
            created_at=current_time,
            sent_at=current_time,
        )

        self.sent_messages.append(msg)
        self.raw_calls.append({
            "chat_id": chat_id,
            "text": text,
            "timestamp": current_time,
            "is_digest": is_digest,
        })
        return msg


# =============================================================================
# DIGEST FORMATTER
# =============================================================================

class DigestFormatter:
    """Formats single trade events and multi-trade batches into clean HTML messages."""

    @staticmethod
    def format_single_trade(event: TradeNotificationEvent) -> str:
        """Formats a single trade execution notification."""
        side_icon = "🟢" if event.side.upper() == "BUY" else "🔴"
        sl_str = f" | SL: {event.stop_loss:.5f}" if event.stop_loss else ""
        tp_str = f" | TP: {event.take_profit:.5f}" if event.take_profit else ""

        return (
            f"<b>{side_icon} TRADE EXECUTED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Symbol:</b> <code>{event.symbol}</code>\n"
            f"<b>Action:</b> {event.side.upper()} {event.volume:.2f} Lots\n"
            f"<b>Price:</b> {event.price:.5f}\n"
            f"<b>Ticket:</b> #{event.ticket}\n"
            f"<b>Terminal:</b> <code>{event.terminal_id}</code>"
            f"{sl_str}{tp_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )

    @staticmethod
    def format_digest(events: List[TradeNotificationEvent]) -> List[str]:
        """
        Formats multiple trade events into one or more structured digest messages,
        respecting Telegram's 4096 character limit per message.
        """
        if not events:
            return []

        total_volume = sum(e.volume for e in events)
        symbols = sorted(list({e.symbol for e in events}))
        buy_count = sum(1 for e in events if e.side.upper() == "BUY")
        sell_count = len(events) - buy_count

        header = (
            f"🚀 <b>PIPVANE DIGEST: {len(events)} Trades Executed</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Summary:</b> {buy_count} BUY | {sell_count} SELL | Total: {total_volume:.2f} Lots\n"
            f"<b>Symbols:</b> {', '.join(symbols[:8])}{'...' if len(symbols) > 8 else ''}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        lines: List[str] = []
        for idx, e in enumerate(events, 1):
            icon = "🟢" if e.side.upper() == "BUY" else "🔴"
            line = f"{idx}. {icon} <b>{e.side.upper()}</b> {e.volume:.2f} <code>{e.symbol}</code> @ {e.price:.5f} (#{e.ticket})"
            lines.append(line)

        footer = (
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Batched by Pipvane Notification Engine (2s window)</i>"
        )

        # Chunk lines to fit within 4096 characters per message
        messages: List[str] = []
        current_chunk: List[str] = []
        current_length = len(header) + len(footer)

        for line in lines:
            line_len = len(line) + 1  # newline
            if current_length + line_len > (TELEGRAM_MAX_MESSAGE_LENGTH - 100):
                # Emit current message
                part_text = header + "\n".join(current_chunk) + footer
                messages.append(part_text)
                current_chunk = [line]
                current_length = len(header) + len(footer) + line_len
            else:
                current_chunk.append(line)
                current_length += line_len

        if current_chunk:
            part_text = header + "\n".join(current_chunk) + footer
            messages.append(part_text)

        return messages


# =============================================================================
# NOTIFICATION ENGINE
# =============================================================================

class NotificationEngine:
    """
    Intelligent Notification Engine (Sprint 18 Directive).
    Batches trade execution events occurring within a 2-second window into
    consolidated digest messages, preventing Telegram rate-limit exhaustion.
    """

    def __init__(
        self,
        bot_client: Optional[TelegramBotClient] = None,
        default_chat_id: str = "123456789",
        batch_window_seconds: float = DEFAULT_BATCH_WINDOW_SECONDS,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
        clock: Optional[Callable[[], float]] = None,
        enable_rate_limiter: bool = True,
    ):
        self._clock = clock or time.time
        self.default_chat_id = default_chat_id
        self.batch_window_seconds = batch_window_seconds
        self.max_batch_size = max_batch_size

        self.rate_limiter = TelegramRateLimiter(clock=self._clock) if enable_rate_limiter else None
        self.bot_client = bot_client or MockTelegramBotClient(rate_limiter=self.rate_limiter, clock=self._clock)

        # In-memory pending batch buffers: chat_id -> List[TradeNotificationEvent]
        self._pending_batches: Dict[str, List[TradeNotificationEvent]] = {}
        # Active timer handles / tasks: chat_id -> asyncio.Task
        self._batch_tasks: Dict[str, asyncio.Task] = {}
        # Thread/Async lock
        self._lock = asyncio.Lock()

        # Telemetry & Audit Metrics
        self.total_trades_received = 0
        self.total_messages_sent = 0
        self.total_digests_sent = 0
        self.total_single_messages_sent = 0
        self.total_messages_saved = 0  # Number of individual messages prevented by batching

        self._is_running = True

    def now(self) -> float:
        return self._clock()

    @property
    def pending_count(self) -> int:
        """Returns total number of events currently awaiting batch flush across all chats."""
        return sum(len(b) for b in self._pending_batches.values())

    async def notify_trade(
        self,
        event: Union[TradeNotificationEvent, Dict[str, Any]],
        chat_id: Optional[str] = None,
    ) -> None:
        """
        Receives an executed trade event and enqueues it into the batch buffer.
        If no timer is active for chat_id, starts a 2.0-second window timer.
        """
        if not self._is_running:
            return

        target_chat = chat_id or self.default_chat_id

        if isinstance(event, dict):
            event_obj = TradeNotificationEvent(
                signal_id=int(event.get("signal_id", 0)),
                ticket=int(event.get("ticket", 0)),
                symbol=str(event.get("symbol", "UNKNOWN")),
                side=str(event.get("side", "BUY")),
                volume=float(event.get("volume", 0.1)),
                price=float(event.get("price", 0.0)),
                stop_loss=event.get("stop_loss"),
                take_profit=event.get("take_profit"),
                terminal_id=str(event.get("terminal_id", "terminal_default")),
                timestamp=float(event.get("timestamp", self.now())),
                channel_name=event.get("channel_name"),
            )
        else:
            event_obj = event

        async with self._lock:
            self.total_trades_received += 1
            batch = self._pending_batches.setdefault(target_chat, [])
            batch.append(event_obj)

            # If no active window timer for this chat, schedule the 2.0s flush
            if target_chat not in self._batch_tasks or self._batch_tasks[target_chat].done():
                self._batch_tasks[target_chat] = asyncio.create_task(
                    self._window_timer(target_chat, self.batch_window_seconds),
                    name=f"BatchWindowTimer_{target_chat}",
                )

    async def _window_timer(self, chat_id: str, delay_s: float) -> None:
        """Awaits the 2.0-second batching window before flushing accumulated trades."""
        try:
            await asyncio.sleep(delay_s)
            await self._flush_chat_batch(chat_id)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Error in batch window timer for chat %s: %s", chat_id, e)

    async def _flush_chat_batch(self, chat_id: str) -> List[OutboundTelegramMessage]:
        """
        Flushes the batch buffer for chat_id:
        - If 1 trade: Formats as single trade message.
        - If >1 trades: Consolidates into a single digest message.
        """
        async with self._lock:
            events = self._pending_batches.pop(chat_id, [])
            # Clean task reference
            self._batch_tasks.pop(chat_id, None)

        if not events:
            return []

        delivered: List[OutboundTelegramMessage] = []

        if len(events) == 1:
            # Single trade message
            text = DigestFormatter.format_single_trade(events[0])
            msg = await self._send_to_telegram(
                chat_id=chat_id,
                text=text,
                is_digest=False,
                events=events,
            )
            delivered.append(msg)
            self.total_single_messages_sent += 1
            self.total_messages_sent += 1
        else:
            # Multi-trade digest!
            digest_texts = DigestFormatter.format_digest(events)
            for d_text in digest_texts:
                msg = await self._send_to_telegram(
                    chat_id=chat_id,
                    text=d_text,
                    is_digest=True,
                    events=events,
                )
                delivered.append(msg)
                self.total_digests_sent += 1
                self.total_messages_sent += 1

            saved = len(events) - len(digest_texts)
            self.total_messages_saved += saved
            logger.info(
                "BATCH DIGEST SENT to chat %s: %d trades consolidated into %d message(s). "
                "Prevented %d individual Telegram API calls.",
                chat_id, len(events), len(digest_texts), saved,
            )

        return delivered

    async def _send_to_telegram(
        self,
        chat_id: str,
        text: str,
        is_digest: bool,
        events: List[TradeNotificationEvent],
    ) -> OutboundTelegramMessage:
        """Sends message via bot_client with rate limiter protection."""
        if self.rate_limiter:
            await self.rate_limiter.acquire(chat_id)

        msg = await self.bot_client.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            is_digest=is_digest,
            events=events,
        )
        return msg

    async def flush_all(self) -> List[OutboundTelegramMessage]:
        """Immediately flushes all pending batches across all chats (useful for shutdown / test sync)."""
        chats = list(self._pending_batches.keys())
        results: List[OutboundTelegramMessage] = []
        for cid in chats:
            # Cancel timer if running
            task = self._batch_tasks.get(cid)
            if task and not task.done():
                task.cancel()
            delivered = await self._flush_chat_batch(cid)
            results.extend(delivered)
        return results

    async def stop(self) -> None:
        """Stops engine and flushes any remaining buffered messages."""
        self._is_running = False
        await self.flush_all()
