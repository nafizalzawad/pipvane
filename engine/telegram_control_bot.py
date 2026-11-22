"""
Pipvane v3: Personal Telegram Control Bot (Sprint 18 Directive)
==============================================================
Provides a secure, personal Telegram Bot interface for remote engine control:
1. Bot Initialization:
   - Uses standard HTTP Telegram Bot API wrapper (separate from TDLib, using a
     Bot Token issued by @BotFather).
   - Operates in long-polling mode (getUpdates with timeout parameter) to minimize
     bandwidth and CPU overhead on the VPS.
2. Security Lock:
   - Verifies the chat_id of every incoming message against the registered owner's ID.
   - Silently drops messages from unauthorized users with zero response or acknowledgment.
3. Command Handling:
   - /status: Returns real-time equity, account financials, and active failover lease state.
   - /pause: Triggers the global kill switch on ExecutionOrchestrator.
   - /resume: Resumes automated order execution.
   - /help, /start: Command discovery and usage guide.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("pipvane.engine.control_bot")


# =============================================================================
# DATA STRUCTURES & CONFIGURATION
# =============================================================================

@dataclass
class TelegramControlBotConfig:
    """Configuration for the personal Telegram Control Bot."""
    bot_token: str
    owner_chat_id: Union[str, int]
    allowed_chat_ids: List[Union[str, int]] = field(default_factory=list)
    long_poll_timeout_s: int = 30  # Standard long-polling timeout for VPS bandwidth conservation
    polling_interval_s: float = 0.2
    api_base_url: str = "https://api.telegram.org"
    request_timeout_s: float = 35.0  # Must be greater than long_poll_timeout_s
    max_retries: int = 3
    retry_delay_s: float = 2.0


@dataclass
class TelegramUpdate:
    """Normalized inbound Telegram message update."""
    update_id: int
    message_id: int
    chat_id: Union[str, int]
    user_id: Union[str, int]
    text: str
    username: Optional[str] = None
    first_name: Optional[str] = None
    date: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional["TelegramUpdate"]:
        """Extracts and parses update from raw Telegram JSON payload."""
        update_id = data.get("update_id")
        if update_id is None:
            return None

        message = data.get("message") or data.get("channel_post")
        if not message or not isinstance(message, dict):
            return None

        msg_id = message.get("message_id", 0)
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        from_user = message.get("from", {})
        user_id = from_user.get("id", chat_id)
        text = str(message.get("text", "")).strip()

        if chat_id is None:
            return None

        return cls(
            update_id=int(update_id),
            message_id=int(msg_id),
            chat_id=chat_id,
            user_id=user_id,
            text=text,
            username=from_user.get("username"),
            first_name=from_user.get("first_name"),
            date=int(message.get("date", 0)),
            raw=data,
        )


# =============================================================================
# TELEGRAM BOT HTTP CLIENT INTERFACE & IMPLEMENTATIONS
# =============================================================================

class TelegramBotApiClient:
    """Abstract interface for HTTP Telegram Bot API requests."""

    async def get_updates(
        self,
        offset: Optional[int] = None,
        limit: int = 100,
        timeout: int = 30,
    ) -> List[Dict[str, Any]]:
        """Long-polls for incoming updates from Telegram servers."""
        raise NotImplementedError

    async def send_message(
        self,
        chat_id: Union[str, int],
        text: str,
        parse_mode: str = "HTML",
    ) -> Dict[str, Any]:
        """Dispatches an outbound message to a target chat."""
        raise NotImplementedError

    async def get_me(self) -> Dict[str, Any]:
        """Validates the bot token by querying bot identity."""
        raise NotImplementedError


class HttpTelegramBotApiClient(TelegramBotApiClient):
    """
    Standard HTTP Telegram Bot API wrapper using long-polling.
    Uses standard library urllib.request executed asynchronously off the main thread.
    """

    def __init__(self, config: TelegramControlBotConfig):
        self.config = config
        self.base_url = f"{config.api_base_url.rstrip('/')}/bot{config.bot_token}"

    def _execute_http(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Synchronous HTTP POST/GET executor."""
        url = f"{self.base_url}/{endpoint}"
        post_data = None
        headers = {"User-Agent": "Pipvane-ControlBot/3.0"}

        if params:
            post_data = json.dumps(params).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=post_data, headers=headers)
        timeout = timeout_s or self.config.request_timeout_s

        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = response.read().decode("utf-8")
                res_json = json.loads(body)
                if not res_json.get("ok"):
                    desc = res_json.get("description", "Unknown Telegram API error")
                    raise urllib.error.HTTPError(url, 400, desc, {}, None)
                return res_json
        except urllib.error.HTTPError as he:
            logger.warning("Telegram Bot API HTTP error (%s): %s", endpoint, he)
            raise
        except Exception as e:
            logger.warning("Telegram Bot API network failure (%s): %s", endpoint, e)
            raise

    async def get_updates(
        self,
        offset: Optional[int] = None,
        limit: int = 100,
        timeout: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Long-polling HTTP request for updates.
        The Telegram server holds the socket open for up to `timeout` seconds
        before returning an empty array if no updates are pending, drastically
        conserving VPS bandwidth.
        """
        payload: Dict[str, Any] = {
            "limit": limit,
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = offset

        loop = asyncio.get_running_loop()
        # Request timeout must allow for long-poll hold duration
        req_timeout = timeout + 5.0

        try:
            res = await loop.run_in_executor(
                None,
                self._execute_http,
                "getUpdates",
                payload,
                req_timeout,
            )
            return res.get("result", [])
        except urllib.error.HTTPError as he:
            if he.code == 409:
                logger.error("Conflict error on getUpdates: Another bot instance is running!")
            return []
        except Exception:
            return []

    async def send_message(
        self,
        chat_id: Union[str, int],
        text: str,
        parse_mode: str = "HTML",
    ) -> Dict[str, Any]:
        """Sends an HTML formatted message via Telegram Bot API."""
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self._execute_http,
            "sendMessage",
            payload,
            10.0,
        )

    async def get_me(self) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, self._execute_http, "getMe", None, 10.0)
        return res.get("result", {})


class MockTelegramBotApiClient(TelegramBotApiClient):
    """
    Mock Telegram Bot API Client for deterministic offline testing (Strict Mode).
    Simulates inbound update queues and records sent outbound messages.
    """

    def __init__(self):
        self.inbound_updates: List[Dict[str, Any]] = []
        self.sent_messages: List[Dict[str, Any]] = []
        self.get_updates_called_count: int = 0
        self.long_poll_timeout_observed: Optional[int] = None
        self.simulate_network_delay_s: float = 0.0

    def enqueue_update(
        self,
        update_id: int,
        chat_id: Union[str, int],
        text: str,
        user_id: Optional[Union[str, int]] = None,
        username: Optional[str] = None,
    ) -> None:
        """Enqueues a mock incoming user message update."""
        uid = user_id if user_id is not None else chat_id
        update = {
            "update_id": update_id,
            "message": {
                "message_id": 1000 + update_id,
                "chat": {"id": chat_id, "type": "private"},
                "from": {"id": uid, "username": username or "user", "first_name": "TestUser"},
                "text": text,
                "date": int(time.time()),
            },
        }
        self.inbound_updates.append(update)

    async def get_updates(
        self,
        offset: Optional[int] = None,
        limit: int = 100,
        timeout: int = 30,
    ) -> List[Dict[str, Any]]:
        self.get_updates_called_count += 1
        self.long_poll_timeout_observed = timeout

        if self.simulate_network_delay_s > 0:
            await asyncio.sleep(self.simulate_network_delay_s)

        if not self.inbound_updates:
            return []

        # Return updates respecting offset
        if offset is not None:
            filtered = [u for u in self.inbound_updates if u.get("update_id", 0) >= offset]
        else:
            filtered = list(self.inbound_updates)

        to_return = filtered[:limit]
        return to_return

    async def send_message(
        self,
        chat_id: Union[str, int],
        text: str,
        parse_mode: str = "HTML",
    ) -> Dict[str, Any]:
        msg = {
            "message_id": len(self.sent_messages) + 1,
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "timestamp": time.time(),
        }
        self.sent_messages.append(msg)
        return {"ok": True, "result": msg}

    async def get_me(self) -> Dict[str, Any]:
        return {
            "id": 999999999,
            "is_bot": True,
            "first_name": "Pipvane Control Bot",
            "username": "PipvaneControlBot",
        }


# =============================================================================
# TELEGRAM CONTROL BOT
# =============================================================================

class TelegramControlBot:
    """
    Pipvane v3 Personal Telegram Control Bot.
    - Security Lock: Enforces owner verification and silently drops unauthorized commands.
    - Long Polling: Uses HTTP getUpdates with long polling timeout to save bandwidth.
    - Command Routing: Handles /status, /pause, /resume, /help.
    - Engine Bridge: Interacts with ExecutionOrchestrator and FailoverManager.
    """

    def __init__(
        self,
        config: TelegramControlBotConfig,
        orchestrator: Optional[Any] = None,
        failover_manager: Optional[Any] = None,
        api_client: Optional[TelegramBotApiClient] = None,
        status_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        self.config = config
        self.orchestrator = orchestrator
        self.failover_manager = failover_manager
        self.api_client = api_client or HttpTelegramBotApiClient(config)
        self.status_provider = status_provider

        # Lifecycle & state
        self._is_running: bool = False
        self._poll_task: Optional[asyncio.Task] = None
        self._last_update_id: int = 0

        # Security & Audit telemetry
        self.unauthorized_messages_dropped: int = 0
        self.authorized_commands_processed: int = 0
        self.last_command_time: Optional[float] = None
        self.command_history: List[Dict[str, Any]] = []

    # -------------------------------------------------------------------------
    # Security Lock: Owner Verification
    # -------------------------------------------------------------------------

    def is_authorized(self, chat_id: Union[str, int], user_id: Optional[Union[str, int]] = None) -> bool:
        """
        Security Lock: Checks if the incoming chat_id (or user_id) matches the
        registered owner. All other users MUST be silently dropped.
        """
        owner_str = str(self.config.owner_chat_id).strip()
        chat_str = str(chat_id).strip()
        user_str = str(user_id).strip() if user_id is not None else ""

        # Check owner_chat_id
        if chat_str == owner_str or user_str == owner_str:
            return True

        # Check allowed_chat_ids
        for allowed in self.config.allowed_chat_ids:
            allowed_str = str(allowed).strip()
            if chat_str == allowed_str or user_str == allowed_str:
                return True

        return False

    # -------------------------------------------------------------------------
    # Inbound Update Dispatcher & Processor
    # -------------------------------------------------------------------------

    async def process_update(self, update_dict: Dict[str, Any]) -> Optional[str]:
        """
        Processes a single incoming update:
        1. Normalizes payload into TelegramUpdate.
        2. Validates Security Lock against owner ID.
        3. If unauthorized: Silently drops the message (no reply sent).
        4. If authorized: Parses slash command and dispatches reply.
        """
        update = TelegramUpdate.from_dict(update_dict)
        if not update:
            return None

        # Track offset to acknowledge update
        if update.update_id >= self._last_update_id:
            self._last_update_id = update.update_id

        # ---------------------------------------------------------------------
        # TASK 2: SECURITY LOCK
        # ---------------------------------------------------------------------
        if not self.is_authorized(update.chat_id, update.user_id):
            self.unauthorized_messages_dropped += 1
            logger.warning(
                "SECURITY LOCK: Unauthorized message from chat_id=%s (user_id=%s) SILENTLY DROPPED.",
                update.chat_id, update.user_id,
            )
            # Silently drop: absolutely NO message is sent back to the unauthorized user!
            return None

        # ---------------------------------------------------------------------
        # TASK 3: COMMAND HANDLING
        # ---------------------------------------------------------------------
        raw_text = update.text.strip()
        if not raw_text.startswith("/"):
            return None

        parts = raw_text.split()
        cmd_raw = parts[0][1:].lower()
        # Handle bot username mentions, e.g., /status@PipvaneBot
        command_name = cmd_raw.split("@")[0]
        args = parts[1:]

        reply_text = await self.handle_command(command_name, args, update)
        if reply_text:
            await self.api_client.send_message(
                chat_id=update.chat_id,
                text=reply_text,
                parse_mode="HTML",
            )
            self.authorized_commands_processed += 1
            self.last_command_time = time.time()
            self._record_command_audit(command_name, update.chat_id, reply_text)

        return reply_text

    def _record_command_audit(self, cmd: str, chat_id: Union[str, int], reply: str) -> None:
        """Maintains bounded in-memory audit log of executed control commands."""
        self.command_history.append({
            "timestamp": time.time(),
            "command": cmd,
            "chat_id": chat_id,
            "response_preview": reply[:100],
        })
        if len(self.command_history) > 100:
            self.command_history.pop(0)

    # -------------------------------------------------------------------------
    # Slash Command Handlers (/status, /pause, /resume, /help)
    # -------------------------------------------------------------------------

    async def handle_command(
        self,
        command: str,
        args: List[str],
        update: TelegramUpdate,
    ) -> str:
        """Routes authorized slash commands to their respective engine handlers."""

        # 1. /status: Returns equity and active lease state
        if command == "status":
            return self._handle_status_command()

        # 2. /pause: Triggers global kill switch
        elif command in ("pause", "kill", "stop"):
            return self._handle_pause_command()

        # 3. /resume: Clears global kill switch and resumes trading
        elif command in ("resume", "start_trading"):
            return self._handle_resume_command()

        # 4. /help or /start: Information
        elif command in ("help", "start"):
            return self._handle_help_command()

        # Unknown command fallback
        return (
            f"❓ <b>Unknown command:</b> <code>/{command}</code>\n\n"
            f"<b>Available Commands:</b>\n"
            f"• <code>/status</code> - View equity, terminals & failover lease state\n"
            f"• <code>/pause</code> - Activate global kill switch\n"
            f"• <code>/resume</code> - Resume automated order execution\n"
            f"• <code>/help</code> - Show this guide"
        )

    def _handle_status_command(self) -> str:
        """
        Generates /status response:
        - Trading Mode: ACTIVE vs PAUSED
        - Failover Lease State: ROLE_ACTIVE / ROLE_STANDBY / ROLE_LOCKED
        - Account Financials: Real-time Equity, Balance, Free Margin
        - Connected Terminals & Open Orders Count
        """
        # 1. Trading Execution Mode
        is_paused = False
        if self.orchestrator and hasattr(self.orchestrator, "is_paused"):
            is_paused = self.orchestrator.is_paused()

        mode_badge = "⏸️ <b>PAUSED (Kill-Switch Active)</b>" if is_paused else "▶️ <b>RUNNING (Trading Active)</b>"

        # 2. Failover Lease State
        lease_role = "ROLE_ACTIVE"
        lease_status_desc = "Lease Held (Unfenced)"
        node_id = "local_node"
        license_id = "UNKNOWN"

        if self.failover_manager:
            role_obj = getattr(self.failover_manager, "role", None)
            lease_role = getattr(role_obj, "value", str(role_obj)) if role_obj else "ROLE_ACTIVE"
            is_active = getattr(self.failover_manager, "is_active", True)
            cfg = getattr(self.failover_manager, "config", None)
            if cfg:
                node_id = getattr(cfg, "node_id", node_id)
                license_id = getattr(cfg, "license_id", license_id)

            if lease_role == "ROLE_ACTIVE":
                lease_status_desc = "🛡️ <b>ROLE_ACTIVE (Primary Lease Held)</b>"
            elif lease_role == "ROLE_STANDBY":
                lease_status_desc = "🔒 <b>ROLE_STANDBY (Standby Lockout)</b>"
            elif lease_role == "ROLE_LOCKED":
                lease_status_desc = "⛔ <b>ROLE_LOCKED (Self-Fenced)</b>"
            elif lease_role == "ROLE_SAFE_HALT":
                lease_status_desc = "🚨 <b>ROLE_SAFE_HALT (Emergency Halt)</b>"
            else:
                lease_status_desc = f"⚠️ <b>{lease_role}</b>"

        # 3. Account Financials & Terminals
        total_equity = 0.0
        total_balance = 0.0
        terminals_count = 0
        open_orders_count = 0
        terminal_details: List[str] = []

        if self.orchestrator:
            # Aggregate from orchestrator
            if hasattr(self.orchestrator, "get_total_equity"):
                total_equity = self.orchestrator.get_total_equity()
            if hasattr(self.orchestrator, "get_total_balance"):
                total_balance = self.orchestrator.get_total_balance()

            if hasattr(self.orchestrator, "get_account_states"):
                states = self.orchestrator.get_account_states()
                terminals_count = len(states)
                for tid, st in states.items():
                    terminal_details.append(
                        f"  • <code>{tid}</code>: Equity ${st.equity:,.2f} | Bal ${st.balance:,.2f}"
                    )

            if hasattr(self.orchestrator, "get_active_orders"):
                open_orders = self.orchestrator.get_active_orders()
                open_orders_count = len(open_orders)

            # If no account states were streamed, check registered terminals
            if terminals_count == 0 and hasattr(self.orchestrator, "ipc_dispatcher"):
                disp = self.orchestrator.ipc_dispatcher
                if hasattr(disp, "get_registered_terminals"):
                    term_list = disp.get_registered_terminals()
                    terminals_count = len(term_list)

        # Allow custom status provider override/enrichment
        if self.status_provider:
            try:
                custom_data = self.status_provider()
                if "total_equity" in custom_data:
                    total_equity = float(custom_data["total_equity"])
                if "total_balance" in custom_data:
                    total_balance = float(custom_data["total_balance"])
                if "lease_status_desc" in custom_data:
                    lease_status_desc = custom_data["lease_status_desc"]
                if "open_orders_count" in custom_data:
                    open_orders_count = int(custom_data["open_orders_count"])
            except Exception as e:
                logger.debug("Error in custom status provider: %s", e)

        # Build clean HTML card
        term_section = ""
        if terminal_details:
            term_section = "\n<b>Terminal Accounts:</b>\n" + "\n".join(terminal_details) + "\n"

        return (
            f"📊 <b>PIPVANE v3 ENGINE STATUS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Trading State:</b> {mode_badge}\n"
            f"<b>Failover State:</b> {lease_status_desc}\n"
            f"<b>Active Node:</b> <code>{node_id}</code>\n"
            f"<b>License:</b> <code>{license_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>ACCOUNT FINANCIALS</b>\n"
            f"<b>Total Equity:</b> <b>${total_equity:,.2f}</b>\n"
            f"<b>Total Balance:</b> ${total_balance:,.2f}\n"
            f"<b>Terminals Online:</b> {terminals_count}\n"
            f"<b>Open Orders:</b> {open_orders_count} position(s)\n"
            f"{term_section}"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}</i>"
        )

    def _handle_pause_command(self) -> str:
        """
        Executes /pause command:
        Triggers the global kill switch on the ExecutionOrchestrator.
        """
        if self.orchestrator and hasattr(self.orchestrator, "pause_trading"):
            self.orchestrator.pause_trading()
            logger.warning("GLOBAL KILL SWITCH ACTIVATED via Telegram /pause command.")

        return (
            f"⏸️ <b>TRADING PAUSED (KILL SWITCH ACTIVE)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Action:</b> Automated trade execution is strictly LOCKED.\n"
            f"<b>Safety Gate:</b> Inbound signals will be evaluated and logged, "
            f"but all broker order dispatches are BLOCKED.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Type <code>/resume</code> to re-enable automated order execution.</i>"
        )

    def _handle_resume_command(self) -> str:
        """
        Executes /resume command:
        Clears the global kill switch on the ExecutionOrchestrator.
        """
        if self.orchestrator and hasattr(self.orchestrator, "resume_trading"):
            self.orchestrator.resume_trading()
            logger.info("TRADING RESUMED via Telegram /resume command.")

        return (
            f"▶️ <b>TRADING RESUMED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Action:</b> Global kill switch cleared.\n"
            f"<b>Safety Gate:</b> Automated order dispatch is ACTIVE.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>All inbound signals will execute according to risk profiles.</i>"
        )

    def _handle_help_command(self) -> str:
        """Generates /help command reference card."""
        return (
            f"🤖 <b>PIPVANE v3 CONTROL BOT COMMANDS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <code>/status</code>\n"
            f"  Display real-time aggregate equity, balance, and active cloud lease state.\n\n"
            f"• <code>/pause</code>\n"
            f"  Activate the global emergency kill switch. Blocks all outbound broker orders.\n\n"
            f"• <code>/resume</code>\n"
            f"  Deactivate the kill switch and resume automated trading.\n\n"
            f"• <code>/help</code>\n"
            f"  Show this command reference.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Protected by strict Owner Security Lock.</i>"
        )

    # -------------------------------------------------------------------------
    # Long-Polling Worker Loop & Lifecycle
    # -------------------------------------------------------------------------

    async def poll_once(self) -> int:
        """
        Executes a single long-polling query.
        Returns the number of updates retrieved and processed.
        """
        offset = self._last_update_id + 1 if self._last_update_id > 0 else None
        updates = await self.api_client.get_updates(
            offset=offset,
            limit=100,
            timeout=self.config.long_poll_timeout_s,
        )

        processed = 0
        for u in updates:
            up_id = u.get("update_id", 0)
            if up_id > 0:
                self._last_update_id = max(self._last_update_id, up_id)
            await self.process_update(u)
            processed += 1

        return processed

    async def start(self) -> None:
        """Starts the background long-polling worker."""
        if self._is_running:
            return

        self._is_running = True
        self._poll_task = asyncio.create_task(
            self._poll_loop(),
            name="TelegramControlBotPoller",
        )
        logger.info(
            "TelegramControlBot started with %ds long-polling timeout for owner %s.",
            self.config.long_poll_timeout_s, self.config.owner_chat_id,
        )

    async def stop(self) -> None:
        """Stops the background worker and terminates cleanly."""
        self._is_running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        logger.info("TelegramControlBot stopped.")

    async def _poll_loop(self) -> None:
        """Continuous long-polling loop."""
        retry_delay = self.config.retry_delay_s

        while self._is_running:
            try:
                await self.poll_once()
                retry_delay = self.config.retry_delay_s  # Reset backoff on success
                if self.config.polling_interval_s > 0:
                    await asyncio.sleep(self.config.polling_interval_s)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in TelegramControlBot long-polling loop: %s", e)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30.0)
