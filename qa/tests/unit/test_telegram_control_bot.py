"""
Pipvane v3 - Unit Test Suite: Telegram Control Bot (Sprint 18 Directive)
========================================================================
Validates the personal Telegram Bot interface according to Sprint 18 mandates:
1. Bot Initialization & Long Polling:
   - Validates HTTP API wrapper long polling parameter (timeout=30) to minimize VPS bandwidth.
2. Security Lock:
   - Enforces owner verification against registered owner_chat_id.
   - Silently drops all incoming messages from unauthorized users (0 replies sent).
3. Command Handling:
   - /status: Returns real-time equity, financial metrics, and active failover lease state.
   - /pause: Triggers global kill switch on ExecutionOrchestrator.
   - /resume: Resumes automated trade execution.
   - /help & command fallback handling.
"""

from __future__ import annotations

import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
)
from engine.failover_manager import EngineRole, FailoverManager, FailoverManagerConfig
from engine.ipc_protocol import AccountState
from engine.telegram_control_bot import (
    MockTelegramBotApiClient,
    TelegramControlBot,
    TelegramControlBotConfig,
    TelegramUpdate,
)


class TestTelegramControlBotSecurityAndCommands(unittest.IsolatedAsyncioTestCase):
    """Asynchronous test suite validating Security Lock, slash commands, and long polling."""

    async def asyncSetUp(self):
        self.owner_id = "777888999"
        self.unauthorized_id = "111222333"

        self.config = TelegramControlBotConfig(
            bot_token="123456789:ABC_TEST_BOT_TOKEN_XYZ",
            owner_chat_id=self.owner_id,
            long_poll_timeout_s=30,  # Sprint 18 Long-polling mandate
            polling_interval_s=0.01,
        )

        self.api_client = MockTelegramBotApiClient()

        # Mock Orchestrator
        self.orchestrator = MagicMock()
        self.orchestrator._is_paused = False
        self.orchestrator.is_paused = lambda: self.orchestrator._is_paused

        def mock_pause():
            self.orchestrator._is_paused = True
            return True

        def mock_resume():
            self.orchestrator._is_paused = False
            return True

        self.orchestrator.pause_trading = mock_pause
        self.orchestrator.resume_trading = mock_resume
        self.orchestrator.get_total_equity = MagicMock(return_value=12450.75)
        self.orchestrator.get_total_balance = MagicMock(return_value=12000.00)
        self.orchestrator.get_active_orders = MagicMock(return_value=[{"ticket": 101}, {"ticket": 102}])
        self.orchestrator.get_account_states = MagicMock(return_value={
            "mt5_primary": AccountState(account_login=9991, balance=12000.0, equity=12450.75),
        })

        # Mock FailoverManager
        self.failover_manager = MagicMock()
        self.failover_manager.role = EngineRole.ROLE_ACTIVE
        self.failover_manager.is_active = True
        self.failover_manager.config = MagicMock()
        self.failover_manager.config.node_id = "vps_primary_node"
        self.failover_manager.config.license_id = "LIC-PIPVANE-PRO"

        self.bot = TelegramControlBot(
            config=self.config,
            orchestrator=self.orchestrator,
            failover_manager=self.failover_manager,
            api_client=self.api_client,
        )

    # -------------------------------------------------------------------------
    # Task 2: Security Lock Tests
    # -------------------------------------------------------------------------

    async def test_01_security_lock_owner_authorized(self):
        """Verifies that the registered owner's commands are accepted and processed."""
        self.assertTrue(self.bot.is_authorized(self.owner_id))
        self.assertTrue(self.bot.is_authorized(int(self.owner_id)))

    async def test_02_security_lock_unauthorized_user_silently_dropped(self):
        """
        TASK 2 MANDATE:
        The bot MUST verify the chat_id of every incoming message against the registered owner's ID.
        Silently drop messages from any unauthorized users (0 messages sent back).
        """
        # Enqueue command from unauthorized user
        self.api_client.enqueue_update(
            update_id=1,
            chat_id=self.unauthorized_id,
            text="/status",
            user_id=self.unauthorized_id,
        )

        processed = await self.bot.poll_once()
        self.assertEqual(processed, 1)

        # 1. No reply was dispatched to the unauthorized user
        self.assertEqual(len(self.api_client.sent_messages), 0)

        # 2. Unauthorized drop counter was incremented
        self.assertEqual(self.bot.unauthorized_messages_dropped, 1)
        self.assertEqual(self.bot.authorized_commands_processed, 0)

    async def test_03_secondary_allowed_chat_ids(self):
        """Verifies that secondary authorized chat IDs are recognized."""
        secondary_id = "999000111"
        self.config.allowed_chat_ids.append(secondary_id)

        self.assertTrue(self.bot.is_authorized(secondary_id))
        self.assertFalse(self.bot.is_authorized("666666666"))

    # -------------------------------------------------------------------------
    # Task 3: Command Handling (/status, /pause, /resume)
    # -------------------------------------------------------------------------

    async def test_04_status_command_returns_equity_and_active_lease(self):
        """
        TASK 3 MANDATE:
        /status returns equity and active lease state.
        """
        self.api_client.enqueue_update(
            update_id=10,
            chat_id=self.owner_id,
            text="/status",
        )

        await self.bot.poll_once()

        self.assertEqual(len(self.api_client.sent_messages), 1)
        sent = self.api_client.sent_messages[0]
        self.assertEqual(sent["chat_id"], self.owner_id)

        response_text = sent["text"]
        # Verify Equity & Financials
        self.assertIn("12,450.75", response_text)
        self.assertIn("12,000.00", response_text)
        self.assertIn("mt5_primary", response_text)

        # Verify Active Lease State & Node
        self.assertIn("ROLE_ACTIVE", response_text)
        self.assertIn("vps_primary_node", response_text)
        self.assertIn("LIC-PIPVANE-PRO", response_text)

        # Verify Trading Mode
        self.assertIn("RUNNING", response_text)

    async def test_05_status_command_standby_mode(self):
        """Verifies /status accurately reflects Standby Lockout when role is ROLE_STANDBY."""
        self.failover_manager.role = EngineRole.ROLE_STANDBY
        self.failover_manager.is_active = False

        self.api_client.enqueue_update(
            update_id=11,
            chat_id=self.owner_id,
            text="/status",
        )

        await self.bot.poll_once()
        sent = self.api_client.sent_messages[-1]
        self.assertIn("ROLE_STANDBY (Standby Lockout)", sent["text"])

    async def test_06_pause_command_triggers_global_kill_switch(self):
        """
        TASK 3 MANDATE:
        /pause triggers global kill switch on ExecutionOrchestrator.
        """
        self.assertFalse(self.orchestrator.is_paused())

        self.api_client.enqueue_update(
            update_id=20,
            chat_id=self.owner_id,
            text="/pause",
        )

        await self.bot.poll_once()

        # 1. Orchestrator global kill switch activated
        self.assertTrue(self.orchestrator.is_paused())

        # 2. Confirmation sent back to owner
        self.assertEqual(len(self.api_client.sent_messages), 1)
        sent = self.api_client.sent_messages[0]
        self.assertIn("TRADING PAUSED (KILL SWITCH ACTIVE)", sent["text"])
        self.assertIn("strictly LOCKED", sent["text"])

    async def test_07_resume_command_restores_trading(self):
        """
        TASK 3 MANDATE:
        /resume restores automated trade execution.
        """
        # First put orchestrator into paused state
        self.orchestrator.pause_trading()
        self.assertTrue(self.orchestrator.is_paused())

        self.api_client.enqueue_update(
            update_id=21,
            chat_id=self.owner_id,
            text="/resume",
        )

        await self.bot.poll_once()

        # 1. Orchestrator kill switch cleared
        self.assertFalse(self.orchestrator.is_paused())

        # 2. Confirmation sent back to owner
        self.assertEqual(len(self.api_client.sent_messages), 1)
        sent = self.api_client.sent_messages[0]
        self.assertIn("TRADING RESUMED", sent["text"])
        self.assertIn("ACTIVE", sent["text"])

    async def test_08_command_with_bot_mention(self):
        """Supports commands formatted with bot handle, e.g., /status@PipvaneBot."""
        self.api_client.enqueue_update(
            update_id=30,
            chat_id=self.owner_id,
            text="/status@PipvaneControlBot",
        )

        await self.bot.poll_once()
        self.assertEqual(len(self.api_client.sent_messages), 1)
        self.assertIn("PIPVANE v3 ENGINE STATUS", self.api_client.sent_messages[0]["text"])

    async def test_09_help_and_unknown_commands(self):
        """Verifies /help and unknown command fallback."""
        self.api_client.enqueue_update(
            update_id=40,
            chat_id=self.owner_id,
            text="/help",
        )
        self.api_client.enqueue_update(
            update_id=41,
            chat_id=self.owner_id,
            text="/nonexistent_cmd",
        )

        await self.bot.poll_once()
        self.assertEqual(len(self.api_client.sent_messages), 2)
        self.assertIn("CONTROL BOT COMMANDS", self.api_client.sent_messages[0]["text"])
        self.assertIn("Unknown command", self.api_client.sent_messages[1]["text"])

    async def test_10_non_command_text_ignored(self):
        """Regular chat messages (not starting with slash) are ignored."""
        self.api_client.enqueue_update(
            update_id=50,
            chat_id=self.owner_id,
            text="Hello bot",
        )

        await self.bot.poll_once()
        self.assertEqual(len(self.api_client.sent_messages), 0)

    # -------------------------------------------------------------------------
    # Task 4: Long-Polling Bandwidth Optimization
    # -------------------------------------------------------------------------

    async def test_11_long_polling_timeout_passed_to_api(self):
        """
        TASK 4 MANDATE:
        Ensure the HTTP polling uses long-polling (timeout=30) to minimize bandwidth on the VPS.
        """
        self.assertEqual(self.config.long_poll_timeout_s, 30)

        await self.bot.poll_once()

        # Assert client observed timeout=30 in get_updates call
        self.assertEqual(self.api_client.long_poll_timeout_observed, 30)
        self.assertGreaterEqual(self.api_client.get_updates_called_count, 1)

    async def test_12_update_offset_progression(self):
        """Ensures update_id offset increments to acknowledge and discard processed updates."""
        self.api_client.enqueue_update(update_id=101, chat_id=self.owner_id, text="/pause")
        self.api_client.enqueue_update(update_id=102, chat_id=self.owner_id, text="/resume")

        await self.bot.poll_once()
        self.assertEqual(self.bot._last_update_id, 102)

    async def test_13_background_worker_lifecycle(self):
        """Tests start() and stop() background long-polling lifecycle."""
        await self.bot.start()
        self.assertTrue(self.bot._is_running)

        # Allow loop to execute
        await asyncio.sleep(0.05)
        self.assertGreater(self.api_client.get_updates_called_count, 0)

        await self.bot.stop()
        self.assertFalse(self.bot._is_running)
        self.assertIsNone(self.bot._poll_task)


class TestHttpTelegramBotApiClient(unittest.IsolatedAsyncioTestCase):
    """Asynchronous test suite validating HttpTelegramBotApiClient with mocked HTTP responses."""

    async def asyncSetUp(self):
        self.config = TelegramControlBotConfig(
            bot_token="TEST_BOT_TOKEN_12345",
            owner_chat_id="12345678",
            long_poll_timeout_s=30,
        )
        from engine.telegram_control_bot import HttpTelegramBotApiClient
        self.client = HttpTelegramBotApiClient(self.config)

    async def test_14_http_client_get_updates_payload_and_timeout(self):
        """Verifies long-polling payload includes limit, timeout, allowed_updates, and offset."""
        mock_response_data = {
            "ok": True,
            "result": [
                {
                    "update_id": 501,
                    "message": {
                        "message_id": 1,
                        "chat": {"id": 12345678},
                        "text": "/status",
                    },
                }
            ],
        }

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_response_data).encode("utf-8")
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            updates = await self.client.get_updates(offset=500, limit=50, timeout=30)
            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0]["update_id"], 501)

            # Inspect Request
            req = mock_urlopen.call_args[0][0]
            self.assertIn("botTEST_BOT_TOKEN_12345/getUpdates", req.full_url)
            sent_payload = json.loads(req.data.decode("utf-8"))
            self.assertEqual(sent_payload["timeout"], 30)
            self.assertEqual(sent_payload["offset"], 500)
            self.assertEqual(sent_payload["limit"], 50)
            self.assertEqual(sent_payload["allowed_updates"], ["message"])

    async def test_15_http_client_send_message(self):
        """Verifies send_message POSTs JSON payload with chat_id, text, and parse_mode."""
        mock_response_data = {
            "ok": True,
            "result": {
                "message_id": 99,
                "chat": {"id": 12345678},
                "text": "Hello Owner",
            },
        }

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = json.dumps(mock_response_data).encode("utf-8")
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            res = await self.client.send_message(chat_id="12345678", text="Hello Owner", parse_mode="HTML")
            self.assertTrue(res.get("ok"))

            req = mock_urlopen.call_args[0][0]
            self.assertIn("botTEST_BOT_TOKEN_12345/sendMessage", req.full_url)
            sent_payload = json.loads(req.data.decode("utf-8"))
            self.assertEqual(sent_payload["chat_id"], "12345678")
            self.assertEqual(sent_payload["text"], "Hello Owner")
            self.assertEqual(sent_payload["parse_mode"], "HTML")

    async def test_16_http_client_handles_network_timeout_gracefully(self):
        """Verifies that network timeout returns empty list without crashing."""
        with patch("urllib.request.urlopen", side_effect=TimeoutError("Socket timeout")):
            updates = await self.client.get_updates(timeout=30)
            self.assertEqual(updates, [])
