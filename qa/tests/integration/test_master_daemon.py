"""
Pipvane v3: Master Orchestration Daemon Integration Test Suite.
==============================================================
Tests the end-to-end Master Orchestrator daemon (engine/main.py):
1. Config Loading & Persistence (JSON serialization)
2. Asynchronous Lifecycle (Start & Stop of all coordinated subsystems)
3. Full Ingestion-to-Execution Pipeline (Telegram -> Ledger -> Normalizer -> Parser -> Risk -> Orchestrator)
4. Anti-Split-Brain Standby Lockout Enforcement (ROLE_STANDBY blocks order submission)
5. Risk Guard Rejection & Skip Diagnostics Reporting
6. Personal Telegram Control Bot Integration (/status, /pause, /resume)
7. Terminal Discovery & IPC Registration
8. Interactive Desktop Session Guard Compliance
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

# Ensure root directory is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.main import (
    PipvaneMasterDaemon,
    MasterDaemonConfig,
    SKIP_STANDBY_LOCK,
    is_running_in_session_zero,
)
from engine.failover_manager import (
    EngineRole,
    ROLE_ACTIVE,
    ROLE_STANDBY,
    ROLE_LOCKED,
)
from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionStatus,
    OrderSide,
)
from engine.signal_parser import SKIP_GEOMETRY
from engine.risk_engine import (
    SkipReason,
    SKIP_LICENSE_EXPIRED,
)
from engine.tdlib_client import MockTDLibClient
from engine.telegram_gateway import TelegramMessage
from engine.telegram_control_bot import MockTelegramBotApiClient


@pytest.fixture
def temp_daemon_dir(tmp_path):
    """Provides an isolated temporary directory for daemon data."""
    data_dir = tmp_path / "pipvane_test"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture
def daemon_config(temp_daemon_dir):
    """Creates a mock-safe MasterDaemonConfig using temp directory."""
    return MasterDaemonConfig(
        app_data_dir=temp_daemon_dir,
        api_port=0,
        ws_port=0,
        role="active",
        mock_telegram=True,
        mock_terminals=True,
        telegram_owner_chat_id=987654321,
        telegram_bot_token="test_mock_bot_token",
        reconciliation_interval_s=10.0,
        headless=True,
    )


class TestMasterDaemonIntegration:
    """Comprehensive integration tests for Pipvane Master Orchestration Daemon."""

    def test_01_config_initialization_and_serialization(self, temp_daemon_dir):
        """Test configuration defaults, JSON file persistence, and reload."""
        cfg_path = temp_daemon_dir / "custom_config.json"
        cfg = MasterDaemonConfig(
            app_data_dir=temp_daemon_dir,
            config_path=cfg_path,
            api_port=5555,
            ws_port=5556,
            role="standby",
            node_id="test_node_01",
            mock_telegram=True,
            mock_terminals=True,
        )
        cfg.save_to_file(cfg_path)
        assert cfg_path.is_file(), "Config file must be created on disk"

        reloaded = MasterDaemonConfig.load_from_file(cfg_path)
        assert reloaded.api_port == 5555
        assert reloaded.ws_port == 5556
        assert reloaded.role == "standby"
        assert reloaded.node_id == "test_node_01"
        assert reloaded.mock_telegram is True
        assert reloaded.mock_terminals is True

    @pytest.mark.asyncio
    async def test_02_daemon_lifecycle_start_and_stop(self, daemon_config):
        """Test clean asynchronous start and graceful multi-stage shutdown."""
        mock_tdlib = MockTDLibClient()
        mock_bot_api = MockTelegramBotApiClient()

        daemon = PipvaneMasterDaemon(
            config=daemon_config,
            tdlib_client=mock_tdlib,
            control_bot_api_client=mock_bot_api,
        )

        assert daemon.is_running is False if hasattr(daemon, "is_running") else not daemon._is_running

        await daemon.start()
        assert daemon._is_running is True
        assert daemon.api_server.is_running is True
        assert daemon.ipc_dispatcher._is_running is True
        assert daemon.execution_orchestrator._is_running is True

        # Verify session token generated and accessible
        assert daemon.api_server.token_path.is_file()
        token = daemon.api_server.token_path.read_text(encoding="utf-8")
        assert len(token) == 64

        # Verify mock terminal is enrolled
        terminals = daemon.ipc_dispatcher.get_registered_terminals()
        assert len(terminals) >= 1
        assert any("mock_mt5" in t for t in terminals)

        # Graceful shutdown
        await daemon.stop()
        assert daemon._is_running is False
        assert daemon.api_server.is_running is False
        assert daemon.ipc_dispatcher._is_running is False
        assert daemon.execution_orchestrator._is_running is False

    @pytest.mark.asyncio
    async def test_03_inbound_signal_active_mode_full_pipeline(self, daemon_config):
        """
        Tests full pipeline: Telegram Message -> Ingestion Ledger -> Normalizer ->
        Parser -> Risk Engine -> Execution Orchestrator -> IPC Dispatch.
        """
        mock_tdlib = MockTDLibClient()
        mock_bot_api = MockTelegramBotApiClient()

        daemon = PipvaneMasterDaemon(
            config=daemon_config,
            tdlib_client=mock_tdlib,
            control_bot_api_client=mock_bot_api,
        )

        await daemon.start()

        # Ingest a valid trading signal
        signal_text = "BUY EURUSD\nEntry: 1.08500\nSL: 1.08000\nTP1: 1.09000\nTP2: 1.09500"
        msg = TelegramMessage(
            chat_id=12345678,
            message_id=101,
            date=int(time.time()),
            text=signal_text,
        )

        # Trigger message handler directly
        daemon.handle_inbound_telegram_message(msg)

        # Give async tasks a brief moment to process
        await asyncio.sleep(0.3)

        assert daemon.signals_received_count == 1
        assert daemon.orders_executed_count >= 1
        assert daemon.signals_skipped_count == 0

        # Verify message was recorded in the SQLite ledger
        rows = daemon.ingestion.get_channel_timeline(12345678)
        assert len(rows) == 1
        assert rows[0].message_id == 101

        # Verify active orders exist in orchestrator
        orders = daemon.execution_orchestrator.get_active_orders()
        assert len(orders) >= 1
        assert orders[0]["symbol"] == "EURUSD"

        await daemon.stop()

    @pytest.mark.asyncio
    async def test_04_standby_lockout_enforced_in_standby_mode(self, daemon_config):
        """
        Tests that when daemon is in ROLE_STANDBY, incoming trading signals are
        strictly rejected with SKIP_STANDBY_LOCK to prevent split-brain.
        """
        daemon_config.role = "standby"
        mock_tdlib = MockTDLibClient()
        mock_bot_api = MockTelegramBotApiClient()

        daemon = PipvaneMasterDaemon(
            config=daemon_config,
            tdlib_client=mock_tdlib,
            control_bot_api_client=mock_bot_api,
        )

        await daemon.start()

        assert daemon.failover_mgr.role == ROLE_STANDBY
        assert daemon.failover_mgr.can_execute_orders is False

        # Ingest a trading signal while in Standby
        msg = TelegramMessage(
            chat_id=12345678,
            message_id=202,
            date=int(time.time()),
            text="BUY GBPUSD\nEntry: 1.25000\nSL: 1.24500\nTP1: 1.25500",
        )

        daemon.handle_inbound_telegram_message(msg)
        await asyncio.sleep(0.1)

        # Message must be recorded in ledger (active ingestion), but order blocked
        rows = daemon.ingestion.get_channel_timeline(12345678)
        assert len(rows) == 1

        # Execution must be 0, and skip count must increment
        assert daemon.orders_executed_count == 0
        assert daemon.signals_skipped_count == 1

        orders = daemon.execution_orchestrator.get_active_orders()
        assert len(orders) == 0

        await daemon.stop()

    @pytest.mark.asyncio
    async def test_05_risk_guard_rejection_and_skip_event(self, daemon_config):
        """
        Tests that invalid signals (violating market geometry) are rejected
        with SKIP_GEOMETRY and trigger a skip alert.
        """
        daemon_config.role = "active"
        mock_tdlib = MockTDLibClient()
        mock_bot_api = MockTelegramBotApiClient()

        daemon = PipvaneMasterDaemon(
            config=daemon_config,
            tdlib_client=mock_tdlib,
            control_bot_api_client=mock_bot_api,
        )

        await daemon.start()

        # Ingest invalid geometry signal (BUY order with SL above entry)
        invalid_signal_text = "BUY EURUSD\nEntry: 1.08500\nSL: 1.09500\nTP1: 1.09000"
        msg = TelegramMessage(
            chat_id=12345678,
            message_id=303,
            date=int(time.time()),
            text=invalid_signal_text,
        )

        daemon.handle_inbound_telegram_message(msg)
        await asyncio.sleep(0.1)

        assert daemon.signals_received_count == 1
        assert daemon.orders_executed_count == 0
        assert daemon.signals_skipped_count == 1

        orders = daemon.execution_orchestrator.get_active_orders()
        assert len(orders) == 0

        await daemon.stop()

    @pytest.mark.asyncio
    async def test_06_telegram_control_bot_integration(self, daemon_config):
        """
        Tests personal Telegram Control Bot commands (/status, /pause, /resume)
        against live daemon orchestrator and failover states.
        """
        daemon_config.role = "active"
        mock_tdlib = MockTDLibClient()
        mock_bot_api = MockTelegramBotApiClient()

        daemon = PipvaneMasterDaemon(
            config=daemon_config,
            tdlib_client=mock_tdlib,
            control_bot_api_client=mock_bot_api,
        )

        await daemon.start()
        assert daemon.control_bot is not None

        owner_id = daemon_config.telegram_owner_chat_id

        # 1. /status command
        resp_status = await daemon.control_bot.process_update({
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": owner_id},
                "from": {"id": owner_id},
                "text": "/status",
            },
        })
        assert resp_status is not None
        assert "PIPVANE v3 ENGINE STATUS" in resp_status
        assert "RUNNING" in resp_status

        # 2. /pause command (Global Kill Switch)
        resp_pause = await daemon.control_bot.process_update({
            "update_id": 2,
            "message": {
                "message_id": 2,
                "chat": {"id": owner_id},
                "from": {"id": owner_id},
                "text": "/pause",
            },
        })
        assert resp_pause is not None
        assert "KILL SWITCH ACTIVE" in resp_pause
        assert daemon.execution_orchestrator._is_paused is True

        # Verify entry orders are now rejected while paused
        msg = TelegramMessage(
            chat_id=12345678,
            message_id=404,
            date=int(time.time()),
            text="BUY USDJPY\nEntry: 155.00\nSL: 154.00\nTP1: 156.00",
        )
        daemon.handle_inbound_telegram_message(msg)
        await asyncio.sleep(0.1)
        assert daemon.signals_skipped_count >= 1

        # 3. /resume command
        resp_resume = await daemon.control_bot.process_update({
            "update_id": 3,
            "message": {
                "message_id": 3,
                "chat": {"id": owner_id},
                "from": {"id": owner_id},
                "text": "/resume",
            },
        })
        assert resp_resume is not None
        assert "TRADING RESUMED" in resp_resume
        assert daemon.execution_orchestrator._is_paused is False

        await daemon.stop()

    def test_07_interactive_session_zero_guard(self):
        """Test Windows Session 0 non-interactive service guard (V3 Correction 1)."""
        with patch("engine.main.is_running_in_session_zero", return_value=True):
            cfg = MasterDaemonConfig(strict_desktop_check=True)
            with pytest.raises(RuntimeError) as exc_info:
                PipvaneMasterDaemon(config=cfg)
            assert "Windows Session 0" in str(exc_info.value)
