"""
Unit tests for MessageBackfill Engine & Gateway Integration (Sprint 16 Directive).
==================================================================================
Validates:
1. Last Processed State: Queries local SQLite RawMessage database for highest message_id per channel.
2. TDLib History Request: Uses TDLib getChatHistory endpoint to iterate backward from live state
   until matching local database state.
3. Deduplication Check: Cross-references Signal Hash and Signal ID against reconciled broker orders
   (from Step C of takeover) and marks duplicates as SKIP_DUP.
4. Integration with TelegramGateway and ExecutionOrchestrator.
"""

import sqlite3
import time
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch
import pytest

from engine.message_backfill import (
    MessageBackfill,
    BackfillMessageItem,
    BackfillChannelReport,
    BackfillSummary,
    compute_signal_hash,
    compute_signal_id,
)
from engine.tdlib_client import MockTDLibClient
from engine.ingestion_controller import IngestionController
from engine.signal_parser import SignalParser
from engine.risk_engine import SKIP_DUP
from engine.ipc_protocol import PositionItem, OrderSide, ActionType
from engine.execution_orchestrator import ExecutionOrchestrator
from engine.ipc_dispatcher import IPCDispatcher


@pytest.fixture
def mock_dispatcher():
    return IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)


@pytest.fixture
def mock_orchestrator(mock_dispatcher):
    return ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )


def create_in_memory_raw_db():
    """Initializes in-memory SQLite schema matching IngestionController."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute("""
            CREATE TABLE raw_messages (
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                raw_text TEXT NOT NULL,
                media_reference TEXT,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL,
                received_at INTEGER NOT NULL,
                reply_to_message_id INTEGER,
                sender_id INTEGER,
                previous_hash TEXT NOT NULL,
                hash TEXT NOT NULL,
                PRIMARY KEY (channel_id, message_id, version)
            );
        """)
        conn.execute("""
            CREATE TABLE channel_timeline_meta (
                channel_id INTEGER PRIMARY KEY,
                head_hash TEXT NOT NULL,
                total_events INTEGER NOT NULL DEFAULT 0,
                last_message_id INTEGER NOT NULL DEFAULT 0,
                last_timestamp INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
        """)
    return conn


# =============================================================================
# 1. Task 1: Last Processed State Tests
# =============================================================================

def test_last_processed_state_queries_sqlite_database():
    """Verifies that MessageBackfill extracts the highest message_id per channel from SQLite."""
    conn = create_in_memory_raw_db()
    with conn:
        # Seed channel -1001 with max message_id 500
        conn.execute(
            "INSERT INTO raw_messages VALUES (-1001, 100, 1, 'text', NULL, 0, 1000, 1000, NULL, NULL, 'h0', 'h1');"
        )
        conn.execute(
            "INSERT INTO raw_messages VALUES (-1001, 500, 1, 'text', NULL, 0, 2000, 2000, NULL, NULL, 'h1', 'h2');"
        )
        # Seed channel -1002 in channel_timeline_meta with last_message_id 1200
        conn.execute(
            "INSERT INTO channel_timeline_meta VALUES (-1002, 'head', 10, 1200, 3000, 3000);"
        )

    # Attach to mock IngestionController
    mock_ingestion = MagicMock()
    mock_ingestion.conn = conn

    backfill = MessageBackfill(ingestion_controller=mock_ingestion)
    state = backfill.get_last_processed_state(subscribed_channels=[-1001, -1002, -1003])

    # Channel -1001 max is 500
    assert state[-1001] == 500
    # Channel -1002 max is 1200
    assert state[-1002] == 1200
    # Channel -1003 has no prior records -> defaults to 0
    assert state[-1003] == 0


# =============================================================================
# 2. Task 2: TDLib History Request (getChatHistory Backward Iteration)
# =============================================================================

def test_fetch_chat_history_backward_iterates_until_local_state():
    """
    Verifies that fetch_chat_history_backward iterates backward through getChatHistory
    and halts immediately when encountering a message_id <= last_processed_id.
    """
    client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    chat_id = -100555

    # Mock historical messages in Telegram (newest to oldest)
    # IDs: 105, 104, 103, 102, 101, 100, 99...
    now_ts = int(time.time())
    mock_history = [
        {"id": 105, "date": now_ts - 10, "content": {"text": {"text": "BUY EURUSD 1.0850 SL 1.0800 TP 1.0950"}}},
        {"id": 104, "date": now_ts - 20, "content": {"text": {"text": "GOLD SELL 2650 SL 2660 TP 2630"}}},
        {"id": 103, "date": now_ts - 30, "content": {"text": {"text": "BTCUSD BUY 90000 SL 89000 TP 92000"}}},
        {"id": 102, "date": now_ts - 40, "content": {"text": {"text": "US30 BUY 43500 SL 43300 TP 43800"}}},
        {"id": 101, "date": now_ts - 50, "content": {"text": {"text": "Earlier chatter"}}},
        {"id": 100, "date": now_ts - 60, "content": {"text": {"text": "Already processed in SQLite"}}},
        {"id": 99, "date": now_ts - 70, "content": {"text": {"text": "Ancient message"}}},
    ]
    client.mock_chat_histories[chat_id] = mock_history

    backfill = MessageBackfill(tdlib_client=client, page_limit=2)

    # Local state is 100 (so missed messages are 101, 102, 103, 104, 105)
    retrieved = backfill.fetch_chat_history_backward(chat_id=chat_id, last_processed_id=100)

    # Must retrieve exactly 101, 102, 103, 104, 105
    assert len(retrieved) == 5
    retrieved_ids = [m["id"] for m in retrieved]
    assert retrieved_ids == [101, 102, 103, 104, 105], "Messages must be sorted chronologically"
    assert 100 not in retrieved_ids
    assert 99 not in retrieved_ids


# =============================================================================
# 3. Task 3: Deduplication Check (Cross-referencing Reconciled Broker Orders)
# =============================================================================

def test_deduplication_marks_existing_broker_trade_as_skip_dup(mock_orchestrator):
    """
    Verifies that when a backfilled signal matches a newly reconciled broker order
    (from Step C of takeover), it is strictly marked as SKIP_DUP.
    """
    backfill = MessageBackfill(
        signal_parser=SignalParser(),
        orchestrator=mock_orchestrator,
    )

    chat_id = -100777
    msg_id = 450
    sig_id = compute_signal_id(chat_id, msg_id)

    # Reconciled broker positions dump from Step C
    mock_reconciled_positions = [
        PositionItem(
            ticket=987654,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="EURUSD",
            position_type=int(OrderSide.BUY),
            volume=1.0,
            open_price=1.0850,
            current_price=1.0855,
            stop_loss=1.0800,
            take_profit=1.0950,
            comment=f"sig_{sig_id}",  # Carries signal_id in comment
        )
    ]

    # Pre-index in orchestrator's discovered open signals (simulating Step C reconciliation)
    mock_orchestrator._discovered_open_signals[sig_id] = 987654

    raw_msg = {
        "id": msg_id,
        "date": int(time.time()),
        "content": {"text": {"text": "BUY EURUSD 1.0850 SL 1.0800 TP 1.0950"}},
    }

    item: BackfillMessageItem = backfill.process_backfilled_message(
        raw_msg=raw_msg,
        chat_id=chat_id,
        reconciled_orders=mock_reconciled_positions,
        discovered_open_signals=mock_orchestrator._discovered_open_signals,
    )

    # Must be detected as duplicate and marked SKIP_DUP
    assert item.is_duplicate is True
    assert item.skip_reason is not None
    assert "SKIP_DUP" in item.skip_reason
    assert item.parsed_signal.skip_reason == SKIP_DUP
    assert item.order_intent is None, "Duplicate signals must NOT generate executable OrderIntent"


def test_non_duplicate_signal_generates_executable_order_intent():
    """
    Verifies that a backfilled signal not matching any broker position
    is marked as executable with a valid OrderIntent.
    """
    backfill = MessageBackfill(signal_parser=SignalParser())
    chat_id = -100777
    msg_id = 451

    raw_msg = {
        "id": msg_id,
        "date": int(time.time()),
        "content": {"text": {"text": "BUY GBPUSD 1.2500 SL 1.2450 TP 1.2600"}},
    }

    # Reconciled broker has only EURUSD, not GBPUSD
    mock_reconciled_positions = [
        PositionItem(
            ticket=111111,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="EURUSD",
            position_type=int(OrderSide.BUY),
            volume=0.5,
            open_price=1.0850,
            current_price=1.0855,
            stop_loss=1.0800,
            take_profit=1.0950,
        )
    ]

    item: BackfillMessageItem = backfill.process_backfilled_message(
        raw_msg=raw_msg,
        chat_id=chat_id,
        reconciled_orders=mock_reconciled_positions,
    )

    assert item.is_duplicate is False
    assert item.skip_reason is None
    assert item.parsed_signal is not None
    assert item.parsed_signal.symbol == "GBPUSD"
    assert item.order_intent is not None
    assert item.order_intent.symbol == "GBPUSD"
    assert item.order_intent.signal_id == compute_signal_id(chat_id, msg_id)


# =============================================================================
# 4. Full Execution Pipeline & Summary
# =============================================================================

def test_full_execute_backfill_pipeline():
    """
    Executes full end-to-end backfill across 2 channels:
    - Channel 1 has 2 missed signals (1 duplicate on broker, 1 fresh executable).
    - Channel 2 has 1 chatter message (non-signal).
    """
    client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    chat1 = -100111
    chat2 = -100222

    now_ts = int(time.time())
    sig1_id = compute_signal_id(chat1, 102)

    client.mock_chat_histories[chat1] = [
        {"id": 102, "date": now_ts - 10, "content": {"text": {"text": "BUY XAUUSD 2650 SL 2640 TP 2670"}}},
        {"id": 101, "date": now_ts - 20, "content": {"text": {"text": "SELL EURUSD 1.0800 SL 1.0850 TP 1.0700"}}},
        {"id": 100, "date": now_ts - 50, "content": {"text": {"text": "Old message"}}},
    ]

    client.mock_chat_histories[chat2] = [
        {"id": 201, "date": now_ts - 15, "content": {"text": {"text": "Market looks bullish today!"}}},
        {"id": 200, "date": now_ts - 50, "content": {"text": {"text": "Old message"}}},
    ]

    # Reconciled orders contain trade for sig1_id (XAUUSD)
    reconciled_orders = [
        PositionItem(
            ticket=55555,
            magic_number=123456,
            open_time_unix=int(time.time()),
            symbol="XAUUSD",
            position_type=int(OrderSide.BUY),
            volume=0.1,
            open_price=2650.0,
            current_price=2655.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            comment=f"sig_{sig1_id}",
        )
    ]

    # SQLite mock state: chat1 at 100, chat2 at 200
    mock_ingestion = MagicMock()
    conn = create_in_memory_raw_db()
    with conn:
        conn.execute("INSERT INTO channel_timeline_meta VALUES (-100111, 'h1', 1, 100, 1000, 1000);")
        conn.execute("INSERT INTO channel_timeline_meta VALUES (-100222, 'h2', 1, 200, 1000, 1000);")
    mock_ingestion.conn = conn

    backfill = MessageBackfill(
        tdlib_client=client,
        ingestion_controller=mock_ingestion,
        signal_parser=SignalParser(),
    )

    summary: BackfillSummary = backfill.execute_backfill(
        subscribed_channels=[chat1, chat2],
        reconciled_orders=reconciled_orders,
    )

    assert summary.total_channels == 2
    # chat1 got 2 messages (101, 102), chat2 got 1 message (201)
    assert summary.total_messages_retrieved == 3
    assert summary.total_signals_parsed == 2  # XAUUSD and EURUSD
    assert summary.total_skipped_dup == 1     # XAUUSD was already open on broker
    assert summary.total_executable == 1      # EURUSD was fresh

    rep1 = summary.channel_reports[chat1]
    assert rep1.highest_local_message_id == 100
    assert rep1.new_highest_message_id == 102
    assert rep1.signals_skipped_dup == 1
    assert rep1.signals_executable == 1

    rep2 = summary.channel_reports[chat2]
    assert rep2.highest_local_message_id == 200
    assert rep2.new_highest_message_id == 201
    assert rep2.signals_parsed == 0  # Chatter only


# =============================================================================
# 5. Gateway Integration Test
# =============================================================================

def test_telegram_gateway_message_backfill_integration():
    """Verifies get_or_create_message_backfill on TelegramGateway."""
    from engine.telegram_gateway import TelegramGateway, TelegramGatewayConfig
    from engine.vault import VaultStorage

    vault = VaultStorage(":memory:")
    client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    gateway = TelegramGateway(
        account_id="acc_test",
        vault=vault,
        client=client,
    )

    mb = gateway.get_or_create_message_backfill()
    assert isinstance(mb, MessageBackfill)
    assert gateway.message_backfill is mb

    # Subsequent call returns same instance
    assert gateway.get_or_create_message_backfill() is mb
