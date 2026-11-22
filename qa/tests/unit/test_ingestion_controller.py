"""
Unit tests validating IngestionController, SQLite RawMessage timeline,
cryptographic hash-chaining, edit/delete versioning, and WAL mode (Sprint 2 / ADR-002).
"""

import time
import sqlite3
import tempfile
import os
import pytest

from engine.ingestion_controller import (
    IngestionController,
    RawMessageRecord,
    compute_record_hash,
    GENESIS_HASH,
)
from engine.telegram_gateway import TelegramGateway
from engine.tdlib_client import MockTDLibClient
from engine.vault import VaultStorage


# =============================================================================
# HASH COMPUTATION & SCHEMA TESTS
# =============================================================================

@pytest.mark.unit
def test_compute_record_hash_determinism():
    """Verify hash computation is completely deterministic and matches canonical specification."""
    h1 = compute_record_hash(
        channel_id=-1001234567890,
        message_id=101,
        version=1,
        timestamp=1700000000000,
        is_deleted=0,
        raw_text="BUY XAUUSD 2650.50 SL 2640 TP 2670",
        media_reference=None,
        previous_hash=GENESIS_HASH,
    )
    assert len(h1) == 64
    assert isinstance(h1, str)

    # Identical inputs must yield identical hash
    h2 = compute_record_hash(
        channel_id=-1001234567890,
        message_id=101,
        version=1,
        timestamp=1700000000000,
        is_deleted=0,
        raw_text="BUY XAUUSD 2650.50 SL 2640 TP 2670",
        media_reference=None,
        previous_hash=GENESIS_HASH,
    )
    assert h1 == h2

    # Any field mutation must change hash
    h_mutated = compute_record_hash(
        channel_id=-1001234567890,
        message_id=101,
        version=1,
        timestamp=1700000000000,
        is_deleted=0,
        raw_text="BUY XAUUSD 2650.50 SL 2641 TP 2670",  # Changed SL
        media_reference=None,
        previous_hash=GENESIS_HASH,
    )
    assert h1 != h_mutated


@pytest.mark.unit
def test_ingestion_controller_schema_and_wal_mode():
    """Verify tables, indexes, and WAL journal mode configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_ingestion.db")
        controller = IngestionController(db_path=db_path)

        with controller._get_connection() as conn:
            # Verify PRAGMAs
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            assert mode.lower() == "wal"

            # Verify tables exist
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
            assert "raw_messages" in tables
            assert "channel_timeline_meta" in tables

            # Verify indexes exist
            indexes = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index';").fetchall()]
            assert "idx_raw_messages_lookup" in indexes
            assert "idx_raw_messages_timeline" in indexes
            assert "idx_raw_messages_reply" in indexes
            assert "idx_raw_messages_hash" in indexes

        controller.close()


# =============================================================================
# NEW MESSAGE INGESTION & HASH-CHAINING TESTS
# =============================================================================

@pytest.mark.unit
def test_new_message_ingestion_and_chaining():
    """
    Verify new message ingestion establishes an unbroken cryptographic hash chain
    linked from GENESIS_HASH.
    """
    controller = IngestionController(db_path=":memory:")
    chat_id = -100999000111

    # 1. First message in channel (Genesis block)
    msg1 = {
        "id": 1,
        "chat_id": chat_id,
        "date": 1700000001,
        "content": {"text": {"text": "Signal 1: BUY EURUSD 1.0850"}},
    }
    rec1 = controller.handle_new_message(msg1)
    assert rec1.version == 1
    assert rec1.previous_hash == GENESIS_HASH
    assert rec1.raw_text == "Signal 1: BUY EURUSD 1.0850"

    # Verify channel_timeline_meta updated
    meta1 = controller.get_timeline_meta(chat_id)
    assert meta1["head_hash"] == rec1.hash
    assert meta1["total_events"] == 1
    assert meta1["last_message_id"] == 1

    # 2. Second message in channel
    msg2 = {
        "id": 2,
        "chat_id": chat_id,
        "date": 1700000005,
        "content": {"text": {"text": "Signal 2: SELL GBPUSD 1.3000"}},
    }
    rec2 = controller.handle_new_message(msg2)
    assert rec2.version == 1
    assert rec2.previous_hash == rec1.hash, "Message 2 must cryptographically link to Message 1 hash!"

    # 3. Third message in channel
    msg3 = {
        "id": 3,
        "chat_id": chat_id,
        "date": 1700000010,
        "content": {"text": {"text": "Signal 3: BUY XAUUSD 2650.00"}},
    }
    rec3 = controller.handle_new_message(msg3)
    assert rec3.previous_hash == rec2.hash, "Message 3 must cryptographically link to Message 2 hash!"

    meta3 = controller.get_timeline_meta(chat_id)
    assert meta3["head_hash"] == rec3.hash
    assert meta3["total_events"] == 3

    # Mathematical chain verification
    is_valid, reason = controller.verify_channel_integrity(chat_id)
    assert is_valid is True, f"Integrity check failed: {reason}"

    controller.close()


@pytest.mark.unit
def test_idempotent_new_message_ingestion():
    """Verify re-ingesting the exact same message does not duplicate records or break chain."""
    controller = IngestionController(db_path=":memory:")
    chat_id = -100555666777

    msg = {
        "id": 100,
        "chat_id": chat_id,
        "date": 1700000100,
        "content": {"text": {"text": "Idempotency Test Signal"}},
    }

    rec_a = controller.handle_new_message(msg)
    rec_b = controller.handle_new_message(msg)

    assert rec_a.hash == rec_b.hash
    assert rec_a.version == rec_b.version

    timeline = controller.get_channel_timeline(chat_id)
    assert len(timeline) == 1, "Duplicate message must not create a duplicate row"

    controller.close()


# =============================================================================
# EDIT & DELETE LOGIC TESTS (ZERO IN-PLACE MUTABILITY)
# =============================================================================

@pytest.mark.unit
def test_message_edit_versioning_and_chaining():
    """
    Verify updateMessageEdited:
    - Appends a new row with incremented version.
    - Never overwrites historical version 1.
    - Maintains cryptographic chain continuity.
    """
    controller = IngestionController(db_path=":memory:")
    chat_id = -100111222333

    # 1. Initial post (Version 1)
    msg = {
        "id": 50,
        "chat_id": chat_id,
        "date": 1700000020,
        "content": {"text": {"text": "BUY EURUSD 1.0850 SL 1.0800 TP 1.0950"}},
    }
    rec_v1 = controller.handle_new_message(msg)
    assert rec_v1.version == 1

    # 2. Provider edits message (Move SL to breakeven)
    edit_event = {
        "@type": "updateMessageEdited",
        "chat_id": chat_id,
        "message_id": 50,
        "edit_date": 1700000080,
        "content": {"text": {"text": "BUY EURUSD 1.0850 SL 1.0850 (BE) TP 1.0950"}},
    }
    rec_v2 = controller.handle_message_edited(edit_event)

    assert rec_v2.version == 2
    assert rec_v2.message_id == 50
    assert rec_v2.raw_text == "BUY EURUSD 1.0850 SL 1.0850 (BE) TP 1.0950"
    assert rec_v2.previous_hash == rec_v1.hash, "Edit row must link to previous timeline head!"
    assert rec_v2.is_deleted == 0

    # 3. Provider edits message again (Version 3)
    edit_event_2 = {
        "@type": "updateMessageEdited",
        "chat_id": chat_id,
        "message_id": 50,
        "edit_date": 1700000120,
        "content": {"text": {"text": "BUY EURUSD 1.0850 TP 1 HIT! Secure 50% lots"}},
    }
    rec_v3 = controller.handle_message_edited(edit_event_2)
    assert rec_v3.version == 3
    assert rec_v3.previous_hash == rec_v2.hash

    # Verify all 3 versions are preserved in SQLite (Zero In-Place Mutability)
    versions = controller.get_message_versions(chat_id, 50)
    assert len(versions) == 3
    assert versions[0].version == 1 and versions[0].raw_text == "BUY EURUSD 1.0850 SL 1.0800 TP 1.0950"
    assert versions[1].version == 2 and "BE" in versions[1].raw_text
    assert versions[2].version == 3 and "HIT" in versions[2].raw_text

    # Verify latest message returns Version 3
    latest = controller.get_latest_message(chat_id, 50)
    assert latest.version == 3

    # Verify unbroken hash chain
    is_valid, _ = controller.verify_channel_integrity(chat_id)
    assert is_valid is True

    controller.close()


@pytest.mark.unit
def test_message_deletion_tombstone():
    """
    Verify updateDeleteMessages:
    - Appends a tombstone row with version = V + 1 and is_deleted = 1.
    - Historical rows are preserved.
    - Latest message query reflects is_deleted = 1.
    """
    controller = IngestionController(db_path=":memory:")
    chat_id = -100444555666

    # Ingest 2 messages
    controller.handle_new_message({
        "id": 10,
        "chat_id": chat_id,
        "date": 1700000001,
        "content": {"text": {"text": "Message 10 to keep"}},
    })
    rec_v1 = controller.handle_new_message({
        "id": 20,
        "chat_id": chat_id,
        "date": 1700000002,
        "content": {"text": {"text": "Losing trade signal to be deleted"}},
    })

    # Delete message 20
    delete_event = {
        "@type": "updateDeleteMessages",
        "chat_id": chat_id,
        "message_ids": [20],
        "is_permanent": True,
    }
    deleted_records = controller.handle_delete_messages(delete_event)
    assert len(deleted_records) == 1
    del_rec = deleted_records[0]

    assert del_rec.message_id == 20
    assert del_rec.version == 2
    assert del_rec.is_deleted == 1
    assert del_rec.deleted is True
    assert del_rec.raw_text == ""

    # Historical version 1 must still exist
    versions = controller.get_message_versions(chat_id, 20)
    assert len(versions) == 2
    assert versions[0].version == 1
    assert versions[0].is_deleted == 0
    assert versions[0].raw_text == "Losing trade signal to be deleted"

    # Latest version is flagged as deleted
    latest = controller.get_latest_message(chat_id, 20)
    assert latest.version == 2
    assert latest.is_deleted == 1

    # Cryptographic integrity must remain valid
    is_valid, reason = controller.verify_channel_integrity(chat_id)
    assert is_valid is True, f"Integrity failed: {reason}"

    controller.close()


# =============================================================================
# CRYPTOGRAPHIC TAMPER DETECTION TESTS (AUDIT TRAIL FR-9.5)
# =============================================================================

@pytest.mark.unit
def test_cryptographic_tamper_detection():
    """
    Verify that any retroactive tampering with text or historical data
    immediately breaks the cryptographic chain and is detected by verify_channel_integrity.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "tamper_test.db")
        controller = IngestionController(db_path=db_path)
        chat_id = -100777888999

        # Ingest 3 messages
        for i in range(1, 4):
            controller.handle_new_message({
                "id": i,
                "chat_id": chat_id,
                "date": 1700000000 + i,
                "content": {"text": {"text": f"Legitimate Signal {i}"}},
            })

        # Integrity check before tampering must pass
        valid_before, _ = controller.verify_channel_integrity(chat_id)
        assert valid_before is True

        # Malicious actor tampers with Message 1 in the database
        with controller._get_connection() as conn:
            conn.execute(
                "UPDATE raw_messages SET raw_text = 'Tampered text!' WHERE channel_id = ? AND message_id = 1",
                (chat_id,)
            )
            conn.commit()

        # Integrity verification must now FAIL!
        valid_after, error_msg = controller.verify_channel_integrity(chat_id)
        assert valid_after is False
        assert "Internal hash mismatch" in error_msg

        controller.close()


# =============================================================================
# GATEWAY & TDLib UPDATE EVENT LISTENER INTEGRATION TESTS
# =============================================================================

@pytest.mark.unit
def test_gateway_event_listener_integration():
    """
    Verify IngestionController attached to TelegramGateway automatically captures
    updateNewMessage, updateMessageEdited, and updateDeleteMessages.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=12345,
        api_hash="mock_hash",
        vault=vault,
        tdlib_client=mock_client,
    )

    controller = IngestionController(db_path=":memory:")
    controller.attach_gateway(gateway)

    target_chat = -100123999999
    gateway.subscribe_chat(target_chat)
    gateway.start()
    time.sleep(0.05)

    now = int(time.time())

    # 1. Simulate updateNewMessage from TDLib
    mock_client.simulate_incoming_update({
        "@type": "updateNewMessage",
        "message": {
            "id": 1001,
            "chat_id": target_chat,
            "date": now,
            "content": {
                "@type": "messageText",
                "text": {"@type": "formattedText", "text": "BUY GBPJPY 195.50 SL 194.80 TP 196.50"}
            }
        }
    })
    time.sleep(0.1)

    rec1 = controller.get_latest_message(target_chat, 1001)
    assert rec1 is not None
    assert rec1.version == 1
    assert "GBPJPY" in rec1.raw_text

    # 2. Simulate updateMessageEdited from TDLib
    mock_client.simulate_incoming_update({
        "@type": "updateMessageEdited",
        "chat_id": target_chat,
        "message_id": 1001,
        "edit_date": now + 30,
        "content": {
            "@type": "messageText",
            "text": {"@type": "formattedText", "text": "BUY GBPJPY 195.50 SL 195.50 (BE) TP 196.50"}
        }
    })
    time.sleep(0.1)

    rec2 = controller.get_latest_message(target_chat, 1001)
    assert rec2 is not None
    assert rec2.version == 2
    assert "BE" in rec2.raw_text

    # 3. Simulate updateDeleteMessages from TDLib
    mock_client.simulate_incoming_update({
        "@type": "updateDeleteMessages",
        "chat_id": target_chat,
        "message_ids": [1001],
        "is_permanent": True,
    })
    time.sleep(0.1)

    rec3 = controller.get_latest_message(target_chat, 1001)
    assert rec3 is not None
    assert rec3.version == 3
    assert rec3.is_deleted == 1
    assert rec3.deleted is True

    # Complete audit trail check
    is_valid, _ = controller.verify_channel_integrity(target_chat)
    assert is_valid is True

    gateway.stop()
    controller.close()


# =============================================================================
# HIGH-THROUGHPUT BURST TEST (WAL MODE CONCURRENCY)
# =============================================================================

@pytest.mark.unit
def test_high_throughput_message_burst():
    """Verify WAL mode handles rapid burst insertions without database locking errors."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "burst_test.db")
        controller = IngestionController(db_path=db_path)
        chat_id = -100999111222

        burst_count = 50
        t0 = time.perf_counter()

        for i in range(1, burst_count + 1):
            msg = {
                "id": i,
                "chat_id": chat_id,
                "date": 1700000000 + i,
                "content": {"text": {"text": f"Burst Signal #{i}: BUY XAUUSD 2650.{i}"}},
            }
            rec = controller.handle_new_message(msg)
            assert rec.version == 1

        elapsed = time.perf_counter() - t0
        avg_ms = (elapsed / burst_count) * 1000.0

        # Verify all 50 messages ingested and chained
        timeline = controller.get_channel_timeline(chat_id, limit=100)
        assert len(timeline) == burst_count

        # Verify integrity
        is_valid, _ = controller.verify_channel_integrity(chat_id)
        assert is_valid is True

        controller.close()
