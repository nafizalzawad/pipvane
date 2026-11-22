"""
Comprehensive IngestionController and Edit Tracking Test Suite (ADR-002 / FR-2.7 / FR-7.1).

Validates:
1. Edit Tracking Suite: Mocks a TDLib event stream with 1 updateNewMessage,
   followed by 2 updateMessageEdited events, and 1 updateDeleteMessages event for the same ID.
2. Database Assertion: Querying SQLite for that message_id returns exactly 3 versions,
   with the final version flagged as deleted (is_deleted == 1), and unbroken cryptographic hashes.
3. 100% test coverage on engine.ingestion_controller.
"""

import time
import pytest
import sqlite3
from unittest.mock import MagicMock

from engine.ingestion_controller import (
    IngestionController,
    ChannelTimelineMeta,
    compute_record_hash,
    _extract_text_from_content,
    GENESIS_HASH,
    IngestionError,
    IntegrityError,
)


# =============================================================================
# PRIMARY EDIT TRACKING & DATABASE ASSERTION SUITE (PROMPT REQUIREMENT)
# =============================================================================

@pytest.mark.unit
def test_edit_tracking_and_deletion_database_assertion():
    """
    Validates TDLib event stream:
    - 1 updateNewMessage (v1)
    - 2 updateMessageEdited (v2, v3)
    - 1 updateDeleteMessages for the same ID
    Database assertions:
    - Exactly 3 versions returned for message_id.
    - Final version (v3) flagged as deleted (is_deleted == 1).
    - Unbroken cryptographic hash chain across all 3 versions.
    """
    controller = IngestionController(db_path=":memory:", tombstone_mode="flag_latest")
    channel_id = -1001234567890
    message_id = 9999

    now = int(time.time())

    # 1. First event: updateNewMessage
    event_new = {
        "@type": "updateNewMessage",
        "message": {
            "id": message_id,
            "chat_id": channel_id,
            "date": now,
            "content": {
                "@type": "messageText",
                "text": {"@type": "formattedText", "text": "Signal: BUY XAUUSD 2650.00 SL 2640.00 TP 2670.00"}
            }
        }
    }
    rec_v1 = controller.handle_tdlib_event(event_new)
    assert rec_v1 is not None
    assert rec_v1.version == 1
    assert rec_v1.is_deleted == 0
    assert rec_v1.previous_hash == GENESIS_HASH

    # 2. Second event: updateMessageEdited (Version 2)
    event_edit_1 = {
        "@type": "updateMessageEdited",
        "chat_id": channel_id,
        "message_id": message_id,
        "edit_date": now + 60,
        "content": {
            "@type": "messageText",
            "text": {"@type": "formattedText", "text": "Signal Update 1: BUY XAUUSD 2650.00 SL 2645.00 TP 2670.00 (Move SL)"}
        }
    }
    rec_v2 = controller.handle_tdlib_event(event_edit_1)
    assert rec_v2 is not None
    assert rec_v2.version == 2
    assert rec_v2.is_deleted == 0
    assert rec_v2.previous_hash == rec_v1.hash

    # 3. Third event: updateMessageEdited (Version 3)
    event_edit_2 = {
        "@type": "updateMessageEdited",
        "chat_id": channel_id,
        "message_id": message_id,
        "edit_date": now + 120,
        "content": {
            "@type": "messageText",
            "text": {"@type": "formattedText", "text": "Signal Update 2: BUY XAUUSD 2650.00 SL 2650.00 TP 2680.00 (Breakeven)"}
        }
    }
    rec_v3 = controller.handle_tdlib_event(event_edit_2)
    assert rec_v3 is not None
    assert rec_v3.version == 3
    assert rec_v3.is_deleted == 0
    assert rec_v3.previous_hash == rec_v2.hash

    # 4. Fourth event: updateDeleteMessages for the same ID
    event_delete = {
        "@type": "updateDeleteMessages",
        "chat_id": channel_id,
        "message_ids": [message_id],
        "is_permanent": True,
    }
    del_records = controller.handle_tdlib_event(event_delete)
    assert del_records is not None
    assert len(del_records) == 1
    del_rec = del_records[0]
    assert del_rec.message_id == message_id
    assert del_rec.version == 3
    assert del_rec.is_deleted == 1
    assert del_rec.deleted is True

    # =========================================================================
    # STRICT DATABASE ASSERTIONS
    # =========================================================================

    # 1. Query SQLite mock database directly
    with controller._get_connection() as conn:
        rows = conn.execute("""
            SELECT channel_id, message_id, version, raw_text, is_deleted, previous_hash, hash
            FROM raw_messages
            WHERE channel_id = ? AND message_id = ?
            ORDER BY version ASC;
        """, (channel_id, message_id)).fetchall()

    # ASSERTION A: Querying SQLite returns EXACTLY 3 versions
    assert len(rows) == 3, f"Expected exactly 3 versions, found {len(rows)}"

    # ASSERTION B: Version inspection and final version flagged as deleted
    v1_row, v2_row, v3_row = rows[0], rows[1], rows[2]

    assert v1_row["version"] == 1
    assert v1_row["is_deleted"] == 0
    assert "Signal: BUY XAUUSD" in v1_row["raw_text"]

    assert v2_row["version"] == 2
    assert v2_row["is_deleted"] == 0
    assert "Move SL" in v2_row["raw_text"]

    assert v3_row["version"] == 3
    assert v3_row["is_deleted"] == 1, "Final version 3 must be strictly flagged as deleted (is_deleted == 1)!"

    # Also assert via get_message_versions
    versions = controller.get_message_versions(channel_id, message_id)
    assert len(versions) == 3
    assert versions[0].version == 1 and versions[0].is_deleted == 0 and versions[0].deleted is False
    assert versions[1].version == 2 and versions[1].is_deleted == 0 and versions[1].deleted is False
    assert versions[2].version == 3 and versions[2].is_deleted == 1 and versions[2].deleted is True

    # ASSERTION C: Cryptographic hashes form an unbroken chain
    # Genesis link
    assert versions[0].previous_hash == GENESIS_HASH
    # Version 1 -> Version 2 link
    assert versions[1].previous_hash == versions[0].hash
    # Version 2 -> Version 3 link
    assert versions[2].previous_hash == versions[1].hash

    # Verify mathematical hash accuracy for all 3 records
    for v in versions:
        computed = compute_record_hash(
            channel_id=v.channel_id,
            message_id=v.message_id,
            version=v.version,
            timestamp=v.timestamp,
            is_deleted=v.is_deleted,
            raw_text=v.raw_text,
            media_reference=v.media_reference,
            previous_hash=v.previous_hash,
        )
        assert v.hash == computed, f"Hash for version {v.version} does not match canonical payload computation!"

    # ASSERTION D: Full channel integrity check
    is_valid, reason = controller.verify_channel_integrity(channel_id)
    assert is_valid is True, f"Cryptographic integrity chain failed: {reason}"
    assert controller.verify_channel_chain(channel_id) is True

    # ASSERTION E: Channel head points to final version
    meta = controller.get_timeline_meta(channel_id)
    assert meta["head_hash"] == versions[2].hash
    assert meta["total_events"] == 3
    assert meta["last_message_id"] == message_id

    controller.close()


# =============================================================================
# EXHAUSTIVE BRANCH & STATEMENT COVERAGE TESTS
# =============================================================================

@pytest.mark.unit
def test_extract_text_from_content_all_variants():
    """Verify _extract_text_from_content handles all valid and edge-case inputs."""
    assert _extract_text_from_content("plain text") == "plain text"
    assert _extract_text_from_content(None) == ""
    assert _extract_text_from_content(12345) == ""
    assert _extract_text_from_content({"text": "string text"}) == "string text"
    assert _extract_text_from_content({"text": {"text": "nested text"}}) == "nested text"
    assert _extract_text_from_content({"text": 42}) == ""
    assert _extract_text_from_content({"other_key": "val"}) == ""


@pytest.mark.unit
def test_compute_record_hash_media_reference():
    """Verify compute_record_hash handles media_reference present vs absent."""
    h_none = compute_record_hash(1, 2, 1, 1000, 0, "txt", None, GENESIS_HASH)
    h_empty = compute_record_hash(1, 2, 1, 1000, 0, "txt", "", GENESIS_HASH)
    assert h_none == h_empty

    h_media = compute_record_hash(1, 2, 1, 1000, 0, "txt", "photo_file_id_123", GENESIS_HASH)
    assert h_media != h_none


@pytest.mark.unit
def test_context_manager_and_meta_dataclass():
    """Verify IngestionController context manager protocol and ChannelTimelineMeta."""
    with IngestionController(db_path=":memory:") as ctrl:
        assert ctrl.conn is not None
        rec = ctrl.handle_new_message(
            channel_id=10,
            message_id=1,
            raw_text="Test",
            timestamp=1000,
        )
        assert rec.version == 1

    # ChannelTimelineMeta dataclass instantiation
    meta = ChannelTimelineMeta(
        channel_id=10,
        head_hash=rec.hash,
        total_events=1,
        last_message_id=1,
        last_timestamp=1000,
        updated_at=2000,
    )
    assert meta.channel_id == 10
    assert meta.total_events == 1


@pytest.mark.unit
def test_handle_new_message_invocation_styles():
    """Verify handle_new_message supports positional args, kwargs, and dict with fallback text."""
    ctrl = IngestionController(db_path=":memory:")
    chat_id = -100222333444

    # 1. Dict with "text" top-level key fallback
    msg_dict_fallback = {
        "channel_id": chat_id,
        "message_id": 1,
        "text": "Fallback text format",
        "timestamp": 1700000000000,
    }
    r1 = ctrl.handle_new_message(msg_dict_fallback)
    assert r1.raw_text == "Fallback text format"

    # 2. Positional arguments
    r2 = ctrl.handle_new_message(
        chat_id, 2, "Positional text", 1700000001000, "media_ref_1", 1, 999, 1700000002000
    )
    assert r2.message_id == 2
    assert r2.media_reference == "media_ref_1"
    assert r2.reply_to_message_id == 1
    assert r2.sender_id == 999

    # 3. Explicit keyword arguments with seconds timestamp (< 1e10)
    r3 = ctrl.handle_new_message(
        channel_id=chat_id,
        message_id=3,
        raw_text="Kwargs text",
        timestamp=1700000005,  # in seconds
        media_reference=None,
        reply_to_message_id=None,
        sender_id=None,
    )
    assert r3.message_id == 3
    assert r3.timestamp == 1700000005000  # multiplied to ms

    ctrl.close()


@pytest.mark.unit
def test_handle_message_edited_invocation_styles():
    """Verify handle_message_edited supports positional args, kwargs, dict with text key, and unseen message."""
    ctrl = IngestionController(db_path=":memory:")
    chat_id = -100333444555

    # 1. Edit a message that was NEVER ingested before (unseen initial message, max_v = 0 -> v1)
    r_unseen = ctrl.handle_message_edited(
        channel_id=chat_id,
        message_id=100,
        new_raw_text="Unseen edit message creates version 1",
        timestamp=1700000000000,
    )
    assert r_unseen.version == 1
    assert r_unseen.previous_hash == GENESIS_HASH

    # 2. Positional arguments on existing message
    r_pos = ctrl.handle_message_edited(
        chat_id, 100, "Positional edit version 2", 1700000010000, "media_2", 1700000011000
    )
    assert r_pos.version == 2
    assert r_pos.raw_text == "Positional edit version 2"
    assert r_pos.previous_hash == r_unseen.hash

    # 3. Dict input with "text" top-level fallback
    edit_dict_fallback = {
        "channel_id": chat_id,
        "message_id": 100,
        "text": "Dict fallback edit version 3",
        "timestamp": 1700000020,  # seconds
    }
    r_dict = ctrl.handle_message_edited(edit_dict_fallback)
    assert r_dict.version == 3
    assert r_dict.raw_text == "Dict fallback edit version 3"

    ctrl.close()


@pytest.mark.unit
def test_handle_delete_messages_invocation_styles():
    """Verify handle_delete_messages supports positional args, kwargs, flag_latest override, and non-existent IDs."""
    ctrl = IngestionController(db_path=":memory:", tombstone_mode="append_version")
    chat_id = -100444555666

    ctrl.handle_new_message(channel_id=chat_id, message_id=5, raw_text="To be deleted", timestamp=1000)

    # 1. Non-existent message deletion: gracefully ignored
    res_non_existent = ctrl.handle_delete_messages(chat_id, [999999])
    assert len(res_non_existent) == 0

    # 2. Positional arguments deletion (append_version)
    del_pos = ctrl.handle_delete_messages(chat_id, [5], 2000)
    assert len(del_pos) == 1
    assert del_pos[0].version == 2
    assert del_pos[0].is_deleted == 1

    # 3. Kwargs with flag_latest=True override
    ctrl.handle_new_message(channel_id=chat_id, message_id=6, raw_text="Flag latest test", timestamp=3000)
    del_flag = ctrl.handle_delete_messages(
        channel_id=chat_id,
        message_ids=[6],
        timestamp=4000,
        flag_latest=True,
    )
    assert len(del_flag) == 1
    assert del_flag[0].version == 1
    assert del_flag[0].is_deleted == 1

    ctrl.close()


@pytest.mark.unit
def test_unknown_tdlib_event_and_gateway_attachment():
    """Verify handle_tdlib_event handles unrecognized events and attach_gateway handles objects without on_update."""
    ctrl = IngestionController(db_path=":memory:")

    # Unknown event returns None
    assert ctrl.handle_tdlib_event({"@type": "updateUserStatus"}) is None

    # Dummy gateway without on_update doesn't crash
    dummy_obj = object()
    ctrl.attach_gateway(dummy_obj)

    ctrl.close()


@pytest.mark.unit
def test_unqueried_or_nonexistent_records():
    """Verify queries on non-existent channels/messages return empty/genesis values."""
    ctrl = IngestionController(db_path=":memory:")

    # Channel head for non-existent channel
    head_hash, total = ctrl.get_channel_head(-999)
    assert head_hash == GENESIS_HASH
    assert total == 0

    # Meta for non-existent channel
    meta = ctrl.get_timeline_meta(-999)
    assert meta["channel_id"] == -999
    assert meta["head_hash"] == GENESIS_HASH
    assert meta["total_events"] == 0

    # Non-existent message
    assert ctrl.get_latest_message(-999, 123) is None
    assert ctrl.get_latest_version(-999, 123) is None

    # Empty channel integrity
    is_valid, msg = ctrl.verify_channel_integrity(-999)
    assert is_valid is True
    assert "empty" in msg

    ctrl.close()


@pytest.mark.unit
def test_verify_channel_integrity_tampering_paths():
    """Verify all failure branches in verify_channel_integrity (genesis, chain link, head hash)."""
    ctrl = IngestionController(db_path=":memory:")
    chat_id = -100555777888

    # Ingest 2 messages
    ctrl.handle_new_message(channel_id=chat_id, message_id=1, raw_text="Msg 1", timestamp=1000)
    ctrl.handle_new_message(channel_id=chat_id, message_id=2, raw_text="Msg 2", timestamp=2000)

    # 1. Test Genesis previous_hash corruption on row 0
    with ctrl._get_connection() as conn:
        conn.execute(
            "UPDATE raw_messages SET previous_hash = 'corrupted_genesis_prev' WHERE channel_id = ? AND message_id = 1",
            (chat_id,)
        )
        conn.commit()
    valid_genesis, err_genesis = ctrl.verify_channel_integrity(chat_id)
    assert valid_genesis is False
    # Will fail internal hash mismatch first because previous_hash is part of hash!
    assert "Internal hash mismatch" in err_genesis

    # 2. Reset and manually craft a valid row hash but broken genesis previous_hash
    with ctrl._get_connection() as conn:
        row1 = conn.execute("SELECT * FROM raw_messages WHERE channel_id = ? AND message_id = 1", (chat_id,)).fetchone()
        ts1 = row1["timestamp"]
        corrupted_prev = "1" * 64
        h_crafted = compute_record_hash(chat_id, 1, 1, ts1, 0, "Msg 1", None, corrupted_prev)
        conn.execute(
            "UPDATE raw_messages SET previous_hash = ?, hash = ? WHERE channel_id = ? AND message_id = 1",
            (corrupted_prev, h_crafted, chat_id)
        )
        conn.commit()
    valid_g2, err_g2 = ctrl.verify_channel_integrity(chat_id)
    assert valid_g2 is False
    assert "Genesis previous_hash mismatch" in err_g2

    # 3. Test broken link at index 1 (row 1 previous_hash != row 0 hash)
    with ctrl._get_connection() as conn:
        row2 = conn.execute("SELECT * FROM raw_messages WHERE channel_id = ? AND message_id = 2", (chat_id,)).fetchone()
        ts2 = row2["timestamp"]
        # Restore row 0
        h0 = compute_record_hash(chat_id, 1, 1, ts1, 0, "Msg 1", None, GENESIS_HASH)
        conn.execute(
            "UPDATE raw_messages SET previous_hash = ?, hash = ? WHERE channel_id = ? AND message_id = 1",
            (GENESIS_HASH, h0, chat_id)
        )
        # Craft row 1 with invalid prev_hash
        bad_prev = "2" * 64
        h1 = compute_record_hash(chat_id, 2, 1, ts2, 0, "Msg 2", None, bad_prev)
        conn.execute(
            "UPDATE raw_messages SET previous_hash = ?, hash = ? WHERE channel_id = ? AND message_id = 2",
            (bad_prev, h1, chat_id)
        )
        conn.commit()
    valid_link, err_link = ctrl.verify_channel_integrity(chat_id)
    assert valid_link is False
    assert "Previous hash mismatch at index 1" in err_link

    # 4. Test head_hash mismatch in timeline meta table
    with ctrl._get_connection() as conn:
        # Restore row 1 linked correctly to row 0
        h1_ok = compute_record_hash(chat_id, 2, 1, ts2, 0, "Msg 2", None, h0)
        conn.execute(
            "UPDATE raw_messages SET previous_hash = ?, hash = ? WHERE channel_id = ? AND message_id = 2",
            (h0, h1_ok, chat_id)
        )
        # Corrupt timeline meta head_hash
        conn.execute(
            "UPDATE channel_timeline_meta SET head_hash = 'corrupted_head' WHERE channel_id = ?",
            (chat_id,)
        )
        conn.commit()
    valid_head, err_head = ctrl.verify_channel_integrity(chat_id)
    assert valid_head is False
    assert "Timeline head hash mismatch" in err_head

    ctrl.close()


@pytest.mark.unit
def test_exceptions_and_double_close():
    """Verify exception classes can be instantiated and double close does not raise."""
    e1 = IngestionError("Ingestion failed")
    e2 = IntegrityError("Tampering detected")
    assert isinstance(e1, Exception)
    assert isinstance(e2, IngestionError)

    ctrl = IngestionController(db_path=":memory:")
    ctrl.close()
    ctrl.close()  # Must not throw

    # Test close exception handling branch
    ctrl2 = IngestionController(db_path=":memory:")
    ctrl2.conn = MagicMock()
    ctrl2.conn.close.side_effect = sqlite3.Error("Mocked close failure")
    ctrl2.close()  # Must gracefully swallow exception


@pytest.mark.unit
def test_pragma_operational_error_handling(monkeypatch):
    """Verify pragma OperationalError in _init_database is caught and handled."""
    real_conn = sqlite3.connect(":memory:", check_same_thread=False)

    class MockCursor:
        def __init__(self, c):
            self._c = c

        def execute(self, sql, *a):
            if "PRAGMA journal_mode" in sql:
                raise sqlite3.OperationalError("Simulated pragma unsupported in memory")
            return self._c.execute(sql, *a)

        def fetchone(self):
            return self._c.fetchone()

        def fetchall(self):
            return self._c.fetchall()

    class MockConnection:
        def __init__(self, r):
            self._r = r
            self.row_factory = None

        def cursor(self):
            return MockCursor(self._r.cursor())

        def commit(self):
            return self._r.commit()

        def close(self):
            return self._r.close()

    monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: MockConnection(real_conn))
    ctrl = IngestionController(db_path=":memory:")
    assert ctrl.conn is not None
    ctrl.close()
