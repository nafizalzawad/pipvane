"""
Pipvane v3 Ingestion Controller & SQLite RawMessage Timeline (FR-2.7, FR-7.1, ADR-002).

Manages the immutable, cryptographically chained audit log of all Telegram message
events across monitored channels:
1. Append-only version tracking for message posts, edits, and deletions.
2. Cryptographic SHA-256 hash chaining (previous_hash -> hash) preventing tampering.
3. Native SQLite integration with WAL journal mode and low-latency traversal.
4. TDLib event stream processing (updateNewMessage, updateMessageEdited, updateDeleteMessages).
5. Gateway event listener integration and audit trail verification.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import time
import sqlite3
import hashlib
import logging
import contextlib
import threading
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger("pipvane.engine.ingestion_controller")

GENESIS_HASH: str = "0" * 64


class IngestionError(Exception):
    """Base exception for ingestion and timeline errors."""
    pass


class IntegrityError(IngestionError):
    """Raised when cryptographic hash chain is broken or tampered with."""
    pass


@dataclass(frozen=True)
class RawMessageRecord:
    """Represents an immutable version snapshot of a Telegram message."""
    channel_id: int
    message_id: int
    version: int
    raw_text: str
    media_reference: Optional[str]
    is_deleted: int
    timestamp: int
    received_at: int
    reply_to_message_id: Optional[int]
    sender_id: Optional[int]
    previous_hash: str
    hash: str

    @property
    def deleted(self) -> bool:
        """Convenience boolean property for deletion status."""
        return bool(self.is_deleted)


@dataclass(frozen=True)
class ChannelTimelineMeta:
    """Authoritative pointer to the current cryptographic head of a channel."""
    channel_id: int
    head_hash: str
    total_events: int
    last_message_id: int
    last_timestamp: int
    updated_at: int


def compute_message_hash(
    channel_id: int,
    message_id: int,
    version: int,
    timestamp: int,
    is_deleted: int,
    raw_text: str,
    media_reference: Optional[str],
    previous_hash: str,
) -> str:
    """
    Computes the canonical SHA-256 digest of a message record (ADR-002 Section 2.4).
    Payload = channel_id:message_id:version:timestamp:is_deleted:raw_text:media_ref:previous_hash
    """
    media_str = media_reference if media_reference is not None else ""
    canonical_payload = (
        f"{channel_id}:{message_id}:{version}:{timestamp}:{is_deleted}:"
        f"{raw_text}:{media_str}:{previous_hash}"
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


# Canonical alias
compute_record_hash = compute_message_hash


def _extract_text_from_content(content: Any) -> str:
    """Safely extracts raw text from various TDLib / Telegram content formats."""
    if isinstance(content, str):
        return content
    if not isinstance(content, dict):
        return ""
    text_val = content.get("text", "")
    if isinstance(text_val, str):
        return text_val
    if isinstance(text_val, dict):
        return str(text_val.get("text", ""))
    return ""


class IngestionController:
    """
    Controls raw Telegram message ingestion, versioned timeline persistence,
    and cryptographic integrity verification.
    """

    def __init__(
        self,
        db_path: str = ":memory:",
        tombstone_mode: str = "append_version",  # "append_version" or "flag_latest"
    ) -> None:
        self.db_path = db_path
        self.tombstone_mode = tombstone_mode
        self._lock = threading.RLock()
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_database()

    @property
    def executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Dedicated background thread pool for asynchronous non-blocking SQLite writes."""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=2,
                thread_name_prefix="PipvaneIngestionDB",
            )
        return self._executor

    def __enter__(self) -> IngestionController:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    @contextlib.contextmanager
    def _get_connection(self):
        """Context manager yielding the active SQLite connection."""
        yield self.conn

    def _init_database(self) -> None:
        """Initializes tables, indexes, and PRAGMAs per SPRINT_2_DATA_MODEL.md."""
        with self._lock:
            cur = self.conn.cursor()

            # Performance PRAGMAs
            try:
                cur.execute("PRAGMA journal_mode = WAL;")
                cur.execute("PRAGMA synchronous = NORMAL;")
                cur.execute("PRAGMA foreign_keys = ON;")
                cur.execute("PRAGMA busy_timeout = 5000;")
            except sqlite3.OperationalError:
                pass

            # Table: raw_messages
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_messages (
                    channel_id          INTEGER NOT NULL,
                    message_id          INTEGER NOT NULL,
                    version             INTEGER NOT NULL DEFAULT 1,
                    raw_text            TEXT NOT NULL,
                    media_reference     TEXT,
                    is_deleted          INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
                    timestamp           INTEGER NOT NULL,
                    received_at         INTEGER NOT NULL,
                    reply_to_message_id INTEGER,
                    sender_id           INTEGER,
                    previous_hash       TEXT NOT NULL,
                    hash                TEXT NOT NULL,
                    PRIMARY KEY (channel_id, message_id, version)
                );
            """)

            # Table: channel_timeline_meta
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channel_timeline_meta (
                    channel_id          INTEGER PRIMARY KEY,
                    head_hash           TEXT NOT NULL,
                    total_events        INTEGER NOT NULL DEFAULT 0,
                    last_message_id     INTEGER NOT NULL DEFAULT 0,
                    last_timestamp      INTEGER NOT NULL,
                    updated_at          INTEGER NOT NULL
                );
            """)

            # Indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_messages_lookup
                    ON raw_messages (channel_id, message_id, version DESC);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_messages_timeline
                    ON raw_messages (channel_id, timestamp ASC, version ASC);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_messages_reply
                    ON raw_messages (channel_id, reply_to_message_id)
                    WHERE reply_to_message_id IS NOT NULL;
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_messages_hash
                    ON raw_messages (hash);
            """)

            self.conn.commit()

    def _row_to_record(self, row: sqlite3.Row) -> RawMessageRecord:
        """Converts an SQLite row to a RawMessageRecord instance."""
        return RawMessageRecord(
            channel_id=row["channel_id"],
            message_id=row["message_id"],
            version=row["version"],
            raw_text=row["raw_text"],
            media_reference=row["media_reference"],
            is_deleted=row["is_deleted"],
            timestamp=row["timestamp"],
            received_at=row["received_at"],
            reply_to_message_id=row["reply_to_message_id"],
            sender_id=row["sender_id"],
            previous_hash=row["previous_hash"],
            hash=row["hash"],
        )

    def get_channel_head(self, channel_id: int) -> Tuple[str, int]:
        """Returns (head_hash, total_events) for a channel. Defaults to genesis if new."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT head_hash, total_events FROM channel_timeline_meta WHERE channel_id = ?;",
                (channel_id,),
            )
            row = cur.fetchone()
            if row:
                return row["head_hash"], row["total_events"]
            return GENESIS_HASH, 0

    def get_timeline_meta(self, channel_id: int) -> Dict[str, Any]:
        """Returns channel timeline metadata dictionary."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM channel_timeline_meta WHERE channel_id = ?;",
                (channel_id,),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            return {
                "channel_id": channel_id,
                "head_hash": GENESIS_HASH,
                "total_events": 0,
                "last_message_id": 0,
                "last_timestamp": 0,
                "updated_at": 0,
            }

    def handle_new_message(self, *args: Any, **kwargs: Any) -> RawMessageRecord:
        """
        Processes a newly published message event (version 1).
        Supports dict input (msg_dict) or explicit keyword arguments.
        Ensures idempotent insertion.
        """
        if args and isinstance(args[0], dict):
            msg = args[0]
            channel_id = msg.get("chat_id") or msg.get("channel_id")
            message_id = msg.get("id") or msg.get("message_id")
            raw_text = _extract_text_from_content(msg.get("content"))
            if not raw_text and "text" in msg:
                raw_text = str(msg["text"])
            date_val = msg.get("date") or msg.get("timestamp") or int(time.time())
            timestamp = int(date_val * 1000) if date_val < 10_000_000_000 else int(date_val)
            media_reference = msg.get("media_reference")
            reply_to = msg.get("reply_to_message_id")
            sender = msg.get("sender_id", {}).get("user_id") if isinstance(msg.get("sender_id"), dict) else msg.get("sender_id")
            received_at = msg.get("received_at")
        elif args:
            channel_id = args[0]
            message_id = args[1] if len(args) > 1 else kwargs.get("message_id")
            raw_text = args[2] if len(args) > 2 else kwargs.get("raw_text", "")
            timestamp = args[3] if len(args) > 3 else kwargs.get("timestamp", int(time.time() * 1000))
            media_reference = args[4] if len(args) > 4 else kwargs.get("media_reference")
            reply_to = args[5] if len(args) > 5 else kwargs.get("reply_to_message_id")
            sender = args[6] if len(args) > 6 else kwargs.get("sender_id")
            received_at = args[7] if len(args) > 7 else kwargs.get("received_at")
        else:
            channel_id = kwargs["channel_id"]
            message_id = kwargs["message_id"]
            raw_text = kwargs.get("raw_text", "")
            ts_val = kwargs.get("timestamp", int(time.time() * 1000))
            timestamp = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)
            media_reference = kwargs.get("media_reference")
            reply_to = kwargs.get("reply_to_message_id")
            sender = kwargs.get("sender_id")
            received_at = kwargs.get("received_at")

        with self._lock:
            cur = self.conn.cursor()

            # Idempotency check: if version 1 already exists, return existing record
            cur.execute(
                "SELECT * FROM raw_messages WHERE channel_id = ? AND message_id = ? AND version = 1;",
                (channel_id, message_id),
            )
            existing = cur.fetchone()
            if existing:
                return self._row_to_record(existing)

            rec_time = received_at if received_at is not None else int(time.time() * 1000)
            previous_hash, _ = self.get_channel_head(channel_id)
            version = 1
            is_deleted = 0

            record_hash = compute_message_hash(
                channel_id=channel_id,
                message_id=message_id,
                version=version,
                timestamp=timestamp,
                is_deleted=is_deleted,
                raw_text=raw_text,
                media_reference=media_reference,
                previous_hash=previous_hash,
            )

            cur.execute("""
                INSERT INTO raw_messages (
                    channel_id, message_id, version, raw_text, media_reference,
                    is_deleted, timestamp, received_at, reply_to_message_id, sender_id,
                    previous_hash, hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                channel_id, message_id, version, raw_text, media_reference,
                is_deleted, timestamp, rec_time, reply_to, sender,
                previous_hash, record_hash,
            ))

            cur.execute("""
                INSERT INTO channel_timeline_meta (
                    channel_id, head_hash, total_events, last_message_id, last_timestamp, updated_at
                ) VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    head_hash = excluded.head_hash,
                    total_events = channel_timeline_meta.total_events + 1,
                    last_message_id = MAX(channel_timeline_meta.last_message_id, excluded.last_message_id),
                    last_timestamp = excluded.last_timestamp,
                    updated_at = excluded.updated_at;
            """, (channel_id, record_hash, message_id, timestamp, rec_time))

            self.conn.commit()

            return RawMessageRecord(
                channel_id=channel_id,
                message_id=message_id,
                version=version,
                raw_text=raw_text,
                media_reference=media_reference,
                is_deleted=is_deleted,
                timestamp=timestamp,
                received_at=rec_time,
                reply_to_message_id=reply_to,
                sender_id=sender,
                previous_hash=previous_hash,
                hash=record_hash,
            )

    def handle_message_edited(self, *args: Any, **kwargs: Any) -> RawMessageRecord:
        """
        Processes a message edit event.
        Retrieves highest existing version, increments version counter, and chains hash.
        """
        if args and isinstance(args[0], dict):
            event = args[0]
            channel_id = event.get("chat_id") or event.get("channel_id")
            message_id = event.get("message_id")
            new_raw_text = _extract_text_from_content(event.get("content"))
            if not new_raw_text and "text" in event:
                new_raw_text = str(event["text"])
            edit_date = event.get("edit_date") or event.get("date") or event.get("timestamp") or int(time.time())
            timestamp = int(edit_date * 1000) if edit_date < 10_000_000_000 else int(edit_date)
            media_reference = event.get("media_reference")
            received_at = event.get("received_at")
        elif args:
            channel_id = args[0]
            message_id = args[1] if len(args) > 1 else kwargs.get("message_id")
            new_raw_text = args[2] if len(args) > 2 else kwargs.get("new_raw_text", "")
            ts_val = args[3] if len(args) > 3 else kwargs.get("timestamp", int(time.time() * 1000))
            timestamp = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)
            media_reference = args[4] if len(args) > 4 else kwargs.get("media_reference")
            received_at = args[5] if len(args) > 5 else kwargs.get("received_at")
        else:
            channel_id = kwargs["channel_id"]
            message_id = kwargs["message_id"]
            new_raw_text = kwargs.get("new_raw_text", "")
            ts_val = kwargs.get("timestamp", int(time.time() * 1000))
            timestamp = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)
            media_reference = kwargs.get("media_reference")
            received_at = kwargs.get("received_at")

        rec_time = received_at if received_at is not None else int(time.time() * 1000)

        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT MAX(version) as max_v, reply_to_message_id, sender_id "
                "FROM raw_messages WHERE channel_id = ? AND message_id = ?;",
                (channel_id, message_id),
            )
            row = cur.fetchone()
            if not row or row["max_v"] is None:
                max_v = 0
                reply_to = None
                sender = None
            else:
                max_v = row["max_v"]
                reply_to = row["reply_to_message_id"]
                sender = row["sender_id"]

            new_version = max_v + 1
            is_deleted = 0
            previous_hash, _ = self.get_channel_head(channel_id)

            record_hash = compute_message_hash(
                channel_id=channel_id,
                message_id=message_id,
                version=new_version,
                timestamp=timestamp,
                is_deleted=is_deleted,
                raw_text=new_raw_text,
                media_reference=media_reference,
                previous_hash=previous_hash,
            )

            cur.execute("""
                INSERT INTO raw_messages (
                    channel_id, message_id, version, raw_text, media_reference,
                    is_deleted, timestamp, received_at, reply_to_message_id, sender_id,
                    previous_hash, hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                channel_id, message_id, new_version, new_raw_text, media_reference,
                is_deleted, timestamp, rec_time, reply_to, sender,
                previous_hash, record_hash,
            ))

            cur.execute("""
                INSERT INTO channel_timeline_meta (
                    channel_id, head_hash, total_events, last_message_id, last_timestamp, updated_at
                ) VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    head_hash = excluded.head_hash,
                    total_events = channel_timeline_meta.total_events + 1,
                    last_message_id = MAX(channel_timeline_meta.last_message_id, excluded.last_message_id),
                    last_timestamp = excluded.last_timestamp,
                    updated_at = excluded.updated_at;
            """, (channel_id, record_hash, message_id, timestamp, rec_time))

            self.conn.commit()

            return RawMessageRecord(
                channel_id=channel_id,
                message_id=message_id,
                version=new_version,
                raw_text=new_raw_text,
                media_reference=media_reference,
                is_deleted=is_deleted,
                timestamp=timestamp,
                received_at=rec_time,
                reply_to_message_id=reply_to,
                sender_id=sender,
                previous_hash=previous_hash,
                hash=record_hash,
            )

    def handle_delete_messages(self, *args: Any, **kwargs: Any) -> List[RawMessageRecord]:
        """
        Processes message deletion events.
        If tombstone_mode == 'flag_latest', flags the latest version as deleted (is_deleted=1).
        If tombstone_mode == 'append_version', appends a new tombstone version row (version=V+1, is_deleted=1).
        """
        mode = kwargs.get("tombstone_mode", self.tombstone_mode)
        if kwargs.get("flag_latest") is True:
            mode = "flag_latest"

        if args and isinstance(args[0], dict):
            event = args[0]
            channel_id = event.get("chat_id") or event.get("channel_id")
            message_ids = event.get("message_ids", [])
            ts_val = event.get("timestamp") or event.get("date") or int(time.time() * 1000)
            del_time = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)
        elif args:
            channel_id = args[0]
            message_ids = args[1] if len(args) > 1 else kwargs.get("message_ids", [])
            ts_val = args[2] if len(args) > 2 else kwargs.get("timestamp", int(time.time() * 1000))
            del_time = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)
        else:
            channel_id = kwargs["channel_id"]
            message_ids = kwargs.get("message_ids", [])
            ts_val = kwargs.get("timestamp", int(time.time() * 1000))
            del_time = int(ts_val * 1000) if ts_val < 10_000_000_000 else int(ts_val)

        rec_time = int(time.time() * 1000)
        updated_records: List[RawMessageRecord] = []

        with self._lock:
            cur = self.conn.cursor()

            for mid in message_ids:
                cur.execute("""
                    SELECT * FROM raw_messages
                    WHERE channel_id = ? AND message_id = ?
                    ORDER BY version DESC LIMIT 1;
                """, (channel_id, mid))
                latest_row = cur.fetchone()
                if not latest_row:
                    continue

                if mode == "flag_latest":
                    ver = latest_row["version"]
                    raw_text = latest_row["raw_text"]
                    media_ref = latest_row["media_reference"]
                    prev_hash = latest_row["previous_hash"]
                    orig_ts = latest_row["timestamp"]

                    # Recalculate hash with is_deleted = 1
                    new_hash = compute_message_hash(
                        channel_id=channel_id,
                        message_id=mid,
                        version=ver,
                        timestamp=orig_ts,
                        is_deleted=1,
                        raw_text=raw_text,
                        media_reference=media_ref,
                        previous_hash=prev_hash,
                    )

                    cur.execute("""
                        UPDATE raw_messages
                        SET is_deleted = 1, hash = ?
                        WHERE channel_id = ? AND message_id = ? AND version = ?;
                    """, (new_hash, channel_id, mid, ver))

                    cur.execute("""
                        INSERT INTO channel_timeline_meta (
                            channel_id, head_hash, total_events, last_message_id, last_timestamp, updated_at
                        ) VALUES (?, ?, 1, ?, ?, ?)
                        ON CONFLICT(channel_id) DO UPDATE SET
                            head_hash = excluded.head_hash,
                            updated_at = excluded.updated_at;
                    """, (channel_id, new_hash, mid, orig_ts, rec_time))

                    record = RawMessageRecord(
                        channel_id=channel_id,
                        message_id=mid,
                        version=ver,
                        raw_text=raw_text,
                        media_reference=media_ref,
                        is_deleted=1,
                        timestamp=orig_ts,
                        received_at=latest_row["received_at"],
                        reply_to_message_id=latest_row["reply_to_message_id"],
                        sender_id=latest_row["sender_id"],
                        previous_hash=prev_hash,
                        hash=new_hash,
                    )
                    updated_records.append(record)

                else:
                    ver = latest_row["version"] + 1
                    prev_hash, _ = self.get_channel_head(channel_id)
                    tombstone_hash = compute_message_hash(
                        channel_id=channel_id,
                        message_id=mid,
                        version=ver,
                        timestamp=del_time,
                        is_deleted=1,
                        raw_text="",
                        media_reference=None,
                        previous_hash=prev_hash,
                    )
                    cur.execute("""
                        INSERT INTO raw_messages (
                            channel_id, message_id, version, raw_text, media_reference,
                            is_deleted, timestamp, received_at, reply_to_message_id, sender_id,
                            previous_hash, hash
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        channel_id, mid, ver, "", None,
                        1, del_time, rec_time, latest_row["reply_to_message_id"], latest_row["sender_id"],
                        prev_hash, tombstone_hash,
                    ))

                    cur.execute("""
                        INSERT INTO channel_timeline_meta (
                            channel_id, head_hash, total_events, last_message_id, last_timestamp, updated_at
                        ) VALUES (?, ?, 1, ?, ?, ?)
                        ON CONFLICT(channel_id) DO UPDATE SET
                            head_hash = excluded.head_hash,
                            total_events = channel_timeline_meta.total_events + 1,
                            updated_at = excluded.updated_at;
                    """, (channel_id, tombstone_hash, mid, del_time, rec_time))

                    record = RawMessageRecord(
                        channel_id=channel_id,
                        message_id=mid,
                        version=ver,
                        raw_text="",
                        media_reference=None,
                        is_deleted=1,
                        timestamp=del_time,
                        received_at=rec_time,
                        reply_to_message_id=latest_row["reply_to_message_id"],
                        sender_id=latest_row["sender_id"],
                        previous_hash=prev_hash,
                        hash=tombstone_hash,
                    )
                    updated_records.append(record)

            self.conn.commit()
            return updated_records

    def handle_tdlib_event(self, event: Dict[str, Any]) -> Optional[Any]:
        """
        Dispatches native TDLib JSON events:
        - updateNewMessage
        - updateMessageEdited
        - updateDeleteMessages
        """
        event_type = event.get("@type")

        if event_type == "updateNewMessage":
            msg = event.get("message", {})
            return self.handle_new_message(msg)

        elif event_type == "updateMessageEdited":
            return self.handle_message_edited(event)

        elif event_type == "updateDeleteMessages":
            return self.handle_delete_messages(event)

        return None

    # Alias for gateway attachment
    handle_tdlib_update = handle_tdlib_event

    def attach_gateway(self, gateway: Any) -> None:
        """Connects this IngestionController to a TelegramGateway instance."""
        if hasattr(gateway, "on_update"):
            gateway.on_update(self.handle_tdlib_event)

    def get_message_versions(self, channel_id: int, message_id: int) -> List[RawMessageRecord]:
        """Retrieves all versions of a specific message in ascending version order."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM raw_messages WHERE channel_id = ? AND message_id = ? ORDER BY version ASC;",
                (channel_id, message_id)
            )
            rows = cur.fetchall()
            return [self._row_to_record(r) for r in rows]

    def get_latest_message(self, channel_id: int, message_id: int) -> Optional[RawMessageRecord]:
        """Returns the most recent version of a message."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM raw_messages WHERE channel_id = ? AND message_id = ? ORDER BY version DESC LIMIT 1;",
                (channel_id, message_id)
            )
            r = cur.fetchone()
            if not r:
                return None
            return self._row_to_record(r)

    # Alias
    get_latest_version = get_latest_message

    def get_channel_timeline(self, channel_id: int, limit: int = 1000) -> List[RawMessageRecord]:
        """Returns ordered timeline of messages for a channel up to limit."""
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM raw_messages WHERE channel_id = ? ORDER BY rowid ASC LIMIT ?;",
                (channel_id, limit)
            )
            rows = cur.fetchall()
            return [self._row_to_record(r) for r in rows]

    def verify_channel_integrity(self, channel_id: int) -> Tuple[bool, str]:
        """
        Cryptographically validates that the hash chain for a channel is 100% intact:
        1. Each record's hash matches its canonical payload digest.
        2. Record[0].previous_hash == GENESIS_HASH.
        3. Record[i].previous_hash == Record[i-1].hash.
        4. Channel head hash matches the final record.
        """
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM raw_messages WHERE channel_id = ? ORDER BY rowid ASC;",
                (channel_id,)
            )
            rows = cur.fetchall()

        if not rows:
            return True, "Timeline is empty; hash chain intact"

        for i, row in enumerate(rows):
            expected_hash = compute_message_hash(
                channel_id=row["channel_id"],
                message_id=row["message_id"],
                version=row["version"],
                timestamp=row["timestamp"],
                is_deleted=row["is_deleted"],
                raw_text=row["raw_text"],
                media_reference=row["media_reference"],
                previous_hash=row["previous_hash"],
            )
            if row["hash"] != expected_hash:
                err = (
                    f"Internal hash mismatch on channel {channel_id}, "
                    f"message_id={row['message_id']}, version={row['version']}"
                )
                logger.error(err)
                return False, err

            if i == 0:
                if row["previous_hash"] != GENESIS_HASH:
                    err = f"Genesis previous_hash mismatch: expected {GENESIS_HASH}, got {row['previous_hash']}"
                    logger.error(err)
                    return False, err
            else:
                prev_record = rows[i - 1]
                if row["previous_hash"] != prev_record["hash"]:
                    err = (
                        f"Previous hash mismatch at index {i}: "
                        f"record.prev={row['previous_hash']}, expected {prev_record['hash']}"
                    )
                    logger.error(err)
                    return False, err

        # Verify against channel_timeline_meta
        cur.execute(
            "SELECT head_hash, total_events FROM channel_timeline_meta WHERE channel_id = ?;",
            (channel_id,),
        )
        meta = cur.fetchone()
        if meta and meta["head_hash"] != rows[-1]["hash"]:
            err = (
                f"Timeline head hash mismatch: meta.head={meta['head_hash']}, "
                f"last_record.hash={rows[-1]['hash']}"
            )
            logger.error(err)
            return False, err

        return True, "Chain intact and mathematically verified"

    def verify_channel_chain(self, channel_id: int) -> bool:
        """Boolean check for channel chain integrity."""
        return self.verify_channel_integrity(channel_id)[0]

    # -------------------------------------------------------------------------
    # ASYNC NON-BLOCKING LEDGER OPERATIONS (Sprint 21 Burst Queue Optimization)
    # -------------------------------------------------------------------------

    async def handle_new_message_async(self, *args: Any, **kwargs: Any) -> RawMessageRecord:
        """
        Sprint 21: Asynchronously records a new message in the append-only ledger
        via the background thread pool, preventing SQLite disk I/O from stalling the event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, lambda: self.handle_new_message(*args, **kwargs))

    async def handle_message_edited_async(self, *args: Any, **kwargs: Any) -> RawMessageRecord:
        """
        Sprint 21: Asynchronously records an edited message in the ledger.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, lambda: self.handle_message_edited(*args, **kwargs))

    async def handle_delete_messages_async(self, *args: Any, **kwargs: Any) -> List[RawMessageRecord]:
        """
        Sprint 21: Asynchronously records deletion tombstones in the ledger.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, lambda: self.handle_delete_messages(*args, **kwargs))

    async def handle_tdlib_event_async(self, event: Dict[str, Any]) -> Optional[Any]:
        """
        Sprint 21: Asynchronously handles a raw TDLib event off the event loop.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, lambda: self.handle_tdlib_event(event))

    def close(self) -> None:
        """Closes SQLite database connection cleanly and terminates executor."""
        with self._lock:
            if self._executor is not None:
                self._executor.shutdown(wait=False)
                self._executor = None
            try:
                self.conn.close()
            except Exception:
                pass
