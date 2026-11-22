"""
Pipvane v3 Shared Memory Ring Buffer Writer (ADR-001).

Manages high-throughput, sub-microsecond Single-Producer Single-Consumer (SPSC)
lock-free circular queue memory-mapped onto Windows Shared Memory (CreateFileMappingW / MapViewOfFile).
Writes 128-byte OrderIntent slots strictly aligned to 64-byte CPU cache line boundaries.
Signals Windows Event (SetEvent) as a low-overhead hybrid fallback for idle terminal threads.
"""

from __future__ import annotations

import os
import sys
import mmap
import time
import struct
import logging
from typing import Optional, Dict, Any

from engine.ipc_protocol import (
    SHM_MAGIC,
    SHM_VERSION,
    TOTAL_SLOTS,
    SLOT_SIZE,
    HEADER_SIZE,
    TOTAL_SHM_SIZE,
    OrderIntent,
    serialize_order_intent_into,
)

logger = logging.getLogger("pipvane.engine.shm_writer")

# Win32 Kernel32 Ctypes setup for OS event signaling
if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes
    _kernel32 = ctypes.windll.kernel32
else:
    _kernel32 = None  # type: ignore


class ShmWriterError(Exception):
    """Base exception for Shared Memory errors."""
    pass


class ShmQueueFullError(ShmWriterError):
    """Raised when the SPSC ring buffer is saturated and cannot accept new intents."""
    pass


class ShmRingBufferWriter:
    """
    Zero-copy lock-free ring buffer producer writing to Windows Shared Memory.
    Layout: 256-byte header + (1024 slots * 128 bytes) = 131,328 bytes.
    """

    def __init__(
        self,
        terminal_id: str,
        shm_name: Optional[str] = None,
        event_name: Optional[str] = None,
        use_mock_buffer: bool = False,
    ) -> None:
        self.terminal_id = terminal_id
        self.shm_name = shm_name or f"Local\\pipvane_shm_{terminal_id}"
        self.event_name = event_name or f"Local\\pipvane_event_{terminal_id}"
        self.use_mock_buffer = use_mock_buffer or (sys.platform != "win32")

        self.capacity: int = TOTAL_SLOTS
        self.slot_size: int = SLOT_SIZE
        self.header_size: int = HEADER_SIZE
        self.total_size: int = TOTAL_SHM_SIZE

        self._mmap: Optional[mmap.mmap] = None
        self._buf: Optional[bytearray] = None
        self._h_event: Optional[int] = None
        self._is_closed: bool = False

        self._init_shm()
        self._init_event()

    def _init_shm(self) -> None:
        """Allocates shared memory segment or in-memory bytearray."""
        if self.use_mock_buffer:
            self._buf = bytearray(self.total_size)
        else:
            try:
                # -1 maps system pagefile backed memory with Windows tagname
                self._mmap = mmap.mmap(-1, self.total_size, tagname=self.shm_name)
            except OSError as e:
                logger.warning(
                    "Failed to allocate named mmap '%s': %s. Falling back to mock bytearray.",
                    self.shm_name, e
                )
                self.use_mock_buffer = True
                self._buf = bytearray(self.total_size)

        self._init_header()

    def _init_event(self) -> None:
        """Creates or opens Win32 notification event for hybrid spin/wait consumer."""
        if sys.platform == "win32" and _kernel32 is not None and not self.use_mock_buffer:
            try:
                # CreateEventW(lpEventAttributes=None, bManualReset=False, bInitialState=False, lpName)
                h_event = _kernel32.CreateEventW(None, False, False, self.event_name)
                if h_event:
                    self._h_event = h_event
                    logger.debug("Initialized Win32 notification event: %s", self.event_name)
            except Exception as e:
                logger.warning("Could not create Win32 event '%s': %s", self.event_name, e)

    def _init_header(self) -> None:
        """Initializes the 256-byte header with static configuration and zeroed sequences."""
        buf = self._get_buffer()
        now_ns = time.time_ns()
        pid = os.getpid()

        # Cache Line 0 (Static config):
        # uint32 magic, uint32 version, uint32 capacity, uint32 slot_size,
        # uint32 flags (1=Active), uint32 producer_pid, uint32 consumer_pid, uint32 reserved,
        # uint64 heartbeat_ns, 24 bytes pad
        struct.pack_into(
            "<IIIIIIIIQ24s",
            buf,
            0,
            SHM_MAGIC,
            SHM_VERSION,
            self.capacity,
            self.slot_size,
            1,  # Active flag (Bit 0)
            pid,
            0,  # consumer_pid (to be populated by DLL)
            0,
            now_ns,
            b"\x00" * 24,
        )

        # Cache Line 1 (Producer write head at offset 64):
        # uint64 write_head, uint64 producer_dropped_intents, 48 bytes pad
        struct.pack_into("<QQ48s", buf, 64, 0, 0, b"\x00" * 48)

        # Cache Line 2 (Consumer read tail at offset 128):
        # uint64 read_tail, uint64 consumer_processed, uint64 consumer_latency_sum, 40 bytes pad
        struct.pack_into("<QQQ40s", buf, 128, 0, 0, 0, b"\x00" * 40)

        # Cache Line 3 (Signaling & Diagnostics at offset 192):
        # uint64 event_signal_count, uint64 last_consumed_seq, 48 bytes pad
        struct.pack_into("<QQ48s", buf, 192, 0, 0, b"\x00" * 48)

    def _get_buffer(self) -> bytearray | mmap.mmap:
        if self._mmap is not None:
            return self._mmap
        if self._buf is not None:
            return self._buf
        raise ShmWriterError("Shared memory buffer is uninitialized or closed")

    # --- Fast-Path SPSC Queue Push ---

    def push_order_intent(self, intent: OrderIntent) -> bool:
        """
        Dispatches an OrderIntent into the SPSC circular queue slot without heap allocations.
        Returns True on successful dispatch; False if the queue is full (overrun protection).
        """
        if self._is_closed:
            raise ShmWriterError("Cannot push to closed ShmRingBufferWriter")

        buf = self._get_buffer()

        # Atomic read of write_head (producer) and read_tail (consumer)
        # Offset 64: write_head, Offset 128: read_tail
        write_head = struct.unpack_from("<Q", buf, 64)[0]
        read_tail = struct.unpack_from("<Q", buf, 128)[0]

        # Overrun guard: if queue is full
        if write_head - read_tail >= self.capacity:
            # Increment dropped intents counter at offset 72
            dropped = struct.unpack_from("<Q", buf, 72)[0]
            struct.pack_into("<Q", buf, 72, dropped + 1)
            logger.error(
                "SHM Ring Buffer overrun on terminal %s (head=%d, tail=%d, capacity=%d)",
                self.terminal_id, write_head, read_tail, self.capacity
            )
            return False

        # Compute circular slot index (fast bitwise AND for power of 2)
        slot_idx = write_head & (self.capacity - 1)
        slot_offset = self.header_size + (slot_idx * self.slot_size)

        # Serialize OrderIntent directly into memory slot at slot_offset (128 bytes)
        serialize_order_intent_into(buf, slot_offset, intent)

        # Advance write_head sequence index (release semantic)
        struct.pack_into("<Q", buf, 64, write_head + 1)

        # Update heartbeat timestamp at offset 32
        struct.pack_into("<Q", buf, 32, time.time_ns())

        # Signal OS Event to awaken sleeping consumer thread
        self._signal_event(buf)

        return True

    def _signal_event(self, buf: bytearray | mmap.mmap) -> None:
        """Signals the Win32 Event and updates diagnostics counter."""
        if self._h_event and _kernel32 is not None:
            _kernel32.SetEvent(self._h_event)

        # Increment event_signal_count at offset 192
        ev_count = struct.unpack_from("<Q", buf, 192)[0]
        struct.pack_into("<Q", buf, 192, ev_count + 1)

    # --- Routing Control Flags ---

    def pause(self) -> None:
        """Pauses the ring buffer by setting Bit 1 in status flags."""
        if self._is_closed:
            return
        buf = self._get_buffer()
        flags = struct.unpack_from("<I", buf, 16)[0]
        struct.pack_into("<I", buf, 16, flags | 0x02)
        logger.info("Paused SHM ring buffer for terminal %s", self.terminal_id)

    def resume(self) -> None:
        """Resumes the ring buffer by clearing Bit 1 in status flags."""
        if self._is_closed:
            return
        buf = self._get_buffer()
        flags = struct.unpack_from("<I", buf, 16)[0]
        struct.pack_into("<I", buf, 16, flags & ~0x02)
        logger.info("Resumed SHM ring buffer for terminal %s", self.terminal_id)

    def is_paused(self) -> bool:
        """Returns True if terminal ring buffer is paused."""
        if self._is_closed:
            return True
        buf = self._get_buffer()
        flags = struct.unpack_from("<I", buf, 16)[0]
        return bool(flags & 0x02)

    # --- Diagnostics & Metrics ---

    def get_metrics(self) -> Dict[str, Any]:
        """Reads current diagnostic metrics from the SHM header."""
        buf = self._get_buffer()
        write_head = struct.unpack_from("<Q", buf, 64)[0]
        dropped = struct.unpack_from("<Q", buf, 72)[0]
        read_tail = struct.unpack_from("<Q", buf, 128)[0]
        processed = struct.unpack_from("<Q", buf, 136)[0]
        latency_sum = struct.unpack_from("<Q", buf, 144)[0]
        ev_count = struct.unpack_from("<Q", buf, 192)[0]

        queue_depth = write_head - read_tail
        avg_latency_us = (latency_sum / processed / 1000.0) if processed > 0 else 0.0

        return {
            "terminal_id": self.terminal_id,
            "write_head": write_head,
            "read_tail": read_tail,
            "queue_depth": queue_depth,
            "producer_dropped_intents": dropped,
            "consumer_processed_intents": processed,
            "avg_transit_latency_us": avg_latency_us,
            "event_signal_count": ev_count,
            "is_paused": self.is_paused(),
        }

    # --- Lifecycle ---

    def close(self) -> None:
        """Closes event handle and unmaps shared memory."""
        if self._is_closed:
            return
        self._is_closed = True

        if self._mmap is not None:
            try:
                # Mark Bit 2: Terminating in status flags
                flags = struct.unpack_from("<I", self._mmap, 16)[0]
                struct.pack_into("<I", self._mmap, 16, flags | 0x04)
                self._mmap.close()
            except Exception as e:
                logger.debug("Error closing mmap: %s", e)
            self._mmap = None

        if self._h_event and _kernel32 is not None:
            try:
                _kernel32.CloseHandle(self._h_event)
            except Exception as e:
                logger.debug("Error closing Win32 event handle: %s", e)
            self._h_event = None

        self._buf = None
        logger.info("Closed ShmRingBufferWriter for terminal %s", self.terminal_id)
