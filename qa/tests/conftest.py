"""
Pytest configuration and shared fixtures for Pipvane v3 QA testing suite.
Provides monotonic timers, mock shared memory layout fixtures, and failover states.
"""

import pytest
import time
import struct


@pytest.fixture
def high_res_timer():
    """Returns a monotonic high-resolution timer object measuring nanoseconds."""
    class HighResTimer:
        def __init__(self):
            self.start_ns = 0
            self.end_ns = 0

        def start(self):
            self.start_ns = time.perf_counter_ns()
            return self

        def stop(self):
            self.end_ns = time.perf_counter_ns()
            return self

        @property
        def elapsed_ns(self) -> int:
            return self.end_ns - self.start_ns

        @property
        def elapsed_us(self) -> float:
            return (self.end_ns - self.start_ns) / 1_000.0

        @property
        def elapsed_ms(self) -> float:
            return (self.end_ns - self.start_ns) / 1_000_000.0

    return HighResTimer()


@pytest.fixture
def mock_shm_header_bytes():
    """
    Returns 256 bytes formatted according to ADR-001 ShmRingBufferHeader layout:
    - 4 x 64-byte Cache Lines
    - Magic 0x50495056 ('PIPV')
    - Version 1
    - Capacity 1024
    - Slot Size 128
    """
    # Cache Line 0 (64 bytes):
    # uint32 magic (0x50495056), uint32 version (1), uint32 capacity (1024), uint32 slot_size (128)
    # uint32 flags (1), uint32 producer_pid (1234), uint32 consumer_pid (5678), uint32 pad (0)
    # uint64 heartbeat_ns (time.time_ns()), 24 bytes padding
    now_ns = time.time_ns()
    cl0 = struct.pack("<IIIIIIIIQ24s",
                      0x50495056, 1, 1024, 128,
                      1, 1234, 5678, 0,
                      now_ns, b"\x00" * 24)
    assert len(cl0) == 64

    # Cache Line 1 (64 bytes): Producer Write Head
    # uint64 write_sequence, uint64 write_failures, uint64 last_batch_id, 40 bytes pad
    cl1 = struct.pack("<QQQ40s", 0, 0, 0, b"\x00" * 40)
    assert len(cl1) == 64

    # Cache Line 2 (64 bytes): Consumer Read Tail
    # uint64 read_sequence, uint64 orders_executed, uint64 orders_rejected, 40 bytes pad
    cl2 = struct.pack("<QQQ40s", 0, 0, 0, b"\x00" * 40)
    assert len(cl2) == 64

    # Cache Line 3 (64 bytes): Latency & Diagnostics
    # uint64 min_latency_ns, uint64 max_latency_ns, uint64 total_latency_ns, uint64 sample_count, 32 bytes pad
    cl3 = struct.pack("<QQQQ32s", 0, 0, 0, 0, b"\x00" * 32)
    assert len(cl3) == 64

    return cl0 + cl1 + cl2 + cl3


@pytest.fixture
def failover_node_states():
    """State constants for anti-split-brain failover verification (FR-7.4 & 7.5)."""
    return {
        "ROLE_PRIMARY": "PRIMARY",
        "ROLE_STANDBY": "STANDBY",
        "STATE_ACTIVE_EXECUTION": "ACTIVE_EXECUTION",
        "STATE_STANDBY_HOT": "STANDBY_HOT",
        "STATE_STRICT_LOCKED": "STRICT_LOCKED",
        "STATE_FAIL_LIVE": "FAIL_LIVE",
    }
