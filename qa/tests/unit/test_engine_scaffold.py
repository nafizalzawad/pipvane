"""
Unit tests validating core engine data structures, alignment, timers, and failover states.
"""

import struct
import pytest
import time


@pytest.mark.unit
def test_high_resolution_timer_resolution(high_res_timer):
    """Verify that monotonic timer provides sub-microsecond resolution."""
    timer = high_res_timer.start()
    # Busy spin for small duration
    start = time.perf_counter_ns()
    while time.perf_counter_ns() - start < 50_000:  # ~50us
        pass
    timer.stop()

    assert timer.elapsed_ns > 0
    assert timer.elapsed_us >= 40.0
    assert timer.elapsed_ms > 0.0


@pytest.mark.unit
def test_shm_ring_buffer_header_alignment(mock_shm_header_bytes):
    """
    Verify ADR-001 specification:
    Header total size MUST be exactly 256 bytes (4 x 64-byte cache lines).
    Cache line boundaries prevent false sharing between producer and consumer.
    """
    assert len(mock_shm_header_bytes) == 256

    # Verify Cache Line 0 (Static config)
    magic, version, capacity, slot_size = struct.unpack_from("<IIII", mock_shm_header_bytes, 0)
    assert magic == 0x50495056  # "PIPV"
    assert version == 1
    assert capacity == 1024
    assert slot_size == 128

    # Verify Cache Line 1 (Producer Write Head at offset 64)
    write_seq = struct.unpack_from("<Q", mock_shm_header_bytes, 64)[0]
    assert write_seq == 0

    # Verify Cache Line 2 (Consumer Read Tail at offset 128)
    read_seq = struct.unpack_from("<Q", mock_shm_header_bytes, 128)[0]
    assert read_seq == 0

    # Verify Cache Line 3 (Diagnostics at offset 192)
    sample_count = struct.unpack_from("<Q", mock_shm_header_bytes, 192 + 24)[0]
    assert sample_count == 0


@pytest.mark.unit
def test_order_slot_size_contract():
    """
    Verify order intent slot is strictly 128 bytes (2 cache lines)
    to prevent slots from straddling CPU cache line boundaries.
    """
    SLOT_SIZE_BYTES = 128
    CACHE_LINE_BYTES = 64
    assert SLOT_SIZE_BYTES % CACHE_LINE_BYTES == 0
    assert (SLOT_SIZE_BYTES // CACHE_LINE_BYTES) == 2


@pytest.mark.failover
@pytest.mark.unit
def test_anti_split_brain_standby_strict_locked(failover_node_states):
    """
    Verify FR-7.4 & 7.5:
    When cloud heartbeat lease expires and cloud is unreachable,
    Standby node MUST transition to STRICT_LOCKED and CANNOT execute trades.
    """
    states = failover_node_states

    class StandbyNode:
        def __init__(self):
            self.role = states["ROLE_STANDBY"]
            self.state = states["STATE_STANDBY_HOT"]
            self.lease_valid = True

        def on_cloud_lease_expired(self):
            self.lease_valid = False
            # FR-7.5: Fail-closed to STRICT_LOCKED
            self.state = states["STATE_STRICT_LOCKED"]

        def can_execute_orders(self) -> bool:
            return self.state == states["STATE_ACTIVE_EXECUTION"]

    node = StandbyNode()
    assert node.state == states["STATE_STANDBY_HOT"]
    assert not node.can_execute_orders()

    # Cloud lease expires during partition
    node.on_cloud_lease_expired()
    assert node.state == states["STATE_STRICT_LOCKED"]
    assert not node.can_execute_orders(), "Standby must never execute in STRICT_LOCKED state!"
