"""
Unit tests validating IPCDispatcher, ShmRingBufferWriter, Named Pipe Framing,
OrderIntent serialization, and terminal fault handling (ADR-001).
"""

import time
import struct
import binascii
import asyncio
import pytest

from engine.ipc_protocol import (
    SHM_MAGIC,
    SHM_VERSION,
    TOTAL_SLOTS,
    SLOT_SIZE,
    HEADER_SIZE,
    TOTAL_SHM_SIZE,
    NAMED_PIPE_FRAME_MAGIC,
    OrderSide,
    ActionType,
    ExecutionFlags,
    NamedPipeOpcode,
    ExecutionStatus,
    TerminalStatus,
    OrderIntent,
    AccountState,
    InitHelloPayload,
    ExecutionFeedback,
    serialize_order_intent,
    serialize_order_intent_into,
    deserialize_order_intent,
    pack_pipe_frame,
    unpack_pipe_frame_header,
)
from engine.shm_writer import ShmRingBufferWriter
from engine.named_pipe_server import AsyncNamedPipeServer
from engine.ipc_dispatcher import IPCDispatcher


# =============================================================================
# ORDER INTENT SERIALIZATION TESTS (128 BYTES CONTRACT)
# =============================================================================

@pytest.mark.unit
def test_order_intent_struct_size_contract():
    """Verify ADR-001 contract: OrderIntent struct is strictly 128 bytes (2 cache lines)."""
    intent = OrderIntent(
        symbol="XAUUSD.raw",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.50,
        price=2650.50,
        stop_loss=2640.00,
        take_profit=2670.00,
        slippage_cap_pips=1.5,
        magic_number=123456,
        order_ticket=0,
        signal_id=9876543210,
        timestamp_ns=time.time_ns(),
        flags=ExecutionFlags.IOC | ExecutionFlags.ALLOW_PARTIAL,
    )
    raw = serialize_order_intent(intent)
    assert len(raw) == 128, f"OrderIntent size must be exactly 128 bytes, got {len(raw)}"


@pytest.mark.unit
def test_order_intent_roundtrip_and_crc32():
    """Verify OrderIntent serialization, deserialization, and CRC32 verification."""
    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.LIMIT_ENTRY,
        volume=1.25,
        price=1.08550,
        stop_loss=1.09000,
        take_profit=1.07500,
        slippage_cap_pips=2.0,
        magic_number=888999,
        order_ticket=12345678,
        signal_id=555123456,
        timestamp_ns=1700000000123456789,
        flags=ExecutionFlags.FOK,
    )
    raw = serialize_order_intent(intent)
    deserialized = deserialize_order_intent(raw)

    assert deserialized.symbol == "EURUSD"
    assert deserialized.side == OrderSide.SELL
    assert deserialized.action_type == ActionType.LIMIT_ENTRY
    assert pytest.approx(deserialized.volume, 1e-4) == 1.25
    assert pytest.approx(deserialized.price, 1e-5) == 1.08550
    assert pytest.approx(deserialized.stop_loss, 1e-5) == 1.09000
    assert pytest.approx(deserialized.take_profit, 1e-5) == 1.07500
    assert deserialized.magic_number == 888999
    assert deserialized.order_ticket == 12345678
    assert deserialized.signal_id == 555123456
    assert deserialized.flags == ExecutionFlags.FOK

    # Corrupting a single byte must trigger CRC32 validation error
    corrupted = bytearray(raw)
    corrupted[10] ^= 0xFF
    with pytest.raises(ValueError) as exc:
        deserialize_order_intent(corrupted)
    assert "CRC32 mismatch" in str(exc.value)


@pytest.mark.unit
def test_serialize_order_intent_into_buffer():
    """Verify zero-overhead in-place serialization into preallocated buffer."""
    buf = bytearray(256)
    offset = 64
    intent = OrderIntent(
        symbol="GBPUSD",
        side=OrderSide.BUY,
        volume=0.10,
        price=1.3000,
    )
    serialize_order_intent_into(buf, offset, intent)
    deserialized = deserialize_order_intent(buf, offset)
    assert deserialized.symbol == "GBPUSD"
    assert deserialized.side == OrderSide.BUY
    assert pytest.approx(deserialized.volume, 1e-4) == 0.10


# =============================================================================
# SHARED MEMORY SPSC RING BUFFER WRITER TESTS
# =============================================================================

@pytest.mark.unit
def test_shm_writer_header_initialization():
    """Verify ShmRingBufferWriter initializes ADR-001 header cache lines."""
    writer = ShmRingBufferWriter(terminal_id="test_term_1", use_mock_buffer=True)
    buf = writer._get_buffer()

    assert len(buf) == TOTAL_SHM_SIZE

    # Cache Line 0 (Static config)
    magic, version, capacity, slot_size, flags, prod_pid = struct.unpack_from("<IIIIII", buf, 0)
    assert magic == SHM_MAGIC
    assert version == SHM_VERSION
    assert capacity == TOTAL_SLOTS
    assert slot_size == SLOT_SIZE
    assert flags == 1  # Active

    # Cache Line 1 (Write head at offset 64)
    write_head = struct.unpack_from("<Q", buf, 64)[0]
    assert write_head == 0

    # Cache Line 2 (Read tail at offset 128)
    read_tail = struct.unpack_from("<Q", buf, 128)[0]
    assert read_tail == 0

    writer.close()


@pytest.mark.unit
def test_shm_writer_push_and_wrap_around():
    """Verify SPSC push advances write head and writes to correct circular slots."""
    writer = ShmRingBufferWriter(terminal_id="test_term_2", use_mock_buffer=True)
    buf = writer._get_buffer()

    # Push 5 orders
    for i in range(5):
        intent = OrderIntent(
            symbol=f"SYM_{i}",
            side=OrderSide.BUY,
            volume=float(i + 1),
            signal_id=1000 + i,
        )
        assert writer.push_order_intent(intent) is True

    # Write head should be 5
    write_head = struct.unpack_from("<Q", buf, 64)[0]
    assert write_head == 5

    # Verify order 3 at slot 3
    slot_3_offset = HEADER_SIZE + (3 * SLOT_SIZE)
    order_3 = deserialize_order_intent(buf, slot_3_offset)
    assert order_3.symbol == "SYM_3"
    assert order_3.signal_id == 1003

    writer.close()


@pytest.mark.unit
def test_shm_writer_overrun_guard():
    """Verify overrun guard when queue is saturated (write_head - read_tail >= capacity)."""
    writer = ShmRingBufferWriter(terminal_id="test_term_3", use_mock_buffer=True)
    buf = writer._get_buffer()

    # Artificially set write_head to 1024 while read_tail is 0 (capacity is 1024)
    struct.pack_into("<Q", buf, 64, 1024)
    struct.pack_into("<Q", buf, 128, 0)

    intent = OrderIntent(symbol="OVERFLOW", side=OrderSide.BUY)
    success = writer.push_order_intent(intent)
    assert success is False, "Push must fail when ring buffer is saturated"

    # Dropped intents counter at offset 72 should be incremented
    dropped = struct.unpack_from("<Q", buf, 72)[0]
    assert dropped == 1

    # Simulate consumer advancing read_tail
    struct.pack_into("<Q", buf, 128, 10)  # Consumer popped 10 items
    assert writer.push_order_intent(intent) is True
    assert struct.unpack_from("<Q", buf, 64)[0] == 1025

    writer.close()


@pytest.mark.unit
def test_shm_writer_pause_and_resume():
    """Verify pause and resume status flags in SHM header."""
    writer = ShmRingBufferWriter(terminal_id="test_term_4", use_mock_buffer=True)
    assert writer.is_paused() is False

    writer.pause()
    assert writer.is_paused() is True

    writer.resume()
    assert writer.is_paused() is False

    writer.close()


# =============================================================================
# NAMED PIPE WIRE FRAMING TESTS (32 BYTES HEADER)
# =============================================================================

@pytest.mark.unit
def test_named_pipe_frame_packing():
    """Verify 32-byte Named Pipe wire framing and CRC32 payload checksum."""
    payload = b"HelloMetaTraderDLL"
    frame = pack_pipe_frame(
        opcode=NamedPipeOpcode.MSG_INIT_HELLO,
        payload=payload,
        seq_id=42,
    )
    assert len(frame) == 32 + len(payload)

    hdr = unpack_pipe_frame_header(frame[:32])
    assert hdr.magic == NAMED_PIPE_FRAME_MAGIC
    assert hdr.opcode == NamedPipeOpcode.MSG_INIT_HELLO
    assert hdr.seq_id == 42
    assert hdr.payload_len == len(payload)
    assert hdr.crc32 == binascii.crc32(payload) & 0xFFFFFFFF


@pytest.mark.unit
def test_account_state_payload_packing():
    """Verify MsgAccountStatePayload 64-byte packing (equity, margin, balance)."""
    state = AccountState(
        account_login=12345678,
        balance=10000.50,
        equity=10550.25,
        margin=500.00,
        free_margin=10050.25,
        margin_level=2110.05,
        open_positions_count=3,
        pending_orders_count=1,
        is_connected_to_broker=True,
    )
    packed = state.pack()
    assert len(packed) == 64

    unpacked = AccountState.unpack(packed)
    assert unpacked.account_login == 12345678
    assert pytest.approx(unpacked.balance, 1e-2) == 10000.50
    assert pytest.approx(unpacked.equity, 1e-2) == 10550.25
    assert pytest.approx(unpacked.margin, 1e-2) == 500.00
    assert pytest.approx(unpacked.free_margin, 1e-2) == 10050.25
    assert unpacked.open_positions_count == 3
    assert unpacked.is_connected_to_broker is True


# =============================================================================
# ASYNC NAMED PIPE SERVER & PROTOCOL STREAM PARSING TESTS
# =============================================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_named_pipe_server_stream_parsing():
    """
    Verify AsyncNamedPipeServer accurately parses incoming framed messages
    (MSG_INIT_HELLO, MSG_ACCOUNT_STATE, MSG_EXECUTION_FEEDBACK) without blocking.
    """
    server = AsyncNamedPipeServer(terminal_id="mt4_1", pipe_name=r"\\.\pipe\test_mock_pipe_1")
    await server.start()

    received_states = []
    received_feedbacks = []
    server.on_account_state(lambda tid, st: received_states.append(st))
    server.on_execution_feedback(lambda tid, fb: received_feedbacks.append(fb))

    # 1. Simulate client streaming MSG_INIT_HELLO
    hello = InitHelloPayload(
        terminal_id="mt4_1",
        account_login=998877,
        broker_server="ICMarkets-Demo",
    )
    hello_frame = pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack())
    server.simulate_inbound_data(hello_frame)

    assert server.last_hello is not None
    assert server.last_hello.account_login == 998877

    # 2. Simulate client streaming MSG_ACCOUNT_STATE
    state = AccountState(
        account_login=998877,
        balance=5000.0,
        equity=5250.0,
        margin=200.0,
        free_margin=5050.0,
        open_positions_count=2,
        is_connected_to_broker=True,
    )
    state_frame = pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack())
    server.simulate_inbound_data(state_frame)

    assert len(received_states) == 1
    assert received_states[0].equity == 5250.0

    # 3. Simulate client streaming MSG_EXECUTION_FEEDBACK
    feedback = ExecutionFeedback(
        signal_id=1234567,
        order_ticket=88888,
        execution_status=ExecutionStatus.FILLED,
        executed_price=2650.40,
        slippage_pips=0.1,
        execution_latency_us=45,
    )
    feedback_frame = pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, feedback.pack())
    server.simulate_inbound_data(feedback_frame)

    assert len(received_feedbacks) == 1
    assert received_feedbacks[0].order_ticket == 88888
    assert received_feedbacks[0].execution_status == ExecutionStatus.FILLED

    await server.stop()


# =============================================================================
# IPC DISPATCHER ORCHESTRATION & FAULT HANDLING TESTS
# =============================================================================

@pytest.mark.unit
@pytest.mark.asyncio
async def test_ipc_dispatcher_lifecycle_and_dispatch():
    """
    Verify IPCDispatcher orchestrates terminals, verifies routing availability,
    and pushes OrderIntents to Shared Memory.
    """
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    ctx = dispatcher.register_terminal("terminal_alpha")
    await dispatcher.start()

    intent = OrderIntent(
        symbol="XAUUSD",
        side=OrderSide.BUY,
        volume=0.50,
        price=2650.0,
    )

    # Initially DISCONNECTED: dispatch must fail safely
    assert dispatcher.can_route_to_terminal("terminal_alpha") is False
    assert dispatcher.dispatch_order_intent("terminal_alpha", intent) is False

    # Simulate terminal connecting and completing handshake
    hello = InitHelloPayload(terminal_id="terminal_alpha", account_login=1001)
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))

    # Provide active connected account state
    state = AccountState(account_login=1001, equity=10000.0, is_connected_to_broker=True)
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack()))

    # Terminal should now be CONNECTED and eligible for routing
    assert dispatcher.get_terminal_status("terminal_alpha") == TerminalStatus.CONNECTED
    assert dispatcher.can_route_to_terminal("terminal_alpha") is True

    # Dispatch OrderIntent to Shared Memory
    dispatched = dispatcher.dispatch_order_intent("terminal_alpha", intent)
    assert dispatched is True

    # Verify write_head advanced in SHM
    metrics = ctx.shm_writer.get_metrics()
    assert metrics["write_head"] == 1
    assert metrics["queue_depth"] == 1

    await dispatcher.stop()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ipc_dispatcher_broken_pipe_crash_handling():
    """
    Verify Error Handling: When MetaTrader crashes or pipe breaks,
    the Engine logs disconnect and gracefully pauses routing to that specific terminal.
    """
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    ctx = dispatcher.register_terminal("terminal_crasher")
    await dispatcher.start()

    status_changes = []
    dispatcher.on_status_change(lambda tid, st, r: status_changes.append((tid, st, r)))

    # Handshake to CONNECTED
    hello = InitHelloPayload(terminal_id="terminal_crasher", account_login=2002)
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
    assert dispatcher.get_terminal_status("terminal_crasher") == TerminalStatus.CONNECTED

    # Simulate terminal process crash / pipe broken
    ctx.pipe_server._on_client_disconnected(None, ConnectionResetError("MetaTrader terminal process died"))

    # Status must transition to DISCONNECTED and SHM writer must pause
    assert dispatcher.get_terminal_status("terminal_crasher") == TerminalStatus.DISCONNECTED
    assert ctx.shm_writer.is_paused() is True
    assert dispatcher.can_route_to_terminal("terminal_crasher") is False

    # Attempting to dispatch to crashed terminal must fail safely
    intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY)
    assert dispatcher.dispatch_order_intent("terminal_crasher", intent) is False

    # Status change callback should have recorded disconnect
    assert any(tid == "terminal_crasher" and st == TerminalStatus.DISCONNECTED for tid, st, _ in status_changes)

    # Reconnection: terminal restarts and sends INIT_HELLO
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
    assert dispatcher.get_terminal_status("terminal_crasher") == TerminalStatus.CONNECTED
    assert ctx.shm_writer.is_paused() is False

    await dispatcher.stop()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ipc_dispatcher_heartbeat_watchdog_timeout():
    """
    Verify watchdog automatically pauses routing when terminal heartbeat goes stale.
    """
    # Short heartbeat timeout for test: 0.1s
    dispatcher = IPCDispatcher(heartbeat_timeout_s=0.1, watchdog_interval_s=0.05, use_mock_buffers=True)
    ctx = dispatcher.register_terminal("terminal_watchdog")
    await dispatcher.start()

    # Handshake to CONNECTED
    hello = InitHelloPayload(terminal_id="terminal_watchdog", account_login=3003)
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
    assert dispatcher.get_terminal_status("terminal_watchdog") == TerminalStatus.CONNECTED

    # Wait for watchdog to detect timeout
    await asyncio.sleep(0.25)

    # Terminal should be paused automatically due to heartbeat timeout
    assert dispatcher.get_terminal_status("terminal_watchdog") == TerminalStatus.PAUSED
    assert dispatcher.can_route_to_terminal("terminal_watchdog") is False

    # Streaming fresh state should automatically recover terminal
    state = AccountState(account_login=3003, equity=15000.0, is_connected_to_broker=True)
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack()))
    assert dispatcher.get_terminal_status("terminal_watchdog") == TerminalStatus.CONNECTED

    await dispatcher.stop()
