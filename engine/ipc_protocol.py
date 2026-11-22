"""
Pipvane v3 IPC Binary Protocol Specification (ADR-001).

Implements the zero-copy C++ Plain Old Data (POD) struct layouts for:
1. ShmRingBufferHeader: 256 bytes (4 x 64-byte Cache Lines)
2. OrderIntent: 128 bytes (2 x 64-byte Cache Lines, cache-line aligned)
3. NamedPipeFrameHeader: 32 bytes binary wire frame
4. Named Pipe Payload Structs:
   - MsgAccountStatePayload: 64 bytes
   - MsgInitHelloPayload: 248 bytes
   - MsgExecutionFeedbackPayload: 136 bytes
"""

from __future__ import annotations

import enum
import time
import struct
import binascii
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List


# =============================================================================
# CONSTANTS & MAGIC SIGNATURES (ADR-001)
# =============================================================================

SHM_MAGIC: int = 0x50495056          # "PIPV" in Little-Endian uint32
SHM_VERSION: int = 1
TOTAL_SLOTS: int = 1024              # Must be power of 2
SLOT_SIZE: int = 128                 # Exactly 2 x 64-byte Cache Lines
HEADER_SIZE: int = 256               # Exactly 4 x 64-byte Cache Lines
TOTAL_SHM_SIZE: int = HEADER_SIZE + (TOTAL_SLOTS * SLOT_SIZE)  # 131,328 bytes (128.25 KB)

NAMED_PIPE_FRAME_MAGIC: int = 0x50564E50  # "PVNP" in Little-Endian uint32
NAMED_PIPE_HEADER_SIZE: int = 32          # 32 bytes


# =============================================================================
# ENUMS
# =============================================================================

class OrderSide(enum.IntEnum):
    """Trading order direction."""
    BUY = 0
    SELL = 1


class ActionType(enum.IntEnum):
    """Action requested from broker core."""
    MARKET_ENTRY = 1
    LIMIT_ENTRY = 2
    STOP_ENTRY = 3
    MODIFY_SL_TP = 4
    PARTIAL_CLOSE = 5
    FULL_CLOSE = 6
    CANCEL_PENDING = 7


class ExecutionFlags(enum.IntFlag):
    """Execution policy bitmask."""
    IOC = 0x01             # Immediate or Cancel
    FOK = 0x02             # Fill or Kill
    ALLOW_PARTIAL = 0x04   # Allow partial fill laddering


class NamedPipeOpcode(enum.IntEnum):
    """Opcodes for Named Pipe control and state synchronization."""
    MSG_INIT_HELLO = 0x0001
    MSG_CAPABILITIES_REQ = 0x0002
    MSG_CAPABILITIES_RESP = 0x0003
    MSG_ACCOUNT_STATE = 0x0010
    MSG_POSITIONS_SYNC = 0x0011
    MSG_POSITION_UPDATE = 0x0012
    MSG_SYMBOLS_REQ = 0x0014
    MSG_SYMBOLS_RESP = 0x0015
    MSG_EXECUTION_FEEDBACK = 0x0020
    MSG_HEARTBEAT_PING = 0x0030
    MSG_HEARTBEAT_PONG = 0x0031
    MSG_SHUTDOWN = 0x00FF


class ExecutionStatus(enum.IntEnum):
    """Broker execution outcome status."""
    FILLED = 0
    PARTIAL = 1
    REJECTED = 2
    REQUOTE = 3
    EXPIRED = 4


class TerminalStatus(enum.Enum):
    """Operational status of a connected MetaTrader terminal."""
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    PAUSED = "PAUSED"
    SECURITY_BLOCKED = "SECURITY_BLOCKED"


# =============================================================================
# ORDER INTENT (128 BYTES, 2 CACHE LINES)
# =============================================================================

# Fast pre-compiled struct packers:
# Prefix (116 bytes): symbol (32s), side (B), action_type (B), padding (6s),
#                     volume (d), price (d), stop_loss (d), take_profit (d),
#                     slippage_cap_pips (d), magic_number (Q), order_ticket (Q),
#                     signal_id (Q), timestamp_ns (Q), flags (I)
_ORDER_INTENT_PREFIX_STRUCT = struct.Struct("<32sBB6sdddddQQQQI")
# Full struct (128 bytes): Prefix + crc32 (I) + reserved (Q)
_ORDER_INTENT_FULL_STRUCT = struct.Struct("<32sBB6sdddddQQQQIIQ")

assert _ORDER_INTENT_PREFIX_STRUCT.size == 116
assert _ORDER_INTENT_FULL_STRUCT.size == 128


@dataclass
class OrderIntent:
    """
    Atomic trading order directive dispatched over Shared Memory to MetaTrader C++ DLL.
    Memory layout strictly sized to 128 bytes (ADR-001).
    """
    symbol: str
    side: OrderSide | int
    action_type: ActionType | int = ActionType.MARKET_ENTRY
    volume: float = 0.01
    price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    slippage_cap_pips: float = 2.0
    magic_number: int = 123456
    order_ticket: int = 0
    signal_id: int = 0
    timestamp_ns: int = 0
    flags: int = 0
    crc32: int = 0
    reserved: int = 0


def serialize_order_intent(intent: OrderIntent) -> bytes:
    """
    Serializes an OrderIntent into exactly 128 raw bytes with computed IEEE 802.3 CRC32.
    """
    sym_bytes = intent.symbol.encode("ascii")[:31]  # Null-terminated
    now_ns = intent.timestamp_ns or time.time_ns()

    prefix = _ORDER_INTENT_PREFIX_STRUCT.pack(
        sym_bytes,
        int(intent.side),
        int(intent.action_type),
        b"\x00" * 6,
        float(intent.volume),
        float(intent.price),
        float(intent.stop_loss),
        float(intent.take_profit),
        float(intent.slippage_cap_pips),
        int(intent.magic_number),
        int(intent.order_ticket),
        int(intent.signal_id),
        int(now_ns),
        int(intent.flags),
    )

    crc = binascii.crc32(prefix) & 0xFFFFFFFF
    return prefix + struct.pack("<IQ", crc, int(intent.reserved))


def serialize_order_intent_into(buffer: bytearray | memoryview, offset: int, intent: OrderIntent) -> None:
    """
    Zero-overhead direct serialization into a memory buffer or mmap segment.
    """
    sym_bytes = intent.symbol.encode("ascii")[:31]
    now_ns = intent.timestamp_ns or time.time_ns()

    _ORDER_INTENT_PREFIX_STRUCT.pack_into(
        buffer,
        offset,
        sym_bytes,
        int(intent.side),
        int(intent.action_type),
        b"\x00" * 6,
        float(intent.volume),
        float(intent.price),
        float(intent.stop_loss),
        float(intent.take_profit),
        float(intent.slippage_cap_pips),
        int(intent.magic_number),
        int(intent.order_ticket),
        int(intent.signal_id),
        int(now_ns),
        int(intent.flags),
    )

    # Calculate CRC32 over the 116-byte prefix
    prefix_view = memoryview(buffer)[offset : offset + 116]
    crc = binascii.crc32(prefix_view) & 0xFFFFFFFF
    struct.pack_into("<IQ", buffer, offset + 116, crc, int(intent.reserved))


def deserialize_order_intent(data: bytes | bytearray | memoryview, offset: int = 0) -> OrderIntent:
    """
    Deserializes 128 bytes from data into an OrderIntent object with CRC32 integrity check.
    """
    unpacked = _ORDER_INTENT_FULL_STRUCT.unpack_from(data, offset)
    sym_raw = unpacked[0]
    symbol = sym_raw.split(b"\x00", 1)[0].decode("ascii", errors="replace")

    # Verify CRC32
    prefix_slice = memoryview(data)[offset : offset + 116]
    expected_crc = unpacked[14]
    computed_crc = binascii.crc32(prefix_slice) & 0xFFFFFFFF
    if expected_crc != 0 and computed_crc != expected_crc:
        raise ValueError(f"CRC32 mismatch in OrderIntent slot (expected 0x{expected_crc:08X}, got 0x{computed_crc:08X})")

    return OrderIntent(
        symbol=symbol,
        side=OrderSide(unpacked[1]),
        action_type=ActionType(unpacked[2]),
        volume=unpacked[4],
        price=unpacked[5],
        stop_loss=unpacked[6],
        take_profit=unpacked[7],
        slippage_cap_pips=unpacked[8],
        magic_number=unpacked[9],
        order_ticket=unpacked[10],
        signal_id=unpacked[11],
        timestamp_ns=unpacked[12],
        flags=unpacked[13],
        crc32=unpacked[14],
        reserved=unpacked[15],
    )


# =============================================================================
# NAMED PIPE WIRE FRAMING (32 BYTES HEADER)
# =============================================================================

# Header format: magic (I), opcode (H), flags (H), payload_len (I), seq_id (I),
#                timestamp_ns (Q), crc32 (I), reserved (I) = 32 bytes
_PIPE_HEADER_STRUCT = struct.Struct("<IHHIIQII")
assert _PIPE_HEADER_STRUCT.size == 32


@dataclass
class NamedPipeFrameHeader:
    """32-byte header framing every binary message across the Windows Named Pipe."""
    magic: int = NAMED_PIPE_FRAME_MAGIC
    opcode: int = 0
    flags: int = 0
    payload_len: int = 0
    seq_id: int = 0
    timestamp_ns: int = 0
    crc32: int = 0
    reserved: int = 0


def pack_pipe_frame(opcode: int | NamedPipeOpcode, payload: bytes = b"", seq_id: int = 0, flags: int = 0) -> bytes:
    """
    Constructs a framed Named Pipe message consisting of a 32-byte header and payload.
    """
    payload_crc = binascii.crc32(payload) & 0xFFFFFFFF if payload else 0
    now_ns = time.time_ns()
    header_bytes = _PIPE_HEADER_STRUCT.pack(
        NAMED_PIPE_FRAME_MAGIC,
        int(opcode),
        int(flags),
        len(payload),
        int(seq_id),
        now_ns,
        payload_crc,
        0,
    )
    return header_bytes + payload


def unpack_pipe_frame_header(header_bytes: bytes | bytearray) -> NamedPipeFrameHeader:
    """
    Unpacks a 32-byte frame header and validates magic.
    """
    if len(header_bytes) < 32:
        raise ValueError(f"Header too short: {len(header_bytes)} bytes (expected 32)")

    magic, opcode, flags, payload_len, seq_id, timestamp_ns, crc32, reserved = _PIPE_HEADER_STRUCT.unpack_from(header_bytes, 0)
    if magic != NAMED_PIPE_FRAME_MAGIC:
        raise ValueError(f"Invalid Named Pipe magic signature: 0x{magic:08X} (expected 0x{NAMED_PIPE_FRAME_MAGIC:08X})")

    return NamedPipeFrameHeader(
        magic=magic,
        opcode=opcode,
        flags=flags,
        payload_len=payload_len,
        seq_id=seq_id,
        timestamp_ns=timestamp_ns,
        crc32=crc32,
        reserved=reserved,
    )


# =============================================================================
# NAMED PIPE PAYLOAD STRUCTS
# =============================================================================

# MsgAccountStatePayload: 64 bytes
# account_login (Q), balance (d), equity (d), margin (d), free_margin (d),
# margin_level (d), open_positions_count (I), pending_orders_count (I),
# is_connected_to_broker (B), reserved (7s) = 64 bytes
_ACCOUNT_STATE_STRUCT = struct.Struct("<QdddddIIB7s")
assert _ACCOUNT_STATE_STRUCT.size == 64


@dataclass
class AccountState:
    """Real-time account balance and margin metrics streamed by MetaTrader DLL."""
    account_login: int = 0
    balance: float = 0.0
    equity: float = 0.0
    margin: float = 0.0
    free_margin: float = 0.0
    margin_level: float = 0.0
    open_positions_count: int = 0
    pending_orders_count: int = 0
    is_connected_to_broker: bool = True

    def pack(self) -> bytes:
        return _ACCOUNT_STATE_STRUCT.pack(
            self.account_login,
            self.balance,
            self.equity,
            self.margin,
            self.free_margin,
            self.margin_level,
            self.open_positions_count,
            self.pending_orders_count,
            1 if self.is_connected_to_broker else 0,
            b"\x00" * 7,
        )

    @classmethod
    def unpack(cls, data: bytes) -> AccountState:
        unpacked = _ACCOUNT_STATE_STRUCT.unpack_from(data, 0)
        return cls(
            account_login=unpacked[0],
            balance=unpacked[1],
            equity=unpacked[2],
            margin=unpacked[3],
            free_margin=unpacked[4],
            margin_level=unpacked[5],
            open_positions_count=unpacked[6],
            pending_orders_count=unpacked[7],
            is_connected_to_broker=bool(unpacked[8]),
        )


# MsgInitHelloPayload: 248 bytes
# terminal_id (32s), platform_type (B), is_portable (B), reserved (H),
# terminal_build (I), dll_bridge_version (I), account_login (Q), broker_server (64s),
# company_name (64s), shm_segment_name (64s), shm_capacity (I) = 248 bytes
_INIT_HELLO_STRUCT = struct.Struct("<32sBBHIIQ64s64s64sI")
assert _INIT_HELLO_STRUCT.size == 248


@dataclass
class InitHelloPayload:
    """Terminal attach notification from C++ DLL."""
    terminal_id: str
    platform_type: int = 0        # 0: MT4, 1: MT5
    is_portable: bool = False
    terminal_build: int = 1420
    dll_bridge_version: int = 0x03000001
    account_login: int = 0
    broker_server: str = ""
    company_name: str = ""
    shm_segment_name: str = ""
    shm_capacity: int = TOTAL_SLOTS

    def pack(self) -> bytes:
        return _INIT_HELLO_STRUCT.pack(
            self.terminal_id.encode("ascii")[:31],
            self.platform_type,
            1 if self.is_portable else 0,
            0,
            self.terminal_build,
            self.dll_bridge_version,
            self.account_login,
            self.broker_server.encode("ascii")[:63],
            self.company_name.encode("ascii")[:63],
            self.shm_segment_name.encode("ascii")[:63],
            self.shm_capacity,
        )

    @classmethod
    def unpack(cls, data: bytes) -> InitHelloPayload:
        unpacked = _INIT_HELLO_STRUCT.unpack_from(data, 0)
        return cls(
            terminal_id=unpacked[0].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
            platform_type=unpacked[1],
            is_portable=bool(unpacked[2]),
            terminal_build=unpacked[4],
            dll_bridge_version=unpacked[5],
            account_login=unpacked[6],
            broker_server=unpacked[7].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
            company_name=unpacked[8].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
            shm_segment_name=unpacked[9].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
            shm_capacity=unpacked[10],
        )


# =============================================================================
# MSG_CAPABILITIES_RESP (0x0003) & CAPABILITIES PAYLOAD (64 BYTES)
# =============================================================================

# MsgCapabilitiesPayload: 64 bytes
# supports_hedging (B), is_fifo_enforced (B), trade_allowed (B), dll_allowed (B),
# execution_mode (B), reserved_flags (3s), leverage (I), reserved_align (4s),
# min_lot (d), max_lot (d), lot_step (d), stop_level_points (I), freeze_level_points (I),
# account_currency (16s) = 64 bytes
_CAPABILITIES_STRUCT = struct.Struct("<BBBBB3sI4sdddII16s")
assert _CAPABILITIES_STRUCT.size == 64


@dataclass
class CapabilitiesPayload:
    """Broker and terminal trading capabilities returned by MetaTrader C++ DLL."""
    supports_hedging: bool = True
    is_fifo_enforced: bool = False
    trade_allowed: bool = True
    dll_allowed: bool = True
    execution_mode: int = 0
    leverage: int = 500
    min_lot: float = 0.01
    max_lot: float = 100.0
    lot_step: float = 0.01
    stop_level_points: int = 0
    freeze_level_points: int = 0
    account_currency: str = "USD"

    def pack(self) -> bytes:
        return _CAPABILITIES_STRUCT.pack(
            1 if self.supports_hedging else 0,
            1 if self.is_fifo_enforced else 0,
            1 if self.trade_allowed else 0,
            1 if self.dll_allowed else 0,
            self.execution_mode,
            b"\x00" * 3,
            self.leverage,
            b"\x00" * 4,
            float(self.min_lot),
            float(self.max_lot),
            float(self.lot_step),
            int(self.stop_level_points),
            int(self.freeze_level_points),
            self.account_currency.encode("ascii")[:15].ljust(16, b"\x00"),
        )

    @classmethod
    def unpack(cls, data: bytes) -> CapabilitiesPayload:
        if not data:
            return cls()
        trimmed = data.lstrip()
        if trimmed.startswith(b"{"):
            import json
            obj = json.loads(data.decode("utf-8", errors="replace"))
            return cls(
                supports_hedging=bool(obj.get("supports_hedging", True)),
                is_fifo_enforced=bool(obj.get("is_fifo_enforced", False)),
                trade_allowed=bool(obj.get("trade_allowed", True)),
                dll_allowed=bool(obj.get("dll_allowed", True)),
                execution_mode=int(obj.get("execution_mode", 0)),
                leverage=int(obj.get("leverage", 500)),
                min_lot=float(obj.get("min_lot", 0.01)),
                max_lot=float(obj.get("max_lot", 100.0)),
                lot_step=float(obj.get("lot_step", 0.01)),
                stop_level_points=int(obj.get("stop_level_points", 0)),
                freeze_level_points=int(obj.get("freeze_level_points", 0)),
                account_currency=str(obj.get("account_currency", "USD")),
            )
        unpacked = _CAPABILITIES_STRUCT.unpack_from(data, 0)
        return cls(
            supports_hedging=bool(unpacked[0]),
            is_fifo_enforced=bool(unpacked[1]),
            trade_allowed=bool(unpacked[2]),
            dll_allowed=bool(unpacked[3]),
            execution_mode=unpacked[4],
            leverage=unpacked[6],
            min_lot=unpacked[8],
            max_lot=unpacked[9],
            lot_step=unpacked[10],
            stop_level_points=unpacked[11],
            freeze_level_points=unpacked[12],
            account_currency=unpacked[13].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
        )


# MsgExecutionFeedbackPayload: 136 bytes
# signal_id (Q), order_ticket (Q), execution_status (B), reserved (3s),
# broker_retcode (I), requested_volume (d), executed_volume (d), requested_price (d),
# executed_price (d), slippage_pips (d), execution_latency_us (Q), error_description (64s)
_EXEC_FEEDBACK_STRUCT = struct.Struct("<QQB3sIdddddQ64s")
assert _EXEC_FEEDBACK_STRUCT.size == 136


@dataclass
class ExecutionFeedback:
    """Immediate fill or broker rejection confirmation received over Named Pipe."""
    signal_id: int
    order_ticket: int
    execution_status: ExecutionStatus | int
    broker_retcode: int = 10009
    requested_volume: float = 0.01
    executed_volume: float = 0.01
    requested_price: float = 0.0
    executed_price: float = 0.0
    slippage_pips: float = 0.0
    execution_latency_us: int = 0
    error_description: str = ""

    def pack(self) -> bytes:
        return _EXEC_FEEDBACK_STRUCT.pack(
            self.signal_id,
            self.order_ticket,
            int(self.execution_status),
            b"\x00" * 3,
            self.broker_retcode,
            self.requested_volume,
            self.executed_volume,
            self.requested_price,
            self.executed_price,
            self.slippage_pips,
            self.execution_latency_us,
            self.error_description.encode("ascii")[:63],
        )

    @classmethod
    def unpack(cls, data: bytes) -> ExecutionFeedback:
        unpacked = _EXEC_FEEDBACK_STRUCT.unpack_from(data, 0)
        return cls(
            signal_id=unpacked[0],
            order_ticket=unpacked[1],
            execution_status=ExecutionStatus(unpacked[2]),
            broker_retcode=unpacked[4],
            requested_volume=unpacked[5],
            executed_volume=unpacked[6],
            requested_price=unpacked[7],
            executed_price=unpacked[8],
            slippage_pips=unpacked[9],
            execution_latency_us=unpacked[10],
            error_description=unpacked[11].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
        )


# =============================================================================
# MSG_POSITIONS_SYNC (0x0011) & POSITION ITEM
# =============================================================================

_POSITION_ITEM_STRUCT = struct.Struct("<QQQ32sB7sdddddddd32s")
_POSITIONS_SYNC_HDR_STRUCT = struct.Struct("<QII")
assert _POSITION_ITEM_STRUCT.size == 160
assert _POSITIONS_SYNC_HDR_STRUCT.size == 16


@dataclass
class PositionItem:
    """Represents an active position record streamed by MetaTrader DLL."""
    ticket: int
    magic_number: int
    open_time_unix: int
    symbol: str
    position_type: int        # 0: BUY, 1: SELL
    volume: float
    open_price: float
    current_price: float
    stop_loss: float = 0.0
    take_profit: float = 0.0
    profit: float = 0.0
    swap: float = 0.0
    commission: float = 0.0
    comment: str = ""

    def pack(self) -> bytes:
        return _POSITION_ITEM_STRUCT.pack(
            self.ticket,
            self.magic_number,
            self.open_time_unix,
            self.symbol.encode("ascii")[:31],
            self.position_type,
            b"\x00" * 7,
            self.volume,
            self.open_price,
            self.current_price,
            self.stop_loss,
            self.take_profit,
            self.profit,
            self.swap,
            self.commission,
            self.comment.encode("ascii")[:31],
        )

    @classmethod
    def unpack(cls, data: bytes, offset: int = 0) -> PositionItem:
        unpacked = _POSITION_ITEM_STRUCT.unpack_from(data, offset)
        return cls(
            ticket=unpacked[0],
            magic_number=unpacked[1],
            open_time_unix=unpacked[2],
            symbol=unpacked[3].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
            position_type=unpacked[4],
            volume=unpacked[6],
            open_price=unpacked[7],
            current_price=unpacked[8],
            stop_loss=unpacked[9],
            take_profit=unpacked[10],
            profit=unpacked[11],
            swap=unpacked[12],
            commission=unpacked[13],
            comment=unpacked[14].split(b"\x00", 1)[0].decode("ascii", errors="replace"),
        )


@dataclass
class PositionsSyncPayload:
    """Full position array sync payload from MetaTrader DLL."""
    account_login: int
    positions: List[PositionItem] = field(default_factory=list)

    def pack(self) -> bytes:
        hdr = _POSITIONS_SYNC_HDR_STRUCT.pack(self.account_login, len(self.positions), 0)
        items_bytes = b"".join(pos.pack() for pos in self.positions)
        return hdr + items_bytes

    @classmethod
    def unpack(cls, data: bytes) -> PositionsSyncPayload:
        if not data:
            return cls(account_login=0, positions=[])

        trimmed = data.lstrip()
        if trimmed.startswith(b"[") or trimmed.startswith(b"{"):
            import json
            obj = json.loads(data.decode("utf-8", errors="replace"))
            login = 0
            items_raw = []
            if isinstance(obj, dict):
                login = int(obj.get("account_login", 0))
                items_raw = obj.get("positions", [])
            elif isinstance(obj, list):
                items_raw = obj

            positions = []
            for item in items_raw:
                positions.append(PositionItem(
                    ticket=int(item.get("ticket", 0)),
                    magic_number=int(item.get("magic_number", 0)),
                    open_time_unix=int(item.get("open_time_unix", item.get("open_time", 0))),
                    symbol=str(item.get("symbol", "")),
                    position_type=int(item.get("position_type", item.get("side", 0))),
                    volume=float(item.get("volume", 0.0)),
                    open_price=float(item.get("open_price", item.get("price", 0.0))),
                    current_price=float(item.get("current_price", item.get("price", 0.0))),
                    stop_loss=float(item.get("stop_loss", item.get("sl", 0.0))),
                    take_profit=float(item.get("take_profit", item.get("tp", 0.0))),
                    profit=float(item.get("profit", 0.0)),
                    swap=float(item.get("swap", 0.0)),
                    commission=float(item.get("commission", 0.0)),
                    comment=str(item.get("comment", "")),
                ))
            return cls(account_login=login, positions=positions)

        if len(data) < 16:
            return cls(account_login=0, positions=[])

        login, count, _ = _POSITIONS_SYNC_HDR_STRUCT.unpack_from(data, 0)
        positions = []
        offset = 16
        for _ in range(count):
            if offset + 160 <= len(data):
                positions.append(PositionItem.unpack(data, offset))
                offset += 160
        return cls(account_login=login, positions=positions)


# =============================================================================
# MSG_SYMBOLS_SYNC (0x0014 / 0x0015) & SYMBOLS SYNC PAYLOAD
# =============================================================================

@dataclass
class SymbolsSyncPayload:
    """Tradable symbols list received from MetaTrader DLL."""
    terminal_id: str
    symbols: List[str] = field(default_factory=list)

    def pack(self) -> bytes:
        import json
        return json.dumps({
            "terminal_id": self.terminal_id,
            "symbols": self.symbols,
        }).encode("utf-8")

    @classmethod
    def unpack(cls, data: bytes) -> SymbolsSyncPayload:
        import json
        if not data:
            return cls(terminal_id="", symbols=[])
        try:
            obj = json.loads(data.decode("utf-8", errors="replace"))
            if isinstance(obj, dict):
                return cls(
                    terminal_id=str(obj.get("terminal_id", "")),
                    symbols=[str(s) for s in obj.get("symbols", [])],
                )
            elif isinstance(obj, list):
                return cls(terminal_id="", symbols=[str(s) for s in obj])
        except Exception:
            lines = [s.strip() for s in data.decode("ascii", errors="replace").splitlines() if s.strip()]
            return cls(terminal_id="", symbols=lines)
        return cls(terminal_id="", symbols=[])
