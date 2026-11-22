#pragma once

#ifndef PIPVANE_IPC_BRIDGE_H
#define PIPVANE_IPC_BRIDGE_H

/**
 * =============================================================================
 * Pipvane v3: Production-Ready Native IPC Bridge for MetaTrader 5
 * =============================================================================
 * Architecture Reference: ./architecture/SPRINT_1_ARCHITECTURE_BLUEPRINT.md
 * 
 * Channels:
 *  1. Shared Memory SPSC Ring Buffer: Local\pipvane_shm_{terminal_id}
 *     - Ultra-low latency (<50 us) OrderIntent transmission from Engine to DLL.
 *     - 64-byte cache line aligned, lock-free C++11 atomic acquire/release.
 *     - SSE-accelerated 128-byte unrolled copy and IEEE 802.3 CRC32 verification.
 *  2. Asynchronous Duplex Named Pipe Channel: \\.\pipe\pipvane_mt_{terminal_id}
 *     - Asynchronous outbound telemetry queue with background streaming worker.
 *     - Non-blocking state reporting: EA thread enqueues in <100 ns.
 *     - 32-byte binary wire framing header ('PVNP' magic 0x50564E50).
 * 
 * Security:
 *  - Compiled with ASLR (/DYNAMICBASE, /HIGHENTROPYVA), DEP (/NXCOMPAT), CFG (/guard:cf), /GS.
 *  - Zero network socket symbols (ws2_32.dll excluded).
 *  - Windows CNG SHA-256 cryptographic verification for binaries (bcrypt.dll).
 * =============================================================================
 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <cstdint>
#include <cstddef>
#include <atomic>
#include <string>
#include <vector>

// Export macro for MetaTrader 5 DLL interface
#ifdef PIPVANE_BRIDGE_EXPORTS
    #define PIPVANE_API __declspec(dllexport)
#else
    #define PIPVANE_API __declspec(dllimport)
#endif

#define PIPVANE_CALL WINAPI

namespace pipvane {

// =============================================================================
// Constants & Magic Signatures (Strictly conforming to SPRINT_1 Blueprint)
// =============================================================================
constexpr uint32_t SHM_MAGIC_SIGNATURE    = 0x50495056; // "PIPV" in Little-Endian
constexpr uint32_t SHM_PROTOCOL_VERSION   = 1;
constexpr uint32_t SHM_DEFAULT_CAPACITY   = 1024;       // Power of 2
constexpr uint32_t SHM_SLOT_SIZE          = 128;        // 2 x 64-byte Cache Lines
constexpr uint32_t SHM_HEADER_SIZE        = 256;        // 4 x 64-byte Cache Lines
constexpr size_t   TOTAL_SHM_SEGMENT_SIZE = SHM_HEADER_SIZE + (SHM_DEFAULT_CAPACITY * SHM_SLOT_SIZE); // 131,328 bytes

constexpr uint32_t PIPE_MAGIC_SIGNATURE   = 0x50564E50; // "PVNP" (PipVane Named Pipe)
constexpr uint32_t PIPE_HEADER_SIZE       = 32;         // 32-byte binary header
constexpr uint32_t PIPE_BUFFER_SIZE       = 65536;      // 64 KB I/O buffer
constexpr size_t   OUTBOUND_QUEUE_SIZE    = 262144;     // 256 KB Asynchronous Telemetry Queue

constexpr uint32_t BRIDGE_VERSION         = 0x03000001; // v3.0.1

// Header status flags
constexpr uint32_t SHM_FLAG_ACTIVE       = 0x00000001;
constexpr uint32_t SHM_FLAG_PAUSED       = 0x00000002;
constexpr uint32_t SHM_FLAG_TERMINATING  = 0x00000004;

// OrderIntent execution flags
constexpr uint32_t INTENT_FLAG_IOC           = 0x00000001; // Immediate or Cancel
constexpr uint32_t INTENT_FLAG_FOK           = 0x00000002; // Fill or Kill
constexpr uint32_t INTENT_FLAG_ALLOW_PARTIAL = 0x00000004; // Allow Partial Fills

// =============================================================================
// Enums
// =============================================================================
enum OrderSide : uint8_t {
    ORDER_SIDE_BUY  = 0,
    ORDER_SIDE_SELL = 1
};

enum ActionType : uint8_t {
    ACTION_MARKET_ENTRY   = 1,
    ACTION_LIMIT_ENTRY    = 2,
    ACTION_STOP_ENTRY     = 3,
    ACTION_MODIFY_SL_TP   = 4,
    ACTION_PARTIAL_CLOSE  = 5,
    ACTION_FULL_CLOSE     = 6,
    ACTION_CANCEL_PENDING = 7
};

enum PipeOpcode : uint16_t {
    MSG_INIT_HELLO          = 0x0001,
    MSG_CAPABILITIES_REQ    = 0x0002,
    MSG_CAPABILITIES_RESP   = 0x0003,
    MSG_ACCOUNT_STATE       = 0x0010,
    MSG_POSITIONS_SYNC      = 0x0011,
    MSG_POSITION_UPDATE     = 0x0012,
    MSG_EXECUTION_FEEDBACK  = 0x0020,
    MSG_HEARTBEAT_PING      = 0x0030,
    MSG_HEARTBEAT_PONG      = 0x0031,
    MSG_SHUTDOWN            = 0x00FF
};

enum ExecutionStatus : uint8_t {
    EXEC_STATUS_FILLED   = 0,
    EXEC_STATUS_PARTIAL  = 1,
    EXEC_STATUS_REJECTED = 2,
    EXEC_STATUS_REQUOTE  = 3,
    EXEC_STATUS_EXPIRED  = 4
};

enum PlatformType : uint8_t {
    PLATFORM_MT4 = 0,
    PLATFORM_MT5 = 1
};

// =============================================================================
// 1. Shared Memory Struct Layouts (ADR-001)
// =============================================================================
#pragma pack(push, 8)

// Total Size: Exactly 256 bytes (4 x 64-byte Cache Lines)
struct ShmRingBufferHeader {
    // =========================================================================
    // CACHE LINE 0 (Bytes 0x00 - 0x3F): Static Metadata & Configuration
    // Written once during initialization; Read-Only thereafter.
    // =========================================================================
    uint32_t magic;                 // 0x00: Magic signature (0x50495056 = "PIPV")
    uint32_t version;               // 0x04: Protocol version (1)
    uint32_t capacity;              // 0x08: Slot capacity (must be power of 2, default 1024)
    uint32_t slot_size;             // 0x0C: Size of each slot in bytes (128)
    uint32_t flags;                 // 0x10: Status flags (Bit 0: Active, Bit 1: Paused, Bit 2: Terminating)
    uint32_t producer_pid;          // 0x14: Windows Process ID of Engine
    uint32_t consumer_pid;          // 0x18: Windows Process ID of MetaTrader Terminal
    uint32_t reserved_cl0_1;        // 0x1C: Alignment padding
    uint64_t heartbeat_ns;          // 0x20: Last heartbeat timestamp (UTC nanoseconds)
    uint8_t  cl0_pad[24];           // 0x28 - 0x3F: Padding to 64 bytes

    // =========================================================================
    // CACHE LINE 1 (Bytes 0x40 - 0x7F): Producer Control Line
    // Written exclusively by the Engine Producer.
    // =========================================================================
    alignas(64) std::atomic<uint64_t> write_head; // 0x40: Next slot sequence index to write
    uint64_t producer_dropped_intents;            // 0x48: Overrun drop counter
    uint8_t  cl1_pad[48];                         // 0x50 - 0x7F: Padding to 64 bytes

    // =========================================================================
    // CACHE LINE 2 (Bytes 0x80 - 0xBF): Consumer Control Line
    // Written exclusively by the MetaTrader C++ DLL Consumer.
    // =========================================================================
    alignas(64) std::atomic<uint64_t> read_tail;  // 0x80: Next slot sequence index to read
    uint64_t consumer_processed_intents;          // 0x88: Completed executions counter
    uint64_t consumer_latency_sum_ns;             // 0x90: Cumulative transit latency in ns
    uint8_t  cl2_pad[40];                         // 0x98 - 0xBF: Padding to 64 bytes

    // =========================================================================
    // CACHE LINE 3 (Bytes 0xC0 - 0xFF): OS Event Signaling & Diagnostics
    // Shared fallback notification state.
    // =========================================================================
    alignas(64) uint64_t event_signal_count;      // 0xC0: Count of SetEvent calls
    uint64_t last_consumed_seq;                   // 0xC8: Sequence ID of last consumed order
    uint8_t  cl3_pad[48];                         // 0xD0 - 0xFF: Padding to 64 bytes
};

// Total Size: Exactly 128 bytes (2 x 64-byte Cache Lines)
struct OrderIntent {
    // =========================================================================
    // CACHE LINE 0 (Bytes 0x00 - 0x3F): Routing, Core Symbols & Pricing
    // =========================================================================
    char        symbol[32];         // 0x00: Null-terminated broker symbol (e.g., "XAUUSD.raw\0")
    uint8_t     side;               // 0x20: OrderSide (0: BUY, 1: SELL)
    uint8_t     action_type;        // 0x21: ActionType (1: MARKET, 2: LIMIT, etc.)
    uint8_t     _padding_1[6];      // 0x22 - 0x27: Natural alignment padding
    double      volume;             // 0x28: Lot size (e.g., 0.50)
    double      price;              // 0x30: Requested entry price (or 0.0 for pure market)
    double      stop_loss;          // 0x38: Absolute Stop Loss price level

    // =========================================================================
    // CACHE LINE 1 (Bytes 0x40 - 0x7F): Risk Caps, Identifiers & Telemetry
    // =========================================================================
    double      take_profit;        // 0x40: Absolute Take Profit price level
    double      slippage_cap_pips;  // 0x48: Maximum permitted slippage in pips
    uint64_t    magic_number;       // 0x50: MetaTrader unique EA magic number
    uint64_t    order_ticket;       // 0x58: Target ticket (for MODIFY, CLOSE, CANCEL)
    uint64_t    signal_id;          // 0x60: Pipvane UUID/Idempotency token hash
    uint64_t    timestamp_ns;       // 0x68: Dispatch timestamp (UTC nanoseconds)
    uint32_t    flags;              // 0x70: Execution flags (Bit 0: IOC, Bit 1: FOK, Bit 2: Allow Partial)
    uint32_t    crc32;              // 0x74: IEEE 802.3 CRC32 of bytes 0x00 to 0x73
    uint64_t    reserved;           // 0x78 - 0x7F: Future expansion padding
};

#pragma pack(pop)

// Static compile-time verification of exact binary byte layout
static_assert(sizeof(ShmRingBufferHeader) == 256, "ShmRingBufferHeader size must be exactly 256 bytes");
static_assert(offsetof(ShmRingBufferHeader, write_head) == 64, "write_head must align to Cache Line 1 (0x40)");
static_assert(offsetof(ShmRingBufferHeader, read_tail) == 128, "read_tail must align to Cache Line 2 (0x80)");
static_assert(offsetof(ShmRingBufferHeader, event_signal_count) == 192, "event_signal_count must align to Cache Line 3 (0xC0)");

static_assert(sizeof(OrderIntent) == 128, "OrderIntent size must be exactly 128 bytes");
static_assert(offsetof(OrderIntent, take_profit) == 64, "take_profit must align to Cache Line 1 (0x40)");
static_assert(offsetof(OrderIntent, crc32) == 116, "crc32 must reside at offset 0x74 (116)");

// =============================================================================
// 2. Named Pipe Wire Framing Header & Payloads
// =============================================================================
#pragma pack(push, 4)

struct NamedPipeFrameHeader {
    uint32_t magic;         // 0x00: 0x50564E50 ("PVNP" - PipVane Named Pipe)
    uint16_t opcode;        // 0x04: Message Opcode (PipeOpcode)
    uint16_t flags;         // 0x06: Framing flags (Bit 0: Compressed, Bit 1: Response Required)
    uint32_t payload_len;   // 0x08: Byte length of following payload
    uint32_t seq_id;        // 0x0C: Monotonically increasing sequence ID
    uint64_t timestamp_ns;  // 0x10: Epoch timestamp in UTC nanoseconds
    uint32_t crc32;         // 0x18: CRC32 checksum over the payload bytes
    uint32_t reserved;      // 0x1C: Alignment padding (Total Header = 32 bytes)
};

#pragma pack(pop)

static_assert(sizeof(NamedPipeFrameHeader) == 32, "NamedPipeFrameHeader size must be exactly 32 bytes");

#pragma pack(push, 8)

// Opcode: MSG_INIT_HELLO (0x0001) - Terminal -> Engine
struct MsgInitHelloPayload {
    char     terminal_id[32];      // Unique Pipvane terminal UUID
    uint8_t  platform_type;        // 0: MetaTrader 4, 1: MetaTrader 5
    uint8_t  is_portable;          // 1: Running in /portable mode, 0: standard
    uint16_t reserved;             // Alignment padding
    uint32_t terminal_build;       // e.g., 1420 (MT4) or 4150 (MT5)
    uint32_t dll_bridge_version;   // e.g., 0x03000001 (v3.0.1)
    uint64_t account_login;        // Broker account login number
    char     broker_server[64];    // Broker server name (e.g., "ICMarketsSC-Live01")
    char     company_name[64];     // Broker company name
    char     shm_segment_name[64]; // Associated Shared Memory object name
    uint32_t shm_capacity;         // Verified SHM slot capacity (1024)
};

// Opcode: MSG_CAPABILITIES_RESP (0x0003) - Terminal -> Engine
struct MsgCapabilitiesPayload {
    uint8_t  supports_hedging;       // 1: Hedging allowed, 0: Netting account
    uint8_t  is_fifo_enforced;       // 1: US NFA FIFO rule enforced, 0: standard
    uint8_t  trade_allowed;          // 1: Expert Advisor trading enabled, 0: blocked
    uint8_t  dll_allowed;            // 1: DLL imports allowed, 0: blocked
    uint8_t  execution_mode;         // 0: Market, 1: Instant, 2: Exchange
    uint8_t  reserved_flags[3];      // Alignment
    uint32_t leverage;               // Account leverage (e.g., 500 for 1:500)
    double   min_lot;                // Minimum lot size (e.g., 0.01)
    double   max_lot;                // Maximum lot size (e.g., 100.00)
    double   lot_step;               // Lot stepping precision (e.g., 0.01)
    uint32_t stop_level_points;      // Broker StopLevel in points
    uint32_t freeze_level_points;    // Broker FreezeLevel in points
    char     account_currency[16];   // e.g., "USD", "EUR"
};

// Opcode: MSG_ACCOUNT_STATE (0x0010) - Terminal -> Engine
struct MsgAccountStatePayload {
    uint64_t account_login;          // Account login identifier
    double   balance;                // Current balance
    double   equity;                 // Current real-time equity
    double   margin;                 // Used margin
    double   free_margin;            // Available free margin
    double   margin_level;           // Margin level percentage (Equity / Margin * 100)
    uint32_t open_positions_count;   // Count of currently open positions
    uint32_t pending_orders_count;   // Count of active pending orders
    uint8_t  is_connected_to_broker; // 1: Connected to trade server, 0: Disconnected
    uint8_t  reserved[7];            // Alignment padding
};
static_assert(sizeof(MsgAccountStatePayload) == 64, "MsgAccountStatePayload must be exactly 64 bytes");

// Opcode: MSG_EXECUTION_FEEDBACK (0x0020) - Terminal -> Engine
struct MsgExecutionFeedbackPayload {
    uint64_t signal_id;              // Correlation hash to original OrderIntent
    uint64_t order_ticket;           // Assigned broker ticket number
    uint8_t  execution_status;       // 0: FILLED, 1: PARTIAL, 2: REJECTED, 3: REQUOTE, 4: EXPIRED
    uint8_t  reserved[3];            // Padding
    uint32_t broker_retcode;         // MetaTrader RetCode (e.g., 10009 DONE, 10004 REQUOTE)
    double   requested_volume;       // Requested lot volume
    double   executed_volume;        // Actual executed lot volume
    double   requested_price;        // Price requested in intent
    double   executed_price;         // Actual executed price from broker
    double   slippage_pips;          // Measured slippage (executed - requested)
    uint64_t execution_latency_us;   // Execution time inside MT terminal in microseconds
    char     error_description[64];  // Plain-text broker rejection reason
};
static_assert(sizeof(MsgExecutionFeedbackPayload) == 136, "MsgExecutionFeedbackPayload must be exactly 136 bytes");

// Opcode: MSG_POSITIONS_SYNC (0x0011) Item & Header
struct PositionItem {
    uint64_t ticket;                 // Broker order/position ticket
    uint64_t magic_number;           // EA magic identifier
    uint64_t open_time_unix;         // Position open timestamp (UTC epoch seconds)
    char     symbol[32];             // Instrument symbol
    uint8_t  position_type;          // 0: BUY, 1: SELL
    uint8_t  reserved[7];            // Alignment padding
    double   volume;                 // Lot size
    double   open_price;             // Open price level
    double   current_price;          // Current market price level
    double   stop_loss;              // Current Stop Loss
    double   take_profit;            // Current Take Profit
    double   profit;                 // Unrealized profit in account currency
    double   swap;                   // Cumulative swap
    double   commission;             // Cumulative commission
    char     comment[32];            // Position comment
};

struct MsgPositionsSyncPayloadHeader {
    uint64_t account_login;          // Broker account login
    uint32_t position_count;         // Number of PositionItem records following
    uint32_t reserved;               // Alignment padding
};

// Opcode: MSG_POSITION_UPDATE (0x0012)
struct MsgPositionUpdatePayload {
    uint64_t     account_login;
    uint8_t      update_action;      // 0: OPENED, 1: MODIFIED, 2: CLOSED
    uint8_t      reserved[7];
    PositionItem position;
};

#pragma pack(pop)

// =============================================================================
// 3. Low-Latency High-Resolution Utilities
// =============================================================================

// Fast IEEE 802.3 CRC32 (polynomial 0xEDB88320)
uint32_t CalculateCRC32(const void* data, size_t length);

// High-resolution UTC epoch timestamp in nanoseconds
uint64_t GetCurrentTimestampNs();

// Windows CNG SHA-256 cryptographic file hashing (FR-6.3 & FR-6.4)
bool ComputeFileSHA256(const wchar_t* filePath, std::string& outHexHash);

// =============================================================================
// 4. Core C++ IPC Bridge Engine Interface
// =============================================================================
class IpcBridge {
public:
    static IpcBridge& Instance();

    bool Initialize(
        const wchar_t* terminal_id,
        PlatformType platform,
        uint32_t terminal_build,
        bool is_portable,
        uint64_t account_login,
        const wchar_t* broker_server,
        const wchar_t* company_name
    );

    void Shutdown();
    bool IsConnected() const;

    // Lock-Free Fast Path: Polling OrderIntent from Shared Memory (<50 us)
    // Non-blocking; returns immediately (0 if empty, 1 if popped, -1 if corrupted).
    int PollOrderIntent(OrderIntent* out_intent, uint64_t* out_transit_latency_ns);

    // Hybrid Latency: Spin-Wait with OS Event fallback
    int WaitForOrderIntent(uint32_t timeout_ms, OrderIntent* out_intent, uint64_t* out_transit_latency_ns);

    // Asynchronous Non-Blocking Named Pipe Control & Telemetry Reporting (<100 ns enqueue)
    bool SendAccountState(const MsgAccountStatePayload& state);
    bool SendExecutionFeedback(const MsgExecutionFeedbackPayload& feedback);
    bool SendCapabilities(const MsgCapabilitiesPayload& caps);
    bool SendPositionsSync(uint64_t account_login, const PositionItem* items, uint32_t count);
    bool SendPositionUpdate(uint64_t account_login, uint8_t action, const PositionItem& item);

    // Direct synchronous send (used for initial handshake hello)
    bool SendFrameDirect(uint16_t opcode, const void* payload, uint32_t payload_len, uint16_t flags = 0);

    // Telemetry accessors
    uint64_t GetLastTransitLatencyNs() const;
    uint64_t GetProcessedIntentsCount() const;
    uint64_t GetDroppedIntentsCount() const;
    const char* GetLastErrorMessage() const;

private:
    IpcBridge();
    ~IpcBridge();

    // Prevent copy/move
    IpcBridge(const IpcBridge&) = delete;
    IpcBridge& operator=(const IpcBridge&) = delete;

    // Shared Memory internal state
    HANDLE               m_hShmMap;
    void*                m_pShmView;
    ShmRingBufferHeader* m_pShmHeader;
    OrderIntent*         m_pShmSlots;
    HANDLE               m_hShmNotifyEvent;

    // Named Pipe internal state
    HANDLE               m_hPipe;
    std::wstring         m_pipeName;
    std::wstring         m_shmName;
    std::wstring         m_terminalId;
    std::atomic<bool>    m_isConnected;
    std::atomic<bool>    m_isRunning;
    std::atomic<uint32_t> m_pipeSequence;
    CRITICAL_SECTION     m_pipeLock;

    // Asynchronous Outbound Telemetry Queue
    SRWLOCK              m_outboundLock;
    HANDLE               m_hOutboundEvent;
    std::vector<uint8_t> m_outboundRingBuffer;
    size_t               m_outboundWriteOffset;
    size_t               m_outboundReadOffset;
    size_t               m_outboundOccupied;

    // Background listener & streaming worker thread
    HANDLE               m_hPipeWorkerThread;
    static DWORD WINAPI  PipeWorkerThreadProc(LPVOID lpParam);
    void                 RunPipeWorker();

    // Internal asynchronous enqueue
    bool EnqueueOutboundFrame(uint16_t opcode, const void* payload, uint32_t payload_len, uint16_t flags = 0);
    void FlushOutboundQueue();

    // Cached telemetry & diagnostics
    std::atomic<uint64_t> m_lastTransitLatencyNs;
    MsgCapabilitiesPayload m_cachedCapabilities;
    bool                   m_hasCachedCapabilities;
    char                   m_lastError[256];

    void SetLastError(const char* fmt, ...);
};

} // namespace pipvane

// =============================================================================
// 5. C DLL Exports for MetaTrader 5 (__declspec(dllexport) WINAPI)
// =============================================================================
extern "C" {

/**
 * MT5 Primary Direct Exports (Sprint 10 Directive)
 */

/**
 * Non-blocking polling function for MQL5: retrieves the next OrderIntent from Shared Memory.
 * Returns:
 *   1 : OrderIntent retrieved and copied to out_intent
 *   0 : Ring buffer empty (no pending orders)
 *  -1 : CRC32 corruption detected; slot was discarded
 */
PIPVANE_API int PIPVANE_CALL GetNextOrderIntent(pipvane::OrderIntent* out_intent);

/**
 * Non-blocking state telemetry reporter: enqueues current account financial metrics.
 * Returns 0 on success, negative on error.
 */
PIPVANE_API int PIPVANE_CALL ReportTerminalState(
    uint64_t account_login,
    double   balance,
    double   equity,
    double   margin,
    double   free_margin,
    double   margin_level,
    int      open_positions_count,
    int      pending_orders_count,
    int      is_connected_to_broker
);

/**
 * Non-blocking execution feedback reporter: enqueues trade fill/slippage results.
 * Returns 0 on success, negative on error.
 */
PIPVANE_API int PIPVANE_CALL ReportExecutionFill(
    uint64_t       signal_id,
    uint64_t       order_ticket,
    int            execution_status,
    uint32_t       broker_retcode,
    double         requested_volume,
    double         executed_volume,
    double         requested_price,
    double         executed_price,
    double         slippage_pips,
    uint64_t       execution_latency_us,
    const wchar_t* error_description
);

/**
 * Non-blocking position snapshot reporter: enqueues active position array.
 */
PIPVANE_API int PIPVANE_CALL ReportPositionsSnapshot(
    uint64_t                     account_login,
    const pipvane::PositionItem* positions_array,
    int                          position_count
);

/**
 * Life-Cycle Management Functions
 */
PIPVANE_API int PIPVANE_CALL InitializeBridgeW(
    const wchar_t* terminal_id,
    int            platform_type,
    int            terminal_build,
    int            is_portable,
    uint64_t       account_login,
    const wchar_t* broker_server,
    const wchar_t* company_name
);

PIPVANE_API int PIPVANE_CALL InitializeBridgeA(
    const char* terminal_id,
    int         platform_type,
    int         terminal_build,
    int         is_portable,
    uint64_t    account_login,
    const char* broker_server,
    const char* company_name
);

PIPVANE_API void PIPVANE_CALL ShutdownBridge();
PIPVANE_API int  PIPVANE_CALL IsBridgeConnected();

/**
 * Legacy & Multi-Language Aliases (Sprint 3 Compatibility)
 */
PIPVANE_API int  PIPVANE_CALL Pipvane_InitW(
    const wchar_t* terminal_id,
    int            platform_type,
    int            terminal_build,
    int            is_portable,
    uint64_t       account_login,
    const wchar_t* broker_server,
    const wchar_t* company_name
);

PIPVANE_API int  PIPVANE_CALL Pipvane_InitA(
    const char* terminal_id,
    int         platform_type,
    int         terminal_build,
    int         is_portable,
    uint64_t    account_login,
    const char* broker_server,
    const char* company_name
);

PIPVANE_API void PIPVANE_CALL Pipvane_Shutdown();
PIPVANE_API int  PIPVANE_CALL Pipvane_IsConnected();

PIPVANE_API int  PIPVANE_CALL Pipvane_PollOrderIntent(pipvane::OrderIntent* out_intent);

PIPVANE_API int  PIPVANE_CALL Pipvane_PollOrderIntentDirect(
    char*     out_symbol,
    int       symbol_buf_len,
    int*      out_side,
    int*      out_action_type,
    double*   out_volume,
    double*   out_price,
    double*   out_stop_loss,
    double*   out_take_profit,
    double*   out_slippage_cap_pips,
    uint64_t* out_magic_number,
    uint64_t* out_order_ticket,
    uint64_t* out_signal_id,
    uint32_t* out_flags
);

PIPVANE_API int  PIPVANE_CALL Pipvane_WaitForOrderIntent(
    uint32_t              timeout_ms,
    pipvane::OrderIntent* out_intent
);

PIPVANE_API int  PIPVANE_CALL Pipvane_SendAccountState(
    uint64_t account_login,
    double   balance,
    double   equity,
    double   margin,
    double   free_margin,
    double   margin_level,
    int      open_positions_count,
    int      pending_orders_count,
    int      is_connected_to_broker
);

PIPVANE_API int  PIPVANE_CALL Pipvane_SendExecutionFeedback(
    uint64_t       signal_id,
    uint64_t       order_ticket,
    int            execution_status,
    uint32_t       broker_retcode,
    double         requested_volume,
    double         executed_volume,
    double         requested_price,
    double         executed_price,
    double         slippage_pips,
    uint64_t       execution_latency_us,
    const wchar_t* error_description
);

PIPVANE_API int  PIPVANE_CALL Pipvane_SendCapabilities(
    int            supports_hedging,
    int            is_fifo_enforced,
    int            trade_allowed,
    int            dll_allowed,
    int            execution_mode,
    uint32_t       leverage,
    double         min_lot,
    double         max_lot,
    double         lot_step,
    uint32_t       stop_level_points,
    uint32_t       freeze_level_points,
    const wchar_t* account_currency
);

PIPVANE_API int  PIPVANE_CALL Pipvane_SendPositionsSync(
    uint64_t                     account_login,
    const pipvane::PositionItem* positions_array,
    int                          position_count
);

PIPVANE_API int  PIPVANE_CALL Pipvane_VerifyFileSHA256(
    const wchar_t* file_path,
    const char*    expected_hex_hash
);

PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetLastTransitLatencyNs();
PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetProcessedIntentsCount();
PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetDroppedIntentsCount();
PIPVANE_API const char* PIPVANE_CALL Pipvane_GetLastError();

} // extern "C"

#endif // PIPVANE_IPC_BRIDGE_H
