# Pipvane v3: MetaTrader 4 & 5 Native C++ IPC Bridge

## 1. Overview & Architectural Role

The **Pipvane Native C++ IPC Bridge** is the ultra-low-latency execution link between the local Pipvane Engine (Python 3.12 / Node.js) and the MetaTrader 4 / 5 terminals. 

Conforming strictly to **ADR-001** in `architecture/SPRINT_1_ARCHITECTURE_BLUEPRINT.md`, the bridge eliminates all intermediate TCP/WebSocket serialization overhead, utilizing a **Dual-Channel Hybrid Architecture**:

```
+----------------------------------------------------------------------------------+
|                              PIPVANE LOCAL ENGINE                                |
+----------------------------------------------------------------------------------+
         |                                                          ^
         | [1] Critical Execution Path                              | [2] Control & Telemetry
         |     Lock-Free SPSC Shared Memory Ring Buffer             |     Full-Duplex Named Pipe
         |     Local\pipvane_shm_{terminal_id}                      |     \\.\pipe\pipvane_mt_{terminal_id}
         |     - Sized: 131,328 bytes (128 KB)                      |     - 32-Byte Binary Frame Header
         |     - Latency: < 50 microseconds                         |     - Opcode Catalog & IEEE CRC32
         v                                                          |
+----------------------------------------------------------------------------------+
|                   PIPVANE BRIDGE DLL (PipvaneBridge.dll)                         |
|                   Lock-Free Poller | Wire Framer | CNG SHA-256                   |
+----------------------------------------------------------------------------------+
         |                                                          ^
         | Non-Blocking Polling (< 50 ns)                           | Telemetry / Feedback
         v                                                          |
+----------------------------------------------------------------------------------+
|                     METATRADER TERMINAL (MT4 / MT5)                              |
|                   Interactive Win32 Desktop Session (FR-1.6)                     |
|                   OnTick() / OnTimer() Millisecond Execution                     |
+----------------------------------------------------------------------------------+
```

---

## 2. Shared Memory Ring Buffer Memory Map

### SPSC Circular Queue Sizing
* **Slot Capacity ($C$):** 1024 slots (power of 2)
* **Index Mask ($M$):** $C - 1 = 1023$ (`head & 1023`)
* **Slot Size:** 128 bytes (exactly 2 x 64-byte CPU cache lines)
* **Header Size:** 256 bytes (exactly 4 x 64-byte CPU cache lines)
* **Total Segment Allocation:** $256 + (1024 \times 128) = 131,328\text{ bytes}$

### Header Cache Line Layout (`ShmRingBufferHeader`)
Every section is aligned to a dedicated 64-byte cache line to completely eliminate CPU **false sharing** between the Producer (Engine) and Consumer (MT Terminal):

| Cache Line | Offset Range | Alignment | Primary Field | Writer | Purpose |
|:---|:---|:---|:---|:---|:---|
| **CL 0** | `0x00 - 0x3F` (64B) | 8-byte | `magic`, `version`, `capacity` | Producer | Static metadata & configuration |
| **CL 1** | `0x40 - 0x7F` (64B) | 64-byte (`alignas(64)`) | `write_head` (atomic), `dropped_intents` | Producer | Producer write head sequence |
| **CL 2** | `0x80 - 0xBF` (64B) | 64-byte (`alignas(64)`) | `read_tail` (atomic), `processed_intents` | Consumer | Consumer read tail sequence |
| **CL 3** | `0xC0 - 0xFF` (64B) | 64-byte (`alignas(64)`) | `event_signal_count`, `last_consumed_seq` | Shared | Diagnostic counters & OS Event fallback |

### OrderIntent POD Memory Layout (128 Bytes)

| Offset (Hex) | Offset (Dec) | Field Name | Data Type | Size (Bytes) | Cache Line | Description |
|:---|:---|:---|:---|:---|:---|:---|
| `0x00 - 0x1F` | 0 - 31 | `symbol` | `char[32]` | 32 | CL 0 | Instrument symbol (null-terminated) |
| `0x20` | 32 | `side` | `uint8_t` | 1 | CL 0 | 0 = BUY, 1 = SELL |
| `0x21` | 33 | `action_type` | `uint8_t` | 1 | CL 0 | 1 = Market, 2 = Limit, 4 = Modify, etc. |
| `0x22 - 0x27` | 34 - 39 | `_padding_1` | `uint8_t[6]` | 6 | CL 0 | Natural alignment padding |
| `0x28 - 0x2F` | 40 - 47 | `volume` | `double` | 8 | CL 0 | Order lot size (e.g., 0.50) |
| `0x30 - 0x37` | 48 - 55 | `price` | `double` | 8 | CL 0 | Target / limit price |
| `0x38 - 0x3F` | 56 - 63 | `stop_loss` | `double` | 8 | CL 0 | Absolute Stop Loss level |
| **`0x40 - 0x47`** | **64 - 71** | `take_profit` | `double` | 8 | **CL 1** | Absolute Take Profit level |
| `0x48 - 0x4F` | 72 - 79 | `slippage_cap_pips` | `double` | 8 | CL 1 | Maximum permitted slippage |
| `0x50 - 0x57` | 80 - 87 | `magic_number` | `uint64_t` | 8 | CL 1 | EA unique magic number |
| `0x58 - 0x5F` | 88 - 95 | `order_ticket` | `uint64_t` | 8 | CL 1 | Target ticket (modify/close) |
| `0x60 - 0x67` | 96 - 103 | `signal_id` | `uint64_t` | 8 | CL 1 | Global idempotency tracking token |
| `0x68 - 0x6F` | 104 - 111 | `timestamp_ns` | `uint64_t` | 8 | CL 1 | Engine dispatch UTC nanoseconds |
| `0x70 - 0x73` | 112 - 115 | `flags` | `uint32_t` | 4 | CL 1 | Execution control bitmask |
| `0x74 - 0x77` | 116 - 119 | `crc32` | `uint32_t` | 4 | CL 1 | IEEE 802.3 CRC32 of bytes 0x00..0x73 |
| `0x78 - 0x7F` | 120 - 127 | `reserved` | `uint64_t` | 8 | CL 1 | Expansion padding |

---

## 3. Named Pipe Binary Framing Protocol

* **Pipe URI:** `\\.\pipe\pipvane_mt_{terminal_id}`
* **Mode:** `PIPE_READMODE_MESSAGE`
* **Header Size:** Exactly 32 bytes (`NamedPipeFrameHeader`)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 Magic: 0x50564E50 ("PVNP")                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Opcode (u16)         |          Flags (u16)          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length (u32)                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Sequence ID (u32)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                    Timestamp Nanoseconds (u64)                +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload CRC32 (u32)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Reserved (u32)                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 Payload Bytes (0 to N bytes)...               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Opcode Summary
* `0x0001` (`MSG_INIT_HELLO`): Terminal attach greeting with build, login, broker.
* `0x0002` (`MSG_CAPABILITIES_REQ`): Engine requests broker trading limits.
* `0x0003` (`MSG_CAPABILITIES_RESP`): Terminal reports hedging, FIFO, leverage, lot limits.
* `0x0010` (`MSG_ACCOUNT_STATE`): Real-time balance, equity, margin, free margin.
* `0x0011` (`MSG_POSITIONS_SYNC`): Snapshot of all open positions on the terminal.
* `0x0012` (`MSG_POSITION_UPDATE`): Incremental order fill / modify / close notification.
* `0x0020` (`MSG_EXECUTION_FEEDBACK`): Immediate fill confirmation, requote, or error code.
* `0x0030` (`MSG_HEARTBEAT_PING`): Latency probe from Engine.
* `0x0031` (`MSG_HEARTBEAT_PONG`): Sub-millisecond echo response from DLL background worker.
* `0x00FF` (`MSG_SHUTDOWN`): Graceful detach.

---

## 4. Security Hardening Configuration

The CMake and compiler configurations enforce enterprise-grade security flags:

1. **ASLR (`/DYNAMICBASE`, `/HIGHENTROPYVA`):** Randomizes DLL load address in memory to mitigate buffer exploitation.
2. **DEP (`/NXCOMPAT`):** Marks all data pages as non-executable.
3. **Stack Canary (`/GS`):** Detects buffer overruns and halts immediately upon corruption.
4. **Control Flow Guard (`/guard:cf`):** MSVC compiler and linker exploit mitigation preventing indirect call hijacking.
5. **Zero Socket Symbols:** Static and dynamic inspection confirms `ws2_32.dll` is never referenced. Network sockets are strictly forbidden in the bridge DLL.
6. **Windows CNG SHA-256 (`ComputeFileSHA256`):** Hardware-accelerated cryptographic verification of `.ex4` binaries prior to execution (FR-6.3 & FR-6.4).

---

## 5. Non-Blocking Execution Protocol in MetaTrader

### Why Main Thread Polling Never Blocks
MetaTrader executes Expert Advisors on a single thread per chart. A blocking syscall in `OnTick()` or `OnTimer()` freezes chart rendering and delays subsequent ticks.

In `PipvaneBridge.dll`, polling Shared Memory is **strictly non-blocking**:
```cpp
const uint64_t tail = m_pShmHeader->read_tail.load(std::memory_order_relaxed);
const uint64_t head = m_pShmHeader->write_head.load(std::memory_order_acquire);
if (tail >= head) {
    return 0; // Empty! Returns in ~2 nanoseconds.
}
```
* **No Locks:** Atomic compare without mutexes or critical sections on the SHM fast path.
* **No Allocations:** Uses caller-provided stack struct.
* **No Syscalls:** Pure memory loads from mapped virtual memory.

### Background Worker Thread
The bridge automatically spawns an internal Win32 worker thread (`PipeWorkerThreadProc`) dedicated solely to handling Named Pipe incoming pings and capability requests. As a result:
* Latency probes (`MSG_HEARTBEAT_PING`) are answered in $< 1\text{ ms}$ even if the EA is executing a complex order calculation.
* The main chart UI thread never participates in pipe read loops.

---

## 6. MQL Integration Example

```mql5
#include <PipvaneBridge.mqh>

int OnInit()
{
   // Attach to IPC channels
   int res = Pipvane_InitW(
      "term_01",
      1, // MT5
      TerminalInfoInteger(TERMINAL_BUILD),
      0,
      AccountInfoInteger(ACCOUNT_LOGIN),
      AccountInfoString(ACCOUNT_SERVER),
      AccountInfoString(ACCOUNT_COMPANY)
   );
   
   if (res != 0) {
      Print("Failed to attach Pipvane IPC Bridge!");
      return INIT_FAILED;
   }
   
   // Start high-resolution 5ms polling timer
   EventSetMillisecondTimer(5);
   return INIT_SUCCEEDED;
}

void OnTimer()
{
   ShmOrderIntent intent;
   // Non-blocking poll (<50 ns)
   while (Pipvane_QuickPoll(intent)) {
      ExecuteOrder(intent);
   }
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   Pipvane_Shutdown();
}
```

---

## 7. MQL5 Production Expert Advisor (`Pipvane_MT5.mq5`)

The production-ready MT5 Expert Advisor (`Pipvane_MT5.mq5`) provides an out-of-the-box execution wrapper conforming strictly to ADR-001:

* **CTrade Integration:** Native order execution routines for Market, Limit, Stop, SL/TP Modifications, Partial/Full Closures, and Order Cancellations.
* **Smart Filling Mode Selection:** Automatically queries `SYMBOL_FILLING_MODE` to configure `ORDER_FILLING_IOC`, `ORDER_FILLING_FOK`, or `ORDER_FILLING_RETURN`, eliminating MT5 broker rejection `10030`.
* **Microsecond Latency Profiling:** Measures terminal execution time using `GetMicrosecondCount()` and calculates slippage in pips against requested entry prices.
* **Dual Event Polling (`OnTick` + `OnTimer`):**
  - Lock-free memory polling (<50 ns) executes on every incoming market tick and every 5 ms timer event.
  - Telemetry state reporting is throttled to 1,000 ms to avoid tick thread starvation during high-frequency volatility bursts.
* **Event-Driven Reconciliation:** Implements `OnTradeTransaction()` to trigger immediate account and position state pushes to the Engine on order fills and position closures.

---

## 8. MQL4 Production Expert Advisor (`Pipvane_MT4.mq4`) & 32-bit Bridge

The MT4 Expert Advisor (`Pipvane_MT4.mq4`) bridges 32-bit MetaTrader 4 terminals to the Pipvane Engine using `ipc_bridge_x86.dll`:

* **ECN Two-Step Execution:** Avoids MT4 broker Error 130 (`ERR_INVALID_STOPS`) by executing market entry first (`OrderSend` with SL=0, TP=0) and immediately modifying the ticket with Stop Loss and Take Profit via `OrderModify`.
* **Slippage Point Conversion:** Strictly handles integer slippage in points, auto-detecting 3/5 digit broker pip multipliers.
* **Transient Error Retries:** Implements backoff retries for broker busy (Error 137) and trade context busy (Error 146).
* **32-Bit Calling Convention:** C++ DLL exports use standard `__stdcall` (`WINAPI`) with `/SAFESEH` (Safe Exception Handlers) and SSE2 optimization.
* **State Telemetry Gathering:** Iterates `OrdersTotal()` with `OrderSelect()`, streaming Account Equity, Margin, Balance, and active position arrays back to the Engine.

### Compilation Commands

```bash
# Build 32-bit DLL for MT4 (produces ipc_bridge_x86.dll)
cmake -B build32 -A Win32
cmake --build build32 --config Release

# Build 64-bit DLL for MT5 (produces PipvaneBridge.dll)
cmake -B build64 -A x64
cmake --build build64 --config Release
```


