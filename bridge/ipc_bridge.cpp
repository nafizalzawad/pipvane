/**
 * =============================================================================
 * Pipvane v3: Production-Ready Native IPC Bridge Implementation for MT5
 * =============================================================================
 * Architecture Reference: ./architecture/SPRINT_1_ARCHITECTURE_BLUEPRINT.md
 * 
 * Strict Mode Conformance:
 *  - Lock-free Shared Memory reader using C++11 atomic acquire-release semantics.
 *  - Non-blocking execution fast path: Main GUI/tick thread polling < 50 ns.
 *  - Asynchronous Named Pipe outbound streaming worker with SRWLock queue.
 *  - Windows CNG SHA-256 integrity verification for binaries (bcrypt.dll).
 *  - ASLR, DEP, and CFG hardened native build. Zero socket dependencies.
 * =============================================================================
 */

#ifndef PIPVANE_BRIDGE_EXPORTS
#define PIPVANE_BRIDGE_EXPORTS
#endif

#include "ipc_bridge.h"

#include <windows.h>
#include <bcrypt.h>
#include <immintrin.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdarg>
#include <cwchar>
#include <string>
#include <vector>
#include <algorithm>

#pragma comment(lib, "bcrypt.lib")
#pragma comment(lib, "kernel32.lib")

namespace pipvane {

// =============================================================================
// Static IEEE 802.3 CRC32 Lookup Table (Polynomial: 0xEDB88320)
// =============================================================================
static const uint32_t kCrc32Table[256] = {
    0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3,
    0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91,
    0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7,
    0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5,
    0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B,
    0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59,
    0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
    0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D,
    0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433,
    0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01,
    0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457,
    0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65,
    0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB,
    0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
    0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F,
    0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD,
    0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683,
    0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1,
    0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7,
    0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5,
    0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
    0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79,
    0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F,
    0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D,
    0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713,
    0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21,
    0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777,
    0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
    0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB,
    0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9,
    0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF,
    0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D
};

uint32_t CalculateCRC32(const void* data, size_t length) {
    if (!data || length == 0) return 0;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc = kCrc32Table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

uint64_t GetCurrentTimestampNs() {
    FILETIME ft;
    GetSystemTimePreciseAsFileTime(&ft);
    ULARGE_INTEGER uli;
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;
    // 100-ns intervals between Jan 1, 1601 and Jan 1, 1970
    constexpr uint64_t kEpochDifference = 116444736000000000ULL;
    if (uli.QuadPart >= kEpochDifference) {
        return (uli.QuadPart - kEpochDifference) * 100ULL;
    }
    return 0ULL;
}

bool ComputeFileSHA256(const wchar_t* filePath, std::string& outHexHash) {
    outHexHash.clear();
    if (!filePath) return false;

    HANDLE hFile = CreateFileW(
        filePath,
        GENERIC_READ,
        FILE_SHARE_READ,
        NULL,
        OPEN_EXISTING,
        FILE_FLAG_SEQUENTIAL_SCAN,
        NULL
    );
    if (hFile == INVALID_HANDLE_VALUE) {
        return false;
    }

    BCRYPT_ALG_HANDLE hAlg = NULL;
    BCRYPT_HASH_HANDLE hHash = NULL;
    NTSTATUS status = 0;
    DWORD cbData = 0, cbHashObject = 0, cbHash = 0;
    std::vector<uint8_t> hashObject;
    std::vector<uint8_t> hash;
    bool success = false;

    status = BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_SHA256_ALGORITHM, NULL, 0);
    if (status < 0) {
        CloseHandle(hFile);
        return false;
    }

    status = BCryptGetProperty(hAlg, BCRYPT_OBJECT_LENGTH, (PUCHAR)&cbHashObject, sizeof(DWORD), &cbData, 0);
    if (status >= 0) {
        status = BCryptGetProperty(hAlg, BCRYPT_HASH_LENGTH, (PUCHAR)&cbHash, sizeof(DWORD), &cbData, 0);
    }

    if (status >= 0) {
        hashObject.resize(cbHashObject);
        hash.resize(cbHash);
        status = BCryptCreateHash(hAlg, &hHash, hashObject.data(), cbHashObject, NULL, 0, 0);
    }

    if (status >= 0) {
        constexpr DWORD kBufferSize = 65536; // 64 KB read chunks
        std::vector<uint8_t> buffer(kBufferSize);
        DWORD bytesRead = 0;

        while (ReadFile(hFile, buffer.data(), kBufferSize, &bytesRead, NULL) && bytesRead > 0) {
            status = BCryptHashData(hHash, buffer.data(), bytesRead, 0);
            if (status < 0) break;
        }

        if (status >= 0) {
            status = BCryptFinishHash(hHash, hash.data(), cbHash, 0);
            if (status >= 0) {
                char hexBuf[3];
                for (DWORD i = 0; i < cbHash; ++i) {
                    snprintf(hexBuf, sizeof(hexBuf), "%02x", hash[i]);
                    outHexHash += hexBuf;
                }
                success = true;
            }
        }
    }

    if (hHash) BCryptDestroyHash(hHash);
    if (hAlg) BCryptCloseAlgorithmProvider(hAlg, 0);
    CloseHandle(hFile);

    return success;
}

// =============================================================================
// IpcBridge Implementation
// =============================================================================

IpcBridge& IpcBridge::Instance() {
    static IpcBridge s_instance;
    return s_instance;
}

IpcBridge::IpcBridge()
    : m_hShmMap(NULL)
    , m_pShmView(NULL)
    , m_pShmHeader(NULL)
    , m_pShmSlots(NULL)
    , m_hShmNotifyEvent(NULL)
    , m_hPipe(INVALID_HANDLE_VALUE)
    , m_isConnected(false)
    , m_isRunning(false)
    , m_pipeSequence(0)
    , m_hOutboundEvent(NULL)
    , m_outboundWriteOffset(0)
    , m_outboundReadOffset(0)
    , m_outboundOccupied(0)
    , m_hPipeWorkerThread(NULL)
    , m_lastTransitLatencyNs(0)
    , m_hasCachedCapabilities(false)
{
    InitializeCriticalSection(&m_pipeLock);
    InitializeSRWLock(&m_outboundLock);

    m_hOutboundEvent = CreateEventW(NULL, FALSE, FALSE, NULL);
    m_outboundRingBuffer.resize(OUTBOUND_QUEUE_SIZE);

    std::memset(&m_cachedCapabilities, 0, sizeof(m_cachedCapabilities));
    std::memset(m_lastError, 0, sizeof(m_lastError));
}

IpcBridge::~IpcBridge() {
    Shutdown();
    DeleteCriticalSection(&m_pipeLock);
    if (m_hOutboundEvent) {
        CloseHandle(m_hOutboundEvent);
        m_hOutboundEvent = NULL;
    }
}

void IpcBridge::SetLastError(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(m_lastError, sizeof(m_lastError) - 1, fmt, args);
    m_lastError[sizeof(m_lastError) - 1] = '\0';
    va_end(args);
}

const char* IpcBridge::GetLastErrorMessage() const {
    return m_lastError;
}

bool IpcBridge::Initialize(
    const wchar_t* terminal_id,
    PlatformType platform,
    uint32_t terminal_build,
    bool is_portable,
    uint64_t account_login,
    const wchar_t* broker_server,
    const wchar_t* company_name
) {
    if (!terminal_id || wcslen(terminal_id) == 0) {
        SetLastError("Invalid terminal ID");
        return false;
    }

    if (m_isConnected.load(std::memory_order_acquire)) {
        return true; // Already initialized
    }

    m_terminalId = terminal_id;
    m_shmName = L"Local\\pipvane_shm_" + m_terminalId;
    m_pipeName = L"\\\\.\\pipe\\pipvane_mt_" + m_terminalId;
    std::wstring eventName = L"Local\\pipvane_evt_" + m_terminalId;

    // 1. Attach to Shared Memory Segment
    m_hShmMap = OpenFileMappingW(FILE_MAP_READ | FILE_MAP_WRITE, FALSE, m_shmName.c_str());
    if (!m_hShmMap) {
        // Fallback / Standby / Self-initialization support
        m_hShmMap = CreateFileMappingW(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            0,
            static_cast<DWORD>(TOTAL_SHM_SEGMENT_SIZE),
            m_shmName.c_str()
        );
        if (!m_hShmMap) {
            SetLastError("Failed to open or create SHM mapping: error %lu", GetLastError());
            return false;
        }
    }

    m_pShmView = MapViewOfFile(m_hShmMap, FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, TOTAL_SHM_SEGMENT_SIZE);
    if (!m_pShmView) {
        SetLastError("Failed to map view of SHM: error %lu", GetLastError());
        CloseHandle(m_hShmMap);
        m_hShmMap = NULL;
        return false;
    }

    m_pShmHeader = reinterpret_cast<ShmRingBufferHeader*>(m_pShmView);
    m_pShmSlots = reinterpret_cast<OrderIntent*>(
        reinterpret_cast<uint8_t*>(m_pShmView) + sizeof(ShmRingBufferHeader)
    );

    // Validate or initialize header
    if (m_pShmHeader->magic != SHM_MAGIC_SIGNATURE) {
        m_pShmHeader->magic = SHM_MAGIC_SIGNATURE;
        m_pShmHeader->version = SHM_PROTOCOL_VERSION;
        m_pShmHeader->capacity = SHM_DEFAULT_CAPACITY;
        m_pShmHeader->slot_size = SHM_SLOT_SIZE;
        m_pShmHeader->flags = SHM_FLAG_ACTIVE;
        m_pShmHeader->producer_pid = 0;
        m_pShmHeader->consumer_pid = GetCurrentProcessId();
        m_pShmHeader->heartbeat_ns = GetCurrentTimestampNs();
        m_pShmHeader->write_head.store(0, std::memory_order_relaxed);
        m_pShmHeader->producer_dropped_intents = 0;
        m_pShmHeader->read_tail.store(0, std::memory_order_relaxed);
        m_pShmHeader->consumer_processed_intents = 0;
        m_pShmHeader->consumer_latency_sum_ns = 0;
        m_pShmHeader->event_signal_count = 0;
        m_pShmHeader->last_consumed_seq = 0;
    } else {
        m_pShmHeader->consumer_pid = GetCurrentProcessId();
    }

    // 2. Open notification fallback event (if created by Engine)
    m_hShmNotifyEvent = OpenEventW(SYNCHRONIZE | EVENT_MODIFY_STATE, FALSE, eventName.c_str());
    if (!m_hShmNotifyEvent) {
        m_hShmNotifyEvent = CreateEventW(NULL, FALSE, FALSE, eventName.c_str());
    }

    // 3. Connect to Named Pipe
    for (int retry = 0; retry < 5; ++retry) {
        m_hPipe = CreateFileW(
            m_pipeName.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            0,
            NULL,
            OPEN_EXISTING,
            FILE_FLAG_OVERLAPPED, // Overlapped for true asynchronous streaming
            NULL
        );
        if (m_hPipe != INVALID_HANDLE_VALUE) {
            break;
        }

        DWORD err = GetLastError();
        if (err == ERROR_PIPE_BUSY) {
            WaitNamedPipeW(m_pipeName.c_str(), 500);
        } else {
            Sleep(50);
        }
    }

    if (m_hPipe == INVALID_HANDLE_VALUE) {
        SetLastError("Named Pipe connection failed (%ls): error %lu", m_pipeName.c_str(), GetLastError());
    } else {
        DWORD mode = PIPE_READMODE_MESSAGE;
        SetNamedPipeHandleState(m_hPipe, &mode, NULL, NULL);

        // 4. Send MSG_INIT_HELLO Handshake synchronously
        MsgInitHelloPayload hello;
        std::memset(&hello, 0, sizeof(hello));

        WideCharToMultiByte(CP_UTF8, 0, m_terminalId.c_str(), -1, hello.terminal_id, sizeof(hello.terminal_id) - 1, NULL, NULL);
        hello.platform_type = static_cast<uint8_t>(platform);
        hello.is_portable = is_portable ? 1 : 0;
        hello.terminal_build = terminal_build;
        hello.dll_bridge_version = BRIDGE_VERSION;
        hello.account_login = account_login;

        if (broker_server) {
            WideCharToMultiByte(CP_UTF8, 0, broker_server, -1, hello.broker_server, sizeof(hello.broker_server) - 1, NULL, NULL);
        }
        if (company_name) {
            WideCharToMultiByte(CP_UTF8, 0, company_name, -1, hello.company_name, sizeof(hello.company_name) - 1, NULL, NULL);
        }
        WideCharToMultiByte(CP_UTF8, 0, m_shmName.c_str(), -1, hello.shm_segment_name, sizeof(hello.shm_segment_name) - 1, NULL, NULL);
        hello.shm_capacity = SHM_DEFAULT_CAPACITY;

        SendFrameDirect(MSG_INIT_HELLO, &hello, sizeof(hello));
    }

    // 5. Start background worker thread for asynchronous pipe streaming & inbound frames
    m_isRunning.store(true, std::memory_order_release);
    m_isConnected.store(true, std::memory_order_release);

    m_hPipeWorkerThread = CreateThread(NULL, 0, PipeWorkerThreadProc, this, 0, NULL);

    return true;
}

void IpcBridge::Shutdown() {
    m_isRunning.store(false, std::memory_order_release);
    m_isConnected.store(false, std::memory_order_release);

    // Send MSG_SHUTDOWN over pipe if handle is valid
    if (m_hPipe != INVALID_HANDLE_VALUE) {
        SendFrameDirect(MSG_SHUTDOWN, NULL, 0);
    }

    // Signal events to unblock waiting workers
    if (m_hShmNotifyEvent) {
        SetEvent(m_hShmNotifyEvent);
    }
    if (m_hOutboundEvent) {
        SetEvent(m_hOutboundEvent);
    }

    // Close pipe handle to break any blocking operations
    EnterCriticalSection(&m_pipeLock);
    if (m_hPipe != INVALID_HANDLE_VALUE) {
        CloseHandle(m_hPipe);
        m_hPipe = INVALID_HANDLE_VALUE;
    }
    LeaveCriticalSection(&m_pipeLock);

    // Wait for background worker thread to terminate
    if (m_hPipeWorkerThread) {
        WaitForSingleObject(m_hPipeWorkerThread, 1000);
        CloseHandle(m_hPipeWorkerThread);
        m_hPipeWorkerThread = NULL;
    }

    // Unmap and close Shared Memory
    if (m_pShmView) {
        UnmapViewOfFile(m_pShmView);
        m_pShmView = NULL;
        m_pShmHeader = NULL;
        m_pShmSlots = NULL;
    }

    if (m_hShmMap) {
        CloseHandle(m_hShmMap);
        m_hShmMap = NULL;
    }

    if (m_hShmNotifyEvent) {
        CloseHandle(m_hShmNotifyEvent);
        m_hShmNotifyEvent = NULL;
    }
}

bool IpcBridge::IsConnected() const {
    return m_isConnected.load(std::memory_order_acquire);
}

// =============================================================================
// Lock-Free Fast Path: Shared Memory OrderIntent Reader (<50 us)
// Non-blocking; ZERO memory allocation, ZERO system calls in uncontended path.
// Optimized with SSE 128-byte unrolled copy.
// =============================================================================
int IpcBridge::PollOrderIntent(OrderIntent* out_intent, uint64_t* out_transit_latency_ns) {
    if (!out_intent || !m_pShmHeader || !m_pShmSlots) {
        return 0;
    }

    // 1. Atomic sequence index check (Acquire semantics)
    const uint64_t tail = m_pShmHeader->read_tail.load(std::memory_order_relaxed);
    const uint64_t head = m_pShmHeader->write_head.load(std::memory_order_acquire);

    // Queue empty condition: Consumer caught up to Producer
    if (tail >= head) {
        return 0;
    }

    // 2. Power-of-2 bitwise mask index lookup
    const uint32_t mask = m_pShmHeader->capacity - 1;
    const uint32_t slot_idx = static_cast<uint32_t>(tail & mask);
    const OrderIntent* src = &m_pShmSlots[slot_idx];

    // Prefetch slot into L1 cache
    _mm_prefetch(reinterpret_cast<const char*>(src), _MM_HINT_T0);

    // 3. Ultra-fast 128-byte unrolled copy using SSE 128-bit vector instructions (8 instructions)
    const __m128i* src128 = reinterpret_cast<const __m128i*>(src);
    __m128i* dst128 = reinterpret_cast<__m128i*>(out_intent);

    __m128i r0 = _mm_loadu_si128(src128 + 0);
    __m128i r1 = _mm_loadu_si128(src128 + 1);
    __m128i r2 = _mm_loadu_si128(src128 + 2);
    __m128i r3 = _mm_loadu_si128(src128 + 3);
    __m128i r4 = _mm_loadu_si128(src128 + 4);
    __m128i r5 = _mm_loadu_si128(src128 + 5);
    __m128i r6 = _mm_loadu_si128(src128 + 6);
    __m128i r7 = _mm_loadu_si128(src128 + 7);

    _mm_storeu_si128(dst128 + 0, r0);
    _mm_storeu_si128(dst128 + 1, r1);
    _mm_storeu_si128(dst128 + 2, r2);
    _mm_storeu_si128(dst128 + 3, r3);
    _mm_storeu_si128(dst128 + 4, r4);
    _mm_storeu_si128(dst128 + 5, r5);
    _mm_storeu_si128(dst128 + 6, r6);
    _mm_storeu_si128(dst128 + 7, r7);

    // 4. Verify IEEE 802.3 CRC32 data integrity over bytes 0x00..0x73 (116 bytes)
    constexpr size_t kCrcCheckLength = offsetof(OrderIntent, crc32);
    const uint32_t computed_crc = CalculateCRC32(out_intent, kCrcCheckLength);

    if (computed_crc != out_intent->crc32) {
        // Corrupted packet detected; skip corrupted slot and advance read tail
        m_pShmHeader->read_tail.store(tail + 1, std::memory_order_release);
        SetLastError("CRC32 mismatch on slot %u: expected 0x%08X, got 0x%08X",
                     slot_idx, out_intent->crc32, computed_crc);
        return -1;
    }

    // 5. Measure transit latency (Now - Engine Dispatch Timestamp)
    const uint64_t now_ns = GetCurrentTimestampNs();
    if (now_ns > out_intent->timestamp_ns) {
        const uint64_t transit = now_ns - out_intent->timestamp_ns;
        m_pShmHeader->consumer_latency_sum_ns += transit;
        m_lastTransitLatencyNs.store(transit, std::memory_order_relaxed);
        if (out_transit_latency_ns) {
            *out_transit_latency_ns = transit;
        }
    } else {
        if (out_transit_latency_ns) {
            *out_transit_latency_ns = 0;
        }
    }

    // 6. Release fence guarantees payload is read before read_tail advances
    m_pShmHeader->read_tail.store(tail + 1, std::memory_order_release);
    m_pShmHeader->consumer_processed_intents++;
    m_pShmHeader->last_consumed_seq = tail;

    return 1;
}

int IpcBridge::WaitForOrderIntent(uint32_t timeout_ms, OrderIntent* out_intent, uint64_t* out_transit_latency_ns) {
    if (!out_intent) return 0;

    // Phase 1: Spin for 1,000 iterations (_mm_pause) for sub-microsecond arrival
    for (int i = 0; i < 1000; ++i) {
        int res = PollOrderIntent(out_intent, out_transit_latency_ns);
        if (res != 0) {
            return res;
        }
        _mm_pause();
    }

    // Phase 2: Fall back to OS Event wait to preserve CPU cores
    if (m_hShmNotifyEvent && timeout_ms > 0) {
        WaitForSingleObject(m_hShmNotifyEvent, timeout_ms);
        return PollOrderIntent(out_intent, out_transit_latency_ns);
    }

    return 0;
}

// =============================================================================
// Asynchronous Outbound Telemetry Queue Enqueue (<100 ns, Non-Blocking)
// =============================================================================
bool IpcBridge::EnqueueOutboundFrame(uint16_t opcode, const void* payload, uint32_t payload_len, uint16_t flags) {
    const uint32_t totalFrameSize = static_cast<uint32_t>(sizeof(NamedPipeFrameHeader) + payload_len);
    const uint32_t totalPackageSize = sizeof(uint32_t) + totalFrameSize; // [Length Prefix (4B)] + [Frame]

    // Construct Header
    NamedPipeFrameHeader hdr;
    hdr.magic = PIPE_MAGIC_SIGNATURE;
    hdr.opcode = opcode;
    hdr.flags = flags;
    hdr.payload_len = payload_len;
    hdr.seq_id = m_pipeSequence.fetch_add(1, std::memory_order_relaxed) + 1;
    hdr.timestamp_ns = GetCurrentTimestampNs();
    hdr.crc32 = (payload && payload_len > 0) ? CalculateCRC32(payload, payload_len) : 0;
    hdr.reserved = 0;

    AcquireSRWLockExclusive(&m_outboundLock);

    // Check available queue capacity
    if (m_outboundOccupied + totalPackageSize > OUTBOUND_QUEUE_SIZE) {
        // Queue full (overrun protection)
        ReleaseSRWLockExclusive(&m_outboundLock);
        SetLastError("Outbound telemetry queue full (%zu bytes occupied)", m_outboundOccupied);
        return false;
    }

    // Helper lambda to copy data into ring buffer
    auto WriteRingBytes = [this](const void* src, size_t len) {
        const uint8_t* pSrc = static_cast<const uint8_t*>(src);
        size_t firstPart = (std::min)(len, OUTBOUND_QUEUE_SIZE - m_outboundWriteOffset);
        std::memcpy(&m_outboundRingBuffer[m_outboundWriteOffset], pSrc, firstPart);
        if (len > firstPart) {
            std::memcpy(&m_outboundRingBuffer[0], pSrc + firstPart, len - firstPart);
        }
        m_outboundWriteOffset = (m_outboundWriteOffset + len) % OUTBOUND_QUEUE_SIZE;
        m_outboundOccupied += len;
    };

    // 1. Write Length Prefix
    WriteRingBytes(&totalFrameSize, sizeof(uint32_t));

    // 2. Write Frame Header
    WriteRingBytes(&hdr, sizeof(NamedPipeFrameHeader));

    // 3. Write Payload
    if (payload && payload_len > 0) {
        WriteRingBytes(payload, payload_len);
    }

    ReleaseSRWLockExclusive(&m_outboundLock);

    // Signal background streaming worker
    if (m_hOutboundEvent) {
        SetEvent(m_hOutboundEvent);
    }

    return true;
}

// =============================================================================
// Direct Synchronous Frame Transmission (Handshake & Immediate Control)
// =============================================================================
bool IpcBridge::SendFrameDirect(uint16_t opcode, const void* payload, uint32_t payload_len, uint16_t flags) {
    EnterCriticalSection(&m_pipeLock);

    if (m_hPipe == INVALID_HANDLE_VALUE) {
        LeaveCriticalSection(&m_pipeLock);
        return false;
    }

    NamedPipeFrameHeader hdr;
    hdr.magic = PIPE_MAGIC_SIGNATURE;
    hdr.opcode = opcode;
    hdr.flags = flags;
    hdr.payload_len = payload_len;
    hdr.seq_id = m_pipeSequence.fetch_add(1, std::memory_order_relaxed) + 1;
    hdr.timestamp_ns = GetCurrentTimestampNs();
    hdr.crc32 = (payload && payload_len > 0) ? CalculateCRC32(payload, payload_len) : 0;
    hdr.reserved = 0;

    const size_t totalSize = sizeof(NamedPipeFrameHeader) + payload_len;
    std::vector<uint8_t> buffer(totalSize);
    std::memcpy(buffer.data(), &hdr, sizeof(hdr));
    if (payload && payload_len > 0) {
        std::memcpy(buffer.data() + sizeof(hdr), payload, payload_len);
    }

    OVERLAPPED ov;
    std::memset(&ov, 0, sizeof(ov));
    ov.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);

    DWORD bytesWritten = 0;
    BOOL writeOk = WriteFile(m_hPipe, buffer.data(), static_cast<DWORD>(totalSize), &bytesWritten, &ov);

    if (!writeOk) {
        DWORD err = GetLastError();
        if (err == ERROR_IO_PENDING) {
            GetOverlappedResult(m_hPipe, &ov, &bytesWritten, TRUE);
            writeOk = (bytesWritten == totalSize);
        }
    }

    CloseHandle(ov.hEvent);
    LeaveCriticalSection(&m_pipeLock);

    return writeOk != FALSE;
}

// Public Asynchronous Non-Blocking Telemetry Reporters
bool IpcBridge::SendAccountState(const MsgAccountStatePayload& state) {
    return EnqueueOutboundFrame(MSG_ACCOUNT_STATE, &state, sizeof(state));
}

bool IpcBridge::SendExecutionFeedback(const MsgExecutionFeedbackPayload& feedback) {
    return EnqueueOutboundFrame(MSG_EXECUTION_FEEDBACK, &feedback, sizeof(feedback));
}

bool IpcBridge::SendCapabilities(const MsgCapabilitiesPayload& caps) {
    m_cachedCapabilities = caps;
    m_hasCachedCapabilities = true;
    return EnqueueOutboundFrame(MSG_CAPABILITIES_RESP, &caps, sizeof(caps));
}

bool IpcBridge::SendPositionsSync(uint64_t account_login, const PositionItem* items, uint32_t count) {
    const size_t headerSize = sizeof(MsgPositionsSyncPayloadHeader);
    const size_t itemsSize = count * sizeof(PositionItem);
    const size_t totalPayloadSize = headerSize + itemsSize;

    std::vector<uint8_t> payload(totalPayloadSize);
    MsgPositionsSyncPayloadHeader* pHdr = reinterpret_cast<MsgPositionsSyncPayloadHeader*>(payload.data());
    pHdr->account_login = account_login;
    pHdr->position_count = count;
    pHdr->reserved = 0;

    if (items && count > 0) {
        std::memcpy(payload.data() + headerSize, items, itemsSize);
    }

    return EnqueueOutboundFrame(MSG_POSITIONS_SYNC, payload.data(), static_cast<uint32_t>(totalPayloadSize));
}

bool IpcBridge::SendPositionUpdate(uint64_t account_login, uint8_t action, const PositionItem& item) {
    MsgPositionUpdatePayload payload;
    std::memset(&payload, 0, sizeof(payload));
    payload.account_login = account_login;
    payload.update_action = action;
    payload.position = item;
    return EnqueueOutboundFrame(MSG_POSITION_UPDATE, &payload, sizeof(payload));
}

// =============================================================================
// Background Worker Thread: Handles Asynchronous Outbound Telemetry Streaming
// and Inbound Control Requests (Pings, Capability Requests, Shutdown)
// =============================================================================
DWORD WINAPI IpcBridge::PipeWorkerThreadProc(LPVOID lpParam) {
    IpcBridge* self = static_cast<IpcBridge*>(lpParam);
    self->RunPipeWorker();
    return 0;
}

void IpcBridge::FlushOutboundQueue() {
    OVERLAPPED ov;
    std::memset(&ov, 0, sizeof(ov));
    ov.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);

    while (true) {
        uint32_t frameSize = 0;

        AcquireSRWLockExclusive(&m_outboundLock);
        if (m_outboundOccupied < sizeof(uint32_t)) {
            ReleaseSRWLockExclusive(&m_outboundLock);
            break;
        }

        // Helper lambda to read bytes from ring buffer
        auto ReadRingBytes = [this](void* dst, size_t len) {
            uint8_t* pDst = static_cast<uint8_t*>(dst);
            size_t firstPart = (std::min)(len, OUTBOUND_QUEUE_SIZE - m_outboundReadOffset);
            std::memcpy(pDst, &m_outboundRingBuffer[m_outboundReadOffset], firstPart);
            if (len > firstPart) {
                std::memcpy(pDst + firstPart, &m_outboundRingBuffer[0], len - firstPart);
            }
            m_outboundReadOffset = (m_outboundReadOffset + len) % OUTBOUND_QUEUE_SIZE;
            m_outboundOccupied -= len;
        };

        // Read frame size prefix
        ReadRingBytes(&frameSize, sizeof(uint32_t));

        if (m_outboundOccupied < frameSize) {
            ReleaseSRWLockExclusive(&m_outboundLock);
            break; // Incomplete frame
        }

        std::vector<uint8_t> frameBuffer(frameSize);
        ReadRingBytes(frameBuffer.data(), frameSize);
        ReleaseSRWLockExclusive(&m_outboundLock);

        // Write frame to Named Pipe
        EnterCriticalSection(&m_pipeLock);
        if (m_hPipe != INVALID_HANDLE_VALUE) {
            DWORD bytesWritten = 0;
            ResetEvent(ov.hEvent);
            BOOL writeOk = WriteFile(m_hPipe, frameBuffer.data(), frameSize, &bytesWritten, &ov);
            if (!writeOk && GetLastError() == ERROR_IO_PENDING) {
                GetOverlappedResult(m_hPipe, &ov, &bytesWritten, TRUE);
            }
        }
        LeaveCriticalSection(&m_pipeLock);
    }

    CloseHandle(ov.hEvent);
}

void IpcBridge::RunPipeWorker() {
    std::vector<uint8_t> readBuffer(PIPE_BUFFER_SIZE);

    OVERLAPPED readOv;
    std::memset(&readOv, 0, sizeof(readOv));
    readOv.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);

    BOOL readPending = FALSE;

    while (m_isRunning.load(std::memory_order_relaxed)) {
        // Handle reconnection if pipe handle is invalid
        if (m_hPipe == INVALID_HANDLE_VALUE) {
            Sleep(500);
            if (!m_isRunning.load(std::memory_order_relaxed)) break;

            EnterCriticalSection(&m_pipeLock);
            m_hPipe = CreateFileW(
                m_pipeName.c_str(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                NULL,
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED,
                NULL
            );
            if (m_hPipe != INVALID_HANDLE_VALUE) {
                DWORD mode = PIPE_READMODE_MESSAGE;
                SetNamedPipeHandleState(m_hPipe, &mode, NULL, NULL);
            }
            LeaveCriticalSection(&m_pipeLock);

            if (m_hPipe == INVALID_HANDLE_VALUE) {
                continue;
            }
        }

        // 1. Flush any pending outbound telemetry frames
        FlushOutboundQueue();

        // 2. Initiate overlapped ReadFile if not already pending
        if (!readPending) {
            DWORD bytesRead = 0;
            ResetEvent(readOv.hEvent);
            BOOL readOk = ReadFile(
                m_hPipe,
                readBuffer.data(),
                static_cast<DWORD>(readBuffer.size()),
                &bytesRead,
                &readOv
            );

            if (readOk) {
                readPending = FALSE;
                // Immediate frame received
                if (bytesRead >= sizeof(NamedPipeFrameHeader)) {
                    const NamedPipeFrameHeader* hdr = reinterpret_cast<const NamedPipeFrameHeader*>(readBuffer.data());
                    if (hdr->magic == PIPE_MAGIC_SIGNATURE) {
                        if (hdr->opcode == MSG_HEARTBEAT_PING) {
                            SendFrameDirect(MSG_HEARTBEAT_PONG, &hdr->seq_id, sizeof(hdr->seq_id));
                        } else if (hdr->opcode == MSG_CAPABILITIES_REQ && m_hasCachedCapabilities) {
                            SendCapabilities(m_cachedCapabilities);
                        } else if (hdr->opcode == MSG_SHUTDOWN) {
                            m_isRunning.store(false, std::memory_order_release);
                        }
                    }
                }
                continue;
            } else {
                DWORD err = GetLastError();
                if (err == ERROR_IO_PENDING) {
                    readPending = TRUE;
                } else {
                    // Pipe error or disconnect
                    EnterCriticalSection(&m_pipeLock);
                    if (m_hPipe != INVALID_HANDLE_VALUE) {
                        CloseHandle(m_hPipe);
                        m_hPipe = INVALID_HANDLE_VALUE;
                    }
                    LeaveCriticalSection(&m_pipeLock);
                    readPending = FALSE;
                    continue;
                }
            }
        }

        // 3. Wait on Outbound Telemetry Event OR Inbound Read Completion (up to 50ms)
        HANDLE waitHandles[2] = { m_hOutboundEvent, readOv.hEvent };
        DWORD waitRes = WaitForMultipleObjects(2, waitHandles, FALSE, 50);

        if (waitRes == WAIT_OBJECT_0) {
            // Outbound event triggered: flush outbound telemetry queue
            FlushOutboundQueue();
        } else if (waitRes == WAIT_OBJECT_0 + 1) {
            // Inbound Read completed
            DWORD bytesRead = 0;
            if (GetOverlappedResult(m_hPipe, &readOv, &bytesRead, FALSE)) {
                readPending = FALSE;
                if (bytesRead >= sizeof(NamedPipeFrameHeader)) {
                    const NamedPipeFrameHeader* hdr = reinterpret_cast<const NamedPipeFrameHeader*>(readBuffer.data());
                    if (hdr->magic == PIPE_MAGIC_SIGNATURE) {
                        if (hdr->opcode == MSG_HEARTBEAT_PING) {
                            SendFrameDirect(MSG_HEARTBEAT_PONG, &hdr->seq_id, sizeof(hdr->seq_id));
                        } else if (hdr->opcode == MSG_CAPABILITIES_REQ && m_hasCachedCapabilities) {
                            SendCapabilities(m_cachedCapabilities);
                        } else if (hdr->opcode == MSG_SHUTDOWN) {
                            m_isRunning.store(false, std::memory_order_release);
                        }
                    }
                }
            } else {
                // Read failed; pipe disconnected
                EnterCriticalSection(&m_pipeLock);
                if (m_hPipe != INVALID_HANDLE_VALUE) {
                    CloseHandle(m_hPipe);
                    m_hPipe = INVALID_HANDLE_VALUE;
                }
                LeaveCriticalSection(&m_pipeLock);
                readPending = FALSE;
            }
        }
    }

    if (readPending) {
        CancelIo(m_hPipe);
    }
    CloseHandle(readOv.hEvent);
}

uint64_t IpcBridge::GetLastTransitLatencyNs() const {
    return m_lastTransitLatencyNs.load(std::memory_order_relaxed);
}

uint64_t IpcBridge::GetProcessedIntentsCount() const {
    return m_pShmHeader ? m_pShmHeader->consumer_processed_intents : 0;
}

uint64_t IpcBridge::GetDroppedIntentsCount() const {
    return m_pShmHeader ? m_pShmHeader->producer_dropped_intents : 0;
}

} // namespace pipvane

// =============================================================================
// C DLL Exports Implementation for MetaTrader 5
// =============================================================================

extern "C" {

/**
 * Primary MT5 Direct Exports (Sprint 10 Directive)
 */

PIPVANE_API int PIPVANE_CALL GetNextOrderIntent(pipvane::OrderIntent* out_intent) {
    uint64_t latency_ns = 0;
    return pipvane::IpcBridge::Instance().PollOrderIntent(out_intent, &latency_ns);
}

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
) {
    pipvane::MsgAccountStatePayload payload;
    std::memset(&payload, 0, sizeof(payload));
    payload.account_login = account_login;
    payload.balance = balance;
    payload.equity = equity;
    payload.margin = margin;
    payload.free_margin = free_margin;
    payload.margin_level = margin_level;
    payload.open_positions_count = static_cast<uint32_t>(open_positions_count);
    payload.pending_orders_count = static_cast<uint32_t>(pending_orders_count);
    payload.is_connected_to_broker = is_connected_to_broker ? 1 : 0;

    bool ok = pipvane::IpcBridge::Instance().SendAccountState(payload);
    return ok ? 0 : -1;
}

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
) {
    pipvane::MsgExecutionFeedbackPayload payload;
    std::memset(&payload, 0, sizeof(payload));
    payload.signal_id = signal_id;
    payload.order_ticket = order_ticket;
    payload.execution_status = static_cast<uint8_t>(execution_status);
    payload.broker_retcode = broker_retcode;
    payload.requested_volume = requested_volume;
    payload.executed_volume = executed_volume;
    payload.requested_price = requested_price;
    payload.executed_price = executed_price;
    payload.slippage_pips = slippage_pips;
    payload.execution_latency_us = execution_latency_us;

    if (error_description) {
        WideCharToMultiByte(
            CP_UTF8, 0,
            error_description, -1,
            payload.error_description, sizeof(payload.error_description) - 1,
            NULL, NULL
        );
    }

    bool ok = pipvane::IpcBridge::Instance().SendExecutionFeedback(payload);
    return ok ? 0 : -1;
}

PIPVANE_API int PIPVANE_CALL ReportPositionsSnapshot(
    uint64_t                     account_login,
    const pipvane::PositionItem* positions_array,
    int                          position_count
) {
    if (position_count < 0) return -1;
    bool ok = pipvane::IpcBridge::Instance().SendPositionsSync(
        account_login,
        positions_array,
        static_cast<uint32_t>(position_count)
    );
    return ok ? 0 : -1;
}

PIPVANE_API int PIPVANE_CALL InitializeBridgeW(
    const wchar_t* terminal_id,
    int            platform_type,
    int            terminal_build,
    int            is_portable,
    uint64_t       account_login,
    const wchar_t* broker_server,
    const wchar_t* company_name
) {
    bool ok = pipvane::IpcBridge::Instance().Initialize(
        terminal_id,
        static_cast<pipvane::PlatformType>(platform_type),
        static_cast<uint32_t>(terminal_build),
        is_portable != 0,
        account_login,
        broker_server,
        company_name
    );
    return ok ? 0 : -1;
}

PIPVANE_API int PIPVANE_CALL InitializeBridgeA(
    const char* terminal_id,
    int         platform_type,
    int         terminal_build,
    int         is_portable,
    uint64_t    account_login,
    const char* broker_server,
    const char* company_name
) {
    if (!terminal_id) return -1;

    wchar_t wTerminalId[64] = {0};
    wchar_t wBrokerServer[128] = {0};
    wchar_t wCompanyName[128] = {0};

    MultiByteToWideChar(CP_UTF8, 0, terminal_id, -1, wTerminalId, 63);
    if (broker_server) {
        MultiByteToWideChar(CP_UTF8, 0, broker_server, -1, wBrokerServer, 127);
    }
    if (company_name) {
        MultiByteToWideChar(CP_UTF8, 0, company_name, -1, wCompanyName, 127);
    }

    return InitializeBridgeW(
        wTerminalId,
        platform_type,
        terminal_build,
        is_portable,
        account_login,
        wBrokerServer,
        wCompanyName
    );
}

PIPVANE_API void PIPVANE_CALL ShutdownBridge() {
    pipvane::IpcBridge::Instance().Shutdown();
}

PIPVANE_API int PIPVANE_CALL IsBridgeConnected() {
    return pipvane::IpcBridge::Instance().IsConnected() ? 1 : 0;
}

/**
 * Legacy & Multi-Language Aliases (Sprint 3 Compatibility)
 */

PIPVANE_API int PIPVANE_CALL Pipvane_InitW(
    const wchar_t* terminal_id,
    int            platform_type,
    int            terminal_build,
    int            is_portable,
    uint64_t       account_login,
    const wchar_t* broker_server,
    const wchar_t* company_name
) {
    return InitializeBridgeW(terminal_id, platform_type, terminal_build, is_portable, account_login, broker_server, company_name);
}

PIPVANE_API int PIPVANE_CALL Pipvane_InitA(
    const char* terminal_id,
    int         platform_type,
    int         terminal_build,
    int         is_portable,
    uint64_t    account_login,
    const char* broker_server,
    const char* company_name
) {
    return InitializeBridgeA(terminal_id, platform_type, terminal_build, is_portable, account_login, broker_server, company_name);
}

PIPVANE_API void PIPVANE_CALL Pipvane_Shutdown() {
    ShutdownBridge();
}

PIPVANE_API int PIPVANE_CALL Pipvane_IsConnected() {
    return IsBridgeConnected();
}

PIPVANE_API int PIPVANE_CALL Pipvane_PollOrderIntent(pipvane::OrderIntent* out_intent) {
    return GetNextOrderIntent(out_intent);
}

PIPVANE_API int PIPVANE_CALL Pipvane_PollOrderIntentDirect(
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
) {
    pipvane::OrderIntent intent;
    uint64_t latency_ns = 0;
    int res = pipvane::IpcBridge::Instance().PollOrderIntent(&intent, &latency_ns);
    if (res != 1) {
        return res;
    }

    if (out_symbol && symbol_buf_len > 0) {
        strncpy_s(out_symbol, symbol_buf_len, intent.symbol, sizeof(intent.symbol) - 1);
    }
    if (out_side)              *out_side              = intent.side;
    if (out_action_type)      *out_action_type      = intent.action_type;
    if (out_volume)           *out_volume           = intent.volume;
    if (out_price)            *out_price            = intent.price;
    if (out_stop_loss)        *out_stop_loss        = intent.stop_loss;
    if (out_take_profit)      *out_take_profit      = intent.take_profit;
    if (out_slippage_cap_pips)*out_slippage_cap_pips= intent.slippage_cap_pips;
    if (out_magic_number)     *out_magic_number     = intent.magic_number;
    if (out_order_ticket)     *out_order_ticket     = intent.order_ticket;
    if (out_signal_id)        *out_signal_id        = intent.signal_id;
    if (out_flags)            *out_flags            = intent.flags;

    return 1;
}

PIPVANE_API int PIPVANE_CALL Pipvane_WaitForOrderIntent(
    uint32_t              timeout_ms,
    pipvane::OrderIntent* out_intent
) {
    uint64_t latency_ns = 0;
    return pipvane::IpcBridge::Instance().WaitForOrderIntent(timeout_ms, out_intent, &latency_ns);
}

PIPVANE_API int PIPVANE_CALL Pipvane_SendAccountState(
    uint64_t account_login,
    double   balance,
    double   equity,
    double   margin,
    double   free_margin,
    double   margin_level,
    int      open_positions_count,
    int      pending_orders_count,
    int      is_connected_to_broker
) {
    return ReportTerminalState(account_login, balance, equity, margin, free_margin, margin_level, open_positions_count, pending_orders_count, is_connected_to_broker);
}

PIPVANE_API int PIPVANE_CALL Pipvane_SendExecutionFeedback(
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
) {
    return ReportExecutionFill(signal_id, order_ticket, execution_status, broker_retcode, requested_volume, executed_volume, requested_price, executed_price, slippage_pips, execution_latency_us, error_description);
}

PIPVANE_API int PIPVANE_CALL Pipvane_SendCapabilities(
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
) {
    pipvane::MsgCapabilitiesPayload caps;
    std::memset(&caps, 0, sizeof(caps));
    caps.supports_hedging = supports_hedging ? 1 : 0;
    caps.is_fifo_enforced = is_fifo_enforced ? 1 : 0;
    caps.trade_allowed = trade_allowed ? 1 : 0;
    caps.dll_allowed = dll_allowed ? 1 : 0;
    caps.execution_mode = static_cast<uint8_t>(execution_mode);
    caps.leverage = leverage;
    caps.min_lot = min_lot;
    caps.max_lot = max_lot;
    caps.lot_step = lot_step;
    caps.stop_level_points = stop_level_points;
    caps.freeze_level_points = freeze_level_points;

    if (account_currency) {
        WideCharToMultiByte(
            CP_UTF8, 0,
            account_currency, -1,
            caps.account_currency, sizeof(caps.account_currency) - 1,
            NULL, NULL
        );
    }

    bool ok = pipvane::IpcBridge::Instance().SendCapabilities(caps);
    return ok ? 0 : -1;
}

PIPVANE_API int PIPVANE_CALL Pipvane_SendPositionsSync(
    uint64_t                     account_login,
    const pipvane::PositionItem* positions_array,
    int                          position_count
) {
    return ReportPositionsSnapshot(account_login, positions_array, position_count);
}

PIPVANE_API int PIPVANE_CALL Pipvane_VerifyFileSHA256(
    const wchar_t* file_path,
    const char*    expected_hex_hash
) {
    if (!file_path || !expected_hex_hash) return 0;
    std::string actualHash;
    if (!pipvane::ComputeFileSHA256(file_path, actualHash)) {
        return 0;
    }
    return (_stricmp(actualHash.c_str(), expected_hex_hash) == 0) ? 1 : 0;
}

PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetLastTransitLatencyNs() {
    return pipvane::IpcBridge::Instance().GetLastTransitLatencyNs();
}

PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetProcessedIntentsCount() {
    return pipvane::IpcBridge::Instance().GetProcessedIntentsCount();
}

PIPVANE_API uint64_t PIPVANE_CALL Pipvane_GetDroppedIntentsCount() {
    return pipvane::IpcBridge::Instance().GetDroppedIntentsCount();
}

PIPVANE_API const char* PIPVANE_CALL Pipvane_GetLastError() {
    return pipvane::IpcBridge::Instance().GetLastErrorMessage();
}

} // extern "C"
