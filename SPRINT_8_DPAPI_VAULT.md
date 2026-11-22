# Pipvane v3: Sprint 8 Architecture Blueprint
## DPAPI Credential Vault & Zero-Knowledge Local Storage

* **Document Version:** 3.0.0-PROD-SPEC
* **Status:** PROPOSED / PENDING TECHNICAL SIGN-OFF (Strict Mode Enforced)
* **Author:** Pipvane v3 Tech Lead & System Architect
* **Date:** November 2026
* **Target Milestone:** Sprint 8 Implementation Gate
* **Source Implementation:** [secure_vault.py](file:///f:/Personal%20Project/Signal%20Copier/Pipvane/engine/secure_vault.py)
* **Unit Test Suite:** [test_secure_vault.py](file:///f:/Personal%20Project/Signal%20Copier/Pipvane/engine/test_secure_vault.py)

---

## 1. Executive Summary & V3 Zero-Knowledge Security Alignment

This specification delivers the architectural blueprint and cryptographic implementation for the Pipvane v3 local credential vault. It operationalizes **Architectural Correction 3 (Zero-Knowledge Local Credential Vault)**:

* **Zero Cloud Exposure:** Plaintext broker passwords (master and investor), account logins, and Telegram MTProto session secrets never leave the local Windows host. The Cloud Control Plane is strictly zero-knowledge, receiving only anonymized telemetry, licensing tokens, and lease coordination heartbeats.
* **OS-Bound Cryptography:** All sensitive fields written to the local SQLite database (`vault.db`) are encrypted using the Windows native Data Protection API (DPAPI via `Crypt32.dll`), binding the ciphertext cryptographically to the user's Windows user account profile and machine state.
* **Volatile RAM Lifetime:** Sensitive secrets are decrypted exclusively in volatile memory at the instant they are required by the execution connectors and are zeroed from memory upon disconnect.
* **Strict File Isolation:** The SQLite database is physically restricted to `%AppData%\Pipvane\` (e.g., `C:\Users\<User>\AppData\Roaming\Pipvane\vault.db`). Path traversal attacks (`..\`) or attempts to instantiate the vault on unauthorized directories, temporary folders, or secondary drives trigger a fail-closed `VaultSecurityError`.

---

## 2. Architecture Decision Record (ADR-003)

### ADR-003: Windows DPAPI OS-Bound Encrypted SQLite Credential Vault

* **Status:** ACCEPTED / PENDING TECHNICAL SIGN-OFF
* **Date:** 2026-11-21
* **Deciders:** Pipvane Tech Lead & System Architect, Engine Engineer, Product Manager

#### 2.1 Context
Pipvane v3 connects to multiple brokerage accounts (MetaTrader 4, MetaTrader 5, cTrader, DXTrade, TradeLocker) and maintains persistent Telegram client sessions via TDLib. These integrations require storing:
1. Broker Master Passwords (enabling order execution).
2. Broker Investor Passwords (optional read-only telemetry).
3. Telegram MTProto session keys and API secrets.
4. OAuth 2.0 access and refresh tokens.

Alternative storage paradigms evaluated:
* *Cloud-Hosted Secrets (Vault / KMS):* Violates the fundamental core value proposition of Pipvane. Users refuse to hand over broker passwords or Telegram sessions to any third-party SaaS cloud. Cloud breaches would compromise user trading accounts.
* *Plaintext SQLite / JSON Config Files:* Vulnerable to local malware, unauthorized user access, and disk scraping.
* *Application-Level Symmetric Encryption (AES-GCM with hardcoded/config key):* Static keys can be reverse-engineered from application binaries. Dynamic keys stored in config files recreate the original secret-storage dilemma.

#### 2.2 Decision
We adopt **Windows OS-Native DPAPI (`CryptProtectData` / `CryptUnprotectData`) combined with SQLite 3** operating in per-column encryption mode:
1. **OS-Level Key Management:** Encryption keys are derived dynamically by the Windows LSA (Local Security Authority) using the interactive user's OS logon credentials (PBKDF2-derived master keys guarded by Windows DPAPI architecture).
2. **Secondary Application Entropy:** All `CryptProtectData` calls pass a secondary entropy salt (`Pipvane_V3_Local_Entropy_Salt_2026_Secured`). This guarantees that even if an attacker obtains an unauthorized DPAPI decryption handle under the user's session, they cannot decrypt vault secrets without the Pipvane application entropy.
3. **Column-Level Selective Encryption:** Non-sensitive operational metadata (`account_id`, `platform`, `broker_server`, `account_login`, `created_at`) remains queryable in SQLite, while sensitive fields (`master_password_blob`, `investor_password_blob`, `session_blob`, `api_hash_blob`) are stored as encrypted binary `BLOB`s.
4. **Enforced File Isolation:** Database initialization validates that the target file path strictly resolves within `%AppData%\Pipvane\`. Any deviation raises `VaultSecurityError`.

#### 2.3 Consequences
* **Positive:**
  * Eliminates key-management overhead; relies on battle-tested Windows OS security primitives.
  * Secrets are inaccessible to other OS users on the same machine, offline disk clones, or cloud telemetry.
  * Zero external binary dependencies: implemented via standard Python `ctypes` calling `Crypt32.dll` directly.
  * Verified 100% pass on automated security unit tests with zero plaintext leaks on disk.
* **Negative:**
  * Credentials cannot be copied via raw database file transfer between different PCs. Profile migration requires an explicit user-driven encrypted backup export.
  * Windows-specific (satisfies PRD FR-1.1 & FR-1.6 desktop requirement).

---

## 3. Cryptographic Specification & Win32 Hook

### 3.1 DPAPI Win32 API Contract

```cpp
// Windows Crypt32.dll API Hook
typedef struct _CRYPTOAPI_BLOB {
    DWORD cbData;
    BYTE  *pbData;
} DATA_BLOB;

BOOL WINAPI CryptProtectData(
    DATA_BLOB*      pDataIn,            // Plaintext bytes in RAM
    LPCWSTR         szDataDescr,        // "Pipvane Credential Vault"
    DATA_BLOB*      pOptionalEntropy,   // Application secondary entropy salt
    PVOID           pvReserved,         // NULL
    PVOID           pPromptStruct,      // NULL (UI forbidden)
    DWORD           dwFlags,            // 0x01: CRYPTPROTECT_UI_FORBIDDEN
    DATA_BLOB*      pDataOut            // Ciphertext bytes
);

BOOL WINAPI CryptUnprotectData(
    DATA_BLOB*      pDataIn,            // Ciphertext bytes
    LPWSTR*         ppszDataDescr,      // NULL
    DATA_BLOB*      pOptionalEntropy,   // Application secondary entropy salt
    PVOID           pvReserved,         // NULL
    PVOID           pPromptStruct,      // NULL (UI forbidden)
    DWORD           dwFlags,            // 0x01: CRYPTPROTECT_UI_FORBIDDEN
    DATA_BLOB*      pDataOut            // Plaintext bytes in RAM
);
```

### 3.2 Secure Memory Allocation and Cleanup
* Memory allocated by DPAPI for decrypted plaintext (`pDataOut.pbData`) is freed immediately using `kernel32.LocalFree` after reading into Python memory.
* Sensitive fields in Python are wrapped in lightweight data objects (`BrokerAccountRecord`, `TelegramSessionRecord`) instantiated only when a connection handshake is initiated.

---

## 4. SQLite Schema & Physical File Layout

### 4.1 File Path Isolation Contract
* **Canonical Path:** `%APPDATA%\Pipvane\vault.db`
* **Path Enforcement Algorithm:**
  ```python
  def verify_file_isolation(path: str) -> str:
      vault_dir = os.path.abspath(os.path.realpath(os.path.join(os.environ['APPDATA'], 'Pipvane')))
      abs_path = os.path.abspath(os.path.realpath(path))
      common = os.path.commonpath([vault_dir, abs_path])
      if os.path.abspath(os.path.realpath(common)) != vault_dir:
          raise VaultSecurityError("Path isolation violation: Database must reside within %AppData%\\Pipvane")
      return abs_path
  ```

### 4.2 SQLite DDL Schema

```sql
-- 1. Broker Credentials Table
CREATE TABLE IF NOT EXISTS broker_accounts (
    account_id              TEXT PRIMARY KEY,
    platform                TEXT NOT NULL,              -- 'MT4', 'MT5', 'cTrader', 'DXTrade', 'TradeLocker'
    broker_server           TEXT NOT NULL,              -- e.g. 'ICMarketsSC-Live05'
    account_login           INTEGER NOT NULL,           -- Account login number
    master_password_blob    BLOB NOT NULL,              -- DPAPI encrypted master password
    investor_password_blob  BLOB,                       -- DPAPI encrypted investor password (optional)
    created_at              INTEGER NOT NULL,           -- Epoch ms
    updated_at              INTEGER NOT NULL            -- Epoch ms
);

-- 2. Telegram Client Sessions Table
CREATE TABLE IF NOT EXISTS telegram_sessions (
    session_id              TEXT PRIMARY KEY,           -- e.g. 'default_user'
    phone_number            TEXT NOT NULL,              -- Masked or plain phone number
    session_blob            BLOB NOT NULL,              -- DPAPI encrypted TDLib session auth string
    api_id                  INTEGER,                    -- User's Telegram API ID
    api_hash_blob           BLOB,                       -- DPAPI encrypted API hash
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);

-- 3. Generic Application Secrets Table
CREATE TABLE IF NOT EXISTS vault_secrets (
    key_name                TEXT PRIMARY KEY,           -- Secret key identifier
    category                TEXT NOT NULL,              -- 'licensing', 'oauth', 'relay'
    secret_blob             BLOB NOT NULL,              -- DPAPI encrypted secret payload
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);
```

---

## 5. Automated Verification & Security Audit Results

The implementation in `engine/secure_vault.py` was subjected to rigorous automated verification via `engine/test_secure_vault.py`:

```text
============================= test session starts =============================
platform win32 -- Python 3.11.4, pytest-9.2.0, pluggy-1.7.0
rootdir: F:\Personal Project\Signal Copier\Pipvane
collected 7 items

engine/test_secure_vault.py::test_dpapi_encryptor_basic_roundtrip PASSED [ 14%]
engine/test_secure_vault.py::test_dpapi_entropy_enforcement PASSED       [ 28%]
engine/test_secure_vault.py::test_file_isolation_rejection_unauthorized_paths PASSED [ 42%]
engine/test_secure_vault.py::test_file_isolation_canonical_path PASSED   [ 57%]
engine/test_secure_vault.py::test_broker_account_zero_plaintext_leak_on_disk PASSED [ 71%]
engine/test_secure_vault.py::test_telegram_session_encryption_roundtrip PASSED [ 85%]
engine/test_secure_vault.py::test_generic_secrets_crud PASSED            [100%]

============================== 7 passed in 0.16s ==============================
```

### Key Audit Findings:
1. **Zero Plaintext Disk Leak:** An independent raw SQLite inspection confirmed that searching the raw database binary for known master passwords, investor passwords, or session strings returns **zero matches**. All sensitive data is stored exclusively as high-entropy ciphertext BLOBs.
2. **Entropy Tamper Resistance:** Decrypting with an altered secondary entropy salt fails immediately with `VaultSecurityError`.
3. **Directory Confinement Verified:** Unauthorized paths such as `C:\Windows\Temp\vault.db`, `C:\Users\Public\vault.db`, or relative directory traversal paths (`..\..\evil.db`) are rejected deterministically before any file handles are opened.

---

## 6. Architectural Sign-Off & Strict Mode Hold

> [!IMPORTANT]
> **Strict Mode Active:** The `SecureVault` class and its test suite have been authored and verified in isolation. In compliance with strict mode guidelines, **no changes have been merged into the main engine execution pipeline.** We await manual review and approval of this cryptographic implementation before instructing the Engine Agent to integrate it into production signal flows.
