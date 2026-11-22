# Pipvane v3 - Cloud Control Plane: Identity, Licensing, Leases, Relay & Telemetry

## Overview
Thin, high-availability Cloud Control Plane for **Pipvane v3**, implementing:
* **Sprint 26 (Founder Provisioning & Support Prioritization):** Auth0 Role / Custom Claim (`Edition: Founder`), permanent `account_limit` (4 standard, 7 pro pack), and support queue prioritization in anonymous telemetry.
* **Sprint 21 (Anonymous Telemetry Pipeline):** Secure serverless ingestion endpoint (`POST /telemetry`) with aggressive data scrubbing (explicitly stripping IP addresses, User-Agents, and raw Telegram signals).
* **Sprint 17 (Encrypted Command Relay):** Secure, stateless message broker with Inbox/Outbox queues tied to `license_id` for Mobile Companion App $\leftrightarrow$ Local Engine commands, strictly enforcing Zero-Knowledge (metadata/ciphertext separation).
* **FR-7.4 & FR-7.5 (Active-Passive Lease Coordinator):** Anti-Split-Brain Enforcement via high-concurrency WebSocket server with Mutual TLS (mTLS 1.3) and RS256 JWT authentication.
* **FR-8.1 (Resilient Licensing):** 30-Day Cryptographic Licensing (strictly `exp - iat = 2,592,000` seconds).
* **FR-8.4 (Silent Refresh):** 7-Day Silent Background Refresh & Token Rotation with Anti-Replay Revocation.
* **Zero-Knowledge Architecture:** Zero access to user broker credentials, passwords, or signal contents.
* **Hardware Binding:** SHA-256 hardware device fingerprint binding preventing unauthorized license migration.

---

## Architecture & Directory Structure

```
cloud/
├── config/
│   └── auth0_tenant.json          # Auth0 Deploy CLI tenant configuration schema
├── src/
│   ├── api/
│   │   └── handlers.js            # Express/Node HTTP route handlers (/issue, /refresh, /validate, /revoke)
│   ├── coordinator/
│   │   ├── leaseCoordinator.js    # State & Lease Coordinator (30s TTL, 10s ping, failover)
│   │   ├── mutex.js               # Asynchronous KeyedMutex (zero split-brain concurrency lock)
│   │   ├── mtls.js                # Mutual TLS 1.3 certificate utilities
│   │   └── wsServer.js            # High-concurrency WebSocket server (mTLS/JWT authenticated)
│   ├── crypto/
│   │   ├── jwt.js                 # RFC 7519 & RFC 7515 RS256 JWT implementation
│   │   └── keys.js                # RSA 2048/4096-bit key management & rotation
│   ├── relay/
│   │   ├── handlers.js            # REST Handlers for /relay/command (push, pop, ack, stats)
│   │   ├── relayQueue.js          # In-Memory, DynamoDB, and Redis FIFO directional queues
│   │   ├── relayService.js        # Zero-Knowledge envelope validation & broker logic
│   │   └── wsRelay.js             # Real-time WebSocket streaming relay handler
│   ├── services/
│   │   ├── licenseService.js      # Licensing business logic, terminal caps, & silent refresh
│   │   ├── relayService.js        # E2EE Configuration Relay Sync (/relay/sync)
│   │   └── revocationStore.js     # Pluggable anti-replay store (Memory, DynamoDB, Redis)
│   ├── telemetry/
│   │   ├── handlers.js            # HTTP Handlers for /telemetry and /telemetry/metrics
│   │   ├── telemetryScrubber.js   # Strips IPs, User-Agents, Telegram text; hashes license_id
│   │   ├── telemetryService.js    # Ingestion orchestration & metrics calculation
│   │   └── telemetryStore.js      # Memory ring buffer & DynamoDB storage adapters
│   ├── lambda.js                  # AWS Lambda serverless adapter (API Gateway v1/v2)
│   ├── server.js                  # Standalone Node.js HTTP server
│   └── worker.js                  # Cloudflare Workers Fetch API adapter
├── terraform/
│   ├── auth0.tf                   # Terraform HCL Auth0 tenant, M2M client, API, roles & action
│   ├── outputs.tf                 # Terraform output attributes
│   └── variables.tf               # Terraform input variables
├── tests/
│   ├── api_endpoints.test.js      # E2E HTTP server endpoint tests
│   ├── founder_provisioning.test.js # Sprint 26 Founder cohort provisioning & support prioritization tests
│   ├── jwt_lifecycle.test.js      # Cryptographic RS256, 30-day expiry, & rotation tests
│   ├── lease_coordinator.test.js  # Lease Coordinator & concurrency race condition tests
│   ├── relay.test.js              # Sprint 17 Encrypted Command Relay & Zero-Knowledge tests
│   ├── relay_sync.test.js         # Sprint 15 E2EE Configuration Relay Sync tests
│   ├── revocation_store.test.js   # Revocation store & TTL pruning tests
│   ├── telemetry.test.js          # Sprint 21 Anonymous Telemetry Pipeline & Data Scrubbing tests
│   └── ws_server.test.js          # WebSocket server E2E tests (mTLS & JWT auth, failover)
├── package.json                   # Project metadata and test runner config
└── skill.md                       # Cloud & Backend Engineer skill profile
```

---

## Sprint 21: Telemetry Ingestion Pipeline

### Ingestion Endpoint (`POST /telemetry`)
The ingestion endpoint accepts anonymous performance data from the local execution engine.

#### Client Request Schema:
```json
{
  "license_id": "lic_prod_778899",
  "os_version": "Windows 11 Pro 23H2 (build 22631)",
  "latency_ms": 0.048,
  "crash_code": "CRASH_NONE"
}
```
*(Alternatively, clients may pass `license_id_hash` pre-hashed).*

#### Data Scrubbing Rules:
1. **IP Addresses Stripped:** Client IPs from sockets, headers (`X-Forwarded-For`, `X-Real-IP`, `CF-Connecting-IP`) are discarded immediately.
2. **User-Agents Stripped:** Browser / client User-Agent strings are never logged or stored.
3. **Raw Telegram Text Purged:** Any field containing trade signal text or messages (`raw_text`, `telegram_text`, `signal_text`, `message`, `password`) is purged with extreme prejudice.
4. **License Anonymization:** `license_id` is converted to a one-way cryptographic SHA-256 hash (`license_id_hash`). The plain-text license key is never stored in the database.

#### Persisted Record Schema (Database Store):
```json
{
  "telemetry_id": "c1f7a0b5-7798-4bb9-bf12-706f9d2e7d7a",
  "license_id_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "os_version": "Windows 11 Pro 23H2",
  "latency_ms": 0.048,
  "crash_code": "CRASH_NONE",
  "timestamp": 1770000000
}
```

---

## Sprint 17: Encrypted Command Relay

### Zero-Knowledge Envelope Specification
```json
{
  "metadata": {
    "message_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "license_id": "lic_9876543210",
    "direction": "mobile_to_engine",
    "sender_type": "mobile_app",
    "timestamp": 1770000000000,
    "ttl_seconds": 3600,
    "algorithm": "XChaCha20-Poly1305",
    "iv": "dGVzdF9ub25jZV8xMjM=",
    "tag": "dGVzdF9hdXRoX3RhZ180NTY="
  },
  "ciphertext": "k7W9JzL...[Opaque Base64 Encrypted Blob]...=="
}
```

---

## Sprint 26: Founder Cohort Provisioning & Support Prioritization

### 1. Auth0 Role & Custom Claims
* **Auth0 Role:** `Pipvane Founder Edition` (description: *"Founder cohort license - Edition: Founder with permanent 4/7 terminal entitlement and prioritized support"*).
* **Claims Injected:**
  * `edition`: `"Founder"`
  * `tier`: `"Standard"` or `"Pro"`
  * `account_limit`: `4` (permanent Standard) or `7` (if pro pack purchased)
  * `priority_support`: `true`
  * `license_id`: User's assigned license identifier

### 2. Permanent Limit Overrides
* For users in the Founder cohort, `account_limit` is permanently locked to 4 (default) or 7 (if pro pack is purchased). This entitlement cannot be downgraded below entitlement level.
* Claims and limits are preserved through the 7-day silent refresh loop (`/api/license/refresh`) and single-use token rotation.

### 3. Support Queue Prioritization
* Founder telemetry submissions automatically receive `support_queue_priority: "HIGH"`.
* The support queue view (`GET /telemetry/support-queue`) sorts Founder records to the top of the queue, ensuring zero wait times for Founder bug reports or latency queries.
* Aggregated metrics track `founder_sample_count` alongside overall sample count and percentiles.

---

## Running Automated Tests

```bash
# Run all 97 automated tests across 42 suites
npm test --prefix cloud

# Or run directly via Node.js native test runner
node --test cloud/tests/**/*.test.js
```
