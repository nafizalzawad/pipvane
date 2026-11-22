# Pipvane v3: 30-Day Telegram Safety Mode Soak & Chaos VPS Deployment Guide

## Overview

This guide details the procedure for deploying and orchestrating the **continuous 30-day (720-hour) soak and chaos test** for Pipvane v3's Telegram Safety Mode (FR-4.1). 

The test orchestrates **3 concurrent dummy Telegram accounts**, continuously ingests market signals without crashing, injects **randomized network socket drops every 4 to 12 hours**, records daily metrics into `ban_risk_telemetry.csv`, and **asserts zero account bans** (failing immediately upon `USER_DEACTIVATED` or `AUTH_KEY_UNREGISTERED`).

---

## 1. System Requirements & Hardware Sizing

| Metric | Minimum Specification | Recommended Specification |
| :--- | :--- | :--- |
| **OS** | Ubuntu 22.04 LTS / 24.04 LTS (x86_64) | Ubuntu 24.04 LTS (x86_64) |
| **vCPU** | 1 Core | 2 Cores |
| **RAM** | 1.0 GB | 2.0 GB |
| **Disk** | 15 GB SSD | 30 GB NVMe |
| **Python** | Python 3.11 or 3.12 | Python 3.11.x |
| **Network** | Dedicated IPv4 / Stable Outbound | 100 Mbps Port |

---

## 2. Fast-Track Provisioning (Single Command)

SSH into your remote VPS and run the automated provisioning script:

```bash
# Clone the repository (or copy workspace)
git clone https://github.com/your-org/pipvane.git /opt/pipvane
cd /opt/pipvane

# Run automated setup script
sudo bash qa/chaos/deploy_vps/setup_vps.sh
```

---

## 3. Manual Step-by-Step Deployment

If preferred or customizing for a hardened cloud environment, follow these steps:

### Step 3.1: Install System Packages
```bash
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv tmux htop jq curl
```

### Step 3.2: Create Dedicated Service User & Directories
```bash
sudo useradd -r -m -d /opt/pipvane -s /bin/bash pipvane
sudo mkdir -p /opt/pipvane/data/soak_telemetry/sessions
sudo chown -R pipvane:pipvane /opt/pipvane
```

### Step 3.3: Set Up Python Virtual Environment
```bash
sudo -u pipvane python3 -m venv /opt/pipvane/venv
sudo -u pipvane /opt/pipvane/venv/bin/pip install --upgrade pip
sudo -u pipvane /opt/pipvane/venv/bin/pip install psutil pytest pytest-asyncio flake8
```

---

## 4. Execution Methods

### Option A: Production Background Execution via `systemd` (Recommended)

1. Copy the systemd service unit file:
   ```bash
   sudo cp qa/chaos/deploy_vps/pipvane-telegram-soak.service /etc/systemd/system/
   sudo systemctl daemon-reload
   ```

2. Start and enable the service on system boot:
   ```bash
   sudo systemctl enable --now pipvane-telegram-soak
   ```

3. Verify service health:
   ```bash
   sudo systemctl status pipvane-telegram-soak
   ```

4. Stream live logs via `journalctl`:
   ```bash
   journalctl -u pipvane-telegram-soak -f
   ```

---

### Option B: Interactive Execution via `tmux`

To run interactively in an attached terminal session:

```bash
# Launch a dedicated tmux session
tmux new -s pipvane-soak

# Activate virtualenv and launch orchestrator for 30 days (720 hours)
source /opt/pipvane/venv/bin/activate
export PYTHONPATH=/opt/pipvane

python qa/chaos/soak_orchestrator.py \
  --duration-hours 720 \
  --output-dir /opt/pipvane/data/soak_telemetry

# Detach from session: Press Ctrl+B, then D
# Reattach at any time: tmux attach -t pipvane-soak
```

---

## 5. Daily Telemetry & Ban Risk Monitoring

The orchestrator continuously appends records to:
`/opt/pipvane/data/soak_telemetry/ban_risk_telemetry.csv`

### Monitored Telemetry Fields:
* `timestamp`: ISO 8601 UTC timestamp.
* `date`: YYYY-MM-DD grouping for daily trend analysis.
* `account_id`: Account profile identifier (`dummy_acc_1`, `dummy_acc_2`, `dummy_acc_3`).
* `phone_number`: Account telephone number.
* `session_uptime_seconds` & `session_uptime_hours`: Cumulative uptime.
* `connection_state`: Current TDLib state (`connectionStateReady`, `connectionStateWaitingForNetwork`).
* `flood_wait_count` & `flood_wait_total_seconds`: Count and duration of any API backoffs.
* `network_disruptions_count`: Number of randomized chaos socket disruptions injected.
* `messages_received`: Total live signals ingested.
* `messages_backfilled`: Missed signals retrieved through channel backfill post-disruption.
* `termination_code`: `RUNNING`, `DURATION_COMPLETED`, or ban error code.
* `tdlib_rss_mb` & `tdlib_vms_mb`: Process Resident Set Size and Virtual Memory Size.
* `cpu_percent`: Process CPU utilization.
* `ban_risk_level`: `LOW_SAFE`, `ELEVATED_FLOOD_WAIT`, or `CRITICAL_BANNED`.

### Real-Time Inspection Commands:
```bash
# Monitor live telemetry rows as they flush
tail -f /opt/pipvane/data/soak_telemetry/ban_risk_telemetry.csv

# View the last 10 entries formatted as a table
column -s, -t < /opt/pipvane/data/soak_telemetry/ban_risk_telemetry.csv | tail -n 10
```

---

## 6. Strict Ban Assertions (Fail-Fast Rule)

> [!CAUTION]
> **Zero Tolerance Gate:** If any Telegram account receives a `USER_DEACTIVATED` or `AUTH_KEY_UNREGISTERED` error from TDLib:
> 1. The orchestrator immediately marks the account status as `CRITICAL_BANNED`.
> 2. The event is flushed to `ban_risk_telemetry.csv`.
> 3. The orchestrator throws an immediate fatal `AssertionError`, halting the test process with non-zero exit code (`1`).
> 4. The systemd service detects `RestartPreventExitStatus=1` and refuses to restart, preserving process forensics and database state for root cause inspection.

---

## 7. Running the Automated Soak Test Suite on VPS

To run the automated validation test suite on the VPS prior to launching the 30-day soak run:

```bash
cd /opt/pipvane
source venv/bin/activate
export PYTHONPATH=.

# Execute the complete chaos and soak test suite
pytest qa/chaos/test_telegram_safety_soak.py -v
```

Expected output:
```text
qa/chaos/test_telegram_safety_soak.py::test_orchestrator_initializes_3_accounts_with_safety_mode PASSED
qa/chaos/test_telegram_safety_soak.py::test_strict_safety_mode_blocks_outgoing_messages PASSED
qa/chaos/test_telegram_safety_soak.py::test_network_fault_injection_and_auto_reconnect_backfill PASSED
qa/chaos/test_telegram_safety_soak.py::test_telemetry_csv_generation_and_metrics PASSED
qa/chaos/test_telegram_safety_soak.py::test_strict_assertion_fails_immediately_on_user_deactivated PASSED
qa/chaos/test_telegram_safety_soak.py::test_strict_assertion_fails_immediately_on_auth_key_unregistered PASSED
qa/chaos/test_telegram_safety_soak.py::test_continuous_soak_run_accelerated PASSED

============================== 7 passed in 3.56s ==============================
```
