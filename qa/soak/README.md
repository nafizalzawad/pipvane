# Pipvane v3: Sprint 13 — Terminal Footprint Soak Test Lab

## Objective
Validate Pipvane Engine stability while driving the maximum licensed limit (**7 concurrent MetaTrader accounts**) on a strictly constrained hardware profile (**2 vCPUs and 4GB RAM**) during continuous high-frequency order dispatch for **48 hours**.

---

## Hardware Constraint Constraints (SLA)
1. **Engine Memory Footprint:** The Pipvane Engine process (`pipvane.exe` / `python.exe`) must **NOT exceed 400 MB RAM (RSS)** at any point during execution.
2. **System Memory Footprint:** Total host/VM system memory utilization must **NOT hit or exceed 98.0%**.
3. **IPC Latency & Throughput:** Sub-5ms round-trip execution acknowledgment across all 7 terminals concurrently with zero ring-buffer slot leakage.
4. **Failure Behavior:** If either constraint is violated, the test immediately logs a critical violation and halts with **exit code 1**.

---

## Terminal Setup (7 Licensed Accounts)
The soak lab coordinates a mixed environment of MT4 and MT5 accounts matching the maximum licensed limit:
* **MT5 Terminal 1:** `mt5_hedge_01` (Login: 500101, Hedging, Pipvane-Demo-MT5-01)
* **MT5 Terminal 2:** `mt5_hedge_02` (Login: 500102, Hedging, Pipvane-Demo-MT5-02)
* **MT5 Terminal 3:** `mt5_netting_03` (Login: 500103, Netting, Pipvane-Demo-MT5-Netting)
* **MT5 Terminal 4:** `mt5_hedge_04` (Login: 500104, Hedging, Pipvane-Demo-MT5-04)
* **MT4 Terminal 5:** `mt4_std_01` (Login: 400101, Standard Hedging, Pipvane-Demo-MT4-01)
* **MT4 Terminal 6:** `mt4_std_02` (Login: 400102, Standard Hedging, Pipvane-Demo-MT4-02)
* **MT4 Terminal 7:** `mt4_std_03` (Login: 400103, Standard Hedging, Pipvane-Demo-MT4-03)

Terminals are automatically identified by the **`TerminalDiscovery`** module and enrolled into the Engine's **`IPCDispatcher`**.

---

## Running the 48-Hour Soak Test

### 1. Local / Dedicated Windows VM (Hyper-V)
To provision the local VM with 2 vCPUs and 4GB RAM:
```powershell
# Provision Hyper-V VM constrained to 2 vCPUs and 4GB RAM:
powershell -ExecutionPolicy Bypass -File qa/soak/setup_vm.ps1

# Launch the 48-Hour Soak Orchestrator inside the VM:
python qa/soak/terminal_soak_runner.py --duration-hours 48 --interval 60 --terminals 7
```

### 2. Docker-Windows Container (2 vCPUs / 4GB RAM)
```powershell
# Build and run the constrained container:
docker-compose -f qa/soak/docker-compose.soak.yml up --build
```

### 3. Standalone Resource Monitor Execution
To run the resource monitor alongside any running Pipvane instance:
```powershell
# Monitor specific Engine PID every 60 seconds for 48 hours:
python resource_monitor.py --pid <ENGINE_PID> --interval 60 --duration-hours 48 --output soak_metrics.csv
```

### 4. Automated Regression Tests (Pytest)
```powershell
python -m pytest qa/tests/test_terminal_soak.py -v
```
