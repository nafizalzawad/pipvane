# Pipvane v3: QA Automation & CI Pipeline Documentation

Welcome to the **Pipvane v3 QA Automation Test Suite**. This repository houses the automated testing frameworks, Chaos Engineering harnesses, latency benchmarks, and Continuous Integration (CI/CD) pipelines designed to enforce the sub-5ms latency threshold (VR-3.1), 98% Telegram signal parser accuracy, and anti-split-brain high-availability guarantees.

---

## 1. Directory Structure

```text
qa/
├── README.md                           # QA test suite and execution guide
├── skill.md                            # QA Automation Engineer role profile
├── package.json                        # QA Node dependencies (Playwright, Jest, ts-jest)
├── jest.config.js                      # Jest configuration for TypeScript components
├── playwright.config.ts                # Playwright E2E configuration (headless verification)
├── pytest.ini                          # Pytest configuration and custom test markers
├── ci.yml                              # Staged GitHub Actions workflow definition
├── pipvane_latency_load_test.jmx       # JMeter 50 signals/min burst load test plan
├── golden_set_sample.json              # 5-signal Golden Set sample for parser verification
├── test_parser_gate.py                 # Parser CI quality gate runner (>= 98% threshold)
├── LATENCY_BENCHMARK_STRATEGY.md       # High-resolution timing & latency measurement blueprint
├── benchmark/
│   ├── latency_benchmark.py           # Synthetic IPC shared memory benchmark harness
│   └── latency_benchmark_results.json # Generated latency percentile benchmark results
└── tests/
    ├── __init__.py
    ├── conftest.py                     # Shared Pytest fixtures (high-res timers, SHM mock)
    ├── unit/
    │   ├── __init__.py
    │   ├── test_engine_scaffold.py    # Python unit tests for structs, timers & failover
    │   └── sample_component.test.ts   # Jest unit tests for IPC payload & contracts
    ├── integration/
    │   └── __init__.py                # Multi-terminal integration test harness
    ├── parser_gate/
    │   ├── __init__.py
    │   └── test_parser_gate.py        # Pytest integration for Golden Set parser gate
    └── e2e/
        └── dashboard.spec.ts          # Playwright headless UI E2E test suite
```

---

## 2. Test Suites & Execution Instructions

### A. Parser CI Gate (Mandatory >= 98% Accuracy Threshold)
Evaluates signal normalization, emoji handling, and multi-TP ladders against the Golden Set:
```bash
# Standalone execution
python qa/test_parser_gate.py

# Via Pytest
python -m pytest qa/tests/parser_gate -v
```

### B. Python Unit & Engine Tests (Pytest)
Validates 256-byte cache-line aligned shared memory headers, 128-byte slot contracts, high-resolution monotonic timers, and `STRICT_LOCKED` anti-split-brain failover transitions:
```bash
# From workspace root
python -m pytest

# Targeting QA specifically
python -m pytest qa/tests/unit -v
```

### C. High-Resolution IPC Latency Benchmark (VR-3.1)
Measures monotonic nanosecond round-trip times through simulated SPSC shared memory ring buffer queues under 50 signals/min burst load:
```bash
python qa/benchmark/latency_benchmark.py
```
*Output percentiles (p50, p95, p99) must satisfy: **p99 < 5.000 ms**.*

### D. TypeScript Component Tests (Jest)
Validates TypeScript type definitions, payload schemas, and zero-knowledge vault boundaries:
```bash
cd qa
npm install
npm test
```

### E. Headless UI Verification (Playwright)
Executes end-to-end headless browser tests against the static local dashboard export (`frontend/out`):
```bash
cd qa
npm install
npx playwright install --with-deps chromium
npx playwright test
```

### F. JMeter High-Throughput Load Test (50 Signals/Min Burst)
Runs the load testing plan against the local agent HTTP dispatch endpoint:
```bash
jmeter -n -t qa/pipvane_latency_load_test.jmx -l qa/results.jtl -e -o qa/load_report/
```

---

## 3. GitHub Actions CI/CD Pipeline

The Continuous Integration pipeline is configured at `.github/workflows/ci.yml` and mirrors `qa/ci.yml`. It runs automatically on pull requests and pushes to `main`:

1. **Lint & Static Analysis:** Enforces Flake8 on Python and ESLint on TypeScript.
2. **Pytest Unit & Struct Tests:** Verifies ADR-001 memory alignment and failover states.
3. **Parser CI Gate:** Runs `test_parser_gate.py` with hard build failure if accuracy < 98%.
4. **Jest Protocol Tests:** Asserts frontend and IPC data contracts.
5. **Playwright UI Tests:** Validates dashboard rendering and interactive flows in headless mode.
6. **VR-3.1 IPC Latency Benchmark Gate:** Validates sub-5ms p99 latency threshold.
