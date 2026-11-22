#!/usr/bin/env python3
"""
Pipvane v3 Anti-Split-Brain Failover Simulation (FR-7.4, FR-7.5 & ADR-001).

Automates a 1,000-run chaos and split-brain simulation validating the Cloud Agent's
WebSocket Lease Coordinator against dual-active execution hazards.

Simulation Lifecycle (Repeated 1,000 Times):
1. Happy Path:
   - Node A connects and claims the lease -> Granted 'Active' status.
   - Node B connects, attempts to claim lease -> Rejected and assigned 'Standby'.
   - Node A sustains active trading by transmitting heartbeats for 60 seconds.
2. Chaos Partition Simulation:
   - Simulates a network partition by forcefully dropping Node A's WebSocket connection.
   - Advances time to allow the 30-second lease TTL to expire.
   - Node A transitions to SAFE_HALT / inactive (self-fencing).
   - Node B polls the coordinator and successfully claims the 'Active' lease.
3. Split-Brain Prevention:
   - Reconnects Node A while Node B actively holds the lease.
   - Verifies that Node A is strictly rejected and forced into 'Standby' / 'STRICT_LOCKED'
     (honoring the V3 offline lock rule).
4. Continuous Invariant Enforcement:
   - Invariant: |A(t)| <= 1 for all continuous time t.
   - Fails immediately (Exit Code 1) if a double-active state is detected even once.

Usage:
  python failover_simulation.py
  python failover_simulation.py --runs 1000
  python failover_simulation.py --runs 100 --export-json results.json
"""

from __future__ import annotations

import os
import sys
import time
import json
import uuid
import enum
import logging
import argparse
import asyncio
from typing import Dict, Any, Optional, List, Callable

import websockets

# Ensure project root is available in sys.path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger("pipvane.failover_simulation")

# ANSI Color formatting
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


class NodeRole(enum.Enum):
    """Configured role preference for a Local Agent node."""
    PRIMARY = "PRIMARY"
    STANDBY = "STANDBY"


class NodeState(enum.Enum):
    """Operational state of a Local Agent node in the failover state machine."""
    INITIALIZING = "INITIALIZING"
    STANDBY_HOT = "STANDBY_HOT"
    ACTIVE_EXECUTION = "ACTIVE_EXECUTION"
    FAIL_LIVE_DEGRADED = "FAIL_LIVE_DEGRADED"
    STRICT_LOCKED = "STRICT_LOCKED"
    ROLE_LOCKED = "ROLE_LOCKED"
    SAFE_HALT = "SAFE_HALT"


class SplitBrainViolationError(Exception):
    """Raised when both Node A and Node B are detected in an Active state simultaneously."""
    pass


class VirtualClock:
    """Deterministic simulated clock allowing instantaneous time progression in testing."""

    def __init__(self, initial_time: float = 1700000000.0) -> None:
        self._current_time = float(initial_time)

    def now(self) -> float:
        return self._current_time

    def advance(self, seconds: float) -> None:
        self._current_time += float(seconds)

    def reset(self, initial_time: float = 1700000000.0) -> None:
        self._current_time = float(initial_time)


class WebSocketLeaseCoordinator:
    """
    Cloud Agent WebSocket Lease Coordinator (ADR-001 & FR-7.4).
    Enforces single-lease mutual exclusion across distributed Local Agents.
    """

    def __init__(
        self,
        time_provider: Optional[Callable[[], float]] = None,
        ttl_seconds: float = 30.0,
    ) -> None:
        self.time_provider = time_provider or time.time
        self.ttl_seconds = float(ttl_seconds)

        self.active_holder: Optional[str] = None
        self.active_token: Optional[str] = None
        self.granted_at: float = 0.0
        self.expires_at: float = 0.0
        self.generation: int = 0
        self.total_claims_granted: int = 0
        self.total_claims_rejected: int = 0
        self.total_heartbeats_processed: int = 0

    def reset(self) -> None:
        """Resets lease store for next test iteration."""
        self.active_holder = None
        self.active_token = None
        self.granted_at = 0.0
        self.expires_at = 0.0
        self.generation = 0

    def _is_expired(self, current_time: float) -> bool:
        if self.active_holder is None:
            return True
        return current_time >= self.expires_at

    async def handle_client(self, websocket: websockets.WebSocketServerProtocol) -> None:
        """Processes inbound WebSocket JSON messages from Local Agents."""
        try:
            async for raw_message in websocket:
                try:
                    msg = json.loads(raw_message)
                except Exception as e:
                    await websocket.send(json.dumps({"status": "error", "message": f"Malformed JSON: {e}"}))
                    continue

                action = msg.get("action")
                node_id = msg.get("node_id")
                now = self.time_provider()

                # Lazy expiration check on every incoming message
                if self.active_holder is not None and now >= self.expires_at:
                    logger.debug("Lease for %s expired at %.2f (now: %.2f)", self.active_holder, self.expires_at, now)
                    self.active_holder = None
                    self.active_token = None

                if action == "claim_lease":
                    # Lease is available if no current holder or previous lease expired
                    if self.active_holder is None or self.active_holder == node_id:
                        self.active_holder = node_id
                        self.active_token = uuid.uuid4().hex
                        self.generation += 1
                        self.granted_at = now
                        self.expires_at = now + self.ttl_seconds
                        self.total_claims_granted += 1

                        response = {
                            "status": "granted",
                            "role": "Active",
                            "holder": node_id,
                            "lease_token": self.active_token,
                            "generation": self.generation,
                            "ttl_seconds": self.ttl_seconds,
                            "granted_at": self.granted_at,
                            "expires_at": self.expires_at,
                        }
                    else:
                        self.total_claims_rejected += 1
                        response = {
                            "status": "rejected",
                            "role": "Standby",
                            "holder": self.active_holder,
                            "generation": self.generation,
                            "expires_in": max(0.0, self.expires_at - now),
                            "reason": f"Lease actively held by {self.active_holder}",
                        }
                    await websocket.send(json.dumps(response))

                elif action == "heartbeat":
                    token = msg.get("lease_token")
                    if self.active_holder == node_id and self.active_token == token and now < self.expires_at:
                        self.expires_at = now + self.ttl_seconds
                        self.total_heartbeats_processed += 1
                        response = {
                            "status": "renewed",
                            "role": "Active",
                            "holder": node_id,
                            "generation": self.generation,
                            "expires_at": self.expires_at,
                        }
                    else:
                        response = {
                            "status": "expired",
                            "role": "Standby",
                            "holder": self.active_holder,
                            "reason": "Lease expired, revoked, or token mismatch",
                        }
                    await websocket.send(json.dumps(response))

                elif action in ("poll", "check_lease"):
                    is_active = (self.active_holder == node_id) and (now < self.expires_at)
                    role = "Active" if is_active else "Standby"
                    response = {
                        "status": "ok",
                        "role": role,
                        "holder": self.active_holder,
                        "generation": self.generation,
                        "expires_in": max(0.0, self.expires_at - now),
                    }
                    await websocket.send(json.dumps(response))

                elif action == "release_lease":
                    token = msg.get("lease_token")
                    if self.active_holder == node_id and self.active_token == token:
                        self.active_holder = None
                        self.active_token = None
                        response = {"status": "released", "role": "Standby"}
                    else:
                        response = {"status": "ignored"}
                    await websocket.send(json.dumps(response))

                else:
                    await websocket.send(json.dumps({"status": "unknown_action", "action": action}))
        except websockets.exceptions.ConnectionClosed:
            pass


class MockLocalAgent:
    """
    Simulates a Pipvane Local Agent node running the Active-Passive state machine.
    Enforces V3 Architectural Correction 2 (Strict Offline Lock on Standby).
    """

    def __init__(
        self,
        node_id: str,
        role: NodeRole,
        ws_url: str,
        time_provider: Callable[[], float],
    ) -> None:
        self.node_id = node_id
        self.role = role
        self.ws_url = ws_url
        self.time_provider = time_provider

        self.state = NodeState.INITIALIZING
        self.lease_token: Optional[str] = None
        self.lease_expires_at: float = 0.0
        self.generation: int = 0
        self.ws: Optional[websockets.WebSocketClientProtocol] = None

    @property
    def is_active(self) -> bool:
        """A node is strictly active only when in ACTIVE_EXECUTION state with valid lease."""
        if self.state != NodeState.ACTIVE_EXECUTION:
            return False
        # Double check local lease expiration guard
        if self.time_provider() >= self.lease_expires_at:
            return False
        return True

    @property
    def can_execute_trades(self) -> bool:
        """V3 Rule: Standby nodes or partition-locked nodes can NEVER execute orders."""
        if self.state in (NodeState.STRICT_LOCKED, NodeState.STANDBY_HOT, NodeState.SAFE_HALT, NodeState.ROLE_LOCKED):
            return False
        return self.is_active

    async def connect(self) -> None:
        """Establishes WebSocket connection to Cloud Lease Coordinator."""
        self.ws = await websockets.connect(self.ws_url)

    async def disconnect(self, force: bool = True) -> None:
        """Simulates network partition / abrupt socket drop."""
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None

        # State machine transition upon network partition
        if self.state == NodeState.ACTIVE_EXECUTION:
            # Primary enters FAIL_LIVE_DEGRADED during partition (FR-7.5)
            self.state = NodeState.FAIL_LIVE_DEGRADED
        elif self.state == NodeState.STANDBY_HOT:
            # Standby enters STRICT_LOCKED (FR-7.4 & FR-7.5)
            self.state = NodeState.STRICT_LOCKED

    async def claim_lease(self) -> Dict[str, Any]:
        """Sends claim_lease request to Cloud Coordinator."""
        if not self.ws:
            raise RuntimeError(f"Node {self.node_id} is not connected to WebSocket Coordinator")

        msg = {
            "action": "claim_lease",
            "node_id": self.node_id,
            "role_preference": self.role.value,
        }
        await self.ws.send(json.dumps(msg))
        resp = json.loads(await self.ws.recv())

        if resp.get("status") == "granted" and resp.get("role") == "Active":
            self.state = NodeState.ACTIVE_EXECUTION
            self.lease_token = resp.get("lease_token")
            self.lease_expires_at = resp.get("expires_at", 0.0)
            self.generation = resp.get("generation", 0)
        else:
            # V3 Offline Lock rule: Standby mode assigned
            self.state = NodeState.STANDBY_HOT
            self.lease_token = None
            self.lease_expires_at = 0.0

        return resp

    async def send_heartbeat(self) -> Dict[str, Any]:
        """Sends periodic heartbeat renewal."""
        if not self.ws:
            raise RuntimeError(f"Node {self.node_id} is disconnected")

        msg = {
            "action": "heartbeat",
            "node_id": self.node_id,
            "lease_token": self.lease_token,
        }
        await self.ws.send(json.dumps(msg))
        resp = json.loads(await self.ws.recv())

        if resp.get("status") == "renewed":
            self.lease_expires_at = resp.get("expires_at", self.lease_expires_at)
            self.generation = resp.get("generation", self.generation)
        else:
            # Heartbeat rejected or lease expired: self-fence immediately
            self.state = NodeState.STANDBY_HOT
            self.lease_token = None
            self.lease_expires_at = 0.0

        return resp

    async def poll_lease(self) -> Dict[str, Any]:
        """Polls current coordinator lease state."""
        if not self.ws:
            raise RuntimeError(f"Node {self.node_id} is disconnected")

        msg = {"action": "poll", "node_id": self.node_id}
        await self.ws.send(json.dumps(msg))
        return json.loads(await self.ws.recv())

    def on_lease_expired_locally(self) -> None:
        """Invoked when local timer indicates lease TTL has expired."""
        if self.state in (NodeState.ACTIVE_EXECUTION, NodeState.FAIL_LIVE_DEGRADED):
            self.state = NodeState.ROLE_LOCKED
            self.lease_token = None
            self.lease_expires_at = 0.0


class FailoverSimulationOrchestrator:
    """
    Automated 1,000-run split-brain simulation orchestrator.
    Continuously validates |A(t)| <= 1 across nominal, chaos, and recovery phases.
    """

    def __init__(
        self,
        total_runs: int = 1000,
        lease_ttl_s: float = 30.0,
        heartbeat_interval_s: float = 3.0,
        heartbeat_duration_s: float = 60.0,
        port: int = 0,
        export_json: Optional[str] = None,
        inject_split_brain_at: Optional[int] = None,
        quiet: bool = False,
    ) -> None:
        self.total_runs = total_runs
        self.lease_ttl_s = lease_ttl_s
        self.heartbeat_interval_s = heartbeat_interval_s
        self.heartbeat_duration_s = heartbeat_duration_s
        self.port = port
        self.export_json = export_json
        self.inject_split_brain_at = inject_split_brain_at
        self.quiet = quiet

        self.clock = VirtualClock()
        self.coordinator = WebSocketLeaseCoordinator(
            time_provider=self.clock.now,
            ttl_seconds=self.lease_ttl_s,
        )
        self.server: Optional[websockets.WebSocketServer] = None
        self.ws_url: str = ""

        self.node_a: Optional[MockLocalAgent] = None
        self.node_b: Optional[MockLocalAgent] = None

        # Telemetry & Diagnostics
        self.total_invariant_checks = 0
        self.double_active_violations = 0
        self.violation_details: List[Dict[str, Any]] = []

    async def start_server(self) -> None:
        """Spins up the WebSocket Lease Coordinator server."""
        self.server = await websockets.serve(
            self.coordinator.handle_client,
            "127.0.0.1",
            self.port,
        )
        actual_port = self.server.sockets[0].getsockname()[1]
        self.ws_url = f"ws://127.0.0.1:{actual_port}"
        logger.info("WebSocket Lease Coordinator listening on %s", self.ws_url)

    async def stop_server(self) -> None:
        """Stops the WebSocket server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    def _assert_invariant(self, scenario_phase: str, run_index: int) -> None:
        """
        Global Invariant Assertion: |A(t)| <= 1.
        Under no circumstances may both Node A and Node B be in an Active state simultaneously.
        """
        self.total_invariant_checks += 1
        a_active = self.node_a.is_active if self.node_a else False
        b_active = self.node_b.is_active if self.node_b else False

        if a_active and b_active:
            self.double_active_violations += 1
            violation_info = {
                "run_index": run_index,
                "phase": scenario_phase,
                "timestamp": self.clock.now(),
                "node_a_state": self.node_a.state.value if self.node_a else "None",
                "node_b_state": self.node_b.state.value if self.node_b else "None",
                "coordinator_holder": self.coordinator.active_holder,
            }
            self.violation_details.append(violation_info)
            logger.critical(
                "CRITICAL SPLIT-BRAIN VIOLATION at run %d (%s): Node A and Node B both ACTIVE!",
                run_index, scenario_phase
            )
            raise SplitBrainViolationError(
                f"Double-active state detected at run {run_index} ({scenario_phase}): "
                f"NodeA={self.node_a.state.value}, NodeB={self.node_b.state.value}"
            )

    async def run_single_cycle(self, run_idx: int) -> None:
        """
        Executes one full 4-phase chaos failover iteration:
        1. Happy path: Node A claims Active, Node B Standby, 60s heartbeats.
        2. Chaos partition: Node A connection severed, 30s TTL expiry, Node B claims Active.
        3. Split-brain prevention: Node A reconnects, strictly rejected to Standby.
        4. Clean reset.
        """
        self.coordinator.reset()

        # Instantiate fresh mock nodes
        self.node_a = MockLocalAgent("Node_A", NodeRole.PRIMARY, self.ws_url, self.clock.now)
        self.node_b = MockLocalAgent("Node_B", NodeRole.STANDBY, self.ws_url, self.clock.now)

        try:
            # -------------------------------------------------------------
            # PHASE 1: Happy Path Testing
            # -------------------------------------------------------------
            # 1.1 Node A connects and claims the lease
            await self.node_a.connect()
            resp_a = await self.node_a.claim_lease()
            assert resp_a["role"] == "Active", f"Node A must be granted Active, got: {resp_a}"
            assert self.node_a.state == NodeState.ACTIVE_EXECUTION
            self._assert_invariant("Phase 1: Node A claimed lease", run_idx)

            # 1.2 Node B connects and attempts to claim lease (must be rejected)
            await self.node_b.connect()
            resp_b = await self.node_b.claim_lease()
            assert resp_b["role"] == "Standby", f"Node B must be rejected, got: {resp_b}"
            assert self.node_b.state == NodeState.STANDBY_HOT
            assert not self.node_b.can_execute_trades
            self._assert_invariant("Phase 1: Node B rejected as Standby", run_idx)

            # 1.3 Node A sends heartbeats for 60 seconds (20 heartbeats at 3s intervals)
            heartbeat_steps = int(self.heartbeat_duration_s / self.heartbeat_interval_s)
            for _ in range(heartbeat_steps):
                self.clock.advance(self.heartbeat_interval_s)
                hb_resp = await self.node_a.send_heartbeat()
                assert hb_resp["role"] == "Active"
                self._assert_invariant("Phase 1: Node A heartbeat loop", run_idx)

            # -------------------------------------------------------------
            # PHASE 2: Chaos Partition Simulation
            # -------------------------------------------------------------
            # 2.1 Forcefully drop Node A's WebSocket connection
            await self.node_a.disconnect(force=True)
            assert self.node_a.state == NodeState.FAIL_LIVE_DEGRADED
            self._assert_invariant("Phase 2: Node A partitioned", run_idx)

            # 2.2 Wait for the 30-second lease TTL to expire (advance clock + 1s buffer)
            self.clock.advance(self.lease_ttl_s + 1.0)

            # Primary self-fences locally once TTL expires without renewal
            self.node_a.on_lease_expired_locally()
            assert self.node_a.state in (NodeState.ROLE_LOCKED, NodeState.SAFE_HALT)
            assert not self.node_a.can_execute_trades
            self._assert_invariant("Phase 2: Node A lease expired / self-fenced", run_idx)

            # 2.3 Node B polls / claims the active lease
            resp_b2 = await self.node_b.claim_lease()
            assert resp_b2["role"] == "Active", f"Node B must claim active on takeover, got: {resp_b2}"
            assert self.node_b.state == NodeState.ACTIVE_EXECUTION
            assert self.node_b.can_execute_trades

            # Chaos Fault Injection: Test split-brain fail-fast assertion
            if self.inject_split_brain_at is not None and run_idx == self.inject_split_brain_at:
                logger.warning("SYNTHETIC INJECTION: Forcing Node A Active concurrently with Node B!")
                self.node_a.state = NodeState.ACTIVE_EXECUTION
                self.node_a.lease_expires_at = self.clock.now() + 100.0

            self._assert_invariant("Phase 2: Node B promoted to Active", run_idx)

            # -------------------------------------------------------------
            # PHASE 3: Split-Brain Prevention (Reconnection)
            # -------------------------------------------------------------
            # 3.1 Reconnect Node A while Node B holds the active lease
            await self.node_a.connect()

            # 3.2 Node A attempts to claim the lease
            resp_a2 = await self.node_a.claim_lease()

            # Node A MUST be strictly rejected and assigned Standby
            assert resp_a2["role"] == "Standby", f"Node A must be rejected upon reconnect, got: {resp_a2}"
            assert resp_a2["status"] == "rejected"
            assert self.node_a.state == NodeState.STANDBY_HOT
            assert not self.node_a.can_execute_trades

            # Node B is still Active
            assert self.node_b.state == NodeState.ACTIVE_EXECUTION
            self._assert_invariant("Phase 3: Node A reconnected and locked to Standby", run_idx)

        finally:
            # -------------------------------------------------------------
            # PHASE 4: Teardown Connections for Clean Next Run
            # -------------------------------------------------------------
            if self.node_a:
                await self.node_a.disconnect(force=True)
            if self.node_b:
                await self.node_b.disconnect(force=True)

    async def execute_simulation(self) -> Dict[str, Any]:
        """Runs the complete 1,000-run simulation and aggregates metrics."""
        await self.start_server()

        if not self.quiet:
            print(f"{CYAN}{BOLD}================================================================={RESET}")
            print(f"{CYAN}{BOLD}    PIPVANE v3 ANTI-SPLIT-BRAIN FAILOVER SIMULATION (FR-7.4)     {RESET}")
            print(f"{CYAN}{BOLD}================================================================={RESET}")
            print(f"[*] Target Chaos Cycles:    {BOLD}{self.total_runs:,}{RESET}")
            print(f"[*] Lease TTL:              {BOLD}{self.lease_ttl_s:.1f} s{RESET}")
            print(f"[*] Heartbeat Interval:     {BOLD}{self.heartbeat_interval_s:.1f} s{RESET}")
            print(f"[*] Happy Path Heartbeats:  {BOLD}{self.heartbeat_duration_s:.1f} s{RESET}")
            print(f"[*] Coordinator URI:        {BOLD}{self.ws_url}{RESET}")
            print(f"[*] Invariant Rule:         {BOLD}|A(t)| <= 1 (Zero Dual-Active Tolerance){RESET}")
            print("-" * 65)

        start_time = time.perf_counter()
        passed_runs = 0

        try:
            for run_idx in range(1, self.total_runs + 1):
                try:
                    await self.run_single_cycle(run_idx)
                    passed_runs += 1

                    if not self.quiet and (run_idx % 100 == 0 or run_idx == self.total_runs):
                        pct = (run_idx / self.total_runs) * 100
                        print(f"  -> Progress: {run_idx:>4}/{self.total_runs} cycles ({pct:>5.1f}%) | "
                              f"Violations: {self.double_active_violations}")
                except SplitBrainViolationError as e:
                    logger.error("Simulation aborted due to split-brain: %s", e)
                    break
        finally:
            await self.stop_server()

        wall_duration = time.perf_counter() - start_time
        success = (passed_runs == self.total_runs) and (self.double_active_violations == 0)

        results = {
            "total_runs_requested": self.total_runs,
            "runs_completed": passed_runs,
            "wall_duration_seconds": wall_duration,
            "throughput_runs_sec": (passed_runs / wall_duration) if wall_duration > 0 else 0.0,
            "total_invariant_checks": self.total_invariant_checks,
            "double_active_violations": self.double_active_violations,
            "success": success,
            "violation_details": self.violation_details,
        }

        if self.export_json:
            self._export_json(results)

        return results

    def _export_json(self, results: Dict[str, Any]) -> None:
        """Exports simulation metrics to JSON."""
        try:
            with open(self.export_json, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
            if not self.quiet:
                print(f"[+] Exported simulation metrics to '{self.export_json}'")
        except Exception as e:
            logger.error("Failed to export JSON results: %s", e)

    def print_report(self, r: Dict[str, Any]) -> None:
        """Prints ASCII simulation report."""
        success = r["success"]
        status_banner = (
            f"{GREEN}{BOLD}[ PASS ] SPLIT-BRAIN GATE SATISFIED (0 VIOLATIONS){RESET}"
            if success else
            f"{RED}{BOLD}[ FAIL ] DOUBLE-ACTIVE STATE DETECTED{RESET}"
        )

        print("\n" + "=" * 65)
        print(f"       FAILOVER & SPLIT-BRAIN SIMULATION REPORT ({r['total_runs_requested']:,} RUNS)       ")
        print("=" * 65)
        print(f" Status:                     {status_banner}")
        print(f" Total Chaos Runs Executed:  {BOLD}{r['runs_completed']:,} / {r['total_runs_requested']:,}{RESET}")
        print(f" Simulation Wall Duration:   {BOLD}{r['wall_duration_seconds']:.2f} s{RESET}")
        print(f" Execution Velocity:         {BOLD}{r['throughput_runs_sec']:.1f} runs/sec{RESET}")
        print(f" Total Invariant Invocations:{BOLD}{r['total_invariant_checks']:,} checks{RESET}")
        print("-" * 65)
        print(f" Dual-Active Violations:     {BOLD}{r['double_active_violations']}{RESET}")
        total_attempts = max(1, r['runs_completed'] + r['double_active_violations'])
        freq = (r['double_active_violations'] / total_attempts) * 100.0
        print(f" Split-Brain Frequency:      {BOLD}{freq:.5f}%{RESET}")
        print("=" * 65)
        print(" ARCHITECTURAL COMPLIANCE VERIFICATION:")
        # Invariant check status
        sym_inv = "✓ PASS" if r["double_active_violations"] == 0 else "✗ FAIL"
        col_inv = GREEN if r["double_active_violations"] == 0 else RED
        print(f"   [1] Invariant |A(t)| <= 1:           {col_inv}{sym_inv} (0 concurrent active states){RESET}")

        # Standby lock check status
        sym_lock = "✓ PASS" if success else "✗ FAIL"
        col_lock = GREEN if success else RED
        print(f"   [2] Standby Reconnect Lock (FR-7.4): {col_lock}{sym_lock} (Node A strictly Standby){RESET}")

        # TTL Takeover check status
        sym_take = "✓ PASS" if success else "✗ FAIL"
        col_take = GREEN if success else RED
        print(f"   [3] 30s TTL Expiry Takeover:        {col_take}{sym_take} (Node B safely promoted){RESET}")
        print("=" * 65 + "\n")


async def run_simulation_async(args: argparse.Namespace) -> int:
    """Async simulation entry point."""
    orchestrator = FailoverSimulationOrchestrator(
        total_runs=args.runs,
        lease_ttl_s=args.ttl,
        heartbeat_interval_s=args.heartbeat_interval,
        heartbeat_duration_s=args.heartbeat_duration,
        port=args.port,
        export_json=args.export_json,
        inject_split_brain_at=args.inject_split_brain_at,
        quiet=args.quiet,
    )

    results = await orchestrator.execute_simulation()
    orchestrator.print_report(results)
    return 0 if results["success"] else 1


def main() -> None:
    """CLI Entry point."""
    parser = argparse.ArgumentParser(
        description="Pipvane v3 Anti-Split-Brain Failover Simulation (FR-7.4 & FR-7.5)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--runs", type=int, default=1000, help="Number of chaos partition cycles to execute")
    parser.add_argument("--ttl", type=float, default=30.0, help="Lease TTL duration in seconds")
    parser.add_argument("--heartbeat-interval", type=float, default=3.0, help="Heartbeat renewal cadence in seconds")
    parser.add_argument(
        "--heartbeat-duration", type=float, default=60.0, help="Happy path heartbeat duration in seconds"
    )
    parser.add_argument("--port", type=int, default=0, help="WebSocket Coordinator port (0 for random)")
    parser.add_argument("--export-json", default=None, help="Optional output path to export results JSON")
    parser.add_argument("--inject-split-brain-at", type=int, default=None, help="Inject synthetic split-brain at run N")
    parser.add_argument("--quiet", action="store_true", help="Quiet output mode")
    parser.add_argument("--verbose", action="store_true", help="Verbose debug logging")
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if not args.verbose:
        logging.getLogger("websockets").setLevel(logging.WARNING)

    exit_code = asyncio.run(run_simulation_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
