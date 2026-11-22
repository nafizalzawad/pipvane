"""
Pipvane v3 - Automated Security Audit: Mobile Relay & Remote Commands (Sprint 17)
==================================================================================
Validates end-to-end security of the Mobile Companion App remote commands:
1. Replay Attack Simulation:
   - Intercepts a valid encrypted "Close All Trades" payload.
   - Simulates delayed delivery 5 minutes (300 seconds) later.
   - Asserts that Local Engine strictly rejects it based on the timestamp.
   - Asserts duplicate nonces within the freshness window are also rejected.
2. Spoofing Simulation:
   - Injects unencrypted plaintext JSON commands directly to the queue.
   - Injects improperly encrypted commands (bit-flipped ciphertext, corrupted MAC tag,
     tampered IV, unauthorized/wrong key).
   - Asserts that Local Engine strictly rejects every spoofed payload and logs security warnings.
3. Live Cloud Relay Integration:
   - Spawns live Node.js Cloud Control Plane server.
   - Asserts that Cloud Relay enforces Zero-Knowledge compliance (rejects plaintext).
   - Validates end-to-end flow from Mobile App push -> Cloud Relay queue -> Local Engine rejection.
4. Execution Protection:
   - Asserts that zero unauthorized trades or liquidations occur during attacks.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import logging
import os
import subprocess
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

import pytest

from engine.ipc_protocol import ActionType, OrderIntent, OrderSide
from engine.remote_commands import (
    DEFAULT_MAX_DRIFT_SECONDS,
    InvalidPassphraseError,
    MobileRelayClient,
    ReplayAttackError,
    RemoteCommand,
    RemoteCommandCrypto,
    RemoteCommandEnvelope,
    RemoteCommandReceiver,
    RemoteCommandType,
    RemoteSecurityError,
    SecurityRejectionCode,
    SpoofedPayloadError,
    StaleTimestampError,
    TamperedPayloadError,
)


# =============================================================================
# MOCK EXECUTION ORCHESTRATOR FOR TESTING
# =============================================================================

class MockTestOrchestrator:
    """Mock execution orchestrator tracking position states and journey events."""

    def __init__(self):
        self.orders: Dict[int, Dict[str, Any]] = {
            1001: {
                "ticket": 1001,
                "signal_id": 501,
                "symbol": "EURUSD",
                "volume": 0.50,
                "terminal_id": "demo_terminal_1",
                "status": "OPEN",
            },
            1002: {
                "ticket": 1002,
                "signal_id": 502,
                "symbol": "GBPUSD",
                "volume": 0.30,
                "terminal_id": "demo_terminal_1",
                "status": "OPEN",
            },
            1003: {
                "ticket": 1003,
                "signal_id": 503,
                "symbol": "XAUUSD",
                "volume": 0.10,
                "terminal_id": "demo_terminal_2",
                "status": "OPEN",
            },
        }
        self.journey_events: List[Dict[str, Any]] = []
        self.panic_calls: int = 0
        self.is_locked: bool = False

    def get_active_orders(self, terminal_id: Optional[str] = None) -> List[Dict[str, Any]]:
        result = []
        for o in self.orders.values():
            if o["status"] == "OPEN":
                if terminal_id is None or o.get("terminal_id") == terminal_id:
                    result.append(copy.deepcopy(o))
        return result

    def record_journey_event(self, **kwargs) -> None:
        self.journey_events.append(kwargs)

    def lock(self) -> None:
        self.is_locked = True

    def unlock(self) -> None:
        self.is_locked = False

    async def panic_close_all(self, reason: str = "EMERGENCY_MOBILE_PANIC") -> List[Dict[str, Any]]:
        self.panic_calls += 1
        liquidated = []
        for o in self.orders.values():
            if o["status"] == "OPEN":
                o["status"] = "CLOSED"
                liquidated.append(copy.deepcopy(o))
                self.record_journey_event(
                    signal_id=o["signal_id"],
                    ticket=o["ticket"],
                    terminal_id=o["terminal_id"],
                    event_type="EMERGENCY_PANIC_CLOSE",
                    status="CLOSED",
                    reason=reason,
                )
        return liquidated


# =============================================================================
# MOCK SIMULATED CLOCK
# =============================================================================

class MockClock:
    """Controllable mock clock for deterministic time-travel and replay simulations."""

    def __init__(self, initial_time: float = 1770000000.0):
        self._current_time = initial_time

    def now(self) -> float:
        return self._current_time

    def advance(self, seconds: float) -> None:
        self._current_time += seconds

    def set_time(self, new_time: float) -> None:
        self._current_time = new_time


# =============================================================================
# TEST SUITE 1: UNIT & CRYPTOGRAPHIC SECURITY TESTS
# =============================================================================

class TestRemoteSecurityUnit(unittest.TestCase):
    """Unit tests for cryptographic anti-spoofing and anti-replay guards."""

    def setUp(self):
        self.license_id = "LIC-TEST-RELAY-2026-X01"
        self.passphrase = "UltraSecureSharedPassphrase_2026!#"
        self.clock = MockClock(initial_time=1770000000.0)
        self.orchestrator = MockTestOrchestrator()
        self.receiver = RemoteCommandReceiver(
            passphrase=self.passphrase,
            license_id=self.license_id,
            orchestrator=self.orchestrator,
            max_drift_seconds=60.0,
            clock=self.clock.now,
        )

    # -------------------------------------------------------------------------
    # Nominal Baseline
    # -------------------------------------------------------------------------

    def test_01_nominal_encrypted_command_executes_successfully(self):
        """Valid encrypted 'Close All Trades' command within time window executes and liquidates positions."""
        cmd = RemoteCommand(
            command_id="cmd-nominal-001",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
            terminal_id="ALL",
            params={"reason": "MOBILE_PANIC_TEST"},
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        receipt = asyncio.run(self.receiver.process_remote_envelope(envelope))

        self.assertTrue(receipt["success"])
        self.assertEqual(receipt["status"], "EXECUTED")
        self.assertEqual(receipt["command"], "CLOSE_ALL_TRADES")
        self.assertEqual(self.orchestrator.panic_calls, 1)
        self.assertEqual(len(self.orchestrator.get_active_orders()), 0)
        self.assertEqual(len(self.receiver.security_alerts), 0)

    # -------------------------------------------------------------------------
    # Task 1: Replay Attack Simulations
    # -------------------------------------------------------------------------

    def test_02_replay_attack_rejected_after_5_minutes(self):
        """
        TASK 1 MANDATE:
        Intercept a valid encrypted payload ('Close All Trades') and attempt to push
        the exact same payload 5 minutes (300 seconds) later.
        Assert that the Local Engine strictly rejects it based on the timestamp.
        """
        t0 = self.clock.now()

        # Step 1: Mobile App creates and intercepts valid encrypted payload at t0
        valid_cmd = RemoteCommand(
            command_id="cmd-close-all-trades-intercepted-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=t0,
            terminal_id="ALL",
            params={"reason": "AUTHORIZED_MOBILE_CLOSE"},
        )
        intercepted_envelope = RemoteCommandCrypto.encrypt_command(
            command=valid_cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # Step 2: Time advances by exactly 5 minutes (300 seconds)
        replay_delay_seconds = 5 * 60.0  # 300 seconds
        self.clock.advance(replay_delay_seconds)
        self.assertEqual(self.clock.now() - t0, 300.0)

        # Step 3: Attacker attempts to push the exact same payload to the engine
        with self.assertRaises(StaleTimestampError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(intercepted_envelope))

        # Step 4: Verify rejection details
        err = ctx.exception
        self.assertEqual(err.code, SecurityRejectionCode.STALE_TIMESTAMP)
        self.assertIn("Replay attack detected", str(err))
        self.assertIn("300.0s", str(err))
        self.assertGreaterEqual(err.age_seconds, 300.0)

        # Step 5: Verify engine logged security alert
        alerts = self.receiver.security_alerts
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_type"], "REPLAY_ATTACK")
        self.assertEqual(alerts[0]["code"], "STALE_TIMESTAMP")

        # Step 6: Verify positions were NOT touched / NOT closed
        self.assertEqual(self.orchestrator.panic_calls, 0)
        active_orders = self.orchestrator.get_active_orders()
        self.assertEqual(len(active_orders), 3, "Orders must remain open during replay attack")

    def test_03_replay_attack_rejected_duplicate_nonce_within_freshness_window(self):
        """
        Duplicate nonce replay: Sending the exact same payload a second time within
        the 60s freshness window is strictly rejected by the Deduplication Guard.
        """
        cmd = RemoteCommand(
            command_id="cmd-close-all-trades-unique-nonce-02",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # First execution succeeds
        receipt1 = asyncio.run(self.receiver.process_remote_envelope(envelope))
        self.assertEqual(receipt1["status"], "EXECUTED")
        self.assertEqual(self.orchestrator.panic_calls, 1)

        # Attacker replays 5 seconds later (within 60s window)
        self.clock.advance(5.0)
        with self.assertRaises(ReplayAttackError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(envelope))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.DUPLICATE_NONCE)
        self.assertIn("Duplicate command_id", str(ctx.exception))
        self.assertEqual(self.orchestrator.panic_calls, 1, "Must not execute command again")

    def test_04_replay_attack_rejected_future_timestamp_drift(self):
        """Commands with forged future timestamps (>60s in the future) are strictly rejected."""
        future_time = self.clock.now() + 600.0  # 10 minutes in the future
        cmd = RemoteCommand(
            command_id="cmd-future-drift-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=future_time,
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        with self.assertRaises(ReplayAttackError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(envelope))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.FUTURE_TIMESTAMP_DRIFT)
        self.assertEqual(self.orchestrator.panic_calls, 0)

    # -------------------------------------------------------------------------
    # Task 2: Spoofing Simulations (Plaintext & Improperly Encrypted Commands)
    # -------------------------------------------------------------------------

    def test_05_spoofing_rejected_plaintext_json_command(self):
        """
        TASK 2 MANDATE:
        Attempt to push a plaintext JSON command to the queue.
        Assert that the Local Engine rejects it and logs a security warning.
        """
        # Attacker sends unencrypted plaintext JSON string
        plaintext_payload = json.dumps({
            "command": "CLOSE_ALL_TRADES",
            "symbol": "EURUSD",
            "action": "panic_close",
            "lots": 1.0,
            "timestamp": self.clock.now(),
        })

        with self.assertRaises(SpoofedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(plaintext_payload))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.PLAINTEXT_REJECTED)
        self.assertIn("Plaintext remote command rejected", str(ctx.exception))

        # Verify security warning logged
        alerts = self.receiver.security_alerts
        self.assertTrue(len(alerts) >= 1)
        self.assertIn("SPOOFING", alerts[-1]["alert_type"])
        self.assertEqual(alerts[-1]["code"], "PLAINTEXT_REJECTED")

        # Verify zero trades executed
        self.assertEqual(self.orchestrator.panic_calls, 0)
        self.assertEqual(len(self.orchestrator.get_active_orders()), 3)

    def test_06_spoofing_rejected_plaintext_dict_missing_ciphertext(self):
        """Pushing a dictionary without ciphertext or encryption envelope is rejected as spoofing."""
        raw_dict = {
            "metadata": {"license_id": self.license_id, "direction": "mobile_to_engine"},
            "command_name": "CLOSE_ALL_TRADES",
        }

        with self.assertRaises(SpoofedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(raw_dict))

        self.assertIn(ctx.exception.code, (SecurityRejectionCode.PLAINTEXT_REJECTED, SecurityRejectionCode.MALFORMED_ENVELOPE))
        self.assertEqual(self.orchestrator.panic_calls, 0)

    def test_07_spoofing_rejected_tampered_ciphertext_bit_flip(self):
        """
        TASK 2 MANDATE:
        Improperly encrypted payload with a modified/tampered ciphertext byte.
        Assert that GCM MAC tag authentication fails, engine rejects it, and logs a security warning.
        """
        cmd = RemoteCommand(
            command_id="cmd-tamper-target-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # Attacker modifies a single byte in the base64 ciphertext
        cipher_bytes = bytearray(base64.b64decode(envelope.ciphertext))
        cipher_bytes[0] ^= 0xFF  # Flip 8 bits
        tampered_ciphertext = base64.b64encode(bytes(cipher_bytes)).decode("ascii")

        tampered_envelope = copy.deepcopy(envelope.to_dict())
        tampered_envelope["ciphertext"] = tampered_ciphertext

        with self.assertRaises(TamperedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(tampered_envelope))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.TAMPERED_CIPHERTEXT)
        self.assertIn("MAC tag mismatch", str(ctx.exception))

        # Verify security warning was logged
        alerts = self.receiver.security_alerts
        self.assertTrue(len(alerts) >= 1)
        self.assertEqual(alerts[-1]["code"], "TAMPERED_CIPHERTEXT")
        self.assertEqual(self.orchestrator.panic_calls, 0)

    def test_08_spoofing_rejected_corrupted_auth_tag(self):
        """Improperly encrypted payload with corrupted MAC tag is strictly rejected."""
        cmd = RemoteCommand(
            command_id="cmd-tamper-tag-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        tampered_envelope = copy.deepcopy(envelope.to_dict())
        tag_bytes = bytearray(base64.b64decode(tampered_envelope["metadata"]["tag"]))
        tag_bytes[4] ^= 0x55  # Corrupt tag byte
        tampered_envelope["metadata"]["tag"] = base64.b64encode(bytes(tag_bytes)).decode("ascii")

        with self.assertRaises(TamperedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(tampered_envelope))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.TAMPERED_CIPHERTEXT)
        self.assertEqual(self.orchestrator.panic_calls, 0)

    def test_09_spoofing_rejected_tampered_iv(self):
        """Improperly encrypted payload with tampered IV is strictly rejected."""
        cmd = RemoteCommand(
            command_id="cmd-tamper-iv-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        tampered_envelope = copy.deepcopy(envelope.to_dict())
        iv_bytes = bytearray(base64.b64decode(tampered_envelope["metadata"]["iv"]))
        iv_bytes[2] ^= 0xAA  # Corrupt IV byte
        tampered_envelope["metadata"]["iv"] = base64.b64encode(bytes(iv_bytes)).decode("ascii")

        with self.assertRaises(TamperedPayloadError):
            asyncio.run(self.receiver.process_remote_envelope(tampered_envelope))

        self.assertEqual(self.orchestrator.panic_calls, 0)

    def test_10_spoofing_rejected_unauthorized_key_or_wrong_passphrase(self):
        """Attacker encrypts a command with an unauthorized key/passphrase. Local Engine rejects it."""
        rogue_passphrase = "AttackerRoguePassphrase_99999!"
        cmd = RemoteCommand(
            command_id="cmd-unauthorized-key-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        # Encrypted with rogue key
        rogue_envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=rogue_passphrase,
            license_id=self.license_id,
        )

        # Receiver expects legitimate user passphrase
        with self.assertRaises(TamperedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(rogue_envelope))

        self.assertIn(ctx.exception.code, (SecurityRejectionCode.TAMPERED_CIPHERTEXT, SecurityRejectionCode.INVALID_SIGNATURE_OR_KEY))
        self.assertEqual(self.orchestrator.panic_calls, 0)

    def test_11_spoofing_rejected_license_id_mismatch(self):
        """Payload intended for a different license is strictly rejected."""
        cmd = RemoteCommand(
            command_id="cmd-wrong-license-01",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=self.clock.now(),
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id="LIC-SOME-OTHER-USER-999",
        )

        with self.assertRaises(SpoofedPayloadError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(envelope))

        self.assertIn("License ID mismatch", str(ctx.exception))
        self.assertEqual(self.orchestrator.panic_calls, 0)


# =============================================================================
# TEST SUITE 2: LIVE NODE.JS CLOUD CONTROL PLANE INTEGRATION
# =============================================================================

class TestRemoteSecurityLiveCloudRelay(unittest.TestCase):
    """
    Spawns the Node.js Cloud Control Plane server and tests the complete flow:
    Mobile Companion App -> Cloud Relay Queue -> Local Engine.
    Proves both Cloud Relay and Local Engine security gates under active conditions.
    """

    server_proc = None
    port = 8991
    relay_url = f"http://127.0.0.1:{port}"

    @classmethod
    def setUpClass(cls):
        cloud_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "cloud"))
        server_script = os.path.join(cloud_dir, "src", "server.js")

        env = dict(os.environ)
        env["PORT"] = str(cls.port)
        env["HOST"] = "127.0.0.1"

        cls.server_proc = subprocess.Popen(
            ["node", server_script],
            cwd=cloud_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        ready = False
        health_url = f"{cls.relay_url}/api/health"
        for _ in range(40):
            try:
                with urllib.request.urlopen(health_url, timeout=0.5) as resp:
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:
                time.sleep(0.1)

        if not ready:
            cls.server_proc.kill()
            raise RuntimeError("Failed to start Cloud Control Plane server for Sprint 17 Relay tests.")

    @classmethod
    def tearDownClass(cls):
        if cls.server_proc:
            cls.server_proc.terminate()
            try:
                cls.server_proc.wait(timeout=2.0)
            except Exception:
                cls.server_proc.kill()

    def setUp(self):
        self.license_id = f"LIC-E2E-RELAY-{int(time.time()*1000)}"
        self.passphrase = "MasterSyncPassphrase_E2E_2026!"
        self.relay_client = MobileRelayClient(self.relay_url)
        self.clock = MockClock(initial_time=time.time())
        self.orchestrator = MockTestOrchestrator()
        self.receiver = RemoteCommandReceiver(
            passphrase=self.passphrase,
            license_id=self.license_id,
            orchestrator=self.orchestrator,
            max_drift_seconds=60.0,
            clock=self.clock.now,
        )

    def test_12_cloud_relay_rejects_plaintext_trading_parameters(self):
        """Cloud Relay enforces Zero-Knowledge compliance: rejects push with plaintext parameters."""
        url = f"{self.relay_url}/relay/command"
        body = {
            "metadata": {
                "license_id": self.license_id,
                "direction": "mobile_to_engine",
            },
            # Plaintext trade parameters violating Zero-Knowledge
            "ciphertext": "some_ciphertext",
            "symbol": "EURUSD",
            "lots": 1.0,
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=3.0)

        self.assertEqual(ctx.exception.code, 400)
        resp_data = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(resp_data.get("code"), "ZERO_KNOWLEDGE_VIOLATION")

    def test_13_cloud_relay_rejects_missing_ciphertext(self):
        """Cloud Relay rejects payloads missing the required opaque ciphertext string."""
        url = f"{self.relay_url}/relay/command"
        body = {
            "metadata": {
                "license_id": self.license_id,
                "direction": "mobile_to_engine",
            },
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=3.0)

        self.assertEqual(ctx.exception.code, 400)
        resp_data = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(resp_data.get("code"), "MISSING_CIPHERTEXT")

    def test_14_live_end_to_end_close_all_trades_nominal(self):
        """End-to-End Nominal: Mobile pushes encrypted command -> Cloud queues -> Local Engine executes."""
        now_ts = self.clock.now()
        cmd = RemoteCommand(
            command_id=f"cmd-e2e-close-all-{int(now_ts)}",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=now_ts,
            terminal_id="ALL",
            params={"reason": "MOBILE_PANIC_BUTTON_PRESSED"},
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # 1. Mobile App pushes to Cloud Relay
        push_receipt = self.relay_client.push_command(envelope, license_id=self.license_id)
        self.assertTrue(push_receipt.get("success"))

        # 2. Local Engine pops from Cloud Relay and processes
        receipt = asyncio.run(self.relay_client.poll_and_dispatch_once(self.receiver))

        self.assertIsNotNone(receipt)
        self.assertTrue(receipt["success"])
        self.assertEqual(receipt["command"], "CLOSE_ALL_TRADES")
        self.assertEqual(self.orchestrator.panic_calls, 1)
        self.assertEqual(len(self.orchestrator.get_active_orders()), 0)

    def test_15_live_e2e_replay_attack_5_minutes_later_fails(self):
        """
        TASK 1 END-TO-END MANDATE:
        Intercept a valid encrypted payload from the live Cloud Relay queue.
        Attempt to push the exact same payload to the Relay queue 5 minutes later.
        Assert that the Local Engine rejects it based on the timestamp and does not liquidate trades.
        """
        t0 = self.clock.now()
        cmd = RemoteCommand(
            command_id=f"cmd-e2e-intercepted-{int(t0)}",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=t0,
            terminal_id="ALL",
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # Mobile pushes command at t0
        self.relay_client.push_command(envelope, license_id=self.license_id)

        # Intercept payload by popping it from the relay queue
        intercepted_payload = self.relay_client.pop_command(self.license_id)
        self.assertIsNotNone(intercepted_payload)

        # Advance engine clock by 5 minutes (300 seconds)
        self.clock.advance(300.0)

        # Attacker attempts to re-push the intercepted payload to the Cloud Relay queue 5 minutes later
        self.relay_client.push_command(intercepted_payload, license_id=self.license_id)

        # Local Engine pops the replayed message from the Cloud Relay
        popped_replayed = self.relay_client.pop_command(self.license_id)
        self.assertIsNotNone(popped_replayed)

        # Local Engine security pipeline must strictly reject the replayed payload
        with self.assertRaises(StaleTimestampError) as ctx:
            asyncio.run(self.receiver.process_remote_envelope(popped_replayed))

        self.assertIn("300.0s", str(ctx.exception))
        self.assertEqual(self.orchestrator.panic_calls, 0, "No trades liquidated on replayed command")
        self.assertEqual(len(self.orchestrator.get_active_orders()), 3, "All positions remain open")

    def test_16_live_e2e_spoofed_tampered_payload_rejected(self):
        """
        TASK 2 END-TO-END MANDATE:
        Push an improperly encrypted command (tampered ciphertext) to Cloud Relay.
        Assert that Local Engine pops it, rejects it with MAC tag failure, and logs a security warning.
        """
        t0 = self.clock.now()
        cmd = RemoteCommand(
            command_id=f"cmd-e2e-tamper-{int(t0)}",
            command=RemoteCommandType.CLOSE_ALL_TRADES,
            timestamp=t0,
        )
        envelope = RemoteCommandCrypto.encrypt_command(
            command=cmd,
            passphrase=self.passphrase,
            license_id=self.license_id,
        )

        # Tamper with ciphertext bytes
        cipher_bytes = bytearray(base64.b64decode(envelope.ciphertext))
        cipher_bytes[2] ^= 0x33
        tampered_ciphertext = base64.b64encode(bytes(cipher_bytes)).decode("ascii")

        tampered_envelope = copy.deepcopy(envelope.to_dict())
        tampered_envelope["ciphertext"] = tampered_ciphertext

        # Push tampered envelope to Cloud Relay
        self.relay_client.push_command(tampered_envelope, license_id=self.license_id)

        # Local Engine attempts to process popped payload
        with self.assertRaises(TamperedPayloadError) as ctx:
            asyncio.run(self.relay_client.poll_and_dispatch_once(self.receiver))

        self.assertEqual(ctx.exception.code, SecurityRejectionCode.TAMPERED_CIPHERTEXT)
        self.assertEqual(self.orchestrator.panic_calls, 0, "Tampered payload must not liquidate trades")
        self.assertTrue(len(self.receiver.security_alerts) >= 1)
        self.assertEqual(self.receiver.security_alerts[-1]["code"], "TAMPERED_CIPHERTEXT")


# =============================================================================
# TEST SUITE 3: STRESS & MONTE CARLO ATTACK FUZZING (100 ITERATIONS)
# =============================================================================

class TestRemoteSecurityStressLoop(unittest.TestCase):
    """
    Runs 100 consecutive randomized attack scenarios (replay, bit-flip, wrong key,
    spoofed plaintext, malformed envelopes) to prove 100% rejection reliability.
    """

    def setUp(self):
        self.license_id = "LIC-STRESS-AUDIT-2026"
        self.passphrase = "StrictStressPassphrase_99214#"
        self.clock = MockClock(initial_time=1770000000.0)
        self.orchestrator = MockTestOrchestrator()
        self.receiver = RemoteCommandReceiver(
            passphrase=self.passphrase,
            license_id=self.license_id,
            orchestrator=self.orchestrator,
            max_drift_seconds=60.0,
            clock=self.clock.now,
        )

    def test_17_stress_100_random_attack_scenarios_rejected(self):
        """Asserts 100% failure rate across 100 varied replay and spoofing attacks."""
        rejection_count = 0
        total_runs = 100

        for i in range(total_runs):
            attack_type = i % 5
            self.clock.set_time(1770000000.0 + (i * 10.0))

            if attack_type == 0:
                # 5-minute delayed replay attack
                t_old = self.clock.now() - 305.0
                cmd = RemoteCommand(f"stress-cmd-{i}", RemoteCommandType.CLOSE_ALL_TRADES, timestamp=t_old)
                env = RemoteCommandCrypto.encrypt_command(cmd, self.passphrase, self.license_id)
                with self.assertRaises(StaleTimestampError):
                    asyncio.run(self.receiver.process_remote_envelope(env))
                rejection_count += 1

            elif attack_type == 1:
                # Plaintext JSON spoofing
                payload = json.dumps({"command": "CLOSE_ALL_TRADES", "id": i, "timestamp": self.clock.now()})
                with self.assertRaises(SpoofedPayloadError):
                    asyncio.run(self.receiver.process_remote_envelope(payload))
                rejection_count += 1

            elif attack_type == 2:
                # Bit-flipped ciphertext
                cmd = RemoteCommand(f"stress-tamper-{i}", RemoteCommandType.CLOSE_ALL_TRADES, timestamp=self.clock.now())
                env = RemoteCommandCrypto.encrypt_command(cmd, self.passphrase, self.license_id)
                raw = copy.deepcopy(env.to_dict())
                c_bytes = bytearray(base64.b64decode(raw["ciphertext"]))
                c_bytes[i % len(c_bytes)] ^= 0xAA
                raw["ciphertext"] = base64.b64encode(bytes(c_bytes)).decode("ascii")
                with self.assertRaises(TamperedPayloadError):
                    asyncio.run(self.receiver.process_remote_envelope(raw))
                rejection_count += 1

            elif attack_type == 3:
                # Unauthorized rogue passphrase
                cmd = RemoteCommand(f"stress-rogue-{i}", RemoteCommandType.CLOSE_ALL_TRADES, timestamp=self.clock.now())
                env = RemoteCommandCrypto.encrypt_command(cmd, f"UnauthorizedPassphrase_{i}", self.license_id)
                with self.assertRaises(TamperedPayloadError):
                    asyncio.run(self.receiver.process_remote_envelope(env))
                rejection_count += 1

            elif attack_type == 4:
                # Duplicate nonce replay
                cmd = RemoteCommand(f"stress-dup-fixed-{i}", RemoteCommandType.CLOSE_ALL_TRADES, timestamp=self.clock.now())
                env = RemoteCommandCrypto.encrypt_command(cmd, self.passphrase, self.license_id)
                # First run succeeds
                asyncio.run(self.receiver.process_remote_envelope(env))
                # Replay with same nonce
                with self.assertRaises(ReplayAttackError):
                    asyncio.run(self.receiver.process_remote_envelope(env))
                rejection_count += 1

        self.assertEqual(rejection_count, total_runs)
        self.assertEqual(len(self.receiver.security_alerts), total_runs)


if __name__ == "__main__":
    unittest.main()
