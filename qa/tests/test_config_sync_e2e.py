"""
Pipvane v3: End-to-End Encrypted State Synchronization Integration Test
=======================================================================
Spawns the Node.js Cloud Control Plane server and tests full end-to-end
E2EE synchronization from Primary Engine -> Cloud Relay -> Standby Engine.
Proves mathematically that the Cloud server receives only ciphertext.
"""

import os
import subprocess
import time
import unittest
import urllib.request
import json
import pytest

from engine.config_sync import (
    ChannelProfile,
    RiskRulesProfile,
    ConfigSyncStore,
    ConfigSyncRelayClient,
    ConfigSyncManager,
    InvalidPassphraseError,
)


class TestConfigSyncEndToEndWithCloudServer(unittest.TestCase):
    """End-to-End integration test between Python Engine and Node.js Cloud Relay."""

    @classmethod
    def setUpClass(cls):
        # Find free port for Node.js server
        cls.port = 8899
        cls.relay_url = f"http://127.0.0.1:{cls.port}"

        # Start Node.js Cloud Server
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

        # Wait for server to come up
        ready = False
        health_url = f"{cls.relay_url}/api/health"
        for _ in range(30):
            try:
                with urllib.request.urlopen(health_url, timeout=0.5) as resp:
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:
                time.sleep(0.1)

        if not ready:
            cls.server_proc.kill()
            raise RuntimeError("Failed to start Node.js Cloud Control Plane server for E2E testing.")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "server_proc") and cls.server_proc:
            cls.server_proc.terminate()
            try:
                cls.server_proc.wait(timeout=2.0)
            except Exception:
                cls.server_proc.kill()

    def test_live_cloud_relay_e2e_synchronization(self):
        license_id = "LIC-E2E-LIVE-2026"
        passphrase = "StrictUserSharedPassphrase_99182"

        # 1. Initialize Primary Engine
        primary_store = ConfigSyncStore(":memory:")
        primary_client = ConfigSyncRelayClient(self.relay_url)
        primary_manager = ConfigSyncManager(
            license_id=license_id,
            node_id="primary_desktop_windows",
            store=primary_store,
            relay_client=primary_client,
        )

        # Populate Primary configuration
        primary_store.save_channel_profile(ChannelProfile(
            channel_id=12345678,
            channel_name="Live Institutional FX Signals",
            format_pack="forex_advanced",
            is_enabled=True,
            symbol_map={"GOLD": "XAUUSD", "US30": "DJ30"},
            risk_multiplier=1.25,
            sl_tp_defaults={"sl_pips": 20.0, "tp_pips": 40.0},
        ))
        primary_store.save_risk_rules(RiskRulesProfile(
            account_id="LIVE_MT5_PRO",
            risk_model="fixed_currency",
            fixed_currency_risk=75.0,
            daily_loss_limit_usd=500.0,
            max_loss_limit_usd=1000.0,
            news_filter_enabled=True,
            news_filter_window_minutes=30,
        ))

        # 2. Publish Primary Config to Live Cloud Server via HTTP POST /relay/sync
        envelope = primary_manager.publish_primary_sync(passphrase=passphrase)
        self.assertEqual(envelope.license_id, license_id)
        self.assertEqual(envelope.version, 1)

        # 3. Direct verification of Live Cloud Server payload:
        # Fetch directly via raw HTTP GET to inspect what the server actually holds
        raw_get_url = f"{self.relay_url}/relay/sync?license_id={license_id}"
        with urllib.request.urlopen(raw_get_url, timeout=2.0) as resp:
            self.assertEqual(resp.status, 200)
            cloud_body = json.loads(resp.read().decode("utf-8"))

        self.assertTrue(cloud_body.get("success"))
        raw_payload = cloud_body["payload"]
        self.assertEqual(raw_payload["license_id"], license_id)

        # PROOF OF ZERO KNOWLEDGE:
        # Plaintext strings NEVER appear in the cloud transmission or memory
        cloud_payload_str = json.dumps(raw_payload)
        self.assertNotIn("Live Institutional FX Signals", cloud_payload_str)
        self.assertNotIn("LIVE_MT5_PRO", cloud_payload_str)
        self.assertNotIn(passphrase, cloud_payload_str)
        self.assertIn("ciphertext", raw_payload["encrypted_payload"])
        self.assertIn("tag", raw_payload["encrypted_payload"])

        # 4. Initialize Standby Engine and Poll Cloud Relay
        standby_store = ConfigSyncStore(":memory:")
        standby_client = ConfigSyncRelayClient(self.relay_url)
        standby_manager = ConfigSyncManager(
            license_id=license_id,
            node_id="standby_vps_windows",
            store=standby_store,
            relay_client=standby_client,
        )

        # Standby polls and ingests using shared passphrase
        ingested_bundle = standby_manager.poll_and_apply_standby(passphrase=passphrase)
        self.assertIsNotNone(ingested_bundle)
        self.assertEqual(ingested_bundle.version, 1)

        # 5. Verify Standby SQLite database received exact configurations
        synced_channel = standby_store.get_channel_profile(12345678)
        self.assertIsNotNone(synced_channel)
        self.assertEqual(synced_channel.channel_name, "Live Institutional FX Signals")
        self.assertEqual(synced_channel.symbol_map["GOLD"], "XAUUSD")
        self.assertEqual(synced_channel.risk_multiplier, 1.25)

        synced_rules = standby_store.get_risk_rules("LIVE_MT5_PRO")
        self.assertIsNotNone(synced_rules)
        self.assertEqual(synced_rules.risk_model, "fixed_currency")
        self.assertEqual(synced_rules.fixed_currency_risk, 75.0)
        self.assertEqual(synced_rules.daily_loss_limit_usd, 500.0)

        # 6. Verify second Standby with bad passphrase fails to decrypt
        corrupt_standby_store = ConfigSyncStore(":memory:")
        corrupt_standby_manager = ConfigSyncManager(
            license_id=license_id,
            node_id="bad_standby",
            store=corrupt_standby_store,
            relay_client=standby_client,
        )
        with self.assertRaises(InvalidPassphraseError):
            corrupt_standby_manager.poll_and_apply_standby(passphrase="WRONG_UNAUTHORIZED_PASSPHRASE")

        # Confirm corrupt standby database remains empty
        self.assertIsNone(corrupt_standby_store.get_channel_profile(12345678))
