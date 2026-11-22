"""
Pipvane v3: Encrypted State Synchronization Unit Tests (Sprint 15 Directive)
=============================================================================
Tests the E2EE configuration sync pipeline between Primary and Standby devices:
1. Non-credential payload bundling & zero-knowledge sanitizer.
2. AES-256-GCM encryption & PBKDF2 key derivation from user passphrase.
3. Decryption authentication tag checks and wrong passphrase rejection.
4. SQLite local storage updates (Channel Profiles, Risk Rules, Sync State).
5. Relay client transmission and Standby ingestion lifecycle.
6. Standby asynchronous sync daemon.
"""

import asyncio
import copy
import json
import os
import sqlite3
import time
import unittest
from unittest.mock import MagicMock, patch

import pytest

from engine.config_sync import (
    ChannelProfile,
    RiskRulesProfile,
    SyncBundle,
    EncryptedSyncEnvelope,
    ConfigCrypto,
    ConfigSyncStore,
    ConfigSyncRelayClient,
    ConfigSyncManager,
    ConfigSyncError,
    PassphraseRequiredError,
    InvalidPassphraseError,
    CredentialLeakPreventedError,
    RelayCommunicationError,
    SyncVersionOutdatedError,
    sanitize_config_payload,
)


class TestConfigSyncProfilesAndSanitizer(unittest.TestCase):
    """Tests data models and Zero-Knowledge sanitizer."""

    def test_channel_profile_roundtrip(self):
        profile = ChannelProfile(
            channel_id=100123456,
            channel_name="VIP Forex Signals",
            format_pack="forex_v1",
            is_enabled=True,
            symbol_map={"GOLD": "XAUUSD", "US30": "DJ30"},
            risk_multiplier=1.5,
            sl_tp_defaults={"sl_pips": 25.0, "tp_pips": 50.0},
            custom_rules={"allow_pending": True},
        )
        d = profile.to_dict()
        restored = ChannelProfile.from_dict(d)
        self.assertEqual(restored.channel_id, 100123456)
        self.assertEqual(restored.channel_name, "VIP Forex Signals")
        self.assertEqual(restored.symbol_map["GOLD"], "XAUUSD")
        self.assertEqual(restored.risk_multiplier, 1.5)

    def test_risk_rules_profile_roundtrip(self):
        rules = RiskRulesProfile(
            account_id="ACC_MT5_DEMO",
            risk_model="percent_equity",
            fixed_lot=0.2,
            equity_risk_pct=1.5,
            daily_loss_limit_usd=750.0,
            max_loss_limit_usd=1500.0,
            news_filter_enabled=True,
            news_filter_window_minutes=45,
            slippage_tolerance_pips=2.0,
        )
        d = rules.to_dict()
        restored = RiskRulesProfile.from_dict(d)
        self.assertEqual(restored.account_id, "ACC_MT5_DEMO")
        self.assertEqual(restored.equity_risk_pct, 1.5)
        self.assertEqual(restored.daily_loss_limit_usd, 750.0)
        self.assertEqual(restored.news_filter_window_minutes, 45)

    def test_sanitizer_permits_clean_config(self):
        clean_payload = {
            "channel_id": 100123,
            "channel_name": "Safe Forex Channel",
            "risk_rules": {"max_drawdown": 4.5, "symbol": "EURUSD"},
        }
        sanitized = sanitize_config_payload(clean_payload, strict=True)
        self.assertEqual(sanitized["channel_name"], "Safe Forex Channel")

    def test_sanitizer_blocks_credential_keys_in_strict_mode(self):
        dirty_payloads = [
            {"account": "123", "password": "super_secret_broker_password"},
            {"account": "123", "master_password": "mt5_master_password"},
            {"account": "123", "session_blob": "raw_telegram_session_hex"},
            {"account": "123", "api_hash": "telegram_api_hash_secret"},
            {"channel": "vip", "token": "bot_token_secret"},
        ]
        for payload in dirty_payloads:
            with self.assertRaises(CredentialLeakPreventedError):
                sanitize_config_payload(payload, strict=True)


class TestConfigSyncStore(unittest.TestCase):
    """Tests SQLite persistence for profiles and sync state."""

    def setUp(self):
        self.store = ConfigSyncStore(":memory:")

    def test_channel_profile_crud(self):
        p1 = ChannelProfile(
            channel_id=101,
            channel_name="Forex Alpha",
            format_pack="alpha_pack",
            symbol_map={"GOLD": "XAUUSD"},
        )
        p2 = ChannelProfile(
            channel_id=102,
            channel_name="Crypto Scalper",
            format_pack="crypto_pack",
            symbol_map={"BTC": "BTCUSD"},
        )
        self.store.save_channel_profile(p1)
        self.store.save_channel_profile(p2)

        retrieved = self.store.get_channel_profile(101)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.channel_name, "Forex Alpha")
        self.assertEqual(retrieved.symbol_map["GOLD"], "XAUUSD")

        all_profiles = self.store.list_channel_profiles()
        self.assertEqual(len(all_profiles), 2)

    def test_risk_rules_crud(self):
        r1 = RiskRulesProfile(
            account_id="ACC_1",
            risk_model="fixed_currency",
            fixed_currency_risk=100.0,
            daily_loss_limit_usd=500.0,
        )
        self.store.save_risk_rules(r1)

        retrieved = self.store.get_risk_rules("ACC_1")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.risk_model, "fixed_currency")
        self.assertEqual(retrieved.fixed_currency_risk, 100.0)

    def test_bundle_export_and_apply(self):
        # Setup source store
        self.store.save_channel_profile(ChannelProfile(channel_id=500, channel_name="Gold VIP"))
        self.store.save_risk_rules(RiskRulesProfile(account_id="PRIMARY_ACC", equity_risk_pct=2.0))

        bundle = self.store.export_bundle(
            license_id="LIC-TEST-123",
            source_node_id="node_primary",
            general_settings={"slippage": 1.0},
        )
        self.assertEqual(bundle.version, 1)
        self.assertEqual(len(bundle.channel_profiles), 1)

        # Apply to target standby store
        standby_store = ConfigSyncStore(":memory:")
        applied = standby_store.apply_bundle(bundle)
        self.assertTrue(applied)

        # Verify ingested data
        synced_ch = standby_store.get_channel_profile(500)
        self.assertIsNotNone(synced_ch)
        self.assertEqual(synced_ch.channel_name, "Gold VIP")

        synced_rules = standby_store.get_risk_rules("PRIMARY_ACC")
        self.assertIsNotNone(synced_rules)
        self.assertEqual(synced_rules.equity_risk_pct, 2.0)

        # Version check: applying older or same version returns False
        reapplied = standby_store.apply_bundle(bundle)
        self.assertFalse(reapplied, "Applying older/identical version must be skipped")


class TestConfigCryptoAESGCM(unittest.TestCase):
    """Tests AES-256-GCM encryption, PBKDF2 derivation, and tamper resistance."""

    def setUp(self):
        self.passphrase = "UltraSecureSyncPassphrase2026!"
        self.bundle = SyncBundle(
            bundle_id="b_001",
            version=1,
            timestamp=time.time(),
            source_node_id="primary_node",
            license_id="LIC-PRO-2026-X889",
            channel_profiles=[{"channel_id": 1, "channel_name": "Alpha"}],
            risk_rules={"ACC_1": {"equity_risk_pct": 1.0}},
            general_settings={"news_filter": True},
        )

    def test_encryption_and_decryption_roundtrip(self):
        envelope = ConfigCrypto.encrypt_bundle(self.bundle, passphrase=self.passphrase)
        self.assertEqual(envelope.license_id, "LIC-PRO-2026-X889")
        self.assertEqual(envelope.version, 1)

        enc_payload = envelope.encrypted_payload
        self.assertEqual(enc_payload["kdf"], "PBKDF2-HMAC-SHA256")
        self.assertIn("ciphertext", enc_payload)
        self.assertIn("salt", enc_payload)
        self.assertIn("iv", enc_payload)
        self.assertIn("tag", enc_payload)

        # Plaintext is not present in envelope
        self.assertNotIn("Alpha", str(enc_payload))

        # Decrypt with correct passphrase
        decrypted_bundle = ConfigCrypto.decrypt_envelope(envelope, passphrase=self.passphrase)
        self.assertEqual(decrypted_bundle.bundle_id, "b_001")
        self.assertEqual(decrypted_bundle.version, 1)
        self.assertEqual(len(decrypted_bundle.channel_profiles), 1)
        self.assertEqual(decrypted_bundle.channel_profiles[0]["channel_name"], "Alpha")

    def test_decryption_rejects_wrong_passphrase(self):
        envelope = ConfigCrypto.encrypt_bundle(self.bundle, passphrase=self.passphrase)
        with self.assertRaises(InvalidPassphraseError):
            ConfigCrypto.decrypt_envelope(envelope, passphrase="WRONG_USER_PASSPHRASE")

    def test_decryption_rejects_empty_passphrase(self):
        with self.assertRaises(PassphraseRequiredError):
            ConfigCrypto.encrypt_bundle(self.bundle, passphrase="")

    def test_decryption_rejects_tampered_ciphertext(self):
        envelope = ConfigCrypto.encrypt_bundle(self.bundle, passphrase=self.passphrase)
        # Corrupt ciphertext bytes
        enc = copy.deepcopy(envelope)
        raw_cipher = enc.encrypted_payload["ciphertext"]
        # Invert last char
        corrupted = raw_cipher[:-2] + ("A" if raw_cipher[-2] != "A" else "B") + raw_cipher[-1]
        enc.encrypted_payload["ciphertext"] = corrupted

        with self.assertRaises(InvalidPassphraseError):
            ConfigCrypto.decrypt_envelope(enc, passphrase=self.passphrase)


class TestConfigSyncManagerAndRelay(unittest.TestCase):
    """Tests end-to-end sync between Primary, Mock Relay, and Standby."""

    def setUp(self):
        self.passphrase = "CorrectPassphrase2026"
        self.license_id = "LIC-TEST-XYZ"

        self.primary_store = ConfigSyncStore(":memory:")
        self.standby_store = ConfigSyncStore(":memory:")

        # In-memory mock cloud relay
        self.mock_relay_db: dict = {}

        # Configure mock relay client
        self.mock_client = MagicMock(spec=ConfigSyncRelayClient)

        def mock_upload(envelope, timeout_s=5.0):
            self.mock_relay_db[envelope.license_id] = envelope
            return {
                "success": True,
                "license_id": envelope.license_id,
                "version": envelope.version,
                "expires_at": time.time() + 86400,
            }

        def mock_fetch(license_id, timeout_s=5.0):
            return self.mock_relay_db.get(license_id)

        self.mock_client.upload_encrypted_envelope.side_effect = mock_upload
        self.mock_client.fetch_encrypted_envelope.side_effect = mock_fetch

        self.primary_manager = ConfigSyncManager(
            license_id=self.license_id,
            node_id="primary_node_01",
            store=self.primary_store,
            relay_client=self.mock_client,
        )

        self.standby_manager = ConfigSyncManager(
            license_id=self.license_id,
            node_id="standby_node_02",
            store=self.standby_store,
            relay_client=self.mock_client,
        )

    def test_end_to_end_state_sync(self):
        # 1. Primary configures channel profiles and risk rules
        p = ChannelProfile(
            channel_id=999,
            channel_name="Master Signal Provider",
            format_pack="forex_gold",
            symbol_map={"GOLD": "XAUUSD"},
            risk_multiplier=1.2,
        )
        r = RiskRulesProfile(
            account_id="ACC_PRIMARY",
            risk_model="percent_equity",
            equity_risk_pct=1.5,
            daily_loss_limit_usd=600.0,
        )
        self.primary_store.save_channel_profile(p)
        self.primary_store.save_risk_rules(r)

        # 2. Primary publishes encrypted bundle to relay
        envelope = self.primary_manager.publish_primary_sync(passphrase=self.passphrase)
        self.assertIn(self.license_id, self.mock_relay_db)

        # 3. Verify Cloud Relay only received opaque ciphertext (Zero-Knowledge)
        stored_on_cloud = self.mock_relay_db[self.license_id]
        raw_cloud_json = json.dumps(stored_on_cloud.to_dict())
        self.assertNotIn("Master Signal Provider", raw_cloud_json)
        self.assertNotIn("ACC_PRIMARY", raw_cloud_json)

        # 4. Standby polls relay and ingests state
        applied_bundle = self.standby_manager.poll_and_apply_standby(passphrase=self.passphrase)
        self.assertIsNotNone(applied_bundle)
        self.assertEqual(applied_bundle.version, 1)

        # 5. Verify Standby SQLite database now contains Primary's exact configurations
        standby_ch = self.standby_store.get_channel_profile(999)
        self.assertIsNotNone(standby_ch)
        self.assertEqual(standby_ch.channel_name, "Master Signal Provider")
        self.assertEqual(standby_ch.symbol_map["GOLD"], "XAUUSD")
        self.assertEqual(standby_ch.risk_multiplier, 1.2)

        standby_r = self.standby_store.get_risk_rules("ACC_PRIMARY")
        self.assertIsNotNone(standby_r)
        self.assertEqual(standby_r.equity_risk_pct, 1.5)
        self.assertEqual(standby_r.daily_loss_limit_usd, 600.0)

    def test_standby_poll_with_wrong_passphrase_raises(self):
        # Primary publishes
        self.primary_store.save_channel_profile(ChannelProfile(channel_id=1, channel_name="Test"))
        self.primary_manager.publish_primary_sync(passphrase=self.passphrase)

        # Standby attempts to ingest with bad passphrase
        with self.assertRaises(InvalidPassphraseError):
            self.standby_manager.poll_and_apply_standby(passphrase="WRONG_PASSPHRASE")

    def test_standby_poll_skips_when_no_new_version(self):
        self.primary_store.save_channel_profile(ChannelProfile(channel_id=1, channel_name="Test"))
        self.primary_manager.publish_primary_sync(passphrase=self.passphrase)

        # First ingestion applies
        res1 = self.standby_manager.poll_and_apply_standby(passphrase=self.passphrase)
        self.assertIsNotNone(res1)

        # Second poll returns None (already up-to-date)
        res2 = self.standby_manager.poll_and_apply_standby(passphrase=self.passphrase)
        self.assertIsNone(res2)


@pytest.mark.asyncio
async def test_standby_sync_loop_daemon():
    """Tests the asynchronous standby sync polling loop."""
    mock_client = MagicMock(spec=ConfigSyncRelayClient)
    envelope = EncryptedSyncEnvelope(
        license_id="LIC-ASYNC-1",
        version=1,
        timestamp=time.time(),
        source_node_id="primary",
        encrypted_payload={},
    )
    mock_client.fetch_encrypted_envelope.return_value = envelope

    manager = ConfigSyncManager(
        license_id="LIC-ASYNC-1",
        node_id="standby_node",
        relay_client=mock_client,
    )

    # Mock poll_and_apply_standby to track calls
    poll_calls = []
    def fake_poll(passphrase, force=False):
        poll_calls.append(time.time())
        return None

    manager.poll_and_apply_standby = fake_poll

    # Start loop in task with very fast interval
    task = asyncio.create_task(manager.start_standby_sync_loop(
        passphrase="pass",
        poll_interval_s=0.05,
    ))

    await asyncio.sleep(0.15)
    manager.stop_standby_sync_loop()
    await asyncio.sleep(0.05)
    task.cancel()

    assert len(poll_calls) >= 2, f"Expected at least 2 poll cycles, got {len(poll_calls)}"
