"""
Unit tests validating DPAPI encryption/decryption, machine entropy,
and VaultStorage SQLite persistence.
"""

import sys
import tempfile
import os
import pytest

from engine.vault import (
    VaultStorage,
    dpapi_protect,
    dpapi_unprotect,
    get_machine_entropy,
    secure_zero,
)


@pytest.mark.unit
def test_dpapi_roundtrip_basic():
    """Verify DPAPI CryptProtectData and CryptUnprotectData roundtrip."""
    secret = b"UltraSecretMasterPassword_2026!"
    ciphertext = dpapi_protect(secret)
    assert ciphertext != secret
    assert len(ciphertext) > len(secret)

    plaintext = dpapi_unprotect(ciphertext)
    assert plaintext == secret


@pytest.mark.unit
def test_dpapi_with_entropy():
    """Verify DPAPI with machine-bound entropy."""
    secret = b"TDLib_Session_Encryption_Master_Key"
    entropy = get_machine_entropy(custom_salt="pipvane_unit_test")
    assert isinstance(entropy, bytes)
    assert len(entropy) == 32

    ciphertext = dpapi_protect(secret, entropy=entropy)
    assert ciphertext != secret

    decrypted = dpapi_unprotect(ciphertext, entropy=entropy)
    assert decrypted == secret

    # Wrong entropy should fail to decrypt on Windows
    if sys.platform == "win32":
        from engine.vault import VaultCryptoError
        wrong_entropy = b"x" * 32
        with pytest.raises(VaultCryptoError):
            dpapi_unprotect(ciphertext, entropy=wrong_entropy)


@pytest.mark.unit
def test_secure_zero():
    """Verify in-memory buffer wiping."""
    buf = bytearray(b"sensitive_credential_bytes")
    assert any(b != 0 for b in buf)
    secure_zero(buf)
    assert all(b == 0 for b in buf)


@pytest.mark.unit
def test_vault_storage_crud():
    """Verify VaultStorage set, get, overwrite, and delete operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_vault.db")
        vault = VaultStorage(db_path=db_path)

        # Non-existent key
        assert vault.get_secret("nonexistent") is None

        # Set and get string
        vault.set_secret("broker_password", "MyTradingPassword123")
        retrieved = vault.get_secret_str("broker_password")
        assert retrieved == "MyTradingPassword123"

        # Set and get raw bytes
        vault.set_secret("raw_bytes", b"\x00\x01\x02\xff")
        assert vault.get_secret("raw_bytes") == b"\x00\x01\x02\xff"

        # Update existing secret
        vault.set_secret("broker_password", "UpdatedPassword456")
        assert vault.get_secret_str("broker_password") == "UpdatedPassword456"

        # Delete secret
        assert vault.delete_secret("broker_password") is True
        assert vault.get_secret("broker_password") is None
        assert vault.delete_secret("broker_password") is False


@pytest.mark.unit
def test_vault_tdlib_key_generation():
    """Verify TDLib encryption key generation and persistence."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_vault.db")
        vault = VaultStorage(db_path=db_path)

        key1 = vault.get_or_create_tdlib_key()
        assert len(key1) == 64  # 32 bytes in hex

        # Subsequent retrieval should return the exact same key
        key2 = vault.get_or_create_tdlib_key()
        assert key1 == key2

        # Re-opening the vault from disk should load the same key
        vault2 = VaultStorage(db_path=db_path)
        assert vault2.get_or_create_tdlib_key() == key1


@pytest.mark.unit
def test_vault_chat_checkpoints():
    """Verify chat checkpoint persistence for backfill reconciliation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_vault.db")
        vault = VaultStorage(db_path=db_path)

        chat_id = -1001234567890

        # Unrecorded chat checkpoint
        assert vault.get_chat_checkpoint(chat_id) is None

        # Record checkpoint
        vault.update_chat_checkpoint(chat_id, 100)
        assert vault.get_chat_checkpoint(chat_id) == 100

        # Update to higher message id
        vault.update_chat_checkpoint(chat_id, 150)
        assert vault.get_chat_checkpoint(chat_id) == 150

        # Attempt to record lower message id - should preserve MAX
        vault.update_chat_checkpoint(chat_id, 120)
        assert vault.get_chat_checkpoint(chat_id) == 150

        # Multiple chats
        vault.update_chat_checkpoint(-1009876543210, 500)
        checkpoints = vault.get_all_chat_checkpoints()
        assert checkpoints[chat_id] == 150
        assert checkpoints[-1009876543210] == 500
