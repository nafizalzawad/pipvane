"""
Unit tests for Pipvane v3 DPAPI SecureVault and DPAPIEncryptor.
Validates:
1. Native Windows DPAPI encryption/decryption roundtrips
2. Secondary entropy enforcement
3. Strict file isolation to %AppData%\\Pipvane\\
4. On-disk SQLite ciphertext verification (Zero plaintext leaks)
5. Runtime volatile RAM decryption
"""

import os
import pytest
import sqlite3
from secure_vault import (
    DPAPIEncryptor,
    SecureVault,
    VaultSecurityError,
    BrokerAccountRecord,
    TelegramSessionRecord
)


def test_dpapi_encryptor_basic_roundtrip():
    encryptor = DPAPIEncryptor()
    plaintext = "SuperSecretBrokerPass#2026!"
    
    ciphertext = encryptor.encrypt(plaintext)
    assert isinstance(ciphertext, bytes)
    assert len(ciphertext) > len(plaintext)
    assert plaintext.encode("utf-8") not in ciphertext  # Ciphertext is thoroughly scrambled
    
    decrypted = encryptor.decrypt_str(ciphertext)
    assert decrypted == plaintext


def test_dpapi_entropy_enforcement():
    entropy_a = b"Salt_Alpha_123"
    entropy_b = b"Salt_Beta_456"
    
    encryptor_a = DPAPIEncryptor(secondary_entropy=entropy_a)
    encryptor_b = DPAPIEncryptor(secondary_entropy=entropy_b)
    
    secret = "ConfidentialTradeKey_XYZ"
    ciphertext = encryptor_a.encrypt(secret)
    
    # Decryption with correct entropy succeeds
    assert encryptor_a.decrypt_str(ciphertext) == secret
    
    # Decryption with wrong entropy must fail
    with pytest.raises(VaultSecurityError):
        encryptor_b.decrypt_str(ciphertext)


def test_file_isolation_rejection_unauthorized_paths(tmp_path):
    # Attempting to place db on another drive or outside %AppData%\Pipvane
    invalid_paths = [
        r"C:\Windows\Temp\vault.db",
        r"C:\Users\Public\vault.db",
        os.path.join(str(tmp_path), "unauthorized.db")
    ]
    
    for path in invalid_paths:
        with pytest.raises(VaultSecurityError) as exc_info:
            SecureVault(db_path=path)
        assert "File isolation check failed" in str(exc_info.value)


def test_file_isolation_canonical_path():
    canonical_dir = SecureVault.get_canonical_vault_directory()
    expected_appdata = os.environ.get("APPDATA")
    assert expected_appdata in canonical_dir
    assert canonical_dir.endswith("Pipvane")


def test_broker_account_zero_plaintext_leak_on_disk(tmp_path):
    # Use isolated test sandbox directory to protect production vault during test
    sandbox = str(tmp_path / "Pipvane")
    db_file = os.path.join(sandbox, "test_vault.db")
    
    master_pass = "UltraSecretMasterPassword999!"
    investor_pass = "ReadonlyInvestorPass111!"
    
    with SecureVault(db_path=db_file, _test_sandbox_dir=sandbox) as vault:
        vault.save_broker_account(
            account_id="acct_icmarkets_01",
            platform="MT5",
            broker_server="ICMarketsSC-Live05",
            account_login=10928374,
            master_password=master_pass,
            investor_password=investor_pass
        )
        
        # 1. Audit on-disk rows: verify raw SQLite database contains NO plaintext
        raw_rows = vault.inspect_raw_on_disk_rows("broker_accounts")
        assert len(raw_rows) == 1
        raw_row = raw_rows[0]
        
        # Plaintext must NOT appear in the BLOB
        assert isinstance(raw_row["master_password_blob"], bytes)
        assert isinstance(raw_row["investor_password_blob"], bytes)
        assert master_pass.encode("utf-8") not in raw_row["master_password_blob"]
        assert investor_pass.encode("utf-8") not in raw_row["investor_password_blob"]
        
        # 2. Querying external raw sqlite connection also proves zero plaintext on disk
        raw_conn = sqlite3.connect(db_file)
        raw_cur = raw_conn.cursor()
        raw_cur.execute("SELECT master_password_blob, investor_password_blob FROM broker_accounts")
        row = raw_cur.fetchone()
        assert master_pass.encode("utf-8") not in row[0]
        assert investor_pass.encode("utf-8") not in row[1]
        raw_conn.close()
        
        # 3. Retrieve through SecureVault: correctly decrypted in volatile memory
        acct = vault.get_broker_account("acct_icmarkets_01")
        assert acct is not None
        assert acct.account_id == "acct_icmarkets_01"
        assert acct.platform == "MT5"
        assert acct.broker_server == "ICMarketsSC-Live05"
        assert acct.account_login == 10928374
        assert acct.master_password == master_pass
        assert acct.investor_password == investor_pass
        
        # 4. List accounts: metadata only, no password fields exposed
        account_list = vault.list_broker_accounts()
        assert len(account_list) == 1
        assert "master_password" not in account_list[0]
        assert "master_password_blob" not in account_list[0]


def test_telegram_session_encryption_roundtrip(tmp_path):
    sandbox = str(tmp_path / "Pipvane")
    db_file = os.path.join(sandbox, "test_vault.db")
    
    session_secret = "1BvBMQeyAAACD3V2...long_tdlib_session_auth_key_string..."
    api_hash_secret = "0123456789abcdef0123456789abcdef"
    
    with SecureVault(db_path=db_file, _test_sandbox_dir=sandbox) as vault:
        vault.save_telegram_session(
            session_id="tg_session_primary",
            phone_number="+15550192834",
            session_string=session_secret,
            api_id=9876543,
            api_hash=api_hash_secret
        )
        
        # Raw inspection proves encryption
        raw_rows = vault.inspect_raw_on_disk_rows("telegram_sessions")
        assert len(raw_rows) == 1
        assert session_secret.encode("utf-8") not in raw_rows[0]["session_blob"]
        assert api_hash_secret.encode("utf-8") not in raw_rows[0]["api_hash_blob"]
        
        # Decrypted in RAM
        sess = vault.get_telegram_session("tg_session_primary")
        assert sess is not None
        assert sess.session_string == session_secret
        assert sess.api_id == 9876543
        assert sess.api_hash == api_hash_secret
        
        # Delete session
        deleted = vault.delete_telegram_session("tg_session_primary")
        assert deleted is True
        assert vault.get_telegram_session("tg_session_primary") is None


def test_generic_secrets_crud(tmp_path):
    sandbox = str(tmp_path / "Pipvane")
    db_file = os.path.join(sandbox, "test_vault.db")
    
    with SecureVault(db_path=db_file, _test_sandbox_dir=sandbox) as vault:
        vault.set_secret("cloud_jwt_token", "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...", category="license")
        
        val = vault.get_secret("cloud_jwt_token")
        assert val == "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
        
        # Non-existent key
        assert vault.get_secret("unknown_key") is None
        
        # Delete
        assert vault.delete_secret("cloud_jwt_token") is True
        assert vault.get_secret("cloud_jwt_token") is None
