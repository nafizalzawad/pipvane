"""
Unit tests validating TelegramGateway authentication, ingestion, backfill,
and reconnection resilience without live network dependencies.
"""

import time
import pytest

from engine.vault import VaultStorage
from engine.tdlib_client import MockTDLibClient
from engine.telegram_gateway import TelegramGateway


@pytest.mark.unit
def test_gateway_initialization_and_tdlib_parameters():
    """
    Verify gateway configures TDLib parameters with DPAPI-encrypted key
    and stable device profile on start.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
    )
    gateway.start()

    # Wait for parameters to be sent
    time.sleep(0.05)
    gateway.stop()

    # Check requests sent by gateway
    param_reqs = [r for r in mock_client.sent_requests if r.get("@type") == "setTdlibParameters"]
    assert len(param_reqs) == 1
    req = param_reqs[0]
    assert req["api_id"] == 123456
    assert req["api_hash"] == "mock_api_hash_abc"
    assert len(req["database_encryption_key"]) == 64  # Hex key from vault
    assert "device_model" in req
    assert "system_version" in req
    assert "application_version" in req


@pytest.mark.unit
def test_gateway_phone_authentication_flow():
    """
    Verify phone number login flow through code verification to READY.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
    )
    gateway.start()
    time.sleep(0.05)

    assert gateway.auth_state == "authorizationStateWaitPhoneNumber"

    # Step 1: Submit phone number
    gateway.login_with_phone("+12025550199")
    time.sleep(0.05)
    assert gateway.auth_state == "authorizationStateWaitCode"

    # Step 2: Submit SMS/Telegram code
    gateway.submit_auth_code("12345")
    time.sleep(0.05)
    assert gateway.auth_state == "authorizationStateReady"
    assert gateway.wait_until_ready(timeout=1.0) is True

    gateway.stop()


@pytest.mark.unit
def test_gateway_2fa_password_flow():
    """
    Verify 2FA password flow when code requires two-factor authentication.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
    )
    gateway.start()
    time.sleep(0.05)

    gateway.login_with_phone("+12025550199")
    time.sleep(0.05)

    # Submit code that triggers 2FA in MockTDLibClient
    gateway.submit_auth_code("2FA")
    time.sleep(0.05)
    assert gateway.auth_state == "authorizationStateWaitPassword"

    # Submit 2FA password
    gateway.submit_2fa_password("MySuper2FASecret!")
    time.sleep(0.05)
    assert gateway.auth_state == "authorizationStateReady"
    assert gateway.wait_until_ready(timeout=1.0) is True

    gateway.stop()


@pytest.mark.unit
def test_gateway_qr_code_authentication():
    """
    Verify QR code login sequence returning tg://login URL.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
    )
    gateway.start()
    time.sleep(0.05)

    # Request QR code
    qr_link = gateway.login_with_qr_code(timeout=2.0)
    assert qr_link.startswith("tg://login?token=")
    assert gateway.auth_state == "authorizationStateWaitOtherDeviceConfirmation"

    gateway.stop()


@pytest.mark.unit
def test_gateway_message_ingestion_and_checkpoint():
    """
    Verify incoming message updates trigger registered callback and update vault checkpoint.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
    )

    received_messages = []
    gateway.add_message_handler(lambda msg: received_messages.append(msg))
    target_chat = -1001234567890
    gateway.subscribe_chat(target_chat)

    gateway.start()
    time.sleep(0.05)

    now = time.time()
    mock_client.simulate_incoming_update({
        "@type": "updateNewMessage",
        "message": {
            "id": 1001,
            "chat_id": target_chat,
            "date": int(now),
            "content": {
                "@type": "messageText",
                "text": {"@type": "formattedText", "text": "BUY EURUSD 1.0850 SL 1.0800 TP 1.0950"}
            }
        }
    })

    time.sleep(0.1)

    assert len(received_messages) == 1
    assert received_messages[0]["id"] == 1001
    assert vault.get_chat_checkpoint(target_chat) == 1001

    gateway.stop()


@pytest.mark.unit
def test_gateway_signal_age_guard_drops_stale_messages():
    """
    Verify FR-7.6 Signal Age Guard: drops incoming signals older than max_signal_age_s (30s).
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
        max_signal_age_s=30.0,
    )

    received_messages = []
    gateway.add_message_handler(lambda msg: received_messages.append(msg))
    target_chat = -1001234567890
    gateway.subscribe_chat(target_chat)

    gateway.start()
    time.sleep(0.05)

    now = time.time()
    # Stale message: 60 seconds old (> 30s threshold)
    mock_client.simulate_incoming_update({
        "@type": "updateNewMessage",
        "message": {
            "id": 2001,
            "chat_id": target_chat,
            "date": int(now - 60),
            "content": {
                "@type": "messageText",
                "text": {"@type": "formattedText", "text": "OLD SIGNAL - SHOULD BE DROPPED"}
            }
        }
    })

    time.sleep(0.1)

    # Must be dropped!
    assert len(received_messages) == 0
    assert vault.get_chat_checkpoint(target_chat) is None

    gateway.stop()


@pytest.mark.unit
def test_gateway_backfill_reconciliation_logic():
    """
    Verify FR-7.6 Compliance: Backfill queries chat message history
    since last_processed_message_id, filters out already-processed and stale signals,
    and updates checkpoints.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    vault = VaultStorage(db_path=":memory:")

    target_chat = -100999888777
    # Set existing checkpoint in vault: message ID 500
    vault.update_chat_checkpoint(target_chat, 500)

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
        max_signal_age_s=30.0,
    )

    received_backfill = []
    gateway.add_message_handler(lambda msg: received_backfill.append(msg))
    gateway.subscribe_chat(target_chat)

    now = time.time()
    # Configure mock chat history with 4 messages:
    # 503: Fresh (date = now - 5s) -> SHOULD BE DISPATCHED
    # 502: Stale (date = now - 45s) -> SHOULD BE DROPPED by Signal Age Guard
    # 500: Already processed (id <= last_id) -> SHOULD BE IGNORED
    # 499: Already processed (id <= last_id) -> SHOULD BE IGNORED
    mock_client.set_mock_chat_history(target_chat, [
        {"id": 503, "chat_id": target_chat, "date": int(now - 5), "text": "SIGNAL 503"},
        {"id": 502, "chat_id": target_chat, "date": int(now - 45), "text": "SIGNAL 502"},
        {"id": 500, "chat_id": target_chat, "date": int(now - 60), "text": "SIGNAL 500"},
        {"id": 499, "chat_id": target_chat, "date": int(now - 70), "text": "SIGNAL 499"},
    ])

    gateway.start()
    time.sleep(0.05)

    # Trigger backfill
    gateway.backfill_chat(target_chat)
    time.sleep(0.1)

    # Only message 503 should be accepted
    assert len(received_backfill) == 1
    assert received_backfill[0]["id"] == 503

    # Checkpoint in vault must be updated to 503
    assert vault.get_chat_checkpoint(target_chat) == 503

    gateway.stop()


@pytest.mark.unit
def test_gateway_exponential_backoff_and_reconnection():
    """
    Verify auto-reconnection loop:
    1. Exponential backoff delay calculation.
    2. Connection state transitions and automatic backfill upon reconnection.
    """
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateReady")
    vault = VaultStorage(db_path=":memory:")

    gateway = TelegramGateway(
        api_id=123456,
        api_hash="mock_api_hash_abc",
        vault=vault,
        tdlib_client=mock_client,
        base_reconnect_delay=1.0,
        max_reconnect_delay=32.0,
    )

    # Verify backoff calculation math
    d0 = gateway.calculate_backoff_delay(0)
    assert 1.0 <= d0 <= 1.25  # base 1.0 + jitter up to 25%

    d1 = gateway.calculate_backoff_delay(1)
    assert 2.0 <= d1 <= 2.50

    d2 = gateway.calculate_backoff_delay(2)
    assert 4.0 <= d2 <= 5.00

    d10 = gateway.calculate_backoff_delay(10)
    assert 32.0 <= d10 <= 40.0  # capped at max 32 + jitter

    target_chat = -100111222333
    gateway.subscribe_chat(target_chat)

    gateway.start()
    time.sleep(0.05)

    # Simulate network disconnect
    mock_client.simulate_incoming_update({
        "@type": "updateConnectionState",
        "state": {"@type": "connectionStateWaitingForNetwork"}
    })
    time.sleep(0.05)
    assert gateway.connection_state == "connectionStateWaitingForNetwork"

    # Simulate reconnection restored
    mock_client.simulate_incoming_update({
        "@type": "updateConnectionState",
        "state": {"@type": "connectionStateReady"}
    })
    time.sleep(0.05)
    assert gateway.connection_state == "connectionStateReady"

    # Check that backfill was automatically requested
    backfill_reqs = [
        r for r in mock_client.sent_requests
        if r.get("@type") == "getChatHistory" and r.get("chat_id") == target_chat
    ]
    assert len(backfill_reqs) >= 1

    gateway.stop()
