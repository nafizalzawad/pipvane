"""
Unit tests validating Strict Safety Mode enforcement.
Verifies that outbound write, reaction, message sending, and channel join/leave operations
are completely prohibited and intercepted.
"""

import pytest

from engine.safety_mode import (
    assert_safety_mode_allows,
    SafetyModeViolationError,
)
from engine.telegram_gateway import TelegramGateway
from engine.tdlib_client import MockTDLibClient
from engine.vault import VaultStorage


@pytest.mark.unit
@pytest.mark.parametrize("method", [
    "sendMessage",
    "sendChatAction",
    "forwardMessages",
    "sendBotStartMessage",
    "joinChat",
    "joinChatByInviteLink",
    "leaveChat",
    "setMessageReactions",
    "addReaction",
    "removeReaction",
    "addChatMember",
    "deleteChat",
    "deleteMessages",
    "editMessageText",
    "pinChatMessage",
    "setChatTitle",
    "createPrivateChat",
    "createSupergroupChat",
])
def test_safety_mode_blocks_forbidden_methods(method):
    """Verify that every explicit forbidden TDLib method raises SafetyModeViolationError."""
    with pytest.raises(SafetyModeViolationError) as exc_info:
        assert_safety_mode_allows(method)
    assert "strictly forbidden" in str(exc_info.value) or "prohibited" in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.parametrize("allowed_method", [
    "setTdlibParameters",
    "setAuthenticationPhoneNumber",
    "checkAuthenticationCode",
    "checkAuthenticationPassword",
    "requestQrCodeAuthentication",
    "getChat",
    "getChats",
    "getChatHistory",
    "getMessage",
    "getMessages",
    "getMe",
    "getUser",
    "searchChatMessages",
    "getConnectionState",
    "close",
])
def test_safety_mode_allows_read_only_methods(allowed_method):
    """Verify that read-only ingestion and auth methods pass without error."""
    # Should not raise
    assert_safety_mode_allows(allowed_method)


@pytest.mark.unit
def test_gateway_safety_mode_immutable():
    """Verify that gateway.safety_mode is always True and cannot be disabled."""
    mock_client = MockTDLibClient()
    gateway = TelegramGateway(
        api_id=12345,
        api_hash="fake_hash",
        tdlib_client=mock_client,
        vault=VaultStorage(db_path=":memory:"),
    )

    assert gateway.safety_mode is True

    # Attempting to disable safety mode must raise SafetyModeViolationError
    with pytest.raises(SafetyModeViolationError):
        gateway.safety_mode = False


@pytest.mark.unit
def test_gateway_direct_outbound_methods_prohibited():
    """
    Verify that calling outbound methods on TelegramGateway raises SafetyModeViolationError.
    """
    mock_client = MockTDLibClient()
    gateway = TelegramGateway(
        api_id=12345,
        api_hash="fake_hash",
        tdlib_client=mock_client,
        vault=VaultStorage(db_path=":memory:"),
    )

    with pytest.raises(SafetyModeViolationError):
        gateway.send_message(chat_id=123, text="Test signal")

    with pytest.raises(SafetyModeViolationError):
        gateway.join_channel(channel_username="pipvane_signals")

    with pytest.raises(SafetyModeViolationError):
        gateway.leave_channel(chat_id=123)

    with pytest.raises(SafetyModeViolationError):
        gateway.react_to_message(chat_id=123, message_id=456, reaction="👍")

    with pytest.raises(SafetyModeViolationError):
        gateway.forward_messages(from_chat_id=123, to_chat_id=456, message_ids=[789])


@pytest.mark.unit
def test_gateway_send_request_intercepts_forbidden():
    """
    Verify that attempting to invoke forbidden methods via send_request is intercepted.
    """
    mock_client = MockTDLibClient()
    gateway = TelegramGateway(
        api_id=12345,
        api_hash="fake_hash",
        tdlib_client=mock_client,
        vault=VaultStorage(db_path=":memory:"),
    )

    with pytest.raises(SafetyModeViolationError):
        gateway.send_request({
            "@type": "sendMessage",
            "chat_id": 999,
            "input_message_content": {
                "@type": "inputMessageText",
                "text": {"@type": "formattedText", "text": "prohibited payload"}
            }
        })

    # Ensure client inbox never received the forbidden request
    assert len(mock_client.sent_requests) == 0
