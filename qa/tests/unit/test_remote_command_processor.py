"""
Pipvane v3 - Unit Test Suite: Remote Command Processor (Sprint 17)
==================================================================
Tests the RemoteCommandProcessor module in accordance with Sprint 17 directives:
1. Relay Poller: Background task polling Cloud Relay outbox every 2-3 seconds.
2. Decryption & Validation: E2EE AES-256-GCM using QR-paired shared key, dropping
   commands older than 60 seconds (anti-replay) and rejecting duplicate nonces.
3. Action Dispatcher: Mapping decrypted commands (e.g. {"action": "CLOSE_ALL", "channel": "GoldVIP"})
   to ExecutionOrchestrator manual override functions.
"""

import asyncio
import base64
import json
import time
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from engine.execution_orchestrator import (
    ActionType,
    ExecutionOrchestrator,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    TerminalExecutionConfig,
)
from engine.ipc_protocol import NamedPipeOpcode, ExecutionFeedback
from engine.remote_command_processor import (
    DEFAULT_POLLING_INTERVAL_S,
    MAX_COMMAND_AGE_S,
    CommandAuthenticationError,
    CommandExecutionResult,
    CommandExpiredError,
    DecryptedCommand,
    InvalidCommandPayloadError,
    ReplayAttackError,
    ReplayAttackGuard,
    RemoteCommandCrypto,
    RemoteCommandEnvelope,
    RemoteCommandProcessor,
)


# =============================================================================
# FIXTURES & HELPERS
# =============================================================================

@pytest.fixture
def shared_key() -> bytes:
    """Generates a standard 256-bit symmetric pairing key."""
    return RemoteCommandCrypto.generate_shared_key()


@pytest.fixture
def mock_dispatcher():
    """Mock IPCDispatcher tracking dispatched order intents."""
    dispatcher = MagicMock()
    dispatcher.dispatched_intents = []
    dispatcher._terminals = {}
    feedback_callbacks = []

    dispatcher.on_execution_feedback = lambda cb: feedback_callbacks.append(cb)

    def fake_dispatch(terminal_id: str, intent: OrderIntent):
        dispatcher.dispatched_intents.append((terminal_id, intent))
        feedback = ExecutionFeedback(
            signal_id=intent.signal_id,
            broker_retcode=10009,
            order_ticket=10001,
            executed_volume=intent.volume,
            executed_price=intent.price or 1.0850,
            execution_status=ExecutionStatus.FILLED,
            slippage_pips=0.0,
            execution_latency_us=50,
            error_description="OK",
        )
        for cb in feedback_callbacks:
            try:
                cb(terminal_id, feedback)
            except Exception:
                pass
        return True

    dispatcher.dispatch_order_intent = MagicMock(side_effect=fake_dispatch)
    dispatcher.can_route_to_terminal = MagicMock(return_value=True)
    dispatcher.on_account_state = MagicMock()
    return dispatcher


@pytest.fixture
def orchestrator(mock_dispatcher):
    """Initializes ExecutionOrchestrator with in-memory SQLite storage."""
    orch = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        db_path=":memory:",
    )
    orch.register_terminal_config(TerminalExecutionConfig(terminal_id="mt5_1", platform_type=1))
    orch.register_terminal_config(TerminalExecutionConfig(terminal_id="mt4_1", platform_type=0))
    return orch


def create_mock_clock(initial_time: float = 1770000000.0):
    """Returns a controllable mock clock."""
    current_time = [initial_time]

    def _now() -> float:
        return current_time[0]

    def _advance(seconds: float) -> None:
        current_time[0] += seconds

    def _set(target_time: float) -> None:
        current_time[0] = target_time

    return _now, _advance, _set


# =============================================================================
# 1. QR Code Pairing & Cryptographic Round-Trip
# =============================================================================

def test_qr_code_pairing_round_trip():
    """
    Verifies that the 256-bit shared key and license_id can be exported into
    a standard QR pairing payload and imported back losslessly.
    """
    key = RemoteCommandCrypto.generate_shared_key()
    license_id = "LIC-TEST-QR-99"

    qr_payload = RemoteCommandCrypto.export_qr_pairing_payload(
        license_id=license_id,
        shared_key=key,
        device_name="Pipvane Desktop Workstation",
    )

    imported_lic, imported_key = RemoteCommandCrypto.import_qr_pairing_payload(qr_payload)
    assert imported_lic == license_id
    assert imported_key == key
    assert len(imported_key) == 32


def test_aes_gcm_encrypt_decrypt_round_trip(shared_key):
    """
    Verifies AES-256-GCM encryption and authenticated decryption of a mobile command.
    """
    now = time.time()
    payload = {
        "command_id": "cmd-test-101",
        "action": "CLOSE_ALL",
        "timestamp": now,
        "channel": "GoldVIP",
        "params": {"urgency": "HIGH"},
    }

    envelope: RemoteCommandEnvelope = RemoteCommandCrypto.encrypt_command(
        command_payload=payload,
        shared_key=shared_key,
        license_id="LIC-TEST-01",
    )

    assert envelope.message_id == "cmd-test-101"
    assert envelope.ciphertext is not None
    assert envelope.iv is not None
    assert envelope.tag is not None

    decrypted: DecryptedCommand = RemoteCommandCrypto.decrypt_envelope(
        envelope=envelope,
        shared_key=shared_key,
    )

    assert decrypted.command_id == "cmd-test-101"
    assert decrypted.action == "CLOSE_ALL"
    assert decrypted.channel == "GoldVIP"
    assert abs(decrypted.timestamp - now) < 0.001
    assert decrypted.params.get("urgency") == "HIGH"


def test_decryption_rejects_tampered_ciphertext(shared_key):
    """
    Verifies that any bit-flip or tampering in ciphertext strictly causes
    AES-GCM MAC verification failure (CommandAuthenticationError).
    """
    payload = {"command_id": "cmd-tamper-1", "action": "CLOSE_ALL", "timestamp": time.time()}
    envelope = RemoteCommandCrypto.encrypt_command(payload, shared_key, "LIC-TEST-01")

    # Corrupt ciphertext byte
    raw_bytes = bytearray(base64.b64decode(envelope.ciphertext))
    raw_bytes[0] ^= 0xFF
    corrupted_cipher = base64.b64encode(bytes(raw_bytes)).decode("ascii")
    envelope.ciphertext = corrupted_cipher

    with pytest.raises(CommandAuthenticationError) as exc_info:
        RemoteCommandCrypto.decrypt_envelope(envelope, shared_key)
    assert "authentication tag verification failed" in str(exc_info.value).lower()


def test_decryption_rejects_wrong_shared_key(shared_key):
    """
    Verifies that attempting decryption with an unauthorized / mismatched shared key
    strictly raises CommandAuthenticationError.
    """
    payload = {"command_id": "cmd-wrong-key", "action": "PAUSE_TRADING", "timestamp": time.time()}
    envelope = RemoteCommandCrypto.encrypt_command(payload, shared_key, "LIC-TEST-01")

    wrong_key = RemoteCommandCrypto.generate_shared_key()
    with pytest.raises(CommandAuthenticationError):
        RemoteCommandCrypto.decrypt_envelope(envelope, wrong_key)


# =============================================================================
# 2. Replay Attack Guard & Freshness Validation (Drop > 60s)
# =============================================================================

def test_replay_guard_drops_commands_older_than_60_seconds():
    """
    Mandate: Validate the timestamp to prevent replay attacks (drop commands older than 60 seconds).
    """
    now_fn, advance_fn, set_fn = create_mock_clock(1770000000.0)
    guard = ReplayAttackGuard(max_age_s=MAX_COMMAND_AGE_S, time_provider=now_fn)

    # 1. Fresh command (10 seconds old) -> VALID
    cmd_fresh = DecryptedCommand(
        command_id="cmd-fresh",
        action="CLOSE_ALL",
        timestamp=now_fn() - 10.0,
    )
    guard.validate_and_record(cmd_fresh)  # Does not raise

    # 2. Borderline fresh command (59 seconds old) -> VALID
    cmd_borderline = DecryptedCommand(
        command_id="cmd-border",
        action="CLOSE_ALL",
        timestamp=now_fn() - 59.0,
    )
    guard.validate_and_record(cmd_borderline)  # Does not raise

    # 3. Stale command (61 seconds old) -> DROPPED (CommandExpiredError)
    cmd_stale = DecryptedCommand(
        command_id="cmd-stale-61",
        action="CLOSE_ALL",
        timestamp=now_fn() - 61.0,
    )
    with pytest.raises(CommandExpiredError) as exc_info:
        guard.validate_and_record(cmd_stale)
    assert "older than 60.0s limit" in str(exc_info.value)

    # 4. Delayed 5-minute replay attack (300 seconds old) -> STRICTLY DROPPED
    cmd_replay_5m = DecryptedCommand(
        command_id="cmd-stale-300",
        action="CLOSE_ALL",
        timestamp=now_fn() - 300.0,
    )
    with pytest.raises(CommandExpiredError) as exc_info:
        guard.validate_and_record(cmd_replay_5m)
    assert "age: 300.00s" in str(exc_info.value)


def test_replay_guard_rejects_duplicate_command_id():
    """
    Verifies that a command with the same command_id is strictly rejected if replayed
    even within the 60-second validity window.
    """
    now_fn, advance_fn, set_fn = create_mock_clock(1770000000.0)
    guard = ReplayAttackGuard(max_age_s=60.0, time_provider=now_fn)

    cmd1 = DecryptedCommand(
        command_id="cmd-duplicate-check",
        action="CLOSE_ALL",
        timestamp=now_fn() - 5.0,
    )

    # First presentation succeeds
    guard.validate_and_record(cmd1)

    # Replay attack 2 seconds later with the same command_id
    advance_fn(2.0)
    cmd1_replay = DecryptedCommand(
        command_id="cmd-duplicate-check",
        action="CLOSE_ALL",
        timestamp=now_fn() - 7.0,  # 7 seconds old (well within 60s)
    )

    with pytest.raises(ReplayAttackError) as exc_info:
        guard.validate_and_record(cmd1_replay)
    assert "Replay attack detected" in str(exc_info.value)


def test_replay_guard_rejects_future_timestamp_clock_skew():
    """Verifies that commands with timestamps skewed into the future (>15s) are rejected."""
    now_fn, advance_fn, set_fn = create_mock_clock(1770000000.0)
    guard = ReplayAttackGuard(max_age_s=60.0, future_skew_s=15.0, time_provider=now_fn)

    cmd_future = DecryptedCommand(
        command_id="cmd-future",
        action="CLOSE_ALL",
        timestamp=now_fn() + 25.0,
    )

    with pytest.raises(CommandExpiredError) as exc_info:
        guard.validate_and_record(cmd_future)
    assert "skewed into future" in str(exc_info.value)


# =============================================================================
# 3. Action Dispatcher & ExecutionOrchestrator Integration
# =============================================================================

@pytest.mark.asyncio
async def test_action_dispatcher_close_all_filtered_by_channel(orchestrator, shared_key):
    """
    Action Dispatcher: Map decrypted command {"action": "CLOSE_ALL", "channel": "GoldVIP"}
    to ExecutionOrchestrator manual override functions, closing only matching channel positions.
    """
    # Channel 1: GoldVIP (channel_id = -100111)
    cid_gold = -100111
    orchestrator.register_channel_mapping(cid_gold, "GoldVIP")
    sig_gold = (abs(cid_gold) & 0x7FFFFFFF) << 32 | 101

    # Channel 2: CryptoVIP (channel_id = -100222)
    cid_crypto = -100222
    orchestrator.register_channel_mapping(cid_crypto, "CryptoVIP")
    sig_crypto = (abs(cid_crypto) & 0x7FFFFFFF) << 32 | 201

    # Seed open orders into SQLite
    orchestrator.record_order_intent(
        signal_id=sig_gold,
        ticket=1001,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="XAUUSD",
        side=int(OrderSide.BUY),
        volume=1.0,
        open_price=2650.0,
    )
    orchestrator.record_order_intent(
        signal_id=sig_crypto,
        ticket=1002,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="BTCUSD",
        side=int(OrderSide.BUY),
        volume=0.5,
        open_price=90000.0,
    )

    assert len(orchestrator.get_active_orders()) == 2

    # Initialize RemoteCommandProcessor
    processor = RemoteCommandProcessor(
        license_id="LIC-TEST-DISPATCH",
        shared_key=shared_key,
        orchestrator=orchestrator,
    )

    # Command: {"action": "CLOSE_ALL", "channel": "GoldVIP"}
    command = DecryptedCommand(
        command_id="cmd-dispatch-close-gold",
        action="CLOSE_ALL",
        channel="GoldVIP",
        timestamp=time.time(),
    )

    result = await processor.dispatch_action(command)
    assert result.success is True
    assert result.action == "CLOSE_ALL"
    assert result.dispatched_trades_count == 1
    assert result.details["channel"] == "GoldVIP"

    # Verify positions: GoldVIP is CLOSED, CryptoVIP remains OPEN
    active_after = orchestrator.get_active_orders()
    assert len(active_after) == 1
    assert active_after[0]["ticket"] == 1002
    assert active_after[0]["symbol"] == "BTCUSD"


@pytest.mark.asyncio
async def test_action_dispatcher_close_position_single_ticket(orchestrator, shared_key):
    """
    Tests CLOSE_POSITION command targeting a single order ticket.
    """
    orchestrator.record_order_intent(
        signal_id=999,
        ticket=5555,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="EURUSD",
        side=int(OrderSide.BUY),
        volume=0.20,
        open_price=1.0850,
    )

    processor = RemoteCommandProcessor(
        license_id="LIC-TEST",
        shared_key=shared_key,
        orchestrator=orchestrator,
    )

    command = DecryptedCommand(
        command_id="cmd-close-single",
        action="CLOSE_POSITION",
        ticket=5555,
        timestamp=time.time(),
    )

    result = await processor.dispatch_action(command)
    assert result.success is True
    assert result.action == "CLOSE_POSITION"
    assert len(orchestrator.get_active_orders()) == 0


@pytest.mark.asyncio
async def test_action_dispatcher_close_symbol(orchestrator, shared_key):
    """
    Tests CLOSE_SYMBOL command closing all positions matching symbol.
    """
    orchestrator.record_order_intent(
        signal_id=101,
        ticket=2001,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="XAUUSD",
        side=int(OrderSide.BUY),
        volume=0.10,
        open_price=2650.0,
    )
    orchestrator.record_order_intent(
        signal_id=102,
        ticket=2002,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="XAUUSD",
        side=int(OrderSide.BUY),
        volume=0.20,
        open_price=2655.0,
    )
    orchestrator.record_order_intent(
        signal_id=103,
        ticket=2003,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="EURUSD",
        side=int(OrderSide.BUY),
        volume=0.50,
        open_price=1.0800,
    )

    processor = RemoteCommandProcessor(
        license_id="LIC-TEST",
        shared_key=shared_key,
        orchestrator=orchestrator,
    )

    cmd = DecryptedCommand(
        command_id="cmd-close-sym",
        action="CLOSE_SYMBOL",
        symbol="XAUUSD",
        timestamp=time.time(),
    )

    result = await processor.dispatch_action(cmd)
    assert result.success is True
    assert result.dispatched_trades_count == 2

    remaining = orchestrator.get_active_orders()
    assert len(remaining) == 1
    assert remaining[0]["symbol"] == "EURUSD"


@pytest.mark.asyncio
async def test_action_dispatcher_pause_and_resume_trading(orchestrator, shared_key):
    """
    Tests PAUSE_TRADING and RESUME_TRADING commands:
    When paused, new market order submissions must be rejected with SKIP_TRADING_PAUSED.
    When resumed, normal order submission proceeds.
    """
    processor = RemoteCommandProcessor(
        license_id="LIC-TEST",
        shared_key=shared_key,
        orchestrator=orchestrator,
    )

    # 1. Dispatch PAUSE_TRADING
    pause_cmd = DecryptedCommand(command_id="cmd-pause", action="PAUSE_TRADING", timestamp=time.time())
    res_pause = await processor.dispatch_action(pause_cmd)
    assert res_pause.success is True
    assert orchestrator.is_paused() is True

    # 2. Attempt market entry while paused -> Rejected with SKIP_TRADING_PAUSED
    entry_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.10,
        price=1.0850,
        signal_id=777,
    )
    exec_res = await orchestrator.submit_order("mt5_1", entry_intent)
    assert exec_res.success is False
    assert exec_res.status == ExecutionStatus.REJECTED
    assert exec_res.skip_reason is not None
    assert exec_res.skip_reason.code == "SKIP_TRADING_PAUSED"

    # 3. Dispatch RESUME_TRADING
    resume_cmd = DecryptedCommand(command_id="cmd-resume", action="RESUME_TRADING", timestamp=time.time())
    res_resume = await processor.dispatch_action(resume_cmd)
    assert res_resume.success is True
    assert orchestrator.is_paused() is False

    # 4. Attempt market entry after resume -> Accepted
    exec_res2 = await orchestrator.submit_order("mt5_1", entry_intent)
    assert exec_res2.status != ExecutionStatus.REJECTED or (exec_res2.skip_reason and exec_res2.skip_reason.code != "SKIP_TRADING_PAUSED")


# =============================================================================
# 4. Poller Background Task Lifecycle & Cloud Outbox Polling
# =============================================================================

@pytest.mark.asyncio
async def test_relay_poller_background_task_lifecycle(shared_key):
    """
    Verifies that the background task polls Cloud Relay outbox every 2-3 seconds,
    and cleanly starts and cancels without leaking unhandled tasks.
    """
    poll_count = 0

    def mock_transport(method: str, data: Any, path: str):
        nonlocal poll_count
        if "outbox" in path:
            poll_count += 1
            return {"commands": []}
        return {"success": True}

    processor = RemoteCommandProcessor(
        license_id="LIC-TEST-POLLER",
        shared_key=shared_key,
        polling_interval_s=0.05,  # Accelerated for unit test
        http_transport=mock_transport,
    )

    await processor.start()
    assert processor._is_running is True
    assert processor._poller_task is not None
    assert not processor._poller_task.done()

    # Let it poll several times
    await asyncio.sleep(0.2)
    assert poll_count >= 2

    # Stop poller cleanly
    await processor.stop()
    assert processor._is_running is False
    assert processor._poller_task is None


@pytest.mark.asyncio
async def test_poll_once_end_to_end_decrypt_validate_dispatch_and_ack(orchestrator, shared_key):
    """
    End-to-End poll_once verification:
    1. Queues an encrypted CLOSE_ALL command in mock relay outbox.
    2. Runs poll_once().
    3. Verifies envelope is popped, decrypted, replay checked, dispatched to orchestrator, and ACKed.
    """
    # Seed an open order
    orchestrator.record_order_intent(
        signal_id=888,
        ticket=7001,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="USDJPY",
        side=int(OrderSide.BUY),
        volume=0.5,
        open_price=150.0,
    )
    assert len(orchestrator.get_active_orders()) == 1

    # Encrypt a fresh CLOSE_ALL command
    cmd_data = {
        "command_id": "cmd-e2e-poll-once",
        "action": "CLOSE_ALL",
        "timestamp": time.time(),
    }
    encrypted_env = RemoteCommandCrypto.encrypt_command(
        command_payload=cmd_data,
        shared_key=shared_key,
        license_id="LIC-POLL-ONCE",
    )

    acked_ids: List[str] = []
    outbox_queue = [encrypted_env.to_dict()]

    def mock_relay_transport(method: str, payload: Any, path: str):
        if method == "GET" and "outbox" in path:
            if outbox_queue:
                return {"commands": [outbox_queue.pop(0)]}
            return {"commands": []}
        elif method == "POST" and "ack" in path:
            acked_ids.append(payload.get("message_id"))
            return {"success": True}
        return {"success": True}

    processor = RemoteCommandProcessor(
        license_id="LIC-POLL-ONCE",
        shared_key=shared_key,
        orchestrator=orchestrator,
        http_transport=mock_relay_transport,
    )

    # Execute single poll cycle
    results = await processor.poll_once()
    assert len(results) == 1
    res = results[0]

    assert res.command_id == "cmd-e2e-poll-once"
    assert res.action == "CLOSE_ALL"
    assert res.success is True
    assert res.status == "COMPLETED"

    # Verify positions closed in orchestrator
    assert len(orchestrator.get_active_orders()) == 0

    # Verify message was acknowledged on Cloud Relay
    assert "cmd-e2e-poll-once" in acked_ids


@pytest.mark.asyncio
async def test_poll_once_drops_expired_command_and_still_acks_relay(orchestrator, shared_key):
    """
    Verifies that if an expired command (>60s old) arrives from Cloud Relay,
    the processor drops it (no trading action executed), but still ACKs it so it
    does not poison the queue.
    """
    # Seed an open order that must NOT be closed
    orchestrator.record_order_intent(
        signal_id=999,
        ticket=8001,
        magic_number=123456,
        terminal_id="mt5_1",
        symbol="EURUSD",
        side=int(OrderSide.BUY),
        volume=1.0,
        open_price=1.0850,
    )

    # Command generated 90 seconds ago (> 60s limit)
    cmd_expired = {
        "command_id": "cmd-expired-relay",
        "action": "CLOSE_ALL",
        "timestamp": time.time() - 90.0,
    }
    encrypted_env = RemoteCommandCrypto.encrypt_command(
        command_payload=cmd_expired,
        shared_key=shared_key,
        license_id="LIC-EXPIRED-TEST",
    )

    acked_ids: List[str] = []
    outbox_queue = [encrypted_env.to_dict()]

    def mock_relay_transport(method: str, payload: Any, path: str):
        if method == "GET" and "outbox" in path:
            return {"commands": [outbox_queue.pop(0)]} if outbox_queue else {"commands": []}
        elif method == "POST" and "ack" in path:
            acked_ids.append(payload.get("message_id"))
            return {"success": True}
        return {}

    processor = RemoteCommandProcessor(
        license_id="LIC-EXPIRED-TEST",
        shared_key=shared_key,
        orchestrator=orchestrator,
        http_transport=mock_relay_transport,
    )

    results = await processor.poll_once()
    assert len(results) == 1
    res = results[0]

    assert res.status == "DROPPED_EXPIRED"
    assert res.success is False
    assert "older than 60.0s" in res.error

    # Position must NOT have been closed!
    assert len(orchestrator.get_active_orders()) == 1

    # But relay ACK was sent to prune expired message from queue
    assert "cmd-expired-relay" in acked_ids
