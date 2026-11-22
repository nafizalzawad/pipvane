"""
Pipvane v3: Pytest Suite for Telegram Safety Mode Soak & Chaos Testing
-----------------------------------------------------------------------
Validates 3-account concurrent soak testing, FR-4.1 Read-Only Safety Mode enforcement,
randomized network fault injection, auto-reconnect backfill recovery, telemetry logging,
and strict assertions failing immediately upon USER_DEACTIVATED or AUTH_KEY_UNREGISTERED.
"""

import os
import csv
import pytest
import asyncio

from engine.vault import VaultStorage
from engine.tdlib_client import MockTDLibClient
from engine.safety_mode import SafetyModeViolationError
from engine.telegram_gateway import TelegramGateway
from qa.chaos.fault_injector import NetworkChaosFaultInjector
from qa.chaos.telemetry import BanRiskTelemetryLogger
from qa.chaos.soak_orchestrator import TelegramSafetySoakOrchestrator


@pytest.fixture
def temp_output_dir(tmp_path):
    output_dir = tmp_path / "soak_test_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_orchestrator_initializes_3_accounts_with_safety_mode(temp_output_dir):
    """
    Verify Task 1:
    Orchestrator configures 3 distinct accounts, each with FR-4.1 Read-Only Safety Mode.
    """
    orchestrator = TelegramSafetySoakOrchestrator(
        output_dir=temp_output_dir,
        accelerated=True
    )
    assert len(orchestrator.gateways) == 3

    account_ids = [gw.profile_id for gw in orchestrator.gateways]
    assert len(set(account_ids)) == 3, "Account IDs must be distinct!"

    for gw in orchestrator.gateways:
        assert gw.safety_mode is True, f"Account {gw.profile_id} must have Safety Mode enabled (FR-4.1)!"
        assert gw.connection_state == "connectionStateConnecting"


@pytest.mark.chaos
def test_strict_safety_mode_blocks_outgoing_messages(temp_output_dir):
    """
    Verify FR-4.1:
    Under Safety Mode, any attempt to write or send messages MUST raise SafetyModeViolationError
    to prevent aggressive calls or Telegram spam bans.
    """
    vault = VaultStorage(db_path=":memory:")
    mock_client = MockTDLibClient()
    gw = TelegramGateway(
        vault=vault,
        tdlib_client=mock_client,
        profile_id="safety_test_gw",
        phone_number="+15550199001",
    )

    with pytest.raises(SafetyModeViolationError) as exc_info:
        gw.send_message(chat_id=12345, text="Unauthorized Outgoing Signal")

    assert "Safety Mode strictly prohibits" in str(exc_info.value)


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_network_fault_injection_and_auto_reconnect_backfill(temp_output_dir):
    """
    Verify Task 2:
    Disrupts active socket (dropping connection) and verifies auto-reconnection
    and message backfill logic executed seamlessly without crashing.
    """
    vault = VaultStorage(db_path=":memory:")
    mock_client = MockTDLibClient(initial_auth_state="authorizationStateWaitTdlibParameters")
    gw = TelegramGateway(
        vault=vault,
        tdlib_client=mock_client,
        profile_id="fault_test_gw",
        phone_number="+15550199002",
    )
    gw.start()
    await asyncio.sleep(0.05)
    gw.login_with_phone("+15550199002")
    await asyncio.sleep(0.05)
    gw.submit_auth_code("12345")
    await asyncio.sleep(0.05)
    assert gw.wait_until_ready(timeout=1.0) is True

    # Prepare chat history for backfill test
    chat_id = 777
    time_now = 1800000000
    mock_client.set_mock_chat_history(chat_id, [
        {
            "id": 201,
            "chat_id": chat_id,
            "date": time_now,
            "content": {"@type": "messageText", "text": {"text": "Backfill msg 1"}},
        },
        {
            "id": 202,
            "chat_id": chat_id,
            "date": time_now + 1,
            "content": {"@type": "messageText", "text": {"text": "Backfill msg 2"}},
        },
    ])

    received_messages = []
    gw.add_message_handler(lambda m: received_messages.append(m))
    gw.subscribe_chat(chat_id)

    # Inject fault: drop socket
    injector = NetworkChaosFaultInjector(accelerated_mode=True)
    await injector.inject_immediate_socket_drop(gw, reason="CHAOS_TEST_DROP")

    assert gw.network_disruptions_count >= 1
    assert injector.disruption_count == 1

    # Allow recovery and backfill response processing
    await asyncio.sleep(0.15)
    assert gw.connection_state == "connectionStateReady"
    assert gw.messages_backfilled_count >= 1

    gw.stop()


@pytest.mark.chaos
def test_telemetry_csv_generation_and_metrics(temp_output_dir):
    """
    Verify Task 3:
    Validates daily ban_risk_telemetry.csv output: session uptime, flood-wait errors,
    termination codes, and TDLib process memory usage (RSS/VMS).
    """
    logger = BanRiskTelemetryLogger(output_dir=temp_output_dir)
    assert os.path.exists(logger.file_path)

    vault = VaultStorage(db_path=":memory:")
    mock_client = MockTDLibClient()
    gw = TelegramGateway(
        vault=vault,
        tdlib_client=mock_client,
        profile_id="acc_telemetry_test",
        phone_number="+15550199003",
    )
    gw.start()

    # Record snapshot
    rows = logger.record_snapshot([gw])
    assert len(rows) == 1
    row = rows[0]

    assert row["account_id"] == "acc_telemetry_test"
    assert row["tdlib_rss_mb"] > 0.0, "RSS memory must be tracked!"
    assert row["tdlib_vms_mb"] > 0.0, "VMS memory must be tracked!"
    assert row["ban_risk_level"] == "LOW_SAFE"
    assert row["termination_code"] == "RUNNING"

    # Verify CSV file contents
    with open(logger.file_path, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) >= 1
        csv_row = reader[-1]
        assert csv_row["account_id"] == "acc_telemetry_test"
        assert float(csv_row["tdlib_rss_mb"]) > 0.0

    gw.stop()


@pytest.mark.chaos
def test_strict_assertion_fails_immediately_on_user_deactivated(temp_output_dir):
    """
    Verify Task 4 (Deliverable requirement):
    Test MUST fail immediately with an AssertionError if USER_DEACTIVATED is thrown.
    """
    orchestrator = TelegramSafetySoakOrchestrator(output_dir=temp_output_dir, accelerated=True)
    target_gw = orchestrator.gateways[0]
    target_gw.start()

    with pytest.raises(AssertionError) as exc_info:
        target_gw.inject_ban_exception("USER_DEACTIVATED")

    assert "USER_DEACTIVATED" in str(exc_info.value)
    assert target_gw.termination_code == "USER_DEACTIVATED"
    target_gw.stop()


@pytest.mark.chaos
def test_strict_assertion_fails_immediately_on_auth_key_unregistered(temp_output_dir):
    """
    Verify Task 4 (Deliverable requirement):
    Test MUST fail immediately with an AssertionError if AUTH_KEY_UNREGISTERED is thrown.
    """
    orchestrator = TelegramSafetySoakOrchestrator(output_dir=temp_output_dir, accelerated=True)
    target_gw = orchestrator.gateways[1]
    target_gw.start()

    with pytest.raises(AssertionError) as exc_info:
        target_gw.inject_ban_exception("AUTH_KEY_UNREGISTERED")

    assert "AUTH_KEY_UNREGISTERED" in str(exc_info.value)
    assert target_gw.termination_code == "AUTH_KEY_UNREGISTERED"
    target_gw.stop()


@pytest.mark.chaos
@pytest.mark.asyncio
async def test_continuous_soak_run_accelerated(temp_output_dir):
    """
    End-to-end integration test:
    Executes an accelerated multi-account soak run verifying background message listening,
    randomized fault injection, backfilling, and clean telemetry termination without crashing.
    """
    orchestrator = TelegramSafetySoakOrchestrator(
        output_dir=temp_output_dir,
        accelerated=True,
    )

    # Run for 2.0 seconds in accelerated mode (exercises multiple fault & traffic cycles)
    await orchestrator.run_duration(seconds=2.0)

    # Assert all accounts ran without bans
    for gw in orchestrator.gateways:
        assert gw.termination_code in ["DURATION_COMPLETED", "RUNNING", "NORMAL_SHUTDOWN"]
        assert gw.messages_received_count > 0, f"Account {gw.profile_id} must have received messages!"

    # Verify telemetry CSV logged snapshots
    csv_file = os.path.join(temp_output_dir, "ban_risk_telemetry.csv")
    assert os.path.exists(csv_file)
    with open(csv_file, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) >= 3, "Telemetry must contain snapshots for all 3 accounts"
