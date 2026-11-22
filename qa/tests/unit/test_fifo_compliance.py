"""
Unit tests for US FIFO Compliance (Sprint 20 Directive: US NFA Rule 2-43(b)).

Verifies:
1. Mode Detection:
   - Reads AccountProfile for fifo_compliant: true flag.
   - Detects FIFO compliance at AccountProfile, TerminalExecutionConfig, and Orchestrator levels.
2. Hedge Blocking:
   - When FIFO is True, receiving a SELL signal for EURUSD while a BUY is open intercepts
     the signal and returns SKIP_FIFO_HEDGE code.
   - Receiving a BUY signal while SELL is open returns SKIP_FIFO_HEDGE code.
   - Accumulating same-direction positions (BUY + BUY) is allowed.
   - Different symbols (EURUSD BUY + GBPUSD SELL) are allowed.
   - When FIFO is False, opposite hedging orders are permitted.
3. Closure Routing:
   - When attempting to close a portion of an accumulated position, queries local DB,
     sorts open positions by timestamp, and executes strictly against the oldest ticket ID.
   - Re-targets close intents to the oldest ticket even if the caller passed a newer ticket.
   - Supports multi-ticket laddered liquidation via execute_fifo_closure.
   - Properly updates remaining volumes and status in SQLite after partial closes.
"""

import asyncio
import struct
import time
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFeedback,
    ExecutionFlags,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    InitHelloPayload,
    AccountState,
    pack_pipe_frame,
    NamedPipeOpcode,
    HEADER_SIZE,
    TOTAL_SLOTS,
    SLOT_SIZE,
    deserialize_order_intent,
)
from engine.ipc_dispatcher import IPCDispatcher
from engine.risk_engine import (
    AccountProfile,
    MarketQuote,
    RiskEngine,
    RiskModel,
    SkipReason,
    SKIP_FIFO_HEDGE,
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    ExecutionResult,
    TerminalExecutionConfig,
)
from engine.notification_engine import explain_skip_reason, SKIP_REASON_EXPLANATIONS


@pytest.fixture
def mock_dispatcher():
    """Provides an IPCDispatcher instance using mock shared-memory buffers."""
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    return dispatcher


async def setup_active_terminal(
    dispatcher: IPCDispatcher,
    terminal_id: str,
    platform_type: int = 1,
    account_login: int = 888123,
):
    """Helper to register and advance terminal state to fully CONNECTED."""
    ctx = dispatcher.register_terminal(terminal_id)
    hello = InitHelloPayload(
        terminal_id=terminal_id,
        platform_type=platform_type,
        account_login=account_login,
        broker_server="OANDA-US-Live",
    )
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
    state = AccountState(
        account_login=account_login,
        balance=25000.0,
        equity=25000.0,
        margin=0.0,
        free_margin=25000.0,
        is_connected_to_broker=True,
    )
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_ACCOUNT_STATE, state.pack()))
    assert dispatcher.can_route_to_terminal(terminal_id) is True
    return ctx


def drain_next_intent(ctx) -> OrderIntent:
    """Helper reading the next pending OrderIntent from SHM buffer and advancing read tail."""
    buf = ctx.shm_writer._get_buffer()
    read_tail = struct.unpack_from("<Q", buf, 128)[0]
    slot_offset = HEADER_SIZE + ((read_tail % TOTAL_SLOTS) * SLOT_SIZE)
    intent = deserialize_order_intent(buf, slot_offset)
    struct.pack_into("<Q", buf, 128, read_tail + 1)
    return intent


# =============================================================================
# Task 1: Mode Detection Tests
# =============================================================================

class TestFifoModeDetection:
    """Tests detection of fifo_compliant flag across AccountProfile and Orchestrator."""

    def test_01_account_profile_default_and_explicit_flag(self):
        """AccountProfile defaults fifo_compliant to False, can be explicitly enabled."""
        default_prof = AccountProfile()
        assert default_prof.fifo_compliant is False

        fifo_prof = AccountProfile(fifo_compliant=True)
        assert fifo_prof.fifo_compliant is True

    def test_02_terminal_execution_config_fifo_flag(self):
        """TerminalExecutionConfig supports fifo_compliant flag and disables hedging."""
        cfg = TerminalExecutionConfig(
            terminal_id="term_oanda_us",
            fifo_compliant=True,
        )
        assert cfg.fifo_compliant is True
        assert cfg.supports_hedging is False

    def test_03_orchestrator_mode_detection_hierarchies(self, mock_dispatcher):
        """Orchestrator accurately detects FIFO mode from direct param, terminal cfg, or profile."""
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)

        # 1. Default orchestrator: not FIFO
        assert orch.is_fifo_compliant("term_1") is False

        # 2. Passed AccountProfile with fifo_compliant=True
        profile_fifo = AccountProfile(fifo_compliant=True)
        assert orch.is_fifo_compliant("term_1", account_profile=profile_fifo) is True

        # 3. Passed AccountProfile with fifo_compliant=False
        profile_standard = AccountProfile(fifo_compliant=False)
        assert orch.is_fifo_compliant("term_1", account_profile=profile_standard) is False

        # 4. Registered AccountProfile on Orchestrator
        orch.set_account_profile(profile_fifo, terminal_id="term_oanda")
        assert orch.is_fifo_compliant("term_oanda") is True
        assert orch.is_fifo_compliant("term_other") is False

        # 5. Global default AccountProfile on Orchestrator
        orch.set_account_profile(profile_fifo)
        assert orch.is_fifo_compliant("term_other") is True


# =============================================================================
# Task 2: Hedge Blocking Tests
# =============================================================================

class TestFifoHedgeBlocking:
    """Tests interception and rejection of opposing hedge orders under FIFO rules."""

    @pytest.mark.asyncio
    async def test_01_sell_signal_blocked_when_buy_is_open(self, mock_dispatcher):
        """If FIFO is true and EURUSD BUY is open, SELL signal is intercepted with SKIP_FIFO_HEDGE."""
        terminal_id = "term_us_1"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        # Record an open BUY position for EURUSD in SQLite database
        now = time.time()
        orch.record_order_intent(
            signal_id=101,
            ticket=55501,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08500,
            status="OPEN",
        )

        # Attempt to submit an opposing SELL signal on EURUSD
        sell_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08450,
            signal_id=102,
        )

        result = await orch.submit_order(terminal_id, sell_intent, account_profile=fifo_profile)

        assert result.success is False
        assert result.status == ExecutionStatus.REJECTED
        assert result.skip_reason is not None
        assert result.skip_reason.code == SKIP_FIFO_HEDGE
        assert result.skip_reason.code == "SKIP_FIFO_HEDGE"
        assert "FIFO rules" in result.skip_reason.reason
        assert 55501 in result.skip_reason.details.get("opposing_tickets", [])

        # Verify journey event was logged
        events = orch.get_journey_logs(signal_id=102)
        assert len(events) >= 1
        assert events[0]["event_type"] == SKIP_FIFO_HEDGE

        # Verify plain-language explanation in Notification engine
        explanation = explain_skip_reason(result.skip_reason)
        assert "FIFO" in explanation or "Hedging is prohibited" in explanation

        await orch.stop()

    @pytest.mark.asyncio
    async def test_02_buy_signal_blocked_when_sell_is_open(self, mock_dispatcher):
        """If FIFO is true and EURUSD SELL is open, BUY signal is intercepted with SKIP_FIFO_HEDGE."""
        terminal_id = "term_us_2"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        orch.record_order_intent(
            signal_id=201,
            ticket=55502,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.SELL),
            volume=0.30,
            open_price=1.08500,
            status="OPEN",
        )

        buy_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.15,
            price=1.08550,
            signal_id=202,
        )

        result = await orch.submit_order(terminal_id, buy_intent, account_profile=fifo_profile)

        assert result.success is False
        assert result.status == ExecutionStatus.REJECTED
        assert result.skip_reason is not None
        assert result.skip_reason.code == "SKIP_FIFO_HEDGE"

        await orch.stop()

    @pytest.mark.asyncio
    async def test_03_accumulating_same_direction_is_allowed(self, mock_dispatcher):
        """If FIFO is true and EURUSD BUY is open, another BUY signal (accumulation) is allowed."""
        terminal_id = "term_us_3"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        orch.record_order_intent(
            signal_id=301,
            ticket=55503,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.10,
            open_price=1.08500,
            status="OPEN",
        )

        # Another BUY for EURUSD is an accumulation, NOT a hedge
        buy_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08600,
            signal_id=302,
        )

        async def broker_fill():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=55504,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.08600,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        fill_task = asyncio.create_task(broker_fill())
        result = await orch.submit_order(terminal_id, buy_intent, account_profile=fifo_profile)
        await fill_task

        assert result.success is True
        assert result.ticket == 55504
        assert result.skip_reason is None

        await orch.stop()

    @pytest.mark.asyncio
    async def test_04_different_symbol_opposing_direction_is_allowed(self, mock_dispatcher):
        """Holding EURUSD BUY while opening GBPUSD SELL is allowed (different symbols)."""
        terminal_id = "term_us_4"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        orch.record_order_intent(
            signal_id=401,
            ticket=55505,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08500,
            status="OPEN",
        )

        gbp_intent = OrderIntent(
            symbol="GBPUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.27000,
            signal_id=402,
        )

        async def broker_fill():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=55506,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.27000,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        fill_task = asyncio.create_task(broker_fill())
        result = await orch.submit_order(terminal_id, gbp_intent, account_profile=fifo_profile)
        await fill_task

        assert result.success is True
        assert result.ticket == 55506

        await orch.stop()

    @pytest.mark.asyncio
    async def test_05_hedging_allowed_when_fifo_is_false(self, mock_dispatcher):
        """When fifo_compliant is False, opposing hedge orders are dispatched normally."""
        terminal_id = "term_eu_hedging"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        std_profile = AccountProfile(fifo_compliant=False)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=std_profile)
        await orch.start()

        orch.record_order_intent(
            signal_id=501,
            ticket=55507,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08500,
            status="OPEN",
        )

        sell_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08450,
            signal_id=502,
        )

        async def broker_fill():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=55508,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.08450,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        fill_task = asyncio.create_task(broker_fill())
        result = await orch.submit_order(terminal_id, sell_intent, account_profile=std_profile)
        await fill_task

        assert result.success is True
        assert result.ticket == 55508
        assert result.skip_reason is None

        await orch.stop()


# =============================================================================
# Task 3: Closure Routing Tests
# =============================================================================

class TestFifoClosureRouting:
    """Tests sorting open positions by timestamp and routing closes strictly against oldest ticket."""

    @pytest.mark.asyncio
    async def test_01_closure_strictly_routed_to_oldest_ticket_in_sqlite(self, mock_dispatcher):
        """
        When closing a portion of an accumulated position, query local SQLite DB,
        sort by timestamp, and execute strictly against the oldest ticket ID.
        """
        terminal_id = "term_us_fifo_close"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        # Setup accumulated EURUSD BUY positions with distinct timestamps
        # Ticket 1001: oldest (t=100)
        # Ticket 1002: middle (t=200)
        # Ticket 1003: newest (t=300)
        orch.record_order_intent(
            signal_id=1001,
            ticket=70001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.50,
            open_price=1.08100,
            status="OPEN",
        )
        # Force updated created_at timestamps
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 100.0 WHERE ticket = 70001")

        orch.record_order_intent(
            signal_id=1002,
            ticket=70002,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.30,
            open_price=1.08200,
            status="OPEN",
        )
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 200.0 WHERE ticket = 70002")

        orch.record_order_intent(
            signal_id=1003,
            ticket=70003,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08300,
            status="OPEN",
        )
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 300.0 WHERE ticket = 70003")

        # Verify candidate positions helper returns them in strict timestamp order
        candidates = orch.get_open_positions_for_symbol("EURUSD", terminal_id=terminal_id)
        assert len(candidates) == 3
        assert candidates[0]["ticket"] == 70001
        assert candidates[1]["ticket"] == 70002
        assert candidates[2]["ticket"] == 70003

        # Oldest position helper
        oldest = orch.get_oldest_open_position("EURUSD", terminal_id=terminal_id)
        assert oldest is not None
        assert oldest["ticket"] == 70001

        # Now submit a close order attempting to close ticket #70003 (newest) or ticket 0
        close_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.PARTIAL_CLOSE,
            volume=0.20,
            order_ticket=70003, # Attempt to close newest
            signal_id=1004,
        )

        captured_commands = []

        async def broker_fill_close():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            captured_commands.append(cmd)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=cmd.order_ticket,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=cmd.volume,
                executed_price=1.08500,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        fill_task = asyncio.create_task(broker_fill_close())
        result = await orch.submit_order(terminal_id, close_intent, account_profile=fifo_profile)
        await fill_task

        assert result.success is True
        # Verify the close was executed strictly against ticket #70001 (oldest ticket), NOT #70003!
        assert len(captured_commands) == 1
        assert captured_commands[0].order_ticket == 70001
        assert pytest.approx(captured_commands[0].volume, 0.01) == 0.20

        # Verify SQLite remaining volume for #70001 was updated (0.50 - 0.20 = 0.30)
        order_70001 = orch.get_order_by_ticket(70001)
        assert order_70001 is not None
        assert pytest.approx(order_70001["volume"], 0.01) == 0.30
        assert order_70001["status"] in ("OPEN", "PARTIAL")

        await orch.stop()

    @pytest.mark.asyncio
    async def test_02_sequential_liquidation_advances_to_next_oldest_ticket(self, mock_dispatcher):
        """When an oldest ticket is fully closed, the next closure targets the next oldest ticket."""
        terminal_id = "term_us_seq_close"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        # Ticket 80001: 0.10 lot, t=100
        # Ticket 80002: 0.20 lot, t=200
        orch.record_order_intent(
            signal_id=8001,
            ticket=80001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.10,
            open_price=1.08100,
            status="OPEN",
        )
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 100.0 WHERE ticket = 80001")

        orch.record_order_intent(
            signal_id=8002,
            ticket=80002,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08200,
            status="OPEN",
        )
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 200.0 WHERE ticket = 80002")

        # 1. Close 0.10 lot (liquidates ticket #80001 completely)
        close_intent_1 = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.FULL_CLOSE,
            volume=0.10,
            signal_id=8003,
        )

        async def broker_fill_1():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            assert cmd.order_ticket == 80001
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=80001,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.08500,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        f_task_1 = asyncio.create_task(broker_fill_1())
        res_1 = await orch.submit_order(terminal_id, close_intent_1, account_profile=fifo_profile)
        await f_task_1
        assert res_1.success is True

        # Verify #80001 is CLOSED in DB
        o1 = orch.get_order_by_ticket(80001)
        assert o1["status"] == "CLOSED"

        # 2. Next close of 0.10 lot MUST now target ticket #80002
        close_intent_2 = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.PARTIAL_CLOSE,
            volume=0.10,
            signal_id=8004,
        )

        async def broker_fill_2():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            assert cmd.order_ticket == 80002
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=80002,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.08500,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        f_task_2 = asyncio.create_task(broker_fill_2())
        res_2 = await orch.submit_order(terminal_id, close_intent_2, account_profile=fifo_profile)
        await f_task_2
        assert res_2.success is True

        o2 = orch.get_order_by_ticket(80002)
        assert pytest.approx(o2["volume"], 0.01) == 0.10
        assert o2["status"] in ("OPEN", "PARTIAL")

        await orch.stop()

    @pytest.mark.asyncio
    async def test_03_execute_fifo_closure_utility_multi_ticket(self, mock_dispatcher):
        """execute_fifo_closure liquidates spanning volume sequentially across oldest tickets."""
        terminal_id = "term_us_multi_close"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        # Ticket 90001: 0.10 lot, t=10
        # Ticket 90002: 0.15 lot, t=20
        # Ticket 90003: 0.20 lot, t=30
        orch.record_order_intent(
            signal_id=9001,
            ticket=90001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.10,
            open_price=1.08100,
            status="OPEN",
        )
        orch.record_order_intent(
            signal_id=9002,
            ticket=90002,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.15,
            open_price=1.08200,
            status="OPEN",
        )
        orch.record_order_intent(
            signal_id=9003,
            ticket=90003,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.20,
            open_price=1.08300,
            status="OPEN",
        )
        with orch._db_conn:
            orch._db_conn.execute("UPDATE order_intents SET created_at = 10.0 WHERE ticket = 90001")
            orch._db_conn.execute("UPDATE order_intents SET created_at = 20.0 WHERE ticket = 90002")
            orch._db_conn.execute("UPDATE order_intents SET created_at = 30.0 WHERE ticket = 90003")

        # Close 0.20 lots across EURUSD. This must close 90001 (0.10) completely and 90002 (0.10) partially!
        captured_tickets = []

        async def broker_fill_both():
            for _ in range(2):
                while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                    await asyncio.sleep(0.005)
                cmd = drain_next_intent(ctx)
                captured_tickets.append((cmd.order_ticket, cmd.volume))
                fb = ExecutionFeedback(
                    signal_id=cmd.signal_id,
                    order_ticket=cmd.order_ticket,
                    execution_status=ExecutionStatus.FILLED,
                    executed_volume=cmd.volume,
                    executed_price=1.08500,
                )
                ctx.pipe_server.simulate_inbound_data(
                    pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
                )

        f_task = asyncio.create_task(broker_fill_both())
        results = await orch.execute_fifo_closure(
            terminal_id=terminal_id,
            symbol="EURUSD",
            volume=0.20,
            account_profile=fifo_profile,
        )
        await f_task

        assert len(results) == 2
        assert captured_tickets[0] == (90001, 0.10) # 1st: ticket 90001 closed 0.10
        assert captured_tickets[1] == (90002, 0.10) # 2nd: ticket 90002 closed 0.10

        o1 = orch.get_order_by_ticket(90001)
        o2 = orch.get_order_by_ticket(90002)
        o3 = orch.get_order_by_ticket(90003)

        assert o1["status"] == "CLOSED"
        assert pytest.approx(o2["volume"], 0.01) == 0.05
        assert o2["status"] in ("OPEN", "PARTIAL")
        assert pytest.approx(o3["volume"], 0.01) == 0.20
        assert o3["status"] == "OPEN"

        await orch.stop()

    @pytest.mark.asyncio
    async def test_04_symbol_suffix_normalized_in_hedge_and_close(self, mock_dispatcher):
        """Hedge blocking and closure routing handle broker suffix/prefix normalizations (e.g. EURUSD.pro)."""
        terminal_id = "term_us_suffix"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(fifo_compliant=True)
        orch = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher, account_profile=fifo_profile)
        await orch.start()

        # Open EURUSD.pro BUY
        orch.record_order_intent(
            signal_id=9501,
            ticket=95001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD.pro",
            side=int(OrderSide.BUY),
            volume=0.25,
            open_price=1.08200,
            status="OPEN",
        )

        # 1. Hedge check: incoming raw 'EURUSD' SELL is blocked by open 'EURUSD.pro' BUY
        hedge_sell = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08150,
            signal_id=9502,
        )
        res_hedge = await orch.submit_order(terminal_id, hedge_sell, account_profile=fifo_profile)
        assert res_hedge.success is False
        assert res_hedge.skip_reason.code == SKIP_FIFO_HEDGE

        # 2. Closure routing: incoming raw 'EURUSD' close routes to oldest 'EURUSD.pro' ticket #95001
        close_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.PARTIAL_CLOSE,
            volume=0.10,
            order_ticket=0,
            signal_id=9503,
        )

        async def broker_fill_close():
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            assert cmd.order_ticket == 95001
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=95001,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=0.10,
                executed_price=1.08500,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

        f_task = asyncio.create_task(broker_fill_close())
        res_close = await orch.submit_order(terminal_id, close_intent, account_profile=fifo_profile)
        await f_task

        assert res_close.success is True
        o = orch.get_order_by_ticket(95001)
        assert pytest.approx(o["volume"], 0.01) == 0.15

        await orch.stop()

    @pytest.mark.asyncio
    async def test_05_orchestrate_signal_end_to_end_fifo_enforcement(self, mock_dispatcher):
        """orchestrate_signal passes AccountProfile down and intercepts FIFO hedges."""
        terminal_id = "term_us_orch"
        ctx = await setup_active_terminal(mock_dispatcher, terminal_id)

        fifo_profile = AccountProfile(
            fifo_compliant=True,
            risk_model=RiskModel.FIXED_LOT,
            fixed_lot=0.10,
        )
        risk_engine = RiskEngine()
        orch = ExecutionOrchestrator(
            ipc_dispatcher=mock_dispatcher,
            risk_engine=risk_engine,
            account_profile=fifo_profile,
        )
        await orch.start()

        # Pre-seed open BUY on EURUSD
        orch.record_order_intent(
            signal_id=9801,
            ticket=98001,
            magic_number=123456,
            terminal_id=terminal_id,
            symbol="EURUSD",
            side=int(OrderSide.BUY),
            volume=0.10,
            open_price=1.08500,
            status="OPEN",
        )

        quote = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08510, timestamp=time.time())

        # Attempt to orchestrate an opposing SELL signal
        intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.08500,
            stop_loss=1.09000,
            take_profit=1.07500,
            signal_id=9802,
            timestamp_ns=time.time_ns(),
        )

        res = await orch.orchestrate_signal(
            terminal_id=terminal_id,
            intent=intent,
            account_profile=fifo_profile,
            quote=quote,
        )

        assert res.success is False
        assert res.status == ExecutionStatus.REJECTED
        assert res.skip_reason is not None
        assert res.skip_reason.code == SKIP_FIFO_HEDGE

        await orch.stop()

