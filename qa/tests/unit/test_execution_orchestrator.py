"""
Unit tests for ExecutionOrchestrator (Sprint 12 Directive).

Validates:
1. Intent Translation: formatting OrderIntent for target terminals (MT4 vs MT5).
2. Per-Account Serialization Queue: sequential execution for concurrent bursts targeting
   the same terminal, and parallel execution across distinct terminals.
3. Retry & Backoff Mechanism: 3 retries with exponential backoff on requotes/busy broker,
   immediate rejection on non-retryable errors, and SkipReason generation.
4. TP Ladder decomposition and Partial Close handling.
5. RiskEngine integration and admission guard filtering.
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
    TerminalStatus,
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
)
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    ExecutionResult,
    TerminalExecutionConfig,
    BreakEvenConfig,
    VirtualTPLevel,
    ManagedPosition,
)


@pytest.fixture
def mock_dispatcher():
    """Provides an IPCDispatcher instance using mock shared-memory buffers."""
    dispatcher = IPCDispatcher(heartbeat_timeout_s=5.0, use_mock_buffers=True)
    return dispatcher


async def setup_active_terminal(
    dispatcher: IPCDispatcher,
    terminal_id: str,
    platform_type: int = 1, # 0: MT4, 1: MT5
    account_login: int = 123456,
):
    """Helper to register and advance terminal state to fully CONNECTED."""
    ctx = dispatcher.register_terminal(terminal_id)
    hello = InitHelloPayload(
        terminal_id=terminal_id,
        platform_type=platform_type,
        account_login=account_login,
        broker_server="ICMarkets-Demo",
    )
    ctx.pipe_server.simulate_inbound_data(pack_pipe_frame(NamedPipeOpcode.MSG_INIT_HELLO, hello.pack()))
    state = AccountState(
        account_login=account_login,
        balance=10000.0,
        equity=10000.0,
        margin=0.0,
        free_margin=10000.0,
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
# 1. Intent Translation Tests (MT4 vs MT5)
# =============================================================================

def test_intent_translation_mt5(mock_dispatcher: IPCDispatcher):
    """MT5 intent translation applies filling mode flags and symbol suffixes."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    cfg = TerminalExecutionConfig(
        terminal_id="term_mt5",
        platform_type=1, # MT5
        symbol_suffix=".raw",
        symbol_map={"GOLD": "XAUUSD"},
        default_filling_mode=int(ExecutionFlags.IOC),
        min_lot=0.01,
        max_lot=50.0,
        lot_step=0.01,
    )
    orchestrator.register_terminal_config(cfg)

    raw_intent = OrderIntent(
        symbol="GOLD",
        side=OrderSide.BUY,
        volume=0.354, # Quantizes to 0.35
        price=2650.00,
        stop_loss=2640.00,
        take_profit=2670.00,
        flags=0,
    )

    translated = orchestrator.translate_intent(raw_intent, cfg)

    assert translated.symbol == "XAUUSD.raw"
    assert translated.volume == 0.35
    assert (translated.flags & ExecutionFlags.IOC) != 0
    assert translated.stop_loss == 2640.00
    assert translated.take_profit == 2670.00


def test_intent_translation_mt4_ecn_two_step(mock_dispatcher: IPCDispatcher):
    """MT4 ECN mode masks IOC/FOK flags and zeros initial SL/TP for two-step execution."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    cfg = TerminalExecutionConfig(
        terminal_id="term_mt4",
        platform_type=0, # MT4
        symbol_suffix="_i",
        is_ecn=True, # Two-step execution
        min_lot=0.01,
        max_lot=100.0,
        lot_step=0.01,
    )
    orchestrator.register_terminal_config(cfg)

    raw_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        take_profit=1.09500,
        flags=ExecutionFlags.IOC | ExecutionFlags.FOK, # Unsupported on MT4
    )

    translated = orchestrator.translate_intent(raw_intent, cfg)

    assert translated.symbol == "EURUSD_i"
    # IOC and FOK flags must be masked out for MT4
    assert (translated.flags & ExecutionFlags.IOC) == 0
    assert (translated.flags & ExecutionFlags.FOK) == 0
    # In MT4 ECN two-step, initial market order must have SL=0 and TP=0
    assert translated.stop_loss == 0.0
    assert translated.take_profit == 0.0


# =============================================================================
# 2. Per-Account Serialization Queue Tests
# =============================================================================

@pytest.mark.asyncio
async def test_per_account_sequential_queue(mock_dispatcher: IPCDispatcher):
    """
    If a channel fires 3 signals in one second to the SAME account,
    they must be processed strictly sequentially without race conditions.
    """
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_seq_1", platform_type=1)

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        default_execution_timeout_s=1.0,
    )
    await orchestrator.start()

    execution_order = []

    # Background task simulating broker responding to dispatches
    async def mock_broker_responder():
        for i in range(3):
            # Wait for order to appear in SHM ring buffer
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)

            # Record that an order was picked up by broker
            execution_order.append(f"start_{i}")
            # Simulate broker execution processing delay (20ms)
            await asyncio.sleep(0.02)

            # Drain order from SHM and send feedback
            intent = drain_next_intent(ctx)
            feedback = ExecutionFeedback(
                signal_id=intent.signal_id,
                order_ticket=1000 + i,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=intent.volume,
                executed_price=2650.0 + i,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, feedback.pack())
            )
            execution_order.append(f"finish_{i}")

    responder_task = asyncio.create_task(mock_broker_responder())

    # Fire 3 concurrent signals simultaneously to the SAME account
    intents = [
        OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, volume=0.1, signal_id=101),
        OrderIntent(symbol="EURUSD", side=OrderSide.SELL, volume=0.2, signal_id=102),
        OrderIntent(symbol="GBPUSD", side=OrderSide.BUY, volume=0.3, signal_id=103),
    ]

    results = await asyncio.gather(
        orchestrator.submit_order("term_seq_1", intents[0]),
        orchestrator.submit_order("term_seq_1", intents[1]),
        orchestrator.submit_order("term_seq_1", intents[2]),
    )

    await responder_task

    # All 3 orders must succeed
    assert all(r.success for r in results)
    assert [r.ticket for r in results] == [1000, 1001, 1002]

    # Verify strict sequential execution: order 0 finishes before order 1 starts, etc.
    assert execution_order == [
        "start_0", "finish_0",
        "start_1", "finish_1",
        "start_2", "finish_2",
    ]

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_distinct_accounts_execute_in_parallel(mock_dispatcher: IPCDispatcher):
    """Orders targeting DIFFERENT accounts execute concurrently in parallel."""
    await mock_dispatcher.start()
    ctx_a = await setup_active_terminal(mock_dispatcher, "term_a", platform_type=1, account_login=111)
    ctx_b = await setup_active_terminal(mock_dispatcher, "term_b", platform_type=1, account_login=222)

    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    await orchestrator.start()

    # Respond to both terminals simultaneously
    async def handle_terminal(ctx, term_id, ticket):
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        intent = drain_next_intent(ctx)
        await asyncio.sleep(0.03) # Simulate latency
        feedback = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=ticket,
            execution_status=ExecutionStatus.FILLED,
        )
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, feedback.pack())
        )

    t1 = asyncio.create_task(handle_terminal(ctx_a, "term_a", 5001))
    t2 = asyncio.create_task(handle_terminal(ctx_b, "term_b", 5002))

    t_start = time.perf_counter()
    r_a, r_b = await asyncio.gather(
        orchestrator.submit_order("term_a", OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, signal_id=201)),
        orchestrator.submit_order("term_b", OrderIntent(symbol="EURUSD", side=OrderSide.SELL, signal_id=202)),
    )
    duration = time.perf_counter() - t_start

    await t1
    await t2

    assert r_a.success and r_b.success
    assert r_a.ticket == 5001
    assert r_b.ticket == 5002
    assert duration < 0.15

    await orchestrator.stop()
    await mock_dispatcher.stop()


# =============================================================================
# 3. Retry & Backoff Mechanism Tests
# =============================================================================

@pytest.mark.asyncio
async def test_retry_on_requote_and_eventual_fill(mock_dispatcher: IPCDispatcher):
    """
    If broker returns REQUOTE, orchestrator retries with exponential backoff
    and succeeds when broker fills on subsequent attempt.
    """
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_retry_fill")

    # Use small base_backoff_s (10ms) for fast unit test
    cfg = TerminalExecutionConfig(
        terminal_id="term_retry_fill",
        max_retries=3,
        base_backoff_s=0.01,
        execution_timeout_s=1.0,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    attempts_seen = 0

    async def mock_broker_requote_then_fill():
        nonlocal attempts_seen
        for i in range(2): # 1 requote, then 1 fill
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            attempts_seen += 1
            intent = drain_next_intent(ctx)

            if i == 0:
                # First attempt: Broker REQUOTE
                fb = ExecutionFeedback(
                    signal_id=intent.signal_id,
                    order_ticket=0,
                    execution_status=ExecutionStatus.REQUOTE,
                    broker_retcode=10004, # TRADE_RETCODE_REQUOTE
                    error_description="Requote: Market price changed",
                )
            else:
                # Second attempt: Broker FILLED
                fb = ExecutionFeedback(
                    signal_id=intent.signal_id,
                    order_ticket=888123,
                    execution_status=ExecutionStatus.FILLED,
                    executed_price=2651.50,
                )

            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    broker_task = asyncio.create_task(mock_broker_requote_then_fill())

    intent = OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, volume=0.5, signal_id=301)
    result = await orchestrator.submit_order("term_retry_fill", intent)
    await broker_task

    assert result.success is True
    assert result.status == ExecutionStatus.FILLED
    assert result.ticket == 888123
    assert result.attempts == 2
    assert attempts_seen == 2

    # Verify retry event history recorded
    retries = orchestrator.get_retry_history()
    assert len(retries) == 1
    assert retries[0]["attempt"] == 1
    assert retries[0]["retcode"] == 10004

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_retry_exhausted_logs_skip_reason(mock_dispatcher: IPCDispatcher):
    """
    If broker continuously rejects with 'busy server' or requotes beyond max_retries (3),
    orchestrator fails with SKIP_RETRY_EXHAUSTED and logs explicit SkipReason.
    """
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_retry_fail")

    cfg = TerminalExecutionConfig(
        terminal_id="term_retry_fail",
        max_retries=3,
        base_backoff_s=0.01, # Fast backoff for test
        execution_timeout_s=0.5,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    total_attempts = 0

    async def mock_broker_always_busy():
        nonlocal total_attempts
        # 1 initial + 3 retries = 4 total attempts
        for _ in range(4):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            total_attempts += 1
            intent = drain_next_intent(ctx)

            fb = ExecutionFeedback(
                signal_id=intent.signal_id,
                order_ticket=0,
                execution_status=ExecutionStatus.REJECTED,
                broker_retcode=137, # MT4 ERR_BROKER_BUSY
                error_description="Broker busy: Trade context currently occupied",
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    broker_task = asyncio.create_task(mock_broker_always_busy())

    intent = OrderIntent(symbol="EURUSD", side=OrderSide.SELL, volume=1.0, signal_id=401)
    result = await orchestrator.submit_order("term_retry_fail", intent)
    await broker_task

    assert result.success is False
    assert result.attempts == 4 # 1 initial + 3 retries
    assert total_attempts == 4
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_RETRY_EXHAUSTED"
    assert "retcode=137" in result.skip_reason.reason

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_permanent_rejection_no_retry(mock_dispatcher: IPCDispatcher):
    """Permanent broker error (e.g. ERR_NOT_ENOUGH_MONEY 134) rejects immediately with NO retries."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_perm_fail")

    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    await orchestrator.start()

    attempts = 0

    async def mock_broker_no_money():
        nonlocal attempts
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        attempts += 1
        intent = drain_next_intent(ctx)

        fb = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=0,
            execution_status=ExecutionStatus.REJECTED,
            broker_retcode=134, # MT4 ERR_NOT_ENOUGH_MONEY (non-retryable)
            error_description="Not enough money to cover margin",
        )
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
        )

    b_task = asyncio.create_task(mock_broker_no_money())

    intent = OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, volume=50.0, signal_id=501)
    result = await orchestrator.submit_order("term_perm_fail", intent)
    await b_task

    assert result.success is False
    assert result.attempts == 1 # Did NOT perform retries
    assert attempts == 1
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_BROKER_REJECTED"

    await orchestrator.stop()
    await mock_dispatcher.stop()


# =============================================================================
# 4. TP Ladder Decomposition Tests
# =============================================================================

def test_decompose_tp_ladder(mock_dispatcher: IPCDispatcher):
    """Decomposing an OrderIntent with 3 TP targets splits volume and assigns individual TPs."""
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)

    parent_intent = OrderIntent(
        symbol="XAUUSD",
        side=OrderSide.BUY,
        volume=0.60,
        price=2650.00,
        stop_loss=2640.00,
        signal_id=777000,
    )

    tp_targets = [2660.00, 2670.00, 2680.00]
    sub_orders = orchestrator.decompose_tp_ladder(
        intent=parent_intent,
        tp_ladder=tp_targets,
        weights=[0.5, 0.3, 0.2], # 50% TP1, 30% TP2, 20% TP3
    )

    assert len(sub_orders) == 3
    # Check volume splits
    assert pytest.approx(sub_orders[0].volume, 0.001) == 0.30
    assert pytest.approx(sub_orders[1].volume, 0.001) == 0.18
    assert pytest.approx(sub_orders[2].volume, 0.001) == 0.12
    # Check TP targets
    assert sub_orders[0].take_profit == 2660.00
    assert sub_orders[1].take_profit == 2670.00
    assert sub_orders[2].take_profit == 2680.00
    # Stop loss and symbol preserved
    assert all(o.stop_loss == 2640.00 for o in sub_orders)
    assert all(o.symbol == "XAUUSD" for o in sub_orders)


# =============================================================================
# 5. RiskEngine Full Pipeline Integration Tests
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrate_signal_with_risk_engine_filter(mock_dispatcher: IPCDispatcher):
    """If signal violates RiskEngine admission guards, orchestrator returns SkipReason directly."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_risk_eval")

    orchestrator = ExecutionOrchestrator(
        ipc_dispatcher=mock_dispatcher,
        risk_engine=RiskEngine(),
    )
    await orchestrator.start()

    profile = AccountProfile(
        account_id="ACC_TEST",
        balance=10000.0,
        equity=10000.0,
        free_margin=10000.0,
        risk_model=RiskModel.FIXED_LOT,
        fixed_lot=0.1,
        max_spread_pips=2.0, # Stiff spread guard
    )

    # Wide spread quote (10 pips spread)
    bad_quote = MarketQuote(
        symbol="EURUSD",
        bid=1.08000,
        ask=1.08100, # 10 pips spread > 2.0 pips limit
        timestamp=time.time(),
    )

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=0.1,
        timestamp_ns=time.time_ns(),
        signal_id=601,
    )

    result = await orchestrator.orchestrate_signal(
        terminal_id="term_risk_eval",
        intent=intent,
        account_profile=profile,
        quote=bad_quote,
    )

    assert result.success is False
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_SPREAD"
    # Shared memory was never touched
    assert ctx.shm_writer.get_metrics()["queue_depth"] == 0

    await orchestrator.stop()
    await mock_dispatcher.stop()


# =============================================================================
# 6. Additional Resiliency & Edge Case Tests
# =============================================================================

@pytest.mark.asyncio
async def test_terminal_unavailable_returns_skip_reason(mock_dispatcher: IPCDispatcher):
    """When a terminal is disconnected or paused, submission fails safely with SKIP_TERMINAL_UNAVAILABLE."""
    await mock_dispatcher.start()
    # Register terminal but leave it disconnected (no hello or state)
    mock_dispatcher.register_terminal("term_unready")

    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    await orchestrator.start()

    intent = OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, signal_id=801)
    result = await orchestrator.submit_order("term_unready", intent)

    assert result.success is False
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_TERMINAL_UNAVAILABLE"

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_broker_timeout_handling(mock_dispatcher: IPCDispatcher):
    """If broker fails to respond within execution_timeout_s across retries, SKIP_BROKER_TIMEOUT is returned."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_timeout")

    cfg = TerminalExecutionConfig(
        terminal_id="term_timeout",
        max_retries=1, # 1 retry for test speed
        base_backoff_s=0.01,
        execution_timeout_s=0.03, # 30ms timeout
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    # Worker dispatches to SHM, but broker NEVER sends feedback
    async def drain_without_feedback():
        for _ in range(2):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            drain_next_intent(ctx)

    drain_task = asyncio.create_task(drain_without_feedback())

    intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, signal_id=901)
    result = await orchestrator.submit_order("term_timeout", intent)
    await drain_task

    assert result.success is False
    assert result.status == ExecutionStatus.EXPIRED
    assert result.skip_reason is not None
    assert result.skip_reason.code == "SKIP_BROKER_TIMEOUT"

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_mt4_ecn_two_step_dispatches_order_modify(mock_dispatcher: IPCDispatcher):
    """When MT4 ECN fills, an immediate OrderModify intent is dispatched to attach SL/TP."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_ecn_mod", platform_type=0)

    cfg = TerminalExecutionConfig(
        terminal_id="term_ecn_mod",
        platform_type=0, # MT4
        is_ecn=True, # ECN mode
        execution_timeout_s=1.0,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    dispatched_intents = []

    async def mock_broker_ecn():
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        # Drain initial market entry
        entry_intent = drain_next_intent(ctx)
        dispatched_intents.append(entry_intent)

        # Broker fills order
        fb = ExecutionFeedback(
            signal_id=entry_intent.signal_id,
            order_ticket=445566,
            execution_status=ExecutionStatus.FILLED,
            executed_price=1.0850,
        )
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
        )

        # Await second dispatch (OrderModify)
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        modify_intent = drain_next_intent(ctx)
        dispatched_intents.append(modify_intent)

    broker_task = asyncio.create_task(mock_broker_ecn())

    raw_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=0.5,
        price=1.0850,
        stop_loss=1.0800,
        take_profit=1.0950,
        signal_id=1001,
    )

    result = await orchestrator.submit_order("term_ecn_mod", raw_intent)
    await broker_task

    assert result.success is True
    assert result.ticket == 445566
    assert len(dispatched_intents) == 2

    # Intent 0: initial market entry with SL=0, TP=0
    assert dispatched_intents[0].action_type == ActionType.MARKET_ENTRY
    assert dispatched_intents[0].stop_loss == 0.0
    assert dispatched_intents[0].take_profit == 0.0

    # Intent 1: OrderModify with original SL and TP attached
    assert dispatched_intents[1].action_type == ActionType.MODIFY_SL_TP
    assert dispatched_intents[1].order_ticket == 445566
    assert dispatched_intents[1].stop_loss == 1.0800
    assert dispatched_intents[1].take_profit == 1.0950

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_quote_refresh_on_retry(mock_dispatcher: IPCDispatcher):
    """When retrying after a requote, quote_provider supplies fresh execution price."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_quote_refresh")

    latest_bid = 2650.00
    latest_ask = 2650.50

    def quote_provider(sym: str) -> MarketQuote:
        return MarketQuote(symbol=sym, bid=latest_bid, ask=latest_ask, timestamp=time.time())

    cfg = TerminalExecutionConfig(
        terminal_id="term_quote_refresh",
        max_retries=2,
        base_backoff_s=0.01,
        quote_provider=quote_provider,
        execution_timeout_s=1.0,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    dispatched_prices = []

    async def mock_broker_quote_shift():
        nonlocal latest_bid, latest_ask
        for i in range(2):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            intent = drain_next_intent(ctx)
            dispatched_prices.append(intent.price)

            if i == 0:
                # Price moved before retry
                latest_bid = 2652.00
                latest_ask = 2652.50
                fb = ExecutionFeedback(
                    signal_id=intent.signal_id,
                    order_ticket=0,
                    execution_status=ExecutionStatus.REQUOTE,
                    broker_retcode=10004,
                )
            else:
                fb = ExecutionFeedback(
                    signal_id=intent.signal_id,
                    order_ticket=7788,
                    execution_status=ExecutionStatus.FILLED,
                )

            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    broker_task = asyncio.create_task(mock_broker_quote_shift())

    intent = OrderIntent(symbol="XAUUSD", side=OrderSide.BUY, price=2650.50, signal_id=1101)
    result = await orchestrator.submit_order("term_quote_refresh", intent)
    await broker_task

    assert result.success is True
    assert len(dispatched_prices) == 2
    assert dispatched_prices[0] == 2650.50
    # On retry, refreshed price was used
    assert dispatched_prices[1] == 2652.50

    await orchestrator.stop()
    await mock_dispatcher.stop()


# =============================================================================
# 7. Advanced Trade Management: TP Ladders, Virtual TP & Break-Even Tests
# =============================================================================

@pytest.mark.asyncio
async def test_submit_tp_ladder_hedging_mode_dispatches_3_separate_orders(mock_dispatcher: IPCDispatcher):
    """In Hedging mode, submit_tp_ladder dispatches 3 separate broker orders with volume split (50%, 30%, 20%)."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_hedge_ladder", platform_type=1)

    cfg = TerminalExecutionConfig(
        terminal_id="term_hedge_ladder",
        platform_type=1,
        is_netting=False, # Hedging mode
        execution_timeout_s=1.0,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    dispatched_orders = []

    # Broker fills 3 separate orders sequentially
    async def mock_broker_hedging():
        for i in range(3):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            intent = drain_next_intent(ctx)
            dispatched_orders.append(intent)

            fb = ExecutionFeedback(
                signal_id=intent.signal_id,
                order_ticket=9001 + i,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=intent.volume,
                executed_price=2650.00,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    broker_task = asyncio.create_task(mock_broker_hedging())

    parent_intent = OrderIntent(
        symbol="XAUUSD",
        side=OrderSide.BUY,
        volume=1.00,
        price=2650.00,
        stop_loss=2640.00,
        signal_id=2001,
    )
    tp_ladder = [2660.00, 2670.00, 2680.00]
    weights = [0.5, 0.3, 0.2] # 50%, 30%, 20%

    results = await orchestrator.submit_tp_ladder(
        terminal_id="term_hedge_ladder",
        intent=parent_intent,
        tp_ladder=tp_ladder,
        weights=weights,
    )
    await broker_task

    assert len(results) == 3
    assert all(r.success for r in results)
    assert [r.ticket for r in results] == [9001, 9002, 9003]

    # Verify dispatched sub-order volumes and TP levels
    assert pytest.approx(dispatched_orders[0].volume, 0.01) == 0.50
    assert dispatched_orders[0].take_profit == 2660.00

    assert pytest.approx(dispatched_orders[1].volume, 0.01) == 0.30
    assert dispatched_orders[1].take_profit == 2670.00

    assert pytest.approx(dispatched_orders[2].volume, 0.01) == 0.20
    assert dispatched_orders[2].take_profit == 2680.00

    # Stop loss is identical across all 3
    assert all(o.stop_loss == 2640.00 for o in dispatched_orders)

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_virtual_tp_manager_netting_account_partial_closes(mock_dispatcher: IPCDispatcher):
    """In MT5 Netting mode, single parent order is dispatched and Engine issues partial closes as virtual TPs hit."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_netting_tp", platform_type=1)

    cfg = TerminalExecutionConfig(
        terminal_id="term_netting_tp",
        platform_type=1,
        is_netting=True, # MT5 Netting mode
        execution_timeout_s=1.0,
    )
    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    orchestrator.register_terminal_config(cfg)
    await orchestrator.start()

    # Step 1: Submit parent order to netting account
    async def broker_fill_parent():
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        intent = drain_next_intent(ctx)
        fb = ExecutionFeedback(
            signal_id=intent.signal_id,
            order_ticket=777123,
            execution_status=ExecutionStatus.FILLED,
            executed_volume=1.00,
            executed_price=1.08500,
        )
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
        )

    fill_task = asyncio.create_task(broker_fill_parent())

    parent_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        volume=1.00,
        price=1.08500,
        stop_loss=1.08000,
        signal_id=3001,
    )
    tp_ladder = [1.08700, 1.08900, 1.09100]
    weights = [0.5, 0.3, 0.2] # 50% TP1, 30% TP2, 20% TP3

    res_list = await orchestrator.submit_tp_ladder(
        terminal_id="term_netting_tp",
        intent=parent_intent,
        tp_ladder=tp_ladder,
        weights=weights,
    )
    await fill_task

    assert len(res_list) == 1
    assert res_list[0].success is True
    assert res_list[0].ticket == 777123

    managed = orchestrator.get_managed_position(777123)
    assert managed is not None
    assert managed.is_netting is True
    assert len(managed.virtual_tps) == 3

    # Step 2: Simulate price rising through virtual TP milestones
    partial_commands = []

    async def broker_handle_partial_closes(expected_closes: int):
        for _ in range(expected_closes):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            partial_commands.append(cmd)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=cmd.order_ticket,
                execution_status=ExecutionStatus.FILLED,
                executed_volume=cmd.volume,
                executed_price=cmd.price,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    # 1. Price below TP1 (no triggers)
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08600, ask=1.08610)
    assert len(partial_commands) == 0

    # 2. Price crosses TP1 (1.08700) -> Triggers Partial Close 0.50 lot
    h_task1 = asyncio.create_task(broker_handle_partial_closes(1))
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08720, ask=1.08730)
    await h_task1

    assert len(partial_commands) == 1
    assert partial_commands[0].action_type == ActionType.PARTIAL_CLOSE
    assert pytest.approx(partial_commands[0].volume, 0.01) == 0.50
    assert pytest.approx(managed.current_volume, 0.01) == 0.50

    # 3. Price crosses TP2 (1.08900) -> Triggers Partial Close 0.30 lot
    h_task2 = asyncio.create_task(broker_handle_partial_closes(1))
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08910, ask=1.08920)
    await h_task2

    assert len(partial_commands) == 2
    assert partial_commands[1].action_type == ActionType.PARTIAL_CLOSE
    assert pytest.approx(partial_commands[1].volume, 0.01) == 0.30
    assert pytest.approx(managed.current_volume, 0.01) == 0.20

    # 4. Price crosses TP3 (1.09100) -> Triggers Final Full Close remaining 0.20 lot
    h_task3 = asyncio.create_task(broker_handle_partial_closes(1))
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.09120, ask=1.09130)
    await h_task3

    assert len(partial_commands) == 3
    assert partial_commands[2].action_type == ActionType.FULL_CLOSE
    assert pytest.approx(partial_commands[2].volume, 0.01) == 0.20
    assert managed.current_volume == 0.0
    assert managed.closed is True

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_break_even_modifies_stop_loss_to_entry_plus_offset(mock_dispatcher: IPCDispatcher):
    """When a position reaches TP1, Break-Even logic dispatches OrderModify moving SL to Entry + offset."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_be_mod", platform_type=1)

    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    await orchestrator.start()

    # Register managed position: BUY EURUSD at 1.08500, SL=1.08000, TP1=1.08800
    be_cfg = BreakEvenConfig(
        enabled=True,
        trigger_mode="tp1",
        offset_pips=2.0, # Move SL to Entry + 2 pips (1.08520)
    )
    pos = ManagedPosition(
        ticket=555666,
        terminal_id="term_be_mod",
        symbol="EURUSD",
        side=OrderSide.BUY,
        entry_price=1.08500,
        original_volume=0.50,
        current_volume=0.50,
        current_sl=1.08000,
        current_tp=1.09500,
        pip_size=0.0001,
        virtual_tps=[VirtualTPLevel(level_index=0, target_price=1.08800, volume_to_close=0.50)],
        break_even_config=be_cfg,
    )
    orchestrator.register_managed_position(pos)

    modify_dispatched = []

    async def broker_handle_modify():
        while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
            await asyncio.sleep(0.005)
        cmd = drain_next_intent(ctx)
        modify_dispatched.append(cmd)
        fb = ExecutionFeedback(
            signal_id=cmd.signal_id,
            order_ticket=cmd.order_ticket,
            execution_status=ExecutionStatus.FILLED,
        )
        ctx.pipe_server.simulate_inbound_data(
            pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
        )

    # 1. Price below trigger: 1.08700 < 1.08800 -> No BE dispatch
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08700, ask=1.08710)
    assert len(modify_dispatched) == 0
    assert pos.is_be_activated is False

    # 2. Price hits TP1: bid=1.08805 >= 1.08800 -> Dispatches OrderModify
    mod_task = asyncio.create_task(broker_handle_modify())
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08805, ask=1.08815)
    await mod_task

    assert len(modify_dispatched) == 1
    cmd = modify_dispatched[0]
    assert cmd.action_type == ActionType.MODIFY_SL_TP
    assert cmd.order_ticket == 555666
    # Entry (1.08500) + 2 pips (0.00020) = 1.08520
    assert pytest.approx(cmd.stop_loss, 0.00001) == 1.08520
    assert pos.is_be_activated is True
    assert pytest.approx(pos.current_sl, 0.00001) == 1.08520

    await orchestrator.stop()
    await mock_dispatcher.stop()


@pytest.mark.asyncio
async def test_break_even_multi_order_hedging_ladder_advancement(mock_dispatcher: IPCDispatcher):
    """In Hedging multi-order ladders, triggering BE on TP1 moves SL for remaining orders (TP2, TP3) to BE."""
    await mock_dispatcher.start()
    ctx = await setup_active_terminal(mock_dispatcher, "term_hedge_be", platform_type=1)

    orchestrator = ExecutionOrchestrator(ipc_dispatcher=mock_dispatcher)
    await orchestrator.start()

    group_id = "GRP-HEDGE-99"
    be_cfg = BreakEvenConfig(enabled=True, trigger_mode="tp1", offset_pips=1.0) # Entry + 1 pip

    # Create 2 managed positions in the same group (representing TP1 and TP2 orders)
    pos1 = ManagedPosition(
        ticket=1001,
        terminal_id="term_hedge_be",
        symbol="EURUSD",
        side=OrderSide.BUY,
        entry_price=1.08500,
        original_volume=0.50,
        current_volume=0.50,
        current_sl=1.08000,
        current_tp=1.08700, # TP1
        pip_size=0.0001,
        group_id=group_id,
        virtual_tps=[VirtualTPLevel(level_index=0, target_price=1.08700, volume_to_close=0.50)],
        break_even_config=be_cfg,
    )
    pos2 = ManagedPosition(
        ticket=1002,
        terminal_id="term_hedge_be",
        symbol="EURUSD",
        side=OrderSide.BUY,
        entry_price=1.08500,
        original_volume=0.50,
        current_volume=0.50,
        current_sl=1.08000,
        current_tp=1.09200, # TP2
        pip_size=0.0001,
        group_id=group_id,
        virtual_tps=[VirtualTPLevel(level_index=0, target_price=1.08700, volume_to_close=0.50)],
        break_even_config=be_cfg,
    )
    orchestrator.register_managed_position(pos1)
    orchestrator.register_managed_position(pos2)
    orchestrator._position_groups[group_id] = [1001, 1002]

    modifies = []

    async def broker_handle_both_modifies():
        for _ in range(2):
            while ctx.shm_writer.get_metrics()["queue_depth"] == 0:
                await asyncio.sleep(0.005)
            cmd = drain_next_intent(ctx)
            modifies.append(cmd)
            fb = ExecutionFeedback(
                signal_id=cmd.signal_id,
                order_ticket=cmd.order_ticket,
                execution_status=ExecutionStatus.FILLED,
            )
            ctx.pipe_server.simulate_inbound_data(
                pack_pipe_frame(NamedPipeOpcode.MSG_EXECUTION_FEEDBACK, fb.pack())
            )

    b_task = asyncio.create_task(broker_handle_both_modifies())

    # Market reaches TP1 (1.08700)
    await orchestrator.on_price_update(symbol="EURUSD", bid=1.08705, ask=1.08715)
    await b_task

    assert len(modifies) == 2
    # Both ticket 1001 and peer ticket 1002 have their SL moved to 1.08510
    tickets_modified = {m.order_ticket for m in modifies}
    assert tickets_modified == {1001, 1002}
    assert all(pytest.approx(m.stop_loss, 0.00001) == 1.08510 for m in modifies)
    assert pos1.is_be_activated and pos2.is_be_activated

    await orchestrator.stop()
    await mock_dispatcher.stop()


