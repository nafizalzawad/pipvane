"""
Unit tests validating Pipvane v3 RiskEngine, Position Sizing, and Entry Guards.
Aligned with Sprint 1 ADR-001 and SPRINT_1_ARCHITECTURE_BLUEPRINT.
"""

import math
import time
import pytest

from pathlib import Path

from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
)
from engine.risk_engine import (
    RiskEngine,
    RiskModel,
    AccountProfile,
    MarketQuote,
    SkipReason,
    SKIP_AGE,
    SKIP_DRIFT,
    SKIP_SPREAD,
    SKIP_SL_REQUIRED,
    SKIP_EQUITY_HARD_STOP,
    SKIP_MAX_POSITIONS,
    SKIP_MIN_MARGIN,
    SKIP_INVALID_QUOTE,
    SKIP_CIRCUIT_BREAKER,
    SKIP_NEWS,
    CircuitBreakerResult,
    generate_panic_close_intent,
    check_equity_circuit_breaker,
    EconomicEvent,
    EconomicCalendar,
    load_economic_calendar,
    check_news_filter,
    extract_currencies_for_symbol,
    quantize_lot,
    calculate_fixed_lot,
    calculate_equity_risk_lot,
    calculate_fixed_currency_risk_lot,
    calculate_provider_multiplier_lot,
    check_signal_age,
    check_price_drift,
    check_spread,
    get_default_pip_size,
    get_default_pip_value,
    OpenPosition,
)


# =============================================================================
# 1. LOT QUANTIZATION TESTS
# =============================================================================

@pytest.mark.unit
def test_quantize_lot_rounding_and_clamping():
    """Verify lot quantization respects step, min_lot, and max_lot bounds."""
    # Standard 0.01 step
    assert quantize_lot(0.124, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 0.12
    assert quantize_lot(0.126, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 0.13

    # Clamping min_lot
    assert quantize_lot(0.004, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 0.01
    assert quantize_lot(0.0, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 0.01
    assert quantize_lot(-1.0, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 0.01

    # Clamping max_lot
    assert quantize_lot(150.0, min_lot=0.01, max_lot=100.0, lot_step=0.01) == 100.0

    # Custom step: 0.05 step
    assert quantize_lot(0.12, min_lot=0.05, max_lot=50.0, lot_step=0.05) == 0.10
    assert quantize_lot(0.14, min_lot=0.05, max_lot=50.0, lot_step=0.05) == 0.15

    # 1.0 step (e.g. Crypto/Indices contracts)
    assert quantize_lot(3.7, min_lot=1.0, max_lot=50.0, lot_step=1.0) == 4.0


# =============================================================================
# 2. POSITION SIZING MODEL TESTS
# =============================================================================

@pytest.mark.unit
def test_fixed_lot_sizing():
    """Verify Fixed Lot risk model returns configured lot bounded by broker constraints."""
    # Standard fixed lot
    acc1 = AccountProfile(fixed_lot=0.25, min_lot=0.01, max_lot=100.0, lot_step=0.01)
    assert calculate_fixed_lot(acc1) == 0.25

    # Sub-minimum fixed lot clamped up
    acc2 = AccountProfile(fixed_lot=0.005, min_lot=0.01, max_lot=100.0, lot_step=0.01)
    assert calculate_fixed_lot(acc2) == 0.01

    # Exceeding max lot clamped down
    acc3 = AccountProfile(fixed_lot=150.0, min_lot=0.01, max_lot=100.0, lot_step=0.01)
    assert calculate_fixed_lot(acc3) == 100.0


@pytest.mark.unit
def test_equity_risk_lot_sizing():
    """Verify % of Equity Risk sizing calculates correct lot from SL distance."""
    acc = AccountProfile(
        equity=10000.0,
        equity_risk_pct=1.0,  # 1% of $10,000 = $100 risk
        min_lot=0.01,
        max_lot=100.0,
        lot_step=0.01,
    )
    # EURUSD: Entry 1.08500, SL 1.08000 -> 50 pips distance
    # pip_size = 0.0001, pip_value = $10.00 per pip per lot
    # lot = $100 / (50 * $10) = $100 / $500 = 0.20 lots
    lot, err = calculate_equity_risk_lot(
        account=acc,
        entry_price=1.08500,
        stop_loss=1.08000,
        pip_size=0.0001,
        pip_value=10.0,
    )
    assert err is None
    assert lot is not None
    assert math.isclose(lot, 0.20)

    # Gold XAUUSD: Equity $25,000, 2% risk ($500 risk)
    # Entry 2650.0, SL 2640.0 -> 100 pips of 0.1, pip_value = $10.00
    # lot = $500 / (100 * $10) = $500 / $1000 = 0.50 lots
    acc_gold = AccountProfile(equity=25000.0, equity_risk_pct=2.0)
    lot_gold, err_gold = calculate_equity_risk_lot(
        account=acc_gold,
        entry_price=2650.0,
        stop_loss=2640.0,
        pip_size=0.1,
        pip_value=10.0,
    )
    assert err_gold is None
    assert lot_gold is not None
    assert math.isclose(lot_gold, 0.50)


@pytest.mark.unit
def test_equity_risk_missing_or_invalid_sl():
    """Verify % of Equity Risk fails when SL is 0 or equal to Entry."""
    acc = AccountProfile(equity=10000.0, equity_risk_pct=1.0)

    # Missing SL (0.0)
    lot1, err1 = calculate_equity_risk_lot(acc, 1.0850, 0.0, 0.0001, 10.0)
    assert lot1 is None
    assert "missing or zero" in err1

    # SL equals Entry (0 distance)
    lot2, err2 = calculate_equity_risk_lot(acc, 1.0850, 1.0850, 0.0001, 10.0)
    assert lot2 is None
    assert "equals entry" in err2


@pytest.mark.unit
def test_fixed_currency_risk_lot_sizing():
    """Verify Fixed Currency Risk sizing calculates correct lot from cash risk amount."""
    acc = AccountProfile(
        fixed_currency_risk=50.0,  # $50.00 cash risk
        min_lot=0.01,
        max_lot=100.0,
        lot_step=0.01,
    )
    # EURUSD 50 pips SL ($500/lot): lot = 50 / 500 = 0.10 lots
    lot, err = calculate_fixed_currency_risk_lot(
        account=acc,
        entry_price=1.08500,
        stop_loss=1.08000,
        pip_size=0.0001,
        pip_value=10.0,
    )
    assert err is None
    assert lot is not None
    assert math.isclose(lot, 0.10)

    # Missing SL returns error
    lot_no_sl, err_no_sl = calculate_fixed_currency_risk_lot(
        account=acc,
        entry_price=1.08500,
        stop_loss=0.0,
        pip_size=0.0001,
        pip_value=10.0,
    )
    assert lot_no_sl is None
    assert "missing or zero" in err_no_sl


@pytest.mark.unit
def test_provider_multiplier_lot_sizing():
    """Verify Provider Multiplier scales incoming volume accurately."""
    acc = AccountProfile(
        lot_multiplier=2.5,
        min_lot=0.01,
        max_lot=100.0,
        lot_step=0.01,
    )
    # Provider 0.04 * 2.5 = 0.10 lots
    assert math.isclose(calculate_provider_multiplier_lot(acc, 0.04), 0.10)

    # Provider 0.01 * 0.5 = 0.005 -> clamped up to min_lot 0.01
    acc_sub = AccountProfile(lot_multiplier=0.5, min_lot=0.01)
    assert calculate_provider_multiplier_lot(acc_sub, 0.01) == 0.01


# =============================================================================
# 3. ENTRY GUARD TESTS
# =============================================================================

@pytest.mark.unit
def test_signal_age_guard():
    """Verify signal age guard rejects stale signals per Blueprint Section 5.6."""
    eval_time = 1700000100.0

    # Fresh signal: 5s old (<= 30s) -> Pass
    ok1, reason1 = check_signal_age(
        signal_timestamp_s=1700000095.0,
        eval_timestamp_s=eval_time,
        max_signal_age_s=30.0,
    )
    assert ok1 is True
    assert reason1 is None

    # Stale signal: 45s old (> 30s) -> Fail with SKIP_AGE
    ok2, reason2 = check_signal_age(
        signal_timestamp_s=1700000055.0,
        eval_timestamp_s=eval_time,
        max_signal_age_s=30.0,
    )
    assert ok2 is False
    assert reason2 is not None
    assert reason2.code == SKIP_AGE
    assert "45.00s exceeds maximum allowed age 30.00s" in reason2.reason

    # Signal timestamp 0 (omitted) -> Pass (cannot determine age)
    ok3, reason3 = check_signal_age(0.0, eval_time, 30.0)
    assert ok3 is True
    assert reason3 is None


@pytest.mark.unit
def test_price_drift_guard():
    """Verify price drift guard detects both adverse and excessive price movement."""
    # BUY Order: Signal at 1.08500
    # Current Ask 1.08520 -> 2.0 pips drift (<= 5.0 max drift) -> Pass
    ok1, reason1 = check_price_drift(
        signal_price=1.08500,
        current_price=1.08520,
        side=OrderSide.BUY,
        pip_size=0.0001,
        max_drift_pips=5.0,
    )
    assert ok1 is True
    assert reason1 is None

    # BUY Order: Current Ask 1.08580 -> 8.0 pips drift (> 5.0 max drift) -> Fail with SKIP_DRIFT
    ok2, reason2 = check_price_drift(
        signal_price=1.08500,
        current_price=1.08580,
        side=OrderSide.BUY,
        pip_size=0.0001,
        max_drift_pips=5.0,
    )
    assert ok2 is False
    assert reason2 is not None
    assert reason2.code == SKIP_DRIFT
    assert "8.0 pips exceeds max allowed drift 5.0 pips" in reason2.reason

    # SELL Order: Signal at 1.08500, Current Bid 1.08410 -> 9.0 pips adverse drift -> Fail
    ok3, reason3 = check_price_drift(
        signal_price=1.08500,
        current_price=1.08410,
        side=OrderSide.SELL,
        pip_size=0.0001,
        max_drift_pips=5.0,
    )
    assert ok3 is False
    assert reason3 is not None
    assert reason3.code == SKIP_DRIFT

    # Favorable Drift Test:
    # BUY signal at 1.08500, current Ask 1.08420 (cheaper by 8 pips)
    # Default strict check (allow_favorable=False): Rejects because absolute drift > 5 pips
    ok_strict, _ = check_price_drift(1.08500, 1.08420, OrderSide.BUY, 0.0001, 5.0, allow_favorable=False)
    assert ok_strict is False

    # With allow_favorable=True: Passes because adverse drift is negative (-8 pips)
    ok_fav, _ = check_price_drift(1.08500, 1.08420, OrderSide.BUY, 0.0001, 5.0, allow_favorable=True)
    assert ok_fav is True

    # Signal price 0.0 (pure market order) -> Passes
    ok_zero, _ = check_price_drift(0.0, 1.08520, OrderSide.BUY, 0.0001, 5.0)
    assert ok_zero is True


@pytest.mark.unit
def test_max_spread_guard():
    """Verify broker spread guard rejects execution during market spread spikes."""
    # Normal spread: EURUSD Bid 1.08500, Ask 1.08515 (1.5 pips <= 3.0 max) -> Pass
    quote_normal = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08515)
    ok1, reason1 = check_spread(quote_normal, max_spread_pips=3.0)
    assert ok1 is True
    assert reason1 is None

    # Wide spread: Bid 1.08500, Ask 1.08545 (4.5 pips > 3.0 max) -> Fail with SKIP_SPREAD
    quote_wide = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08545)
    ok2, reason2 = check_spread(quote_wide, max_spread_pips=3.0)
    assert ok2 is False
    assert reason2 is not None
    assert reason2.code == SKIP_SPREAD
    assert "4.5 pips exceeds max allowed spread 3.0 pips" in reason2.reason


# =============================================================================
# 4. ACCOUNT SAFETY GUARDS
# =============================================================================

@pytest.mark.unit
def test_account_safety_guards():
    """Verify equity hard stop, max open positions, and invalid quote safety guards."""
    quote = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08510)
    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        price=1.08500,
        stop_loss=1.08000,
    )

    # 1. Equity Hard Stop
    acc_hard_stop = AccountProfile(equity=4500.0, equity_hard_stop=5000.0)
    res_stop = RiskEngine.evaluate(intent, acc_hard_stop, quote)
    assert isinstance(res_stop, SkipReason)
    assert res_stop.code == SKIP_EQUITY_HARD_STOP

    # 2. Max Open Positions Limit
    acc_max_pos = AccountProfile(open_positions_count=5, max_open_positions=5)
    res_pos = RiskEngine.evaluate(intent, acc_max_pos, quote)
    assert isinstance(res_pos, SkipReason)
    assert res_pos.code == SKIP_MAX_POSITIONS

    # 3. Minimum Free Margin
    acc_margin = AccountProfile(free_margin=150.0, min_free_margin=300.0)
    res_margin = RiskEngine.evaluate(intent, acc_margin, quote)
    assert isinstance(res_margin, SkipReason)
    assert res_margin.code == SKIP_MIN_MARGIN

    # 4. Invalid Market Quote
    bad_quote = MarketQuote(symbol="EURUSD", bid=1.08600, ask=1.08500)  # Inverted bid > ask
    res_bad = RiskEngine.evaluate(intent, AccountProfile(), bad_quote)
    assert isinstance(res_bad, SkipReason)
    assert res_bad.code == SKIP_INVALID_QUOTE


# =============================================================================
# 5. STATELESS RISK ENGINE INTEGRATION TESTS
# =============================================================================

@pytest.mark.unit
def test_stateless_evaluation_immutability_and_pure_functional():
    """Verify RiskEngine never mutates input intent and returns pure new instance."""
    now_ns = int(1700000100.0 * 1e9)
    eval_time = 1700000105.0  # 5s age

    intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.MARKET_ENTRY,
        volume=0.01,  # Raw default
        price=1.08500,
        stop_loss=1.08000,
        timestamp_ns=now_ns,
    )
    account = AccountProfile(
        equity=10000.0,
        equity_risk_pct=1.0,  # 1% risk ($100), 50 pips SL ($500/lot) -> 0.20 lots
        risk_model=RiskModel.PERCENT_EQUITY,
    )
    quote = MarketQuote(symbol="EURUSD", bid=1.08510, ask=1.08520)

    # Evaluate
    evaluated = RiskEngine.evaluate(intent, account, quote, eval_time=eval_time)

    # Verify evaluated intent
    assert isinstance(evaluated, OrderIntent)
    assert math.isclose(evaluated.volume, 0.20)
    assert evaluated.symbol == "EURUSD"
    assert evaluated.side == OrderSide.BUY

    # Verify input intent was NOT mutated (pure function)
    assert math.isclose(intent.volume, 0.01)

    # Repeated calls produce identical results
    evaluated2 = RiskEngine.evaluate(intent, account, quote, eval_time=eval_time)
    assert math.isclose(evaluated2.volume, 0.20)


@pytest.mark.unit
def test_non_entry_actions_bypass_drift_and_preserve_volume():
    """Verify position management actions bypass drift check and retain volumes."""
    quote = MarketQuote(symbol="EURUSD", bid=1.09500, ask=1.09510)  # Drifted 100 pips away!

    # 1. FULL_CLOSE
    close_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.FULL_CLOSE,
        volume=0.50,
        price=1.08500,
    )
    res_close = RiskEngine.evaluate(close_intent, AccountProfile(), quote)
    assert isinstance(res_close, OrderIntent)
    assert res_close.action_type == ActionType.FULL_CLOSE
    assert math.isclose(res_close.volume, 0.50)

    # 2. CANCEL_PENDING
    cancel_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.SELL,
        action_type=ActionType.CANCEL_PENDING,
        volume=0.25,
        price=1.08500,
    )
    res_cancel = RiskEngine.evaluate(cancel_intent, AccountProfile(), quote)
    assert isinstance(res_cancel, OrderIntent)
    assert res_cancel.action_type == ActionType.CANCEL_PENDING

    # 3. PARTIAL_CLOSE
    partial_intent = OrderIntent(
        symbol="EURUSD",
        side=OrderSide.BUY,
        action_type=ActionType.PARTIAL_CLOSE,
        volume=0.126,  # Sized from partial close %
        price=1.08500,
    )
    res_partial = RiskEngine.evaluate(partial_intent, AccountProfile(lot_step=0.01), quote)
    assert isinstance(res_partial, OrderIntent)
    assert math.isclose(res_partial.volume, 0.13)  # Quantized


# =============================================================================
# 6. EQUITY CIRCUIT BREAKER TESTS (SPRINT 7)
# =============================================================================

@pytest.mark.unit
def test_equity_circuit_breaker_within_limits():
    """Verify circuit breaker remains idle when drawdowns are within limits."""
    res = check_equity_circuit_breaker(
        equity=9800.0,
        balance=10000.0,
        daily_loss_limit=500.0,
        max_loss_limit=1000.0,
        starting_equity=10000.0,
    )
    assert res.is_tripped is False
    assert res.pause_entries is False
    assert res.panic_close_intent is None
    assert res.skip_reason is None
    assert math.isclose(res.drawdown_amount, 200.0)
    assert math.isclose(res.drawdown_pct, 2.0)


@pytest.mark.unit
def test_equity_circuit_breaker_daily_loss_breach():
    """Verify daily loss breach immediately trips breaker and creates panic close."""
    res = check_equity_circuit_breaker(
        equity=9400.0,
        balance=10000.0,
        daily_loss_limit=500.0,
        max_loss_limit=1000.0,
        starting_equity=10000.0,
    )
    assert res.is_tripped is True
    assert res.pause_entries is True
    assert res.panic_close_intent is not None
    assert res.panic_close_intent.action_type == ActionType.FULL_CLOSE
    assert "DAILY_LOSS_LIMIT_BREACHED" in res.trigger_reason
    assert res.skip_reason is not None
    assert res.skip_reason.code == SKIP_CIRCUIT_BREAKER
    assert math.isclose(res.drawdown_amount, 600.0)


@pytest.mark.unit
def test_equity_circuit_breaker_max_loss_breach():
    """Verify max loss breach trips breaker with MAX_LOSS_LIMIT_BREACHED."""
    res = check_equity_circuit_breaker(
        equity=8800.0,
        balance=10000.0,
        daily_loss_limit=1500.0,  # Daily limit not breached
        max_loss_limit=1000.0,   # Max limit breached (drawdown 1200 >= 1000)
        starting_equity=10000.0,
    )
    assert res.is_tripped is True
    assert res.pause_entries is True
    assert res.panic_close_intent is not None
    assert "MAX_LOSS_LIMIT_BREACHED" in res.trigger_reason
    assert math.isclose(res.drawdown_amount, 1200.0)


@pytest.mark.unit
def test_equity_circuit_breaker_with_open_positions():
    """Verify panic close intents are generated for every active open position."""
    pos1 = OpenPosition(ticket=101, symbol="EURUSD", side=OrderSide.BUY, volume=0.50, open_price=1.0850, current_price=1.0800)
    pos2 = OpenPosition(ticket=102, symbol="GBPUSD", side=OrderSide.SELL, volume=0.25, open_price=1.2650, current_price=1.2700)

    res = check_equity_circuit_breaker(
        equity=9300.0,
        balance=10000.0,
        daily_loss_limit=500.0,
        open_positions=[pos1, pos2],
        starting_equity=10000.0,
    )
    assert res.is_tripped is True
    assert len(res.panic_close_intents) == 2

    # Intent 1
    intent1 = res.panic_close_intents[0]
    assert intent1.symbol == "EURUSD"
    assert intent1.order_ticket == 101
    assert intent1.action_type == ActionType.FULL_CLOSE
    assert math.isclose(intent1.volume, 0.50)

    # Intent 2
    intent2 = res.panic_close_intents[1]
    assert intent2.symbol == "GBPUSD"
    assert intent2.order_ticket == 102
    assert intent2.action_type == ActionType.FULL_CLOSE
    assert math.isclose(intent2.volume, 0.25)


# =============================================================================
# 7. ECONOMIC NEWS CALENDAR INGESTION TESTS (SPRINT 7)
# =============================================================================

@pytest.mark.unit
def test_economic_calendar_from_file():
    """Verify local JSON cache reader loads and indexes events accurately."""
    calendar_path = Path(__file__).parent.parent.parent.parent / "engine" / "data" / "economic_calendar.json"
    cal = EconomicCalendar.from_file(calendar_path)
    events = cal.get_events()
    assert len(events) >= 5

    # Check NFP event
    nfp = next((e for e in events if "NFP" in e.title or "Non-Farm" in e.title), None)
    assert nfp is not None
    assert nfp.currency == "USD"
    assert nfp.impact == "HIGH"
    assert nfp.timestamp == 1700000000.0


@pytest.mark.unit
def test_economic_calendar_from_dict_list():
    """Verify in-memory dictionary ingestion and currency grouping."""
    records = [
        {"id": "ev1", "title": "CPI", "currency": "usd", "impact": "high", "timestamp": 1700001000},
        {"id": "ev2", "title": "GDP", "currency": "eur", "impact": "medium", "timestamp": 1700002000},
    ]
    cal = EconomicCalendar.from_dict_list(records)
    events = cal.get_events()
    assert len(events) == 2
    assert events[0].currency == "USD"
    assert events[0].impact == "HIGH"
    assert events[1].currency == "EUR"
    assert events[1].impact == "MEDIUM"


# =============================================================================
# 8. ECONOMIC NEWS FILTERING TESTS (SPRINT 7)
# =============================================================================

@pytest.mark.unit
def test_news_filter_blocks_currency_pair_before_event():
    """Verify news filter blocks currency pairs within 30 min before High Impact event."""
    cal = EconomicCalendar.from_dict_list([
        {"id": "nfp", "title": "Non-Farm Payrolls", "currency": "USD", "impact": "HIGH", "timestamp": 1700000000.0}
    ])

    # Case 1: 15 minutes before event (t = 1700000000 - 900) on EURUSD (contains USD) -> BLOCKED
    eval_time = 1700000000.0 - 900.0
    ok1, reason1 = check_news_filter(
        symbol="EURUSD",
        current_time=eval_time,
        calendar=cal,
        window_before_s=1800.0,
        window_after_s=1800.0,
    )
    assert ok1 is False
    assert reason1 is not None
    assert reason1.code == SKIP_NEWS
    assert "Non-Farm Payrolls" in reason1.reason
    assert "USD" in reason1.reason

    # Case 2: Same time on EURGBP (does NOT contain USD) -> PASSED
    ok2, reason2 = check_news_filter(
        symbol="EURGBP",
        current_time=eval_time,
        calendar=cal,
        window_before_s=1800.0,
        window_after_s=1800.0,
    )
    assert ok2 is True
    assert reason2 is None


@pytest.mark.unit
def test_news_filter_blocks_currency_pair_after_event():
    """Verify news filter blocks currency pairs within 30 min after High Impact event."""
    cal = EconomicCalendar.from_dict_list([
        {"id": "ecb", "title": "ECB Rate Decision", "currency": "EUR", "impact": "HIGH", "timestamp": 1700100000.0}
    ])
    # 20 minutes after event on EURJPY (contains EUR) -> BLOCKED
    eval_time = 1700100000.0 + 1200.0
    ok, reason = check_news_filter(
        symbol="EURJPY",
        current_time=eval_time,
        calendar=cal,
        window_before_s=1800.0,
        window_after_s=1800.0,
    )
    assert ok is False
    assert reason is not None
    assert reason.code == SKIP_NEWS
    assert "ECB Rate Decision" in reason.reason


@pytest.mark.unit
def test_news_filter_passes_outside_window():
    """Verify trades pass when outside the active news window."""
    cal = EconomicCalendar.from_dict_list([
        {"id": "boe", "title": "BOE Rate", "currency": "GBP", "impact": "HIGH", "timestamp": 1700120000.0}
    ])
    # 45 minutes before event (window is 30 min) -> PASSED
    eval_time = 1700120000.0 - 2700.0
    ok, reason = check_news_filter(
        symbol="GBPUSD",
        current_time=eval_time,
        calendar=cal,
        window_before_s=1800.0,
        window_after_s=1800.0,
    )
    assert ok is True
    assert reason is None


@pytest.mark.unit
def test_news_filter_ignores_medium_impact_when_filtered_to_high():
    """Verify medium impact events do not block unless explicitly configured."""
    cal = EconomicCalendar.from_dict_list([
        {"id": "pmi", "title": "German PMI", "currency": "EUR", "impact": "MEDIUM", "timestamp": 1700050000.0}
    ])
    eval_time = 1700050000.0

    # Default impact_levels=("HIGH",) -> Should NOT block
    ok_high, _ = check_news_filter("EURUSD", eval_time, cal, impact_levels=("HIGH",))
    assert ok_high is True

    # Configuring impact_levels=("HIGH", "MEDIUM") -> Should block
    ok_med, reason_med = check_news_filter("EURUSD", eval_time, cal, impact_levels=("HIGH", "MEDIUM"))
    assert ok_med is False
    assert reason_med.code == SKIP_NEWS


# =============================================================================
# 9. INTEGRATED RISK ENGINE: CIRCUIT BREAKER & NEWS FILTER
# =============================================================================

@pytest.mark.unit
def test_risk_engine_integration_blocks_on_circuit_breaker():
    """Verify RiskEngine.evaluate blocks new entries when circuit breaker is tripped."""
    intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.0850, stop_loss=1.0800)
    quote = MarketQuote(symbol="EURUSD", bid=1.0850, ask=1.0851)

    # Account with daily loss breached ($600 drawdown > $500 limit)
    account = AccountProfile(
        equity=9400.0,
        balance=10000.0,
        daily_loss_limit=500.0,
        daily_starting_equity=10000.0,
    )
    res = RiskEngine.evaluate(intent, account, quote)
    assert isinstance(res, SkipReason)
    assert res.code == SKIP_CIRCUIT_BREAKER
    assert "DAILY_LOSS_LIMIT_BREACHED" in res.reason


@pytest.mark.unit
def test_risk_engine_integration_blocks_on_high_impact_news():
    """Verify RiskEngine.evaluate blocks new entries during active news window."""
    cal = EconomicCalendar.from_dict_list([
        {"id": "nfp", "title": "NFP Report", "currency": "USD", "impact": "HIGH", "timestamp": 1700000000.0}
    ])
    eval_time = 1700000000.0 - 600.0  # 10 min before NFP

    intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.0850, stop_loss=1.0800)
    quote = MarketQuote(symbol="EURUSD", bid=1.0850, ask=1.0851)

    account = AccountProfile(
        equity=10000.0,
        news_filter_enabled=True,
        economic_calendar=cal,
        news_window_before_s=1800.0,
        news_window_after_s=1800.0,
    )

    # EURUSD blocked because USD is in active window
    res_eurusd = RiskEngine.evaluate(intent, account, quote, eval_time=eval_time)
    assert isinstance(res_eurusd, SkipReason)
    assert res_eurusd.code == SKIP_NEWS
    assert "NFP Report" in res_eurusd.reason

    # EURGBP passes because neither EUR nor GBP is in news window
    intent_eurgbp = OrderIntent(symbol="EURGBP", side=OrderSide.BUY, price=0.8550, stop_loss=0.8500)
    quote_eurgbp = MarketQuote(symbol="EURGBP", bid=0.8550, ask=0.8551)
    res_eurgbp = RiskEngine.evaluate(intent_eurgbp, account, quote_eurgbp, eval_time=eval_time)
    assert isinstance(res_eurgbp, OrderIntent)
    assert res_eurgbp.symbol == "EURGBP"

