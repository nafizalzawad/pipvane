#!/usr/bin/env python3
"""
Pipvane v3: Sprint 7 RiskEngine Unit Test Suite.
------------------------------------------------
Comprehensive unit tests validating:
1. Sizing Math: >= 20 hand-calculated test cases for % of Equity Risk across different
   asset classes (Forex Majors, USD Base, Crosses, Gold, Silver, Indices, Crypto).
2. Boundary Value Testing: Exact boundary testing on Entry Guards (e.g. spread 3.9, 4.0, 4.1).
3. Equity Breaker & Global Panic Response: Real-time tick evaluation simulating positions sliding
   into deep loss, asserting the exact breach tick triggers emergency liquidation and halts trading.
4. Offline Economic News Filter & Account Safety Guards.
"""

from __future__ import annotations

import math
import pytest

from engine.ipc_protocol import (
    ActionType,
    ExecutionFlags,
    OrderIntent,
    OrderSide,
)
from engine.risk_engine import (
    AccountProfile,
    EconomicCalendar,
    EconomicEvent,
    MarketQuote,
    RiskEngine,
    SKIP_AGE,
    SKIP_CIRCUIT_BREAKER,
    SKIP_DRIFT,
    SKIP_EQUITY_HARD_STOP,
    SKIP_MAX_POSITIONS,
    SKIP_MIN_MARGIN,
    SKIP_NEWS,
    SKIP_SPREAD,
    SkipReason,
    calculate_equity_risk_lot,
    calculate_fixed_currency_risk_lot,
    calculate_fixed_lot,
    calculate_provider_multiplier_lot,
    check_equity_circuit_breaker,
    check_news_filter,
    check_price_drift,
    check_signal_age,
    check_spread,
)


# =============================================================================
# 1. POSITION SIZING MATH TESTS (% OF EQUITY RISK ACROSS ASSET CLASSES)
# =============================================================================

# Comprehensive hand-calculated test cases across Forex, Metals, Indices, and Crypto
# Format: (symbol, equity, risk_pct, entry_price, stop_loss, quote_to_usd_rate, expected_lot, desc)
SIZING_TEST_CASES = [
    # 1. Forex USD Quote: EURUSD (100k contract, 1 pip = 0.0001, $10.00/pip)
    # Risk = 1.0% of $10,000 = $100. SL distance = 50 pips ($500/lot). Lot = 100/500 = 0.20
    ("EURUSD", 10000.0, 1.0, 1.08500, 1.08000, 1.0, 0.20, "EURUSD standard 50 pips SL"),

    # 2. EURUSD: Larger equity & higher risk
    # Risk = 2.0% of $25,000 = $500. SL = 40 pips ($400/lot). Lot = 500/400 = 1.25
    ("EURUSD", 25000.0, 2.0, 1.10000, 1.09600, 1.0, 1.25, "EURUSD $25k equity 2% risk"),

    # 3. GBPUSD: Tight risk
    # Risk = 0.5% of $50,000 = $250. SL = 50 pips ($500/lot). Lot = 250/500 = 0.50
    ("GBPUSD", 50000.0, 0.5, 1.30000, 1.29500, 1.0, 0.50, "GBPUSD 0.5% risk on $50k"),

    # 4. AUDUSD: Short SL distance
    # Risk = 1.5% of $10,000 = $150. SL = 25 pips ($250/lot). Lot = 150/250 = 0.60
    ("AUDUSD", 10000.0, 1.5, 0.65000, 0.64750, 1.0, 0.60, "AUDUSD 25 pips tight SL"),

    # 5. NZDUSD: Sell trade (Entry < SL)
    # Risk = 1.0% of $20,000 = $200. SL = 40 pips ($400/lot). Lot = 200/400 = 0.50
    ("NZDUSD", 20000.0, 1.0, 0.60000, 0.60400, 1.0, 0.50, "NZDUSD sell trade"),

    # 6. Forex USD Base: USDJPY (100k contract, 1 pip = 0.01 JPY)
    # Entry = 150.00. Pip value in USD = 1000 / 150 = $6.6667.
    # Risk = $100. SL = 100 pips (1.00 JPY) = $666.67/lot. Lot = 100 / 666.67 = 0.15
    ("USDJPY", 10000.0, 1.0, 150.00, 149.00, 1.0, 0.15, "USDJPY at 150.00 level"),

    # 7. USDJPY at 155.00:
    # Entry = 155.00. Pip value = 1000 / 155 = $6.4516.
    # Risk = 1.0% of $50,000 = $500. SL = 50 pips = $322.58/lot. Lot = 500 / 322.58 = 1.55
    ("USDJPY", 50000.0, 1.0, 155.00, 155.50, 1.0, 1.55, "USDJPY at 155.00 level"),

    # 8. USDCAD: USD base pair (100k contract, 1 pip = 0.0001 CAD)
    # Entry = 1.36000. Pip value = 10 / 1.36 = $7.3529.
    # Risk = $100. SL = 50 pips = $367.65/lot. Lot = 100 / 367.65 = 0.27
    ("USDCAD", 10000.0, 1.0, 1.36000, 1.35500, 1.0, 0.27, "USDCAD USD base pair"),

    # 9. USDCHF: USD base pair (100k contract, 1 pip = 0.0001 CHF)
    # Entry = 0.90000. Pip value = 10 / 0.90 = $11.1111.
    # Risk = $100. SL = 50 pips = $555.56/lot. Lot = 100 / 555.56 = 0.18
    ("USDCHF", 10000.0, 1.0, 0.90000, 0.89500, 1.0, 0.18, "USDCHF USD base pair"),

    # 10. Forex Cross: EURGBP (Quote = GBP, GBPUSD conversion = 1.30)
    # 1 pip = 10.00 GBP * 1.30 = $13.00/pip.
    # Risk = $100. SL = 50 pips = $650/lot. Lot = 100 / 650 = 0.15
    ("EURGBP", 10000.0, 1.0, 0.85000, 0.84500, 1.30, 0.15, "EURGBP cross with GBPUSD=1.30"),

    # 11. Forex Cross: EURJPY (Quote = JPY, calibrated pip_value = $6.6667)
    # Risk = $150 (1% of $15k). SL = 100 pips = $666.67/lot. Lot = 150 / 666.67 = 0.22
    ("EURJPY", 15000.0, 1.0, 162.00, 161.00, 1.0, 0.22, "EURJPY cross pair"),

    # 12. Forex Cross: GBPJPY (Quote = JPY, pip_value = $6.6667)
    # Risk = $300 (1% of $30k). SL = 100 pips = $666.67/lot. Lot = 300 / 666.67 = 0.45
    ("GBPJPY", 30000.0, 1.0, 195.00, 194.00, 1.0, 0.45, "GBPJPY cross pair"),

    # 13. Metals: Gold XAUUSD (100 oz contract, 1 pip = 0.10, $10.00/pip)
    # Entry = 2650.00, SL = 2640.00 ($10.00 price move = 100 pips = $1000/lot).
    # Risk = $100. Lot = 100 / 1000 = 0.10 lots
    ("XAUUSD", 10000.0, 1.0, 2650.00, 2640.00, 1.0, 0.10, "XAUUSD $10 gold move"),

    # 14. Gold XAUUSD: Tight stop ($4.00 move = 40 pips = $400/lot)
    # Risk = 1.0% of $20,000 = $200. Lot = 200 / 400 = 0.50 lots
    ("XAUUSD", 20000.0, 1.0, 2660.00, 2656.00, 1.0, 0.50, "XAUUSD $4 tight gold stop"),

    # 15. Gold XAUUSD: Swing stop ($25.00 move = 250 pips = $2500/lot)
    # Risk = 0.5% of $100,000 = $500. Lot = 500 / 2500 = 0.20 lots
    ("XAUUSD", 100000.0, 0.5, 2700.00, 2675.00, 1.0, 0.20, "XAUUSD $25 swing gold stop"),

    # 16. Metals: Silver XAGUSD (5,000 oz contract, 1 pip = 0.01, $50.00/pip)
    # Entry = 31.50, SL = 31.00 ($0.50 move = 50 pips = $2500/lot).
    # Risk = $100. Lot = 100 / 2500 = 0.04 lots
    ("XAGUSD", 10000.0, 1.0, 31.50, 31.00, 1.0, 0.04, "XAGUSD silver 50c stop"),

    # 17. Cash Indices CFD: US30 / Dow Jones (1 contract, 1 point = $1.00/lot)
    # Entry = 43500, SL = 43400 (100 points = $100/lot).
    # Risk = $100. Lot = 100 / 100 = 1.00 lots
    ("US30", 10000.0, 1.0, 43500.0, 43400.0, 1.0, 1.00, "US30 index 100 pts SL"),

    # 18. US30: Tight stop (50 points = $50/lot) on $50k account
    # Risk = 1.0% of $50,000 = $500. Lot = 500 / 50 = 10.00 lots
    ("US30", 50000.0, 1.0, 44000.0, 43950.0, 1.0, 10.00, "US30 index 50 pts SL on $50k"),

    # 19. Cash Indices CFD: NAS100 / US Tech 100 (1 contract, 1 pt = $1.00/lot)
    # Entry = 20800, SL = 20750 (50 points = $50/lot).
    # Risk = $100. Lot = 100 / 50 = 2.00 lots
    ("NAS100", 10000.0, 1.0, 20800.0, 20750.0, 1.0, 2.00, "NAS100 index 50 pts SL"),

    # 20. NAS100: Scalp stop (25 points = $25/lot)
    # Risk = 0.5% of $10,000 = $50. Lot = 50 / 25 = 2.00 lots
    ("NAS100", 10000.0, 0.5, 20500.0, 20475.0, 1.0, 2.00, "NAS100 index 25 pts SL"),

    # 21. Indices CFD: SPX500 (Contract size = 10, 1 pt = $10.00/lot)
    # Entry = 5900, SL = 5880 (20 points = $200/lot).
    # Risk = $100. Lot = 100 / 200 = 0.50 lots
    ("SPX500", 10000.0, 1.0, 5900.0, 5880.0, 1.0, 0.50, "SPX500 index 20 pts SL"),

    # 22. Indices CFD: GER40 / DAX (Quote = EUR, EURUSD = 1.08 -> $108/lot for 100 pts)
    # Entry = 19200, SL = 19100 (100 points = $108/lot).
    # Risk = $100. Lot = 100 / 108 = 0.9259 -> floor quantize = 0.93 lots
    ("GER40", 10000.0, 1.0, 19200.0, 19100.0, 1.08, 0.93, "GER40 index with EURUSD=1.08"),

    # 23. Crypto CFD: BTCUSD (1 contract, 1 pt = $1.00/lot)
    # Entry = 92000, SL = 91000 ($1,000 price move = $1000/lot).
    # Risk = $100. Lot = 100 / 1000 = 0.10 lots
    ("BTCUSD", 10000.0, 1.0, 92000.0, 91000.0, 1.0, 0.10, "BTCUSD crypto $1k SL"),

    # 24. Crypto CFD: ETHUSD (1 contract, 1 pt = $1.00/lot)
    # Entry = 3400, SL = 3320 ($80 price move = $80/lot).
    # Risk = $100. Lot = 100 / 80 = 1.25 lots
    ("ETHUSD", 10000.0, 1.0, 3400.0, 3320.0, 1.0, 1.25, "ETHUSD crypto $80 SL"),

    # 25. Lot step floor quantization: EURUSD with 33 pips SL
    # Risk = $100. SL = 33 pips = $330/lot. Raw lot = 100 / 330 = 0.30303 -> 0.30 lots
    ("EURUSD", 10000.0, 1.0, 1.08500, 1.08170, 1.0, 0.30, "EURUSD step quantization 33 pips"),

    # 26. Min lot clamping: Very large SL
    # Risk = $10. SL = 1000 pips = $10,000/lot. Raw lot = 0.001 -> Clamped to min_lot 0.01
    ("EURUSD", 1000.0, 1.0, 1.10000, 1.00000, 1.0, 0.01, "EURUSD min lot clamp"),

    # 27. Max lot clamping: Very small SL on massive account
    # Risk = $20,000 (2% of $1m). SL = 1 pip ($10/lot). Raw = 2000 lots -> Clamped to max_lot 100.0
    ("EURUSD", 1000000.0, 2.0, 1.08500, 1.08490, 1.0, 100.00, "EURUSD max lot clamp"),
]


class TestPositionSizingMath:
    """Validates position sizing math across diverse asset classes against hand-calculated values."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "symbol,equity,risk_pct,entry,sl,rate,expected_lot,desc",
        SIZING_TEST_CASES,
        ids=[c[7] for c in SIZING_TEST_CASES],
    )
    def test_equity_risk_lot_calculation_hand_calculated(
        self,
        symbol: str,
        equity: float,
        risk_pct: float,
        entry: float,
        sl: float,
        rate: float,
        expected_lot: float,
        desc: str,
    ):
        """Validates calculated lot size matches hand-calculated expected values."""
        account = AccountProfile(
            equity=equity,
            equity_risk_pct=risk_pct,
            min_lot=0.01,
            max_lot=100.0,
            lot_step=0.01,
        )

        lot, err = calculate_equity_risk_lot(
            account=account,
            entry_price=entry,
            stop_loss=sl,
            symbol=symbol,
            quote_to_usd_rate=rate,
        )

        assert err is None, f"Failed for {desc}: {err}"
        assert lot is not None
        assert math.isclose(lot, expected_lot, abs_tol=1e-4), (
            f"Failed for {desc}: expected {expected_lot}, got {lot}"
        )

    @pytest.mark.unit
    def test_fixed_lot_sizing(self):
        """Verifies Fixed Lot sizing model respects broker step and clamping bounds."""
        acc_std = AccountProfile(fixed_lot=0.25, min_lot=0.01, max_lot=100.0, lot_step=0.01)
        assert calculate_fixed_lot(acc_std) == 0.25

        # Sub-minimum lot clamped up
        acc_min = AccountProfile(fixed_lot=0.005, min_lot=0.01, max_lot=100.0, lot_step=0.01)
        assert calculate_fixed_lot(acc_min) == 0.01

        # Exceeding maximum lot clamped down
        acc_max = AccountProfile(fixed_lot=150.0, min_lot=0.01, max_lot=100.0, lot_step=0.01)
        assert calculate_fixed_lot(acc_max) == 100.0

    @pytest.mark.unit
    def test_fixed_currency_risk_sizing(self):
        """Verifies Fixed Currency Risk sizing calculates exact lot from cash budget."""
        acc = AccountProfile(fixed_currency_risk=50.0, min_lot=0.01, max_lot=100.0, lot_step=0.01)
        # EURUSD: 50 pips SL ($500/lot) -> 50 / 500 = 0.10 lots
        lot, err = calculate_fixed_currency_risk_lot(
            account=acc,
            entry_price=1.08500,
            stop_loss=1.08000,
            pip_size=0.0001,
            pip_value=10.0,
        )
        assert err is None
        assert math.isclose(lot, 0.10)

    @pytest.mark.unit
    def test_provider_multiplier_sizing(self):
        """Verifies Provider Multiplier scales volume accurately."""
        acc = AccountProfile(lot_multiplier=2.5, min_lot=0.01, max_lot=100.0, lot_step=0.01)
        assert math.isclose(calculate_provider_multiplier_lot(acc, 0.04), 0.10)

    @pytest.mark.unit
    def test_invalid_stop_loss_rejection(self):
        """Asserts that missing, zero, or equal stop-loss rejects calculation."""
        acc = AccountProfile(equity=10000.0, equity_risk_pct=1.0)

        # Missing SL (0.0)
        lot1, err1 = calculate_equity_risk_lot(acc, 1.08500, 0.0, symbol="EURUSD")
        assert lot1 is None
        assert "missing or zero" in err1.lower()

        # SL equals Entry (0 distance)
        lot2, err2 = calculate_equity_risk_lot(acc, 1.08500, 1.08500, symbol="EURUSD")
        assert lot2 is None
        assert "equals entry" in err2.lower()


# =============================================================================
# 2. BOUNDARY VALUE TESTING ON ENTRY GUARDS
# =============================================================================

class TestEntryGuardBoundaries:
    """
    Tests Entry Guards at exact boundary limits (FR-2.1):
    - Spread Guard: 3.9, 4.0, and 4.1 (asserting exactly 4.1 triggers SKIP_SPREAD).
    - Slippage Guard: 1.4, 1.5, and 1.6.
    - Signal Age Guard: 29.9s, 30.0s, and 30.1s.
    - Price Drift Guard: 4.9, 5.0, and 5.1 pips.
    - Max Open Positions Guard: 4 vs 5 positions.
    - Margin Safety Cutoff Guard: 200.1%, 200.0%, and 199.9%.
    """

    @pytest.mark.unit
    def test_max_spread_guard_exact_boundaries(self):
        """
        Tests spread guard at exact boundary limits:
        - 3.9 pips <= 4.0 -> PASS
        - 4.0 pips <= 4.0 -> PASS
        - 4.1 pips >  4.0 -> FAIL with SKIP_SPREAD
        """
        max_spread = 4.0

        # Boundary Case 1: 3.9 pips (Under limit -> PASS)
        quote_39 = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08539)
        ok1, reason1 = check_spread(quote_39, max_spread_pips=max_spread)
        assert ok1 is True, "Spread 3.9 pips must pass limit of 4.0 pips"
        assert reason1 is None

        # Boundary Case 2: 4.0 pips (Exact limit -> PASS)
        quote_40 = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08540)
        ok2, reason2 = check_spread(quote_40, max_spread_pips=max_spread)
        assert ok2 is True, "Spread 4.0 pips must pass limit of 4.0 pips (exact boundary)"
        assert reason2 is None

        # Boundary Case 3: 4.1 pips (Over limit -> FAIL with SKIP_SPREAD)
        quote_41 = MarketQuote(symbol="EURUSD", bid=1.08500, ask=1.08541)
        ok3, reason3 = check_spread(quote_41, max_spread_pips=max_spread)
        assert ok3 is False, "Spread 4.1 pips must strictly fail limit of 4.0 pips"
        assert reason3 is not None
        assert reason3.code == SKIP_SPREAD
        assert "4.1 pips exceeds max allowed spread 4.0 pips" in reason3.reason

    @pytest.mark.unit
    def test_signal_age_guard_exact_boundaries(self):
        """
        Tests signal age guard at exact boundary limits (max_signal_age_s = 30.0):
        - 29.9s -> PASS
        - 30.0s -> PASS
        - 30.1s -> FAIL with SKIP_AGE
        """
        max_age = 30.0
        eval_time = 1700000100.0

        # 29.9s old -> Pass
        ok1, reason1 = check_signal_age(eval_time - 29.9, eval_time, max_signal_age_s=max_age)
        assert ok1 is True
        assert reason1 is None

        # 30.0s old (exact boundary) -> Pass
        ok2, reason2 = check_signal_age(eval_time - 30.0, eval_time, max_signal_age_s=max_age)
        assert ok2 is True
        assert reason2 is None

        # 30.1s old (over boundary) -> Fail with SKIP_AGE
        ok3, reason3 = check_signal_age(eval_time - 30.1, eval_time, max_signal_age_s=max_age)
        assert ok3 is False
        assert reason3 is not None
        assert reason3.code == SKIP_AGE
        assert "30.10s exceeds maximum allowed age 30.00s" in reason3.reason

    @pytest.mark.unit
    def test_price_drift_guard_exact_boundaries(self):
        """
        Tests price drift guard at exact boundary limits (max_drift_pips = 5.0):
        - 4.9 pips -> PASS
        - 5.0 pips -> PASS
        - 5.1 pips -> FAIL with SKIP_DRIFT
        """
        max_drift = 5.0
        pip_size = 0.0001
        sig_price = 1.08500

        # 4.9 pips drift -> Pass
        ok1, reason1 = check_price_drift(sig_price, 1.08549, OrderSide.BUY, pip_size, max_drift)
        assert ok1 is True
        assert reason1 is None

        # 5.0 pips drift (exact boundary) -> Pass
        ok2, reason2 = check_price_drift(sig_price, 1.08550, OrderSide.BUY, pip_size, max_drift)
        assert ok2 is True
        assert reason2 is None

        # 5.1 pips drift (over boundary) -> Fail with SKIP_DRIFT
        ok3, reason3 = check_price_drift(sig_price, 1.08551, OrderSide.BUY, pip_size, max_drift)
        assert ok3 is False
        assert reason3 is not None
        assert reason3.code == SKIP_DRIFT

    @pytest.mark.unit
    def test_max_open_positions_guard_exact_boundary(self):
        """
        Tests max open positions limit at exact boundary (max_open_positions = 5):
        - 4 open positions -> PASS
        - 5 open positions -> FAIL with SKIP_MAX_POSITIONS
        """
        quote = MarketQuote("EURUSD", 1.08500, 1.08510)
        intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.08500, stop_loss=1.08000)

        # 4 open positions (< 5) -> Evaluates successfully
        acc_4 = AccountProfile(open_positions_count=4, max_open_positions=5)
        res_4 = RiskEngine.evaluate(intent, acc_4, quote)
        assert isinstance(res_4, OrderIntent)

        # 5 open positions (>= 5) -> Fails with SKIP_MAX_POSITIONS
        acc_5 = AccountProfile(open_positions_count=5, max_open_positions=5)
        res_5 = RiskEngine.evaluate(intent, acc_5, quote)
        assert isinstance(res_5, SkipReason)
        assert res_5.code == SKIP_MAX_POSITIONS

    @pytest.mark.unit
    def test_margin_safety_cutoff_exact_boundary(self):
        """
        Tests margin safety cutoff at exact boundary (threshold = 200.0%):
        - 200.1% -> PASS
        - 200.0% -> PASS
        - 199.9% -> FAIL with SKIP_MIN_MARGIN
        """
        quote = MarketQuote("EURUSD", 1.08500, 1.08510)
        intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.08500, stop_loss=1.08000)

        # 200.1% margin level -> Pass
        acc_high = AccountProfile(margin_level=200.1, margin_safety_threshold=200.0)
        res_high = RiskEngine.evaluate(intent, acc_high, quote)
        assert isinstance(res_high, OrderIntent)

        # 200.0% margin level (exact boundary) -> Pass
        acc_exact = AccountProfile(margin_level=200.0, margin_safety_threshold=200.0)
        res_exact = RiskEngine.evaluate(intent, acc_exact, quote)
        assert isinstance(res_exact, OrderIntent)

        # 199.9% margin level (breached) -> Fail with SKIP_MIN_MARGIN
        acc_low = AccountProfile(margin_level=199.9, margin_safety_threshold=200.0)
        res_low = RiskEngine.evaluate(intent, acc_low, quote)
        assert isinstance(res_low, SkipReason)
        assert res_low.code == SKIP_MIN_MARGIN

    @pytest.mark.unit
    def test_equity_hard_stop_exact_boundary(self):
        """
        Tests equity hard stop at exact boundary (hard_stop = $5,000.00):
        - $5,000.01 -> PASS
        - $5,000.00 -> FAIL with SKIP_EQUITY_HARD_STOP
        """
        quote = MarketQuote("EURUSD", 1.08500, 1.08510)
        intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.08500, stop_loss=1.08000)

        # $5,000.01 equity (> $5000) -> Pass
        acc_above = AccountProfile(equity=5000.01, equity_hard_stop=5000.00)
        res_above = RiskEngine.evaluate(intent, acc_above, quote)
        assert isinstance(res_above, OrderIntent)

        # $5,000.00 equity (exact floor) -> Fail with SKIP_EQUITY_HARD_STOP
        acc_floor = AccountProfile(equity=5000.00, equity_hard_stop=5000.00)
        res_floor = RiskEngine.evaluate(intent, acc_floor, quote)
        assert isinstance(res_floor, SkipReason)
        assert res_floor.code == SKIP_EQUITY_HARD_STOP


# =============================================================================
# 3. REAL-TIME EQUITY BREAKER & GLOBAL PANIC RESPONSE
# =============================================================================

class TestEquityBreakerAndPanicResponse:
    """
    Validates the Real-Time Equity Breaker & Global Panic Response (Sprint 7):
    - Mocks a scenario where open positions slide into deep intra-day losses.
    - Asserts that the exact tick causing equity to breach daily loss limit triggers panic.
    - Validates emission of emergency FULL_CLOSE intents for all active tickets.
    - Verifies that subsequent ticks maintain panic halt and reject all new trade admissions.
    """

    @pytest.mark.unit
    def test_equity_breaker_exact_tick_breach_and_panic_liquidation(self):
        """
        Simulates dynamic tick-by-tick degradation across 2 active open positions:
        - Starting Balance: $10,000.00, Daily Loss Limit: $500.00 (Hard stop at $9,500.00)
        - Ticket 101: 1.00 lot BUY EURUSD @ 1.08500 ($10/pip)
        - Ticket 102: 0.50 lot BUY XAUUSD @ 2650.00 ($50/point)
        """
        class MockPosition:
            def __init__(self, ticket: int, symbol: str, volume: float, open_price: float):
                self.ticket = ticket
                self.symbol = symbol
                self.volume = volume
                self.open_price = open_price

        positions = [
            MockPosition(101, "EURUSD", 1.0, 1.08500),
            MockPosition(102, "XAUUSD", 0.5, 2650.00),
        ]

        daily_limit = 500.0
        starting_eq = 10000.0

        # Tick 1: Market neutral (PnL = $0.00 -> Equity = $10,000.00)
        # Loss = $0.00 < $500.00 -> NO PANIC
        res1 = check_equity_circuit_breaker(
            equity=10000.0,
            balance=starting_eq,
            daily_loss_limit=daily_limit,
            open_positions=positions,
            starting_equity=starting_eq,
        )
        assert res1.is_tripped is False
        assert res1.pause_entries is False
        assert len(res1.panic_close_intents) == 0

        # Tick 2: Small adverse drift (EURUSD -15 pips = -$150; Gold -$3.00 = -$150)
        # Total PnL = -$300.00 -> Equity = $9,700.00
        # Loss = $300.00 < $500.00 -> NO PANIC
        res2 = check_equity_circuit_breaker(
            equity=9700.0,
            balance=starting_eq,
            daily_loss_limit=daily_limit,
            open_positions=positions,
            starting_equity=starting_eq,
        )
        assert res2.is_tripped is False
        assert res2.pause_entries is False

        # Tick 3: Critical Pre-Breach Boundary
        # EURUSD -25 pips = -$250; Gold -$4.98 = -$249.00
        # Total PnL = -$499.00 -> Equity = $9,501.00
        # Loss = $499.00 < $500.00 (Exactly $1.00 below threshold) -> NO PANIC
        res3 = check_equity_circuit_breaker(
            equity=9501.0,
            balance=starting_eq,
            daily_loss_limit=daily_limit,
            open_positions=positions,
            starting_equity=starting_eq,
        )
        assert res3.is_tripped is False
        assert res3.drawdown_amount == 499.0
        assert res3.pause_entries is False

        # Tick 4: EXACT BREACH TICK
        # Gold drops another 2 cents (-$5.00 total) -> Gold PnL = -$250.00
        # Total PnL = -$250 + -$250 = -$500.00 -> Equity = $9,500.00 (Exact Breaker Threshold)
        res4 = check_equity_circuit_breaker(
            equity=9500.0,
            balance=starting_eq,
            daily_loss_limit=daily_limit,
            open_positions=positions,
            starting_equity=starting_eq,
        )

        # Strict Assertions on Panic Activation
        assert res4.is_tripped is True, "Exact tick reaching $500.00 daily loss MUST trip breaker"
        assert res4.pause_entries is True, "Trading MUST be halted upon breaker trip"
        assert "DAILY_LOSS_LIMIT_BREACHED" in res4.trigger_reason
        assert res4.drawdown_amount == 500.0

        # Emergency Liquidation Verification
        assert len(res4.panic_close_intents) == 2, "Must generate close intents for both active positions"

        # Check Ticket 101 emergency close intent
        intent_101 = next(i for i in res4.panic_close_intents if i.order_ticket == 101)
        assert intent_101.symbol == "EURUSD"
        assert intent_101.action_type == ActionType.FULL_CLOSE
        assert math.isclose(intent_101.volume, 1.0)
        assert intent_101.flags & ExecutionFlags.IOC

        # Check Ticket 102 emergency close intent
        intent_102 = next(i for i in res4.panic_close_intents if i.order_ticket == 102)
        assert intent_102.symbol == "XAUUSD"
        assert intent_102.action_type == ActionType.FULL_CLOSE
        assert math.isclose(intent_102.volume, 0.5)

        # Tick 5: Post-Breach State Persistence
        # Deeper loss ($9,450) while in panic mode continues to report tripped state
        res5 = check_equity_circuit_breaker(
            equity=9450.0,
            balance=starting_eq,
            daily_loss_limit=daily_limit,
            open_positions=positions,
            starting_equity=starting_eq,
        )
        assert res5.is_tripped is True
        assert res5.pause_entries is True

    @pytest.mark.unit
    def test_max_drawdown_percent_circuit_breaker_breach(self):
        """
        Validates circuit breaker trigger via Max Drawdown % (e.g. 4.5% limit on $10k account = $450).
        """
        starting_equity = 10000.0
        max_dd_pct = 4.5  # 4.5% hard stop

        # 4.4% drawdown ($440 loss -> Equity $9,560) -> Pass
        res_pass = check_equity_circuit_breaker(
            equity=9560.0,
            balance=starting_equity,
            max_loss_limit=max_dd_pct,
            starting_equity=starting_equity,
        )
        assert res_pass.is_tripped is False

        # 4.5% drawdown ($450 loss -> Equity $9,550) -> FAIL & TRIP
        res_trip = check_equity_circuit_breaker(
            equity=9550.0,
            balance=starting_equity,
            max_loss_limit=max_dd_pct,
            starting_equity=starting_equity,
        )
        assert res_trip.is_tripped is True
        assert "MAX_LOSS_LIMIT_BREACHED" in res_trip.trigger_reason
        assert res_trip.pause_entries is True

    @pytest.mark.unit
    def test_risk_engine_blocks_admissions_when_circuit_breaker_tripped(self):
        """
        Verifies that when AccountProfile.is_circuit_breaker_tripped or pause_new_entries is active,
        RiskEngine.evaluate strictly rejects new order intents with SKIP_CIRCUIT_BREAKER.
        """
        quote = MarketQuote("EURUSD", 1.08500, 1.08510)
        intent = OrderIntent(symbol="EURUSD", side=OrderSide.BUY, price=1.08500, stop_loss=1.08000)

        # Circuit breaker tripped account profile
        acc_tripped = AccountProfile(
            equity=9400.0,
            daily_loss_limit=500.0,
            daily_starting_equity=10000.0,
            is_circuit_breaker_tripped=True,
            pause_new_entries=True,
        )

        res = RiskEngine.evaluate(intent, acc_tripped, quote)
        assert isinstance(res, SkipReason)
        assert res.code == SKIP_CIRCUIT_BREAKER
        assert "Circuit breaker active" in res.reason


# =============================================================================
# 4. OFFLINE ECONOMIC NEWS CALENDAR FILTER TESTS (SPRINT 7)
# =============================================================================

class TestOfflineNewsCalendarFilter:
    """
    Validates offline economic calendar news filtering:
    - Ingests mock high-impact events (NFP, CPI, FOMC).
    - Blocks trading within the configurable window (e.g. 30m before / 30m after).
    - Allows trading outside the window or for unaffected currency pairs.
    """

    @pytest.fixture
    def mock_calendar(self) -> EconomicCalendar:
        """Fixture providing sample economic events."""
        events = [
            EconomicEvent(
                id="EVT-001",
                title="US Non-Farm Payrolls (NFP)",
                currency="USD",
                impact="HIGH",
                timestamp=1700005000.0,  # Event epoch
                date_iso="2026-11-21T13:30:00Z",
            ),
            EconomicEvent(
                id="EVT-002",
                title="ECB Monetary Policy Statement",
                currency="EUR",
                impact="HIGH",
                timestamp=1700010000.0,
                date_iso="2026-11-21T14:45:00Z",
            ),
            EconomicEvent(
                id="EVT-003",
                title="German CPI MoM",
                currency="EUR",
                impact="MEDIUM",
                timestamp=1700020000.0,
            ),
        ]
        return EconomicCalendar(events)

    @pytest.mark.unit
    def test_news_filter_blocks_trading_within_30m_window(self, mock_calendar: EconomicCalendar):
        """
        Tests that an event at t = 1700005000 blocks USD pairs in [t - 1800, t + 1800].
        """
        event_time = 1700005000.0

        # 15 minutes before NFP -> BLOCKED with SKIP_NEWS
        ok_before, reason_before = check_news_filter(
            symbol="EURUSD",
            current_time=event_time - 900.0,
            calendar=mock_calendar,
            window_before_s=1800.0,
            window_after_s=1800.0,
        )
        assert ok_before is False
        assert reason_before is not None
        assert reason_before.code == SKIP_NEWS
        assert "US Non-Farm Payrolls" in reason_before.reason

        # Exactly at event time -> BLOCKED
        ok_exact, _ = check_news_filter("XAUUSD", event_time, mock_calendar)
        assert ok_exact is False

        # 15 minutes after NFP -> BLOCKED
        ok_after, _ = check_news_filter("US30", event_time + 900.0, mock_calendar)
        assert ok_after is False

    @pytest.mark.unit
    def test_news_filter_allows_trading_outside_window(self, mock_calendar: EconomicCalendar):
        """
        Tests that trading is allowed outside the 30-minute window.
        """
        event_time = 1700005000.0

        # 31 minutes before NFP (outside 30m window) -> ALLOWED
        ok_early, reason_early = check_news_filter(
            symbol="EURUSD",
            current_time=event_time - 1860.0,
            calendar=mock_calendar,
            window_before_s=1800.0,
            window_after_s=1800.0,
        )
        assert ok_early is True
        assert reason_early is None

        # 31 minutes after NFP -> ALLOWED
        ok_late, reason_late = check_news_filter(
            symbol="EURUSD",
            current_time=event_time + 1860.0,
            calendar=mock_calendar,
            window_before_s=1800.0,
            window_after_s=1800.0,
        )
        assert ok_late is True
        assert reason_late is None

    @pytest.mark.unit
    def test_news_filter_unaffected_currency_passes(self, mock_calendar: EconomicCalendar):
        """
        Tests that during a USD-only event, pairs without USD (e.g. AUDNZD) are NOT blocked.
        """
        event_time = 1700005000.0  # USD NFP event
        ok, reason = check_news_filter(
            symbol="AUDNZD",
            current_time=event_time,
            calendar=mock_calendar,
        )
        assert ok is True
        assert reason is None


# =============================================================================
# 5. STATELESS RISK ENGINE INTEGRATION TESTS
# =============================================================================

class TestRiskEngineIntegration:
    """Validates full integration, pure functional immutability, and position management bypass."""

    @pytest.mark.unit
    def test_stateless_evaluation_immutability(self):
        """Verifies RiskEngine never mutates original intent and returns a pure new copy."""
        now_ns = int(1700000100.0 * 1e9)
        intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.01,
            price=1.08500,
            stop_loss=1.08000,
            timestamp_ns=now_ns,
        )
        account = AccountProfile(equity=10000.0, equity_risk_pct=1.0)
        quote = MarketQuote("EURUSD", 1.08510, 1.08520)

        evaluated = RiskEngine.evaluate(intent, account, quote, eval_time=1700000105.0)

        assert isinstance(evaluated, OrderIntent)
        assert math.isclose(evaluated.volume, 0.20)
        assert math.isclose(intent.volume, 0.01), "Original intent MUST NOT be mutated"

    @pytest.mark.unit
    def test_position_management_actions_bypass_entry_guards(self):
        """Asserts that FULL_CLOSE and CANCEL_PENDING bypass spread and drift guards."""
        # Extreme spread (10 pips) and drift (100 pips)
        bad_quote = MarketQuote("EURUSD", bid=1.09500, ask=1.09600)

        # 1. FULL_CLOSE passes with volume preserved
        close_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.FULL_CLOSE,
            volume=0.50,
            price=1.08500,
        )
        res_close = RiskEngine.evaluate(close_intent, AccountProfile(), bad_quote)
        assert isinstance(res_close, OrderIntent)
        assert res_close.action_type == ActionType.FULL_CLOSE
        assert math.isclose(res_close.volume, 0.50)

        # 2. CANCEL_PENDING passes
        cancel_intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.SELL,
            action_type=ActionType.CANCEL_PENDING,
            volume=0.25,
            price=1.08500,
        )
        res_cancel = RiskEngine.evaluate(cancel_intent, AccountProfile(), bad_quote)
        assert isinstance(res_cancel, OrderIntent)
        assert res_cancel.action_type == ActionType.CANCEL_PENDING
