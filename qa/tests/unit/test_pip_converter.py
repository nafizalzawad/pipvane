"""
Pipvane v3 - Unit Test Suite: PipConverter & RiskEngine Math Engine (Sprint 19 Directive).
==========================================================================================
Validates:
1. Asset Classification: Standard Forex, JPY Pair, Metals (Gold/Silver), Indices.
2. Tick & Point Math: Query broker for _digits and _point size, translating raw integer pips
   (e.g., "50 pips") into exact price offsets (e.g. 0.0050 for EURUSD, 0.50 for USDJPY, 5.0 for Gold).
3. RiskEngine Integration: Utilizes PipConverter for all SL, TP, and Trailing Stop calculations.
4. Decimal Precision: 5th decimal place (0.00001) zero floating point drift.
"""

from __future__ import annotations

import unittest
from decimal import Decimal
from typing import Dict, Any

from engine.symbol_mapper import (
    AssetClass,
    BrokerSymbolSpec,
    PipConverter,
    SymbolMapper,
    default_pip_converter,
    CANONICAL_ASSET_ALIASES,
)
from engine.risk_engine import (
    RiskEngine,
    AccountProfile,
    MarketQuote,
    RiskModel,
    calculate_sl,
    calculate_tp,
    calculate_trailing_stop,
    pips_to_price_offset,
    price_offset_to_pips,
    classify_asset,
    get_default_pip_size,
)
from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
)


class TestAssetClassification(unittest.TestCase):
    """Task 1: Asset Classification into Standard Forex, JPY Pair, Metals, Indices."""

    def setUp(self) -> None:
        self.converter = PipConverter()
        self.mapper = SymbolMapper(broker_symbols=[
            "EURUSD.pro", "USDJPY.m", "GOLD#", "US30.cash", "USTEC.c", "XAGUSD.raw"
        ])

    def test_01_standard_forex_classification(self) -> None:
        """Classifies major and cross forex pairs without JPY as Standard Forex."""
        fx_pairs = [
            "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
            "EURGBP", "EURAUD", "GBPAUD", "EURCAD", "AUDCAD", "NZDCAD",
            "EURUSD.pro", "fx.GBPUSD", "AUDUSD_i", "USDSEK", "USDNOK",
        ]
        for sym in fx_pairs:
            classified = self.converter.classify_asset(sym)
            self.assertEqual(
                classified,
                AssetClass.STANDARD_FOREX,
                f"Expected {sym} to be classified as STANDARD_FOREX, got {classified}",
            )
            self.assertEqual(classified.value, "Standard Forex")

    def test_02_jpy_pair_classification(self) -> None:
        """Classifies all JPY pairs as JPY Pair."""
        jpy_pairs = [
            "USDJPY", "GBPJPY", "EURJPY", "AUDJPY", "CADJPY", "NZDJPY", "CHFJPY",
            "USDJPY.m", "fx.GBPJPY", "EURJPY_raw",
        ]
        for sym in jpy_pairs:
            classified = self.converter.classify_asset(sym)
            self.assertEqual(
                classified,
                AssetClass.JPY_PAIR,
                f"Expected {sym} to be classified as JPY_PAIR, got {classified}",
            )
            self.assertEqual(classified.value, "JPY Pair")

    def test_03_metals_classification(self) -> None:
        """Classifies Gold and Silver as Metals (Gold/Silver)."""
        metals = [
            "XAUUSD", "GOLD", "GOLD#", "XAUUSD.pro", "XAU_USD",
            "XAGUSD", "SILVER", "SILVER#", "XAGUSD.raw",
        ]
        for sym in metals:
            classified = self.converter.classify_asset(sym)
            self.assertEqual(
                classified,
                AssetClass.METALS,
                f"Expected {sym} to be classified as METALS, got {classified}",
            )
            self.assertEqual(classified.value, "Metals (Gold/Silver)")

    def test_04_indices_classification(self) -> None:
        """Classifies Dow Jones, Nasdaq, S&P, DAX, FTSE as Indices."""
        indices = [
            "US30", "DJ30", "WS30", "WALLSTREET", "US30.cash",
            "NAS100", "USTEC", "US100", "NDX", "USTEC.c",
            "SPX500", "US500", "SP500",
            "GER40", "DAX40", "GER30", "DAX",
            "UK100", "FTSE", "JP225", "NIKKEI",
        ]
        for sym in indices:
            classified = self.converter.classify_asset(sym)
            self.assertEqual(
                classified,
                AssetClass.INDICES,
                f"Expected {sym} to be classified as INDICES, got {classified}",
            )
            self.assertEqual(classified.value, "Indices")

    def test_05_symbol_mapper_populates_asset_class(self) -> None:
        """When SymbolMapper resolves a symbol, it populates asset_class."""
        res_eur = self.mapper.resolve("EURUSD")
        self.assertEqual(res_eur.asset_class, AssetClass.STANDARD_FOREX)

        res_jpy = self.mapper.resolve("USDJPY")
        self.assertEqual(res_jpy.asset_class, AssetClass.JPY_PAIR)

        res_gold = self.mapper.resolve("GOLD")
        self.assertEqual(res_gold.asset_class, AssetClass.METALS)

        res_nas = self.mapper.resolve("NAS100")
        self.assertEqual(res_nas.asset_class, AssetClass.INDICES)


class TestTickAndPointMath(unittest.TestCase):
    """Task 2: Tick & Point Math across different broker specifications."""

    def setUp(self) -> None:
        self.converter = PipConverter()

    def test_06_standard_forex_50_pips_offset(self) -> None:
        """Standard Forex 50 pips = 0.0050 exactly."""
        # Standard 5-digit broker: digits=5, point=0.00001
        offset_5digit = self.converter.pips_to_price_offset("EURUSD", 50, digits=5, point=0.00001)
        self.assertEqual(offset_5digit, Decimal("0.00500"))

        # Standard 4-digit broker: digits=4, point=0.0001
        offset_4digit = self.converter.pips_to_price_offset("EURUSD", 50, digits=4, point=0.0001)
        self.assertEqual(offset_4digit, Decimal("0.0050"))

        # Default catalog fallback (no explicit digits/point)
        offset_default = self.converter.pips_to_price_offset("EURUSD", 50)
        self.assertEqual(offset_default, Decimal("0.0050"))

        # Reverse check: 0.0050 / pip_size = 50.0 pips
        pips_calculated = self.converter.price_offset_to_pips("EURUSD", 0.0050)
        self.assertEqual(pips_calculated, Decimal("50.00000"))

    def test_07_jpy_pair_50_pips_offset(self) -> None:
        """USDJPY 50 pips = 0.50 exactly."""
        # 3-digit broker: digits=3, point=0.001
        offset_3digit = self.converter.pips_to_price_offset("USDJPY", 50, digits=3, point=0.001)
        self.assertEqual(offset_3digit, Decimal("0.500"))

        # 2-digit broker: digits=2, point=0.01
        offset_2digit = self.converter.pips_to_price_offset("USDJPY", 50, digits=2, point=0.01)
        self.assertEqual(offset_2digit, Decimal("0.50"))

        # Default catalog fallback
        offset_default = self.converter.pips_to_price_offset("USDJPY", 50)
        self.assertEqual(offset_default, Decimal("0.50"))

        # Reverse check: 0.50 / 0.01 = 50.0 pips
        pips_calculated = self.converter.price_offset_to_pips("USDJPY", 0.50)
        self.assertEqual(pips_calculated, Decimal("50.00000"))

    def test_08_gold_50_pips_offset(self) -> None:
        """Gold (XAUUSD / GOLD) 50 pips = 5.0 exactly."""
        # 2-digit broker: digits=2, point=0.01 -> 1 pip = 0.10, 50 pips = 5.0
        offset_2digit = self.converter.pips_to_price_offset("XAUUSD", 50, digits=2, point=0.01)
        self.assertEqual(offset_2digit, Decimal("5.00"))

        # 3-digit broker: digits=3, point=0.001 -> 1 pip = 0.10, 50 pips = 5.0
        offset_3digit = self.converter.pips_to_price_offset("GOLD", 50, digits=3, point=0.001)
        self.assertEqual(offset_3digit, Decimal("5.000"))

        # Default catalog fallback
        offset_default = self.converter.pips_to_price_offset("GOLD", 50)
        self.assertEqual(offset_default, Decimal("5.00"))

        # Reverse check: 5.0 / 0.10 = 50.0 pips
        pips_calculated = self.converter.price_offset_to_pips("GOLD", 5.0)
        self.assertEqual(pips_calculated, Decimal("50.00000"))

    def test_09_silver_and_indices_offsets(self) -> None:
        """Silver and Indices point math."""
        # Silver 50 pips = 0.50
        offset_silver = self.converter.pips_to_price_offset("XAGUSD", 50)
        self.assertEqual(offset_silver, Decimal("0.50"))

        # US30 Index 50 pips = 50.0
        offset_us30 = self.converter.pips_to_price_offset("US30", 50)
        self.assertEqual(offset_us30, Decimal("50.00"))

        # SPX500 Index 50 pips = 5.0 (0.10 per pip)
        offset_spx = self.converter.pips_to_price_offset("SPX500", 50)
        self.assertEqual(offset_spx, Decimal("5.00"))

    def test_10_register_broker_spec_dynamic_lookup(self) -> None:
        """Registers broker specifications dynamically and verifies math."""
        self.converter.register_broker_spec("EURUSD.vip", digits=5, point="0.00001")
        self.converter.register_broker_spec("USDJPY.vip", digits=3, point="0.001")
        self.converter.register_broker_spec("GOLD.vip", digits=2, point="0.01")

        spec_gold = self.converter.get_broker_spec("GOLD.vip")
        self.assertIsNotNone(spec_gold)
        self.assertEqual(spec_gold.asset_class, AssetClass.METALS)
        self.assertEqual(spec_gold.pip_size, Decimal("0.10"))

        offset_gold = self.converter.pips_to_price_offset("GOLD.vip", 50)
        self.assertEqual(offset_gold, Decimal("5.00"))


class TestRiskEngineIntegration(unittest.TestCase):
    """Task 3: RiskEngine integration with PipConverter for SL, TP, and Trailing Stop."""

    def setUp(self) -> None:
        self.risk_engine = RiskEngine()

    def test_11_stop_loss_calculation_across_asset_classes(self) -> None:
        """SL price calculation for BUY and SELL orders."""
        # 1. EURUSD BUY at 1.1000 with 50 pip SL -> 1.09500
        sl_eur_buy = self.risk_engine.calculate_sl("EURUSD", 1.10000, 50, side="BUY", digits=5)
        self.assertEqual(sl_eur_buy, 1.09500)

        # 2. EURUSD SELL at 1.1000 with 50 pip SL -> 1.10500
        sl_eur_sell = self.risk_engine.calculate_sl("EURUSD", 1.10000, 50, side="SELL", digits=5)
        self.assertEqual(sl_eur_sell, 1.10500)

        # 3. USDJPY BUY at 150.000 with 50 pip SL -> 149.500
        sl_jpy_buy = self.risk_engine.calculate_sl("USDJPY", 150.000, 50, side="BUY", digits=3)
        self.assertEqual(sl_jpy_buy, 149.500)

        # 4. USDJPY SELL at 150.000 with 50 pip SL -> 150.500
        sl_jpy_sell = self.risk_engine.calculate_sl("USDJPY", 150.000, 50, side="SELL", digits=3)
        self.assertEqual(sl_jpy_sell, 150.500)

        # 5. GOLD BUY at 2000.00 with 50 pip SL -> 1995.00
        sl_gold_buy = self.risk_engine.calculate_sl("GOLD", 2000.00, 50, side="BUY", digits=2)
        self.assertEqual(sl_gold_buy, 1995.00)

        # 6. GOLD SELL at 2000.00 with 50 pip SL -> 2005.00
        sl_gold_sell = self.risk_engine.calculate_sl("GOLD", 2000.00, 50, side="SELL", digits=2)
        self.assertEqual(sl_gold_sell, 2005.00)

    def test_12_take_profit_calculation_across_asset_classes(self) -> None:
        """TP price calculation for BUY and SELL orders."""
        # EURUSD BUY at 1.1000 with 100 pip TP -> 1.11000
        tp_eur_buy = self.risk_engine.calculate_tp("EURUSD", 1.10000, 100, side="BUY", digits=5)
        self.assertEqual(tp_eur_buy, 1.11000)

        # EURUSD SELL at 1.1000 with 100 pip TP -> 1.09000
        tp_eur_sell = self.risk_engine.calculate_tp("EURUSD", 1.10000, 100, side="SELL", digits=5)
        self.assertEqual(tp_eur_sell, 1.09000)

        # GOLD BUY at 2000.00 with 100 pip TP -> 2010.00
        tp_gold_buy = self.risk_engine.calculate_tp("GOLD", 2000.00, 100, side="BUY", digits=2)
        self.assertEqual(tp_gold_buy, 2010.00)

        # GOLD SELL at 2000.00 with 100 pip TP -> 1990.00
        tp_gold_sell = self.risk_engine.calculate_tp("GOLD", 2000.00, 100, side="SELL", digits=2)
        self.assertEqual(tp_gold_sell, 1990.00)

    def test_13_trailing_stop_calculation_and_step_filter(self) -> None:
        """Trailing Stop calculation with step threshold."""
        # BUY EURUSD: Entry at 1.1000, Initial SL at 1.0950.
        # Market price rallies to 1.1080. Trailing distance is 50 pips (0.0050).
        # Proposed SL = 1.1080 - 0.0050 = 1.1030. Since 1.1030 > 1.0950, should modify!
        should_modify, new_sl = self.risk_engine.calculate_trailing_stop(
            symbol="EURUSD",
            current_price=1.10800,
            current_sl=1.09500,
            trailing_pips=50,
            side="BUY",
            step_pips=10,
            digits=5,
        )
        self.assertTrue(should_modify)
        self.assertEqual(new_sl, 1.10300)

        # If current SL is already 1.1025, and proposed is 1.1030:
        # Step is 10 pips (0.0010), so proposed (1.1030) < 1.1025 + 0.0010 (1.1035).
        # Must NOT modify to prevent spamming broker with tiny modifications!
        should_modify_step, unchanged_sl = self.risk_engine.calculate_trailing_stop(
            symbol="EURUSD",
            current_price=1.10800,
            current_sl=1.10250,
            trailing_pips=50,
            side="BUY",
            step_pips=10,
            digits=5,
        )
        self.assertFalse(should_modify_step)
        self.assertEqual(unchanged_sl, 1.10250)

        # SELL GOLD: Entry at 2000.00, Initial SL at 2005.00.
        # Market price drops to 1970.00. Trailing distance is 50 pips ($5.00).
        # Proposed SL = 1970.00 + 5.00 = 1975.00. Since 1975.00 < 2005.00, should modify!
        should_modify_gold, new_gold_sl = self.risk_engine.calculate_trailing_stop(
            symbol="GOLD",
            current_price=1970.00,
            current_sl=2005.00,
            trailing_pips=50,
            side="SELL",
            step_pips=5,
            digits=2,
        )
        self.assertTrue(should_modify_gold)
        self.assertEqual(new_gold_sl, 1975.00)

    def test_14_pip_converter_position_sizing_integration(self) -> None:
        """RiskEngine position sizing uses PipConverter pip distance."""
        account = AccountProfile(
            equity=10_000.0,
            equity_risk_pct=1.0,  # $100 risk
            risk_model=RiskModel.PERCENT_EQUITY,
            min_lot=0.01,
            max_lot=100.0,
            lot_step=0.01,
        )
        quote = MarketQuote(symbol="EURUSD", bid=1.1000, ask=1.1001)

        # Intent with 50 pips SL (1.1000 -> 1.0950)
        # Pip value for EURUSD = $10/pip. 50 pips = $500 risk per lot.
        # For $100 risk -> lot = 100 / 500 = 0.20 lots.
        intent = OrderIntent(
            symbol="EURUSD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            price=1.1000,
            stop_loss=1.0950,
            volume=0.0,
            signal_id=9001,
        )

        evaluated = self.risk_engine.evaluate(intent, account, quote)
        self.assertIsInstance(evaluated, OrderIntent)
        self.assertEqual(evaluated.volume, 0.20)

    def test_15_pure_module_functions_availability(self) -> None:
        """Module-level pure functional helpers work directly without engine instantiation."""
        self.assertEqual(classify_asset("EURUSD"), AssetClass.STANDARD_FOREX)
        self.assertEqual(classify_asset("USDJPY"), AssetClass.JPY_PAIR)
        self.assertEqual(classify_asset("GOLD"), AssetClass.METALS)
        self.assertEqual(classify_asset("US30"), AssetClass.INDICES)

        offset = pips_to_price_offset("USDJPY", 50)
        self.assertEqual(offset, 0.50)

        sl = calculate_sl("GOLD", 2000.00, 50, side="BUY")
        self.assertEqual(sl, 1995.00)

        tp = calculate_tp("GOLD", 2000.00, 50, side="BUY")
        self.assertEqual(tp, 2005.00)


if __name__ == "__main__":
    unittest.main()
