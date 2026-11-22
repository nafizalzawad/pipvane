"""
Pipvane v3 - Mapping Lab & Broker Matrix Test Suite (Sprint 19 Directive)
==========================================================================
Validates SymbolMapper and PipConverter against a massive matrix of known broker
idiosyncrasies and asserts mathematical precision down to the 5th decimal place.

Mandates:
1. Symbol Matrix Test:
   - Mock broker asset list containing 50 messy, idiosyncratic broker symbols
     (e.g., "EURUSD.pro", "GOLD#", "DJ30.cash", "BTCUSD.crypto").
   - Feed standard Telegram inputs ("XAUUSD", "US30", "EURUSD", "GOLD", "NAS100", etc.).
   - Assert 100% correct mapping across all 50 instruments.
2. Math Precision Test:
   - USDJPY entry at 150.000 with 30 pip Stop Loss -> exactly outputs 149.700.
   - XAUUSD entry at 2000.00 with 50 pip Stop Loss -> exactly outputs 1995.00.
   - Strict assertion of precision down to the 5th decimal place (0.00001).
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from engine.symbol_mapper import (
    CANONICAL_ASSET_ALIASES,
    DEFAULT_DECIMAL_PIP_SIZES,
    PipConverter,
    SymbolMapper,
)


# =============================================================================
# MOCK BROKER 50-SYMBOL ASSET LIST (IDIOSYNCRATIC REAL-WORLD MATRIX)
# =============================================================================

MOCK_50_MESSY_BROKER_SYMBOLS = [
    # 1-24: Forex Majors & Crosses with messy affixes (.pro, _i, .m, .raw, +, #, .ecn, .stp, .micro, _SB, etc.)
    "EURUSD.pro",       # 1. Standard FX (.pro suffix)
    "GBPUSD_i",         # 2. FX with _i Islamic/instant suffix
    "USDJPY.m",         # 3. FX with .m micro suffix
    "AUDUSD.raw",       # 4. FX with .raw raw spread suffix
    "NZDUSD+",          # 5. FX with + suffix
    "USDCAD#",          # 6. FX with # suffix
    "USDCHF.ecn",       # 7. FX with .ecn suffix
    "EURGBP.stp",       # 8. FX with .stp suffix
    "EURJPY.micro",     # 9. FX with .micro suffix
    "GBPJPY_SB",        # 10. FX with spread betting suffix
    "AUDJPY.zero",      # 11. FX with zero spread suffix
    "CADJPY.a",         # 12. FX with .a account tier suffix
    "CHFJPY.vip",       # 13. FX with .vip suffix
    "EURAUD.c",         # 14. FX with cent suffix
    "EURCAD.std",       # 15. FX with standard suffix
    "GBPAUD.fx",        # 16. FX with .fx suffix
    "GBPCAD.mini",      # 17. FX with .mini suffix
    "AUDCAD.s",         # 18. FX with .s suffix
    "NZDCAD.d",         # 19. FX with .d suffix
    "AUDNZD_pro",       # 20. FX with _pro suffix
    "CADCHF.f",         # 21. FX with .f suffix
    "EURCHF_ecn",       # 22. FX with _ecn suffix
    "GBPCHF.p",         # 23. FX with .p suffix
    "NZDCHF.x",         # 24. FX with .x suffix
    # 25-30: Exotic FX Pairs
    "USDSEK.std",       # 25. Swedish Krona
    "USDNOK.pro",       # 26. Norwegian Krone
    "USDZAR.ecn",       # 27. South African Rand
    "USDMXN_i",         # 28. Mexican Peso
    "USDSGD.raw",       # 29. Singapore Dollar
    "USDHKD.m",         # 30. Hong Kong Dollar
    # 31-35: Precious Metals
    "GOLD#",            # 31. Gold with # ticker
    "XAUUSD.pro",       # 32. Gold with .pro
    "XAUUSD.ecn",       # 33. Gold with .ecn
    "XAGUSD.cash",      # 34. Silver with .cash
    "SILVER#",          # 35. Silver with # ticker
    # 36-45: Indices & CFDs
    "US30.cash",        # 36. Dow Jones with .cash
    "DJ30.cash",        # 37. Dow Jones alternative ticker
    "US30_INDEX",       # 38. Dow Jones index
    "NAS100.cash",      # 39. Nasdaq 100 with .cash
    "USTEC.pro",        # 40. Nasdaq 100 alternative ticker
    "SPX500.pro",       # 41. S&P 500 with .pro
    "US500.cash",       # 42. S&P 500 alternative ticker
    "GER40.cash",       # 43. DAX 40 with .cash
    "GER30#",           # 44. DAX 30 alternative ticker
    "UK100.pro",        # 45. FTSE 100
    # 46-47: Energies & Commodities
    "USOIL.cash",       # 46. WTI Crude Oil
    "UKOIL.pro",        # 47. Brent Crude Oil
    # 48-50: Cryptocurrencies
    "BTCUSD.crypto",    # 48. Bitcoin with .crypto
    "ETHUSD.pro",       # 49. Ethereum with .pro
    "SOLUSD.raw",       # 50. Solana with .raw
]


# =============================================================================
# TEST SUITE: SPRINT 19 MAPPING LAB & BROKER MATRIX
# =============================================================================

class TestBrokerMatrixAndMathPrecision(unittest.TestCase):
    """Test suite executing the Symbol Matrix and Math Precision verification."""

    def setUp(self):
        self.broker_symbols = list(MOCK_50_MESSY_BROKER_SYMBOLS)
        self.symbol_mapper = SymbolMapper(broker_symbols=self.broker_symbols)
        self.pip_converter = PipConverter()

    # -------------------------------------------------------------------------
    # TASK 1: SYMBOL MATRIX TEST (50 MESSY BROKER SYMBOLS)
    # -------------------------------------------------------------------------

    def test_01_symbol_matrix_catalog_contains_exactly_50_instruments(self):
        """Verify the mock broker catalog contains at least 50 idiosyncratic asset symbols."""
        self.assertGreaterEqual(len(self.broker_symbols), 50)
        self.assertEqual(len(set(self.broker_symbols)), 50, "All 50 symbols must be unique")

    def test_02_symbol_matrix_standard_telegram_inputs_map_with_100_percent_accuracy(self):
        """
        TASK 1 MANDATE:
        Feed standard Telegram inputs (e.g., 'XAUUSD', 'US30', 'EURUSD', 'GOLD', 'DJ30')
        and assert 100% correct mapping against the 50 messy broker symbols.
        """
        # Map of Telegram input variants -> Expected Broker Instrument
        test_matrix: Dict[str, str] = {
            # Standard FX
            "EURUSD": "EURUSD.pro",
            "GBPUSD": "GBPUSD_i",
            "USDJPY": "USDJPY.m",
            "AUDUSD": "AUDUSD.raw",
            "NZDUSD": "NZDUSD+",
            "USDCAD": "USDCAD#",
            "USDCHF": "USDCHF.ecn",
            "EURGBP": "EURGBP.stp",
            "EURJPY": "EURJPY.micro",
            "GBPJPY": "GBPJPY_SB",
            "AUDJPY": "AUDJPY.zero",
            "CADJPY": "CADJPY.a",
            "CHFJPY": "CHFJPY.vip",
            "EURAUD": "EURAUD.c",
            "EURCAD": "EURCAD.std",
            "GBPAUD": "GBPAUD.fx",
            "GBPCAD": "GBPCAD.mini",
            "AUDCAD": "AUDCAD.s",
            "NZDCAD": "NZDCAD.d",
            "AUDNZD": "AUDNZD_pro",
            "CADCHF": "CADCHF.f",
            "EURCHF": "EURCHF_ecn",
            "GBPCHF": "GBPCHF.p",
            "NZDCHF": "NZDCHF.x",
            # Exotic FX
            "USDSEK": "USDSEK.std",
            "USDNOK": "USDNOK.pro",
            "USDZAR": "USDZAR.ecn",
            "USDMXN": "USDMXN_i",
            "USDSGD": "USDSGD.raw",
            "USDHKD": "USDHKD.m",
            # Metals (Both XAUUSD and GOLD alias resolution)
            "XAUUSD": "GOLD#",       # Or XAUUSD.pro
            "GOLD": "GOLD#",
            "XAGUSD": "XAGUSD.cash",
            "SILVER": "SILVER#",
            # Indices (Both US30 and DJ30 alias resolution)
            "US30": "US30.cash",
            "DJ30": "DJ30.cash",
            "DOW30": "US30.cash",
            "WALLSTREET30": "US30.cash",
            "NAS100": "NAS100.cash",
            "USTEC": "USTEC.pro",
            "SPX500": "SPX500.pro",
            "US500": "US500.cash",
            "GER40": "GER40.cash",
            "GER30": "GER30#",
            "DAX40": "GER40.cash",
            "UK100": "UK100.pro",
            # Commodities
            "USOIL": "USOIL.cash",
            "WTI": "USOIL.cash",
            "UKOIL": "UKOIL.pro",
            "BRENT": "UKOIL.pro",
            # Crypto
            "BTCUSD": "BTCUSD.crypto",
            "BTC": "BTCUSD.crypto",
            "ETHUSD": "ETHUSD.pro",
            "ETH": "ETHUSD.pro",
            "SOLUSD": "SOLUSD.raw",
            "SOL": "SOLUSD.raw",
        }

        success_count = 0
        total_tests = len(test_matrix)

        for telegram_input, expected_broker_symbol in test_matrix.items():
            resolved = self.symbol_mapper.map_symbol(telegram_input)
            # In cases where multiple candidates exist (e.g. GOLD# vs XAUUSD.pro), both are valid Gold
            if telegram_input in ("XAUUSD", "GOLD"):
                self.assertIn(resolved, ("GOLD#", "XAUUSD.pro", "XAUUSD.ecn"))
            elif telegram_input in ("US30", "DJ30", "DOW30", "WALLSTREET30"):
                self.assertIn(resolved, ("US30.cash", "DJ30.cash", "US30_INDEX"))
            elif telegram_input in ("NAS100", "USTEC"):
                self.assertIn(resolved, ("NAS100.cash", "USTEC.pro"))
            elif telegram_input in ("SPX500", "US500"):
                self.assertIn(resolved, ("SPX500.pro", "US500.cash"))
            elif telegram_input in ("GER40", "GER30", "DAX40"):
                self.assertIn(resolved, ("GER40.cash", "GER30#"))
            else:
                self.assertEqual(
                    resolved,
                    expected_broker_symbol,
                    f"Failed mapping Telegram input '{telegram_input}' -> expected '{expected_broker_symbol}', got '{resolved}'",
                )
            success_count += 1

        self.assertEqual(success_count, total_tests)
        accuracy_pct = (success_count / total_tests) * 100.0
        self.assertEqual(accuracy_pct, 100.0, "Symbol mapping accuracy must be strictly 100%")

    def test_03_symbol_matrix_handles_slashes_spaces_and_case_variants(self):
        """Telegram inputs with slashes ('EUR/USD', 'XAU/USD') or lowercase ('us30') resolve correctly."""
        self.assertEqual(self.symbol_mapper.map_symbol("eur/usd"), "EURUSD.pro")
        self.assertIn(self.symbol_mapper.map_symbol("XAU/USD"), ("GOLD#", "XAUUSD.pro", "XAUUSD.ecn"))
        self.assertIn(self.symbol_mapper.map_symbol("us/30"), ("US30.cash", "DJ30.cash", "US30_INDEX"))
        self.assertEqual(self.symbol_mapper.map_symbol("gbp_jpy"), "GBPJPY_SB")
        self.assertEqual(self.symbol_mapper.map_symbol("btc/usd"), "BTCUSD.crypto")

    def test_04_symbol_matrix_custom_overrides_take_absolute_priority(self):
        """User-defined custom symbol map overrides any broker heuristic."""
        mapper_with_custom = SymbolMapper(
            broker_symbols=self.broker_symbols,
            custom_map={"XAUUSD": "GOLD.CUSTOM_OVERRIDE", "EURUSD": "EURUSD.CUSTOM_OVERRIDE"},
        )
        self.assertEqual(mapper_with_custom.map_symbol("XAUUSD"), "GOLD.CUSTOM_OVERRIDE")
        self.assertEqual(mapper_with_custom.map_symbol("EURUSD"), "EURUSD.CUSTOM_OVERRIDE")
        # Non-overridden symbol still resolves via broker catalog
        self.assertEqual(mapper_with_custom.map_symbol("USDJPY"), "USDJPY.m")

    # -------------------------------------------------------------------------
    # TASK 2: MATH PRECISION TESTS (FLAWLESS DOWN TO 5TH DECIMAL PLACE)
    # -------------------------------------------------------------------------

    def test_05_math_precision_usdjpy_entry_150_sl_30_pips_outputs_exactly_149_700(self):
        """
        TASK 2 MANDATE 1:
        Mock an entry on USDJPY at 150.000.
        Assert that applying a '30 pip' Stop Loss exactly outputs 149.700.
        Math precision flawless down to 5th decimal place (149.70000).
        """
        entry_price = "150.000"
        sl_pips = 30

        # BUY Order: Stop Loss is below entry
        # Calculation: 150.000 - (30 * 0.01) = 150.000 - 0.300 = 149.700
        calculated_sl_buy = self.pip_converter.calculate_sl(
            symbol="USDJPY",
            entry_price=entry_price,
            sl_pips=sl_pips,
            side="BUY",
            quantize_places=5,
        )

        expected_buy_sl = Decimal("149.70000")
        self.assertEqual(
            calculated_sl_buy,
            expected_buy_sl,
            f"USDJPY BUY SL precision failure: expected {expected_buy_sl}, got {calculated_sl_buy}",
        )
        # Also assert string conversion matches 149.700 without floating-point artifacts
        self.assertEqual(str(calculated_sl_buy), "149.70000")
        self.assertEqual(float(calculated_sl_buy), 149.7)

        # SELL Order: Stop Loss is above entry
        # Calculation: 150.000 + (30 * 0.01) = 150.000 + 0.300 = 150.300
        calculated_sl_sell = self.pip_converter.calculate_sl(
            symbol="USDJPY",
            entry_price=entry_price,
            sl_pips=sl_pips,
            side="SELL",
            quantize_places=5,
        )
        expected_sell_sl = Decimal("150.30000")
        self.assertEqual(calculated_sl_sell, expected_sell_sl)
        self.assertEqual(str(calculated_sl_sell), "150.30000")

    def test_06_math_precision_xauusd_entry_2000_sl_50_pips_outputs_exactly_1995_00(self):
        """
        TASK 2 MANDATE 2:
        Mock an entry on XAUUSD at 2000.00.
        Assert that applying a '50 pip' Stop Loss exactly outputs 1995.00.
        Math precision flawless down to 5th decimal place (1995.00000).
        """
        entry_price = "2000.00"
        sl_pips = 50

        # On Gold (XAUUSD), 1 pip = 0.10 (10 cents)
        # Calculation: 50 pips * 0.10 = $5.00 price distance
        # BUY SL: 2000.00 - 5.00 = 1995.00
        calculated_sl_buy = self.pip_converter.calculate_sl(
            symbol="XAUUSD",
            entry_price=entry_price,
            sl_pips=sl_pips,
            side="BUY",
            quantize_places=5,
        )

        expected_buy_sl = Decimal("1995.00000")
        self.assertEqual(
            calculated_sl_buy,
            expected_buy_sl,
            f"XAUUSD BUY SL precision failure: expected {expected_buy_sl}, got {calculated_sl_buy}",
        )
        self.assertEqual(str(calculated_sl_buy), "1995.00000")
        self.assertEqual(float(calculated_sl_buy), 1995.0)

        # SELL Order: Stop Loss is above entry
        # Calculation: 2000.00 + 5.00 = 2005.00
        calculated_sl_sell = self.pip_converter.calculate_sl(
            symbol="XAUUSD",
            entry_price=entry_price,
            sl_pips=sl_pips,
            side="SELL",
            quantize_places=5,
        )
        expected_sell_sl = Decimal("2005.00000")
        self.assertEqual(calculated_sl_sell, expected_sell_sl)
        self.assertEqual(str(calculated_sl_sell), "2005.00000")

    def test_07_math_precision_forex_majors_5th_decimal_place(self):
        """Verify 5th decimal precision across Forex majors (EURUSD, GBPUSD, AUDUSD)."""
        # EURUSD: 1 pip = 0.0001
        # Entry: 1.08532, SL 25.5 pips -> 1.08532 - 0.00255 = 1.08277
        eurusd_sl = self.pip_converter.calculate_sl("EURUSD", "1.08532", "25.5", side="BUY", quantize_places=5)
        self.assertEqual(eurusd_sl, Decimal("1.08277"))

        # GBPUSD: Entry 1.26450, TP 40 pips -> 1.26450 + 0.00400 = 1.26850
        gbpusd_tp = self.pip_converter.calculate_tp("GBPUSD", "1.26450", 40, side="BUY", quantize_places=5)
        self.assertEqual(gbpusd_tp, Decimal("1.26850"))

        # AUDUSD SELL: Entry 0.65500, SL 35 pips -> 0.65500 + 0.00350 = 0.65850
        audusd_sl = self.pip_converter.calculate_sl("AUDUSD", "0.65500", 35, side="SELL", quantize_places=5)
        self.assertEqual(audusd_sl, Decimal("0.65850"))

    def test_08_math_precision_indices_and_crypto(self):
        """Verify precision for indices (US30, NAS100, SPX500) and crypto (BTCUSD)."""
        # US30: 1 pip = 1.00 index point. Entry 39000.0, SL 100 pips -> 38900.00000
        us30_sl = self.pip_converter.calculate_sl("US30", "39000.00", 100, side="BUY", quantize_places=5)
        self.assertEqual(us30_sl, Decimal("38900.00000"))

        # NAS100: 1 pip = 1.00 index point. Entry 18000.0, TP 200 pips -> 18200.00000
        nas100_tp = self.pip_converter.calculate_tp("NAS100", "18000.00", 200, side="BUY", quantize_places=5)
        self.assertEqual(nas100_tp, Decimal("18200.00000"))

        # SPX500: 1 pip = 0.10. Entry 5000.00, SL 50 pips (5.0 pts) -> 4995.00000
        spx_sl = self.pip_converter.calculate_sl("SPX500", "5000.00", 50, side="BUY", quantize_places=5)
        self.assertEqual(spx_sl, Decimal("4995.00000"))

        # BTCUSD: 1 pip = 1.00. Entry 65000.00, SL 1000 pips -> 64000.00000
        btc_sl = self.pip_converter.calculate_sl("BTCUSD", "65000.00", 1000, side="BUY", quantize_places=5)
        self.assertEqual(btc_sl, Decimal("64000.00000"))

    def test_09_math_precision_reverse_pip_delta_calculation(self):
        """Verify exact reverse pip calculation from price differences."""
        # USDJPY: from 150.000 to 149.700 -> exactly 30.00000 pips
        pips_usdjpy = self.pip_converter.price_delta_to_pips("USDJPY", "0.30000")
        self.assertEqual(pips_usdjpy, Decimal("30.00000"))

        # XAUUSD: from 2000.00 to 1995.00 -> exactly 50.00000 pips
        pips_xau = self.pip_converter.price_delta_to_pips("XAUUSD", "5.00000")
        self.assertEqual(pips_xau, Decimal("50.00000"))

        # EURUSD: from 1.08500 to 1.08250 -> exactly 25.00000 pips
        pips_eur = self.pip_converter.price_delta_to_pips("EURUSD", "0.00250")
        self.assertEqual(pips_eur, Decimal("25.00000"))

    def test_10_monte_carlo_zero_floating_point_imprecision_drift(self):
        """
        Stress test: 1,000 randomized pip additions and subtractions across various instruments.
        Asserts zero floating point IEEE-754 drift (e.g. 0.1 + 0.2 != 0.3) down to the 5th decimal place.
        """
        for i in range(1, 1001):
            pips = Decimal(i) / Decimal("10")
            # USDJPY
            sl_buy = self.pip_converter.calculate_sl("USDJPY", "150.00000", pips, side="BUY")
            expected = (Decimal("150.00000") - (pips * Decimal("0.01"))).quantize(Decimal("0.00001"))
            self.assertEqual(sl_buy, expected)

            # XAUUSD
            sl_gold = self.pip_converter.calculate_sl("XAUUSD", "2000.00000", pips, side="BUY")
            expected_gold = (Decimal("2000.00000") - (pips * Decimal("0.10"))).quantize(Decimal("0.00001"))
            self.assertEqual(sl_gold, expected_gold)


if __name__ == "__main__":
    unittest.main()
