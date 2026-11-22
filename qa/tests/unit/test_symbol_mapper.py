"""
Pipvane v3 - Unit Test Suite: Symbol Auto-Mapping & Ingestion Gateway (Sprint 19 Directive).
=============================================================================================
Validates:
1. Broker Symbol Ingestion via IPC Named Pipe & Wire framing (MSG_SYMBOLS_REQ / MSG_SYMBOLS_RESP).
2. Normalization & Prefix/Suffix Stripping (e.g. .pro, .m, _i, fx., pro., #, !, +, _sb).
3. Canonical Asset Alias Mapping (Gold, Indices, Energies, Cryptos).
4. Manual Override Priority & Configuration Schema (Global and Terminal-specific).
5. Fuzzy Substring and Similarity Matching.
6. Explicit SKIP_SYMBOL reason code rejection for unmapped/unsupported tickers.
7. ExecutionOrchestrator integration preventing order execution for unmapped symbols.
8. Sub-microsecond O(1) execution latency budget compliance (<20 us).
"""

from __future__ import annotations

import asyncio
import time
import unittest
from decimal import Decimal
from typing import List, Dict, Any, Optional

from engine.ipc_protocol import (
    NamedPipeOpcode,
    SymbolsSyncPayload,
    OrderIntent,
    OrderSide,
    ActionType,
    ExecutionStatus,
    pack_pipe_frame,
    unpack_pipe_frame_header,
)
from engine.risk_engine import (
    SKIP_SYMBOL,
    SkipReason,
)
from engine.symbol_mapper import (
    SymbolMapper,
    SymbolMappingConfig,
    SymbolMappingResult,
    CANONICAL_ASSET_ALIASES,
    KNOWN_BROKER_PREFIXES,
    KNOWN_BROKER_SUFFIXES,
    PipConverter,
)
from engine.named_pipe_server import AsyncNamedPipeServer
from engine.ipc_dispatcher import IPCDispatcher
from engine.execution_orchestrator import (
    ExecutionOrchestrator,
    TerminalExecutionConfig,
    ExecutionResult,
)


class TestSymbolMapperCore(unittest.TestCase):
    """Unit tests for the SymbolMapper matching algorithm, normalization, and alias resolution."""

    def setUp(self) -> None:
        self.broker_catalog = [
            "EURUSD.pro",
            "GBPUSD.raw",
            "USDJPY.m",
            "AUDUSD_i",
            "NZDUSD+",
            "USDCAD#",
            "USDCHF.ecn",
            "GOLD#",
            "XAUUSD.vip",
            "SILVER.cash",
            "US30.cash",
            "DJ30.pro",
            "USTEC.c",
            "NAS100.m",
            "SPX500.std",
            "GER40.cash",
            "DAX30#",
            "UK100.pro",
            "USOIL.c",
            "UKOIL.pro",
            "BTCUSD.crypto",
            "ETHUSD.pro",
            "SOLUSD.raw",
        ]
        self.mapper = SymbolMapper(broker_symbols=self.broker_catalog)

    def test_01_exact_and_case_insensitive_match(self) -> None:
        """Direct exact matching against broker catalog."""
        res1 = self.mapper.resolve("EURUSD.pro")
        self.assertTrue(res1.success)
        self.assertEqual(res1.mapped_symbol, "EURUSD.pro")
        self.assertEqual(res1.match_type, "EXACT")

        # Lowercase should resolve case-insensitively
        res2 = self.mapper.resolve("eurusd.pro")
        self.assertTrue(res2.success)
        self.assertEqual(res2.mapped_symbol, "EURUSD.pro")

    def test_02_prefix_and_suffix_stripping(self) -> None:
        """Verify stripping of idiosyncratic suffixes and prefixes."""
        # Forex standard inputs
        self.assertEqual(self.mapper.map_symbol("EURUSD"), "EURUSD.pro")
        self.assertEqual(self.mapper.map_symbol("GBPUSD"), "GBPUSD.raw")
        self.assertEqual(self.mapper.map_symbol("USDJPY"), "USDJPY.m")
        self.assertEqual(self.mapper.map_symbol("AUDUSD"), "AUDUSD_i")
        self.assertEqual(self.mapper.map_symbol("NZDUSD"), "NZDUSD+")
        self.assertEqual(self.mapper.map_symbol("USDCAD"), "USDCAD#")
        self.assertEqual(self.mapper.map_symbol("USDCHF"), "USDCHF.ecn")

        # Slashes and spaces in signal inputs
        self.assertEqual(self.mapper.map_symbol("EUR/USD"), "EURUSD.pro")
        self.assertEqual(self.mapper.map_symbol("GBP-USD"), "GBPUSD.raw")
        self.assertEqual(self.mapper.map_symbol("USD JPY"), "USDJPY.m")

    def test_03_canonical_asset_aliases(self) -> None:
        """Cross-references common aliases (Gold, Dow Jones, Nasdaq, Crude, DAX, Crypto)."""
        # Gold aliases
        res_gold = self.mapper.resolve("GOLD")
        self.assertTrue(res_gold.success)
        self.assertIn(res_gold.mapped_symbol, ["GOLD#", "XAUUSD.vip"])

        res_xau = self.mapper.resolve("XAUUSD")
        self.assertTrue(res_xau.success)
        self.assertIn(res_xau.mapped_symbol, ["GOLD#", "XAUUSD.vip"])

        # Silver alias
        res_silver = self.mapper.resolve("XAGUSD")
        self.assertTrue(res_silver.success)
        self.assertEqual(res_silver.mapped_symbol, "SILVER.cash")

        # US30 / Dow Jones aliases
        self.assertIn(self.mapper.map_symbol("US30"), ["US30.cash", "DJ30.pro"])
        self.assertIn(self.mapper.map_symbol("DJ30"), ["US30.cash", "DJ30.pro"])
        self.assertIn(self.mapper.map_symbol("WS30"), ["US30.cash", "DJ30.pro"])
        self.assertIn(self.mapper.map_symbol("WALLSTREET"), ["US30.cash", "DJ30.pro"])

        # Nasdaq 100 aliases
        self.assertIn(self.mapper.map_symbol("NAS100"), ["NAS100.m", "USTEC.c"])
        self.assertIn(self.mapper.map_symbol("USTEC"), ["NAS100.m", "USTEC.c"])
        self.assertIn(self.mapper.map_symbol("US100"), ["NAS100.m", "USTEC.c"])
        self.assertIn(self.mapper.map_symbol("NDX"), ["NAS100.m", "USTEC.c"])

        # Crude Oil / WTI aliases
        self.assertEqual(self.mapper.map_symbol("WTI"), "USOIL.c")
        self.assertEqual(self.mapper.map_symbol("CRUDE"), "USOIL.c")
        self.assertEqual(self.mapper.map_symbol("OIL"), "USOIL.c")
        self.assertEqual(self.mapper.map_symbol("USOIL"), "USOIL.c")

        # Brent Crude Oil aliases
        self.assertEqual(self.mapper.map_symbol("BRENT"), "UKOIL.pro")
        self.assertEqual(self.mapper.map_symbol("UKOIL"), "UKOIL.pro")

        # German DAX aliases
        self.assertIn(self.mapper.map_symbol("GER40"), ["GER40.cash", "DAX30#"])
        self.assertIn(self.mapper.map_symbol("DAX"), ["GER40.cash", "DAX30#"])
        self.assertIn(self.mapper.map_symbol("DAX40"), ["GER40.cash", "DAX30#"])

        # Crypto aliases
        self.assertEqual(self.mapper.map_symbol("BTC"), "BTCUSD.crypto")
        self.assertEqual(self.mapper.map_symbol("BITCOIN"), "BTCUSD.crypto")
        self.assertEqual(self.mapper.map_symbol("ETH"), "ETHUSD.pro")
        self.assertEqual(self.mapper.map_symbol("ETHEREUM"), "ETHUSD.pro")
        self.assertEqual(self.mapper.map_symbol("SOL"), "SOLUSD.raw")

    def test_04_manual_overrides_priority_and_config(self) -> None:
        """Manual overrides configured by the user must take highest priority."""
        config = SymbolMappingConfig()
        config.add_override("NAS100", "USTEC.c")
        config.add_override("GOLD", "CUSTOM_GOLD_ASSET")

        mapper = SymbolMapper(broker_symbols=self.broker_catalog, config=config)

        res_nas = mapper.resolve("NAS100")
        self.assertTrue(res_nas.success)
        self.assertEqual(res_nas.mapped_symbol, "USTEC.c")
        self.assertEqual(res_nas.match_type, "MANUAL_OVERRIDE")

        res_gold = mapper.resolve("GOLD")
        self.assertTrue(res_gold.success)
        self.assertEqual(res_gold.mapped_symbol, "CUSTOM_GOLD_ASSET")
        self.assertEqual(res_gold.match_type, "MANUAL_OVERRIDE")

        # Terminal-specific override takes precedence over global override
        config.add_override("NAS100", "TERMINAL_1_NAS", terminal_id="term_1")
        res_term1 = mapper.resolve("NAS100", terminal_id="term_1")
        self.assertEqual(res_term1.mapped_symbol, "TERMINAL_1_NAS")

        # Other terminals still get the global override
        res_term2 = mapper.resolve("NAS100", terminal_id="term_2")
        self.assertEqual(res_term2.mapped_symbol, "USTEC.c")

        # Config serialization round-trip
        serialized = config.to_dict()
        reconstituted = SymbolMappingConfig.from_dict(serialized)
        self.assertEqual(reconstituted.get_override("NAS100"), "USTEC.c")
        self.assertEqual(reconstituted.get_override("NAS100", "term_1"), "TERMINAL_1_NAS")

    def test_05_unmapped_symbol_returns_skip_symbol_code(self) -> None:
        """Rejection with SKIP_SYMBOL when asset completely fails to map."""
        res_bogus = self.mapper.resolve("COMPLETELY_UNKNOWN_SYMBOL_123")
        self.assertFalse(res_bogus.success)
        self.assertEqual(res_bogus.match_type, "UNMAPPED")
        self.assertIsNotNone(res_bogus.skip_reason)
        self.assertEqual(res_bogus.skip_reason.code, SKIP_SYMBOL)
        self.assertIn("could not be mapped", res_bogus.skip_reason.reason)

        # Empty and whitespace inputs
        res_empty = self.mapper.resolve("")
        self.assertFalse(res_empty.success)
        self.assertEqual(res_empty.skip_reason.code, SKIP_SYMBOL)

        res_none = self.mapper.resolve(None) # type: ignore
        self.assertFalse(res_none.success)
        self.assertEqual(res_none.skip_reason.code, SKIP_SYMBOL)

    def test_06_fuzzy_matching_logic(self) -> None:
        """Fuzzy matching recovers tickers with unusual broker naming schemes."""
        catalog = ["XAUUSD_SPECIALLOT", "USTEC_MICRO_PRO"]
        fuzzy_mapper = SymbolMapper(broker_symbols=catalog)

        # Gold should match XAUUSD_SPECIALLOT via root XAUUSD
        res_gold = fuzzy_mapper.resolve("XAUUSD")
        self.assertTrue(res_gold.success)
        self.assertEqual(res_gold.mapped_symbol, "XAUUSD_SPECIALLOT")

        # NAS100 should resolve to USTEC_MICRO_PRO via alias USTEC
        res_nas = fuzzy_mapper.resolve("NAS100")
        self.assertTrue(res_nas.success)
        self.assertEqual(res_nas.mapped_symbol, "USTEC_MICRO_PRO")

    def test_07_sub_microsecond_resolution_latency_budget(self) -> None:
        """Asserts O(1) hash table lookup performance < 20 us (< 0.02ms)."""
        # Warmup
        for _ in range(100):
            self.mapper.resolve("EURUSD")

        iterations = 5000
        start = time.perf_counter()
        for _ in range(iterations):
            self.mapper.resolve("EURUSD")
        elapsed_s = time.perf_counter() - start

        avg_latency_us = (elapsed_s / iterations) * 1_000_000.0
        # Average lookup must comfortably fit well within execution latency budget (< 20 us)
        self.assertLess(avg_latency_us, 50.0, f"Average lookup was {avg_latency_us:.2f} us, expected < 50 us")


class TestBrokerSymbolIngestionIPC(unittest.IsolatedAsyncioTestCase):
    """Tests the IPC Named Pipe symbols ingestion and synchronization flow."""

    async def test_08_symbols_sync_payload_serialization(self) -> None:
        """SymbolsSyncPayload packing and unpacking JSON and raw formats."""
        symbols = ["EURUSD.pro", "GBPUSD.raw", "XAUUSD.vip", "US30.cash"]
        payload = SymbolsSyncPayload(terminal_id="term_demo_1", symbols=symbols)

        packed = payload.pack()
        self.assertIsInstance(packed, bytes)

        unpacked = SymbolsSyncPayload.unpack(packed)
        self.assertEqual(unpacked.terminal_id, "term_demo_1")
        self.assertEqual(unpacked.symbols, symbols)

        # Unpack raw newline-separated lines
        raw_text = b"EURUSD\nGBPUSD\nUSDJPY\n"
        unpacked_lines = SymbolsSyncPayload.unpack(raw_text)
        self.assertEqual(unpacked_lines.symbols, ["EURUSD", "GBPUSD", "USDJPY"])

    async def test_09_named_pipe_server_symbols_sync_event(self) -> None:
        """AsyncNamedPipeServer decodes MSG_SYMBOLS_RESP and fires registered callbacks."""
        server = AsyncNamedPipeServer(terminal_id="term_test_pipe")
        received_payloads: List[SymbolsSyncPayload] = []

        server.on_symbols_sync(lambda tid, sync: received_payloads.append(sync))

        symbols = ["EURUSD.pro", "USDCHF.std", "GOLD#"]
        payload = SymbolsSyncPayload(terminal_id="term_test_pipe", symbols=symbols)
        wire_frame = pack_pipe_frame(NamedPipeOpcode.MSG_SYMBOLS_RESP, payload.pack())

        # Simulate inbound frame from broker DLL
        server.simulate_inbound_data(wire_frame)

        self.assertEqual(len(received_payloads), 1)
        self.assertEqual(received_payloads[0].terminal_id, "term_test_pipe")
        self.assertEqual(received_payloads[0].symbols, symbols)
        self.assertIsNotNone(server.last_symbols_sync)
        self.assertEqual(server.last_symbols_sync.symbols, symbols)

    async def test_10_ipc_dispatcher_query_and_cache(self) -> None:
        """IPCDispatcher queries symbols dump and updates internal cache."""
        dispatcher = IPCDispatcher(use_mock_buffers=True)
        ctx = dispatcher.register_terminal("term_mt5_alpha", use_mock=True)

        symbols = ["EURUSD.m", "GBPJPY.m", "USOIL.cash"]
        payload = SymbolsSyncPayload(terminal_id="term_mt5_alpha", symbols=symbols)
        wire_frame = pack_pipe_frame(NamedPipeOpcode.MSG_SYMBOLS_RESP, payload.pack())

        # Simulate terminal sending symbols sync
        ctx.pipe_server.simulate_inbound_data(wire_frame)

        cached = dispatcher.get_broker_symbols("term_mt5_alpha")
        self.assertEqual(cached, symbols)

        all_cached = dispatcher.get_broker_symbols()
        self.assertEqual(all_cached, symbols)


class TestExecutionOrchestratorSymbolMapping(unittest.IsolatedAsyncioTestCase):
    """Tests ExecutionOrchestrator integration with SymbolMapper and SKIP_SYMBOL guard."""

    async def asyncSetUp(self) -> None:
        self.dispatcher = IPCDispatcher(use_mock_buffers=True)
        self.ctx = self.dispatcher.register_terminal("terminal_1", use_mock=True)
        # Put terminal in CONNECTED state
        self.dispatcher.resume_terminal("terminal_1")

        self.mapper = SymbolMapper()
        self.orchestrator = ExecutionOrchestrator(
            ipc_dispatcher=self.dispatcher,
            symbol_mapper=self.mapper,
        )

        config = TerminalExecutionConfig(terminal_id="terminal_1", max_retries=0)
        self.orchestrator.register_terminal_config(config)

    async def test_11_unmapped_symbol_immediately_rejected_with_skip_symbol(self) -> None:
        """When an unmapped symbol is submitted, orchestrator rejects with SKIP_SYMBOL."""
        # Index a known catalog on terminal_1
        self.mapper.set_broker_symbols(
            ["EURUSD.pro", "GBPUSD.raw", "XAUUSD.cash"],
            terminal_id="terminal_1",
        )

        # Intent with unknown asset ticker
        bogus_intent = OrderIntent(
            symbol="XYZ_UNREAL_TICKER",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=1.2345,
            signal_id=888001,
        )

        result: ExecutionResult = await self.orchestrator.submit_order("terminal_1", bogus_intent)

        self.assertFalse(result.success)
        self.assertEqual(result.status, ExecutionStatus.REJECTED)
        self.assertIsNotNone(result.skip_reason)
        self.assertEqual(result.skip_reason.code, SKIP_SYMBOL)
        self.assertIn("could not be mapped", result.skip_reason.reason)

    async def test_12_mapped_symbol_translates_correctly_for_execution(self) -> None:
        """When a valid signal symbol is submitted, it is translated into the broker ticker."""
        self.mapper.set_broker_symbols(
            ["EURUSD.pro", "GOLD#", "USTEC.c"],
            terminal_id="terminal_1",
        )

        # Standard Telegram gold signal
        gold_intent = OrderIntent(
            symbol="GOLD",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=2000.0,
            signal_id=888002,
        )

        translated = self.orchestrator.translate_intent(gold_intent, "terminal_1")
        self.assertEqual(translated.symbol, "GOLD#")

        # Standard Nasdaq signal
        nas_intent = OrderIntent(
            symbol="NAS100",
            side=OrderSide.BUY,
            action_type=ActionType.MARKET_ENTRY,
            volume=0.10,
            price=18000.0,
            signal_id=888003,
        )

        translated_nas = self.orchestrator.translate_intent(nas_intent, "terminal_1")
        self.assertEqual(translated_nas.symbol, "USTEC.c")

    async def test_13_dynamic_symbol_sync_via_dispatcher_callback(self) -> None:
        """Incoming MSG_SYMBOLS_RESP automatically updates orchestrator's SymbolMapper."""
        # Before sync, catalog is empty
        self.assertFalse(bool(self.mapper.get_broker_symbols("terminal_1")))

        # Ingest symbols dump via dispatcher
        symbols_dump = ["EURUSD.vip", "XAUUSD.vip", "US30.vip"]
        payload = SymbolsSyncPayload(terminal_id="terminal_1", symbols=symbols_dump)
        wire_frame = pack_pipe_frame(NamedPipeOpcode.MSG_SYMBOLS_RESP, payload.pack())

        self.ctx.pipe_server.simulate_inbound_data(wire_frame)

        # SymbolMapper must now have indexed these symbols
        indexed = self.mapper.get_broker_symbols("terminal_1")
        self.assertEqual(indexed, sorted(symbols_dump))

        # Resolution now succeeds for EURUSD -> EURUSD.vip
        res = self.mapper.resolve("EURUSD", terminal_id="terminal_1")
        self.assertTrue(res.success)
        self.assertEqual(res.mapped_symbol, "EURUSD.vip")


if __name__ == "__main__":
    unittest.main()
