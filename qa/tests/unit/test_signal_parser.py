"""
Unit tests validating SignalParser, Format Pack registry, Classification Engine,
and OrderIntent extraction (Sprint 2 ADR-002).
"""

import json
import math
import os
import pytest
from pathlib import Path

from engine.signal_parser import (
    SignalParser,
    ParsedSignal,
    MessageType,
    FormatPack,
    GeometryValidationError,
    SKIP_GEOMETRY,
)
from engine.ipc_protocol import (
    OrderIntent,
    OrderSide,
    ActionType,
)


# =============================================================================
# 1. FORMAT PACK REGISTRY & LOADING TESTS
# =============================================================================

@pytest.mark.unit
def test_format_pack_registry_and_loading():
    """Verify that baseline format packs load and compile correctly from JSON."""
    parser = SignalParser()
    packs = parser.list_format_packs()
    assert len(packs) >= 5

    pack_ids = [p.pack_id for p in packs]
    assert "standard_block" in pack_ids
    assert "single_line" in pack_ids
    assert "reply_update" in pack_ids
    assert "emoji_compact" in pack_ids
    assert "localized_arabic" in pack_ids

    # Priority sorting (highest first)
    assert packs[0].priority >= packs[-1].priority

    # Test dynamic loading of custom format pack
    custom_pack_json = {
        "pack_id": "crypto_special",
        "name": "Crypto Special Style",
        "description": "Custom pack for crypto signals",
        "version": "1.0.0",
        "priority": 50,
        "style": "crypto",
        "classification_markers": {
            "signal": ["\\b(MOON|PUMP|LONG)\\b"],
            "update": ["\\b(DUMP|EXIT)\\b"]
        },
        "patterns": {
            "symbol": ["\\b(BTCUSDT|ETHUSDT|SOLUSDT)\\b"],
            "side": ["\\b(LONG)\\b", "\\b(SHORT)\\b"],
            "entry": ["entry[:\\s]*([0-9]+\\.?[0-9]*)"],
            "stop_loss": ["sl[:\\s]*([0-9]+\\.?[0-9]*)"],
            "take_profit": ["tp[:\\s]*([0-9]+\\.?[0-9]*)"]
        }
    }
    loaded = parser.load_format_pack(custom_pack_json)
    assert loaded.pack_id == "crypto_special"
    assert parser.get_format_pack("crypto_special") is not None


# =============================================================================
# 2. CLASSIFICATION ENGINE TESTS
# =============================================================================

@pytest.mark.unit
def test_classification_engine():
    """Verify classification into 'signal', 'update', or 'chatter'."""
    parser = SignalParser()

    # 1. Trade Signals
    sig1 = "🟢 BUY XAUUSD @ 2650.50\nSL: 2640.00\nTP: 2670.00"
    assert parser.classify(sig1) == MessageType.SIGNAL

    sig2 = "SELL LIMIT EURUSD @ 1.05850 SL 1.06200 TP1 1.05500 TP2 1.05200"
    assert parser.classify(sig2) == MessageType.SIGNAL

    # 2. Position Updates / Modifications
    upd1 = "⚠️ MODIFY GBPJPY\nSL TO 195.20\nCLOSE 50%"
    assert parser.classify(upd1) == MessageType.UPDATE

    upd2 = "XAUUSD TP1 HIT! MOVE SL TO BREAKEVEN"
    assert parser.classify(upd2) == MessageType.UPDATE

    upd3 = "CANCEL PENDING ORDER EURUSD"
    assert parser.classify(upd3) == MessageType.UPDATE

    # 3. Chatter / Noise
    chat1 = "Good morning traders! Hope you have a wonderful and profitable week ahead 🚀"
    assert parser.classify(chat1) == MessageType.CHATTER

    chat2 = "Join our VIP channel now for 50% discount! Limited spots: https://t.me/vip_signals"
    assert parser.classify(chat2) == MessageType.CHATTER

    chat3 = "CPI numbers coming out in 15 minutes, be careful and manage risk!"
    assert parser.classify(chat3) == MessageType.CHATTER


# =============================================================================
# 3. EXTRACTION TESTS: STANDARD BLOCK & SINGLE LINE
# =============================================================================

@pytest.mark.unit
def test_standard_block_signal_parsing():
    """Verify structured multi-line block parsing."""
    parser = SignalParser()
    raw = (
        "🟢 BUY XAUUSD @ 2650.50\n"
        "SL: 2640.00\n"
        "TP1: 2660.00\n"
        "TP2: 2670.00\n"
        "TP3: 2680.00"
    )
    res = parser.parse(raw)

    assert res.action == "BUY"
    assert res.symbol == "XAUUSD"
    assert res.order_type == "MARKET"
    assert res.entry_price == 2650.50
    assert res.stop_loss == 2640.00
    assert res.take_profits == [2660.00, 2670.00, 2680.00]
    assert res.is_modification is False
    assert res.message_type == "signal"


@pytest.mark.unit
def test_single_line_signal_parsing():
    """Verify inline single-line signals with limit order and multiple TPs."""
    parser = SignalParser()
    raw = "SELL LIMIT EURUSD @ 1.05850 SL: 1.06200 TP1: 1.05500 TP2: 1.05200"
    res = parser.parse(raw)

    assert res.action == "SELL"
    assert res.symbol == "EURUSD"
    assert res.order_type == "LIMIT"
    assert res.entry_price == 1.05850
    assert res.stop_loss == 1.06200
    assert res.take_profits == [1.05500, 1.05200]
    assert res.is_modification is False


# =============================================================================
# 4. EXTRACTION TESTS: POSITION UPDATES & RISK MODIFIERS
# =============================================================================

@pytest.mark.unit
def test_reply_update_parsing():
    """Verify modification extraction: SL modification, partial close %, breakeven."""
    parser = SignalParser()

    # Case 1: Modify SL + Partial Close
    raw1 = "MODIFY GBPJPY\nSL TO 195.20\nCLOSE 50%"
    res1 = parser.parse(raw1)
    assert res1.action == "MODIFY"
    assert res1.symbol == "GBPJPY"
    assert res1.order_type == "UPDATE"
    assert res1.is_modification is True
    assert res1.stop_loss == 195.20
    assert res1.partial_close_pct == 50.0

    # Case 2: Move to Breakeven
    raw2 = "XAUUSD MOVE SL TO BE"
    res2 = parser.parse(raw2)
    assert res2.action == "MODIFY"
    assert res2.symbol == "XAUUSD"
    assert res2.is_modification is True
    assert res2.move_to_be is True

    # Case 3: Cancel pending order
    raw3 = "CANCEL PENDING ORDER EURUSD"
    res3 = parser.parse(raw3)
    assert res3.cancel_pending is True
    assert res3.symbol == "EURUSD"

    # Case 4: Full Close
    raw4 = "CLOSE ALL US30 NOW"
    res4 = parser.parse(raw4)
    assert res4.full_close is True
    assert res4.symbol == "US30"


# =============================================================================
# 5. EMOJI COMPACT & PRICE RANGE TESTS
# =============================================================================

@pytest.mark.unit
def test_emoji_compact_and_price_range_parsing():
    """Verify entry price range parsing (e.g. 2650.00 - 2652.00)."""
    parser = SignalParser()
    raw = (
        "🟢 GOLD BUY NOW 📈\n"
        "📍 Entry: 2650.00 - 2652.00\n"
        "🛑 SL: 2640.00\n"
        "🎯 TP: 2670.00\n"
        "🎯 TP: 2685.00"
    )
    res = parser.parse(raw)

    assert res.action == "BUY"
    assert res.symbol == "XAUUSD"  # Canonical mapped from GOLD
    assert res.entry_price == 2650.00
    assert res.entry_range == (2650.00, 2652.00)
    assert res.stop_loss == 2640.00
    assert res.take_profits == [2670.00, 2685.00]


# =============================================================================
# 6. MULTI-LINGUAL & ARABIC-INDIC TESTS
# =============================================================================

@pytest.mark.unit
def test_localized_arabic_parsing():
    """Verify localized Arabic signal with Eastern Arabic-Indic numerals."""
    parser = SignalParser()
    raw = (
        "🚨صفقة شراء ذهب عاجلة🚨\n"
        "🌟 الذهب XAUUSD\n"
        "📈 دخول: ٢٦٥٠.٥٠\n"
        "🛑 وقف الخسارة: ٢٦٤٠.٠٠\n"
        "🎯 هدف أول: ٢٦٧٠.٠٠\n"
        "🎯 هدف ثان: ٢٦٨٥.٠٠"
    )
    res = parser.parse(raw)

    assert res.action == "BUY"
    assert res.symbol == "XAUUSD"
    assert res.order_type == "MARKET"
    assert res.entry_price == 2650.50
    assert res.stop_loss == 2640.00
    assert res.take_profits == [2670.00, 2685.00]
    assert res.is_modification is False


# =============================================================================
# 7. TAKE PROFIT LADDER UP TO 10
# =============================================================================

@pytest.mark.unit
def test_take_profit_ladder_up_to_10():
    """Verify extraction of multi-TP ladder with up to 10 targets in sequence."""
    parser = SignalParser()
    raw = (
        "BUY BTCUSD @ 95000\n"
        "SL: 92000\n"
        "TP1: 96000\nTP2: 97000\nTP3: 98000\nTP4: 99000\nTP5: 100000\n"
        "TP6: 101000\nTP7: 102000\nTP8: 103000\nTP9: 104000\nTP10: 105000"
    )
    res = parser.parse(raw)

    assert res.action == "BUY"
    assert res.symbol == "BTCUSD"
    assert len(res.take_profits) == 10
    assert res.take_profits[0] == 96000.0
    assert res.take_profits[4] == 100000.0
    assert res.take_profits[9] == 105000.0


# =============================================================================
# 8. SYMBOL ALIAS RESOLUTION TESTS
# =============================================================================

@pytest.mark.unit
def test_symbol_alias_resolution():
    """Verify standard symbols and aliases map to canonical identifiers."""
    parser = SignalParser()

    assert parser.parse("BUY GOLD 2650 SL 2640 TP 2660").symbol == "XAUUSD"
    assert parser.parse("BUY DJ30 43000 SL 42800 TP 43200").symbol == "US30"
    assert parser.parse("SELL USTEC 21000 SL 21100 TP 20900").symbol == "NAS100"
    assert parser.parse("BUY EUR/USD 1.0850 SL 1.0800 TP 1.0900").symbol == "EURUSD"
    assert parser.parse("SELL WTI 70.50 SL 71.50 TP 69.00").symbol == "USOIL"


# =============================================================================
# 9. ORDER INTENT STRUCT TRANSLATION TESTS (ADR-001)
# =============================================================================

@pytest.mark.unit
def test_to_order_intent_translation():
    """Verify translation of ParsedSignal to 128-byte OrderIntent struct."""
    parser = SignalParser()
    sig = parser.parse("🟢 BUY XAUUSD @ 2650.50\nSL: 2640.00\nTP: 2670.00")

    intent = sig.to_order_intent(volume=0.10, magic_number=999)
    assert intent is not None
    assert isinstance(intent, OrderIntent)
    assert intent.symbol == "XAUUSD"
    assert intent.side == OrderSide.BUY
    assert intent.action_type == ActionType.MARKET_ENTRY
    assert math.isclose(intent.volume, 0.10)
    assert math.isclose(intent.price, 2650.50)
    assert math.isclose(intent.stop_loss, 2640.00)
    assert math.isclose(intent.take_profit, 2670.00)
    assert intent.magic_number == 999

    # Partial close modification translation
    mod_sig = parser.parse("MODIFY GBPJPY CLOSE 50%")
    mod_intent = mod_sig.to_order_intent(volume=0.10)
    assert mod_intent is not None
    assert mod_intent.action_type == ActionType.PARTIAL_CLOSE
    assert math.isclose(mod_intent.volume, 0.05)


# =============================================================================
# 10. GOLDEN SET SAMPLE QUALITY GATE COMPLIANCE
# =============================================================================

@pytest.mark.unit
def test_golden_set_sample_100_percent_compliance():
    """Verify that SignalParser passes 100% of the sample golden set signals."""
    sample_path = Path(__file__).parent.parent.parent / "golden_set_sample.json"
    if not sample_path.exists():
        pytest.skip(f"Golden set sample file not found at {sample_path}")

    with open(sample_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    parser = SignalParser()
    signals = data.get("signals", [])
    assert len(signals) >= 5

    for item in signals:
        raw = item["raw_text"]
        exp = item["expected"]
        actual = parser.parse(raw)

        assert actual.action == exp["action"], f"{item['id']} action mismatch"
        assert actual.symbol == exp["symbol"], f"{item['id']} symbol mismatch"
        assert actual.order_type == exp["order_type"], f"{item['id']} order_type mismatch"
        assert actual.is_modification == exp["is_modification"], f"{item['id']} is_modification mismatch"

        if exp.get("entry_price") is not None:
            assert actual.entry_price is not None
            assert math.isclose(actual.entry_price, exp["entry_price"], rel_tol=1e-4)

        if exp.get("stop_loss") is not None:
            assert actual.stop_loss is not None
            assert math.isclose(actual.stop_loss, exp["stop_loss"], rel_tol=1e-4)

        if exp.get("take_profits"):
            assert len(actual.take_profits) == len(exp["take_profits"])
            for act_tp, exp_tp in zip(actual.take_profits, exp["take_profits"]):
                assert math.isclose(act_tp, exp_tp, rel_tol=1e-4)

        if exp.get("partial_close_pct") is not None:
            assert actual.partial_close_pct is not None
            assert math.isclose(actual.partial_close_pct, exp["partial_close_pct"], rel_tol=1e-4)


# =============================================================================
# 11. MARKET GEOMETRY VALIDATION TESTS (SKIP_GEOMETRY)
# =============================================================================

@pytest.mark.unit
def test_geometry_validation_valid_signals():
    """Verify that logically valid BUY and SELL signals pass geometry check."""
    parser = SignalParser()

    # Valid BUY: SL < Entry < TP1 < TP2
    buy_sig = parser.parse("BUY XAUUSD @ 2650.00 SL: 2640.00 TP1: 2660.00 TP2: 2670.00")
    valid, reason = parser.validate_geometry(buy_sig)
    assert valid is True
    assert reason is None
    assert buy_sig.geometry_valid is True
    assert buy_sig.skip_reason is None

    # Valid SELL: SL > Entry > TP1 > TP2
    sell_sig = parser.parse("SELL EURUSD @ 1.0850 SL: 1.0900 TP1: 1.0800 TP2: 1.0750")
    valid, reason = parser.validate_geometry(sell_sig)
    assert valid is True
    assert reason is None
    assert sell_sig.geometry_valid is True
    assert sell_sig.skip_reason is None


@pytest.mark.unit
def test_geometry_validation_invalid_buy_sl():
    """Verify that BUY with SL >= Entry fails with SKIP_GEOMETRY and raises GeometryValidationError."""
    parser = SignalParser()

    # Inverted SL: SL is above entry for a BUY
    raw = "BUY XAUUSD @ 2650.00 SL: 2655.00 TP: 2670.00"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    # Direct validation returning status
    valid, reason = parser.validate_geometry(sig)
    assert valid is False
    assert reason.startswith(SKIP_GEOMETRY)
    assert "below Entry" in reason

    # Enforce geometry raises GeometryValidationError with SKIP_GEOMETRY reason code
    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "BUY Stop Loss" in str(exc_info.value)

    # enforce_geometry flag on parse()
    with pytest.raises(GeometryValidationError) as exc_info2:
        parser.parse(raw, enforce_geometry=True)
    assert exc_info2.value.reason_code == SKIP_GEOMETRY


@pytest.mark.unit
def test_geometry_validation_invalid_buy_tp():
    """Verify that BUY with TP <= Entry fails with SKIP_GEOMETRY."""
    parser = SignalParser()

    # Inverted TP: TP is below entry for a BUY
    raw = "BUY XAUUSD @ 2650.00 SL: 2640.00 TP: 2645.00"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "above Entry" in str(exc_info.value)


@pytest.mark.unit
def test_geometry_validation_invalid_buy_ladder():
    """Verify that BUY with non-ascending TP ladder fails geometry."""
    parser = SignalParser()

    # Ascending broken: TP2 (2660) < TP1 (2670)
    raw = "BUY XAUUSD @ 2650.00 SL: 2640.00 TP1: 2670.00 TP2: 2660.00"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "ascending" in str(exc_info.value)


@pytest.mark.unit
def test_geometry_validation_invalid_sell_sl():
    """Verify that SELL with SL <= Entry fails with SKIP_GEOMETRY."""
    parser = SignalParser()

    # Inverted SL: SL is below entry for a SELL
    raw = "SELL EURUSD @ 1.08500 SL: 1.08000 TP: 1.07000"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "above Entry" in str(exc_info.value)


@pytest.mark.unit
def test_geometry_validation_invalid_sell_tp():
    """Verify that SELL with TP >= Entry fails with SKIP_GEOMETRY."""
    parser = SignalParser()

    # Inverted TP: TP is above entry for a SELL
    raw = "SELL EURUSD @ 1.08500 SL: 1.09000 TP: 1.09500"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "below Entry" in str(exc_info.value)


@pytest.mark.unit
def test_geometry_validation_invalid_sell_ladder():
    """Verify that SELL with non-descending TP ladder fails geometry."""
    parser = SignalParser()

    # Descending broken: TP2 (1.0820) > TP1 (1.0800)
    raw = "SELL EURUSD @ 1.08500 SL: 1.09000 TP1: 1.08000 TP2: 1.08200"
    sig = parser.parse(raw)
    assert sig.geometry_valid is False
    assert sig.skip_reason == SKIP_GEOMETRY

    with pytest.raises(GeometryValidationError) as exc_info:
        parser.validate_geometry(sig, raise_exception=True)
    assert exc_info.value.reason_code == SKIP_GEOMETRY
    assert "descending" in str(exc_info.value)


@pytest.mark.unit
def test_geometry_validation_omitted_entry():
    """Verify validation when entry price is omitted (market execution)."""
    parser = SignalParser()

    # BUY without entry: TP must be > SL
    sig_ok = parser.parse("BUY EURUSD SL: 1.0800 TP: 1.0900")
    assert sig_ok.geometry_valid is True

    sig_bad = parser.parse("BUY EURUSD SL: 1.0900 TP: 1.0800")
    assert sig_bad.geometry_valid is False
    assert sig_bad.skip_reason == SKIP_GEOMETRY

    # SELL without entry: TP must be < SL
    sell_ok = parser.parse("SELL EURUSD SL: 1.0900 TP: 1.0800")
    assert sell_ok.geometry_valid is True

    sell_bad = parser.parse("SELL EURUSD SL: 1.0800 TP: 1.0900")
    assert sell_bad.geometry_valid is False
    assert sell_bad.skip_reason == SKIP_GEOMETRY


# =============================================================================
# 12. CONFIDENCE SCORING & COMPLETENESS PENALTIES
# =============================================================================

@pytest.mark.unit
def test_confidence_scoring_full_signal():
    """Verify 100 confidence score on fully extracted signals."""
    parser = SignalParser()
    sig = parser.parse("BUY XAUUSD @ 2650.00 SL: 2640.00 TP: 2670.00")
    assert sig.confidence_score == 100
    assert sig.confidence == 1.0


@pytest.mark.unit
def test_confidence_scoring_missing_sl_penalty():
    """Verify -30 penalty when Stop Loss is omitted."""
    parser = SignalParser()
    sig = parser.parse("BUY XAUUSD @ 2650.00 TP: 2670.00")
    assert sig.stop_loss is None
    assert sig.confidence_score == 70  # 100 - 30
    assert sig.confidence == 0.70


@pytest.mark.unit
def test_confidence_scoring_missing_tp_penalty():
    """Verify -10 penalty when Take Profit is omitted."""
    parser = SignalParser()
    sig = parser.parse("BUY XAUUSD @ 2650.00 SL: 2640.00")
    assert not sig.take_profits
    assert sig.confidence_score == 90  # 100 - 10
    assert sig.confidence == 0.90


@pytest.mark.unit
def test_confidence_scoring_missing_both_sl_and_tp():
    """Verify -40 penalty (-30 for SL, -10 for TP) when both omitted."""
    parser = SignalParser()
    sig = parser.parse("BUY XAUUSD @ 2650.00")
    assert sig.confidence_score == 60  # 100 - 30 - 10
    assert sig.confidence == 0.60


@pytest.mark.unit
def test_confidence_scoring_missing_entry_penalty():
    """Verify entry omission penalties (-15 for market, -25 for pending)."""
    parser = SignalParser()

    # Market without explicit entry price: -15
    sig_mkt = parser.parse("BUY XAUUSD SL: 2640.00 TP: 2670.00")
    assert sig_mkt.order_type == "MARKET"
    assert sig_mkt.entry_price is None
    assert sig_mkt.confidence_score == 85  # 100 - 15

    # Limit without entry price: -25
    sig_lim = parser.parse("BUY LIMIT XAUUSD SL: 2640.00 TP: 2670.00")
    assert sig_lim.order_type == "LIMIT"
    assert sig_lim.entry_price is None
    assert sig_lim.confidence_score == 75  # 100 - 25


@pytest.mark.unit
def test_confidence_scoring_failed_geometry_penalty():
    """Verify -50 penalty when signal fails market geometry."""
    parser = SignalParser()
    # Inverted SL: SL above entry on BUY -> 100 - 50 = 50
    sig = parser.parse("BUY XAUUSD @ 2650.00 SL: 2660.00 TP: 2670.00")
    assert sig.geometry_valid is False
    assert sig.confidence_score == 50  # 100 - 50
    assert sig.confidence == 0.50


@pytest.mark.unit
def test_confidence_scoring_chatter():
    """Verify conversational messages receive 0 or 10 confidence score."""
    parser = SignalParser()
    chat1 = parser.parse("Good morning traders, let's have a great week!")
    assert chat1.message_type == "chatter"
    assert chat1.confidence_score == 0

    # Chatter with symbol mention receives nominal 10
    chat2 = parser.parse("Gold XAUUSD is looking very interesting today on the H4 chart!")
    assert chat2.message_type == "chatter"
    assert chat2.confidence_score == 10


# =============================================================================
# 13. MANAGEMENT MESSAGES & ORDERINTENT TRANSLATION
# =============================================================================

@pytest.mark.unit
def test_management_message_close_half():
    """Verify 'Close half' generates PARTIAL_CLOSE intent with 50% volume."""
    parser = SignalParser()
    raw = "XAUUSD CLOSE HALF NOW"
    sig = parser.parse(raw)

    assert sig.is_modification is True
    assert sig.partial_close_pct == 50.0
    assert sig.symbol == "XAUUSD"

    intent = sig.to_order_intent(volume=0.20)
    assert intent is not None
    assert intent.action_type == ActionType.PARTIAL_CLOSE
    assert math.isclose(intent.volume, 0.10)
    assert intent.symbol == "XAUUSD"


@pytest.mark.unit
def test_management_message_move_sl_to_entry():
    """Verify 'Move SL to Entry' generates MODIFY_SL_TP intent with BE flag."""
    parser = SignalParser()
    raw = "EURUSD MOVE SL TO ENTRY"
    sig = parser.parse(raw)

    assert sig.is_modification is True
    assert sig.move_to_be is True
    assert sig.symbol == "EURUSD"

    intent = sig.to_order_intent(volume=0.10)
    assert intent is not None
    assert intent.action_type == ActionType.MODIFY_SL_TP
    assert intent.symbol == "EURUSD"


@pytest.mark.unit
def test_management_message_close_all():
    """Verify 'Close all' generates FULL_CLOSE intent."""
    parser = SignalParser()
    raw = "CLOSE ALL US30 NOW"
    sig = parser.parse(raw)

    assert sig.is_modification is True
    assert sig.full_close is True
    assert sig.symbol == "US30"

    intent = sig.to_order_intent(volume=0.10)
    assert intent is not None
    assert intent.action_type == ActionType.FULL_CLOSE
    assert intent.symbol == "US30"


@pytest.mark.unit
def test_management_message_cancel_pending():
    """Verify 'Cancel order' generates CANCEL_PENDING intent."""
    parser = SignalParser()
    raw = "CANCEL PENDING ORDER EURUSD"
    sig = parser.parse(raw)

    assert sig.is_modification is True
    assert sig.cancel_pending is True
    assert sig.symbol == "EURUSD"

    intent = sig.to_order_intent(volume=0.10)
    assert intent is not None
    assert intent.action_type == ActionType.CANCEL_PENDING
    assert intent.symbol == "EURUSD"

