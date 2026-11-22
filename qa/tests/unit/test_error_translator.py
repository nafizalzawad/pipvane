"""
Unit tests for Sprint 23: Broker Error Translation Subsystem.
=============================================================
Validates:
1. Exact mapping of MT4 Error 130:
   - Plain English: "Stop Loss is too close to the current price. Broker rejected the trade."
   - Next Action: "Check your Broker's Freeze Levels in Settings"
2. Exact mapping of MT5 Error 10015:
   - Plain English: "Invalid price. The requested entry price is no longer available."
   - Next Action: "Increase slippage tolerance or enable Market Execution mode in Risk Settings."
3. Comprehensive coverage of all MT4, MT5, and cTrader error codes with actionable next actions.
4. Auto-detection of broker platform from code range and string patterns.
5. Integration into NotificationEngine, NotificationFormatter, and explain_skip_reason.
"""

from __future__ import annotations

import asyncio
import pytest
import time
from typing import Dict, Any

from engine.error_translator import (
    ErrorTranslator,
    TranslatedError,
    translate_error,
    MT4_ERROR_DICT,
    MT5_RETCODE_DICT,
    CTRADER_ERROR_DICT,
)
from engine.notification_engine import (
    NotificationEngine,
    NotificationEvent,
    NotificationEventType,
    NotificationFormatter,
    explain_skip_reason,
)
from engine.risk_engine import SkipReason


# =============================================================================
# 1. Core Directive Mappings: MT4 130 and MT5 10015
# =============================================================================

def test_mt4_error_130_directive_mapping():
    """
    Directive Requirement:
    Map MT4 Error 130 to:
    - "Stop Loss is too close to the current price. Broker rejected the trade."
    - Next Action: "Check your Broker's Freeze Levels in Settings"
    """
    res = ErrorTranslator.translate(130, platform="MT4")
    assert res.code == 130
    assert res.platform == "MT4"
    assert res.raw_name == "ERR_INVALID_STOPS"
    assert res.plain_english == "Stop Loss is too close to the current price. Broker rejected the trade."
    assert res.next_action == "Check your Broker's Freeze Levels in Settings"


def test_mt5_error_10015_directive_mapping():
    """
    Directive Requirement:
    Map MT5 Error 10015 to:
    - "Invalid price. The requested entry price is no longer available."
    - Next Action contains actionable guidance on slippage / execution mode.
    """
    res = ErrorTranslator.translate(10015, platform="MT5")
    assert res.code == 10015
    assert res.platform == "MT5"
    assert res.raw_name == "TRADE_RETCODE_INVALID_PRICE"
    assert res.plain_english == "Invalid price. The requested entry price is no longer available."
    assert "Risk Settings" in res.next_action
    assert "slippage" in res.next_action.lower()


# =============================================================================
# 2. Comprehensive MT4 Error Code Dictionary Coverage
# =============================================================================

def test_mt4_error_dictionary_completeness():
    """Verifies that all MT4 dictionary entries have valid plain English and actionable Next Action."""
    assert len(MT4_ERROR_DICT) >= 25, "MT4 dictionary must cover standard MQL4 error return codes"

    sample_codes = [0, 1, 2, 3, 4, 6, 7, 64, 65, 128, 129, 130, 131, 132, 133, 134, 135, 136, 138, 146, 148, 149, 150]
    for code in sample_codes:
        assert code in MT4_ERROR_DICT, f"Code {code} missing from MT4_ERROR_DICT"
        trans = ErrorTranslator.translate(code, platform="MT4")
        assert trans.platform == "MT4"
        assert trans.plain_english != ""
        assert trans.next_action != ""
        assert len(trans.next_action) > 10, f"Next action for MT4 #{code} must be substantive and actionable"


def test_mt4_specific_critical_errors():
    """Verifies specific high-impact MT4 error explanations."""
    # 134: Not Enough Money
    trans_134 = translate_error(134, platform="MT4")
    assert "margin" in trans_134.plain_english.lower()
    assert "lot size" in trans_134.next_action.lower() or "deposit" in trans_134.next_action.lower()

    # 132: Market Closed
    trans_132 = translate_error(132, platform="MT4")
    assert "closed" in trans_132.plain_english.lower()

    # 150: FIFO Prohibited
    trans_150 = translate_error(150, platform="MT4")
    assert "fifo" in trans_150.plain_english.lower()
    assert "FIFO" in trans_150.next_action


# =============================================================================
# 3. Comprehensive MT5 Return Code Dictionary Coverage
# =============================================================================

def test_mt5_retcode_dictionary_completeness():
    """Verifies that all MT5 dictionary entries have valid plain English and actionable Next Action."""
    assert len(MT5_RETCODE_DICT) >= 30, "MT5 dictionary must cover MQL5 trade return codes"

    sample_codes = [
        10004, 10006, 10007, 10008, 10009, 10010, 10011, 10012, 10013, 10014,
        10015, 10016, 10017, 10018, 10019, 10020, 10021, 10026, 10027, 10029,
        10031, 10034, 10044, 10045,
    ]
    for code in sample_codes:
        assert code in MT5_RETCODE_DICT, f"Code {code} missing from MT5_RETCODE_DICT"
        trans = ErrorTranslator.translate(code, platform="MT5")
        assert trans.platform == "MT5"
        assert trans.plain_english != ""
        assert trans.next_action != ""
        assert len(trans.next_action) > 10


def test_mt5_specific_critical_errors():
    """Verifies high-impact MT5 error advice."""
    # 10027: Algo Trading Disabled in Client
    trans_10027 = translate_error(10027, platform="MT5")
    assert "Algo Trading" in trans_10027.plain_english or "AutoTrading" in trans_10027.plain_english
    assert "toolbar" in trans_10027.next_action.lower() or "button" in trans_10027.next_action.lower()

    # 10016: Invalid Stops
    trans_10016 = translate_error(10016, platform="MT5")
    assert "Stops Level" in trans_10016.plain_english or "distance" in trans_10016.plain_english.lower()
    assert "Settings" in trans_10016.next_action

    # 10019: No Money
    trans_10019 = translate_error(10019, platform="MT5")
    assert "margin" in trans_10019.plain_english.lower()


# =============================================================================
# 4. cTrader / Open API Dictionary Coverage
# =============================================================================

def test_ctrader_error_dictionary_coverage():
    """Verifies cTrader error translations and actionable advice."""
    assert len(CTRADER_ERROR_DICT) >= 10

    ct_codes = ["MARKET_CLOSED", "NOT_ENOUGH_MONEY", "INVALID_REQUEST", "POSITION_NOT_FOUND", "INVALID_STOPS", "RATE_LIMIT_EXCEEDED"]
    for code in ct_codes:
        trans = ErrorTranslator.translate(code, platform="cTrader")
        assert trans.platform == "cTrader"
        assert trans.plain_english != ""
        assert trans.next_action != ""

    # Prefix OA_ tolerance
    trans_oa = ErrorTranslator.translate("OA_MARKET_CLOSED")
    assert trans_oa.platform == "cTrader"
    assert "closed" in trans_oa.plain_english.lower()


# =============================================================================
# 5. Platform Detection & Helpers
# =============================================================================

def test_platform_auto_detection():
    """Verifies heuristic detection from integer ranges and string formats."""
    # 130 is in MT4 range (< 1000)
    assert ErrorTranslator.detect_platform(130) == "MT4"
    # 10015 is in MT5 range (10000..19999)
    assert ErrorTranslator.detect_platform(10015) == "MT5"
    # String code is in cTrader dictionary
    assert ErrorTranslator.detect_platform("INSUFFICIENT_MARGIN") == "cTrader"

    # String digits
    assert ErrorTranslator.detect_platform("134") == "MT4"
    assert ErrorTranslator.detect_platform("10018") == "MT5"

    # Explicit override takes priority
    assert ErrorTranslator.detect_platform(130, hint_platform="MT5") == "MT5"


def test_actionable_next_action_guarantee():
    """Validates the strict invariant: every single error in all 3 dictionaries provides a Next Action."""
    for code, spec in MT4_ERROR_DICT.items():
        assert "next_action" in spec and len(spec["next_action"].strip()) > 0, f"MT4 #{code} missing next_action"
    for code, spec in MT5_RETCODE_DICT.items():
        assert "next_action" in spec and len(spec["next_action"].strip()) > 0, f"MT5 #{code} missing next_action"
    for code, spec in CTRADER_ERROR_DICT.items():
        assert "next_action" in spec and len(spec["next_action"].strip()) > 0, f"cTrader '{code}' missing next_action"


def test_format_alert_text():
    """Tests clean formatted alert output."""
    alert = ErrorTranslator.format_alert_text(
        code=130,
        symbol="EURUSD",
        platform="MT4",
        terminal_id="mt4_term_01",
    )
    assert "❌ BROKER REJECTION (MT4 #130: ERR_INVALID_STOPS)" in alert
    assert "Symbol: EURUSD" in alert
    assert "Terminal: mt4_term_01" in alert
    assert "Stop Loss is too close to the current price. Broker rejected the trade." in alert
    assert "Next Action: Check your Broker's Freeze Levels in Settings" in alert


# =============================================================================
# 6. Integration into NotificationEngine & Formatters
# =============================================================================

def test_notification_formatter_broker_error_plain_and_discord():
    """Verifies that NotificationFormatter produces clean alerts for BROKER_ERROR events."""
    event = NotificationEvent(
        event_type=NotificationEventType.BROKER_ERROR,
        data={
            "symbol": "XAUUSD",
            "broker_retcode": 130,
            "platform": "MT4",
            "terminal_id": "mt4_vip",
        },
    )

    plain = NotificationFormatter.format_plain(event)
    assert "❌ BROKER REJECTION | XAUUSD | Terminal: mt4_vip" in plain
    assert "Stop Loss is too close to the current price. Broker rejected the trade." in plain
    assert "Next Action: Check your Broker's Freeze Levels in Settings" in plain

    discord = NotificationFormatter.format_discord(event)
    embed = discord["embeds"][0]
    assert "❌ Broker Error: XAUUSD" in embed["title"]
    field_names = [f["name"] for f in embed["fields"]]
    assert "Broker Code" in field_names
    assert "Plain English Issue" in field_names
    assert "Next Action" in field_names


def test_explain_skip_reason_with_broker_retcode():
    """
    Verifies that explain_skip_reason automatically translates SkipReason objects
    carrying a broker_retcode into plain language and actionable advice.
    """
    # MT4 130 in details
    skip_130 = SkipReason(
        code="SKIP_BROKER_REJECTED",
        reason="Broker rejected order: ERR_INVALID_STOPS (retcode=130)",
        evaluated_at=time.time(),
        details={"broker_retcode": 130, "platform": "MT4"},
    )
    explanation = explain_skip_reason(skip_130)
    assert "Stop Loss is too close to the current price. Broker rejected the trade." in explanation
    assert "Check your Broker's Freeze Levels in Settings" in explanation

    # MT5 10015 in details
    skip_10015 = SkipReason(
        code="SKIP_BROKER_REJECTED",
        reason="Broker rejected order: TRADE_RETCODE_INVALID_PRICE (retcode=10015)",
        evaluated_at=time.time(),
        details={"broker_retcode": 10015, "platform": "MT5"},
    )
    explanation_5 = explain_skip_reason(skip_10015)
    assert "Invalid price. The requested entry price is no longer available." in explanation_5
    assert "Risk Settings" in explanation_5


@pytest.mark.asyncio
async def test_notification_engine_notify_broker_error_dispatch():
    """
    Verifies that NotificationEngine.notify_broker_error dispatches a clean event
    with actionable advice through listener callbacks.
    """
    engine = NotificationEngine()
    received_events = []

    def _test_listener(event: NotificationEvent):
        received_events.append(event)

    engine.add_listener(_test_listener)
    await engine.start()

    try:
        engine.notify_broker_error(
            symbol="GBPUSD",
            error_code=10015,
            platform="MT5",
            terminal_id="mt5_live",
        )

        # Allow worker queue to drain
        for _ in range(20):
            if received_events:
                break
            await asyncio.sleep(0.05)

        assert len(received_events) == 1
        evt = received_events[0]
        assert evt.event_type == NotificationEventType.BROKER_ERROR
        assert evt.data["symbol"] == "GBPUSD"
        assert evt.data["broker_retcode"] == 10015
        assert evt.data["plain_english"] == "Invalid price. The requested entry price is no longer available."
        assert "Risk Settings" in evt.data["next_action"]
    finally:
        await engine.stop()
