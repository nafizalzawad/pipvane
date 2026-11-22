#!/usr/bin/env python3
"""
Pipvane v3: Golden Set Telegram Signal Parser CI Gate
-----------------------------------------------------
Enforces the mandatory >= 98% accuracy threshold against the Golden Set of signals.
Fails the CI/CD pipeline if parsing accuracy drops below this quality gate.

Compliance:
- Sub-5ms engine execution compatibility
- Non-standard formatting (emojis, multi-language/Arabic-Indic numerals, TP ladders)
- Standalone CLI execution & Pytest test runner integration
"""

import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# Unicode Arabic-Indic digit translation table: ٠١٢٣٤٥٦٧٨٩ -> 0123456789
ARABIC_INDIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


@dataclass
class ParsedSignal:
    action: str
    symbol: str
    order_type: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profits: List[float] = field(default_factory=list)
    is_modification: bool = False
    partial_close_pct: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "symbol": self.symbol,
            "order_type": self.order_type,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "take_profits": self.take_profits,
            "is_modification": self.is_modification,
            "partial_close_pct": self.partial_close_pct,
        }


class BaselineSignalParser:
    """
    Production-grade baseline parser implementing multi-lingual signal extraction,
    Arabic-Indic numeral normalization, multi-TP ladder recognition, and modification logic.
    """

    KNOWN_SYMBOLS = [
        "XAUUSD", "GOLD", "EURUSD", "GBPUSD", "USDJPY",
        "GBPJPY", "US30", "NAS100", "BTCUSD", "ETHUSD"
    ]

    ACTION_MAP = {
        "BUY": "BUY",
        "شراء": "BUY",
        "LONG": "BUY",
        "SELL": "SELL",
        "بيع": "SELL",
        "SHORT": "SELL",
        "MODIFY": "MODIFY",
        "UPDATE": "MODIFY",
        "تعديل": "MODIFY",
    }

    def __init__(self):
        # Precompile standard regexes
        self.num_pattern = re.compile(r"[-+]?\d*\.?\d+")

    def normalize_text(self, text: str) -> str:
        """Normalize unicode Arabic-Indic digits, standardize colons, and trim."""
        text = text.translate(ARABIC_INDIC_DIGITS)
        # Normalize various colon styles
        text = text.replace("：", ":")
        return text

    def parse(self, raw_text: str) -> ParsedSignal:
        normalized = self.normalize_text(raw_text)

        # 1. Detect Symbol
        symbol = None
        for sym in self.KNOWN_SYMBOLS:
            # Match whole word symbol
            if re.search(rf"\b{re.escape(sym)}\b", normalized, re.IGNORECASE):
                symbol = "XAUUSD" if sym.upper() == "GOLD" else sym.upper()
                break
        if not symbol:
            symbol = "UNKNOWN"

        # 2. Detect Action & Modification
        action = "BUY"
        is_mod = False
        if re.search(r"\b(MODIFY|UPDATE|تعديل)\b", normalized, re.IGNORECASE):
            action = "MODIFY"
            is_mod = True
        elif re.search(r"\b(SELL|بيع|SHORT)\b", normalized, re.IGNORECASE):
            action = "SELL"
        elif re.search(r"\b(BUY|شراء|LONG)\b", normalized, re.IGNORECASE):
            action = "BUY"

        # 3. Detect Order Type
        order_type = "MARKET"
        if is_mod:
            order_type = "UPDATE"
        elif re.search(r"\bLIMIT\b", normalized, re.IGNORECASE):
            order_type = "LIMIT"
        elif re.search(r"\bSTOP\b", normalized, re.IGNORECASE) and not re.search(r"\b(STOP[\s_-]?LOSS|SL)\b", normalized, re.IGNORECASE):
            order_type = "STOP"

        # 4. Extract Entry Price
        entry_price: Optional[float] = None
        if not is_mod:
            entry_match = re.search(
                r"(?:@|at|دخول[:\s]*|price[:\s]*|entry[:\s]*)\s*([0-9]+\.?[0-9]*)",
                normalized,
                re.IGNORECASE,
            )
            if entry_match:
                entry_price = float(entry_match.group(1))
            else:
                # Look for standalone price after action/symbol
                words = normalized.split()
                for i, w in enumerate(words):
                    if w.upper() in [symbol, "BUY", "SELL", "LIMIT", "STOP"]:
                        for next_w in words[i + 1: i + 3]:
                            clean_val = re.sub(r"[^\d.]", "", next_w)
                            if clean_val and self._is_valid_float(clean_val):
                                entry_price = float(clean_val)
                                break
                    if entry_price is not None:
                        break

        # 5. Extract Stop Loss
        stop_loss: Optional[float] = None
        sl_match = re.search(
            r"(?:SL|STOP[\s_-]?LOSS|وقف\s*الخسارة)[:\s]*([TOto\s]*)([0-9]+\.?[0-9]*)",
            normalized,
            re.IGNORECASE,
        )
        if sl_match:
            sl_val = sl_match.group(2)
            if sl_val and self._is_valid_float(sl_val):
                stop_loss = float(sl_val)

        # 6. Extract Take Profits (supports multi-TP ladders)
        take_profits: List[float] = []
        tp_matches = re.finditer(
            r"(?:TP\d?|TAKE[\s_-]?PROFIT\d?|هدف\s*(?:أول|ثان|ثالث|رابع|\d)?)[:\s]*([0-9]+\.?[0-9]*)",
            normalized,
            re.IGNORECASE,
        )
        for m in tp_matches:
            val_str = m.group(1)
            if val_str and self._is_valid_float(val_str):
                val = float(val_str)
                if val not in take_profits:
                    take_profits.append(val)

        # 7. Extract Partial Close / Modification Details
        partial_close_pct: Optional[float] = None
        close_match = re.search(r"CLOSE\s*([0-9]+(?:\.[0-9]+)?)\s*%", normalized, re.IGNORECASE)
        if close_match:
            partial_close_pct = float(close_match.group(1))

        return ParsedSignal(
            action=action,
            symbol=symbol,
            order_type=order_type,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profits=take_profits,
            is_modification=is_mod,
            partial_close_pct=partial_close_pct,
        )

    @staticmethod
    def _is_valid_float(val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False


def compare_signal_fields(actual: ParsedSignal, expected: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Compare parsed signal with expected golden set schema, with floating point tolerances."""
    errors = []

    if actual.action != expected.get("action"):
        errors.append(f"Action mismatch: got '{actual.action}', expected '{expected.get('action')}'")

    if actual.symbol != expected.get("symbol"):
        errors.append(f"Symbol mismatch: got '{actual.symbol}', expected '{expected.get('symbol')}'")

    if actual.order_type != expected.get("order_type"):
        errors.append(f"OrderType mismatch: got '{actual.order_type}', expected '{expected.get('order_type')}'")

    # Compare Entry Price
    exp_entry = expected.get("entry_price")
    if exp_entry is not None:
        if actual.entry_price is None or not math.isclose(actual.entry_price, exp_entry, rel_tol=1e-4):
            errors.append(f"EntryPrice mismatch: got {actual.entry_price}, expected {exp_entry}")
    elif actual.entry_price is not None:
        errors.append(f"EntryPrice expected None, got {actual.entry_price}")

    # Compare Stop Loss
    exp_sl = expected.get("stop_loss")
    if exp_sl is not None:
        if actual.stop_loss is None or not math.isclose(actual.stop_loss, exp_sl, rel_tol=1e-4):
            errors.append(f"StopLoss mismatch: got {actual.stop_loss}, expected {exp_sl}")
    elif actual.stop_loss is not None:
        errors.append(f"StopLoss expected None, got {actual.stop_loss}")

    # Compare Take Profits Ladder
    exp_tps = expected.get("take_profits", [])
    if len(actual.take_profits) != len(exp_tps):
        errors.append(f"TP count mismatch: got {len(actual.take_profits)} ({actual.take_profits}), expected {len(exp_tps)} ({exp_tps})")
    else:
        for i, (act_tp, exp_tp) in enumerate(zip(actual.take_profits, exp_tps)):
            if not math.isclose(act_tp, exp_tp, rel_tol=1e-4):
                errors.append(f"TP[{i}] mismatch: got {act_tp}, expected {exp_tp}")

    # Compare Modification details
    if actual.is_modification != expected.get("is_modification", False):
        errors.append(f"is_modification mismatch: got {actual.is_modification}, expected {expected.get('is_modification')}")

    exp_pct = expected.get("partial_close_pct")
    if exp_pct is not None:
        if actual.partial_close_pct is None or not math.isclose(actual.partial_close_pct, exp_pct, rel_tol=1e-4):
            errors.append(f"partial_close_pct mismatch: got {actual.partial_close_pct}, expected {exp_pct}")
    elif actual.partial_close_pct is not None:
        errors.append(f"partial_close_pct expected None, got {actual.partial_close_pct}")

    return len(errors) == 0, errors


def run_parser_gate(golden_set_path: Optional[str] = None) -> Dict[str, Any]:
    """Execute parser against the golden set and compute accuracy metrics."""
    if not golden_set_path:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        golden_set_path = os.path.join(base_dir, "golden_set_sample.json")

    if not os.path.exists(golden_set_path):
        raise FileNotFoundError(f"Golden Set JSON not found at: {golden_set_path}")

    with open(golden_set_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    signals = data.get("signals", [])
    threshold = data.get("metadata", {}).get("target_accuracy_threshold", 0.98)

    parser = BaselineSignalParser()
    results = []
    passed_count = 0

    print("=" * 78)
    print(" PIPVANE v3 - PARSER CI QUALITY GATE EVALUATION")
    print("=" * 78)
    print(f"Target Accuracy Threshold: {threshold * 100:.1f}%")
    print(f"Total Test Signals:        {len(signals)}")
    print("-" * 78)

    for item in signals:
        sig_id = item["id"]
        category = item["category"]
        raw = item["raw_text"]
        expected = item["expected"]

        actual = parser.parse(raw)
        success, errors = compare_signal_fields(actual, expected)

        if success:
            passed_count += 1
            print(f" [PASS] {sig_id:<14} | Category: {category:<25}")
        else:
            print(f" [FAIL] {sig_id:<14} | Category: {category:<25}")
            for err in errors:
                print(f"        -> {err}")

        results.append({
            "id": sig_id,
            "category": category,
            "passed": success,
            "errors": errors,
            "actual": actual.to_dict(),
            "expected": expected,
        })

    accuracy = (passed_count / len(signals)) if signals else 0.0
    passed_gate = accuracy >= threshold

    print("-" * 78)
    print(f"Accuracy Achieved: {accuracy * 100:.2f}% ({passed_count}/{len(signals)} passed)")
    print(f"Gate Status:       {'SUCCESS (PASSED)' if passed_gate else 'FAILED - BUILD REJECTED'}")
    print("=" * 78)

    summary = {
        "accuracy": accuracy,
        "passed_count": passed_count,
        "total_count": len(signals),
        "threshold": threshold,
        "passed_gate": passed_gate,
        "results": results,
    }

    # Save summary report for CI
    report_file = os.path.join(os.path.dirname(golden_set_path), "parser_gate_results.json")
    with open(report_file, "w", encoding="utf-8") as rf:
        json.dump(summary, rf, indent=2)

    return summary


# Pytest entrypoint
def test_parser_gate_accuracy():
    """Pytest test case asserting parser accuracy >= 98% gate."""
    summary = run_parser_gate()
    assert summary["passed_gate"], (
        f"Parser accuracy ({summary['accuracy'] * 100:.2f}%) did not meet "
        f"the mandatory gate threshold of {summary['threshold'] * 100:.1f}%"
    )


if __name__ == "__main__":
    summary = run_parser_gate()
    if not summary["passed_gate"]:
        sys.exit(1)
    sys.exit(0)
