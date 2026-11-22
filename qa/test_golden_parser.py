#!/usr/bin/env python3
"""
Pipvane v3: Golden Set Parser Accuracy & Regression CI Gate.
------------------------------------------------------------
Validates TextNormalizer and SignalParser against 1,500 anonymized real-world
Telegram signals.

Mandatory Quality Gates:
1. Overall accuracy must be >= 98.0% (target_accuracy_threshold).
2. Regression rule: Build strictly fails if accuracy regresses by more than 0.5%
   (0.005) from the previous main branch build baseline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

# Ensure repository root is in sys.path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from engine.normalizer import TextNormalizer  # noqa: E402
from engine.signal_parser import SignalParser, ParsedSignal  # noqa: E402

DEFAULT_THRESHOLD: float = 0.98
MAX_REGRESSION_ALLOWANCE: float = 0.005  # 0.5% max drop


@dataclass
class ComparisonResult:
    """Detailed result of comparing a single signal against its expected schema."""
    signal_id: str
    category: str
    passed: bool
    errors: List[str]
    actual: Dict[str, Any]
    expected: Dict[str, Any]


def compare_signal_fields(actual: ParsedSignal, expected: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Compares actual extracted fields against expected golden set values."""
    errors: List[str] = []

    # 1. Action
    exp_action = expected.get("action")
    if actual.action != exp_action:
        errors.append(f"Action mismatch: got '{actual.action}', expected '{exp_action}'")

    # 2. Symbol
    exp_symbol = expected.get("symbol")
    if actual.symbol != exp_symbol:
        errors.append(f"Symbol mismatch: got '{actual.symbol}', expected '{exp_symbol}'")

    # 3. Order Type
    exp_order_type = expected.get("order_type")
    if actual.order_type != exp_order_type:
        errors.append(f"OrderType mismatch: got '{actual.order_type}', expected '{exp_order_type}'")

    # 4. Entry Price
    exp_entry = expected.get("entry_price")
    if exp_entry is not None:
        if actual.entry_price is None or not math.isclose(actual.entry_price, exp_entry, rel_tol=1e-4):
            errors.append(f"EntryPrice mismatch: got {actual.entry_price}, expected {exp_entry}")
    elif actual.entry_price is not None:
        errors.append(f"EntryPrice expected None, got {actual.entry_price}")

    # 5. Stop Loss
    exp_sl = expected.get("stop_loss")
    if exp_sl is not None:
        if actual.stop_loss is None or not math.isclose(actual.stop_loss, exp_sl, rel_tol=1e-4):
            errors.append(f"StopLoss mismatch: got {actual.stop_loss}, expected {exp_sl}")
    elif actual.stop_loss is not None:
        errors.append(f"StopLoss expected None, got {actual.stop_loss}")

    # 6. Take Profits Ladder
    exp_tps = expected.get("take_profits", [])
    if len(actual.take_profits) != len(exp_tps):
        errors.append(
            f"TakeProfits count mismatch: got {len(actual.take_profits)} ({actual.take_profits}), "
            f"expected {len(exp_tps)} ({exp_tps})"
        )
    else:
        for i, (act_tp, exp_tp) in enumerate(zip(actual.take_profits, exp_tps)):
            if not math.isclose(act_tp, exp_tp, rel_tol=1e-4):
                errors.append(f"TakeProfit[{i}] mismatch: got {act_tp}, expected {exp_tp}")

    # 7. Modification details
    exp_is_mod = expected.get("is_modification", False)
    if actual.is_modification != exp_is_mod:
        errors.append(f"is_modification mismatch: got {actual.is_modification}, expected {exp_is_mod}")

    exp_pct = expected.get("partial_close_pct")
    if exp_pct is not None:
        if actual.partial_close_pct is None or not math.isclose(actual.partial_close_pct, exp_pct, rel_tol=1e-4):
            errors.append(f"partial_close_pct mismatch: got {actual.partial_close_pct}, expected {exp_pct}")
    elif actual.partial_close_pct is not None:
        errors.append(f"partial_close_pct expected None, got {actual.partial_close_pct}")

    return len(errors) == 0, errors


def load_dataset(dataset_path: Optional[str] = None) -> Tuple[List[Dict[str, Any]], float, str]:
    """Resolves and loads the golden dataset JSON file."""
    if not dataset_path:
        dataset_path = os.environ.get("GOLDEN_SET_PATH")

    if not dataset_path:
        # Check standard locations
        candidates = [
            os.path.join(os.path.dirname(__file__), "golden_set_1500.json"),
            os.path.join(os.path.dirname(__file__), "golden_set_sample.json"),
            os.path.join(os.getcwd(), "qa", "golden_set_1500.json"),
            os.path.join(os.getcwd(), "qa", "golden_set_sample.json"),
        ]
        for c in candidates:
            if os.path.exists(c):
                dataset_path = c
                break

    if not dataset_path or not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Golden Set JSON dataset not found at: {dataset_path}")

    with open(dataset_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    signals = data.get("signals", [])
    threshold = float(data.get("metadata", {}).get("target_accuracy_threshold", DEFAULT_THRESHOLD))
    return signals, threshold, dataset_path


def load_baseline_accuracy(baseline_path: Optional[str] = None) -> Optional[float]:
    """Loads previous main branch accuracy baseline from environment, file, or parameter."""
    # 1. Environment variable override
    env_val = os.environ.get("PREVIOUS_MAIN_ACCURACY")
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            pass

    # 2. File lookup
    if not baseline_path:
        candidates = [
            os.path.join(os.path.dirname(__file__), "baseline_accuracy.json"),
            os.path.join(os.getcwd(), "qa", "baseline_accuracy.json"),
        ]
        for c in candidates:
            if os.path.exists(c):
                baseline_path = c
                break

    if baseline_path and os.path.exists(baseline_path):
        try:
            with open(baseline_path, "r", encoding="utf-8") as f:
                bdata = json.load(f)
                return float(bdata.get("accuracy", bdata.get("baseline_accuracy", 0.985)))
        except Exception:
            pass

    return None


def run_golden_parser_evaluation(
    dataset_path: Optional[str] = None,
    baseline_accuracy: Optional[float] = None,
    threshold: Optional[float] = None,
    max_regression: float = MAX_REGRESSION_ALLOWANCE,
    output_report_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Executes the comprehensive 1,500-signal evaluation through TextNormalizer and SignalParser.
    Computes accuracy, enforces the 98% threshold, and checks against regression limit.
    """
    signals, loaded_threshold, resolved_dataset_path = load_dataset(dataset_path)
    if threshold is None:
        threshold = loaded_threshold
    if baseline_accuracy is None:
        baseline_accuracy = load_baseline_accuracy()

    parser = SignalParser()
    normalizer = TextNormalizer()
    passed_count = 0
    results: List[Dict[str, Any]] = []
    category_stats: Dict[str, Dict[str, int]] = {}

    print("=" * 80)
    print(" PIPVANE v3 - GOLDEN SET 1,500 PARSER ACCURACY & REGRESSION CI GATE")
    print("=" * 80)
    print(f"Dataset Loaded:             {resolved_dataset_path}")
    print(f"Total Signals to Evaluate:  {len(signals)}")
    print(f"Target Accuracy Gate:       {threshold * 100:.1f}%")
    if baseline_accuracy is not None:
        print(f"Previous Main Baseline:     {baseline_accuracy * 100:.2f}% (Max Allowed Regression: 0.50%)")
    print("-" * 80)

    for item in signals:
        sig_id = item["id"]
        category = item.get("category", "uncategorized")
        raw_text = item["raw_text"]
        expected = item["expected"]

        # Track per-category stats
        if category not in category_stats:
            category_stats[category] = {"total": 0, "passed": 0}
        category_stats[category]["total"] += 1

        # Execution: pass through TextNormalizer then SignalParser
        norm_result = normalizer.normalize(raw_text)
        actual_signal = parser.parse(norm_result.normalized_text)

        # Validation logic
        success, errors = compare_signal_fields(actual_signal, expected)

        if success:
            passed_count += 1
            category_stats[category]["passed"] += 1
        else:
            if len(results) < 10:  # Print first few failures for debugging
                print(f" [FAIL] {sig_id:<12} | Category: {category}")
                for err in errors:
                    print(f"        -> {err}")

        results.append({
            "id": sig_id,
            "category": category,
            "passed": success,
            "errors": errors,
            "actual": actual_signal.to_dict(),
            "expected": expected,
        })

    accuracy = (passed_count / len(signals)) if signals else 0.0
    passed_accuracy_gate = accuracy >= threshold

    # Regression gate evaluation
    passed_regression_gate = True
    regression_drop = 0.0
    if baseline_accuracy is not None:
        regression_drop = baseline_accuracy - accuracy
        if regression_drop > max_regression:
            passed_regression_gate = False

    overall_pass = passed_accuracy_gate and passed_regression_gate

    print("-" * 80)
    print(" EVALUATION SUMMARY BY CATEGORY:")
    for cat, stats in sorted(category_stats.items()):
        c_tot = stats["total"]
        c_pass = stats["passed"]
        c_acc = (c_pass / c_tot) * 100.0 if c_tot else 0.0
        print(f" - {cat:<32}: {c_acc:6.2f}% ({c_pass}/{c_tot})")

    print("-" * 80)
    print(f"Overall Accuracy Achieved:  {accuracy * 100:.2f}% ({passed_count}/{len(signals)} passed)")
    acc_gate_str = 'PASSED [OK]' if passed_accuracy_gate else 'FAILED [BELOW THRESHOLD]'
    print(f"Quality Gate (>= {threshold * 100:.1f}%):       {acc_gate_str}")

    if baseline_accuracy is not None:
        diff_pct = (accuracy - baseline_accuracy) * 100.0
        diff_str = f"+{diff_pct:.2f}%" if diff_pct >= 0 else f"{diff_pct:.2f}%"
        print(f"Regression Check vs Main:   {diff_str} (Drop: {max(0.0, regression_drop) * 100:.2f}%)")
        reg_gate_str = 'PASSED [OK]' if passed_regression_gate else 'FAILED [REGRESSION > 0.5%]'
        print(f"Regression Gate:            {reg_gate_str}")

    print("=" * 80)
    print(f"OVERALL BUILD GATE STATUS:  {'SUCCESS (BUILD ACCEPTED)' if overall_pass else 'FAILED (BUILD REJECTED)'}")
    print("=" * 80)

    # Format per-category summary
    per_category_summary = {
        cat: {
            "total": stats["total"],
            "passed": stats["passed"],
            "accuracy": (stats["passed"] / stats["total"]) if stats["total"] else 0.0,
        }
        for cat, stats in category_stats.items()
    }

    report = {
        "dataset": resolved_dataset_path,
        "total_count": len(signals),
        "passed_count": passed_count,
        "failed_count": len(signals) - passed_count,
        "accuracy": accuracy,
        "threshold": threshold,
        "passed_accuracy_gate": passed_accuracy_gate,
        "baseline_accuracy": baseline_accuracy,
        "regression_drop": regression_drop,
        "max_regression_allowance": max_regression,
        "passed_regression_gate": passed_regression_gate,
        "passed_gate": overall_pass,
        "category_breakdown": per_category_summary,
        "results": results,
    }

    # Save results to JSON file
    if not output_report_path:
        output_report_path = os.path.join(
            os.path.dirname(resolved_dataset_path), "golden_parser_results.json"
        )

    with open(output_report_path, "w", encoding="utf-8") as rf:
        json.dump(report, rf, indent=2)
    print(f"Report artifact exported to: {output_report_path}")

    return report


def test_golden_set_1500_accuracy_and_regression():
    """
    Pytest entrypoint validating that parser accuracy on the 1,500 dataset
    meets the mandatory >= 98% quality gate and does not regress > 0.5%.
    """
    report = run_golden_parser_evaluation()

    assert report["passed_accuracy_gate"], (
        f"CI GATE FAILURE: Parser accuracy ({report['accuracy'] * 100:.2f}%) "
        f"is below mandatory quality threshold of {report['threshold'] * 100:.1f}%!"
    )

    assert report["passed_regression_gate"], (
        f"CI REGRESSION FAILURE: Accuracy regressed by {report['regression_drop'] * 100:.2f}%, "
        f"exceeding allowable regression limit of {MAX_REGRESSION_ALLOWANCE * 100:.2f}% "
        f"from previous build baseline ({report['baseline_accuracy'] * 100:.2f}%)!"
    )

    assert report["passed_gate"], "Parser CI Gate evaluation failed!"


if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Pipvane v3 Golden Set Parser CI Quality Gate")
    cli_parser.add_argument("--dataset", "-d", type=str, default=None, help="Path to golden set JSON")
    cli_parser.add_argument(
        "--baseline", "-b", type=str, default=None,
        help="Baseline accuracy float (e.g. 0.985) or path to baseline JSON file",
    )
    cli_parser.add_argument("--threshold", "-t", type=float, default=None, help="Accuracy threshold (e.g. 0.98)")
    cli_parser.add_argument("--max-regression", "-r", type=float, default=0.005, help="Max allowed regression drop")
    cli_parser.add_argument("--output", "-o", type=str, default=None, help="Output path for results JSON")
    args = cli_parser.parse_args()

    cli_baseline: Optional[float] = None
    if args.baseline is not None:
        try:
            cli_baseline = float(args.baseline)
        except ValueError:
            cli_baseline = load_baseline_accuracy(args.baseline)

    report = run_golden_parser_evaluation(
        dataset_path=args.dataset,
        baseline_accuracy=cli_baseline,
        threshold=args.threshold,
        max_regression=args.max_regression,
        output_report_path=args.output,
    )

    if not report["passed_gate"]:
        sys.exit(1)
    sys.exit(0)
