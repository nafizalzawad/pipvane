"""
Pytest integration test suite executing Golden Set 1,500 parser accuracy and regression check.
"""

import os
import pytest
from qa.test_golden_parser import (
    run_golden_parser_evaluation,
    load_baseline_accuracy,
    MAX_REGRESSION_ALLOWANCE,
)


@pytest.mark.unit
def test_golden_parser_1500_accuracy_gate():
    """
    Validates that the SignalParser running on 1,500 real-world Telegram signals
    exceeds the 98% accuracy threshold.
    """
    dataset_path = os.path.join(os.path.dirname(__file__), "..", "golden_set_1500.json")
    baseline_path = os.path.join(os.path.dirname(__file__), "..", "baseline_accuracy.json")
    baseline_val = load_baseline_accuracy(baseline_path)

    report = run_golden_parser_evaluation(
        dataset_path=dataset_path,
        baseline_accuracy=baseline_val,
        output_report_path=os.path.join(os.path.dirname(__file__), "..", "golden_parser_results.json"),
    )

    assert report["passed_accuracy_gate"] is True, (
        f"Parser accuracy ({report['accuracy'] * 100:.2f}%) did not achieve "
        f"the mandatory 98.0% threshold (Target: {report['threshold'] * 100:.1f}%)!"
    )

    assert report["passed_regression_gate"] is True, (
        f"Accuracy regressed by {report['regression_drop'] * 100:.2f}%, exceeding 0.50% limit!"
    )

    assert report["total_count"] == 1500, f"Expected 1,500 signals, evaluated {report['total_count']}"
    assert report["accuracy"] >= 0.98, f"Accuracy {report['accuracy']} below 0.98 gate"


@pytest.mark.unit
def test_regression_detection_explicit_failure():
    """
    Verifies that if accuracy regresses by > 0.5% from baseline, the regression gate strictly fails.
    """
    dataset_path = os.path.join(os.path.dirname(__file__), "..", "golden_set_1500.json")

    # Set an artificially high baseline (e.g. 99.9%) so that current accuracy drops by > 0.5%
    artificial_high_baseline = 0.999

    report = run_golden_parser_evaluation(
        dataset_path=dataset_path,
        baseline_accuracy=artificial_high_baseline,
    )

    # When baseline is 99.9% and actual is e.g. 98.6%, regression drop is > 0.005
    if report["regression_drop"] > MAX_REGRESSION_ALLOWANCE:
        assert report["passed_regression_gate"] is False
        assert report["passed_gate"] is False
