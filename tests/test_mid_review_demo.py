"""Tests for the mid-review ML/math function demonstration."""
from __future__ import annotations

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

from scripts.mid_review_demo import linear_regression_step


def test_linear_regression_step_returns_finite_metrics() -> None:
    result = linear_regression_step(
        [1.0, 2.0, 3.0],
        [3.0, 5.0, 7.0],
        weight=0.5,
        bias=0.0,
        learning_rate=0.01,
    )
    assert result["mse"] == pytest.approx(16.333333333333332)
    assert result["updated_weight"] == pytest.approx(0.7066666666666667)
    assert result["updated_bias"] == pytest.approx(0.08666666666666667)


def test_callable_can_be_serialized_and_executed() -> None:
    function_blob = cloudpickle.dumps(linear_regression_step)
    restored = cloudpickle.loads(function_blob)
    result = restored([1.0, 2.0], [3.0, 5.0], 1.0, 0.0)
    assert result["mse"] == pytest.approx(2.5)


def test_rejects_empty_or_mismatched_dataset() -> None:
    with pytest.raises(ValueError):
        linear_regression_step([], [], 1.0, 0.0)
    with pytest.raises(ValueError):
        linear_regression_step([1.0], [1.0, 2.0], 1.0, 0.0)
