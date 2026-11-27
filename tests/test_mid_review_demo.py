"""Tests for the mid-review ML/math function demonstration."""
from __future__ import annotations

import importlib.util

import pytest

from scripts.mid_review_demo import linear_regression_step

CLOUDPICKLE_AVAILABLE = importlib.util.find_spec("cloudpickle") is not None


def test_linear_regression_step_returns_finite_metrics() -> None:
    result = linear_regression_step(
        [1.0, 2.0, 3.0],
        [3.0, 5.0, 7.0],
        weight=0.5,
        bias=0.0,
        learning_rate=0.01,
    )
    assert result["mse"] == pytest.approx(17.5)
    assert result["updated_weight"] == pytest.approx(0.68)
    assert result["updated_bias"] == pytest.approx(0.08)


@pytest.mark.skipif(not CLOUDPICKLE_AVAILABLE, reason="install the mid-review extra")
def test_callable_can_be_serialized_and_executed() -> None:
    import cloudpickle

    function_blob = cloudpickle.dumps(linear_regression_step)
    restored = cloudpickle.loads(function_blob)
    result = restored([1.0, 2.0], [3.0, 5.0], 1.0, 0.0)
    assert result["mse"] == pytest.approx(6.5)


def test_rejects_empty_or_mismatched_dataset() -> None:
    with pytest.raises(ValueError):
        linear_regression_step([], [], 1.0, 0.0)
    with pytest.raises(ValueError):
        linear_regression_step([1.0], [1.0, 2.0], 1.0, 0.0)
