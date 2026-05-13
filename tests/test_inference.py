from __future__ import annotations

from pathlib import Path

import pytest
from sklearn.linear_model import LogisticRegression

from src.inference import predict_from_features


def test_predict_from_features(tmp_path: Path) -> None:
    X = [[0.0, 10.0], [5.0, 5.0]]
    y = [0, 1]
    m = LogisticRegression().fit(X, y)
    names = ["a", "b"]
    out = predict_from_features({"a": 0.0, "b": 10.0}, m, names)
    assert out["label"] in ("M", "B")
    assert out["malignant_probability"] is not None


def test_predict_missing_field() -> None:
    m = LogisticRegression().fit([[0.0], [1.0]], [0, 1])
    with pytest.raises(ValueError):
        predict_from_features({"a": 1.0}, m, ["a", "b"])
