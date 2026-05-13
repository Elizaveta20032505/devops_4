from __future__ import annotations

import importlib
import json
from pathlib import Path

import joblib
import pytest
from sklearn.linear_model import LogisticRegression


@pytest.fixture
def tiny_api_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    X = [[0.0, 1.0, 2.0], [1.0, 0.0, 1.0], [2.0, 2.0, 0.0]]
    y = [0, 1, 1]
    model = LogisticRegression().fit(X, y)
    joblib.dump(model, tmp_path / "model.joblib")
    feats = ["radius_mean", "texture_mean", "smoothness_mean"]
    (tmp_path / "feature_names.json").write_text(json.dumps(feats), encoding="utf-8")
    monkeypatch.setenv("DEVOPS_MODEL_DIR", str(tmp_path))
    monkeypatch.setenv("KAFKA_ENABLED", "0")
    import src.api as api_mod

    importlib.reload(api_mod)
    return api_mod
