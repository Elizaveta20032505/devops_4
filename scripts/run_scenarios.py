"""Прогон сценариев из scenario.json."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
# При запуске "python scripts/run_scenarios.py" в sys.path попадает только scripts/ — нужен корень репо для import src
_repo_root = str(ROOT)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _get(url: str, timeout_sec: float = 15.0) -> tuple[int, bytes]:
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout_sec) as resp:
        return resp.status, resp.read()


def _get_timeout_for_url(url: str) -> float:
    """GET /kafka/last блокируется на сервере до timeout_sec из query — клиент должен ждать дольше."""
    if "/kafka/last" not in url:
        return 15.0
    m = re.search(r"[?&]timeout_sec=([0-9.]+)", url)
    if m:
        return float(m.group(1)) + 15.0
    return 45.0


def _zeros_feature_names() -> list[str]:
    """Имена признаков для телa POST /predict: файл в репо (если есть) или тот же fallback, что у API (inference)."""
    names_path = ROOT / "experiments" / "feature_names.json"
    if names_path.is_file():
        return json.loads(names_path.read_text(encoding="utf-8"))
    try:
        from src.inference import read_feature_names_for_openapi

        return read_feature_names_for_openapi()
    except Exception as e:
        print(
            f"Нет experiments/feature_names.json и не удалось взять имена из src.inference: {e}",
            file=sys.stderr,
        )
        raise SystemExit(1) from e


def _post_json(url: str, body: dict) -> tuple[int, bytes]:
    data = json.dumps(body).encode("utf-8")
    req = Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=60) as resp:
        return resp.status, resp.read()


def main() -> int:
    scen_path = ROOT / "scenario.json"
    if not scen_path.is_file():
        print("Нет scenario.json в корне проекта", file=sys.stderr)
        return 1
    scen = _load_json(scen_path)
    base = os.environ.get("SCENARIO_BASE_URL", scen.get("default_base_url", "http://127.0.0.1:8000")).rstrip("/")

    for step in scen.get("steps", []):
        sid = step.get("id", "?")
        method = step.get("method", "").upper()
        if not method:
            continue
        path = step.get("path", "/")
        url = base + path
        try:
            if method == "GET":
                status, raw = _get(url, timeout_sec=_get_timeout_for_url(url))
            elif method == "POST":
                tmpl = step.get("body_template", "")
                if tmpl == "zeros_from_feature_names":
                    names = _zeros_feature_names()
                    body = {n: 0.0 for n in names}
                elif "json" in step:
                    body = step["json"]
                else:
                    print(f"[{sid}] нет тела запроса", file=sys.stderr)
                    return 1
                status, raw = _post_json(url, body)
            else:
                print(f"[{sid}] неизвестный method {method}", file=sys.stderr)
                return 1
        except HTTPError as e:
            status = e.code
            raw = e.read() if hasattr(e, "read") else b""
        except URLError as e:
            print(f"[{sid}] ошибка сети: {e}", file=sys.stderr)
            return 1

        allowed = step.get("expect_status_in")
        if allowed is not None:
            if status not in allowed:
                print(f"[{sid}] FAIL status={status} not in {allowed} body={raw[:400]!r}", file=sys.stderr)
                return 1
        else:
            expect = step.get("expect_status", 200)
            if status != expect:
                print(f"[{sid}] FAIL status={status} expected={expect} body={raw[:400]!r}", file=sys.stderr)
                return 1

        if "expect_json" in step:
            got = json.loads(raw.decode("utf-8"))
            for k, v in step["expect_json"].items():
                if got.get(k) != v:
                    print(f"[{sid}] FAIL json {k}: {got.get(k)!r} != {v!r}", file=sys.stderr)
                    return 1
        if "expect_json_keys" in step:
            got = json.loads(raw.decode("utf-8"))
            if not isinstance(got, dict):
                print(f"[{sid}] FAIL json not object: {got!r}", file=sys.stderr)
                return 1
            for k in step["expect_json_keys"]:
                if k not in got:
                    print(f"[{sid}] FAIL json missing key {k!r}", file=sys.stderr)
                    return 1
        print(f"[{sid}] OK (HTTP {status})")

    print("Готово.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
