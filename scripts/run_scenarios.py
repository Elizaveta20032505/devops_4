"""Прогон сценариев из scenario.json."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _get(url: str) -> tuple[int, bytes]:
    req = Request(url, method="GET")
    with urlopen(req, timeout=15) as resp:
        return resp.status, resp.read()


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
                status, raw = _get(url)
            elif method == "POST":
                tmpl = step.get("body_template", "")
                if tmpl == "zeros_from_feature_names":
                    names_path = ROOT / "experiments" / "feature_names.json"
                    if not names_path.is_file():
                        print(f"[{sid}] skip: нет experiments/feature_names.json", file=sys.stderr)
                        continue
                    names = json.loads(names_path.read_text(encoding="utf-8"))
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
