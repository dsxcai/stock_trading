# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from gui.desktop_backend import GuiDesktopBackend
from gui.services import OperationResult


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", required=True, help="Desktop backend action to execute")
    return parser.parse_args()


def _read_payload() -> dict:
    raw = sys.stdin.readline()
    if not str(raw or "").strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("stdin payload must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("stdin payload must be a JSON object")
    return payload


def _build_error_result(message: str) -> OperationResult:
    return OperationResult(
        name="GUI error",
        success=False,
        returncode=1,
        command="ipc",
        stdout=str(message or ""),
        message=str(message or ""),
    )


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent
    backend = GuiDesktopBackend(repo_root)

    try:
        payload = _read_payload()
        session_state = backend.perform_action(str(args.action or ""), payload)
        response = {
            "ok": True,
            "state": backend.build_state(session_state),
        }
    except Exception as exc:
        payload = payload if "payload" in locals() and isinstance(payload, dict) else {}
        session_state = backend.normalize_session_state(payload)
        session_state.last_result = _build_error_result(str(exc))
        try:
            fallback_state = backend.build_state(session_state)
        except Exception as inner_exc:
            fallback_state = {
                "ui": {"selected_report_path": session_state.selected_report_path},
                "report": {"selected": None, "text": "", "error_log_text": ""},
                "recent_reports": [],
                "runtime_config": {},
                "signal_config": {"selected_windows": {}, "candidate_tickers": []},
                "last_result": backend.serialize_operation_result(session_state.last_result),
                "modes": [],
            }
            exc = inner_exc
        response = {
            "ok": False,
            "error": str(exc),
            "state": fallback_state,
        }

    json.dump(response, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
