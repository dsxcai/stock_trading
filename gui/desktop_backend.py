from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from gui.services import GuiServices, OperationResult

_MODE_LABELS = {
    "premarket": "Premarket",
    "intraday": "Intraday",
    "afterclose": "AfterClose",
}


@dataclass
class DesktopSessionState:
    selected_report_path: str = ""
    last_result: Optional[OperationResult] = None


class GuiDesktopBackend:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = Path(repo_root).resolve()
        self.services = GuiServices(self.repo_root)

    @staticmethod
    def serialize_operation_result(result: Optional[OperationResult]) -> Optional[Dict[str, Any]]:
        if result is None:
            return None
        return asdict(result)

    @staticmethod
    def deserialize_operation_result(payload: object) -> Optional[OperationResult]:
        if not isinstance(payload, dict):
            return None
        try:
            return OperationResult(
                name=str(payload.get("name") or ""),
                success=bool(payload.get("success")),
                returncode=int(payload.get("returncode") or 0),
                command=str(payload.get("command") or ""),
                stdout=str(payload.get("stdout") or ""),
                message=str(payload.get("message") or ""),
                log_path=str(payload.get("log_path") or ""),
                report_path=str(payload.get("report_path") or ""),
                report_json_path=str(payload.get("report_json_path") or ""),
            )
        except Exception:
            return None

    def normalize_session_state(self, payload: Dict[str, Any]) -> DesktopSessionState:
        return DesktopSessionState(
            selected_report_path=str(payload.get("selected_report_path") or "").strip(),
            last_result=self.deserialize_operation_result(payload.get("last_result")),
        )

    def build_state(self, session_state: DesktopSessionState) -> Dict[str, Any]:
        recent_reports = self.services.list_recent_reports(limit=20)
        selected_report_path = self._resolve_selected_report_path(session_state.selected_report_path, recent_reports)
        report_text = self.services.read_text(selected_report_path) if selected_report_path else ""
        error_log_text = ""
        if session_state.last_result and session_state.last_result.log_path:
            error_log_text = self.services.read_text(session_state.last_result.log_path)
        selected_report_payload = self._build_selected_report_payload(selected_report_path, recent_reports)
        return {
            "ui": {
                "selected_report_path": selected_report_path,
            },
            "report": {
                "selected": selected_report_payload,
                "text": report_text,
                "error_log_text": error_log_text,
            },
            "recent_reports": [asdict(item) for item in recent_reports],
            "runtime_config": asdict(self.services.load_runtime_config_snapshot()),
            "signal_config": asdict(self.services.load_signal_config()),
            "last_result": self.serialize_operation_result(session_state.last_result),
            "modes": [{"key": key, "label": label} for key, label in _MODE_LABELS.items()],
        }

    def perform_action(self, action: str, payload: Dict[str, Any]) -> DesktopSessionState:
        session_state = self.normalize_session_state(payload)
        action_key = str(action or "").strip()
        if action_key == "get-state":
            session_state.selected_report_path = self._resolve_selected_report_path(
                session_state.selected_report_path,
                self.services.list_recent_reports(limit=20),
            )
            return session_state
        if action_key == "select-report":
            session_state.selected_report_path = str(payload.get("report_path") or "").strip()
            return session_state
        if action_key == "delete-report":
            report_path = str(payload.get("report_path") or "").strip()
            result = self.services.delete_report(report_path)
            if session_state.selected_report_path == report_path:
                session_state.selected_report_path = ""
            session_state.last_result = result
            return session_state
        if action_key == "delete-all-reports":
            session_state.selected_report_path = ""
            session_state.last_result = self.services.delete_all_reports()
            return session_state
        if action_key == "run-mode":
            result = self.services.run_report(
                str(payload.get("mode") or ""),
                str(payload.get("report_date") or ""),
                force_mode=self._coerce_bool(payload.get("force_mode")),
                allow_incomplete_csv_rows=self._coerce_bool(payload.get("allow_incomplete_csv_rows")),
            )
            return self._merge_operation_result(session_state, result)
        if action_key == "generate-report":
            result = self.services.run_report(
                str(payload.get("mode") or ""),
                str(payload.get("report_date") or ""),
                force_mode=self._coerce_bool(payload.get("force_mode")),
                allow_incomplete_csv_rows=self._coerce_bool(payload.get("allow_incomplete_csv_rows")),
            )
            return self._merge_operation_result(session_state, result)
        if action_key == "import-trades":
            result = self.services.run_import_trades(
                str(payload.get("capital_xls_path") or ""),
                trades_import_mode=str(payload.get("trades_import_mode") or "replace"),
                trade_date_from=str(payload.get("trade_date_from") or ""),
                trade_date_to=str(payload.get("trade_date_to") or ""),
                selected_report_path=session_state.selected_report_path,
                allow_incomplete_csv_rows=self._coerce_bool(payload.get("allow_incomplete_csv_rows")),
            )
            return self._merge_operation_result(session_state, result)
        if action_key == "cash-adjust":
            result = self.services.run_cash_adjustment(
                payload.get("cash_adjust_usd", ""),
                cash_adjust_note=str(payload.get("cash_adjust_note") or ""),
                selected_report_path=session_state.selected_report_path,
            )
            return self._merge_operation_result(session_state, result)
        if action_key == "save-runtime-config":
            config_fields = payload.get("config_fields")
            if not isinstance(config_fields, dict):
                raise ValueError("config_fields must be a JSON object")
            result = self.services.save_runtime_config(
                {str(key): value for key, value in config_fields.items()},
                selected_report_path=session_state.selected_report_path,
            )
            return self._merge_operation_result(session_state, result)
        if action_key == "save-signal-config":
            selected_windows_raw = payload.get("selected_windows")
            if not isinstance(selected_windows_raw, dict):
                raise ValueError("selected_windows must be a JSON object")
            selected_windows = {
                str(ticker): int(window)
                for ticker, window in selected_windows_raw.items()
                if str(ticker or "").strip()
            }
            result = self.services.save_signal_config(
                selected_windows,
                selected_report_path=session_state.selected_report_path,
            )
            return self._merge_operation_result(session_state, result)
        raise ValueError(f"unsupported desktop action: {action_key}")

    def _merge_operation_result(self, session_state: DesktopSessionState, result: OperationResult) -> DesktopSessionState:
        session_state.last_result = result
        if result.success and result.report_path:
            session_state.selected_report_path = str(result.report_path or "").strip()
        return session_state

    def _resolve_selected_report_path(self, selected_report_path: str, recent_reports: list) -> str:
        candidate = str(selected_report_path or "").strip()
        if candidate:
            recent_paths = [str(item.path) for item in recent_reports]
            if candidate in recent_paths or Path(candidate).exists():
                return candidate
        if recent_reports:
            return str(recent_reports[0].path)
        return ""

    def _build_selected_report_payload(self, selected_report_path: str, recent_reports: list) -> Optional[Dict[str, Any]]:
        selected_path_value = str(selected_report_path or "").strip()
        if not selected_path_value:
            return None
        for item in recent_reports:
            if str(item.path) == selected_path_value:
                return asdict(item)
        selected_path = Path(selected_path_value)
        identity = self.services.parse_report_identity(selected_path)
        if identity is None:
            return None
        report_date, mode_key = identity
        return {
            "path": str(selected_path),
            "name": selected_path.name,
            "report_date": report_date,
            "mode_key": mode_key,
            "mode_label": _MODE_LABELS.get(mode_key, mode_key.title()),
            "modified_at": "",
        }

    @staticmethod
    def _coerce_bool(value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
