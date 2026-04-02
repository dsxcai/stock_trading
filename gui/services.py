from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from utils.config_access import discover_state_engine_tickers, load_json_object, load_state_engine_config


_REPORT_NAME_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<mode>premarket|intraday|afterclose)\.md$", re.IGNORECASE)
_LOG_LINE_RE = re.compile(r"^\[LOG\] file=(.+)$", re.MULTILINE)
_WROTE_LINE_RE = re.compile(r"^\[OK\] wrote (.+)$", re.MULTILINE)
_ERROR_LINE_RE = re.compile(r"^\[(?:ABORT|ERR|ERROR|EXCEPTION)\]\s*(.+)$", re.MULTILINE)
_WINDOW_RE = re.compile(r"(\d+)")
_MODE_LABELS = {
    "premarket": "Premarket",
    "intraday": "Intraday",
    "afterclose": "AfterClose",
}


@dataclass(frozen=True)
class ReportInfo:
    path: str
    name: str
    report_date: str
    mode_key: str
    mode_label: str
    modified_at: str


@dataclass
class OperationResult:
    name: str
    success: bool
    returncode: int
    command: str
    stdout: str
    message: str
    log_path: str = ""
    report_path: str = ""
    report_json_path: str = ""


@dataclass(frozen=True)
class SignalConfigSnapshot:
    selected_windows: Dict[str, int]
    candidate_tickers: List[str]


class GuiServices:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = Path(repo_root).resolve()

    @property
    def config_path(self) -> Path:
        return self.repo_root / "config.json"

    @property
    def states_path(self) -> Path:
        return self.repo_root / "states.json"

    @property
    def trades_path(self) -> Path:
        return self.repo_root / "trades.json"

    @property
    def report_dir(self) -> Path:
        return self.repo_root / "report"

    @property
    def data_dir(self) -> Path:
        return self.repo_root / "data"

    @property
    def schema_path(self) -> Path:
        return self.repo_root / "report_spec.json"

    def list_recent_reports(self, limit: int = 20) -> List[ReportInfo]:
        reports: List[ReportInfo] = []
        for path in sorted(self.report_dir.glob("*.md"), key=lambda item: item.stat().st_mtime, reverse=True):
            identity = self.parse_report_identity(path)
            if identity is None:
                continue
            report_date, mode_key = identity
            reports.append(
                ReportInfo(
                    path=str(path),
                    name=path.name,
                    report_date=report_date,
                    mode_key=mode_key,
                    mode_label=_MODE_LABELS[mode_key],
                    modified_at=datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y/%m/%d %H:%M:%S"),
                )
            )
            if len(reports) >= limit:
                break
        return reports

    def read_text(self, path: str) -> str:
        target = Path(path)
        if not target.is_absolute():
            target = (self.repo_root / target).resolve()
        if not target.exists():
            return ""
        return target.read_text(encoding="utf-8")

    def load_signal_config(self) -> SignalConfigSnapshot:
        config = load_state_engine_config(str(self.config_path))
        selected_windows: Dict[str, int] = {}
        tactical = (((config.get("strategy") or {}).get("tactical")) or {})
        indicators = tactical.get("indicators") if isinstance(tactical, dict) else {}
        if isinstance(indicators, dict):
            for ticker, spec in indicators.items():
                ticker_norm = str(ticker or "").upper().strip()
                if not ticker_norm:
                    continue
                selected_windows[ticker_norm] = self._window_from_spec(spec)
        fx_tickers = {
            str((payload or {}).get("ticker") or "").upper().strip()
            for payload in (((config.get("data") or {}).get("fx_pairs")) or {}).values()
            if isinstance(payload, dict)
        }
        candidates: List[str] = []
        seen: set[str] = set()
        for ticker in list(selected_windows.keys()) + discover_state_engine_tickers(config):
            ticker_norm = str(ticker or "").upper().strip()
            if not ticker_norm or ticker_norm in fx_tickers or ticker_norm in seen:
                continue
            seen.add(ticker_norm)
            candidates.append(ticker_norm)
        states_obj = {}
        if self.states_path.exists():
            try:
                states_obj = json.loads(self.states_path.read_text(encoding="utf-8"))
            except Exception:
                states_obj = {}
        for position in (((states_obj.get("portfolio") or {}).get("positions")) or []):
            if not isinstance(position, dict):
                continue
            ticker_norm = str(position.get("ticker") or "").upper().strip()
            if ticker_norm and ticker_norm not in fx_tickers and ticker_norm not in seen:
                seen.add(ticker_norm)
                candidates.append(ticker_norm)
        if self.data_dir.exists():
            for csv_path in sorted(self.data_dir.glob("*.csv")):
                ticker_norm = csv_path.stem.upper().strip()
                if ticker_norm and ticker_norm not in fx_tickers and ticker_norm not in seen:
                    seen.add(ticker_norm)
                    candidates.append(ticker_norm)
        ordered = list(selected_windows.keys()) + sorted([ticker for ticker in candidates if ticker not in selected_windows])
        return SignalConfigSnapshot(selected_windows=selected_windows, candidate_tickers=ordered)

    def save_signal_config(
        self,
        selected_windows: Dict[str, int],
        *,
        selected_report_path: str = "",
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        normalized: Dict[str, int] = {}
        for ticker, window in selected_windows.items():
            ticker_norm = str(ticker or "").upper().strip()
            if not ticker_norm:
                continue
            window_int = int(window)
            if window_int not in {50, 100}:
                raise ValueError(f"unsupported SMA window for {ticker_norm}: {window_int}")
            normalized[ticker_norm] = window_int
        if not normalized:
            raise ValueError("at least one tactical ticker must remain selected")
        raw = load_json_object(str(self.config_path))
        state_engine = raw.setdefault("state_engine", {})
        strategy = state_engine.setdefault("strategy", {})
        tactical = strategy.setdefault("tactical", {})
        indicators = {}
        for ticker, window in normalized.items():
            indicators[ticker] = {"ma_type": "SMA", "window": int(window)}
        tactical["indicators"] = indicators
        self.config_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        result = OperationResult(
            name="Save config",
            success=True,
            returncode=0,
            command="config.json update",
            stdout="",
            message=f"Saved {len(indicators)} tactical signal tickers to config.json.",
        )
        refreshed = self.refresh_selected_report(
            selected_report_path,
            allow_incomplete_csv_rows=allow_incomplete_csv_rows,
        )
        if refreshed is None:
            return result
        if refreshed.success:
            result.stdout = refreshed.stdout
            result.log_path = refreshed.log_path
            result.report_path = refreshed.report_path
            result.report_json_path = refreshed.report_json_path
            result.message += f" Refreshed {Path(refreshed.report_path).name}."
            return result
        refreshed.message = f"Config saved, but refreshing the selected report failed: {refreshed.message}"
        return refreshed

    def run_daily_mode(
        self,
        mode_label: str,
        *,
        force_mode: bool = False,
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        mode_key = self._normalize_mode_key(mode_label)
        if mode_key not in _MODE_LABELS:
            raise ValueError(f"unsupported mode: {mode_label}")
        command = [
            sys.executable,
            "update_states.py",
            "--states",
            "states.json",
            "--csv-dir",
            "data",
            "--derive-signals-inputs",
            "force",
            "--derive-threshold-inputs",
            "force",
            "--mode",
            _MODE_LABELS[mode_key],
            "--render-report",
            "--report-schema",
            "report_spec.json",
            "--report-dir",
            "report",
        ]
        if force_mode:
            command.append("--force-mode")
        if allow_incomplete_csv_rows:
            command.append("--allow-incomplete-csv-rows")
        return self._run_command(command, name=f"{_MODE_LABELS[mode_key]} run")

    def run_generate_report(
        self,
        mode_label: str,
        report_date: str = "",
        *,
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        mode_key = self._normalize_mode_key(mode_label)
        if mode_key not in _MODE_LABELS:
            raise ValueError(f"unsupported mode: {mode_label}")
        command = [
            sys.executable,
            "generate_report.py",
            "--states",
            "states.json",
            "--config",
            "config.json",
            "--trades-file",
            "trades.json",
            "--schema",
            "report_spec.json",
            "--mode",
            _MODE_LABELS[mode_key],
            "--out-dir",
            "report",
        ]
        report_date_value = str(report_date or "").strip()
        if report_date_value:
            command.extend(["--date", report_date_value])
        if allow_incomplete_csv_rows:
            command.append("--allow-incomplete-csv-rows")
        return self._run_command(command, name=f"Generate {_MODE_LABELS[mode_key]} report")

    def run_import_trades(
        self,
        capital_xls_path: str,
        *,
        trades_import_mode: str = "replace",
        selected_report_path: str = "",
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        xls_path = str(capital_xls_path or "").strip()
        if not xls_path:
            raise ValueError("Capital XLS path is required")
        command = [
            sys.executable,
            "-m",
            "extensions.capital_xls_import",
            xls_path,
            "--states",
            "states.json",
            "--out",
            "states.json",
            "--config",
            "config.json",
            "--trades-file",
            "trades.json",
            "--trades-import-mode",
            str(trades_import_mode or "replace"),
        ]
        primary = self._run_command(command, name="Import trades")
        if not primary.success:
            return primary
        refreshed = self.refresh_selected_report(
            selected_report_path,
            allow_incomplete_csv_rows=allow_incomplete_csv_rows,
        )
        if refreshed is None:
            return primary
        if refreshed.success:
            primary.stdout = "\n\n".join(part for part in [primary.stdout.strip(), refreshed.stdout.strip()] if part)
            primary.log_path = refreshed.log_path or primary.log_path
            primary.report_path = refreshed.report_path
            primary.report_json_path = refreshed.report_json_path
            primary.message += f" Refreshed {Path(refreshed.report_path).name}."
            return primary
        refreshed.message = f"Trades import succeeded, but refreshing the selected report failed: {refreshed.message}"
        return refreshed

    def refresh_selected_report(
        self,
        selected_report_path: str,
        *,
        allow_incomplete_csv_rows: bool = False,
    ) -> Optional[OperationResult]:
        path_value = str(selected_report_path or "").strip()
        if not path_value:
            return None
        identity = self.parse_report_identity(Path(path_value))
        if identity is None:
            return None
        report_date, mode_key = identity
        command = [
            sys.executable,
            "generate_report.py",
            "--states",
            "states.json",
            "--config",
            "config.json",
            "--trades-file",
            "trades.json",
            "--schema",
            "report_spec.json",
            "--mode",
            _MODE_LABELS[mode_key],
            "--date",
            report_date,
            "--out",
            str(path_value),
        ]
        if allow_incomplete_csv_rows:
            command.append("--allow-incomplete-csv-rows")
        return self._run_command(command, name=f"Refresh {Path(path_value).name}")

    def parse_report_identity(self, path: Path) -> Optional[Tuple[str, str]]:
        match = _REPORT_NAME_RE.match(Path(path).name)
        if not match:
            return None
        return match.group("date"), match.group("mode").lower()

    def _run_command(self, command: List[str], *, name: str) -> OperationResult:
        proc = subprocess.run(
            command,
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        stdout = proc.stdout or ""
        log_path = self._parse_log_path(stdout)
        report_path = ""
        report_json_path = ""
        for written in self._parse_written_paths(stdout):
            if written.endswith(".md"):
                report_path = written
            elif written.endswith(".json"):
                report_json_path = written
        success = int(proc.returncode or 0) == 0
        message = self._success_message(name, report_path) if success else self._failure_message(stdout, proc.returncode)
        return OperationResult(
            name=name,
            success=success,
            returncode=int(proc.returncode or 0),
            command=shlex.join(command),
            stdout=stdout,
            message=message,
            log_path=log_path,
            report_path=report_path,
            report_json_path=report_json_path,
        )

    @staticmethod
    def _window_from_spec(spec: object) -> int:
        if isinstance(spec, dict):
            try:
                return int(spec.get("window") or 50)
            except Exception:
                return 50
        match = _WINDOW_RE.search(str(spec or ""))
        if not match:
            return 50
        return int(match.group(1))

    @staticmethod
    def _normalize_mode_key(value: str) -> str:
        return re.sub(r"[\s_\-]+", "", str(value or "").strip().lower())

    @staticmethod
    def _parse_log_path(stdout: str) -> str:
        match = _LOG_LINE_RE.search(stdout or "")
        return match.group(1).strip() if match else ""

    @staticmethod
    def _parse_written_paths(stdout: str) -> List[str]:
        return [match.group(1).strip() for match in _WROTE_LINE_RE.finditer(stdout or "")]

    @staticmethod
    def _success_message(name: str, report_path: str) -> str:
        if report_path:
            return f"{name} completed and wrote {Path(report_path).name}."
        return f"{name} completed successfully."

    @staticmethod
    def _failure_message(stdout: str, returncode: int) -> str:
        matches = _ERROR_LINE_RE.findall(stdout or "")
        if matches:
            return matches[-1].strip()
        lines = [line.strip() for line in str(stdout or "").splitlines() if line.strip()]
        if lines:
            return lines[-1]
        return f"Command failed with exit code {int(returncode or 1)}."
