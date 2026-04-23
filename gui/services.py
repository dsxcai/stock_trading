# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import io
import json
import re
import shlex
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.config_access import discover_state_engine_tickers, load_json_object, load_state_engine_config


_REPORT_NAME_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<mode>premarket|intraday|afterclose)\.md$", re.IGNORECASE)
_REPORT_ARTIFACT_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})_(?P<mode>premarket|intraday|afterclose)\.(?P<ext>md|json)$", re.IGNORECASE)
_LOG_LINE_RE = re.compile(r"^\[LOG\] file=(.+)$", re.MULTILINE)
_WROTE_LINE_RE = re.compile(r"^\[OK\] wrote (.+)$", re.MULTILINE)
_ERROR_LINE_RE = re.compile(r"^\[(?:ABORT|ERR|ERROR|EXCEPTION)\]\s*(.+)$", re.MULTILINE)
_WINDOW_RE = re.compile(r"(\d+)")
_MODE_LABELS = {
    "premarket": "Premarket",
    "intraday": "Intraday",
    "afterclose": "AfterClose",
}
_DEFAULT_NUMERIC_PRECISION = {
    "usd_amount": 2,
    "display_price": 2,
    "display_pct": 2,
    "trade_cash_amount": 4,
    "trade_dedupe_amount": 6,
    "state_selected_fields": 4,
    "backtest_amount": 4,
    "backtest_price": 4,
    "backtest_rate": 6,
    "backtest_cost_param": 6,
}
_DEFAULT_KEEP_PREV_TRADE_DAYS_SIMPLIFIED = 5


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


@dataclass(frozen=True)
class RuntimeConfigSnapshot:
    doc: str
    trades_file: str
    cash_events_file: str
    buy_fee_rate: float
    sell_fee_rate: float
    core_tickers_text: str
    tactical_tickers_text: str
    tactical_cash_pool_ticker: str
    tactical_cash_pool_tickers_text: str
    fx_pairs_text: str
    csv_sources_text: str
    closed_days_text: str
    early_closes_text: str
    numeric_precision: Dict[str, int]
    keep_prev_trade_days_simplified: int


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _parse_ticker_list(raw_value: str) -> List[str]:
    parts = re.split(r"[\s,]+", str(raw_value or "").strip())
    tickers = [str(part or "").upper().strip() for part in parts if str(part or "").strip()]
    return _dedupe_preserve_order(tickers)


def _format_lines(lines: List[str]) -> str:
    return "\n".join(str(line or "").strip() for line in lines if str(line or "").strip())


def _parse_key_value_lines(raw_value: str, *, value_name: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line_no, raw_line in enumerate(str(raw_value or "").splitlines(), start=1):
        line = str(raw_line or "").strip()
        if not line:
            continue
        if "=" not in line:
            raise ValueError(f"line {line_no} must use key=value format for {value_name}")
        key, value = line.split("=", 1)
        key_norm = str(key or "").strip()
        value_norm = str(value or "").strip()
        if not key_norm:
            raise ValueError(f"line {line_no} is missing the key for {value_name}")
        if not value_norm:
            raise ValueError(f"line {line_no} is missing the value for {value_name}")
        out[key_norm] = value_norm
    return out


def _parse_closed_days(raw_value: str) -> Dict[str, str]:
    return _parse_key_value_lines(raw_value, value_name="closed days")


def _parse_early_close_days(raw_value: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for line_no, raw_line in enumerate(str(raw_value or "").splitlines(), start=1):
        line = str(raw_line or "").strip()
        if not line:
            continue
        if "=" not in line:
            raise ValueError(f"line {line_no} must use YYYY-MM-DD=HH:MM|Reason for early closes")
        day_text, payload = line.split("=", 1)
        day_key = str(day_text or "").strip()
        payload_text = str(payload or "").strip()
        if not day_key:
            raise ValueError(f"line {line_no} is missing the date for early closes")
        if not payload_text:
            raise ValueError(f"line {line_no} is missing the early close payload")
        time_text, sep, reason_text = payload_text.partition("|")
        close_time_et = str(time_text or "").strip()
        reason = str(reason_text or "").strip()
        if not re.fullmatch(r"\d{2}:\d{2}", close_time_et):
            raise ValueError(f"line {line_no} must use HH:MM for early close times")
        out[day_key] = {"close_time_et": close_time_et}
        if sep and reason:
            out[day_key]["reason"] = reason
    return out


def _normalize_numeric_precision_value(raw_value: Any, *, key: str) -> int:
    try:
        parsed = int(raw_value)
    except Exception as exc:
        raise ValueError(f"{key} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{key} must be a non-negative integer")
    return parsed


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

    def _runtime_ledger_args(self) -> List[str]:
        snapshot = self.load_runtime_config_snapshot()
        return [
            "--config",
            "config.json",
            "--trades-file",
            snapshot.trades_file,
            "--cash-events-file",
            snapshot.cash_events_file,
        ]

    def _generate_report_command(self, mode_key: str, *extra_args: str) -> List[str]:
        return [
            sys.executable,
            "generate_report.py",
            "--states",
            "states.json",
            *self._runtime_ledger_args(),
            "--schema",
            "report_spec.json",
            "--mode",
            _MODE_LABELS[mode_key],
            *extra_args,
        ]

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

    def delete_report(self, report_path: str) -> OperationResult:
        target = Path(str(report_path or "").strip())
        if not target.is_absolute():
            target = (self.repo_root / target).resolve()
        else:
            target = target.resolve()
        try:
            target.relative_to(self.report_dir)
        except ValueError as exc:
            raise ValueError("report path must be under report/") from exc
        if target.suffix.lower() != ".md" or self.parse_report_identity(target) is None:
            raise ValueError("report path must be a standard markdown report")
        deleted_paths: List[str] = []
        report_json = target.with_suffix(".json")
        for candidate in [target, report_json]:
            if candidate.exists() and candidate.is_file():
                candidate.unlink()
                deleted_paths.append(candidate.name)
        if deleted_paths:
            artifact_label = "artifact" if len(deleted_paths) == 1 else "artifacts"
            message = f"Deleted {len(deleted_paths)} report {artifact_label} for {target.name}."
        else:
            message = f"No report artifacts were found for {target.name}."
        return OperationResult(
            name="Delete report",
            success=True,
            returncode=0,
            command=f"delete-report {target.name}",
            stdout="\n".join(deleted_paths),
            message=message,
        )

    def delete_all_reports(self) -> OperationResult:
        deleted_paths: List[str] = []
        deleted_report_stems: set[str] = set()
        if self.report_dir.exists():
            for path in sorted(self.report_dir.iterdir()):
                if not path.is_file() or not _REPORT_ARTIFACT_RE.match(path.name):
                    continue
                path.unlink()
                deleted_paths.append(path.name)
                deleted_report_stems.add(path.stem)
        report_count = len(deleted_report_stems)
        artifact_count = len(deleted_paths)
        if artifact_count:
            report_label = "report" if report_count == 1 else "reports"
            artifact_label = "artifact" if artifact_count == 1 else "artifacts"
            message = f"Deleted {artifact_count} report {artifact_label} across {report_count} {report_label}."
        else:
            message = "No standard report artifacts were found under report/."
        return OperationResult(
            name="Delete all reports",
            success=True,
            returncode=0,
            command="delete-all-reports",
            stdout="\n".join(deleted_paths),
            message=message,
        )

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

    def load_runtime_config_snapshot(self) -> RuntimeConfigSnapshot:
        config = load_state_engine_config(str(self.config_path))
        meta = config.get("meta") if isinstance(config.get("meta"), dict) else {}
        execution = config.get("execution") if isinstance(config.get("execution"), dict) else {}
        portfolio = config.get("portfolio") if isinstance(config.get("portfolio"), dict) else {}
        buckets = portfolio.get("buckets") if isinstance(portfolio.get("buckets"), dict) else {}
        core_bucket = buckets.get("core") if isinstance(buckets.get("core"), dict) else {}
        tactical_bucket = buckets.get("tactical") if isinstance(buckets.get("tactical"), dict) else {}
        tactical_cash_pool_bucket = (
            buckets.get("tactical_cash_pool") if isinstance(buckets.get("tactical_cash_pool"), dict) else {}
        )
        data = config.get("data") if isinstance(config.get("data"), dict) else {}
        reporting = config.get("reporting") if isinstance(config.get("reporting"), dict) else {}
        numeric_precision = reporting.get("numeric_precision") if isinstance(reporting.get("numeric_precision"), dict) else {}
        trade_render_policy = (
            reporting.get("trade_render_policy") if isinstance(reporting.get("trade_render_policy"), dict) else {}
        )
        fx_pairs = data.get("fx_pairs") if isinstance(data.get("fx_pairs"), dict) else {}
        csv_sources = data.get("csv_sources") if isinstance(data.get("csv_sources"), dict) else {}
        trading_calendar = data.get("trading_calendar") if isinstance(data.get("trading_calendar"), dict) else {}
        years = trading_calendar.get("years") if isinstance(trading_calendar.get("years"), dict) else {}

        closed_days: List[str] = []
        early_closes: List[str] = []
        for year_key in sorted(years.keys()):
            year_payload = years.get(year_key) if isinstance(years.get(year_key), dict) else {}
            closed = year_payload.get("closed") if isinstance(year_payload.get("closed"), dict) else {}
            early_close = year_payload.get("early_close") if isinstance(year_payload.get("early_close"), dict) else {}
            for day_key in sorted(closed.keys()):
                reason = str(closed.get(day_key) or "").strip()
                if reason:
                    closed_days.append(f"{day_key}={reason}")
            for day_key in sorted(early_close.keys()):
                payload = early_close.get(day_key) if isinstance(early_close.get(day_key), dict) else {}
                close_time_et = str(payload.get("close_time_et") or "").strip()
                reason = str(payload.get("reason") or "").strip()
                if close_time_et:
                    early_closes.append(f"{day_key}={close_time_et}|{reason}" if reason else f"{day_key}={close_time_et}")

        normalized_precision = {
            key: _normalize_numeric_precision_value(numeric_precision.get(key, default_value), key=key)
            for key, default_value in _DEFAULT_NUMERIC_PRECISION.items()
        }
        return RuntimeConfigSnapshot(
            doc=str(meta.get("doc") or "").strip(),
            trades_file=str(meta.get("trades_file") or "trades.json").strip() or "trades.json",
            cash_events_file=str(meta.get("cash_events_file") or "cash_events.json").strip() or "cash_events.json",
            buy_fee_rate=float(execution.get("buy_fee_rate") or 0.0),
            sell_fee_rate=float(execution.get("sell_fee_rate") or 0.0),
            core_tickers_text=_format_lines([str(value or "").upper().strip() for value in core_bucket.get("tickers") or []]),
            tactical_tickers_text=_format_lines(
                [str(value or "").upper().strip() for value in tactical_bucket.get("tickers") or []]
            ),
            tactical_cash_pool_ticker=str(tactical_bucket.get("cash_pool_ticker") or "").upper().strip(),
            tactical_cash_pool_tickers_text=_format_lines(
                [str(value or "").upper().strip() for value in tactical_cash_pool_bucket.get("tickers") or []]
            ),
            fx_pairs_text=_format_lines(
                [
                    f"{alias}={str((payload or {}).get('ticker') or '').strip()}"
                    for alias, payload in sorted(fx_pairs.items())
                    if isinstance(payload, dict) and str((payload or {}).get("ticker") or "").strip()
                ]
            ),
            csv_sources_text=_format_lines(
                [
                    f"{str(ticker or '').upper().strip()}={str(path or '').strip()}"
                    for ticker, path in sorted(csv_sources.items())
                    if str(ticker or "").strip() and str(path or "").strip()
                ]
            ),
            closed_days_text=_format_lines(closed_days),
            early_closes_text=_format_lines(early_closes),
            numeric_precision=normalized_precision,
            keep_prev_trade_days_simplified=_normalize_numeric_precision_value(
                trade_render_policy.get(
                    "keep_prev_trade_days_simplified",
                    _DEFAULT_KEEP_PREV_TRADE_DAYS_SIMPLIFIED,
                ),
                key="keep_prev_trade_days_simplified",
            ),
        )

    def save_runtime_config(
        self,
        config_fields: Dict[str, Any],
        *,
        selected_report_path: str = "",
    ) -> OperationResult:
        raw = load_json_object(str(self.config_path))
        state_engine = raw.get("state_engine") if isinstance(raw.get("state_engine"), dict) else {}
        strategy = state_engine.get("strategy") if isinstance(state_engine.get("strategy"), dict) else {}
        tactical = strategy.get("tactical") if isinstance(strategy.get("tactical"), dict) else {}
        indicators = tactical.get("indicators") if isinstance(tactical.get("indicators"), dict) else {}

        doc = str(config_fields.get("doc") or "").strip()
        trades_file = str(config_fields.get("trades_file") or "trades.json").strip() or "trades.json"
        cash_events_file = str(config_fields.get("cash_events_file") or "cash_events.json").strip() or "cash_events.json"
        buy_fee_rate_raw = str(config_fields.get("buy_fee_rate") or "").strip()
        sell_fee_rate_raw = str(config_fields.get("sell_fee_rate") or "").strip()
        try:
            buy_fee_rate = float(buy_fee_rate_raw or 0.0)
        except Exception as exc:
            raise ValueError("buy_fee_rate must be a number") from exc
        try:
            sell_fee_rate = float(sell_fee_rate_raw or 0.0)
        except Exception as exc:
            raise ValueError("sell_fee_rate must be a number") from exc
        core_tickers = _parse_ticker_list(str(config_fields.get("core_tickers") or ""))
        tactical_tickers = _parse_ticker_list(str(config_fields.get("tactical_tickers") or ""))
        tactical_cash_pool_ticker = str(config_fields.get("tactical_cash_pool_ticker") or "").upper().strip()
        tactical_cash_pool_tickers = _parse_ticker_list(str(config_fields.get("tactical_cash_pool_tickers") or ""))
        fx_pairs_raw = _parse_key_value_lines(str(config_fields.get("fx_pairs") or ""), value_name="FX pairs")
        fx_pairs = {alias: {"ticker": ticker.upper()} for alias, ticker in fx_pairs_raw.items()}
        csv_sources_raw = _parse_key_value_lines(str(config_fields.get("csv_sources") or ""), value_name="CSV sources")
        csv_sources = {str(ticker or "").upper().strip(): str(path or "").strip() for ticker, path in csv_sources_raw.items()}
        closed_days = _parse_closed_days(str(config_fields.get("closed_days") or ""))
        early_close_days = _parse_early_close_days(str(config_fields.get("early_close_days") or ""))

        numeric_precision: Dict[str, int] = {}
        for key, default_value in _DEFAULT_NUMERIC_PRECISION.items():
            numeric_precision[key] = _normalize_numeric_precision_value(
                str(config_fields.get(key) or default_value).strip() or default_value,
                key=key,
            )
        keep_prev_trade_days_simplified = _normalize_numeric_precision_value(
            str(
                config_fields.get(
                    "keep_prev_trade_days_simplified",
                    _DEFAULT_KEEP_PREV_TRADE_DAYS_SIMPLIFIED,
                )
                or _DEFAULT_KEEP_PREV_TRADE_DAYS_SIMPLIFIED
            ).strip()
            or _DEFAULT_KEEP_PREV_TRADE_DAYS_SIMPLIFIED,
            key="keep_prev_trade_days_simplified",
        )

        calendar_years: Dict[str, Dict[str, Any]] = {}
        for day_key, reason in sorted(closed_days.items()):
            year_payload = calendar_years.setdefault(day_key[:4], {})
            year_payload.setdefault("closed", {})[day_key] = reason
        for day_key, payload in sorted(early_close_days.items()):
            year_payload = calendar_years.setdefault(day_key[:4], {})
            year_payload.setdefault("early_close", {})[day_key] = dict(payload)

        canonical_state_engine: Dict[str, Any] = {
            "meta": {
                "doc": doc,
                "trades_file": trades_file,
                "cash_events_file": cash_events_file,
            },
            "execution": {
                "buy_fee_rate": buy_fee_rate,
                "sell_fee_rate": sell_fee_rate,
            },
            "portfolio": {
                "buckets": {
                    "core": {"tickers": core_tickers},
                    "tactical": {
                        "tickers": tactical_tickers,
                        "cash_pool_ticker": tactical_cash_pool_ticker,
                    },
                    "tactical_cash_pool": {"tickers": tactical_cash_pool_tickers},
                }
            },
            "strategy": {
                "tactical": {
                    "indicators": indicators,
                }
            },
            "data": {
                "fx_pairs": fx_pairs,
                "csv_sources": csv_sources,
                "trading_calendar": {"years": calendar_years},
            },
            "reporting": {
                "numeric_precision": numeric_precision,
                "trade_render_policy": {
                    "keep_prev_trade_days_simplified": keep_prev_trade_days_simplified,
                },
            },
        }
        gui_settings = state_engine.get("gui") if isinstance(state_engine.get("gui"), dict) else None
        if gui_settings:
            canonical_state_engine["gui"] = gui_settings
        self.config_path.write_text(
            json.dumps({"state_engine": canonical_state_engine}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        result = OperationResult(
            name="Save runtime config",
            success=True,
            returncode=0,
            command="config.json update",
            stdout="",
            message="Saved runtime config to config.json.",
        )
        refreshed = self.refresh_selected_report(
            selected_report_path,
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

    def save_signal_config(
        self,
        selected_windows: Dict[str, int],
        *,
        selected_report_path: str = "",
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

    def run_report(
        self,
        mode_label: str,
        report_date: str = "",
        *,
        force_mode: bool = False,
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        mode_key = self._normalize_mode_key(mode_label)
        if mode_key not in _MODE_LABELS:
            raise ValueError(f"unsupported mode: {mode_label}")
        report_date_value = str(report_date or "").strip()
        if report_date_value:
            if mode_key == "intraday":
                raise ValueError("Intraday is only meaningful for the latest trading session and is not supported for a specified historical trade date")
            command = self._generate_report_command(mode_key, "--out-dir", "report")
            command.extend(["--date", report_date_value])
            if allow_incomplete_csv_rows:
                command.append("--allow-incomplete-csv-rows")
            return self._run_command(command, name=f"Generate {_MODE_LABELS[mode_key]} report")
        command = [
            sys.executable,
            "update_states.py",
            "--states",
            "states.json",
            *self._runtime_ledger_args(),
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

    def run_daily_mode(
        self,
        mode_label: str,
        *,
        force_mode: bool = False,
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        return self.run_report(
            mode_label,
            "",
            force_mode=force_mode,
            allow_incomplete_csv_rows=allow_incomplete_csv_rows,
        )

    def run_generate_report(
        self,
        mode_label: str,
        report_date: str = "",
        *,
        allow_incomplete_csv_rows: bool = False,
    ) -> OperationResult:
        return self.run_report(
            mode_label,
            report_date,
            allow_incomplete_csv_rows=allow_incomplete_csv_rows,
        )

    def run_import_trades(
        self,
        capital_xls_path: str,
        *,
        trades_import_mode: str = "replace",
        trade_date_from: str = "",
        trade_date_to: str = "",
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
            *self._runtime_ledger_args(),
            "--trades-import-mode",
            str(trades_import_mode or "replace"),
        ]
        trade_date_from_value = str(trade_date_from or "").strip()
        trade_date_to_value = str(trade_date_to or "").strip()
        if trade_date_from_value:
            command.extend(["--trade-date-from", trade_date_from_value])
        if trade_date_to_value:
            command.extend(["--trade-date-to", trade_date_to_value])
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

    def run_cash_adjustment(
        self,
        amount_usd: str | float,
        *,
        cash_adjust_note: str = "",
        selected_report_path: str = "",
    ) -> OperationResult:
        amount_raw = "" if amount_usd is None else str(amount_usd).strip()
        if not amount_raw:
            raise ValueError("Cash adjustment amount is required")
        try:
            amount_value = float(amount_raw)
        except Exception as exc:
            raise ValueError("Cash adjustment amount must be a number") from exc
        command = [
            sys.executable,
            "update_states.py",
            "--states",
            "states.json",
            "--out",
            "states.json",
            *self._runtime_ledger_args(),
            "--cash-adjust-usd",
            format(amount_value, "g"),
        ]
        note_value = str(cash_adjust_note or "").strip()
        if note_value:
            command.extend(["--cash-adjust-note", note_value])
        primary = self._run_command(command, name="Cash adjustment")
        if not primary.success:
            return primary
        refreshed = self.refresh_selected_report(selected_report_path)
        if refreshed is None:
            return primary
        if refreshed.success:
            primary.stdout = "\n\n".join(part for part in [primary.stdout.strip(), refreshed.stdout.strip()] if part)
            primary.log_path = refreshed.log_path or primary.log_path
            primary.report_path = refreshed.report_path
            primary.report_json_path = refreshed.report_json_path
            primary.message += f" Refreshed {Path(refreshed.report_path).name}."
            return primary
        refreshed.message = f"Cash adjustment succeeded, but refreshing the selected report failed: {refreshed.message}"
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
        command = self._generate_report_command(mode_key, "--date", report_date, "--out", str(path_value))
        if allow_incomplete_csv_rows:
            command.append("--allow-incomplete-csv-rows")
        return self._run_command(command, name=f"Refresh {Path(path_value).name}")

    def parse_report_identity(self, path: Path) -> Optional[Tuple[str, str]]:
        match = _REPORT_NAME_RE.match(Path(path).name)
        if not match:
            return None
        return match.group("date"), match.group("mode").lower()

    # --- Environment health check --------------------------------------------------

    _DATA_FILES = ("config.json", "states.json", "trades.json", "cash_events.json")

    def check_environment(self) -> Dict[str, Any]:
        missing: List[str] = []
        invalid: List[str] = []
        for fname in self._DATA_FILES:
            fpath = self.repo_root / fname
            if not fpath.exists():
                missing.append(fname)
                continue
            try:
                raw = json.loads(fpath.read_text(encoding="utf-8"))
                if fname == "config.json" and not isinstance(raw.get("state_engine"), dict):
                    invalid.append(fname)
                elif fname in ("trades.json", "cash_events.json") and not isinstance(raw, list):
                    invalid.append(fname)
                elif fname == "states.json" and not isinstance(raw.get("portfolio"), dict):
                    invalid.append(fname)
            except Exception:
                invalid.append(fname)
        return {
            "ok": not missing and not invalid,
            "missing": missing,
            "invalid": invalid,
        }

    def init_clean_environment(self) -> OperationResult:
        env = self.check_environment()
        created: List[str] = []
        skipped: List[str] = []
        for fname in self._DATA_FILES:
            fpath = self.repo_root / fname
            needs_init = fname in env["missing"] or fname in env["invalid"]
            if not needs_init:
                skipped.append(fname)
                continue
            template = self._minimal_template(fname)
            fpath.write_text(json.dumps(template, indent=2) + "\n", encoding="utf-8")
            created.append(fname)
        if created:
            msg = f"Initialized {len(created)} file(s): {', '.join(created)}."
            if skipped:
                msg += f" Kept {len(skipped)} existing file(s) unchanged."
        else:
            msg = "All required files are already present and valid — nothing to initialize."
        return OperationResult(
            name="Initialize clean environment",
            success=True,
            returncode=0,
            command="init-clean-env",
            stdout=msg,
            message=msg,
        )

    @staticmethod
    def _minimal_template(fname: str) -> Any:
        if fname == "config.json":
            return {
                "state_engine": {
                    "meta": {"doc": "My Trading Portfolio", "trades_file": "trades.json", "cash_events_file": "cash_events.json"},
                    "execution": {"buy_fee_rate": 0.001425, "sell_fee_rate": 0.004425},
                    "portfolio": {"buckets": {"core": {"tickers": []}, "tactical": {"tickers": [], "cash_pool_ticker": ""}, "tactical_cash_pool": {"tickers": []}}},
                    "strategy": {"tactical": {"indicators": {}}},
                    "data": {"fx_pairs": {}, "csv_sources": {}, "trading_calendar": {"closed_days": {}, "early_closes": {}}},
                    "reporting": {"numeric_precision": {}, "trade_render_policy": {"keep_prev_trade_days_simplified": 5}},
                    "gui": {"window": {}},
                }
            }
        if fname == "states.json":
            return {"portfolio": {"positions": [], "cash": {"usd": 0.0, "baseline_usd": 0.0, "deployable_usd": 0.0, "reserve_usd": 0.0, "bucket": "core"}, "performance": {}}}
        if fname in ("trades.json", "cash_events.json"):
            return []
        return {}

    def export_zip(self, dest_path: str) -> OperationResult:
        dest = Path(dest_path)
        included: List[str] = []
        skipped: List[str] = []
        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for fname in (*self._DATA_FILES, "report_spec.json"):
                    fpath = self.repo_root / fname
                    if fpath.exists():
                        zf.write(fpath, arcname=fname)
                        included.append(fname)
                    else:
                        skipped.append(fname)
            dest.write_bytes(buf.getvalue())
        except Exception as exc:
            return OperationResult(
                name="Export zip",
                success=False,
                returncode=1,
                command=f"export-zip {dest_path}",
                stdout=str(exc),
                message=f"Export failed: {exc}",
            )
        msg = f"Exported {len(included)} file(s) to {dest.name}."
        if skipped:
            msg += f" Skipped missing: {', '.join(skipped)}."
        return OperationResult(
            name="Export zip",
            success=True,
            returncode=0,
            command=f"export-zip {dest_path}",
            stdout="\n".join(included),
            message=msg,
        )

    def import_zip(self, zip_path: str) -> OperationResult:
        src = Path(zip_path)
        if not src.exists():
            return OperationResult(
                name="Import zip",
                success=False,
                returncode=1,
                command=f"import-zip {zip_path}",
                stdout="",
                message=f"File not found: {zip_path}",
            )
        extracted: List[str] = []
        skipped: List[str] = []
        allowed = set((*self._DATA_FILES, "report_spec.json"))
        try:
            with zipfile.ZipFile(src, "r") as zf:
                for name in zf.namelist():
                    if name not in allowed:
                        skipped.append(name)
                        continue
                    dest = self.repo_root / name
                    dest.write_bytes(zf.read(name))
                    extracted.append(name)
        except Exception as exc:
            return OperationResult(
                name="Import zip",
                success=False,
                returncode=1,
                command=f"import-zip {zip_path}",
                stdout=str(exc),
                message=f"Import failed: {exc}",
            )
        if not extracted:
            return OperationResult(
                name="Import zip",
                success=False,
                returncode=1,
                command=f"import-zip {zip_path}",
                stdout="\n".join(skipped),
                message="No recognized data files found in the zip archive.",
            )
        msg = f"Imported {len(extracted)} file(s): {', '.join(extracted)}."
        if skipped:
            msg += f" Skipped unrecognized entries: {len(skipped)}."
        return OperationResult(
            name="Import zip",
            success=True,
            returncode=0,
            command=f"import-zip {zip_path}",
            stdout="\n".join(extracted),
            message=msg,
        )

    def _run_command(self, command: List[str], *, name: str) -> OperationResult:
        completed = subprocess.run(
            command,
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        stdout = str(completed.stdout or "")
        log_path = self._parse_log_path(stdout)
        report_path = ""
        report_json_path = ""
        for written in self._parse_written_paths(stdout):
            if written.endswith(".md"):
                report_path = written
            elif written.endswith(".json"):
                report_json_path = written
        returncode = int(completed.returncode or 0)
        success = returncode == 0
        message = self._success_message(name, report_path) if success else self._failure_message(stdout, returncode)
        return OperationResult(
            name=name,
            success=success,
            returncode=returncode,
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
