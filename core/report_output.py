# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from core.report_bundle import build_report_root
from core.report_context import _report_date_from_meta
from core.report_meta import _effective_report_meta, _normalize_mode_key


def _build_report_output(
    states: Dict[str, Any],
    schema_path: str,
    report_dir: str,
    report_out: str,
    mode: str,
    config: Optional[Dict[str, Any]] = None,
    trades: Optional[list[Dict[str, Any]]] = None,
    cash_events: Optional[list[Dict[str, Any]]] = None,
    tactical_plan: Optional[Any] = None,
    report_meta: Optional[Dict[str, Any]] = None,
    market_history: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    report_root = build_report_root(
        states,
        config=config,
        trades=trades,
        cash_events=cash_events,
        tactical_plan=tactical_plan,
        report_meta=report_meta,
        market_history=market_history,
    )
    markdown, out_path = _render_report_output(
        report_root,
        schema_path=schema_path,
        report_dir=report_dir,
        report_out=report_out,
        mode=mode,
        report_meta=report_meta,
    )
    return markdown, out_path


def _build_report_output_path(report_dir: str, report_out: str, mode: str, report_meta: Optional[Dict[str, Any]] = None, states: Optional[Dict[str, Any]] = None) -> str:
    meta = dict(report_meta or {})
    if not meta and isinstance(states, dict):
        meta = _effective_report_meta(states, mode)
    report_date = _report_date_from_meta(meta) or datetime.now().strftime("%Y-%m-%d")
    if str(report_out or "").strip():
        return str(report_out).strip()
    return str(Path(report_dir) / f"{report_date}_{_normalize_mode_key(mode) or 'report'}.md")


def _build_report_json_output_path(report_dir: str, report_json_out: str, mode: str, report_meta: Optional[Dict[str, Any]] = None, states: Optional[Dict[str, Any]] = None) -> str:
    meta = dict(report_meta or {})
    if not meta and isinstance(states, dict):
        meta = _effective_report_meta(states, mode)
    report_date = _report_date_from_meta(meta) or datetime.now().strftime("%Y-%m-%d")
    if str(report_json_out or "").strip():
        return str(report_json_out).strip()
    return str(Path(report_dir) / f"{report_date}_{_normalize_mode_key(mode) or 'report'}.json")


def _render_report_output(
    report_root: Dict[str, Any],
    *,
    schema_path: str,
    report_dir: str,
    report_out: str,
    mode: str,
    report_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    from core.reporting import load_schema as _load_report_schema, render_report as _render_report_markdown

    schema = _load_report_schema(schema_path)
    markdown = _render_report_markdown(report_root, schema, mode)
    out_path = _build_report_output_path(report_dir=report_dir, report_out=report_out, mode=mode, report_meta=report_meta, states=report_root)
    return markdown, out_path
