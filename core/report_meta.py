# Copyright (c) 2026 Sheng-Hsin Tsai
# SPDX-License-Identifier: MIT

from __future__ import annotations

import re
from typing import Any, Dict


def _normalize_mode_key(mode: Any) -> str:
    return re.sub(r"[\s_\-]+", "", str(mode or "").strip().lower())


def _get_mode_snapshot(states: Dict[str, Any], mode: Any) -> Dict[str, Any]:
    mode_label = str(mode or "").strip()
    mode_key = _normalize_mode_key(mode_label)
    if not mode_key:
        return {}
    store = states.get("by_mode")
    if not isinstance(store, dict):
        return {}
    snap = store.get(mode_key)
    if not isinstance(snap, dict):
        return {}
    out = dict(snap)
    out.setdefault("mode", mode_label or out.get("mode") or mode_key)
    out.setdefault("mode_key", mode_key)
    return out


def _migrate_state_schema(states: Dict[str, Any], *, ensure_broker_snapshot: bool = False) -> None:
    states.setdefault("meta", {})
    store = states.get("by_mode")
    if isinstance(store, dict):
        for snap in store.values():
            if isinstance(snap, dict):
                snap.pop("report_context", None)
                snap.pop("broker_context", None)
    portfolio = states.setdefault("portfolio", {})
    if ensure_broker_snapshot:
        broker = portfolio.setdefault("broker", {})
        broker.setdefault("snapshot", {})


def _effective_report_meta(states: Dict[str, Any], mode: Any) -> Dict[str, Any]:
    eff = dict(states.get("meta", {}) or {})
    transient = states.get("_report_meta")
    if isinstance(transient, dict):
        mode_key = _normalize_mode_key(mode)
        transient_mode_key = _normalize_mode_key(transient.get("mode_key") or transient.get("mode"))
        if not (transient_mode_key and mode_key and transient_mode_key != mode_key):
            for key in ("signal_basis", "execution_basis", "version_anchor_et", "version", "price_notes", "generated_at_et"):
                if key in transient:
                    eff[key] = transient.get(key)
            eff["mode"] = transient.get("mode") or str(mode or "").strip()
            return eff
    snap = _get_mode_snapshot(states, mode)
    if snap:
        for key in ("signal_basis", "execution_basis", "version_anchor_et", "version", "price_notes", "generated_at_et"):
            if key in snap:
                eff[key] = snap.get(key)
        eff["mode"] = snap.get("mode") or str(mode or "").strip()
    return eff
